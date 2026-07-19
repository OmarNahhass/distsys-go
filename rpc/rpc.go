// Package rpc implements a minimal request/response RPC system over raw TCP.
//
// This exists to make the RPC illusion concrete: a "remote call" is really
// just serialize -> send bytes -> deserialize -> run function -> serialize
// result -> send bytes back -> deserialize. Every later abstraction we use
// (net/rpc, gRPC, Raft's RPCs) is doing exactly this under the hood.
package rpc

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
)

// --- Message framing ---------------------------------------------------
//
// TCP is just a stream of bytes with no concept of "message boundaries."
// If we write two JSON messages back to back, the reader might see them
// concatenated, or split in the middle, depending entirely on timing.
//
// The fix: prefix every message with its length (4 bytes, big-endian).
// The reader always knows exactly how many bytes to read for "this message",
// no matter how the underlying TCP stream happened to chop up the bytes.

func writeFrame(w io.Writer, payload []byte) error {
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(payload)))
	if _, err := w.Write(lenBuf); err != nil {
		return fmt.Errorf("write length prefix: %w", err)
	}
	if _, err := w.Write(payload); err != nil {
		return fmt.Errorf("write payload: %w", err)
	}
	return nil
}

func readFrame(r io.Reader) ([]byte, error) {
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lenBuf); err != nil {
		return nil, err // includes io.EOF when the connection closes cleanly
	}
	size := binary.BigEndian.Uint32(lenBuf)
	payload := make([]byte, size)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, fmt.Errorf("read payload: %w", err)
	}
	return payload, nil
}

// --- Wire format ---------------------------------------------------------

// request is what goes out on the wire for every call.
// Args stays as raw JSON so the RPC layer itself never needs to know
// the concrete argument types of any given method - handlers decode
// their own args.
type request struct {
	Method string          `json:"method"`
	Args   json.RawMessage `json:"args"`
}

// response is what comes back on the wire for every call.
type response struct {
	Result json.RawMessage `json:"result,omitempty"`
	Error  string          `json:"error,omitempty"`
}

// --- Server ----------------------------------------------------------------

// HandlerFunc is the shape every registered RPC method must have.
// It receives raw args (still-encoded JSON) and returns a result to encode,
// or an error.
type HandlerFunc func(args json.RawMessage) (interface{}, error)

type Server struct {
	mu       sync.RWMutex
	handlers map[string]HandlerFunc
}

func NewServer() *Server {
	return &Server{handlers: make(map[string]HandlerFunc)}
}

// Register binds a method name (e.g. "KV.Get") to a handler function.
func (s *Server) Register(method string, h HandlerFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[method] = h
}

// Serve accepts connections on the listener forever, handling each on its
// own goroutine - this is where Go's concurrency model earns its keep:
// one goroutine per connection, no manual thread pool needed.
func (s *Server) Serve(l net.Listener) error {
	for {
		conn, err := l.Accept()
		if err != nil {
			return err
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	for {
		payload, err := readFrame(conn)
		if err != nil {
			return // client disconnected, or a real error - either way, done
		}

		var req request
		if err := json.Unmarshal(payload, &req); err != nil {
			s.sendError(conn, fmt.Sprintf("bad request: %v", err))
			continue
		}

		s.mu.RLock()
		handler, ok := s.handlers[req.Method]
		s.mu.RUnlock()

		if !ok {
			s.sendError(conn, fmt.Sprintf("unknown method: %s", req.Method))
			continue
		}

		result, err := handler(req.Args)
		if err != nil {
			s.sendError(conn, err.Error())
			continue
		}

		resultBytes, err := json.Marshal(result)
		if err != nil {
			s.sendError(conn, fmt.Sprintf("failed to encode result: %v", err))
			continue
		}

		respBytes, _ := json.Marshal(response{Result: resultBytes})
		if err := writeFrame(conn, respBytes); err != nil {
			return
		}
	}
}

func (s *Server) sendError(conn net.Conn, msg string) {
	respBytes, _ := json.Marshal(response{Error: msg})
	writeFrame(conn, respBytes)
}

// --- Client ----------------------------------------------------------------

type Client struct {
	mu   sync.Mutex // one call at a time per connection, kept deliberately simple for now
	conn net.Conn
}

func Dial(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

// Call sends a request and blocks until the response arrives (or an error
// occurs). reply must be a pointer - the decoded result gets written into it,
// same pattern as json.Unmarshal.
func (c *Client) Call(method string, args interface{}, reply interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	argsBytes, err := json.Marshal(args)
	if err != nil {
		return fmt.Errorf("encode args: %w", err)
	}

	reqBytes, err := json.Marshal(request{Method: method, Args: argsBytes})
	if err != nil {
		return fmt.Errorf("encode request: %w", err)
	}

	if err := writeFrame(c.conn, reqBytes); err != nil {
		return fmt.Errorf("send request: %w", err)
	}

	payload, err := readFrame(c.conn)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	var resp response
	if err := json.Unmarshal(payload, &resp); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	if resp.Error != "" {
		return fmt.Errorf("remote error: %s", resp.Error)
	}

	if reply != nil && resp.Result != nil {
		if err := json.Unmarshal(resp.Result, reply); err != nil {
			return fmt.Errorf("decode result: %w", err)
		}
	}

	return nil
}
