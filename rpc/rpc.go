package rpc

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
)

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
		return nil, err
	}
	size := binary.BigEndian.Uint32(lenBuf)
	payload := make([]byte, size)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, fmt.Errorf("read payload: %w", err)
	}
	return payload, nil
}

type request struct {
	Method string          `json:"method"`
	Args   json.RawMessage `json:"args"`
}

type response struct {
	Result json.RawMessage `json:"result,omitempty"`
	Error  string          `json:"error,omitempty"`
}

type HandlerFunc func(args json.RawMessage) (interface{}, error)

type Server struct {
	mu       sync.RWMutex
	handlers map[string]HandlerFunc
}

func NewServer() *Server {
	return &Server{handlers: make(map[string]HandlerFunc)}
}

func (s *Server) Register(method string, h HandlerFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[method] = h
}

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
			return
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

type Client struct {
	mu   sync.Mutex
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
