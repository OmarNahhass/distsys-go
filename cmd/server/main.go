package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"

	"distsys/rpc"
)

// EchoArgs / AddArgs are the concrete types each handler expects.
// The RPC layer itself never knows about these - it just passes along
// raw JSON and lets each handler decode its own shape.
type EchoArgs struct {
	Message string `json:"message"`
}

type AddArgs struct {
	A int `json:"a"`
	B int `json:"b"`
}

func main() {
	server := rpc.NewServer()

	server.Register("Echo", func(raw json.RawMessage) (interface{}, error) {
		var args EchoArgs
		if err := json.Unmarshal(raw, &args); err != nil {
			return nil, err
		}
		return map[string]string{"echoed": args.Message}, nil
	})

	server.Register("Add", func(raw json.RawMessage) (interface{}, error) {
		var args AddArgs
		if err := json.Unmarshal(raw, &args); err != nil {
			return nil, err
		}
		return map[string]int{"sum": args.A + args.B}, nil
	})

	addr := ":9090"
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", addr, err)
	}
	fmt.Printf("rpc server listening on %s\n", addr)

	if err := server.Serve(listener); err != nil {
		log.Fatalf("server stopped: %v", err)
	}
}
