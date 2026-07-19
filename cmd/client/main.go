package main

import (
	"fmt"
	"log"

	"distsys/rpc"
)

func main() {
	client, err := rpc.Dial("localhost:9090")
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer client.Close()

	var echoReply struct {
		Echoed string `json:"echoed"`
	}
	if err := client.Call("Echo", map[string]string{"message": "hello from client"}, &echoReply); err != nil {
		log.Fatalf("Echo call failed: %v", err)
	}
	fmt.Printf("Echo response: %s\n", echoReply.Echoed)

	var addReply struct {
		Sum int `json:"sum"`
	}
	if err := client.Call("Add", map[string]int{"a": 4, "b": 9}, &addReply); err != nil {
		log.Fatalf("Add call failed: %v", err)
	}
	fmt.Printf("Add response: %d\n", addReply.Sum)

	// Deliberately call a method that doesn't exist, to see error handling
	// flow all the way back through the RPC layer as a real Go error.
	var junk struct{}
	if err := client.Call("DoesNotExist", nil, &junk); err != nil {
		fmt.Printf("Expected error calling unknown method: %v\n", err)
	}
}
