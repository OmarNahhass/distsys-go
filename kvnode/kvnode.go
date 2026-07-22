// Package kvnode wires a store.Store into an rpc.Server, exposing "Put" and
// "Get" as callable RPC methods. This is what actually runs on each node -
// the RPC layer handles the networking, this package handles what those
// requests actually mean.
package kvnode

import (
	"encoding/json"

	"distsys/rpc"
	"distsys/store"
)

type PutArgs struct {
	Key  string `json:"key"`
	Data string `json:"data"`
}

type PutReply struct {
	Version uint64 `json:"version"`
}

type GetArgs struct {
	Key string `json:"key"`
}

type GetReply struct {
	Found   bool   `json:"found"`
	Data    string `json:"data"`
	Version uint64 `json:"version"`
}

// Register binds "Put" and "Get" RPC methods on server to the given store.
func Register(server *rpc.Server, s *store.Store) {
	server.Register("Put", func(raw json.RawMessage) (interface{}, error) {
		var args PutArgs
		if err := json.Unmarshal(raw, &args); err != nil {
			return nil, err
		}
		v := s.Put(args.Key, args.Data)
		return PutReply{Version: v.Version}, nil
	})

	server.Register("Get", func(raw json.RawMessage) (interface{}, error) {
		var args GetArgs
		if err := json.Unmarshal(raw, &args); err != nil {
			return nil, err
		}
		v, ok := s.Get(args.Key)
		return GetReply{Found: ok, Data: v.Data, Version: v.Version}, nil
	})
}
