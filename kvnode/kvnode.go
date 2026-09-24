package kvnode

import (
	"encoding/json"

	"distsys/rpc"
	"distsys/store"
	"distsys/vclock"
)

type PutArgs struct {
	Key   string       `json:"key"`
	Data  string       `json:"data"`
	Clock vclock.Clock `json:"clock,omitempty"`
}

type PutReply struct {
	Clock vclock.Clock `json:"clock"`
}

type ReplicateArgs struct {
	Key   string       `json:"key"`
	Data  string       `json:"data"`
	Clock vclock.Clock `json:"clock"`
}

type ReplicateReply struct {
	OK bool `json:"ok"`
}

type GetArgs struct {
	Key string `json:"key"`
}

type GetReply struct {
	Found bool         `json:"found"`
	Data  string       `json:"data"`
	Clock vclock.Clock `json:"clock"`
}

func Register(server *rpc.Server, s *store.Store) {
	server.Register("Put", func(raw json.RawMessage) (interface{}, error) {
		var args PutArgs
		if err := json.Unmarshal(raw, &args); err != nil {
			return nil, err
		}
		v := s.Put(args.Key, args.Data, args.Clock)
		return PutReply{Clock: v.Clock}, nil
	})

	server.Register("Replicate", func(raw json.RawMessage) (interface{}, error) {
		var args ReplicateArgs
		if err := json.Unmarshal(raw, &args); err != nil {
			return nil, err
		}
		s.Replicate(args.Key, store.Value{Data: args.Data, Clock: args.Clock})
		return ReplicateReply{OK: true}, nil
	})

	server.Register("Get", func(raw json.RawMessage) (interface{}, error) {
		var args GetArgs
		if err := json.Unmarshal(raw, &args); err != nil {
			return nil, err
		}
		v, ok := s.Get(args.Key)
		return GetReply{Found: ok, Data: v.Data, Clock: v.Clock}, nil
	})
}
