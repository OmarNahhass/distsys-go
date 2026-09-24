package coordinator

import (
	"fmt"

	"distsys/kvnode"
	"distsys/ring"
	"distsys/rpc"
	"distsys/vclock"
)

type Coordinator struct {
	Ring    *ring.Ring
	Addrs   map[string]string
	N, W, R int
}

func New(r *ring.Ring, addrs map[string]string, n, w, rQuorum int) *Coordinator {
	return &Coordinator{Ring: r, Addrs: addrs, N: n, W: w, R: rQuorum}
}

func (c *Coordinator) Put(key, data string) error {
	nodes, err := c.Ring.GetNodes(key, c.N)
	if err != nil {
		return fmt.Errorf("finding replicas: %w", err)
	}
	if len(nodes) == 0 {
		return fmt.Errorf("no replicas available")
	}

	primary := nodes[0]
	client, err := rpc.Dial(c.Addrs[primary])
	if err != nil {
		return fmt.Errorf("write failed: primary %s unreachable: %w", primary, err)
	}
	var putReply kvnode.PutReply
	err = client.Call("Put", kvnode.PutArgs{Key: key, Data: data}, &putReply)
	client.Close()
	if err != nil {
		return fmt.Errorf("write failed: primary %s rejected write: %w", primary, err)
	}

	acked := 1

	type repResult struct{ err error }
	results := make(chan repResult, len(nodes)-1)
	for _, node := range nodes[1:] {
		go func(node string) {
			client, err := rpc.Dial(c.Addrs[node])
			if err != nil {
				results <- repResult{err: err}
				return
			}
			defer client.Close()
			var reply kvnode.ReplicateReply
			err = client.Call("Replicate", kvnode.ReplicateArgs{Key: key, Data: data, Clock: putReply.Clock}, &reply)
			results <- repResult{err: err}
		}(node)
	}
	for i := 0; i < len(nodes)-1; i++ {
		if res := <-results; res.err == nil {
			acked++
		}
	}

	if acked < c.W {
		return fmt.Errorf("write failed: only %d/%d replicas have the write (need W=%d)", acked, len(nodes), c.W)
	}
	return nil
}

type VersionedValue struct {
	Data  string
	Clock vclock.Clock
}

type getResult struct {
	reply kvnode.GetReply
	err   error
}

func (c *Coordinator) Get(key string) ([]VersionedValue, error) {
	nodes, err := c.Ring.GetNodes(key, c.N)
	if err != nil {
		return nil, fmt.Errorf("finding replicas: %w", err)
	}

	results := make(chan getResult, len(nodes))
	for _, node := range nodes {
		go func(node string) {
			client, err := rpc.Dial(c.Addrs[node])
			if err != nil {
				results <- getResult{err: err}
				return
			}
			defer client.Close()
			var reply kvnode.GetReply
			err = client.Call("Get", kvnode.GetArgs{Key: key}, &reply)
			results <- getResult{reply: reply, err: err}
		}(node)
	}

	responded := 0
	var candidates []VersionedValue
	for i := 0; i < len(nodes); i++ {
		res := <-results
		if res.err != nil {
			continue
		}
		responded++
		if res.reply.Found {
			candidates = append(candidates, VersionedValue{Data: res.reply.Data, Clock: res.reply.Clock})
		}
	}

	if responded < c.R {
		return nil, fmt.Errorf("read failed: only %d/%d replicas responded (need R=%d)", responded, len(nodes), c.R)
	}
	if len(candidates) == 0 {
		return nil, fmt.Errorf("key not found among %d responding replicas", responded)
	}

	return resolve(candidates), nil
}

func resolve(candidates []VersionedValue) []VersionedValue {
	var survivors []VersionedValue

	for _, candidate := range candidates {
		dominated := false
		var kept []VersionedValue

		for _, existing := range survivors {
			switch candidate.Clock.Compare(existing.Clock) {
			case vclock.Before:

				dominated = true
				kept = append(kept, existing)
			case vclock.After:
				continue
			case vclock.Equal:
				dominated = true
				kept = append(kept, existing)
			case vclock.Concurrent:
				kept = append(kept, existing)
			}
		}

		if !dominated {
			kept = append(kept, candidate)
		}
		survivors = kept
	}

	return survivors
}
