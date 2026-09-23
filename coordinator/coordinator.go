// Package coordinator implements N/R/W quorum reads and writes on top of
// the consistent hash ring, the RPC layer, and vector clocks.
//
// Write path: the FIRST node in the replica list (as returned by the ring)
// acts as the version-assigning replica for this write - it increments its
// own clock slot, and the resulting value is replicated verbatim to the
// other N-1 replicas. This keeps ordinary sequential writes to the same
// key from looking like conflicts with each other.
//
// Read path: all N replicas are queried, and their returned clocks are
// compared. If one version dominates all others, that's the resolved
// answer - no conflict, same as before. If two or more versions are
// mutually Concurrent, that's a REAL conflict (this is the case the old
// "pick highest version" logic would have silently mishandled) - all
// surviving (non-dominated) versions are returned so the caller can decide
// how to resolve it, rather than the coordinator silently discarding data.
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

// Put writes a key: the first replica versions it, the rest receive the
// finalized value verbatim. Succeeds once W total nodes (primary + acked
// replicates) have it.
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

	acked := 1 // primary counts as one ack

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

// VersionedValue is one surviving version of a key after conflict
// resolution. If Get returns more than one of these, it's a genuine,
// detected conflict - not an error, but a fact the caller must handle.
type VersionedValue struct {
	Data  string
	Clock vclock.Clock
}

type getResult struct {
	reply kvnode.GetReply
	err   error
}

// Get queries N replicas, waits for at least R responses, and returns the
// set of mutually-non-dominated ("maximal") versions found. Length 1 means
// no conflict. Length >1 means a real, detected concurrent conflict.
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
			continue // this replica didn't answer at all - doesn't count toward R
		}
		responded++ // it answered, whether or not it had the key
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

// resolve reduces a list of candidate versions down to the "maximal" set:
// versions that no other candidate strictly dominates. If everything
// collapses to one, there's no conflict. If more than one survives,
// they're genuinely concurrent and must all be surfaced.
func resolve(candidates []VersionedValue) []VersionedValue {
	var survivors []VersionedValue

	for _, candidate := range candidates {
		dominated := false
		var kept []VersionedValue

		for _, existing := range survivors {
			switch candidate.Clock.Compare(existing.Clock) {
			case vclock.Before:
				// candidate is strictly older than something we're already
				// keeping - discard the candidate entirely.
				dominated = true
				kept = append(kept, existing)
			case vclock.After:
				// candidate is strictly newer than this existing survivor -
				// the old one is now obsolete, drop it, keep the candidate.
				continue
			case vclock.Equal:
				// same version - keep one copy, discard the duplicate.
				dominated = true
				kept = append(kept, existing)
			case vclock.Concurrent:
				// neither dominates - both are genuinely independent, keep both.
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
