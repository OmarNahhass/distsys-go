// Package coordinator implements N/R/W quorum reads and writes on top of
// the consistent hash ring and the RPC layer.
//
// The idea: a key is replicated to N nodes (via ring.GetNodes). A write
// succeeds once W of those N nodes have acknowledged it. A read queries
// nodes until R of them have responded, then returns whichever response
// has the highest version number.
//
// The guarantee: if W + R > N, any successful read's set of R nodes is
// mathematically guaranteed to overlap with any successful write's set of
// W nodes on at least one node - so a read can never completely miss the
// most recent write, even though not every node is contacted every time.
package coordinator

import (
	"fmt"

	"distsys/kvnode"
	"distsys/ring"
	"distsys/rpc"
)

type Coordinator struct {
	Ring    *ring.Ring
	Addrs   map[string]string // physical node name -> "host:port"
	N, W, R int
}

func New(r *ring.Ring, addrs map[string]string, n, w, rQuorum int) *Coordinator {
	return &Coordinator{Ring: r, Addrs: addrs, N: n, W: w, R: rQuorum}
}

type putResult struct {
	node    string
	version uint64
	err     error
}

// Put replicates a write to N nodes and returns success once W have acked.
func (c *Coordinator) Put(key, data string) error {
	nodes, err := c.Ring.GetNodes(key, c.N)
	if err != nil {
		return fmt.Errorf("finding replicas: %w", err)
	}

	results := make(chan putResult, len(nodes))
	for _, node := range nodes {
		go func(node string) {
			addr := c.Addrs[node]
			client, err := rpc.Dial(addr)
			if err != nil {
				results <- putResult{node: node, err: err}
				return
			}
			defer client.Close()

			var reply kvnode.PutReply
			err = client.Call("Put", kvnode.PutArgs{Key: key, Data: data}, &reply)
			results <- putResult{node: node, version: reply.Version, err: err}
		}(node)
	}

	acked := 0
	var lastErr error
	for i := 0; i < len(nodes); i++ {
		res := <-results
		if res.err == nil {
			acked++
		} else {
			lastErr = res.err
		}
	}

	if acked < c.W {
		return fmt.Errorf("write failed: only %d/%d replicas acked (need W=%d), last error: %v", acked, len(nodes), c.W, lastErr)
	}
	return nil
}

type getResult struct {
	node  string
	reply kvnode.GetReply
	err   error
}

// Get queries N replicas and returns the highest-versioned response, once
// at least R replicas have responded.
func (c *Coordinator) Get(key string) (string, uint64, error) {
	nodes, err := c.Ring.GetNodes(key, c.N)
	if err != nil {
		return "", 0, fmt.Errorf("finding replicas: %w", err)
	}

	results := make(chan getResult, len(nodes))
	for _, node := range nodes {
		go func(node string) {
			addr := c.Addrs[node]
			client, err := rpc.Dial(addr)
			if err != nil {
				results <- getResult{node: node, err: err}
				return
			}
			defer client.Close()

			var reply kvnode.GetReply
			err = client.Call("Get", kvnode.GetArgs{Key: key}, &reply)
			results <- getResult{node: node, reply: reply, err: err}
		}(node)
	}

	responded := 0
	var best kvnode.GetReply
	for i := 0; i < len(nodes); i++ {
		res := <-results
		if res.err != nil {
			continue // this replica didn't answer - fine, as long as enough others do
		}
		responded++
		if res.reply.Found && res.reply.Version > best.Version {
			best = res.reply
		}
	}

	if responded < c.R {
		return "", 0, fmt.Errorf("read failed: only %d/%d replicas responded (need R=%d)", responded, len(nodes), c.R)
	}
	if !best.Found {
		return "", 0, fmt.Errorf("key not found among %d responding replicas", responded)
	}

	return best.Data, best.Version, nil
}
