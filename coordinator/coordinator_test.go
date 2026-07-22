package coordinator

import (
	"net"
	"testing"

	"distsys/kvnode"
	"distsys/ring"
	"distsys/rpc"
	"distsys/store"
)

// testNode is one real, running node: an actual TCP listener plus an
// rpc.Server plus a store - the same shape as the demo server/client we
// built first, just wired up programmatically for the test.
type testNode struct {
	name     string
	addr     string
	listener net.Listener
}

func startTestNode(t *testing.T, name string) *testNode {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0") // :0 = let the OS pick a free port
	if err != nil {
		t.Fatalf("failed to start listener for %s: %v", name, err)
	}

	server := rpc.NewServer()
	kvnode.Register(server, store.New())

	go server.Serve(listener) // real accept loop, same as cmd/server/main.go

	return &testNode{name: name, addr: listener.Addr().String(), listener: listener}
}

func (n *testNode) stop() {
	n.listener.Close()
}

// TestQuorumWriteThenReadSurvivesNodeFailure is the payoff test: it proves
// the W+R>N overlap guarantee in a real scenario, not just in theory.
//
// Setup: N=3, W=2, R=2 (so W+R=4 > N=3 - the guarantee should hold).
// 1. Write a key - succeeds once 2 of 3 replicas ack.
// 2. Kill one node, simulating a real failure.
// 3. Read the key - only 2 nodes can possibly respond now, but the overlap
//    guarantee says at least one of them must have the write we just made.
func TestQuorumWriteThenReadSurvivesNodeFailure(t *testing.T) {
	const n, w, r = 3, 2, 2

	nodeNames := []string{"node-1", "node-2", "node-3"}
	nodes := make(map[string]*testNode)
	addrs := make(map[string]string)

	for _, name := range nodeNames {
		tn := startTestNode(t, name)
		nodes[name] = tn
		addrs[name] = tn.addr
	}
	defer func() {
		for _, tn := range nodes {
			tn.stop()
		}
	}()

	hashRing := ring.New(150)
	for _, name := range nodeNames {
		hashRing.AddNode(name)
	}

	coord := New(hashRing, addrs, n, w, r)

	key := "fight:usman-vs-edwards:odds"
	if err := coord.Put(key, "usman -150"); err != nil {
		t.Fatalf("quorum write failed: %v", err)
	}

	// Figure out which physical nodes this key actually replicates to,
	// so we kill one of *those* - killing an unrelated node would prove
	// nothing.
	replicas, err := hashRing.GetNodes(key, n)
	if err != nil {
		t.Fatalf("failed to get replicas: %v", err)
	}
	t.Logf("key %q replicates to: %v", key, replicas)

	// Kill the first replica - simulates a real node crash mid-operation.
	killed := replicas[0]
	t.Logf("simulating failure of node: %s", killed)
	nodes[killed].stop()

	data, version, err := coord.Get(key)
	if err != nil {
		t.Fatalf("quorum read failed after node failure, but W+R>N should have prevented this: %v", err)
	}

	if data != "usman -150" {
		t.Errorf("got data %q, want %q", data, "usman -150")
	}
	if version != 1 {
		t.Errorf("got version %d, want 1", version)
	}

	t.Logf("read succeeded despite %s being down: data=%q version=%d", killed, data, version)
}

// TestQuorumWriteFailsWithoutEnoughReplicas proves the coordinator actually
// enforces W, rather than silently succeeding on fewer acks than promised.
func TestQuorumWriteFailsWithoutEnoughReplicas(t *testing.T) {
	const n, w, r = 3, 3, 1 // W=3 means ALL replicas must ack

	nodeNames := []string{"node-1", "node-2", "node-3"}
	nodes := make(map[string]*testNode)
	addrs := make(map[string]string)

	for _, name := range nodeNames {
		tn := startTestNode(t, name)
		nodes[name] = tn
		addrs[name] = tn.addr
	}
	defer func() {
		for _, tn := range nodes {
			tn.stop()
		}
	}()

	hashRing := ring.New(150)
	for _, name := range nodeNames {
		hashRing.AddNode(name)
	}

	coord := New(hashRing, addrs, n, w, r)

	key := "some-key"
	replicas, _ := hashRing.GetNodes(key, n)

	// Kill one replica BEFORE writing, so only 2 of 3 can possibly ack -
	// which is less than W=3.
	nodes[replicas[0]].stop()

	err := coord.Put(key, "value")
	if err == nil {
		t.Fatal("expected write to fail with W=3 and only 2 replicas available, but it succeeded")
	}
	t.Logf("write correctly failed: %v", err)
}
