package coordinator

import (
	"net"
	"testing"

	"distsys/kvnode"
	"distsys/ring"
	"distsys/rpc"
	"distsys/store"
)

type testNode struct {
	name     string
	addr     string
	listener net.Listener
}

func startTestNode(t *testing.T, name string) *testNode {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start listener for %s: %v", name, err)
	}
	server := rpc.NewServer()
	kvnode.Register(server, store.New(name)) 
	go server.Serve(listener)
	return &testNode{name: name, addr: listener.Addr().String(), listener: listener}
}

func (n *testNode) stop() {
	n.listener.Close()
}

func setupCluster(t *testing.T, names []string) (map[string]*testNode, *ring.Ring, map[string]string) {
	t.Helper()
	nodes := make(map[string]*testNode)
	addrs := make(map[string]string)
	for _, name := range names {
		tn := startTestNode(t, name)
		nodes[name] = tn
		addrs[name] = tn.addr
	}
	r := ring.New(150)
	for _, name := range names {
		r.AddNode(name)
	}
	return nodes, r, addrs
}


func TestNormalWriteHasNoConflict(t *testing.T) {
	names := []string{"node-1", "node-2", "node-3"}
	nodes, r, addrs := setupCluster(t, names)
	defer func() {
		for _, n := range nodes {
			n.stop()
		}
	}()

	coord := New(r, addrs, 3, 2, 2)
	key := "fight:usman-vs-edwards:odds"

	if err := coord.Put(key, "usman -150"); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	results, err := coord.Get(key)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected exactly 1 version (no conflict), got %d: %+v", len(results), results)
	}
	if results[0].Data != "usman -150" {
		t.Errorf("got data %q, want %q", results[0].Data, "usman -150")
	}
	t.Logf("normal write resolved cleanly, clock: %v", results[0].Clock)
}


func TestConcurrentWritesAreDetectedAsConflict(t *testing.T) {
	names := []string{"node-1", "node-2", "node-3"}
	nodes, r, addrs := setupCluster(t, names)
	defer func() {
		for _, n := range nodes {
			n.stop()
		}
	}()

	key := "fight:usman-vs-edwards:odds"
	replicas, err := r.GetNodes(key, 3)
	if err != nil {
		t.Fatalf("failed to get replicas: %v", err)
	}
	t.Logf("key replicates to: %v", replicas)


	callPutDirectly := func(nodeName, data string) {
		client, err := rpc.Dial(addrs[nodeName])
		if err != nil {
			t.Fatalf("failed to dial %s: %v", nodeName, err)
		}
		defer client.Close()
		var reply kvnode.PutReply
		if err := client.Call("Put", kvnode.PutArgs{Key: key, Data: data}, &reply); err != nil {
			t.Fatalf("direct put to %s failed: %v", nodeName, err)
		}
	}

	callPutDirectly(replicas[0], "usman -150") 
	callPutDirectly(replicas[1], "usman -200") 

	coord := New(r, addrs, 3, 2, 3) 
	results, err := coord.Get(key)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected a detected conflict with 2 surviving versions, got %d: %+v", len(results), results)
	}

	dataSeen := map[string]bool{}
	for _, r := range results {
		dataSeen[r.Data] = true
		t.Logf("surviving conflicting version: data=%q clock=%v", r.Data, r.Clock)
	}
	if !dataSeen["usman -150"] || !dataSeen["usman -200"] {
		t.Errorf("expected both conflicting values to survive, got: %+v", results)
	}
}
