package gossip

import (
	"net"
	"testing"

	"distsys/rpc"
)

type testNode struct {
	name     string
	listener net.Listener
	gossiper *Gossiper
}

func startGossipNode(t *testing.T, name string) *testNode {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start listener for %s: %v", name, err)
	}
	g := New(name, listener.Addr().String())
	server := rpc.NewServer()
	Register(server, g)
	go server.Serve(listener)
	return &testNode{name: name, listener: listener, gossiper: g}
}

func TestFailureSpreadsWithoutDirectContact(t *testing.T) {
	a := startGossipNode(t, "node-A")
	b := startGossipNode(t, "node-B")
	c := startGossipNode(t, "node-C")

	for _, node := range []*testNode{a, b, c} {
		for _, peer := range []*testNode{a, b, c} {
			if peer.name != node.name {
				node.gossiper.Join(Member{Name: peer.name, Addr: peer.listener.Addr().String(), Alive: true})
			}
		}
	}

	c.listener.Close()

	if err := a.gossiper.Tick(); err != nil {
		t.Fatalf("A's first tick (to node-B) should succeed, got: %v", err)
	}

	if err := a.gossiper.Tick(); err == nil {
		t.Fatal("A's second tick (to node-C) should have failed - node-C is dead")
	}

	aView := membersByName(a.gossiper.Members())
	if aView["node-C"].Alive {
		t.Fatal("A directly contacted node-C and it failed - A should mark it dead in its own view")
	}
	t.Logf("A correctly detected node-C's death firsthand")

	if err := b.gossiper.Tick(); err != nil {
		t.Fatalf("B's tick (to node-A) should succeed, got: %v", err)
	}

	bView := membersByName(b.gossiper.Members())
	if bView["node-C"].Alive {
		t.Fatal("B never contacted node-C directly, but should have learned it's dead from A during the exchange - gossip propagation failed")
	}
	t.Logf("B correctly learned node-C is dead purely through gossip exchange with A - it never contacted node-C itself")
}

func TestDeadStatusIsNotOverwrittenByStaleGoodNews(t *testing.T) {
	a := New("node-A", "addr-a")
	a.members["node-C"] = Member{Name: "node-C", Addr: "addr-c", Alive: false}

	staleIncoming := []Member{{Name: "node-C", Addr: "addr-c", Alive: true}}
	a.merge(staleIncoming)

	if a.members["node-C"].Alive {
		t.Fatal("stale 'alive' report incorrectly overwrote A's existing 'dead' record for node-C")
	}
	t.Logf("dead status correctly held firm against stale incoming good news")
}

func membersByName(members []Member) map[string]Member {
	out := make(map[string]Member, len(members))
	for _, m := range members {
		out[m.Name] = m
	}
	return out
}
