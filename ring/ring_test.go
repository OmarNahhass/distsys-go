package ring

import (
	"fmt"
	"testing"
)

// TestDeterministic proves the core property everything else depends on:
// looking up the same key twice, with the ring unchanged, always returns
// the same owner. If this weren't true, nothing downstream could work.
func TestDeterministic(t *testing.T) {
	r := New(100)
	r.AddNode("node-A")
	r.AddNode("node-B")
	r.AddNode("node-C")

	key := "some-key-123"
	first, err := r.GetNode(key)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for i := 0; i < 100; i++ {
		got, err := r.GetNode(key)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != first {
			t.Fatalf("lookup %d: got owner %s, want %s (should never change with a stable ring)", i, got, first)
		}
	}
}

// TestEmptyRing proves we fail loudly instead of silently misbehaving
// when nobody has joined the cluster yet.
func TestEmptyRing(t *testing.T) {
	r := New(100)
	if _, err := r.GetNode("anything"); err == nil {
		t.Fatal("expected an error looking up a key on an empty ring, got nil")
	}
}

// TestMinimalDisruption is the important one: it proves consistent hashing's
// actual selling point over hash(key) % N. We record every key's owner with
// 4 nodes, add a 5th, and check what fraction of keys changed owner.
//
// With naive modulo hashing, going from 4 to 5 nodes reassigns roughly
// (N-1)/N of all keys - about 80% here. With consistent hashing, only the
// keys that happen to fall in the new node's slice of the ring should move -
// roughly 1/5, i.e. about 20%, and never anywhere close to 80%.
func TestMinimalDisruption(t *testing.T) {
	r := New(150) // enough virtual nodes to get a fairly even split

	initialNodes := []string{"node-A", "node-B", "node-C", "node-D"}
	for _, n := range initialNodes {
		r.AddNode(n)
	}

	const numKeys = 10000
	keys := make([]string, numKeys)
	before := make(map[string]string, numKeys)

	for i := 0; i < numKeys; i++ {
		keys[i] = fmt.Sprintf("key-%d", i)
		owner, err := r.GetNode(keys[i])
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		before[keys[i]] = owner
	}

	// Now the cluster scales up - a 5th node joins.
	r.AddNode("node-E")

	moved := 0
	for _, k := range keys {
		owner, err := r.GetNode(k)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if owner != before[k] {
			moved++
		}
	}

	fractionMoved := float64(moved) / float64(numKeys)
	t.Logf("keys reassigned after adding a 5th node: %d / %d (%.1f%%)", moved, numKeys, fractionMoved*100)

	// Expect roughly 1/5 (20%) to move. Allow a generous band (10%-35%)
	// since virtual-node placement introduces some randomness, but this
	// should be nowhere near the ~80% that naive modulo hashing would cause.
	if fractionMoved < 0.10 || fractionMoved > 0.35 {
		t.Errorf("expected roughly 10%%-35%% of keys to move, got %.1f%%", fractionMoved*100)
	}
}
