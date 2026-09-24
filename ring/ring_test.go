package ring

import (
	"fmt"
	"testing"
)

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

func TestEmptyRing(t *testing.T) {
	r := New(100)
	if _, err := r.GetNode("anything"); err == nil {
		t.Fatal("expected an error looking up a key on an empty ring, got nil")
	}
}

func TestMinimalDisruption(t *testing.T) {
	r := New(150)

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

	if fractionMoved < 0.10 || fractionMoved > 0.35 {
		t.Errorf("expected roughly 10%%-35%% of keys to move, got %.1f%%", fractionMoved*100)
	}
}
