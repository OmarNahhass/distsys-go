package vclock

import "testing"

// TestDominance mirrors the first example we reasoned through by hand:
// A = {node-1: 2, node-2: 1}, B = {node-1: 2, node-2: 2}.
// B is ahead on node-2 and tied on node-1 - B should be strictly After A.
func TestDominance(t *testing.T) {
	a := Clock{"node-1": 2, "node-2": 1}
	b := Clock{"node-1": 2, "node-2": 2}

	if got := a.Compare(b); got != Before {
		t.Errorf("a.Compare(b) = %v, want Before", got)
	}
	if got := b.Compare(a); got != After {
		t.Errorf("b.Compare(a) = %v, want After", got)
	}
}

// TestConcurrent mirrors the quiz question: A ahead on node-1, B ahead on
// node-2 - neither dominates, so these must be flagged Concurrent, never
// silently resolved in either direction.
func TestConcurrent(t *testing.T) {
	a := Clock{"node-1": 2, "node-2": 1}
	b := Clock{"node-1": 1, "node-2": 2}

	if got := a.Compare(b); got != Concurrent {
		t.Errorf("a.Compare(b) = %v, want Concurrent", got)
	}
	if got := b.Compare(a); got != Concurrent {
		t.Errorf("b.Compare(a) = %v, want Concurrent", got)
	}
}

func TestEqual(t *testing.T) {
	a := Clock{"node-1": 2, "node-2": 1}
	b := Clock{"node-1": 2, "node-2": 1}

	if got := a.Compare(b); got != Equal {
		t.Errorf("a.Compare(b) = %v, want Equal", got)
	}
}

// TestIncrementOnlyAffectsOwnSlot proves a node writing never touches
// another node's counter - the core rule that makes the whole scheme work.
func TestIncrementOnlyAffectsOwnSlot(t *testing.T) {
	c := Clock{"node-1": 5, "node-2": 3}
	next := c.Increment("node-1")

	if next["node-1"] != 6 {
		t.Errorf("node-1 = %d, want 6", next["node-1"])
	}
	if next["node-2"] != 3 {
		t.Errorf("node-2 changed to %d, want unchanged at 3", next["node-2"])
	}
	// original must be untouched - Increment returns a new clock
	if c["node-1"] != 5 {
		t.Errorf("original clock was mutated: node-1 = %d, want 5", c["node-1"])
	}
}

// TestMissingNodeTreatedAsZero proves a node that's never written to a key
// is correctly treated as having 0 writes, not as an error or special case.
func TestMissingNodeTreatedAsZero(t *testing.T) {
	a := Clock{"node-1": 1} // node-2, node-3 never wrote
	b := Clock{"node-1": 1, "node-2": 1}

	// b knows about a write a doesn't (node-2's) - b is strictly newer.
	if got := a.Compare(b); got != Before {
		t.Errorf("a.Compare(b) = %v, want Before", got)
	}
}

// TestConcurrentWritesSimulation is the realistic version of the scenario
// that motivated this whole package: two nodes, starting from the same
// known state, each handle an independent write without knowing about the
// other's - proving the resulting clocks come back Concurrent, exactly the
// case our old single-counter version.Version would have silently hidden.
func TestConcurrentWritesSimulation(t *testing.T) {
	shared := Clock{"node-1": 3, "node-2": 3, "node-3": 3} // both start in sync

	writeOnNode1 := shared.Increment("node-1") // client A writes, hits node-1
	writeOnNode3 := shared.Increment("node-3") // client B writes, hits node-3, unaware of A's write

	rel := writeOnNode1.Compare(writeOnNode3)
	if rel != Concurrent {
		t.Fatalf("expected Concurrent (a real conflict), got %v - this would mean silently losing one write", rel)
	}
	t.Logf("correctly detected a real conflict: %v vs %v -> %v", writeOnNode1, writeOnNode3, rel)
}
