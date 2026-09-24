package vclock

import "testing"

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

func TestIncrementOnlyAffectsOwnSlot(t *testing.T) {
	c := Clock{"node-1": 5, "node-2": 3}
	next := c.Increment("node-1")

	if next["node-1"] != 6 {
		t.Errorf("node-1 = %d, want 6", next["node-1"])
	}
	if next["node-2"] != 3 {
		t.Errorf("node-2 changed to %d, want unchanged at 3", next["node-2"])
	}
	if c["node-1"] != 5 {
		t.Errorf("original clock was mutated: node-1 = %d, want 5", c["node-1"])
	}
}

func TestMissingNodeTreatedAsZero(t *testing.T) {
	a := Clock{"node-1": 1}
	b := Clock{"node-1": 1, "node-2": 1}

	if got := a.Compare(b); got != Before {
		t.Errorf("a.Compare(b) = %v, want Before", got)
	}
}

func TestConcurrentWritesSimulation(t *testing.T) {
	shared := Clock{"node-1": 3, "node-2": 3, "node-3": 3}

	writeOnNode1 := shared.Increment("node-1")
	writeOnNode3 := shared.Increment("node-3")

	rel := writeOnNode1.Compare(writeOnNode3)
	if rel != Concurrent {
		t.Fatalf("expected Concurrent (a real conflict), got %v - this would mean silently losing one write", rel)
	}
	t.Logf("correctly detected a real conflict: %v vs %v -> %v", writeOnNode1, writeOnNode3, rel)
}
