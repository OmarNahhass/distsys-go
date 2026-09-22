// Package vclock implements vector clocks: a way to compare two versions of
// the same piece of data and determine whether one causally happened-after
// the other, or whether they're genuinely concurrent (independent writes
// that neither knows about the other).
//
// This is the fix for the gap left in store.go's naive per-node version
// counter: a single number can't tell you whether two "version 2"s from
// different nodes are the same write or two different, conflicting writes.
// A vector clock - one counter per node, kept together - can.
package vclock

// Clock maps a node's name to how many writes *that node* has made to this
// piece of data. It only ever grows in the slot belonging to whichever node
// is doing the writing - never any other node's slot.
type Clock map[string]uint64

// New returns an empty clock - no writes yet from anyone.
func New() Clock {
	return Clock{}
}

// Increment returns a new clock with the given node's counter bumped by one,
// leaving every other node's counter untouched. This is the vector-clock
// equivalent of Lamport's "increment before doing something" rule, but
// scoped to just the acting node's own slot instead of one shared counter.
func (c Clock) Increment(node string) Clock {
	next := c.Copy()
	next[node] = next[node] + 1
	return next
}

// Copy returns a independent copy, so callers can't accidentally mutate a
// clock that's shared/stored elsewhere.
func (c Clock) Copy() Clock {
	next := make(Clock, len(c))
	for k, v := range c {
		next[k] = v
	}
	return next
}

// Relationship describes how two clocks relate to each other causally.
type Relationship int

const (
	Equal      Relationship = iota // identical - same version
	Before                         // the receiver happened-before other (other is newer)
	After                          // the receiver happened-after other (receiver is newer)
	Concurrent                     // neither happened-before the other - a real conflict
)

// Compare determines the causal relationship between c and other.
//
// The rule: look at every node mentioned in either clock. If c's count is
// <= other's count for every single node, and strictly less for at least
// one, then c happened-before other (other is strictly newer - safe to
// keep other and discard c). If it's the reverse, c happened-after other.
// If neither holds - c is ahead on some node(s) and behind on others - the
// two are Concurrent: genuinely independent writes, and neither can be
// safely discarded without losing information.
func (c Clock) Compare(other Clock) Relationship {
	cLessOrEqual := true     // does c <= other on every node?
	otherLessOrEqual := true // does other <= c on every node?

	nodes := make(map[string]bool)
	for n := range c {
		nodes[n] = true
	}
	for n := range other {
		nodes[n] = true
	}

	for n := range nodes {
		cVal := c[n]         // missing entries default to 0, which is correct:
		otherVal := other[n] // a node that never wrote has 0 writes.

		if cVal > otherVal {
			cLessOrEqual = false
		}
		if otherVal > cVal {
			otherLessOrEqual = false
		}
	}

	switch {
	case cLessOrEqual && otherLessOrEqual:
		return Equal
	case cLessOrEqual:
		return Before
	case otherLessOrEqual:
		return After
	default:
		return Concurrent
	}
}

// Merge combines two clocks by taking the max of each node's counter - used
// during read-repair/reconciliation once a conflict has been resolved (or
// to compute a new clock that "knows about" everything both inputs knew).
func (c Clock) Merge(other Clock) Clock {
	merged := c.Copy()
	for n, v := range other {
		if v > merged[n] {
			merged[n] = v
		}
	}
	return merged
}
