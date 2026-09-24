package vclock

type Clock map[string]uint64

func New() Clock {
	return Clock{}
}

func (c Clock) Increment(node string) Clock {
	next := c.Copy()
	next[node] = next[node] + 1
	return next
}

func (c Clock) Copy() Clock {
	next := make(Clock, len(c))
	for k, v := range c {
		next[k] = v
	}
	return next
}

type Relationship int

const (
	Equal Relationship = iota
	Before
	After
	Concurrent
)

func (c Clock) Compare(other Clock) Relationship {
	cLessOrEqual := true
	otherLessOrEqual := true

	nodes := make(map[string]bool)
	for n := range c {
		nodes[n] = true
	}
	for n := range other {
		nodes[n] = true
	}

	for n := range nodes {
		cVal := c[n]
		otherVal := other[n]

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

func (c Clock) Merge(other Clock) Clock {
	merged := c.Copy()
	for n, v := range other {
		if v > merged[n] {
			merged[n] = v
		}
	}
	return merged
}
