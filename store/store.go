package store

import (
	"sync"

	"distsys/vclock"
)

type Value struct {
	Data  string       `json:"data"`
	Clock vclock.Clock `json:"clock"`
}

type Store struct {
	mu       sync.Mutex       //lock
	data     map[string]Value //dict
	nodeName string
}

//what is Store?

func New(nodeName string) *Store {
	return &Store{data: make(map[string]Value), nodeName: nodeName}
} // constructor, a pointer is returned to a Store (this is a function)

func (s *Store) Put(key, data string, incomingClock vclock.Clock) Value { //method named Put, s mean this.
	s.mu.Lock()
	defer s.mu.Unlock() //run this when the function returns

	base := incomingClock
	if existing, ok := s.data[key]; ok {
		if base == nil {
			base = existing.Clock
		} else {
			base = base.Merge(existing.Clock)
		}
	}
	if base == nil {
		base = vclock.New()
	}

	newClock := base.Increment(s.nodeName)
	v := Value{Data: data, Clock: newClock}
	s.data[key] = v
	return v
}

// Replicate stores a value exactly as given - no increment. Used to
// propagate an already-versioned write (produced by Put on the
// coordinating replica) to the other replicas holding this key, so a
// single client write produces one consistent clock across all of them,
// rather than each replica inventing its own version independently.
func (s *Store) Replicate(key string, v Value) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = v
}

// Get returns the value for a key, and whether it was found at all.
func (s *Store) Get(key string) (Value, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	v, ok := s.data[key]
	return v, ok
}
