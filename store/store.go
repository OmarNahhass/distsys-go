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
	mu       sync.Mutex
	data     map[string]Value
	nodeName string
}

func New(nodeName string) *Store {
	return &Store{data: make(map[string]Value), nodeName: nodeName}
}

func (s *Store) Put(key, data string, incomingClock vclock.Clock) Value {
	s.mu.Lock()
	defer s.mu.Unlock()

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

func (s *Store) Replicate(key string, v Value) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = v
}

func (s *Store) Get(key string) (Value, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	v, ok := s.data[key]
	return v, ok
}
