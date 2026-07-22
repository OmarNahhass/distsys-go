// Package store implements the actual data a single node holds: an
// in-memory key-value map, with a simple version counter per key.
//
// Note on Version: this is a placeholder, not real conflict detection.
// Each node increments its own local counter independently when a key is
// written. If two different nodes both receive a concurrent write for the
// same key, they can each produce their own "version 2" with no way to tell
// whose is actually newer, or whether they conflict. That gap is exactly
// what vector clocks (the next concept) exist to close - this store is
// deliberately left naive for now so the problem is visible once we build
// the coordinator on top of it.
package store

import "sync"

// Value is what's stored for a key: the data itself, plus a version number.
type Value struct {
	Data    string `json:"data"`
	Version uint64 `json:"version"`
}

// Store is a single node's in-memory data. Safe for concurrent use.
type Store struct {
	mu   sync.Mutex
	data map[string]Value
}

func New() *Store {
	return &Store{data: make(map[string]Value)}
}

// Put writes data for a key, incrementing the version relative to whatever
// this node currently has stored (not relative to any other replica).
func (s *Store) Put(key, data string) Value {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing := s.data[key]
	v := Value{Data: data, Version: existing.Version + 1}
	s.data[key] = v
	return v
}

// Get returns the value for a key, and whether it was found at all.
func (s *Store) Get(key string) (Value, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	v, ok := s.data[key]
	return v, ok
}
