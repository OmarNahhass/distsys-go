// Package ring implements consistent hashing with virtual nodes.
//
// The problem this solves: given a set of nodes that can join or leave at
// any time, and a set of keys, we need a deterministic function
// "which node owns this key?" that (a) any node can compute independently,
// with no coordination, and (b) doesn't reshuffle most of the keyspace
// every time membership changes.
//
// The approach: hash both nodes and keys onto the same circular space
// (0 to 2^32-1). A key belongs to whichever node's hash is the first one
// found walking clockwise from the key's hash. Each physical node is
// actually placed at many points on the ring ("virtual nodes") so that
// ownership - and the migration load when membership changes - is spread
// evenly instead of landing unevenly by hash-collision luck.
package ring

import (
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
)

// Ring is a consistent hash ring. Safe for concurrent use.
type Ring struct {
	mu           sync.RWMutex
	virtualNodes int               // how many points on the ring each physical node gets
	hashes       []uint32          // sorted ring positions
	hashToNode   map[uint32]string // ring position -> owning physical node
	nodes        map[string]bool   // set of physical nodes currently in the ring
}

// New creates a ring. virtualNodes controls how many points each physical
// node occupies - higher spreads load more evenly but costs a bit more
// memory/lookup time. 100-200 is a reasonable range in real systems.
func New(virtualNodes int) *Ring {
	return &Ring{
		virtualNodes: virtualNodes,
		hashToNode:   make(map[uint32]string),
		nodes:        make(map[string]bool),
	}
}

func hashKey(s string) uint32 {
	return crc32.ChecksumIEEE([]byte(s))
}

// AddNode places a physical node's virtual points onto the ring.
func (r *Ring) AddNode(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.nodes[node] {
		return // already present, nothing to do
	}
	r.nodes[node] = true

	for i := 0; i < r.virtualNodes; i++ {
		vKey := fmt.Sprintf("%s#%d", node, i)
		h := hashKey(vKey)
		r.hashToNode[h] = node
		r.hashes = append(r.hashes, h)
	}

	sort.Slice(r.hashes, func(i, j int) bool { return r.hashes[i] < r.hashes[j] })
}

// RemoveNode takes a physical node, and all its virtual points, off the ring.
func (r *Ring) RemoveNode(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.nodes[node] {
		return
	}
	delete(r.nodes, node)

	kept := r.hashes[:0]
	for _, h := range r.hashes {
		if r.hashToNode[h] == node {
			delete(r.hashToNode, h)
			continue
		}
		kept = append(kept, h)
	}
	r.hashes = kept
}

// GetNode returns the physical node that owns the given key: the node whose
// ring position is the first one found walking clockwise from the key's hash.
func (r *Ring) GetNode(key string) (string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.hashes) == 0 {
		return "", fmt.Errorf("ring is empty: no nodes have been added")
	}

	h := hashKey(key)

	// sort.Search finds the first index where hashes[i] >= h - i.e. the
	// first ring position at or clockwise of the key's own position.
	idx := sort.Search(len(r.hashes), func(i int) bool {
		return r.hashes[i] >= h
	})

	// If we walked off the end of the slice, wrap around to the first
	// position on the ring - this is the "clock wraps past midnight" case.
	if idx == len(r.hashes) {
		idx = 0
	}

	return r.hashToNode[r.hashes[idx]], nil
}

// GetNodes returns up to n distinct physical nodes responsible for replicating
// a key: start at the key's clockwise-nearest node (same rule as GetNode),
// then keep walking clockwise, collecting further *distinct* physical nodes,
// until n have been found or the whole ring has been walked.
//
// This is what makes replication possible: instead of one node owning a key
// outright, we get an ordered list of N nodes that should each hold a copy.
func (r *Ring) GetNodes(key string, n int) ([]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.hashes) == 0 {
		return nil, fmt.Errorf("ring is empty: no nodes have been added")
	}

	h := hashKey(key)
	idx := sort.Search(len(r.hashes), func(i int) bool {
		return r.hashes[i] >= h
	})
	if idx == len(r.hashes) {
		idx = 0
	}

	seen := make(map[string]bool)
	result := make([]string, 0, n)

	for i := 0; i < len(r.hashes) && len(result) < n; i++ {
		pos := (idx + i) % len(r.hashes)
		node := r.hashToNode[r.hashes[pos]]
		if seen[node] {
			continue // same physical node, different virtual point - skip
		}
		seen[node] = true
		result = append(result, node)
	}

	if len(result) < n {
		return result, fmt.Errorf("only %d distinct physical nodes available, requested %d", len(result), n)
	}

	return result, nil
}

// Nodes returns the current set of physical nodes in the ring.

func (r *Ring) Nodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]string, 0, len(r.nodes))
	for n := range r.nodes {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}
