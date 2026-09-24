package ring

import (
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
)

type Ring struct {
	mu           sync.RWMutex
	virtualNodes int
	hashes       []uint32
	hashToNode   map[uint32]string
	nodes        map[string]bool
}

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

func (r *Ring) AddNode(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.nodes[node] {
		return
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

func (r *Ring) GetNode(key string) (string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.hashes) == 0 {
		return "", fmt.Errorf("ring is empty: no nodes have been added")
	}

	h := hashKey(key)

	idx := sort.Search(len(r.hashes), func(i int) bool {
		return r.hashes[i] >= h
	})

	if idx == len(r.hashes) {
		idx = 0
	}

	return r.hashToNode[r.hashes[idx]], nil
}

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
			continue
		}
		seen[node] = true
		result = append(result, node)
	}

	if len(result) < n {
		return result, fmt.Errorf("only %d distinct physical nodes available, requested %d", len(result), n)
	}

	return result, nil
}

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
