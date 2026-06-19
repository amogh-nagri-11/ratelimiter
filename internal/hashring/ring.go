// Package hashring implements consistent hashing: a way to map keys onto a set
// of nodes such that adding or removing a node only moves a small fraction of
// keys (roughly 1/N), instead of remapping nearly everything the way a plain
// `hash(key) % N` would.
package hashring

import (
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

// Ring maps keys to nodes by placing both on a circle of 32-bit hash values.
// A key belongs to the first node found walking clockwise from the key's hash.
//
// Each physical node is placed at `replicas` points on the circle ("virtual
// nodes"). Without virtual nodes, a handful of physical nodes would land at
// uneven spots and some would own a far bigger arc than others; spreading each
// node across many points smooths the load distribution.
type Ring struct {
	mu       sync.RWMutex
	replicas int
	hashes   []uint32          // all virtual-node positions, kept sorted for binary search
	ring     map[uint32]string // position -> physical node id
	nodes    map[string]struct{}
}

// New builds a ring with `replicas` virtual nodes per physical node.
func New(replicas int, nodes ...string) *Ring {
	r := &Ring{
		replicas: replicas,
		ring:     make(map[uint32]string),
		nodes:    make(map[string]struct{}),
	}
	for _, n := range nodes {
		r.Add(n)
	}
	return r
}

func hashKey(s string) uint32 { return crc32.ChecksumIEEE([]byte(s)) }

// Add inserts a node and its virtual nodes onto the ring. Idempotent.
func (r *Ring) Add(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.nodes[node]; ok {
		return
	}
	r.nodes[node] = struct{}{}
	for i := 0; i < r.replicas; i++ {
		h := hashKey(node + "#" + strconv.Itoa(i))
		r.ring[h] = node
		r.hashes = append(r.hashes, h)
	}
	sort.Slice(r.hashes, func(i, j int) bool { return r.hashes[i] < r.hashes[j] })
}

// Remove deletes a node and its virtual nodes, then rebuilds the sorted index.
func (r *Ring) Remove(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.nodes[node]; !ok {
		return
	}
	delete(r.nodes, node)
	for i := 0; i < r.replicas; i++ {
		delete(r.ring, hashKey(node+"#"+strconv.Itoa(i)))
	}
	r.hashes = r.hashes[:0]
	for h := range r.ring {
		r.hashes = append(r.hashes, h)
	}
	sort.Slice(r.hashes, func(i, j int) bool { return r.hashes[i] < r.hashes[j] })
}

// Get returns the node that owns `key`. The bool is false only when the ring is
// empty. We binary-search for the first position >= hash(key), wrapping around
// to the start of the circle if the key hashes past the last position.
func (r *Ring) Get(key string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.hashes) == 0 {
		return "", false
	}
	h := hashKey(key)
	idx := sort.Search(len(r.hashes), func(i int) bool { return r.hashes[i] >= h })
	if idx == len(r.hashes) {
		idx = 0 // wrapped past the end of the circle
	}
	return r.ring[r.hashes[idx]], true
}
