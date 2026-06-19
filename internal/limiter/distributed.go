package limiter

import (
	"fmt"
	"time"

	"github.com/amogh-nagri-11/ratelimiter/internal/hashring"
)

// DistributedLimiter shards rate-limit state across several Redis nodes using a
// consistent-hash ring. Each key deterministically maps to exactly ONE node, so
// that key's counter lives in one place and stays atomic (the per-node Lua
// script still guarantees no race). This is how we scale the Redis layer
// horizontally instead of funneling every request through a single Redis.
//
// TRADEOFFS vs a single Redis:
//   - Single Redis: simplest, strongly consistent and exact, but it's a
//     throughput bottleneck and a single point of failure.
//   - Sharded ring: scales throughput with node count, and consistent hashing
//     means adding/removing a node only remaps ~1/N of keys. BUT if a node dies,
//     that shard's counters are gone — those keys effectively get a fresh limit
//     (we lean on the fail-open policy). And right after a node change, a key
//     that moved to a different node briefly has no history there, so a client
//     could momentarily exceed its limit. We trade a little exactness for
//     availability and scale.
type DistributedLimiter struct {
	ring     *hashring.Ring
	nodes    map[string]*RedisLimiter
	failOpen bool
}

// NewDistributedLimiter builds one RedisLimiter per address and a ring over
// those addresses. `replicas` is the virtual-nodes-per-node count for the ring.
func NewDistributedLimiter(addrs []string, replicas int, algo string, limit int, window time.Duration, failOpen bool) (*DistributedLimiter, error) {
	if len(addrs) == 0 {
		return nil, fmt.Errorf("need at least one redis address")
	}
	nodes := make(map[string]*RedisLimiter, len(addrs))
	for _, addr := range addrs {
		rl, err := NewRedisLimiter(addr, algo, limit, window, failOpen)
		if err != nil {
			// Roll back the clients we already opened.
			for _, opened := range nodes {
				opened.Close()
			}
			return nil, err
		}
		nodes[addr] = rl
	}
	return &DistributedLimiter{
		ring:     hashring.New(replicas, addrs...),
		nodes:    nodes,
		failOpen: failOpen,
	}, nil
}

// Allow routes the key to its owning node via the ring, then delegates.
func (d *DistributedLimiter) Allow(key string) bool {
	addr, ok := d.ring.Get(key)
	if !ok {
		return d.failOpen
	}
	return d.nodes[addr].Allow(key)
}

// Close shuts down every node's Redis connection.
func (d *DistributedLimiter) Close() error {
	for _, n := range d.nodes {
		n.Close()
	}
	return nil
}
