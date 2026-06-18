package limiter

import (
	"sync"
	"sync/atomic"
	"time"
)

// KeyLimiter is the per-key limiter contract: the single-key Allow() bool
// that TokenBucket and SlidingWindowLog already implement. The Registry holds
// one of these per key. It's an interface (not a concrete type) so the Registry
// is agnostic about which algorithm it's storing.
type KeyLimiter interface {
	Allow() bool
}

// entry pairs a key's limiter with the last time it was used. lastSeen is an
// atomic so the request path can update it while only holding the registry's
// READ lock: the RWMutex guards the *map structure*, the atomic guards *this
// field*. Without the atomic, two concurrent readers writing lastSeen would be
// a data race even though both hold the (shared) read lock.
type entry struct {
	limiter  KeyLimiter
	lastSeen atomic.Int64 // unix nanoseconds of the most recent access
}

// Registry holds one KeyLimiter per key, creating them on demand. It satisfies
// the Limiter interface (Allow(key string) bool) by routing each key to its own
// limiter. A background janitor evicts entries that have been idle past a TTL so
// the map doesn't grow forever for one-off keys.
type Registry struct {
	mu      sync.RWMutex
	entries map[string]*entry
	factory func() KeyLimiter // builds a fresh limiter for a never-before-seen key
	stop    chan struct{}     // closed by Stop() to shut the janitor down
}

// NewRegistry creates a Registry. `factory` is called once per new key to build
// that key's limiter — e.g. func() KeyLimiter { return NewTokenBucket(5, 1) }.
func NewRegistry(factory func() KeyLimiter) *Registry {
	return &Registry{
		entries: make(map[string]*entry),
		factory: factory,
		stop:    make(chan struct{}),
	}
}

// getOrCreate returns the entry for key, creating and storing one if absent,
// using double-checked locking to close the check-then-act race (see Phase 3).
func (r *Registry) getOrCreate(key string) *entry {
	// Fast path: READ lock. Almost every request is for an existing key, and
	// many readers share the RLock, so this path runs concurrently.
	r.mu.RLock()
	e, ok := r.entries[key]
	r.mu.RUnlock()
	if ok {
		return e
	}

	// Slow path: the key is missing, so we must mutate the map — take the
	// exclusive WRITE lock.
	r.mu.Lock()
	defer r.mu.Unlock()

	// Re-check: another goroutine may have created it between our RUnlock and
	// Lock. Skipping this would overwrite their limiter and lose its state.
	if e, ok := r.entries[key]; ok {
		return e
	}

	e = &entry{limiter: r.factory()}
	r.entries[key] = e
	return e
}

// Allow routes the request for `key` to that key's limiter, and stamps the entry
// as just-used so the janitor won't evict an active key.
func (r *Registry) Allow(key string) bool {
	e := r.getOrCreate(key)
	e.lastSeen.Store(time.Now().UnixNano())
	return e.limiter.Allow()
}

// Len reports how many keys are currently tracked. Useful for tests/metrics.
func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.entries)
}

// evictStale removes every entry not used within `ttl` of `now`, returning the
// count removed. Taking `now` as an argument (instead of calling time.Now)
// makes this deterministic and unit-testable without sleeping.
//
// We hold the exclusive write lock for the whole sweep. While it's held, no
// reader can run, so no lastSeen value can change mid-sweep — that freezes the
// timestamps and closes the "decide-stale then someone-bumps-it" race. The cost
// is that a sweep briefly blocks all traffic; for a very large map you'd instead
// collect stale keys under an RLock, then delete them under the Lock with a
// fresh staleness re-check. At this scale the simple version is the right call.
func (r *Registry) evictStale(now time.Time, ttl time.Duration) int {
	cutoff := now.Add(-ttl).UnixNano()

	r.mu.Lock()
	defer r.mu.Unlock()

	removed := 0
	for key, e := range r.entries { // deleting during range is safe in Go
		if e.lastSeen.Load() < cutoff {
			delete(r.entries, key)
			removed++
		}
	}
	return removed
}

// StartJanitor launches a background goroutine that evicts entries idle longer
// than `ttl`, sweeping every `interval`. Call Stop() to shut it down.
func (r *Registry) StartJanitor(ttl, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				r.evictStale(time.Now(), ttl)
			case <-r.stop:
				return
			}
		}
	}()
}

// Stop halts the janitor goroutine. It must be called at most once.
func (r *Registry) Stop() {
	close(r.stop)
}
