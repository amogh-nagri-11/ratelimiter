package limiter

import "sync"

// KeyLimiter is the per-key limiter contract: the single-key Allow() bool
// that TokenBucket and SlidingWindowLog already implement. The Registry holds
// one of these per key. It's an interface (not a concrete type) so the Registry
// is agnostic about which algorithm it's storing.
type KeyLimiter interface {
	Allow() bool
}

// Registry holds one KeyLimiter per key, creating them on demand. It satisfies
// the Limiter interface (Allow(key string) bool) by bridging to the per-key
// limiters: it routes each key to its own limiter's Allow().
type Registry struct {
	mu       sync.RWMutex
	limiters map[string]KeyLimiter
	factory  func() KeyLimiter // builds a fresh limiter for a never-before-seen key
}

// NewRegistry creates a Registry. `factory` is called once per new key to build
// that key's limiter — e.g. func() KeyLimiter { return NewTokenBucket(5, 1) }.
func NewRegistry(factory func() KeyLimiter) *Registry {
	return &Registry{
		limiters: make(map[string]KeyLimiter),
		factory:  factory,
	}
}

// getOrCreate returns the limiter for key, creating and storing one if absent.
//
// This is the heart of Phase 3. The naive version has a check-then-act race:
//
//	l, ok := r.limiters[key]   // CHECK
//	if !ok {
//	    l = r.factory()
//	    r.limiters[key] = l    // ACT
//	}
//
// Two goroutines for the same new key can both pass the CHECK (both see ok ==
// false) before either ACTs. They each build a limiter; the second write clobbers
// the first. Now requests that already consumed tokens from the discarded limiter
// are forgotten — the limit is silently exceeded. (And concurrent read+write on a
// Go map without a lock is an outright data race that can crash the program.)
//
// The fix is double-checked locking:
func (r *Registry) getOrCreate(key string) KeyLimiter {
	// Fast path: take the READ lock. In steady state almost every request is for
	// a key that already exists, and many readers can hold an RLock at once, so
	// this path runs concurrently with no contention.
	r.mu.RLock()
	l, ok := r.limiters[key]
	r.mu.RUnlock()
	if ok {
		return l
	}

	// Slow path: the key is missing, so we need to mutate the map. Take the
	// exclusive WRITE lock — only one goroutine can be here at a time.
	r.mu.Lock()
	defer r.mu.Unlock()

	// Re-check (the "double" in double-checked locking). Between our RUnlock and
	// Lock above, another goroutine may have already created this key. If we
	// skipped this check we'd overwrite its limiter and lose that limiter's state.
	if l, ok := r.limiters[key]; ok {
		return l
	}

	l = r.factory()
	r.limiters[key] = l
	return l
}

// Allow routes the request for `key` to that key's limiter.
//
// Note the lock scope: getOrCreate only locks the *map*. Once we hold the
// *l pointer, we release the registry lock and call l.Allow(), which is guarded
// by the limiter's own internal mutex. So two requests for different keys never
// block each other on the registry lock while their limiters do their work.
func (r *Registry) Allow(key string) bool {
	return r.getOrCreate(key).Allow()
}
