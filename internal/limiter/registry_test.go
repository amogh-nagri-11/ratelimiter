package limiter

import (
	"sync"
	"sync/atomic"
	"testing"
)

// TestRegistryReusesLimiterPerKey verifies that repeated calls for the same key
// hit the *same* limiter, so its state persists across calls. We use a bucket of
// capacity 1 and refillRate 0 (never refills): the first Allow spends the only
// token, the second must be denied — which only happens if state was reused.
func TestRegistryReusesLimiterPerKey(t *testing.T) {
	reg := NewRegistry(func() KeyLimiter { return NewTokenBucket(1, 0) })

	if !reg.Allow("ip-a") {
		t.Fatal("first request for ip-a should be allowed")
	}
	if reg.Allow("ip-a") {
		t.Fatal("second request for ip-a should be denied (state not reused?)")
	}
}

// TestRegistryIsolatesKeys verifies each key gets its own independent limiter:
// exhausting one key must not affect another.
func TestRegistryIsolatesKeys(t *testing.T) {
	reg := NewRegistry(func() KeyLimiter { return NewTokenBucket(1, 0) })

	reg.Allow("ip-a") // exhaust ip-a
	if !reg.Allow("ip-b") {
		t.Fatal("ip-b should be allowed; it has its own limiter")
	}
}

// TestRegistryConcurrentSingleKey hammers one new key from many goroutines at
// once. The factory counts how many limiters it builds. Despite the stampede,
// double-checked locking must create exactly one — proving the check-then-act
// race is closed. Run with `go test -race` to also catch any map data race.
func TestRegistryConcurrentSingleKey(t *testing.T) {
	var created int64
	reg := NewRegistry(func() KeyLimiter {
		atomic.AddInt64(&created, 1)
		return NewTokenBucket(1000, 0)
	})

	const goroutines = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			reg.Allow("same-key")
		}()
	}
	wg.Wait()

	if got := atomic.LoadInt64(&created); got != 1 {
		t.Fatalf("expected exactly 1 limiter created, got %d", got)
	}
}
