package limiter

import (
	"testing"
	"time"
)

// TestEvictStaleRemovesIdleKeepsActive: backdate one key's lastSeen so it looks
// idle, leave another fresh, and confirm the sweep drops only the idle one.
func TestEvictStaleRemovesIdleKeepsActive(t *testing.T) {
	reg := NewRegistry(func() KeyLimiter { return NewTokenBucket(1, 0) })
	reg.Allow("idle")
	reg.Allow("active")

	// Make "idle" look like it was last used an hour ago.
	reg.entries["idle"].lastSeen.Store(time.Now().Add(-time.Hour).UnixNano())

	removed := reg.evictStale(time.Now(), time.Minute)
	if removed != 1 {
		t.Fatalf("expected 1 eviction, got %d", removed)
	}
	if reg.Len() != 1 {
		t.Fatalf("expected 1 entry left, got %d", reg.Len())
	}
	if _, ok := reg.entries["active"]; !ok {
		t.Fatal("active key should have survived the sweep")
	}
}

// TestAllowRefreshesLastSeen: using a key updates its timestamp, so a key used
// just now is never evicted even with a tiny TTL.
func TestAllowRefreshesLastSeen(t *testing.T) {
	reg := NewRegistry(func() KeyLimiter { return NewTokenBucket(10, 0) })
	reg.Allow("k")
	reg.entries["k"].lastSeen.Store(time.Now().Add(-time.Hour).UnixNano()) // pretend old

	reg.Allow("k") // touch it again -> lastSeen back to ~now

	if removed := reg.evictStale(time.Now(), time.Minute); removed != 0 {
		t.Fatalf("recently-used key should survive, but %d were evicted", removed)
	}
}

// TestJanitorEvicts is an end-to-end check of the background goroutine: with a
// tiny TTL and interval, an untouched key should disappear on its own. We poll
// (rather than sleep a fixed amount) to keep it from being flaky.
func TestJanitorEvicts(t *testing.T) {
	reg := NewRegistry(func() KeyLimiter { return NewTokenBucket(1, 0) })
	reg.StartJanitor(1*time.Millisecond, 1*time.Millisecond)
	defer reg.Stop()

	reg.Allow("ephemeral")

	deadline := time.Now().Add(time.Second)
	for reg.Len() > 0 {
		if time.Now().After(deadline) {
			t.Fatal("janitor did not evict the idle key within 1s")
		}
		time.Sleep(2 * time.Millisecond)
	}
}
