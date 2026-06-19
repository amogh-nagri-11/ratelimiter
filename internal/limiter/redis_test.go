package limiter

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// redisAddr returns the address to test against, or skips the test if no Redis
// is reachable — so `go test` stays green on machines without Redis, while CI /
// local runs with a container exercise the real thing.
func redisAddr(t *testing.T) string {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	c := redis.NewClient(&redis.Options{Addr: addr})
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no Redis at %s (%v) — skipping integration test", addr, err)
	}
	return addr
}

// uniqueKey keeps test runs from colliding with each other's leftover state.
func uniqueKey(prefix string) string {
	return fmt.Sprintf("test:%s:%d", prefix, time.Now().UnixNano())
}

// TestRedisTokenBucketEnforcesLimit: 3 tokens per long window -> 3 allowed, then
// denied. Proves the atomic script enforces the limit across calls.
func TestRedisTokenBucketEnforcesLimit(t *testing.T) {
	addr := redisAddr(t)
	rl, err := NewRedisLimiter(addr, AlgoTokenBucket, 3, time.Hour, true)
	if err != nil {
		t.Fatal(err)
	}
	defer rl.Close()

	key := uniqueKey("tb")
	for i := 0; i < 3; i++ {
		if !rl.Allow(key) {
			t.Fatalf("request %d should be allowed", i+1)
		}
	}
	if rl.Allow(key) {
		t.Fatal("4th request should be denied")
	}
}

// TestRedisSlidingWindowEnforcesLimit: same expectation via the ZSET algorithm.
func TestRedisSlidingWindowEnforcesLimit(t *testing.T) {
	addr := redisAddr(t)
	rl, err := NewRedisLimiter(addr, AlgoSlidingWindow, 3, time.Hour, true)
	if err != nil {
		t.Fatal(err)
	}
	defer rl.Close()

	key := uniqueKey("sw")
	for i := 0; i < 3; i++ {
		if !rl.Allow(key) {
			t.Fatalf("request %d should be allowed", i+1)
		}
	}
	if rl.Allow(key) {
		t.Fatal("4th request should be denied")
	}
}

// TestRedisConcurrentAtomicity is the headline test: fire many requests at the
// same key concurrently (as separate app instances would) and confirm EXACTLY
// `limit` are allowed. A non-atomic read-modify-write would over-admit here.
func TestRedisConcurrentAtomicity(t *testing.T) {
	addr := redisAddr(t)
	const limit = 50
	rl, err := NewRedisLimiter(addr, AlgoTokenBucket, limit, time.Hour, true)
	if err != nil {
		t.Fatal(err)
	}
	defer rl.Close()

	key := uniqueKey("atomic")
	const attempts = 500
	results := make(chan bool, attempts)
	for i := 0; i < attempts; i++ {
		go func() { results <- rl.Allow(key) }()
	}
	allowed := 0
	for i := 0; i < attempts; i++ {
		if <-results {
			allowed++
		}
	}
	if allowed != limit {
		t.Fatalf("expected exactly %d allowed under concurrency, got %d", limit, allowed)
	}
}

// TestRedisFailOpenPolicy: when Redis is unreachable, the configured policy
// decides. We point at a dead port to force the error path without a real Redis.
func TestRedisFailOpenPolicy(t *testing.T) {
	// Can't use the connect-time Ping (it would fail construction), so build the
	// limiter against a live Redis, then swap its client to a dead address.
	addr := redisAddr(t)
	for _, failOpen := range []bool{true, false} {
		rl, err := NewRedisLimiter(addr, AlgoTokenBucket, 5, time.Second, failOpen)
		if err != nil {
			t.Fatal(err)
		}
		rl.client.Close()
		rl.client = redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"}) // nothing listening
		if got := rl.Allow("k"); got != failOpen {
			t.Fatalf("failOpen=%v: expected Allow=%v on Redis error, got %v", failOpen, failOpen, got)
		}
		rl.Close()
	}
}
