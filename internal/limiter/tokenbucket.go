package limiter

import (
	"sync"
	"time"
)

// TokenBucket is a thread-safe token-bucket rate limiter for a single key.
// Tokens refill lazily based on elapsed time, so no background goroutine
// is needed.
type TokenBucket struct {
	mu         sync.Mutex // guards everything below
	capacity   float64    // max tokens the bucket can hold
	tokens     float64    // current token count (float for fractional refills)
	refillRate float64    // tokens added per second
	lastRefill time.Time  // when we last computed a refill
}

// NewTokenBucket creates a bucket that holds up to `capacity` tokens and
// refills at `refillRate` tokens per second. It starts full.
func NewTokenBucket(capacity, refillRate float64) *TokenBucket {
	return &TokenBucket{
		capacity:   capacity,
		tokens:     capacity, // start full so the first burst is allowed
		refillRate: refillRate,
		lastRefill: time.Now(),
	}
}

// refill adds tokens accrued since lastRefill. Caller MUST hold the lock.
func (tb *TokenBucket) refill() {
	now := time.Now()
	elapsed := now.Sub(tb.lastRefill).Seconds() // time since last refill, in seconds
	tb.tokens += elapsed * tb.refillRate         // add accrued tokens
	if tb.tokens > tb.capacity {
		tb.tokens = tb.capacity // never exceed capacity
	}
	tb.lastRefill = now
}

// Allow reports whether one request may proceed, consuming a token if so.
func (tb *TokenBucket) Allow() bool {
	tb.mu.Lock()
	defer tb.mu.Unlock() // unlock automatically when Allow returns

	tb.refill()

	if tb.tokens >= 1 {
		tb.tokens-- // spend a token
		return true
	}
	return false
}