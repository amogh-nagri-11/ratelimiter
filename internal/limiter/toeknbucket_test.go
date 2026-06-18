package limiter

import (
	"testing"
	"time"
)

func TestTokenBucket_AllowsBurstUpToCapacity(t *testing.T) {
	// capacity 5, refill 1/sec. We should get exactly 5 immediate allows.
	tb := NewTokenBucket(5, 1)

	for i := 0; i < 5; i++ {
		if !tb.Allow() {
			t.Fatalf("request %d should have been allowed", i+1)
		}
	}
	// 6th request: bucket empty, should be denied.
	if tb.Allow() {
		t.Fatal("6th request should have been denied (bucket empty)")
	}
}

func TestTokenBucket_RefillsOverTime(t *testing.T) {
	// capacity 1, refill 10/sec. Drain it, wait, expect a refill.
	tb := NewTokenBucket(1, 10)

	if !tb.Allow() {
		t.Fatal("first request should be allowed")
	}
	if tb.Allow() {
		t.Fatal("second immediate request should be denied")
	}

	// Wait 150ms → at 10/sec that's ~1.5 tokens → at least 1 available.
	time.Sleep(150 * time.Millisecond)

	if !tb.Allow() {
		t.Fatal("after refill, request should be allowed")
	}
}