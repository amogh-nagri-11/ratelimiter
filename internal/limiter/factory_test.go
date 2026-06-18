package limiter

import (
	"testing"
	"time"
)

// TestNewFactoryUnknownAlgo: a bad algorithm name is a config error, not a panic.
func TestNewFactoryUnknownAlgo(t *testing.T) {
	if _, err := NewFactory("leakybucket", 5, time.Second); err == nil {
		t.Fatal("expected error for unknown algorithm, got nil")
	}
}

// TestNewFactoryValidatesParams: non-positive limit/window are rejected.
func TestNewFactoryValidatesParams(t *testing.T) {
	if _, err := NewFactory(AlgoTokenBucket, 0, time.Second); err == nil {
		t.Fatal("expected error for limit=0")
	}
	if _, err := NewFactory(AlgoTokenBucket, 5, 0); err == nil {
		t.Fatal("expected error for window=0")
	}
}

// TestFactoryBuildsWorkingLimiters: for each algorithm, "limit N per window"
// should allow exactly N immediate requests then deny the next. We use a long
// window so no refill/expiry happens during the test.
func TestFactoryBuildsWorkingLimiters(t *testing.T) {
	for _, algo := range []string{AlgoTokenBucket, AlgoSlidingWindow} {
		t.Run(algo, func(t *testing.T) {
			factory, err := NewFactory(algo, 3, time.Hour)
			if err != nil {
				t.Fatalf("NewFactory: %v", err)
			}
			l := factory()
			for i := 0; i < 3; i++ {
				if !l.Allow() {
					t.Fatalf("request %d should be allowed (limit 3)", i+1)
				}
			}
			if l.Allow() {
				t.Fatal("4th request should be denied (over limit 3)")
			}
		})
	}
}
