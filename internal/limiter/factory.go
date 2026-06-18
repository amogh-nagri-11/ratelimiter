package limiter

import (
	"fmt"
	"time"
)

// Algorithm names accepted by NewFactory, exported so main (and tests) can refer
// to them without hardcoding strings.
const (
	AlgoTokenBucket   = "tokenbucket"
	AlgoSlidingWindow = "slidingwindow"
)

// NewFactory builds the per-key limiter constructor for the chosen algorithm,
// expressed in one uniform model: allow `limit` requests per `window`.
//
// That single knob maps onto both algorithms:
//   - sliding window: limit and window are used directly.
//   - token bucket:   capacity = limit, refillRate = limit/window — so a client
//     regains a full `limit` worth of tokens over one window, matching the same
//     long-run rate (the bucket also permits a burst up to `limit`).
//
// Returning a factory (not a limiter) is what lets the Registry create one
// independent limiter per key while staying ignorant of the algorithm.
func NewFactory(algo string, limit int, window time.Duration) (func() KeyLimiter, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("limit must be > 0, got %d", limit)
	}
	if window <= 0 {
		return nil, fmt.Errorf("window must be > 0, got %s", window)
	}

	switch algo {
	case AlgoTokenBucket:
		rate := float64(limit) / window.Seconds()
		return func() KeyLimiter { return NewTokenBucket(float64(limit), rate) }, nil
	case AlgoSlidingWindow:
		return func() KeyLimiter { return NewSlidingWindowLog(limit, window) }, nil
	default:
		return nil, fmt.Errorf("unknown algorithm %q (want %q or %q)",
			algo, AlgoTokenBucket, AlgoSlidingWindow)
	}
}
