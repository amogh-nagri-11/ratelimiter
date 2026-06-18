package limiter

import (
	"sync"
	"time"
)

// SlidingWindowLog is a thread-safe rate limiter that allows at most
// `limit` requests within any trailing `window` duration. It is exact:
// it stores a timestamp per request, so memory grows with request volume.
type SlidingWindowLog struct {
	mu       sync.Mutex
	limit    int           // max requests allowed within the window
	window   time.Duration // length of the trailing window
	requests []time.Time   // timestamps of allowed requests, oldest first
}

// NewSlidingWindowLog creates a limiter allowing `limit` requests per `window`.
func NewSlidingWindowLog(limit int, window time.Duration) *SlidingWindowLog {
	return &SlidingWindowLog{
		limit:    limit,
		window:   window,
		requests: make([]time.Time, 0, limit), // preallocate capacity = limit
	}
}

// Allow reports whether a request may proceed, recording it if so.
func (sw *SlidingWindowLog) Allow() bool {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	now := time.Now()
	cutoff := now.Add(-sw.window) // anything before this is too old

	// Evict expired timestamps. Since requests is oldest-first, we find
	// the first index that's still within the window and slice from there.
	i := 0
	for i < len(sw.requests) && sw.requests[i].Before(cutoff) {
		i++
	}
	sw.requests = sw.requests[i:] // drop the expired prefix

	if len(sw.requests) >= sw.limit {
		return false // window is full
	}

	sw.requests = append(sw.requests, now) // record this request
	return true
}