// Package metrics adds Prometheus instrumentation to any Limiter without the
// rest of the code knowing about it. The trick is a decorator: Instrument wraps
// a Limiter and returns a Limiter, so it slots in anywhere the interface is
// used — memory or Redis backend alike.
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/amogh-nagri-11/ratelimiter/internal/limiter"
)

var (
	// requestsTotal counts decisions, split by algorithm and outcome, so you can
	// graph allow-vs-deny rates per algorithm and compute a rejection ratio.
	requestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ratelimiter_requests_total",
		Help: "Total rate-limit decisions, by algorithm and decision (allow/deny).",
	}, []string{"algo", "decision"})

	// decisionDuration measures how long Allow() takes. For the memory backend
	// this is sub-microsecond; for Redis it's dominated by the network round trip,
	// which is exactly the latency cost of going distributed — worth watching.
	decisionDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ratelimiter_decision_duration_seconds",
		Help:    "Time to make a rate-limit decision, by algorithm.",
		Buckets: prometheus.DefBuckets,
	}, []string{"algo"})
)

type instrumented struct {
	next limiter.Limiter
	algo string
}

// Instrument wraps next so every Allow call is timed and counted under the given
// algorithm label.
func Instrument(next limiter.Limiter, algo string) limiter.Limiter {
	return &instrumented{next: next, algo: algo}
}

func (i *instrumented) Allow(key string) bool {
	start := time.Now()
	allowed := i.next.Allow(key)
	decisionDuration.WithLabelValues(i.algo).Observe(time.Since(start).Seconds())

	decision := "deny"
	if allowed {
		decision = "allow"
	}
	requestsTotal.WithLabelValues(i.algo, decision).Inc()
	return allowed
}
