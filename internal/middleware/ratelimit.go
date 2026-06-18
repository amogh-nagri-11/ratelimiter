// Package middleware provides HTTP middleware that wraps handlers with
// cross-cutting behavior — here, rate limiting.
package middleware

import (
	"net"
	"net/http"

	"github.com/amogh-nagri-11/ratelimiter/internal/limiter"
)

// RateLimit returns middleware that rejects requests when the caller (keyed by
// client IP) has exceeded its limit.
//
// The shape is the standard Go middleware pattern: a function that takes the
// "next" handler and returns a new handler wrapping it. Returning a closure over
// `l` lets us configure the limiter once and reuse the middleware everywhere.
func RateLimit(l limiter.Limiter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := clientIP(r)

			if !l.Allow(key) {
				// Retry-After tells the client how long to wait before retrying.
				// We send a static "1" (second) here: the current Limiter
				// interface returns only a bool, so it can't tell us *when* a
				// token will be available. A later phase could widen the
				// interface to return a wait duration and make this exact.
				w.Header().Set("Retry-After", "1")
				http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// clientIP extracts the caller's IP from the request.
//
// r.RemoteAddr is "host:port"; we strip the port. We deliberately do NOT trust
// X-Forwarded-For / X-Real-IP here: those headers are client-controlled and
// trivially spoofable, so honoring them without a trusted proxy in front would
// let anyone forge a key and dodge the limit. Behind a real proxy you'd parse
// XFF, but only after validating the proxy.
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		// No port (some test/transport cases) — use RemoteAddr as-is.
		return r.RemoteAddr
	}
	return host
}
