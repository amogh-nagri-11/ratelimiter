package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/amogh-nagri-11/ratelimiter/internal/limiter"
)

// okHandler is the "next" handler we protect: it just writes 200.
var okHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
})

// newRequest builds a request whose RemoteAddr is the given "ip:port", so the
// middleware's clientIP() keys on `ip`.
func newRequest(remoteAddr string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.RemoteAddr = remoteAddr
	return r
}

// TestMiddlewareAllowsThenDenies uses a capacity-1, no-refill bucket so the
// first request from an IP passes (200) and the second is rate-limited (429
// with a Retry-After header).
func TestMiddlewareAllowsThenDenies(t *testing.T) {
	reg := limiter.NewRegistry(func() limiter.KeyLimiter { return limiter.NewTokenBucket(1, 0) })
	handler := RateLimit(reg)(okHandler)

	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, newRequest("1.2.3.4:5555"))
	if rec1.Code != http.StatusOK {
		t.Fatalf("first request: want 200, got %d", rec1.Code)
	}

	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, newRequest("1.2.3.4:6666")) // same IP, different port
	if rec2.Code != http.StatusTooManyRequests {
		t.Fatalf("second request: want 429, got %d", rec2.Code)
	}
	if rec2.Header().Get("Retry-After") == "" {
		t.Fatal("429 response should carry a Retry-After header")
	}
}

// TestMiddlewareKeysByIP confirms one IP being limited doesn't affect another.
func TestMiddlewareKeysByIP(t *testing.T) {
	reg := limiter.NewRegistry(func() limiter.KeyLimiter { return limiter.NewTokenBucket(1, 0) })
	handler := RateLimit(reg)(okHandler)

	handler.ServeHTTP(httptest.NewRecorder(), newRequest("1.1.1.1:1000")) // exhaust 1.1.1.1

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, newRequest("2.2.2.2:1000"))
	if rec.Code != http.StatusOK {
		t.Fatalf("different IP should be allowed: want 200, got %d", rec.Code)
	}
}
