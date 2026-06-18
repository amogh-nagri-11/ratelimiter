package limiter 

// Limiter is the interface every rate-limiting algorithm will satisfy.
// Allow reports whether a request identified by `key` may proceed right now.
// `key` is whatever you're limiting on — an IP, a user ID, an API key.
type Limiter interface {
	Allow (key string) bool 
}