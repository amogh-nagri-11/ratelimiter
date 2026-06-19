// Command server runs the rate-limiter HTTP service.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/amogh-nagri-11/ratelimiter/internal/limiter"
	"github.com/amogh-nagri-11/ratelimiter/internal/metrics"
	"github.com/amogh-nagri-11/ratelimiter/internal/middleware"
)

func main() {
	// Configuration via flags. Defaults give a sensible "5 requests per second,
	// token bucket" setup; override with e.g. -algo slidingwindow -limit 100 -window 1m.
	addr := flag.String("addr", ":8080", "address to listen on")
	backend := flag.String("backend", "memory",
		"limiter backend: memory (single process) or redis (shared/distributed)")
	algo := flag.String("algo", limiter.AlgoTokenBucket,
		"rate-limit algorithm: tokenbucket or slidingwindow")
	limit := flag.Int("limit", 5, "max requests allowed per window (per client IP)")
	window := flag.Duration("window", time.Second, "the rate-limit window")
	ttl := flag.Duration("ttl", 10*time.Minute,
		"[memory] evict a client's limiter after it has been idle this long")
	cleanup := flag.Duration("cleanup-interval", time.Minute,
		"[memory] how often the janitor sweeps for idle limiters")
	redisAddrs := flag.String("redis", "localhost:6379",
		"[redis] comma-separated Redis node addresses to shard across")
	replicas := flag.Int("replicas", 100,
		"[redis] virtual nodes per Redis node on the consistent-hash ring")
	failOpen := flag.Bool("fail-open", true,
		"[redis] if Redis is unreachable, allow requests (true) or deny them (false)")
	flag.Parse()

	// Build the limiter behind the SAME Limiter interface, so the middleware below
	// doesn't change at all whether we're in-process or sharded across Redis nodes.
	// That interface, designed back in Phase 0, is what makes this swap a one-liner.
	var lim limiter.Limiter
	switch *backend {
	case "memory":
		factory, err := limiter.NewFactory(*algo, *limit, *window)
		if err != nil {
			log.Fatalf("invalid config: %v", err)
		}
		reg := limiter.NewRegistry(factory)
		reg.StartJanitor(*ttl, *cleanup) // background memory-reclaim sweep
		lim = reg
	case "redis":
		dl, err := limiter.NewDistributedLimiter(
			strings.Split(*redisAddrs, ","), *replicas, *algo, *limit, *window, *failOpen)
		if err != nil {
			log.Fatalf("redis backend: %v", err)
		}
		defer dl.Close()
		lim = dl
	default:
		log.Fatalf("unknown backend %q (want memory or redis)", *backend)
	}

	// Wrap the limiter in Prometheus instrumentation. Still a Limiter, so the
	// middleware below is none the wiser.
	lim = metrics.Instrument(lim, *algo)

	mux := http.NewServeMux()

	// /health and /metrics are registered directly on the mux, so they are NOT
	// rate-limited — health probes and metric scrapes must never get a 429.
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})
	mux.Handle("/metrics", promhttp.Handler())

	// Only the application route ("/") goes through the rate-limit middleware.
	app := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "request allowed")
	})
	mux.Handle("/", middleware.RateLimit(lim)(app))

	log.Printf("server listening on %s (backend=%s, algo=%s, limit=%d/%s)",
		*addr, *backend, *algo, *limit, *window)

	if err := http.ListenAndServe(*addr, mux); err != nil {
		log.Fatal(err)
	}
}
