# ratelimiter

A distributed rate limiter in Go, built phase by phase to learn system design.
It starts as a single in-memory token bucket and grows into a horizontally
sharded, Redis-backed limiter with consistent hashing, Prometheus metrics, and a
load-testing harness.

The guiding idea throughout: **one small interface makes every step a swap.**

```go
type Limiter interface {
    Allow(key string) bool
}
```

In-memory registry, single Redis, or a ring of Redis nodes — they all implement
this, so the HTTP middleware never changes.

## Architecture

```
                 ┌──────────────────────────────────────────────┐
   HTTP request  │  middleware.RateLimit (keys on client IP)     │
   ─────────────▶│      │                                        │
                 │      ▼                                         │
                 │  metrics.Instrument  (counts + latency)        │
                 │      │                                         │
                 │      ▼                                         │
                 │   limiter.Limiter  ◀── one of three backends:  │
                 └──────┬─────────────────────────────────────────┘
                        │
        ┌───────────────┼─────────────────────────────┐
        ▼               ▼                             ▼
  Registry         RedisLimiter                DistributedLimiter
 (in-memory,      (single Redis,             (consistent-hash ring
  per-process,     atomic Lua,                over N RedisLimiters;
  janitor TTL)     TTL eviction)              each key -> one node)
```

### Backends, and what limitation each one removes

| Backend | State lives in | Removes the limitation of… | New cost |
|---|---|---|---|
| **memory** (`Registry`) | the process | — (the starting point) | not shared across servers |
| **single Redis** (`RedisLimiter`) | one Redis | memory: now **shared** across all app servers | a network hop per request; Redis is a bottleneck + SPOF |
| **sharded ring** (`DistributedLimiter`) | N Redis nodes | single Redis: scales out, survives one node dying | slightly less exact during ring membership changes |

```
in-memory      →   single Redis      →   sharded ring
(not shared)       (SHARED, 1 node)      (shared, N nodes via the RING)
```

## Algorithms

Both are available behind `-algo` and exist in two implementations (in-process
and in Redis):

- **Token bucket** — capacity `N`, refilling `N/window` tokens per second. Allows
  bursts up to `N`, then a steady rate. Lazy refill (computed on read), so no
  background timer.
- **Sliding window log** — exact; stores a timestamp per request and counts those
  within the trailing window. More memory, but no boundary bursts.

Config is unified as **"N requests per window"**; the factory maps that onto each
algorithm's native parameters.

## Design decisions worth calling out

- **Per-key limiter via a registry.** One algorithm object holds one key's state.
  The `Registry` maps `key → limiter`, creating lazily. The check-then-act race
  (two goroutines creating the same key) is closed with **double-checked locking**
  under a `sync.RWMutex` — read-heavy traffic shares the read lock; only new-key
  creation takes the write lock.
- **Memory leak control (memory backend).** A background **janitor** evicts keys
  idle past a TTL. Last-seen time is an `atomic.Int64` per entry, so the request
  path still updates it under the *read* lock (the mutex guards the map structure;
  the atomic guards the timestamp).
- **Atomicity in Redis via Lua.** A token bucket is a read-modify-write; with
  separate `GET`/`SET` from multiple instances it loses updates and over-admits.
  The whole refill-check-decrement runs as one **Lua script**, which Redis
  executes indivisibly. Lua (not `MULTI/EXEC`) because we must *branch on a value
  read mid-operation*. Time comes from `redis.call('TIME')` so all instances share
  one clock. `EXPIRE` replaces the janitor — idle keys expire themselves.
- **Consistent hashing.** Keys map onto a hash circle; each key goes to the first
  node clockwise. **Virtual nodes** keep the load balanced. Removing 1 of N nodes
  remaps only ~1/N of keys (vs `hash % N`, which remaps nearly all).
- **Partition policy (CAP, made concrete).** If Redis is unreachable, `-fail-open`
  decides: **allow** (default — don't let the limiter take down your service) or
  **deny** (only when over-admitting is worse than an outage).
- **Metrics as a decorator.** `metrics.Instrument` wraps *any* `Limiter`, so
  counters and latency histograms work identically across backends. `/metrics`
  and `/health` are exempt from rate limiting.

## Running

```bash
# In-memory, 5 requests/sec per IP
go run ./cmd/server -backend memory -algo tokenbucket -limit 5 -window 1s

# Single shared Redis
go run ./cmd/server -backend redis -redis 127.0.0.1:6379 -limit 100 -window 1s

# Sharded across multiple Redis nodes (consistent-hash ring)
go run ./cmd/server -backend redis -redis 127.0.0.1:6379,127.0.0.1:6380 -limit 100 -window 1s
```

Key flags: `-backend memory|redis`, `-algo tokenbucket|slidingwindow`, `-limit`,
`-window`, `-redis` (comma-separated nodes), `-replicas` (virtual nodes),
`-fail-open`, `-ttl` / `-cleanup-interval` (memory janitor).

Endpoints: `/` (rate-limited), `/health`, `/metrics`.

## Tests

```bash
docker run -d --name rl-redis -p 6379:6379 redis:7-alpine   # for integration tests
REDIS_ADDR=127.0.0.1:6379 go test -race ./...
```

Redis integration tests **auto-skip** if no Redis is reachable, so the suite stays
green without it. Highlights: a 500-goroutine concurrency test proves the Lua
script admits *exactly* the limit; the hashring tests prove balanced distribution
and minimal key movement on node removal.

## Load testing & benchmarks

A self-contained Go load generator (`cmd/loadtest`) — no external tooling needed:

```bash
go run ./cmd/loadtest -url http://127.0.0.1:8080/ -duration 8s -concurrency 50
```

Measured locally (Apple Silicon, Redis 7 in Docker, 50 workers, 8s, limit set
high so requests pass and we measure the limiter path itself):

| Backend | Throughput | p50 | p95 | p99 | max |
|---|---:|---:|---:|---:|---:|
| **memory** | ~169,000 req/s | 228µs | 646µs | 1.0ms | 7.8ms |
| **single Redis** | ~42,000 req/s | 1.14ms | 1.83ms | 2.24ms | 15.3ms |

**Reading this:** Redis costs ~4× throughput and ~1ms of added p50 latency — the
price of one network round trip per decision. That is exactly the
exactness/sharing-vs-latency tradeoff, made measurable. You pay it to share state
across servers; the ring then buys back throughput by adding nodes.

## Chaos: graceful degradation

With the Redis backend and `-fail-open=true` (default), kill Redis mid-traffic and
the service keeps serving instead of erroring:

```bash
docker stop rl-redis      # simulate a Redis outage
# requests keep returning 200 (fail-open): availability over exact limiting
docker start rl-redis     # limiting resumes automatically
```

With `-fail-open=false` the same outage returns denials instead — availability vs.
consistency, chosen by one flag.

## Layout

```
cmd/server/      HTTP server + flag wiring
cmd/loadtest/    load generator
internal/limiter/    Limiter interface, token bucket, sliding window,
                     Registry (+ janitor), factory, RedisLimiter, DistributedLimiter
internal/hashring/   consistent-hash ring
internal/middleware/ IP-keyed rate-limit middleware
internal/metrics/    Prometheus decorator
```
