# CLAUDE.md

Guidance for Claude Code working in this repository.

## What this is

A distributed rate limiter in Go (`github.com/amogh-nagri-11/ratelimiter`), built
phase by phase as a system-design learning project. It progresses from an
in-memory token bucket to a Redis-backed, consistent-hash-sharded limiter with
Prometheus metrics and a load-testing harness.

## How the owner wants to work (important)

The owner is learning Go and distributed systems and **learns by understanding
code, not by having it written for them.** When extending this project:

- **Explain before and after writing.** State the plan, then give code *with the
  reasoning* behind design/algorithm choices — especially concurrency and
  distributed-systems tradeoffs.
- **One phase / unit of work at a time.** Don't bundle several big changes; pause
  so they can read, run tests, commit, and push before continuing.
- **Don't write core algorithm logic silently.** The "why" matters more than the
  "what." Treat it as pair-programming/tutoring, not autonomous building.

## Commands

```bash
# Build / vet everything
go build ./... && go vet ./...

# Tests (Redis integration tests auto-skip if no Redis is reachable)
docker run -d --name rl-redis -p 6379:6379 redis:7-alpine
REDIS_ADDR=127.0.0.1:6379 go test -race ./...

# Run the server
go run ./cmd/server -backend memory -algo tokenbucket -limit 5 -window 1s
go run ./cmd/server -backend redis  -redis 127.0.0.1:6379 -limit 100 -window 1s
go run ./cmd/server -backend redis  -redis 127.0.0.1:6379,127.0.0.1:6380   # ring

# Load test
go run ./cmd/loadtest -url http://127.0.0.1:8080/ -duration 8s -concurrency 50
```

Endpoints: `/` (rate-limited), `/health` and `/metrics` (NOT rate-limited).

## Layout

```
cmd/server/          HTTP server + flag wiring (backend selection lives here)
cmd/loadtest/        self-contained Go load generator
internal/limiter/    Limiter interface, tokenbucket, slidingwindow,
                     registry (+ janitor), factory, redis, distributed
internal/hashring/   consistent-hash ring (pure, no Redis dependency)
internal/middleware/ IP-keyed rate-limit middleware
internal/metrics/    Prometheus instrumentation (Limiter decorator)
```

## Core conventions

- **Everything routes through one interface:** `limiter.Limiter` is
  `Allow(key string) bool`. Memory `Registry`, `RedisLimiter`, and
  `DistributedLimiter` all implement it, so swapping backends does not touch the
  middleware. Preserve this — it's the project's central abstraction.
- **Two synchronization layers in the memory backend:** the `RWMutex` guards the
  map *structure*; a per-entry `atomic.Int64` guards the last-seen *timestamp*.
  This keeps the request hot path on the shared read lock.
- **Atomicity in Redis is non-negotiable.** Algorithm steps run as a single Lua
  script (`internal/limiter/redis.go`) so read-modify-write can't interleave
  across instances. Use `redis.call('TIME')` for a shared clock; use `EXPIRE` for
  cleanup. Don't replace Lua with separate GET/SET commands.
- **Config model:** always "N requests per `window`"; the factory maps it onto
  each algorithm's native params.
- **Tests:** Redis tests skip gracefully without Redis (see `redisAddr` helper in
  `redis_test.go`). Keep them deterministic where possible (e.g. `evictStale`
  takes `now` as an argument instead of calling `time.Now`).
- Run `-race` on anything touching concurrency.

## Phase history (for context)

0: scaffold + interface · 1: token bucket · 2: sliding window · 3: registry +
middleware · 4: config + janitor · 5: Redis (atomic Lua) + consistent-hash ring ·
6: metrics + load test + README. See `decisions.md` for the *why* behind each.
