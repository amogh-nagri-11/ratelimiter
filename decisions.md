# Architecture Decisions

A running record of the design choices in this project and the tradeoffs behind
them. Newest context at the bottom of each entry. Format: decision → why →
tradeoff/alternative rejected.

---

## 1. One interface: `Limiter.Allow(key string) bool`

**Decision.** Every backend implements a single tiny interface.

**Why.** It lets the HTTP middleware stay identical while the implementation
behind it grows from in-memory to Redis to a sharded ring. Each phase becomes a
swap in `main.go`, not a rewrite.

**Tradeoff.** The interface is deliberately minimal — `bool`, no reason, no
retry-after duration. That keeps it universal but means the middleware can't send
an accurate `Retry-After` (it uses a static value). Widening the interface later
is the natural fix.

---

## 2. Per-key state via a Registry with double-checked locking

**Decision.** One algorithm object holds one key's state; a `Registry` maps
`key → limiter`, creating lazily, guarded by `sync.RWMutex` with a re-check after
taking the write lock.

**Why.** Traffic is read-heavy (mostly existing keys) → `RWMutex` lets reads run
concurrently. The check-then-act race (two goroutines creating the same key, one
clobbering the other's state) is closed by re-checking under the write lock.

**Rejected.** Plain `sync.Mutex` (serializes every lookup, even reads). A
`sync.Map` (works, but hides the locking lesson and complicates the janitor).

---

## 3. Janitor + atomic timestamp for memory reclamation

**Decision.** A background goroutine evicts keys idle past a TTL. Each entry's
`lastSeen` is an `atomic.Int64`.

**Why.** Without eviction the map leaks one entry per unique key forever
(one-off IPs). The atomic lets the request path update `lastSeen` while holding
only the *read* lock — the mutex guards the map structure, the atomic guards the
field. The sweep takes the write lock, which freezes timestamps and closes the
decide-stale-then-bump race.

**Tradeoff.** The sweep briefly blocks all traffic while it holds the write lock.
Fine at this scale; for a huge map, collect stale keys under a read lock then
delete under the write lock with a re-check.

---

## 4. Redis for shared state; Lua for atomicity

**Decision.** The distributed backend stores state in Redis and runs each
algorithm as a single Lua script.

**Why.** Moving state to Redis is what makes it shared across app instances. A
token bucket is a read-modify-write; with separate GET/SET from multiple
instances it loses updates and over-admits. Redis runs a Lua script indivisibly,
so the whole refill-check-decrement is atomic in one round trip.

**Rejected.** `MULTI/EXEC` — can't branch on a value read mid-transaction without
`WATCH` + optimistic retries. Lua is simpler and one round trip.

**Sub-decisions.**
- Clock from `redis.call('TIME')`, not the app, so all instances share one clock
  (app clocks drift).
- `EXPIRE`/`PEXPIRE` for cleanup — the distributed replacement for the janitor;
  no goroutine needed.

---

## 5. Consistent hashing for sharding the Redis layer

**Decision.** A hash ring with virtual nodes routes each key to exactly one Redis
node.

**Why.** A single Redis is a throughput bottleneck and a single point of failure.
The ring spreads keys across N nodes; consistent hashing means adding/removing a
node remaps only ~1/N of keys (vs `hash % N`, which remaps almost everything).
Virtual nodes keep the per-node load balanced.

**Tradeoff.** Slightly less exact than single Redis: when a node joins/leaves, the
keys that move land on a node with no history, so a client can briefly exceed its
limit. A given key is still atomic on its node — we never split one key's counter.

**Vocabulary note (a recurring point of confusion).** "Shared" = the Redis step
(vs in-memory). "Ring" = the multi-node step (vs single Redis). They label
*different layers*, not competing options. Correct framing: *the ring overcomes
the limitations of a single shared Redis.*

---

## 6. Partition policy: fail-open by default

**Decision.** On Redis error, return the `-fail-open` value; default `true`
(allow).

**Why.** This is the CAP tradeoff made concrete. A rate limiter should not take
the whole service down because its datastore blipped — favor Availability, accept
briefly exceeding limits. Fail-closed (deny) is correct only when over-admitting
is more dangerous than an outage.

**Proof.** Killing Redis mid-traffic with fail-open keeps requests returning 200;
limiting resumes when Redis returns.

---

## 7. Metrics as a `Limiter` decorator

**Decision.** `metrics.Instrument(next, algo)` wraps a `Limiter` and returns a
`Limiter`.

**Why.** Same interface trick as everything else — instrumentation works
identically across backends without the middleware or limiters knowing about it.

**Related.** `/metrics` and `/health` are registered outside the rate-limit
middleware; a 429 on a probe or scrape would be self-inflicted downtime.

---

## 8. Self-contained Go load generator (not k6)

**Decision.** `cmd/loadtest` is a small Go program, not an external tool.

**Why.** No install step; one `go run`. Keeps the whole project reproducible with
just Go + Docker.

**Gotcha encountered.** Must drain the response body before closing it — `net/http`
only reuses a keep-alive connection if the body is read to EOF. Skipping it opens
a socket per request and exhausts ephemeral ports (first benchmark run was
artificially ~50× too slow because of this).

**Measured (local, Apple Silicon, 50 workers, 8s):** memory ~169k req/s
(p99 1.0ms) vs single Redis ~42k req/s (p99 2.24ms) — the ~4×/+1ms gap is one
network round trip per decision: the latency price of shared state.
