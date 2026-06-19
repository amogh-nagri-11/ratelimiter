package limiter

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// tokenBucketScript is the token-bucket algorithm, server-side, in Lua.
//
// WHY A SCRIPT AT ALL — atomicity. A token bucket is a read-modify-write:
// read current tokens, refill by elapsed time, check >= 1, decrement, write
// back. If two app instances each do that with separate GET/SET commands, they
// interleave:
//
//	A: GET tokens -> 1        B: GET tokens -> 1
//	A: 1 >= 1, SET 0, ALLOW   B: 1 >= 1, SET 0, ALLOW   // both allowed, 1 token!
//
// That's a classic lost update, and it happens precisely *because* the system is
// distributed — multiple clients hitting one Redis. Redis runs each Lua script
// to completion with nothing else interleaved (single-threaded command
// execution), so wrapping the whole read-refill-check-write in one script makes
// it indivisible. One round trip, no race.
//
// WHY LUA OVER MULTI/EXEC — MULTI/EXEC queues commands and runs them atomically,
// but you can't branch on a value you read mid-transaction (the reads don't
// return until EXEC). You'd need WATCH + optimistic-retry loops on contention.
// Lua lets us read, compute, decide, and write in a single atomic pass.
//
// WHY redis.call('TIME') — the clock is read from Redis, not from the app. In a
// distributed system the app instances' clocks drift; making Redis the single
// source of time keeps every instance's refill math consistent.
//
// The EXPIRE at the end is the distributed replacement for Phase 4's janitor:
// an idle bucket simply expires, so Redis reclaims the memory with no goroutine.
const tokenBucketScript = `
local key      = KEYS[1]
local capacity = tonumber(ARGV[1])
local rate     = tonumber(ARGV[2])   -- tokens per second

local t   = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000

local data   = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts     = tonumber(data[2])
if tokens == nil then
  tokens = capacity
  ts     = now
end

local elapsed = now - ts
if elapsed < 0 then elapsed = 0 end
tokens = math.min(capacity, tokens + elapsed * rate)

local allowed = 0
if tokens >= 1 then
  tokens  = tokens - 1
  allowed = 1
end

redis.call('HSET', key, 'tokens', tokens, 'ts', now)
redis.call('EXPIRE', key, math.ceil(capacity / rate) + 1)
return allowed
`

// slidingWindowScript is the exact sliding-window-log algorithm in Redis using a
// sorted set: members are individual requests, scored by timestamp. We drop
// expired entries, count what's left, and add the new request if there's room —
// all atomically, for the same reason as above.
const slidingWindowScript = `
local key    = KEYS[1]
local window = tonumber(ARGV[1])   -- microseconds
local limit  = tonumber(ARGV[2])
local member = ARGV[3]             -- unique id so two requests never collide

local t   = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])

redis.call('ZREMRANGEBYSCORE', key, 0, now - window)
local count   = redis.call('ZCARD', key)
local allowed = 0
if count < limit then
  redis.call('ZADD', key, now, member)
  allowed = 1
end
redis.call('PEXPIRE', key, math.ceil(window / 1000) + 1000)
return allowed
`

// RedisLimiter is a Limiter whose state lives in Redis, so every app instance
// pointed at the same Redis shares one set of counters. Unlike the in-memory
// limiters it is *naturally* keyed — the key is part of the Redis key — so it
// satisfies Limiter (Allow(key string) bool) directly, with no Registry needed.
type RedisLimiter struct {
	client   *redis.Client
	script   *redis.Script
	algo     string
	capacity float64       // token bucket: bucket size
	rate     float64       // token bucket: tokens/sec
	window   time.Duration // sliding window
	limit    int           // sliding window
	failOpen bool          // on Redis error: allow (true) or deny (false)?
}

// NewRedisLimiter connects to the Redis at addr and prepares the chosen
// algorithm. failOpen sets the partition policy (see Allow).
func NewRedisLimiter(addr, algo string, limit int, window time.Duration, failOpen bool) (*RedisLimiter, error) {
	if limit <= 0 || window <= 0 {
		return nil, fmt.Errorf("limit and window must be > 0")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("connect redis %s: %w", addr, err)
	}

	rl := &RedisLimiter{client: client, algo: algo, window: window, limit: limit, failOpen: failOpen}
	switch algo {
	case AlgoTokenBucket:
		rl.script = redis.NewScript(tokenBucketScript)
		rl.capacity = float64(limit)
		rl.rate = float64(limit) / window.Seconds()
	case AlgoSlidingWindow:
		rl.script = redis.NewScript(slidingWindowScript)
	default:
		return nil, fmt.Errorf("unknown algorithm %q", algo)
	}
	return rl, nil
}

// Allow runs the atomic script for `key`.
//
// PARTITION POLICY: if Redis is unreachable (network partition, node down), we
// can't know the true count. We then return failOpen. The common production
// choice is fail-OPEN (allow): a rate limiter should not take your whole service
// down just because the limiter's datastore blipped — you accept that limits may
// be briefly exceeded. Fail-CLOSED (deny) is the right call only when exceeding
// the limit is more dangerous than an outage. This is the CAP tradeoff made
// concrete: under partition we pick Availability (fail open) or Consistency
// (fail closed).
func (r *RedisLimiter) Allow(key string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var (
		res interface{}
		err error
	)
	switch r.algo {
	case AlgoTokenBucket:
		res, err = r.script.Run(ctx, r.client, []string{key}, r.capacity, r.rate).Result()
	case AlgoSlidingWindow:
		res, err = r.script.Run(ctx, r.client, []string{key}, r.window.Microseconds(), r.limit, uniqueMember()).Result()
	}
	if err != nil {
		return r.failOpen
	}
	allowed, _ := res.(int64)
	return allowed == 1
}

// Close releases the Redis connection pool.
func (r *RedisLimiter) Close() error { return r.client.Close() }

// uniqueMember returns a value unique per request so two requests in the same
// microsecond don't collapse into one sorted-set member.
func uniqueMember() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("%d-%s", time.Now().UnixNano(), hex.EncodeToString(b[:]))
}
