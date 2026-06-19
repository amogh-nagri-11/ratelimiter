package limiter

import (
	"testing"
	"time"
)

// TestDistributedEnforcesLimit checks the end-to-end path: ring routing +
// per-node Redis script. With a single node the ring always picks it, so the
// limit must still hold exactly. (Multi-node routing balance is covered by the
// hashring package's own tests; here we prove the wiring delegates correctly.)
func TestDistributedEnforcesLimit(t *testing.T) {
	addr := redisAddr(t)
	dl, err := NewDistributedLimiter([]string{addr}, 100, AlgoTokenBucket, 3, time.Hour, true)
	if err != nil {
		t.Fatal(err)
	}
	defer dl.Close()

	key := uniqueKey("dist")
	for i := 0; i < 3; i++ {
		if !dl.Allow(key) {
			t.Fatalf("request %d should be allowed", i+1)
		}
	}
	if dl.Allow(key) {
		t.Fatal("4th request should be denied")
	}
}

// TestDistributedIsolatesKeys: two different keys have independent budgets even
// when they happen to land on the same node.
func TestDistributedIsolatesKeys(t *testing.T) {
	addr := redisAddr(t)
	dl, err := NewDistributedLimiter([]string{addr}, 100, AlgoTokenBucket, 1, time.Hour, true)
	if err != nil {
		t.Fatal(err)
	}
	defer dl.Close()

	a, b := uniqueKey("a"), uniqueKey("b")
	dl.Allow(a) // exhaust a
	if !dl.Allow(b) {
		t.Fatal("key b should have its own budget")
	}
}
