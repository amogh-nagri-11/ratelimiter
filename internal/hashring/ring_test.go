package hashring

import (
	"fmt"
	"testing"
)

// TestSameKeySameNode: the ring must be deterministic — a key always routes to
// the same node (that's what lets each key's state live in one place).
func TestSameKeySameNode(t *testing.T) {
	r := New(100, "a", "b", "c")
	first, _ := r.Get("user-42")
	for i := 0; i < 1000; i++ {
		got, _ := r.Get("user-42")
		if got != first {
			t.Fatalf("key routed inconsistently: %s vs %s", first, got)
		}
	}
}

// TestEmptyRing: Get on an empty ring reports not-found rather than panicking.
func TestEmptyRing(t *testing.T) {
	if _, ok := New(100).Get("k"); ok {
		t.Fatal("empty ring should return ok=false")
	}
}

// TestDistributionIsRoughlyBalanced: with virtual nodes, keys should spread
// across nodes without any node hogging everything. We allow a generous band.
func TestDistributionIsRoughlyBalanced(t *testing.T) {
	r := New(200, "a", "b", "c", "d")
	counts := map[string]int{}
	const n = 40000
	for i := 0; i < n; i++ {
		node, _ := r.Get(fmt.Sprintf("key-%d", i))
		counts[node]++
	}
	expected := n / 4
	for node, c := range counts {
		// within +/-25% of an even share
		if c < expected*3/4 || c > expected*5/4 {
			t.Errorf("node %s got %d keys, expected ~%d (uneven distribution)", node, c, expected)
		}
	}
}

// TestRemoveMovesFewKeys is the whole point of consistent hashing: removing one
// of N nodes should move only the keys that lived on it (~1/N), leaving the rest
// untouched. A plain hash%N would remap almost everything.
func TestRemoveMovesFewKeys(t *testing.T) {
	r := New(200, "a", "b", "c", "d")
	const n = 40000

	before := make(map[string]string, n)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key-%d", i)
		before[k], _ = r.Get(k)
	}

	r.Remove("c")

	moved := 0
	for k, oldNode := range before {
		newNode, _ := r.Get(k)
		if newNode != oldNode {
			moved++
		}
	}
	// Only keys formerly on "c" (~1/4) should have moved. Assert well under half.
	if moved > n*40/100 {
		t.Fatalf("removing 1 of 4 nodes moved %d/%d keys (%.0f%%); expected ~25%%",
			moved, n, float64(moved)/n*100)
	}
}
