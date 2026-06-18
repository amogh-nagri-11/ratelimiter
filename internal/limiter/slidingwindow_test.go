 package limiter

import (
	"testing"
	"time"
)

func TestSlidingWindow_AllowsUpToLimit(t *testing.T) {
	sw := NewSlidingWindowLog(3, time.Second)

	for i := 0; i < 3; i++ {
		if !sw.Allow() {
			t.Fatalf("request %d should be allowed", i+1)
		}
	}
	if sw.Allow() {
		t.Fatal("4th request should be denied (limit reached)")
	}
}

func TestSlidingWindow_SlidesOverTime(t *testing.T) {
	// limit 2 per 100ms.
	sw := NewSlidingWindowLog(2, 100*time.Millisecond)

	if !sw.Allow() || !sw.Allow() {
		t.Fatal("first two requests should be allowed")
	}
	if sw.Allow() {
		t.Fatal("third request should be denied")
	}

	// Wait for the window to fully slide past the first two.
	time.Sleep(120 * time.Millisecond)

	if !sw.Allow() || !sw.Allow() {
		t.Fatal("after window slides, two requests should be allowed again")
	}
}