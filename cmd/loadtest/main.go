// Command loadtest is a tiny self-contained HTTP load generator. It runs N
// concurrent workers hammering a URL for a fixed duration, then reports
// throughput, latency percentiles, and the status-code breakdown — enough to
// characterize the rate limiter without pulling in an external tool like k6.
package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"time"
)

type result struct {
	latencies []time.Duration
	statuses  map[int]int
	errors    int
}

func main() {
	url := flag.String("url", "http://localhost:8080/", "target URL")
	duration := flag.Duration("duration", 10*time.Second, "how long to run")
	concurrency := flag.Int("concurrency", 50, "number of concurrent workers")
	flag.Parse()

	deadline := time.Now().Add(*duration)
	results := make([]result, *concurrency)

	// One HTTP client shared by all workers, with a connection pool sized to the
	// concurrency so keep-alive is reused instead of opening a socket per request.
	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        *concurrency,
			MaxIdleConnsPerHost: *concurrency,
		},
	}

	var wg sync.WaitGroup
	wg.Add(*concurrency)
	for i := 0; i < *concurrency; i++ {
		go func(slot int) {
			defer wg.Done()
			r := result{statuses: map[int]int{}}
			for time.Now().Before(deadline) {
				start := time.Now()
				resp, err := client.Get(*url)
				elapsed := time.Since(start)
				if err != nil {
					r.errors++
					continue
				}
				// Drain the body before closing: net/http only reuses a keep-alive
				// connection if the body is read to EOF. Skipping this opens a fresh
				// socket per request and exhausts ephemeral ports under load.
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				r.latencies = append(r.latencies, elapsed)
				r.statuses[resp.StatusCode]++
			}
			results[slot] = r
		}(i)
	}
	wg.Wait()

	// Merge per-worker results (workers wrote disjoint slots, so no locking needed).
	var all []time.Duration
	statuses := map[int]int{}
	errors := 0
	for _, r := range results {
		all = append(all, r.latencies...)
		for code, n := range r.statuses {
			statuses[code] += n
		}
		errors += r.errors
	}

	total := len(all)
	rps := float64(total) / duration.Seconds()

	fmt.Printf("URL:          %s\n", *url)
	fmt.Printf("Duration:     %s   Workers: %d\n", *duration, *concurrency)
	fmt.Printf("Requests:     %d   Errors: %d\n", total, errors)
	fmt.Printf("Throughput:   %.0f req/s\n", rps)
	fmt.Printf("Status codes: ")
	for _, code := range sortedKeys(statuses) {
		fmt.Printf("%d=%d  ", code, statuses[code])
	}
	fmt.Println()

	if total > 0 {
		sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
		fmt.Printf("Latency:      p50=%s  p95=%s  p99=%s  max=%s\n",
			percentile(all, 50), percentile(all, 95), percentile(all, 99), all[total-1])
	}
}

// percentile returns the p-th percentile of a sorted slice (nearest-rank).
func percentile(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := (p * len(sorted)) / 100
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx].Round(time.Microsecond)
}

func sortedKeys(m map[int]int) []int {
	keys := make([]int, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	return keys
}
