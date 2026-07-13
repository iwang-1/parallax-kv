// Command parallax-bench is the closed-loop benchmark client behind
// benchmarks/RESULTS.md. Methodology lives with the results: fixed key/value
// sizes (16B keys, 128B values), a warmup discarded, and medians reported
// across repetitions by the caller.
//
// Closed loop: --clients goroutines each issue one operation, wait for its
// result, then immediately issue the next — so offered concurrency equals
// --clients. Durability is whatever the server runs (group-commit fsync per
// Ready batch, the default and only mode); the benchmark does not and cannot
// change it from the client side.
//
// Usage:
//
//	parallax-bench --cluster host:port,... --clients 64 --duration 30s --workload write
//	parallax-bench --cluster ... --smoke        # CI mode: 3s run, asserts nothing
//
// --smoke is the CI mode: a short run that prints nothing load-bearing and
// asserts nothing about throughput (host performance numbers are never
// asserted in CI).
package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/iwang-1/parallax-kv/client"
)

const (
	keySize   = 16
	valueSize = 128
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "parallax-bench:", err)
		os.Exit(1)
	}
}

func run() error {
	fs := flag.NewFlagSet("parallax-bench", flag.ExitOnError)
	cluster := fs.String("cluster", "localhost:8101", "comma-separated client addresses")
	clients := fs.Int("clients", 8, "closed-loop client concurrency")
	duration := fs.Duration("duration", 30*time.Second, "measured run duration")
	warmup := fs.Duration("warmup", 5*time.Second, "warmup discarded before measuring")
	workload := fs.String("workload", "write", "workload: write | read")
	keyspace := fs.Int("keyspace", 1000, "number of distinct keys touched")
	smoke := fs.Bool("smoke", false, "CI smoke mode: short run, no assertions, no numbers")
	_ = fs.Parse(os.Args[1:])

	if *smoke {
		*duration = 3 * time.Second
		*warmup = 500 * time.Millisecond
		if *clients > 8 {
			*clients = 8
		}
	}
	if *clients < 1 {
		return fmt.Errorf("--clients must be >= 1")
	}
	addrs := splitAddrs(*cluster)
	if len(addrs) == 0 {
		return fmt.Errorf("--cluster is empty")
	}
	read := false
	switch *workload {
	case "write":
	case "read":
		read = true
	default:
		return fmt.Errorf("--workload must be write or read")
	}

	agg, err := runBench(benchParams{
		addrs:    addrs,
		clients:  *clients,
		duration: *duration,
		warmup:   *warmup,
		keyspace: *keyspace,
		read:     read,
	})
	if err != nil {
		return err
	}

	if *smoke {
		fmt.Printf("smoke OK: %d ops in %s (assertions disabled)\n", agg.count, *duration)
		return nil
	}
	agg.report(os.Stdout, *workload, *clients, *duration)
	return nil
}

type benchParams struct {
	addrs    []string
	clients  int
	duration time.Duration
	warmup   time.Duration
	keyspace int
	read     bool
}

// runBench spins up one leader-chasing client per worker, seeds the keyspace,
// then runs the closed loop. Latencies during the warmup window are discarded.
func runBench(p benchParams) (*aggregate, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Seed every key so read workloads hit existing values and writes measure
	// the steady state (not first-insert). Uses a dedicated client.
	seeder, err := client.Dial(p.addrs)
	if err != nil {
		return nil, err
	}
	val := randBytes(valueSize)
	for i := 0; i < p.keyspace; i++ {
		sctx, scancel := context.WithTimeout(ctx, 10*time.Second)
		_, err := seeder.Put(sctx, keyFor(i), val)
		scancel()
		if err != nil {
			seeder.Close()
			return nil, fmt.Errorf("seed key %d: %w", i, err)
		}
	}
	seeder.Close()

	start := time.Now()
	measureAt := start.Add(p.warmup)
	deadline := start.Add(p.warmup + p.duration)

	var wg sync.WaitGroup
	perWorker := make([]*aggregate, p.clients)
	errCh := make(chan error, p.clients)

	for w := 0; w < p.clients; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			c, err := client.Dial(p.addrs)
			if err != nil {
				errCh <- err
				return
			}
			defer c.Close()
			agg := newAggregate()
			perWorker[w] = agg
			val := randBytes(valueSize)
			i := w
			for time.Now().Before(deadline) {
				key := keyFor(i % p.keyspace)
				i++
				opCtx, opCancel := context.WithTimeout(ctx, 10*time.Second)
				t0 := time.Now()
				var opErr error
				if p.read {
					_, opErr = c.Get(opCtx, key)
				} else {
					_, opErr = c.Put(opCtx, key, val)
				}
				lat := time.Since(t0)
				opCancel()
				if opErr != nil {
					// Count errors but do not abort: a benchmark against a
					// cluster mid-election should tolerate transient failures.
					agg.errors++
					continue
				}
				if t0.After(measureAt) {
					agg.record(lat)
				}
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	if err := <-errCh; err != nil {
		return nil, err
	}

	total := newAggregate()
	total.window = p.duration
	for _, a := range perWorker {
		if a != nil {
			total.merge(a)
		}
	}
	return total, nil
}

func keyFor(i int) string { return fmt.Sprintf("key-%011d", i) }

func randBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

func splitAddrs(s string) []string {
	var out []string
	for _, a := range strings.Split(s, ",") {
		if a = strings.TrimSpace(a); a != "" {
			out = append(out, a)
		}
	}
	return out
}

// aggregate accumulates latency samples and error counts for the measured
// window. Latencies are stored raw and percentiles computed by sorting at
// report time — sample counts are small enough (tens of thousands) that this
// is simpler and more accurate than a bucketed histogram.
type aggregate struct {
	latencies []time.Duration
	count     int64
	errors    int64
	window    time.Duration
}

func newAggregate() *aggregate { return &aggregate{} }

func (a *aggregate) record(d time.Duration) {
	a.latencies = append(a.latencies, d)
	a.count++
}

func (a *aggregate) merge(o *aggregate) {
	a.latencies = append(a.latencies, o.latencies...)
	a.count += o.count
	a.errors += o.errors
}

func (a *aggregate) percentile(p float64) time.Duration {
	if len(a.latencies) == 0 {
		return 0
	}
	idx := int(p / 100 * float64(len(a.latencies)))
	if idx >= len(a.latencies) {
		idx = len(a.latencies) - 1
	}
	return a.latencies[idx]
}

// report prints a human-readable summary. Numbers here are measurements, not
// assertions; the RESULTS.md methodology governs how they are quoted.
func (a *aggregate) report(w *os.File, workload string, clients int, dur time.Duration) {
	sort.Slice(a.latencies, func(i, j int) bool { return a.latencies[i] < a.latencies[j] })
	var thru float64
	if a.window > 0 {
		thru = float64(a.count) / a.window.Seconds()
	}
	fmt.Fprintf(w, "workload=%s clients=%d duration=%s\n", workload, clients, dur)
	fmt.Fprintf(w, "  ops (measured)   : %d\n", a.count)
	fmt.Fprintf(w, "  errors           : %d\n", a.errors)
	fmt.Fprintf(w, "  throughput       : %.1f ops/sec\n", thru)
	fmt.Fprintf(w, "  latency p50      : %s\n", a.percentile(50).Round(time.Microsecond))
	fmt.Fprintf(w, "  latency p99      : %s\n", a.percentile(99).Round(time.Microsecond))
	fmt.Fprintf(w, "  latency max      : %s\n", a.percentile(100).Round(time.Microsecond))
}
