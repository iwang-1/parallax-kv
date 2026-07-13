// Command parallax-bench is the closed-loop benchmark client behind
// benchmarks/RESULTS.md. Methodology lives with the results: fixed
// key/value sizes, warmup discarded, repeated runs, medians reported.
//
// Usage (stage S5 runs the real matrix):
//
//	parallax-bench --cluster ... --clients 64 --duration 30s [--smoke]
//
// --smoke is the CI mode: a short run that asserts nothing about
// throughput (host performance numbers are never asserted in CI).
package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	fs := flag.NewFlagSet("parallax-bench", flag.ExitOnError)
	cluster := fs.String("cluster", "localhost:8101", "comma-separated client addresses")
	clients := fs.Int("clients", 8, "closed-loop client concurrency")
	duration := fs.Duration("duration", 0, "measured run duration")
	smoke := fs.Bool("smoke", false, "CI smoke mode: short run, no assertions")
	_ = fs.Parse(os.Args[1:])
	_, _, _, _ = cluster, clients, duration, smoke

	// TODO(S4/S5): workload, latency capture, report emission.
	fmt.Fprintln(os.Stderr, "parallax-bench: not implemented yet (stage S4)")
	os.Exit(1)
}
