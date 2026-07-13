// Command fsync_probe measures the cost of a durable 4KiB write on the build
// host: it writes 4KiB and calls fsync in a loop, timing each fsync, and
// prints a p50/p99 latency table. This is the methodology artifact behind the
// fsync disclosure in benchmarks/RESULTS.md — the number that explains why
// per-entry fsync caps throughput and why group commit is the design's answer.
//
// It is deliberately a standalone script (its own main package under scripts/,
// excluded from the module's test surface) so `go run` reproduces the disk
// disclosure on any host without touching the library build.
//
// Usage:
//
//	go run ./scripts                 # 200 samples (matches RESULTS.md), temp dir
//	go run ./scripts -n 1000         # more samples
//	go run ./scripts -dir /mnt/nvme  # probe a specific filesystem
//
// The probe fsyncs a real file on the target filesystem. It measures the
// syscall latency only (the 4KiB write buffer is prepared once); the reported
// numbers therefore reflect the durability barrier the WAL pays per group
// commit, not write() bandwidth.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

const blockSize = 4096

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "fsync_probe:", err)
		os.Exit(1)
	}
}

func run() error {
	n := flag.Int("n", 200, "number of fsync samples to measure")
	warmup := flag.Int("warmup", 20, "warmup fsyncs discarded before measuring")
	dir := flag.String("dir", "", "directory to probe (default: a fresh temp dir)")
	flag.Parse()

	if *n < 1 {
		return fmt.Errorf("-n must be >= 1")
	}

	probeDir := *dir
	if probeDir == "" {
		d, err := os.MkdirTemp("", "fsync-probe-")
		if err != nil {
			return fmt.Errorf("temp dir: %w", err)
		}
		defer os.RemoveAll(d)
		probeDir = d
	}

	path := filepath.Join(probeDir, "fsync_probe.dat")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer func() {
		f.Close()
		os.Remove(path)
	}()

	buf := make([]byte, blockSize)
	for i := range buf {
		buf[i] = byte(i)
	}

	// Warmup: let the file allocate its first blocks so the measured window
	// times steady-state overwrites, not first-touch allocation.
	for i := 0; i < *warmup; i++ {
		if err := writeSync(f, buf); err != nil {
			return err
		}
	}

	samples := make([]time.Duration, 0, *n)
	for i := 0; i < *n; i++ {
		if _, err := f.WriteAt(buf, 0); err != nil {
			return fmt.Errorf("write: %w", err)
		}
		t0 := time.Now()
		if err := f.Sync(); err != nil {
			return fmt.Errorf("fsync: %w", err)
		}
		samples = append(samples, time.Since(t0))
	}

	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	printTable(probeDir, samples)
	return nil
}

func writeSync(f *os.File, buf []byte) error {
	if _, err := f.WriteAt(buf, 0); err != nil {
		return fmt.Errorf("warmup write: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("warmup fsync: %w", err)
	}
	return nil
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p / 100 * float64(len(sorted)))
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func mean(s []time.Duration) time.Duration {
	if len(s) == 0 {
		return 0
	}
	var total time.Duration
	for _, d := range s {
		total += d
	}
	return total / time.Duration(len(s))
}

func printTable(dir string, sorted []time.Duration) {
	ms := func(d time.Duration) string {
		return fmt.Sprintf("%.2fms", float64(d.Microseconds())/1000)
	}
	fmt.Printf("4KiB fsync latency  (dir=%s, n=%d)\n", dir, len(sorted))
	fmt.Println("----------------------------------------")
	fmt.Printf("  min   %s\n", ms(sorted[0]))
	fmt.Printf("  p50   %s\n", ms(percentile(sorted, 50)))
	fmt.Printf("  p99   %s\n", ms(percentile(sorted, 99)))
	fmt.Printf("  max   %s\n", ms(sorted[len(sorted)-1]))
	fmt.Printf("  mean  %s\n", ms(mean(sorted)))
	fmt.Println("----------------------------------------")
	// The one-liner the resume/RESULTS.md quotes.
	fmt.Printf("4KiB fsync p50=%s p99=%s n=%d\n",
		ms(percentile(sorted, 50)), ms(percentile(sorted, 99)), len(sorted))
}
