// Command recoverytime measures how long disk.Open takes to recover a node's data directory (snapshot load
// plus WAL replay), the dominant cost of restarting a node.
//
//	go run ./scripts/recoverytime -dir <node data dir> [-n 5]
package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/iwang-1/parallax-kv/storage/disk"
)

func main() {
	dir := flag.String("dir", "", "node data directory")
	n := flag.Int("n", 5, "repetitions")
	flag.Parse()
	if *dir == "" {
		fmt.Fprintln(os.Stderr, "-dir is required")
		os.Exit(2)
	}
	var times []time.Duration
	var first, last uint64
	for i := 0; i < *n; i++ {
		t0 := time.Now()
		s, err := disk.Open(*dir)
		if err != nil {
			fmt.Fprintln(os.Stderr, "open:", err)
			os.Exit(1)
		}
		times = append(times, time.Since(t0))
		first, _ = s.FirstIndex()
		last, _ = s.LastIndex()
		s.Close()
	}
	sort.Slice(times, func(a, b int) bool { return times[a] < times[b] })
	fmt.Printf("recovery: median %v (min %v, max %v) over %d opens; log entries in memory: %d (first %d, last %d)\n",
		times[len(times)/2], times[0], times[len(times)-1], *n, last-first+1, first, last)
}
