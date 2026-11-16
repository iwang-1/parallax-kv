package server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iwang-1/parallax-kv/client"
	"github.com/iwang-1/parallax-kv/kv"
)

// node is one restartable member of a compaction-test cluster.
type node struct {
	cfg    Config
	srv    *Server
	cancel context.CancelFunc
	done   chan struct{}
}

func (n *node) start(t *testing.T) {
	t.Helper()
	srv, err := New(n.cfg)
	if err != nil {
		t.Fatalf("new server %d: %v", n.cfg.ID, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = srv.Run(ctx)
	}()
	n.srv, n.cancel, n.done = srv, cancel, done
}

// stop cancels the node and waits for its drive loop to exit, after which its state machine is quiescent.
func (n *node) stop() {
	if n.cancel == nil {
		return
	}
	n.cancel()
	<-n.done
	time.Sleep(100 * time.Millisecond) // let listeners release their ports
	n.cancel = nil
}

func countFiles(t *testing.T, dir, prefix, suffix string) int {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), prefix) && strings.HasSuffix(e.Name(), suffix) {
			n++
		}
	}
	return n
}

// TestCompactionAndInstallSnapshotCatchUp runs a three-node cluster that snapshots every 50 applied entries
// with tiny WAL segments. A follower is stopped while the other two write far past its log and compact it
// away, so on restart it can only catch up through InstallSnapshot. The test then checks, from the
// restarted node's own state machine, that every write is there, and that compaction kept every node's
// WAL and snapshot files bounded.
func TestCompactionAndInstallSnapshotCatchUp(t *testing.T) {
	const peerBase, clientBase = 27200, 28200
	dir := t.TempDir()
	peers := map[uint64]string{}
	clientAddrs := map[uint64]string{}
	for i := 0; i < 3; i++ {
		id := uint64(i + 1)
		peers[id] = fmt.Sprintf("localhost:%d", peerBase+i)
		clientAddrs[id] = fmt.Sprintf("localhost:%d", clientBase+i)
	}
	nodes := make([]*node, 3)
	for i := range nodes {
		id := uint64(i + 1)
		nodes[i] = &node{cfg: Config{
			ID:                 id,
			Peers:              peers,
			ClientAddrs:        clientAddrs,
			DataDir:            filepath.Join(dir, fmt.Sprintf("d%d", id)),
			ListenPeer:         peers[id],
			ListenClient:       clientAddrs[id],
			TickIntervalMillis: 10,
			HeartbeatTicks:     3,
			ElectionTicks:      15,
			SnapshotEntries:    50,
			WALSegmentBytes:    2048,
		}}
		nodes[i].start(t)
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			n.stop()
		}
	})

	addrs := []string{clientAddrs[1], clientAddrs[2], clientAddrs[3]}
	c, err := client.Dial(addrs)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	ctx := context.Background()
	put := func(i int) {
		t.Helper()
		if _, err := putWithRetry(ctx, c, fmt.Sprintf("key-%04d", i), fmt.Sprintf("value-%04d", i)); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	for i := 0; i < 100; i++ {
		put(i)
	}

	// Take node 3 down, then write far enough past its log that the other two compact it away.
	lagging := nodes[2]
	lagging.stop()
	appliedWhenStopped := lagging.srv.appliedIndex.Load()
	for i := 100; i < 500; i++ {
		put(i)
	}
	for _, n := range nodes[:2] {
		if n.srv.snapshots.Load() == 0 {
			t.Fatalf("node %d never compacted its log", n.cfg.ID)
		}
	}

	lagging.start(t)
	target := nodes[0].srv.appliedIndex.Load()
	if a := nodes[1].srv.appliedIndex.Load(); a > target {
		target = a
	}
	deadline := time.Now().Add(15 * time.Second)
	for lagging.srv.appliedIndex.Load() < target {
		if time.Now().After(deadline) {
			t.Fatalf("restarted node applied %d, want >= %d (stopped at %d)",
				lagging.srv.appliedIndex.Load(), target, appliedWhenStopped)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if lagging.srv.installedSnapshots.Load() == 0 {
		t.Fatalf("restarted node caught up from %d to %d without an InstallSnapshot; compaction did not remove its entries",
			appliedWhenStopped, target)
	}

	// Stop everything and inspect quiescent state.
	for _, n := range nodes {
		n.stop()
	}
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("key-%04d", i)
		got := lagging.srv.sm.Read(key)
		if got.Status != kv.StatusOK || string(got.Value) != fmt.Sprintf("value-%04d", i) {
			t.Fatalf("restarted node's state machine: %s = %+v", key, got)
		}
	}
	for _, n := range nodes {
		segs := countFiles(t, n.cfg.DataDir, "wal-", ".seg")
		snaps := countFiles(t, n.cfg.DataDir, "snap-", ".snap")
		t.Logf("node %d: %d WAL segments, %d snapshot files, %d compactions", n.cfg.ID, segs, snaps, n.srv.snapshots.Load())
		if snaps > 2 {
			t.Fatalf("node %d kept %d snapshot files", n.cfg.ID, snaps)
		}
		// 500 writes at ~100 bytes per entry record would fill dozens of 2 KiB segments without compaction.
		if segs > 12 {
			t.Fatalf("node %d has %d WAL segments; compaction is not reclaiming them", n.cfg.ID, segs)
		}
	}
}
