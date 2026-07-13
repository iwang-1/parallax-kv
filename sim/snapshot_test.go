package sim

import (
	"testing"
)

// snapshotConfig is a low-fault config that enables aggressive compaction: a
// small SnapshotEntries threshold so nodes snapshot and truncate their logs
// repeatedly over a short run, exercising the compaction + restore path
// end-to-end on the real consensus core.
func snapshotConfig(seed uint64) Config {
	c := smokeConfig(seed)
	c.SnapshotEntries = 8
	return c
}

// TestSnapshotCompaction is the S3 step-1 headline: with compaction enabled,
// nodes snapshot their state machine and truncate the covered log prefix while
// a workload runs. It asserts that (1) at least one node actually compacted
// (its FirstIndex advanced past 1, so a prefix was discarded), (2) the run
// stayed free of invariant violations, and (3) the client history remained
// linearizable. Compaction must not weaken any safety property.
func TestSnapshotCompaction(t *testing.T) {
	s, err := New(snapshotConfig(1))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.RunUntil(6 * Second); err != nil {
		t.Fatalf("run: %v", err)
	}

	// (1) Some node truncated its log: FirstIndex advanced beyond 1, meaning a
	// prefix now lives only in a snapshot.
	maxFirst := uint64(1)
	for _, id := range s.peers {
		ns := s.nodes[id]
		if ns == nil || ns.storage == nil {
			continue
		}
		if fi, err := ns.storage.FirstIndex(); err == nil && fi > maxFirst {
			maxFirst = fi
		}
	}
	if maxFirst == 1 {
		t.Fatal("no node compacted its log (FirstIndex never advanced past 1)")
	}

	// (2) A workload ran and (3) it was linearizable.
	if got := len(s.History().Operations()); got == 0 {
		t.Fatal("no client operations completed; the check would be vacuous")
	}
	if err := s.CheckLinearizability(); err != nil {
		t.Fatalf("linearizability check failed: %v", err)
	}
	t.Logf("compaction advanced a node's FirstIndex to %d; %d ops (linearizable)",
		maxFirst, len(s.History().Operations()))
}

// TestSnapshotRestartRestore proves the crash-recovery-from-snapshot path: a
// node compacts (so part of its state lives only in a snapshot, not the log),
// then crashes and restarts. On restart it must restore its state machine from
// the persisted snapshot and resume applying from the compacted prefix rather
// than replaying from index 0 — and the cluster must remain linearizable with
// no applied-prefix divergence (both checked every step by the invariant
// checker, which RunUntil surfaces).
func TestSnapshotRestartRestore(t *testing.T) {
	s, err := New(snapshotConfig(7))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Let the cluster elect, replicate, and compact.
	if err := s.RunUntil(4 * Second); err != nil {
		t.Fatalf("pre-crash run: %v", err)
	}

	// Pick a node that has actually compacted (snapshot index > 0), so restart
	// is forced to restore from the snapshot rather than a full log replay.
	var target uint64
	var snapIndex uint64
	for _, id := range s.peers {
		ns := s.nodes[id]
		if ns == nil || ns.storage == nil {
			continue
		}
		if snap, err := ns.storage.Snapshot(); err == nil && snap.Metadata.Index > 0 {
			target, snapIndex = id, snap.Metadata.Index
			break
		}
	}
	if target == 0 {
		t.Fatal("no node produced a snapshot to restore from")
	}

	// Crash and restart it: startNode must rebuild the state machine from the
	// snapshot and set applied to the snapshot index.
	s.Crash(target)
	s.Restart(target)
	if s.Err() != nil {
		t.Fatalf("restart failed: %v", s.Err())
	}
	if ns := s.nodes[target]; ns.applied < snapIndex {
		t.Fatalf("node %d restarted with applied=%d, below its snapshot index %d (state not restored)",
			target, ns.applied, snapIndex)
	}

	// Continue running: the restored node must rejoin and the cluster must stay
	// safe and linearizable through further compaction and workload.
	if err := s.RunUntil(8 * Second); err != nil {
		t.Fatalf("post-restart run: %v", err)
	}
	if err := s.CheckLinearizability(); err != nil {
		t.Fatalf("linearizability check failed after restart: %v", err)
	}
	t.Logf("node %d restored from snapshot index %d and rejoined; cluster linearizable",
		target, snapIndex)
}

// TestSnapshotDeterminism proves compaction did not leak nondeterminism: with
// SnapshotEntries set, the same seed must still produce byte-identical trace
// hashes across two runs. Compaction is scheduled off the applied index (no
// RNG draw), so it must not perturb the determinism gate.
func TestSnapshotDeterminism(t *testing.T) {
	seeds := []uint64{1, 2, 3, 7, 0xC0FFEE}
	run := func(seed uint64) (string, int) {
		s, err := New(snapshotConfig(seed))
		if err != nil {
			t.Fatalf("seed 0x%x: New: %v", seed, err)
		}
		if err := s.RunUntil(6 * Second); err != nil {
			t.Fatalf("seed 0x%x: run: %v", seed, err)
		}
		return s.TraceHash(), len(s.History().Operations())
	}
	for _, seed := range seeds {
		h1, n1 := run(seed)
		h2, n2 := run(seed)
		if h1 != h2 {
			t.Fatalf("seed 0x%x: trace hashes differ with compaction on:\n  %s\n  %s", seed, h1, h2)
		}
		if n1 != n2 {
			t.Fatalf("seed 0x%x: op counts differ: %d vs %d", seed, n1, n2)
		}
		if n1 == 0 {
			t.Fatalf("seed 0x%x: no ops completed (vacuous)", seed)
		}
	}
}
