package sim

import (
	"testing"

	"github.com/iwang-1/parallax-kv/raft"
)

// smokeConfig is a low-fault config for the S2a integration smoke tests: a
// 3-node cluster on a fast, lightly perturbed network. It exercises the REAL
// raft core (New uses defaultNodeFactory), unlike the mock-driven harness
// tests. Drops/dups are kept low so a leader reliably emerges and the
// workload makes steady progress within a few virtual seconds.
func smokeConfig(seed uint64) Config {
	c := baseConfig(seed)
	c.Net.DropRate = 0.01
	c.Net.DupRate = 0.01
	return c
}

// currentLeaders returns the IDs of live nodes that currently report
// StateLeader, in ascending order.
func (s *Simulator) currentLeaders() []uint64 {
	var out []uint64
	for _, id := range s.peers {
		ns := s.nodes[id]
		if ns == nil || ns.crashed || ns.node == nil {
			continue
		}
		if ns.node.State() == raft.StateLeader {
			out = append(out, id)
		}
	}
	return out
}

// leaderElected reports whether at least one node became leader at any point
// during the run so far (observed via the applied log gaining current-term
// entries, or a live leader right now).
func (s *Simulator) sawLeader() bool {
	if len(s.currentLeaders()) > 0 {
		return true
	}
	// A leader may have stepped down after committing; the trace records
	// every entry applied, so any applied entry implies a leader existed.
	for _, id := range s.peers {
		if ns := s.nodes[id]; ns != nil && len(ns.appliedLog) > 0 {
			return true
		}
	}
	return false
}

// TestSmokeElectAndReplicate is the S2a headline: a 3-node cluster running
// the real raft core elects a leader, replicates client writes, and completes
// a basic client workload — all on the deterministic simulator.
func TestSmokeElectAndReplicate(t *testing.T) {
	s, err := New(smokeConfig(1))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.RunUntil(5 * Second); err != nil {
		t.Fatalf("run: %v", err)
	}

	// (1) A leader was elected.
	if !s.sawLeader() {
		t.Fatal("no leader was ever elected")
	}

	// (2) Writes were replicated and committed: every live node applied a
	// nonempty prefix, and the applied prefixes agree (checked every step by
	// the invariant checker, which returned nil above). Confirm the leader
	// applied a positive number of entries.
	maxApplied := uint64(0)
	for _, id := range s.peers {
		if ns := s.nodes[id]; ns != nil && ns.applied > maxApplied {
			maxApplied = ns.applied
		}
	}
	if maxApplied == 0 {
		t.Fatal("no entries were ever committed/applied")
	}

	// (3) The client workload made progress: operations completed.
	ops := s.History().Operations()
	if len(ops) == 0 {
		t.Fatal("real core completed zero client operations")
	}
	t.Logf("leader elected, %d entries applied, %d client ops completed, %d trace events",
		maxApplied, len(ops), len(s.Trace()))
}

// TestSmokeDeterminism is the determinism gate over the REAL core: five fixed
// seeds, each run twice, must produce byte-identical trace hashes and equal
// completed-operation counts. This is the property CI's determinism gate
// enforces; running it over the real consensus core (not just the mock)
// proves the integration introduced no wall-clock, goroutine, or map-order
// nondeterminism.
func TestSmokeDeterminism(t *testing.T) {
	seeds := []uint64{1, 2, 3, 0xC0FFEE, 0xDEADBEEF}
	run := func(seed uint64) (string, int) {
		s, err := New(smokeConfig(seed))
		if err != nil {
			t.Fatalf("seed 0x%x: New: %v", seed, err)
		}
		if err := s.RunUntil(5 * Second); err != nil {
			t.Fatalf("seed 0x%x: run: %v", seed, err)
		}
		return s.TraceHash(), len(s.History().Operations())
	}
	for _, seed := range seeds {
		h1, n1 := run(seed)
		h2, n2 := run(seed)
		if h1 != h2 {
			t.Fatalf("seed 0x%x: trace hashes differ across identical runs:\n  %s\n  %s", seed, h1, h2)
		}
		if n1 != n2 {
			t.Fatalf("seed 0x%x: completed-op counts differ across identical runs: %d vs %d", seed, n1, n2)
		}
		if n1 == 0 {
			t.Fatalf("seed 0x%x: no client operations completed (would make the gate vacuous)", seed)
		}
		t.Logf("seed 0x%x: deterministic (hash %s..., %d ops)", seed, h1[:12], n1)
	}
}
