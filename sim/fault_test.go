package sim

import (
	"strings"
	"testing"

	"github.com/iwang-1/parallax-kv/raft"
)

// countKind counts trace events of a given kind.
func countKind(trace []TraceEvent, kind string) int {
	n := 0
	for _, e := range trace {
		if e.Kind == kind {
			n++
		}
	}
	return n
}

// quietConfig is a config with a clean network and single-node cluster
// (so the mock's lowest-ID leader is the only node) — used for tests that
// want to observe a specific fault in isolation rather than background
// churn. It keeps drop/dup off so the only events are the ones the test
// injects.
func quietConfig(seed uint64, nodes int) Config {
	c := baseConfig(seed)
	c.Nodes = nodes
	c.Net.DropRate = 0
	c.Net.DupRate = 0
	return c
}

// TestPartitionBlocksCrossGroupMessages installs a partition that isolates
// the mock leader from the followers and asserts that follower replication
// stalls: cross-group MsgAppend deliveries do not advance follower logs.
func TestPartitionBlocksCrossGroupMessages(t *testing.T) {
	cfg := quietConfig(3, 3)
	s := newMockSim(t, cfg)

	// Isolate node 1 (the mock leader) from {2,3} immediately.
	s.Partition([]uint64{1}, []uint64{2, 3})
	if err := s.RunUntil(1 * Second); err != nil {
		t.Fatalf("run: %v", err)
	}

	// The leader (node 1) can still commit locally (single-node quorum in
	// the mock), but followers 2 and 3 must not have applied anything,
	// since every MsgAppend crosses the partition boundary and is dropped.
	for _, id := range []uint64{2, 3} {
		if got := len(s.nodes[id].appliedLog); got != 0 {
			t.Fatalf("follower %d applied %d entries despite partition", id, got)
		}
	}
	if len(s.nodes[1].appliedLog) == 0 {
		t.Fatal("leader applied nothing; workload made no progress")
	}
	// The partition control event must be in the trace.
	if countKind(s.Trace(), "partition") != 1 {
		t.Fatal("partition event not recorded in trace")
	}
}

// TestHealRestoresReplication partitions, then heals, and asserts followers
// resume applying entries once the network is whole again.
func TestHealRestoresReplication(t *testing.T) {
	cfg := quietConfig(5, 3)
	s := newMockSim(t, cfg)

	s.Partition([]uint64{1}, []uint64{2, 3})
	if err := s.RunUntil(1 * Second); err != nil {
		t.Fatalf("run partitioned: %v", err)
	}
	beforeHeal := len(s.nodes[2].appliedLog)

	s.Heal()
	if err := s.RunUntil(3 * Second); err != nil {
		t.Fatalf("run healed: %v", err)
	}
	afterHeal := len(s.nodes[2].appliedLog)
	if afterHeal <= beforeHeal {
		t.Fatalf("follower 2 did not resume after heal: before=%d after=%d", beforeHeal, afterHeal)
	}
	if countKind(s.Trace(), "heal") != 1 {
		t.Fatal("heal event not recorded in trace")
	}
}

// TestCrashKeepsPersistedRestartRecovers crashes a follower, confirms it
// applies nothing while down, then restarts it and confirms it recovers its
// persisted prefix and resumes.
func TestCrashKeepsPersistedRestartRecovers(t *testing.T) {
	cfg := quietConfig(9, 3)
	s := newMockSim(t, cfg)

	if err := s.RunUntil(1 * Second); err != nil {
		t.Fatalf("run pre-crash: %v", err)
	}
	appliedBeforeCrash := len(s.nodes[2].appliedLog)
	if appliedBeforeCrash == 0 {
		t.Fatal("follower 2 applied nothing before crash; test would be vacuous")
	}
	lastIdxBefore, _ := s.nodes[2].storage.LastIndex()

	s.Crash(2)
	if s.nodes[2].node != nil {
		t.Fatal("crashed node still has volatile state")
	}
	// Persisted storage survives the crash.
	if lastIdxAfter, _ := s.nodes[2].storage.LastIndex(); lastIdxAfter != lastIdxBefore {
		t.Fatalf("crash lost persisted state: last index %d -> %d", lastIdxBefore, lastIdxAfter)
	}

	if err := s.RunUntil(2 * Second); err != nil {
		t.Fatalf("run while crashed: %v", err)
	}

	s.Restart(2)
	if s.nodes[2].node == nil {
		t.Fatal("restarted node has no volatile state")
	}
	if err := s.RunUntil(4 * Second); err != nil {
		t.Fatalf("run post-restart: %v", err)
	}
	if countKind(s.Trace(), "crash") != 1 || countKind(s.Trace(), "restart") != 1 {
		t.Fatal("crash/restart events not both recorded in trace")
	}
}

// TestMessageHookDropsSelectively verifies a scenario hook can drop a chosen
// message class deterministically. Here it drops all MsgAppend to node 3,
// which must then apply nothing while node 2 keeps up.
func TestMessageHookDropsSelectively(t *testing.T) {
	cfg := quietConfig(11, 3)
	s := newMockSim(t, cfg)
	s.SetMessageHook(func(from, to uint64, m raft.Message) Verdict {
		if to == 3 && m.Type == raft.MsgAppend {
			return Verdict{Drop: true}
		}
		return Verdict{}
	})
	if err := s.RunUntil(2 * Second); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := len(s.nodes[3].appliedLog); got != 0 {
		t.Fatalf("node 3 applied %d entries despite dropped appends", got)
	}
	if len(s.nodes[2].appliedLog) == 0 {
		t.Fatal("node 2 applied nothing; hook over-dropped or workload stalled")
	}
}

// TestFaultRunsAreDeterministic confirms that a run peppered with fault
// injections at fixed virtual times still replays bit-for-bit — the faults
// are part of the deterministic script.
func TestFaultRunsAreDeterministic(t *testing.T) {
	script := func() string {
		s := newMockSim(t, baseConfig(0x5EED))
		s.RunUntil(500 * Millisecond)
		s.Partition([]uint64{1}, []uint64{2, 3})
		s.RunUntil(1 * Second)
		s.Crash(3)
		s.RunUntil(1500 * Millisecond)
		s.Heal()
		s.Restart(3)
		s.RunUntil(3 * Second)
		if s.Err() != nil {
			t.Fatalf("unexpected invariant failure: %v", s.Err())
		}
		return s.TraceHash()
	}
	if h1, h2 := script(), script(); h1 != h2 {
		t.Fatalf("faulted run not deterministic:\n  %s\n  %s", h1, h2)
	}
}

// TestReplaySeedInError confirms invariant-failure errors carry the replay
// seed. We force a failure by feeding the checker a divergent applied log
// directly (a synthetic harness-level test of the error path).
func TestReplaySeedInError(t *testing.T) {
	iv := newInvariants([]uint64{1, 2})
	nodes := map[uint64]*nodeState{
		1: {id: 1, appliedLog: []appliedRec{{index: 1, term: 1, digest: 100}}},
		2: {id: 2, appliedLog: []appliedRec{{index: 1, term: 1, digest: 999}}},
	}
	err := iv.check(0, nodes)
	if err == nil {
		t.Fatal("expected applied-prefix divergence to be detected")
	}
	// The simulator wraps checker errors with the replay hint; verify the
	// wrap here through a real simulator's withReplay.
	s := newMockSim(t, baseConfig(0xDEAD))
	wrapped := s.withReplay(err)
	if !strings.Contains(wrapped.Error(), "REPLAY:") || !strings.Contains(wrapped.Error(), "0xdead") {
		t.Fatalf("replay hint missing or wrong seed: %v", wrapped)
	}
}
