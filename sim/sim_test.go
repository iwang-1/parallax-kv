package sim

import (
	"testing"

	"github.com/iwang-1/parallax-kv/raft"
)

// baseConfig is a small, well-formed simulation config used across the
// harness tests. It runs a 3-node cluster with a modestly faulty network
// and a mixed write/read workload.
func baseConfig(seed uint64) Config {
	return Config{
		Seed:           seed,
		Nodes:          3,
		ElectionTicks:  10,
		HeartbeatTicks: 1,
		TickEvery:      10 * Millisecond,
		Net: NetworkConfig{
			DelayMin: 1 * Millisecond,
			DelayMax: 5 * Millisecond,
			DropRate: 0.05,
			DupRate:  0.05,
		},
		Workload: WorkloadConfig{
			Clients:     4,
			Keys:        3,
			ThinkMin:    1 * Millisecond,
			ThinkMax:    20 * Millisecond,
			PutRatio:    0.5,
			DeleteRatio: 0.1,
			CasRatio:    0.2,
		},
	}
}

// newMockSim builds a simulator driven by the mock echo node so the harness
// is exercised without the real consensus core.
func newMockSim(t *testing.T, cfg Config) *Simulator {
	t.Helper()
	s, err := newWith(cfg, newMockNode, defaultStorageFactory)
	if err != nil {
		t.Fatalf("newWith: %v", err)
	}
	return s
}

// TestDeterministicTraceHash is the determinism gate at the unit level: two
// runs of the identical Config must produce byte-identical traces and
// therefore identical trace hashes. This is the property CI enforces across
// reruns; catching it here means a stray nondeterminism source fails fast.
func TestDeterministicTraceHash(t *testing.T) {
	const seed = 0xC0FFEE
	run := func() (string, int) {
		s := newMockSim(t, baseConfig(seed))
		if err := s.RunUntil(2 * Second); err != nil {
			t.Fatalf("run: %v", err)
		}
		return s.TraceHash(), len(s.Trace())
	}
	h1, n1 := run()
	h2, n2 := run()
	if h1 != h2 {
		t.Fatalf("trace hashes differ across identical runs:\n  %s\n  %s", h1, h2)
	}
	if n1 != n2 {
		t.Fatalf("trace lengths differ across identical runs: %d vs %d", n1, n2)
	}
	if n1 == 0 {
		t.Fatal("empty trace: the run produced no events")
	}
}

// TestDifferentSeedsDiverge confirms the seed actually drives the run:
// different seeds should (with overwhelming probability) yield different
// traces. If they matched, the RNG would not be wired through the harness.
func TestDifferentSeedsDiverge(t *testing.T) {
	a := newMockSim(t, baseConfig(1))
	b := newMockSim(t, baseConfig(2))
	if err := a.RunUntil(2 * Second); err != nil {
		t.Fatalf("run a: %v", err)
	}
	if err := b.RunUntil(2 * Second); err != nil {
		t.Fatalf("run b: %v", err)
	}
	if a.TraceHash() == b.TraceHash() {
		t.Fatal("distinct seeds produced identical traces; RNG is not driving the run")
	}
}

// TestReplayFromSameSeed models the replay workflow: capture the trace of a
// run, then re-run from the same seed and confirm the trace reproduces
// event-for-event (not just by hash).
func TestReplayFromSameSeed(t *testing.T) {
	const seed = 0xABCDEF
	first := newMockSim(t, baseConfig(seed))
	if err := first.RunUntil(1 * Second); err != nil {
		t.Fatalf("first run: %v", err)
	}
	want := first.Trace()

	replay := newMockSim(t, baseConfig(seed))
	if err := replay.RunUntil(1 * Second); err != nil {
		t.Fatalf("replay: %v", err)
	}
	got := replay.Trace()

	if len(want) != len(got) {
		t.Fatalf("replay event count %d != original %d", len(got), len(want))
	}
	for i := range want {
		if want[i] != got[i] {
			t.Fatalf("replay diverged at event %d:\n  want %s\n  got  %s", i, want[i], got[i])
		}
	}
}

// TestNoInvariantViolationsUnderFaults runs the faulty-network workload and
// asserts the harness reports no invariant violation. The mock replicator is
// safe by construction, so any violation here is a harness bug (e.g. the
// applied-prefix checker misreading state).
func TestNoInvariantViolationsUnderFaults(t *testing.T) {
	for seed := uint64(0); seed < 20; seed++ {
		s := newMockSim(t, baseConfig(seed))
		if err := s.RunUntil(3 * Second); err != nil {
			t.Fatalf("seed 0x%x: unexpected invariant failure: %v", seed, err)
		}
	}
}

// TestProgressUnderFaults asserts the workload actually makes progress:
// across the run, clients complete operations. Without progress the other
// tests could pass vacuously.
func TestProgressUnderFaults(t *testing.T) {
	s := newMockSim(t, baseConfig(7))
	if err := s.RunUntil(3 * Second); err != nil {
		t.Fatalf("run: %v", err)
	}
	completed := len(s.History().Operations())
	if completed == 0 {
		t.Fatal("no client operations completed; workload made no progress")
	}
	t.Logf("completed %d client operations", completed)
}

// TestValidateConfig checks the Config validation rejects malformed configs.
func TestValidateConfig(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*Config)
	}{
		{"zero nodes", func(c *Config) { c.Nodes = 0 }},
		{"election<=heartbeat", func(c *Config) { c.ElectionTicks = c.HeartbeatTicks }},
		{"zero heartbeat", func(c *Config) { c.HeartbeatTicks = 0 }},
		{"zero tick", func(c *Config) { c.TickEvery = 0 }},
		{"bad drop", func(c *Config) { c.Net.DropRate = 1.5 }},
		{"bad dup", func(c *Config) { c.Net.DupRate = -0.1 }},
		{"ratios>1", func(c *Config) { c.Workload.PutRatio = 0.8; c.Workload.DeleteRatio = 0.8 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := baseConfig(1)
			tc.mut(&cfg)
			if _, err := New(cfg); err == nil {
				t.Fatal("expected validation error, got nil")
			}
		})
	}
}

// TestValidConfigConstructs confirms New accepts a well-formed config with
// the real core (construction only, no run — the real core's behavior is
// stage S2).
func TestValidConfigConstructs(t *testing.T) {
	if _, err := New(baseConfig(1)); err != nil {
		t.Fatalf("New with valid config: %v", err)
	}
	// State should be observable on a freshly built node.
	s, _ := New(baseConfig(1))
	if got := s.Now(); got != 0 {
		t.Fatalf("fresh simulator Now() = %d, want 0", got)
	}
	_ = raft.StateFollower
}
