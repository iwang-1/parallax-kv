package sim

import (
	"flag"
	"testing"
)

// The nemesis soak: each named scenario is run across many seeds under both
// the per-step safety invariants (election safety, log matching, leader
// completeness, applied-prefix agreement) and the end-of-run Porcupine
// linearizability check. A single seed's run is fully reproducible via the
// -scenario / -seed flags (TestScenarioReplay), which is exactly the command
// the REPLAY line of any failure prints.

// Replay flags. These let a committed regression seed (or a fresh soak
// failure) be re-run in isolation:
//
//	go test ./sim -run TestScenarioReplay -scenario=mixed-chaos -seed=0x1234
var (
	flagScenario = flag.String("scenario", "", "nemesis scenario name for TestScenarioReplay")
	flagSeed     = flag.Uint64("seed", 0, "seed for TestScenarioReplay")
	// Soak range: TestScenarioSoak runs every scenario over seeds
	// [soak-lo, soak-hi). Left as an empty range by default so the normal
	// suite runs only the fast fixed regression corpus; CI and the local soak
	// widen it, e.g. -soak-lo=0 -soak-hi=200.
	flagSoakLo = flag.Uint64("soak-lo", 0, "inclusive low seed for TestScenarioSoak")
	flagSoakHi = flag.Uint64("soak-hi", 0, "exclusive high seed for TestScenarioSoak")
)

// regressionSeeds are the fixed seeds every normal test run soaks across all
// scenarios. New organic-failure seeds get appended here so a fixed bug can
// never silently regress. They are cheap enough (a handful x six scenarios) to
// belong in the default suite; the big fresh-seed soak is opt-in via -soak-*.
var regressionSeeds = []uint64{
	0x1, 0x2, 0x3, 0xC0FFEE, 0xDEADBEEF,
}

// scenarioRunTime is how long (virtual) each scenario run is driven. Long
// enough for many fault cycles and hundreds of client ops, short enough that a
// 25-seed batch stays well under the turn budget.
const scenarioRunTime = 8 * Second

// runScenario builds and drives one scenario/seed, then asserts both layers of
// correctness: no per-step invariant violation (returned by RunUntil) and a
// linearizable client history (CheckLinearizability). It returns the number of
// completed client operations so callers can guard against vacuous runs.
func runScenario(t *testing.T, name string, seed uint64) int {
	t.Helper()
	s, err := NewScenario(name, seed)
	if err != nil {
		t.Fatalf("scenario %s seed 0x%x: build: %v", name, seed, err)
	}
	if err := s.RunUntil(scenarioRunTime); err != nil {
		t.Fatalf("scenario %s seed 0x%x: invariant violation: %v", name, seed, err)
	}
	if err := s.CheckLinearizability(); err != nil {
		t.Fatalf("scenario %s seed 0x%x: %v", name, seed, err)
	}
	return len(s.History().Operations())
}

// TestScenarioSmoke runs every scenario once (seed 1) and asserts each one
// makes real progress: a scenario that never lets the workload complete an op
// would make the soak's linearizability check vacuous, so we fail loudly here.
func TestScenarioSmoke(t *testing.T) {
	for _, name := range ScenarioNames {
		name := name
		t.Run(name, func(t *testing.T) {
			ops := runScenario(t, name, 1)
			if ops == 0 {
				t.Fatalf("scenario %s completed zero client ops; the run is vacuous "+
					"(faults are starving the workload of a quorum)", name)
			}
			t.Logf("scenario %s: %d client ops completed, linearizable", name, ops)
		})
	}
}

// TestScenarioRegression soaks every scenario across the committed fixed
// regression corpus. It is part of the default suite: any bug a fresh soak
// finds gets its seed appended to regressionSeeds, so the fix is guarded
// forever. A run that completes no ops is a vacuous guard and fails.
func TestScenarioRegression(t *testing.T) {
	for _, name := range ScenarioNames {
		for _, seed := range regressionSeeds {
			if ops := runScenario(t, name, seed); ops == 0 {
				t.Fatalf("scenario %s seed 0x%x: zero ops (vacuous regression guard)", name, seed)
			}
		}
	}
}

// TestScenarioSoak is the escalating fresh-seed soak. It is a no-op unless
// -soak-hi > -soak-lo, so it does not slow the default suite; the soak driver
// invokes it in batches (e.g. 25 seeds) to stay inside the per-command budget.
// Every scenario x every seed in the range is checked for both per-step
// invariant safety and end-of-run linearizability. A failure prints the
// REPLAY line (scenario + seed) so it reproduces via TestScenarioReplay and
// the seed can be committed to regressionSeeds.
func TestScenarioSoak(t *testing.T) {
	lo, hi := *flagSoakLo, *flagSoakHi
	if hi <= lo {
		t.Skip("set -soak-lo/-soak-hi to run the fresh-seed soak")
	}
	total, ops := 0, 0
	for _, name := range ScenarioNames {
		for seed := lo; seed < hi; seed++ {
			ops += runScenario(t, name, seed)
			total++
		}
	}
	t.Logf("soak: %d scenario runs (%d scenarios x seeds [0x%x,0x%x)), %d client ops, zero violations",
		total, len(ScenarioNames), lo, hi, ops)
}

// TestScenarioDeterminism is the determinism double-run gate over the nemesis
// layer: each scenario, over the regression corpus, is run twice and must
// produce byte-identical trace hashes and equal completed-op counts. A fault
// schedule that draws timing from the seeded RNG or a checker that reads the
// wall clock would break this immediately. This is the mechanical enforcement
// that "every failure replays from its seed" is literally true even with
// partitions, crashes, and restarts woven in.
func TestScenarioDeterminism(t *testing.T) {
	run := func(name string, seed uint64) (string, int) {
		s, err := NewScenario(name, seed)
		if err != nil {
			t.Fatalf("scenario %s seed 0x%x: build: %v", name, seed, err)
		}
		if err := s.RunUntil(scenarioRunTime); err != nil {
			t.Fatalf("scenario %s seed 0x%x: invariant violation: %v", name, seed, err)
		}
		return s.TraceHash(), len(s.History().Operations())
	}
	for _, name := range ScenarioNames {
		for _, seed := range regressionSeeds {
			h1, n1 := run(name, seed)
			h2, n2 := run(name, seed)
			if h1 != h2 {
				t.Fatalf("scenario %s seed 0x%x: nondeterministic trace hash:\n  %s\n  %s", name, seed, h1, h2)
			}
			if n1 != n2 {
				t.Fatalf("scenario %s seed 0x%x: op count differs across runs: %d vs %d", name, seed, n1, n2)
			}
		}
	}
}

// TestScenarioReplay reproduces a single scenario run from -scenario/-seed.
// It is the literal command the REPLAY line prints; running it with no flags
// is a no-op skip so the normal suite is unaffected.
func TestScenarioReplay(t *testing.T) {
	if *flagScenario == "" {
		t.Skip("set -scenario and -seed to replay a specific run")
	}
	ops := runScenario(t, *flagScenario, *flagSeed)
	t.Logf("replayed scenario %s seed 0x%x: %d ops, linearizable, no invariant violation",
		*flagScenario, *flagSeed, ops)
}
