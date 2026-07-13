package sim

import (
	"fmt"
	"sort"

	"github.com/iwang-1/parallax-kv/raft"
)

// This file defines the NEMESIS scenarios: named, seed-parameterized fault
// programs that drive the simulator through realistic distributed-systems
// failure modes. A scenario has two halves:
//
//   - config: a static mutation of the base run config (network shape,
//     workload) that holds for the whole run;
//   - arm: a dynamic fault schedule installed onto a freshly built simulator,
//     which schedules partitions/crashes/heals/restarts as timeline events.
//
// Every fault event is scheduled through the one event queue and every timing
// choice draws from the run's single seeded RNG, so a scenario run is a pure
// function of (scenario name, seed): it replays bit-for-bit and its trace hash
// is stable. That is what makes the REPLAY line in a failure honest.
//
// Liveness discipline: each scenario keeps a quorum available across time
// (single crash outstanding at once with its restart pre-scheduled; only a
// minority is ever isolated) so the workload keeps making progress and the
// end-of-run linearizability check is never vacuous. The interesting stress is
// not a permanent outage but the churn: leaders deposed mid-flight, nodes
// rejoining with stale logs, messages lost/duplicated/reordered around
// commits.

// Scenario is one named nemesis program.
type Scenario struct {
	Name string
	// config mutates the base config (may be nil for "network as-is").
	config func(c *Config)
	// arm installs the dynamic fault schedule (may be nil for a purely
	// static-network scenario like message loss).
	arm func(s *Simulator)
}

// ScenarioNames lists the scenarios in a stable order.
var ScenarioNames = []string{
	"partition-leader",
	"partition-random-half",
	"crash-leader-loop",
	"30pct-loss",
	"dup+delay",
	"mixed-chaos",
	"snapshot-under-partition",
}

// scenarios is the registry keyed by name.
var scenarios = map[string]Scenario{
	"partition-leader": {
		Name:   "partition-leader",
		config: func(c *Config) {}, // clean-ish network; faults are structural
		arm:    armPartitionLeader,
	},
	"partition-random-half": {
		Name:   "partition-random-half",
		config: func(c *Config) {},
		arm:    armPartitionRandomHalf,
	},
	"crash-leader-loop": {
		Name:   "crash-leader-loop",
		config: func(c *Config) {},
		arm:    armCrashLeaderLoop,
	},
	"30pct-loss": {
		Name: "30pct-loss",
		config: func(c *Config) {
			c.Net.DropRate = 0.30
			c.Net.DupRate = 0.02
		},
		arm: nil,
	},
	"dup+delay": {
		Name: "dup+delay",
		config: func(c *Config) {
			c.Net.DropRate = 0.02
			c.Net.DupRate = 0.30
			// Widen the delay band so reordering is pervasive.
			c.Net.DelayMin = 1 * Millisecond
			c.Net.DelayMax = 30 * Millisecond
		},
		arm: nil,
	},
	"mixed-chaos": {
		Name: "mixed-chaos",
		config: func(c *Config) {
			c.Net.DropRate = 0.10
			c.Net.DupRate = 0.10
			c.Net.DelayMin = 1 * Millisecond
			c.Net.DelayMax = 20 * Millisecond
		},
		arm: armMixedChaos,
	},
	"snapshot-under-partition": {
		Name: "snapshot-under-partition",
		config: func(c *Config) {
			// Aggressive compaction so the majority discards a log prefix
			// while a minority is isolated, forcing an InstallSnapshot on heal.
			c.SnapshotEntries = 16
		},
		arm: armSnapshotUnderPartition,
	},
}

// ScenarioBaseConfig is the base run config all scenarios build on: a 3-node
// cluster, a fast lightly-perturbed network, and a small-keyspace mixed
// workload (contention is what makes linearizability checking bite).
func ScenarioBaseConfig(seed uint64) Config {
	return Config{
		Seed:           seed,
		Nodes:          3,
		ElectionTicks:  10,
		HeartbeatTicks: 1,
		TickEvery:      10 * Millisecond,
		Net: NetworkConfig{
			DelayMin: 1 * Millisecond,
			DelayMax: 5 * Millisecond,
			DropRate: 0.02,
			DupRate:  0.02,
		},
		Workload: WorkloadConfig{
			Clients:     6,
			Keys:        4,
			ThinkMin:    1 * Millisecond,
			ThinkMax:    15 * Millisecond,
			PutRatio:    0.5,
			DeleteRatio: 0.1,
			CasRatio:    0.2,
		},
	}
}

// NewScenario builds a simulator for the named scenario and seed with the
// scenario's static config applied and its dynamic fault schedule armed. It
// does not run the simulation; the caller drives it with RunUntil.
func NewScenario(name string, seed uint64) (*Simulator, error) {
	sc, ok := scenarios[name]
	if !ok {
		return nil, fmt.Errorf("sim: unknown scenario %q", name)
	}
	cfg := ScenarioBaseConfig(seed)
	if sc.config != nil {
		sc.config(&cfg)
	}
	s, err := New(cfg)
	if err != nil {
		return nil, err
	}
	s.scenario = name
	if sc.arm != nil {
		sc.arm(s)
	}
	return s, nil
}

// --- fault scheduling helpers -------------------------------------------

// scheduleFault schedules fn as a nemesis event after the given delay. The
// wrapper carries no trace detail of its own; the fault primitives fn invokes
// (Partition/Crash/Heal/Restart) record their own control events, so the trace
// shows the faults, not the scheduling scaffolding.
func (s *Simulator) scheduleFault(after VirtualTime, fn func()) {
	s.schedule(after, "", 0, "", fn)
}

// drawDur returns a duration uniform in [lo, hi], drawing from the run RNG.
// When the bounds are degenerate it returns lo and consumes no randomness.
func (s *Simulator) drawDur(lo, hi VirtualTime) VirtualTime {
	if hi <= lo {
		return lo
	}
	return lo + VirtualTime(s.rng.Int63n(int64(hi-lo)+1))
}

// liveLeader returns the id of the current live leader, or 0 if none. Iterates
// peers in ascending id order (deterministic; never a map).
func (s *Simulator) liveLeader() uint64 {
	for _, id := range s.peers {
		ns := s.nodes[id]
		if ns == nil || ns.crashed || ns.node == nil {
			continue
		}
		if ns.node.State() == raft.StateLeader {
			return id
		}
	}
	return 0
}

// aLiveFollower returns the id of some live node that is not the current
// leader, or 0 if none. Iterates peers in ascending order (deterministic).
func (s *Simulator) aLiveFollower() uint64 {
	lead := s.liveLeader()
	for _, id := range s.peers {
		ns := s.nodes[id]
		if ns == nil || ns.crashed || ns.node == nil {
			continue
		}
		if id != lead {
			return id
		}
	}
	return 0
}

// liveNodes returns the ids of all currently up nodes in ascending order.
func (s *Simulator) liveNodes() []uint64 {
	var out []uint64
	for _, id := range s.peers {
		ns := s.nodes[id]
		if ns != nil && !ns.crashed && ns.node != nil {
			out = append(out, id)
		}
	}
	return out
}

// quorum is the majority size for the cluster.
func (s *Simulator) quorum() int { return len(s.peers)/2 + 1 }

// --- scenario arm functions ---------------------------------------------

// armPartitionLeader repeatedly isolates the current leader into a singleton
// partition, holds it for a spell (a new leader must emerge in the majority
// and the deposed leader must not serve stale reads or commit), then heals and
// re-arms. Only the leader is ever isolated, so the majority always retains a
// quorum and the workload keeps progressing.
func armPartitionLeader(s *Simulator) {
	var cycle func()
	cycle = func() {
		lead := s.liveLeader()
		if lead == 0 {
			// No leader right now; try again shortly without partitioning.
			s.scheduleFault(s.drawDur(100*Millisecond, 300*Millisecond), cycle)
			return
		}
		rest := make([]uint64, 0, len(s.peers)-1)
		for _, id := range s.peers {
			if id != lead {
				rest = append(rest, id)
			}
		}
		s.Partition([]uint64{lead}, rest)
		hold := s.drawDur(400*Millisecond, 900*Millisecond)
		s.scheduleFault(hold, func() {
			s.Heal()
			gap := s.drawDur(300*Millisecond, 700*Millisecond)
			s.scheduleFault(gap, cycle)
		})
	}
	s.scheduleFault(s.drawDur(300*Millisecond, 700*Millisecond), cycle)
}

// armPartitionRandomHalf repeatedly isolates a random MINORITY subset of nodes
// from the rest, holds, heals, and re-arms. Isolating a minority (never a
// quorum) keeps the majority side serving so progress continues; the stress is
// the isolated nodes falling behind and then catching up on heal.
func armPartitionRandomHalf(s *Simulator) {
	var cycle func()
	cycle = func() {
		live := s.liveNodes()
		if len(live) < 2 {
			s.scheduleFault(s.drawDur(100*Millisecond, 300*Millisecond), cycle)
			return
		}
		// Isolate a random subset of size in [1, quorum-1] so the complement
		// keeps a majority.
		maxIso := s.quorum() - 1
		if maxIso < 1 {
			maxIso = 1
		}
		k := 1 + s.rng.Intn(maxIso)
		perm := s.rng.Perm(len(s.peers))
		iso := make([]uint64, 0, k)
		for _, idx := range perm[:k] {
			iso = append(iso, s.peers[idx])
		}
		sort.Slice(iso, func(i, j int) bool { return iso[i] < iso[j] })
		rest := make([]uint64, 0, len(s.peers)-k)
		isoSet := make(map[uint64]bool, k)
		for _, id := range iso {
			isoSet[id] = true
		}
		for _, id := range s.peers {
			if !isoSet[id] {
				rest = append(rest, id)
			}
		}
		s.Partition(iso, rest)
		hold := s.drawDur(400*Millisecond, 900*Millisecond)
		s.scheduleFault(hold, func() {
			s.Heal()
			s.scheduleFault(s.drawDur(300*Millisecond, 700*Millisecond), cycle)
		})
	}
	s.scheduleFault(s.drawDur(300*Millisecond, 700*Millisecond), cycle)
}

// armCrashLeaderLoop repeatedly crashes the current leader, holds it down for a
// downtime window, restarts it (recovering its durable state), waits a gap, and
// crashes the next leader. At most one node is ever down at a time — its
// restart is scheduled before the next crash — so a 3-node cluster always keeps
// two nodes up (a quorum). This exercises leader failover plus crash-recovery
// of durable state under a live workload.
func armCrashLeaderLoop(s *Simulator) {
	var cycle func()
	cycle = func() {
		target := s.liveLeader()
		if target == 0 {
			// No leader yet; retry without crashing so we always crash the
			// actual leader (the interesting failover case).
			s.scheduleFault(s.drawDur(100*Millisecond, 300*Millisecond), cycle)
			return
		}
		s.Crash(target)
		down := s.drawDur(300*Millisecond, 700*Millisecond)
		s.scheduleFault(down, func() {
			s.Restart(target)
			gap := s.drawDur(300*Millisecond, 700*Millisecond)
			s.scheduleFault(gap, cycle)
		})
	}
	s.scheduleFault(s.drawDur(300*Millisecond, 700*Millisecond), cycle)
}

// armMixedChaos layers everything: a lossy, duplicating, reordering network
// (from the scenario config) PLUS interleaved leader partitions and leader
// crashes. It alternates between a partition cycle and a crash cycle so that at
// most one structural fault is active at a time (still quorum-preserving), with
// the network noise running underneath continuously. This is the kitchen-sink
// regression: if any bug hides in the interaction of faults, this finds it.
func armMixedChaos(s *Simulator) {
	var partitionCycle, crashCycle func()

	partitionCycle = func() {
		lead := s.liveLeader()
		if lead == 0 {
			s.scheduleFault(s.drawDur(100*Millisecond, 300*Millisecond), partitionCycle)
			return
		}
		rest := make([]uint64, 0, len(s.peers)-1)
		for _, id := range s.peers {
			if id != lead {
				rest = append(rest, id)
			}
		}
		s.Partition([]uint64{lead}, rest)
		s.scheduleFault(s.drawDur(300*Millisecond, 700*Millisecond), func() {
			s.Heal()
			// Hand off to a crash cycle after a gap.
			s.scheduleFault(s.drawDur(300*Millisecond, 600*Millisecond), crashCycle)
		})
	}

	crashCycle = func() {
		target := s.liveLeader()
		if target == 0 {
			s.scheduleFault(s.drawDur(100*Millisecond, 300*Millisecond), crashCycle)
			return
		}
		s.Crash(target)
		s.scheduleFault(s.drawDur(300*Millisecond, 700*Millisecond), func() {
			s.Restart(target)
			// Hand back to a partition cycle after a gap.
			s.scheduleFault(s.drawDur(300*Millisecond, 600*Millisecond), partitionCycle)
		})
	}

	s.scheduleFault(s.drawDur(300*Millisecond, 700*Millisecond), partitionCycle)
}

// armSnapshotUnderPartition isolates a single follower into a minority
// partition and holds it there for a LONG window while the majority keeps
// committing and compacting (SnapshotEntries is small in this scenario's
// config). By the time the partition heals, the majority has discarded — into
// its snapshot — the very entries the isolated node still needs, so the
// rejoining node can only be caught up by an InstallSnapshot rather than an
// AppendEntries. This is the snapshot flow under a realistic fault, not a hand
// wired unit test. Only a minority is ever isolated, so the majority always
// keeps a quorum and the workload never starves. The cycle repeats, isolating
// a fresh follower each time.
func armSnapshotUnderPartition(s *Simulator) {
	var cycle func()
	cycle = func() {
		victim := s.aLiveFollower()
		if victim == 0 {
			// No follower distinct from a leader yet; retry shortly.
			s.scheduleFault(s.drawDur(100*Millisecond, 300*Millisecond), cycle)
			return
		}
		rest := make([]uint64, 0, len(s.peers)-1)
		for _, id := range s.peers {
			if id != victim {
				rest = append(rest, id)
			}
		}
		s.Partition([]uint64{victim}, rest)
		// Hold long enough for the majority to commit and compact well past the
		// isolated node's frontier (many multiples of SnapshotEntries).
		hold := s.drawDur(1500*Millisecond, 2500*Millisecond)
		s.scheduleFault(hold, func() {
			s.Heal()
			// After heal, give the InstallSnapshot catch-up time to complete
			// before isolating the next follower.
			s.scheduleFault(s.drawDur(500*Millisecond, 1000*Millisecond), cycle)
		})
	}
	s.scheduleFault(s.drawDur(300*Millisecond, 700*Millisecond), cycle)
}
