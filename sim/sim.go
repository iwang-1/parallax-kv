// Package sim is the deterministic simulation harness: a single-goroutine
// event loop that owns N raft Nodes, their in-memory storages, a simulated
// network, a workload of virtual clients, fault injectors, invariant
// checkers, and a canonical trace recorder.
//
// Determinism contract: a whole run is a pure function of Config — in
// particular of Config.Seed. One seeded *rand.Rand (owned by the event
// loop) is the ONLY source of nondeterminism: election jitter, message
// delays, drops, duplicates, fault timing, and workload choices all draw
// from it, in event order. Events are dispatched from a priority queue
// keyed (virtual time, sequence number), so ties break deterministically.
// Any failure therefore replays bit-for-bit from its seed, and CI's
// determinism gate byte-compares trace hashes across reruns to catch a
// stray time.Now(), goroutine, or map-order dependence the moment it
// leaks in.
package sim

import (
	"fmt"
	"math/rand"

	"github.com/anishathalye/porcupine"

	"github.com/iwang-1/parallax-kv/raft"
	"github.com/iwang-1/parallax-kv/sim/lin"
)

// VirtualTime is simulated time in nanoseconds since the start of the run.
// It has no relationship to the wall clock.
type VirtualTime int64

// Common virtual durations.
const (
	Microsecond VirtualTime = 1000
	Millisecond VirtualTime = 1000 * Microsecond
	Second      VirtualTime = 1000 * Millisecond
)

// NetworkConfig shapes the simulated network. All distributions draw from
// the run's single seeded RNG.
type NetworkConfig struct {
	// DelayMin/DelayMax bound the uniform per-message delivery delay.
	// Randomized delays make message REORDERING emerge naturally.
	DelayMin VirtualTime
	DelayMax VirtualTime
	// DropRate is the probability in [0,1] that a message is lost.
	DropRate float64
	// DupRate is the probability in [0,1] that a message is delivered
	// twice (the duplicate gets its own random delay).
	DupRate float64
}

// WorkloadConfig shapes the virtual client workload.
type WorkloadConfig struct {
	// Clients is the number of closed-loop virtual clients.
	Clients int
	// Keys is the size of the key space (small = more contention).
	Keys int
	// ThinkMin/ThinkMax bound the uniform think time between a client's
	// operations.
	ThinkMin VirtualTime
	ThinkMax VirtualTime
	// PutRatio, DeleteRatio, and CasRatio select the operation mix; the
	// remainder (1 - sum) is Gets. Each must be in [0,1], sum <= 1.
	PutRatio    float64
	DeleteRatio float64
	CasRatio    float64
}

// Config fully specifies a simulation run.
type Config struct {
	// Seed is the run's identity: same Config (including Seed), same run,
	// byte-for-byte.
	Seed uint64
	// Nodes is the cluster size (IDs 1..Nodes).
	Nodes int
	// ElectionTicks/HeartbeatTicks are passed to each node's raft.Config.
	ElectionTicks  int
	HeartbeatTicks int
	// TickEvery is the virtual duration between raft Ticks.
	TickEvery VirtualTime
	// SnapshotEntries triggers log compaction: once a node's applied index
	// has advanced this many entries beyond its last snapshot, the driver
	// snapshots the state machine, persists it, and truncates the covered log
	// prefix. Zero disables compaction. The trigger draws no randomness, so a
	// run stays a pure function of Seed.
	SnapshotEntries uint64
	Net             NetworkConfig
	Workload        WorkloadConfig
}

// Verdict is a network hook's decision about one message.
type Verdict struct {
	// Drop discards the message entirely.
	Drop bool
	// Duplicate schedules a second delivery.
	Duplicate bool
	// ExtraDelay is added to the message's randomly drawn delay.
	ExtraDelay VirtualTime
}

// MessageHook lets a scenario intercept every in-flight message (after the
// built-in drop/dup/delay distributions). Hooks MUST be deterministic:
// they may inspect the message and use state derived from the run, but
// never the wall clock or their own RNG.
type MessageHook func(from, to uint64, m raft.Message) Verdict

// TraceEvent is one canonically serialized event in the run's trace. The
// trace (and its hash) is the determinism-gate artifact.
type TraceEvent struct {
	At   VirtualTime
	Seq  uint64
	Kind string
	Node uint64
	// Detail is a canonical, deterministic rendering of the event
	// payload (no pointers, no map order, no wall-clock timestamps).
	Detail string
}

// Simulator runs one deterministic simulation. Not safe for concurrent
// use, by design: everything happens on the caller's goroutine.
type Simulator struct {
	cfg   Config
	rng   *rand.Rand
	now   VirtualTime
	seq   uint64 // monotonic event sequence counter (tie-breaker)
	queue eventQueue

	// scenario is the nemesis scenario name (empty for a plain New run); it
	// is threaded into the REPLAY hint so a failing run reproduces exactly.
	scenario string

	peers []uint64
	nodes map[uint64]*nodeState

	net      *network
	rec      *recorder
	hist     *lin.History
	clients  []*client
	checker  *invariants
	firstErr error

	// factories are injectable so harness tests can substitute a mock node
	// and/or storage without the real consensus core.
	newNode    nodeFactory
	newStorage storageFactory
}

// New validates cfg and builds the cluster: nodes, mem storages, network,
// workload clients, trace recorder, and invariant checkers. It uses the
// real raft core and in-memory storage; tests that need a mock node build
// the Simulator through newWith.
func New(cfg Config) (*Simulator, error) {
	return newWith(cfg, defaultNodeFactory, defaultStorageFactory)
}

// newWith is the construction seam shared by New and the harness tests. The
// node and storage factories are injected so the event loop, network, fault
// injectors, workload, trace, and invariant machinery can be exercised
// against a trivial mock node (stage S1) and later the real core (S2).
func newWith(cfg Config, nf nodeFactory, sf storageFactory) (*Simulator, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	s := &Simulator{
		cfg:        cfg,
		rng:        rand.New(rand.NewSource(int64(cfg.Seed))),
		nodes:      make(map[uint64]*nodeState, cfg.Nodes),
		rec:        newRecorder(),
		hist:       lin.NewHistory(),
		newNode:    nf,
		newStorage: sf,
	}
	for i := 1; i <= cfg.Nodes; i++ {
		s.peers = append(s.peers, uint64(i))
	}
	s.net = newNetwork(cfg.Net, s.rng)
	s.checker = newInvariants(s.peers)

	for _, id := range s.peers {
		ns := &nodeState{id: id, storage: sf(id)}
		if err := s.startNode(ns); err != nil {
			return nil, fmt.Errorf("sim: starting node %d: %w", id, err)
		}
		s.nodes[id] = ns
	}

	// Schedule the first tick for every node and spin up the workload.
	for _, id := range s.peers {
		s.scheduleTick(id, cfg.TickEvery)
	}
	s.startWorkload()
	return s, nil
}

// validate checks a sim Config.
func (c *Config) validate() error {
	switch {
	case c.Nodes <= 0:
		return fmt.Errorf("sim: Nodes must be positive, got %d", c.Nodes)
	case c.ElectionTicks <= c.HeartbeatTicks:
		return fmt.Errorf("sim: ElectionTicks (%d) must exceed HeartbeatTicks (%d)", c.ElectionTicks, c.HeartbeatTicks)
	case c.HeartbeatTicks <= 0:
		return fmt.Errorf("sim: HeartbeatTicks must be positive, got %d", c.HeartbeatTicks)
	case c.TickEvery <= 0:
		return fmt.Errorf("sim: TickEvery must be positive, got %d", c.TickEvery)
	}
	if c.Net.DropRate < 0 || c.Net.DropRate > 1 {
		return fmt.Errorf("sim: Net.DropRate must be in [0,1], got %v", c.Net.DropRate)
	}
	if c.Net.DupRate < 0 || c.Net.DupRate > 1 {
		return fmt.Errorf("sim: Net.DupRate must be in [0,1], got %v", c.Net.DupRate)
	}
	if r := c.Workload.PutRatio + c.Workload.DeleteRatio + c.Workload.CasRatio; r < 0 || r > 1 {
		return fmt.Errorf("sim: workload op ratios must sum to <= 1, got %v", r)
	}
	return nil
}

// Now returns the current virtual time.
func (s *Simulator) Now() VirtualTime { return s.now }

// Step dispatches the next event from the queue and advances virtual time
// to its firing time. It returns false when no events remain. Invariants
// are checked after every step; the first violation is latched and
// retrievable via Err (and returned by RunUntil).
func (s *Simulator) Step() bool {
	e := s.queue.pop()
	if e == nil {
		return false
	}
	// Virtual time is monotonic: events never fire before the current time
	// because the queue is ordered by (at, seq) and every scheduled event
	// is at >= now.
	s.now = e.at
	if e.detail != "" || e.kind != "" {
		s.rec.record(TraceEvent{At: e.at, Seq: e.seq, Kind: e.kind, Node: e.node, Detail: e.detail})
	}
	e.action()
	if s.firstErr == nil {
		if err := s.checker.check(s.now, s.nodes); err != nil {
			s.firstErr = s.withReplay(err)
		}
	}
	return true
}

// RunUntil steps the simulation until virtual time t (inclusive) or event
// exhaustion, returning the first invariant violation observed. The error
// message includes the replay seed so any failure can be re-run.
func (s *Simulator) RunUntil(t VirtualTime) error {
	for s.firstErr == nil {
		next := s.queue.peek()
		if next == nil || next.at > t {
			break
		}
		s.Step()
	}
	return s.firstErr
}

// Err returns the first invariant violation observed so far (nil if none).
func (s *Simulator) Err() error { return s.firstErr }

// withReplay wraps an error with the exact command needed to reproduce it.
// When the run belongs to a named nemesis scenario, the hint names the
// scenario and seed so it maps directly onto TestScenarioReplay, the
// single-run replay harness.
func (s *Simulator) withReplay(err error) error {
	if s.scenario != "" {
		return fmt.Errorf("%w\nREPLAY: scenario=%s seed=0x%x "+
			"(go test ./sim -run TestScenarioReplay -scenario=%s -seed=0x%x)",
			err, s.scenario, s.cfg.Seed, s.scenario, s.cfg.Seed)
	}
	return fmt.Errorf("%w\nREPLAY: seed=0x%x (go test ./sim -run TestScenarioReplay -seed=0x%x)", err, s.cfg.Seed, s.cfg.Seed)
}

// Partition splits the network into the given groups; messages between
// groups are dropped until Heal. Nodes not listed form an implicit final
// group. The partition takes effect immediately and is recorded in the
// trace (topology changes are part of the deterministic run).
func (s *Simulator) Partition(groups ...[]uint64) {
	s.net.setPartition(groups)
	s.recordControl("partition", 0, renderGroups(groups))
}

// Heal removes all partitions.
func (s *Simulator) Heal() {
	s.net.heal()
	s.recordControl("heal", 0, "")
}

// Crash stops node id, discarding ALL volatile state (the raft node, its
// in-flight messages) while keeping state persisted through LogStorage —
// exactly what a power failure keeps. In-flight messages already scheduled
// for delivery to a crashed node are dropped at delivery time.
func (s *Simulator) Crash(id uint64) {
	ns, ok := s.nodes[id]
	if !ok || ns.crashed {
		return
	}
	ns.crashed = true
	ns.node = nil
	ns.sm = nil
	ns.applied = 0
	ns.appliedLog = nil
	s.recordControl("crash", id, "")
}

// Restart rebuilds node id from its persisted state and rejoins it: a fresh
// raft node recovers term/vote/log from the durable storage that survived
// the crash, and its tick timer is rescheduled.
func (s *Simulator) Restart(id uint64) {
	ns, ok := s.nodes[id]
	if !ok || !ns.crashed {
		return
	}
	ns.crashed = false
	if err := s.startNode(ns); err != nil {
		s.firstErr = s.withReplay(fmt.Errorf("sim: restart node %d: %w", id, err))
		return
	}
	s.recordControl("restart", id, "")
	s.scheduleTick(id, s.cfg.TickEvery)
	s.drain(ns)
}

// SetMessageHook installs h as the scenario-level message interceptor
// (nil to remove). Hooks run after the built-in drop/dup/delay
// distributions and must be deterministic.
func (s *Simulator) SetMessageHook(h MessageHook) { s.net.hook = h }

// Trace returns the recorded events so far.
func (s *Simulator) Trace() []TraceEvent { return s.rec.snapshot() }

// TraceHash returns the lowercase-hex SHA-256 of the canonically
// serialized trace — equal Configs MUST yield equal hashes, and CI
// enforces it.
func (s *Simulator) TraceHash() string { return s.rec.hash() }

// History returns the client operation history for linearizability
// checking (invoke/return pairs stamped with virtual times).
func (s *Simulator) History() *lin.History { return s.hist }

// CheckLinearizability runs Porcupine over the recorded client history and
// reports whether it admits a valid linearization of the KV state machine.
// It is the end-of-run consistency assertion every real-core run makes: the
// per-step invariants prove the replicas agree on an applied log, and this
// proves the client-observed results are consistent with SOME single-copy
// serial execution — the actual definition of linearizability.
//
// It returns nil when a linearization exists (porcupine.Ok) or when the
// bounded search is inconclusive (porcupine.Unknown — a timeout, not a
// violation). It returns a replay-tagged error only on porcupine.Illegal: a
// genuine consistency bug, for which no linearization exists. On a violation
// the error carries the REPLAY command so the failing run reproduces exactly.
func (s *Simulator) CheckLinearizability() error {
	res, _ := lin.Check(s.hist)
	if res == porcupine.Illegal {
		return s.withReplay(fmt.Errorf("linearizability violated: client history admits no valid linearization of the kv state machine"))
	}
	return nil
}

// recordControl records a control-plane event (fault injection) into the
// trace at the current virtual time.
func (s *Simulator) recordControl(kind string, node uint64, detail string) {
	s.seq++
	s.rec.record(TraceEvent{At: s.now, Seq: s.seq, Kind: kind, Node: node, Detail: detail})
}
