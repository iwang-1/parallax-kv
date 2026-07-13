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
	Net       NetworkConfig
	Workload  WorkloadConfig
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
	cfg Config
}

// New validates cfg and builds the cluster: nodes, mem storages, network,
// workload clients, trace recorder, and invariant checkers.
func New(cfg Config) (*Simulator, error) {
	// TODO(S1)
	panic("sim: New not implemented (stage S1)")
}

// Now returns the current virtual time.
func (s *Simulator) Now() VirtualTime {
	// TODO(S1)
	panic("sim: Now not implemented (stage S1)")
}

// Step dispatches the next event from the queue. It returns false when no
// events remain. Invariants are checked after every step; a violation is
// returned by RunUntil (or retrievable via Err after manual stepping).
func (s *Simulator) Step() bool {
	// TODO(S1)
	panic("sim: Step not implemented (stage S1)")
}

// RunUntil steps the simulation until virtual time t (or event exhaustion)
// and returns the first invariant violation, if any. The error message
// includes the replay seed.
func (s *Simulator) RunUntil(t VirtualTime) error {
	// TODO(S1)
	panic("sim: RunUntil not implemented (stage S1)")
}

// Err returns the first invariant violation observed so far (nil if none).
func (s *Simulator) Err() error {
	// TODO(S1)
	panic("sim: Err not implemented (stage S1)")
}

// Partition splits the network into the given groups; messages between
// groups are dropped. Nodes not listed form an implicit final group.
func (s *Simulator) Partition(groups ...[]uint64) {
	// TODO(S1)
	panic("sim: Partition not implemented (stage S1)")
}

// Heal removes all partitions.
func (s *Simulator) Heal() {
	// TODO(S1)
	panic("sim: Heal not implemented (stage S1)")
}

// Crash stops node id, discarding ALL volatile state (the raft.Node, its
// in-flight messages) while keeping state persisted through LogStorage —
// exactly what a power failure keeps.
func (s *Simulator) Crash(id uint64) {
	// TODO(S1)
	panic("sim: Crash not implemented (stage S1)")
}

// Restart rebuilds node id from its persisted state and rejoins it.
func (s *Simulator) Restart(id uint64) {
	// TODO(S1)
	panic("sim: Restart not implemented (stage S1)")
}

// SetMessageHook installs h as the scenario-level message interceptor
// (nil to remove).
func (s *Simulator) SetMessageHook(h MessageHook) {
	// TODO(S1)
	panic("sim: SetMessageHook not implemented (stage S1)")
}

// Trace returns the recorded events so far.
func (s *Simulator) Trace() []TraceEvent {
	// TODO(S1)
	panic("sim: Trace not implemented (stage S1)")
}

// TraceHash returns the lowercase-hex SHA-256 of the canonically
// serialized trace — equal Configs MUST yield equal hashes, and CI
// enforces it.
func (s *Simulator) TraceHash() string {
	// TODO(S1)
	panic("sim: TraceHash not implemented (stage S1)")
}

// History returns the client operation history for linearizability
// checking (invoke/return pairs stamped with virtual times).
func (s *Simulator) History() *lin.History {
	// TODO(S1)
	panic("sim: History not implemented (stage S1)")
}
