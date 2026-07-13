// Package raft implements the Raft consensus algorithm as a pure
// deterministic state machine.
//
// The core has no goroutines, no timers, no I/O, no wall-clock reads, and
// no map-iteration-order dependence; all randomness comes from the
// *rand.Rand injected via Config. A driver owns every effect:
//
//	node := raft.NewNode(cfg, storage)
//	// on timer tick:        node.Tick()
//	// on any input message: node.Step(m)
//	// then drain outputs:
//	for node.HasReady() {
//	    rd := node.Ready()
//	    // 1. persist rd.HardState / rd.Entries / rd.Snapshot (fsync if rd.MustSync)
//	    // 2. send rd.Messages
//	    // 3. apply rd.Snapshot, then rd.CommittedEntries; serve rd.ReadStates
//	    node.Advance(raft.PersistAck{})
//	}
//
// The same core runs under the deterministic simulator (sim) and the
// production runtime (server); that dual-driver design is what makes
// seed-replay of distributed failures honest.
//
// Implemented (per the Raft paper, Ongaro & Ousterhout 2014, with the
// PreVote and ReadIndex extensions from Ongaro's dissertation):
// leader election with PreVote and randomized timeouts, log replication
// with conflict backtracking, the commit rule with the Figure-8
// current-term restriction, linearizable reads via ReadIndex, and
// snapshot-based log compaction with InstallSnapshot.
package raft

// Node is a single Raft peer. It is NOT safe for concurrent use; the
// driver serializes all calls (the simulator is single-goroutine, the
// production server owns a single drive loop).
type Node struct {
	cfg     Config
	storage LogStorage

	// Volatile state, rebuilt on restart. Concrete fields are the
	// implementation's business (stage S1); only the method set below is
	// frozen.
	state StateType
}

// NewNode creates a Node, recovering term, vote, commit, and log position
// from storage. It returns an error for an invalid Config or a storage
// read failure.
func NewNode(cfg Config, storage LogStorage) (*Node, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if storage == nil {
		panic("raft: NewNode called with nil storage")
	}
	n := &Node{cfg: cfg, storage: storage, state: StateFollower}
	// TODO(S1): recover HardState and log bounds from storage.
	return n, nil
}

// Tick advances the node's logical clock by one tick, driving the election
// timer (followers/candidates) and the heartbeat timer (leaders). The
// driver defines the real-time meaning of a tick.
func (n *Node) Tick() {
	// TODO(S1): election and heartbeat timer logic.
	panic("raft: Tick not implemented (stage S1)")
}

// Step feeds one input message into the state machine: a peer RPC
// (request or response), a local MsgPropose, or a local MsgReadIndex.
// It returns ErrNotLeader for proposals and reads stepped into a
// non-leader, and ignores stale-term peer messages per the Raft rules.
func (n *Node) Step(m Message) error {
	// TODO(S1): message dispatch per type and term.
	panic("raft: Step not implemented (stage S1)")
}

// HasReady reports whether a Ready batch is pending. Drivers poll it after
// every Tick/Step (this core is poll-based by design — no channels).
func (n *Node) HasReady() bool {
	// TODO(S1)
	panic("raft: HasReady not implemented (stage S1)")
}

// Ready returns the pending output batch. It must not be called again
// until the previous batch is acknowledged via Advance.
func (n *Node) Ready() Ready {
	// TODO(S1)
	panic("raft: Ready not implemented (stage S1)")
}

// Advance acknowledges the last Ready batch: state persisted, messages
// handed off, committed entries applied. Only after Advance will further
// Ready batches be produced.
func (n *Node) Advance(ack PersistAck) {
	// TODO(S1)
	panic("raft: Advance not implemented (stage S1)")
}

// State returns the node's current role (for tests, invariant checkers,
// and metrics; not part of the consensus contract).
func (n *Node) State() StateType { return n.state }

// Leader returns the node ID this node believes is the current leader,
// or 0 if unknown (used for client redirect).
func (n *Node) Leader() uint64 {
	// TODO(S1)
	panic("raft: Leader not implemented (stage S1)")
}
