// Package raft implements the Raft consensus algorithm as a pure deterministic state machine.
package raft

// Node is a single Raft peer. It is not safe for concurrent use; the driver serializes all calls (the
// simulator is single-goroutine, the production server owns a single drive loop).
type Node struct {
	r *raft

	// Fields captured by the most recent Ready, cleared by Advance so the
	// core knows exactly which prefix the driver has committed to persisting.
	pendingReady *Ready
}

// NewNode creates a Node, recovering term, vote, commit, and log position from storage. It returns an error
// for an invalid Config or a storage read failure.
func NewNode(cfg Config, storage LogStorage) (*Node, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if storage == nil {
		panic("raft: NewNode called with nil storage")
	}
	r, err := newRaft(cfg, storage)
	if err != nil {
		return nil, err
	}
	return &Node{r: r}, nil
}

// Tick advances the node's logical clock by one tick, driving the election timer (followers/candidates) and
// the heartbeat timer (leaders).
func (n *Node) Tick() { n.r.tick() }

// Step feeds one input message into the state machine: a peer RPC (request or response), a local
// MsgPropose, or a local MsgReadIndex.
func (n *Node) Step(m Message) error { return n.r.Step(m) }

// HasReady reports whether a Ready batch is pending. Drivers poll it after
// every Tick/Step (this core is poll-based by design — no channels).
func (n *Node) HasReady() bool {
	if n.pendingReady != nil {
		return false
	}
	r := n.r
	if len(r.msgs) > 0 || len(r.readStates) > 0 {
		return true
	}
	if len(r.raftLog.unstable.entries) > 0 || r.raftLog.unstable.snapshot != nil {
		return true
	}
	if r.raftLog.hasNextCommittedEnts() {
		return true
	}
	if r.hardState() != r.prevHardSt {
		return true
	}
	return false
}

// Ready returns the pending output batch. It must not be called again
// until the previous batch is acknowledged via Advance.
func (n *Node) Ready() Ready {
	r := n.r
	rd := Ready{
		Messages:         r.msgs,
		CommittedEntries: r.raftLog.nextCommittedEnts(),
		ReadStates:       r.readStates,
	}
	if ents := r.raftLog.unstable.entries; len(ents) > 0 {
		rd.Entries = append([]Entry(nil), ents...)
	}
	if snap := r.raftLog.unstable.snapshot; snap != nil {
		s := *snap
		rd.Snapshot = &s
	}
	if hs := r.hardState(); hs != r.prevHardSt {
		h := hs
		rd.HardState = &h
	}
	// An fsync is required before sending when term/vote changed or new
	// entries/snapshot must be durable; a commit-index-only change does not.
	rd.MustSync = len(rd.Entries) > 0 || rd.Snapshot != nil ||
		(rd.HardState != nil && (rd.HardState.Term != r.prevHardSt.Term || rd.HardState.Vote != r.prevHardSt.Vote))

	n.pendingReady = &rd
	return rd
}

// Advance acknowledges the last Ready batch: state persisted, messages handed off, committed entries
// applied.
func (n *Node) Advance(ack PersistAck) {
	rd := n.pendingReady
	if rd == nil {
		return
	}
	r := n.r
	if rd.HardState != nil {
		r.prevHardSt = *rd.HardState
	}
	if len(rd.Entries) > 0 {
		last := rd.Entries[len(rd.Entries)-1]
		r.raftLog.stableTo(last.Index, last.Term)
	}
	if rd.Snapshot != nil {
		r.raftLog.stableSnapTo(rd.Snapshot.Metadata.Index)
	}
	if len(rd.CommittedEntries) > 0 {
		last := rd.CommittedEntries[len(rd.CommittedEntries)-1]
		r.raftLog.appliedTo(last.Index)
	}
	// Messages and read states have been consumed.
	r.msgs = nil
	r.readStates = nil
	n.pendingReady = nil
}

// State returns the node's current role (for tests, invariant checkers,
// and metrics; not part of the consensus contract).
func (n *Node) State() StateType { return n.r.state }

// Leader returns the node ID this node believes is the current leader,
// or 0 if unknown (used for client redirect).
func (n *Node) Leader() uint64 { return n.r.lead }

// Term returns the node's current term (for tests and invariant checkers).
func (n *Node) Term() uint64 { return n.r.Term }
