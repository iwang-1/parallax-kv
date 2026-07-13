package sim

import (
	"fmt"

	"github.com/iwang-1/parallax-kv/kv"
	"github.com/iwang-1/parallax-kv/raft"
)

// This file holds the core drive loop the simulator runs for each node:
// constructing the node, ticking it, draining Ready batches (persist ->
// send -> apply -> Advance), and delivering messages. Every effect is
// scheduled through the one event queue so ordering is fully determined by
// (virtual time, sequence).

// startNode (re)constructs the volatile half of a node from its durable
// storage. It is called on initial build and on Restart; the storage is
// created once (in newWith) and reused so persisted state survives crashes.
func (s *Simulator) startNode(ns *nodeState) error {
	cfg := raft.Config{
		ID:             ns.id,
		Peers:          append([]uint64(nil), s.peers...),
		ElectionTicks:  s.cfg.ElectionTicks,
		HeartbeatTicks: s.cfg.HeartbeatTicks,
		PreVote:        true,
		Rand:           s.rng,
	}
	n, err := s.newNode(cfg, ns.storage)
	if err != nil {
		return err
	}
	ns.node = n
	ns.sm = kv.NewStateMachine()
	ns.applied = 0
	ns.appliedLog = nil
	// Recover applied state from any persisted snapshot so a restarted node
	// resumes from its compacted prefix rather than index 0.
	if snap, err := ns.storage.Snapshot(); err == nil && snap.Metadata.Index > 0 {
		if len(snap.Data) > 0 {
			if err := ns.sm.Restore(snap.Data); err != nil {
				return fmt.Errorf("restore snapshot: %w", err)
			}
		}
		ns.applied = snap.Metadata.Index
	}
	return nil
}

// nextSeq returns the next event sequence number (the tie-breaker that
// makes same-time events dispatch in insertion order).
func (s *Simulator) nextSeq() uint64 {
	s.seq++
	return s.seq
}

// schedule pushes an action onto the queue at time s.now+after.
func (s *Simulator) schedule(after VirtualTime, kind string, node uint64, detail string, action func()) {
	s.queue.push(&event{
		at:     s.now + after,
		seq:    s.nextSeq(),
		kind:   kind,
		node:   node,
		detail: detail,
		action: action,
	})
}

// scheduleTick schedules the next logical tick for a node. Ticks re-arm
// themselves for as long as the node is up; a crashed node's tick is a
// no-op and does not re-arm (Restart re-arms it).
func (s *Simulator) scheduleTick(id uint64, after VirtualTime) {
	s.schedule(after, "tick", id, "", func() {
		ns := s.nodes[id]
		if ns == nil || ns.crashed || ns.node == nil {
			return
		}
		ns.node.Tick()
		s.drain(ns)
		s.scheduleTick(id, s.cfg.TickEvery)
	})
}

// step feeds a message into a live node and drains the resulting outputs.
func (s *Simulator) step(ns *nodeState, m raft.Message) {
	if ns.crashed || ns.node == nil {
		return
	}
	// Proposals/reads into a non-leader return ErrNotLeader; that is a
	// normal control-flow signal for the client layer, not a fault.
	_ = ns.node.Step(m)
	s.drain(ns)
}

// drain processes all pending Ready batches for a node, enforcing the
// persist-before-send ordering the core's contract requires:
//
//  1. persist HardState / Entries / Snapshot durably (here: into mem
//     storage, which models durability across crash/restart),
//  2. send Messages (scheduled onto the network),
//  3. apply Snapshot then CommittedEntries to the state machine and
//     release ReadStates,
//  4. Advance.
func (s *Simulator) drain(ns *nodeState) {
	if ns.crashed || ns.node == nil {
		return
	}
	for ns.node.HasReady() {
		rd := ns.node.Ready()

		// (1) Persist. Order matters: snapshot compacts the log prefix,
		// then new entries extend it, then the hard state records
		// term/vote/commit.
		if rd.Snapshot != nil {
			if err := ns.storage.ApplySnapshot(*rd.Snapshot); err != nil {
				s.fail(fmt.Errorf("node %d ApplySnapshot: %w", ns.id, err))
				return
			}
		}
		if len(rd.Entries) > 0 {
			if err := ns.storage.AppendEntries(rd.Entries); err != nil {
				s.fail(fmt.Errorf("node %d AppendEntries: %w", ns.id, err))
				return
			}
		}
		if rd.HardState != nil {
			if err := ns.storage.SetHardState(*rd.HardState); err != nil {
				s.fail(fmt.Errorf("node %d SetHardState: %w", ns.id, err))
				return
			}
		}

		// (2) Send. Persistence above is durable before any message that
		// depends on it leaves the node.
		for _, m := range rd.Messages {
			s.sendMessage(ns.id, m)
		}

		// (3) Apply snapshot, then committed entries, then release reads.
		if rd.Snapshot != nil {
			if len(rd.Snapshot.Data) > 0 {
				if err := ns.sm.Restore(rd.Snapshot.Data); err != nil {
					s.fail(fmt.Errorf("node %d restore snapshot: %w", ns.id, err))
					return
				}
			}
			if rd.Snapshot.Metadata.Index > ns.applied {
				ns.applied = rd.Snapshot.Metadata.Index
			}
		}
		for _, e := range rd.CommittedEntries {
			s.applyEntry(ns, e)
		}
		for _, rs := range rd.ReadStates {
			s.releaseRead(ns, rs)
		}

		// (4) Acknowledge.
		ns.node.Advance(raft.PersistAck{})

		// (5) Compact: once enough entries have been applied past the last
		// snapshot, snapshot the state machine and truncate the covered log
		// prefix. The core reads the log through LogStorage and tolerates a
		// compacted prefix (falling back to InstallSnapshot for followers that
		// need it), so this is purely a driver-side operation.
		s.maybeCompact(ns)
	}
}

// maybeCompact snapshots ns's state machine and truncates its log prefix once
// the applied index has advanced Config.SnapshotEntries past the last
// snapshot. It is deterministic (draws no randomness) and models a node
// compacting its own durable log to bound its size. Compaction is best-effort:
// a benign ErrCompacted race (a newer snapshot already installed) is ignored.
func (s *Simulator) maybeCompact(ns *nodeState) {
	if s.cfg.SnapshotEntries == 0 || ns.crashed || ns.sm == nil {
		return
	}
	snap, err := ns.storage.Snapshot()
	if err != nil {
		s.fail(fmt.Errorf("node %d Snapshot(): %w", ns.id, err))
		return
	}
	base := snap.Metadata.Index
	if ns.applied <= base || ns.applied-base < s.cfg.SnapshotEntries {
		return
	}
	// The applied index is committed and durable, so its term is available
	// from storage (either as a live entry or the current snapshot boundary).
	term, err := ns.storage.Term(ns.applied)
	if err != nil {
		// Applied entry not individually available yet (e.g. just covered by a
		// freshly installed snapshot); skip this round.
		return
	}
	data, err := ns.sm.Snapshot()
	if err != nil {
		s.fail(fmt.Errorf("node %d state-machine Snapshot(): %w", ns.id, err))
		return
	}
	newSnap := raft.Snapshot{
		Metadata: raft.SnapshotMetadata{Index: ns.applied, Term: term},
		Data:     data,
	}
	if err := ns.storage.ApplySnapshot(newSnap); err != nil && err != raft.ErrCompacted {
		s.fail(fmt.Errorf("node %d compaction ApplySnapshot: %w", ns.id, err))
		return
	}
	s.recordControl("compact", ns.id, fmt.Sprintf("index=%d term=%d", ns.applied, term))
}

// applyEntry applies one committed entry to the node's state machine and
// records it for the applied-prefix invariant. Config/no-op entries carry
// no client command and only advance the applied index.
func (s *Simulator) applyEntry(ns *nodeState, e raft.Entry) {
	if e.Index <= ns.applied {
		return // already applied (e.g. replayed after restart)
	}
	ns.applied = e.Index
	ns.appliedLog = append(ns.appliedLog, appliedRec{index: e.Index, term: e.Term, digest: fnv64(e.Data)})

	if e.Type == raft.EntryNormal && len(e.Data) > 0 {
		cmd, err := kv.DecodeCommand(e.Data)
		if err != nil {
			s.fail(fmt.Errorf("node %d decode committed command at %d: %w", ns.id, e.Index, err))
			return
		}
		res := ns.sm.Apply(cmd)
		// Only the leader answers clients; followers apply for state but do
		// not complete client operations. The client layer resolves
		// completion by (clientID, seq) against the leader that committed it.
		s.completeIfLeader(ns, cmd, res)
	}
	// Advancing applied may unblock reads waiting on this index.
	s.servePendingReads(ns)
}

// fail latches the first fatal simulator error, tagged with the replay seed.
func (s *Simulator) fail(err error) {
	if s.firstErr == nil {
		s.firstErr = s.withReplay(err)
	}
}
