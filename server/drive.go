package server

import (
	"context"
	"encoding/binary"
	"time"

	"github.com/iwang-1/parallax-kv/kv"
	"github.com/iwang-1/parallax-kv/raft"
)

// DriveLoop is the single-threaded owner of the raft node, the state machine, and all waiter bookkeeping.
func (s *Server) driveLoop(ctx context.Context) error {
	ticker := time.NewTicker(time.Duration(s.cfg.TickIntervalMillis) * time.Millisecond)
	defer ticker.Stop()

	recv := s.tr.Recv()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			s.node.Tick()
			if err := s.drain(); err != nil {
				return err
			}
		case m, ok := <-recv:
			if !ok {
				return nil
			}
			_ = s.node.Step(m)
			if !s.absorbQueued(recv) {
				return nil
			}
			if err := s.drain(); err != nil {
				return err
			}
		case req := <-s.reqCh:
			s.queueRequest(req)
			if !s.absorbQueued(recv) {
				return nil
			}
			if err := s.drain(); err != nil {
				return err
			}
		}
	}
}

// maxAbsorb bounds how many already-queued inputs one loop iteration steps into the core before draining,
// so a steady stream of input cannot postpone persistence and replies indefinitely.
const maxAbsorb = 256

// absorbQueued steps every client request and peer message that is already waiting, without blocking, so
// they reach the core before the next drain. Their entries then share one Ready: one storage append, one
// fsync, and one round of outgoing messages, instead of one of each per input. Consecutive write requests
// are proposed together as one MsgPropose, so the leader replicates them in one append per follower
// rather than one overlapping append per write. Inputs are stepped in arrival order: queued proposals are
// flushed before any peer message or read is stepped. Ordering guarantees are unchanged: drain still
// persists everything in a Ready before sending any of its messages.
// It reports false if the transport's receive channel was closed.
func (s *Server) absorbQueued(recv <-chan raft.Message) bool {
	defer s.flushProposals()
	for i := 0; i < maxAbsorb; i++ {
		select {
		case req := <-s.reqCh:
			s.queueRequest(req)
		case m, ok := <-recv:
			if !ok {
				return false
			}
			s.flushProposals()
			_ = s.node.Step(m)
		default:
			return true
		}
	}
	return true
}

// queueRequest holds a write back so it can be proposed together with the writes queued behind it; any
// other request flushes the held writes first and is then handled immediately.
func (s *Server) queueRequest(req *request) {
	if req.kind == reqPropose {
		s.pendingProposals = append(s.pendingProposals, req)
		return
	}
	s.flushProposals()
	s.handleRequest(req)
}

// flushProposals steps all held writes into the core as a single MsgPropose and registers their waiters.
// If this node is not the leader, every held write is answered with a redirect.
func (s *Server) flushProposals() {
	if len(s.pendingProposals) == 0 {
		return
	}
	reqs := s.pendingProposals
	s.pendingProposals = s.pendingProposals[:0]
	entries := make([]raft.Entry, len(reqs))
	for i, req := range reqs {
		entries[i] = raft.Entry{Type: raft.EntryNormal, Data: kv.EncodeCommand(req.cmd)}
	}
	if err := s.node.Step(raft.Message{Type: raft.MsgPropose, Entries: entries}); err != nil {
		lead := s.node.Leader()
		for _, req := range reqs {
			req.resp <- opResult{notLeader: true, leaderID: lead}
		}
		return
	}
	for _, req := range reqs {
		s.writeWaiters[clientKey{req.cmd.ClientID, req.cmd.Seq}] = req.resp
	}
}

// HandleRequest steps a client request into the core and registers its waiter. A proposal/read into a
// non-leader returns ErrNotLeader, which is answered immediately with a redirect hint.
func (s *Server) handleRequest(req *request) {
	switch req.kind {
	case reqPropose:
		if err := s.node.Step(raft.Message{
			Type:    raft.MsgPropose,
			Entries: []raft.Entry{{Type: raft.EntryNormal, Data: kv.EncodeCommand(req.cmd)}},
		}); err != nil {
			req.resp <- opResult{notLeader: true, leaderID: s.node.Leader()}
			return
		}
		s.writeWaiters[clientKey{req.cmd.ClientID, req.cmd.Seq}] = req.resp
	case reqRead:
		s.readToken++
		token := make([]byte, 8)
		binary.BigEndian.PutUint64(token, s.readToken)
		if err := s.node.Step(raft.Message{Type: raft.MsgReadIndex, Context: token}); err != nil {
			req.resp <- opResult{notLeader: true, leaderID: s.node.Leader()}
			return
		}
		s.readWaiters[string(token)] = readWaiter{key: req.key, resp: req.resp}
	}
}

// Drain processes all pending Ready batches, enforcing the persist-before- send ordering the core's
// contract requires: 1. persist HardState / Entries / Snapshot durably (fsync if MustSync), 2. send Messages, 3..
func (s *Server) drain() error {
	// A leadership change invalidates in-flight client waiters: a deposed
	// leader can neither commit its proposals nor safely serve its reads.
	s.checkLeadership()

	for s.node.HasReady() {
		rd := s.node.Ready()

		// (1) Persist. Order matters: snapshot compacts the prefix, entries extend it, hard state records
		// term/vote/commit.
		if rd.Snapshot != nil {
			if err := s.stor.ApplySnapshot(*rd.Snapshot); err != nil && err != raft.ErrCompacted {
				return err
			}
		}
		if len(rd.Entries) > 0 {
			if err := s.stor.AppendEntries(rd.Entries); err != nil {
				return err
			}
		}
		if rd.HardState != nil {
			if err := s.stor.SetHardState(*rd.HardState); err != nil {
				return err
			}
		}
		if rd.MustSync {
			if err := s.stor.Sync(); err != nil {
				return err
			}
		}

		// (2) Send. Persistence above is durable before any dependent
		// message leaves the node.
		if len(rd.Messages) > 0 {
			s.tr.Send(rd.Messages)
		}

		// (3) Apply snapshot, then committed entries, then release reads.
		if rd.Snapshot != nil {
			if len(rd.Snapshot.Data) > 0 {
				if err := s.sm.Restore(rd.Snapshot.Data); err != nil {
					return err
				}
			}
			if rd.Snapshot.Metadata.Index > s.applied {
				s.applied = rd.Snapshot.Metadata.Index
			}
		}
		for _, e := range rd.CommittedEntries {
			s.applyEntry(e)
		}
		for _, rs := range rd.ReadStates {
			s.releaseRead(rs)
		}

		// (4) Acknowledge.
		s.node.Advance(raft.PersistAck{})
	}
	return nil
}

// CheckLeadership fails all in-flight client waiters when this node stops being the leader: their proposals
// will never commit here and their reads must not be served from a deposed leader's stale state.
func (s *Server) checkLeadership() {
	isLeader := s.node.State() == raft.StateLeader
	if s.prevLeader && !isLeader {
		lead := s.node.Leader()
		for k, ch := range s.writeWaiters {
			ch <- opResult{notLeader: true, leaderID: lead}
			delete(s.writeWaiters, k)
		}
		for k, w := range s.readWaiters {
			w.resp <- opResult{notLeader: true, leaderID: lead}
			delete(s.readWaiters, k)
		}
		s.pendingReads = nil
	}
	s.prevLeader = isLeader
}

// applyEntry applies one committed entry to the state machine and completes
// the matching write waiter (if this node is the leader that proposed it).
func (s *Server) applyEntry(e raft.Entry) {
	if e.Index <= s.applied {
		return
	}
	s.applied = e.Index
	if e.Type == raft.EntryNormal && len(e.Data) > 0 {
		cmd, err := kv.DecodeCommand(e.Data)
		if err != nil {
			// A committed entry always decodes; a failure is a programming error, but panicking here would take down
			// a node recovering a valid log, so we skip it (the waiter times out and retries).
			return
		}
		res := s.sm.Apply(cmd)
		k := clientKey{cmd.ClientID, cmd.Seq}
		if ch, ok := s.writeWaiters[k]; ok {
			ch <- opResult{res: res}
			delete(s.writeWaiters, k)
		}
	}
	s.servePendingReads()
}

// releaseRead serves a confirmed read if its index has already been applied,
// otherwise buffers it until applied catches up.
func (s *Server) releaseRead(rs raft.ReadState) {
	if s.applied >= rs.Index {
		s.serveRead(rs)
		return
	}
	s.pendingReads = append(s.pendingReads, rs)
}

// servePendingReads serves buffered reads whose index applied now covers.
func (s *Server) servePendingReads() {
	if len(s.pendingReads) == 0 {
		return
	}
	kept := s.pendingReads[:0]
	for _, rs := range s.pendingReads {
		if s.applied >= rs.Index {
			s.serveRead(rs)
		} else {
			kept = append(kept, rs)
		}
	}
	s.pendingReads = kept
}

// serveRead answers a linearizable read from local state and completes the
// matching read waiter.
func (s *Server) serveRead(rs raft.ReadState) {
	w, ok := s.readWaiters[string(rs.RequestCtx)]
	if !ok {
		return
	}
	delete(s.readWaiters, string(rs.RequestCtx))
	w.resp <- opResult{res: s.sm.Read(w.key)}
}

// submit routes a client request into the drive loop and waits for the
// answer or the request context's deadline.
func (s *Server) submit(ctx context.Context, req *request) (opResult, bool) {
	req.resp = make(chan opResult, 1)
	select {
	case s.reqCh <- req:
	case <-ctx.Done():
		return opResult{}, false
	}
	select {
	case r := <-req.resp:
		return r, true
	case <-ctx.Done():
		return opResult{}, false
	}
}
