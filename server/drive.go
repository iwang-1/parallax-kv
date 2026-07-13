package server

import (
	"context"
	"encoding/binary"
	"time"

	"github.com/iwang-1/parallax-kv/kv"
	"github.com/iwang-1/parallax-kv/raft"
)

// driveLoop is the single-threaded owner of the raft node, the state
// machine, and all waiter bookkeeping. Every input — ticks, inbound peer
// messages, and client requests — is serialized through this one select,
// which is exactly the discipline the pure core requires (it is not safe for
// concurrent use). This mirrors the simulator's single-goroutine event loop;
// the only differences are the real clock and real I/O.
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
			if err := s.drain(); err != nil {
				return err
			}
		case req := <-s.reqCh:
			s.handleRequest(req)
			if err := s.drain(); err != nil {
				return err
			}
		}
	}
}

// handleRequest steps a client request into the core and registers its
// waiter. A proposal/read into a non-leader returns ErrNotLeader, which is
// answered immediately with a redirect hint.
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

// drain processes all pending Ready batches, enforcing the persist-before-
// send ordering the core's contract requires:
//
//  1. persist HardState / Entries / Snapshot durably (fsync if MustSync),
//  2. send Messages,
//  3. apply Snapshot then CommittedEntries; release ReadStates,
//  4. Advance.
func (s *Server) drain() error {
	// A leadership change invalidates in-flight client waiters: a deposed
	// leader can neither commit its proposals nor safely serve its reads.
	s.checkLeadership()

	for s.node.HasReady() {
		rd := s.node.Ready()

		// (1) Persist. Order matters: snapshot compacts the prefix, entries
		// extend it, hard state records term/vote/commit. A single fsync
		// (group commit) covers the whole batch when MustSync is set.
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

// checkLeadership fails all in-flight client waiters when this node stops
// being the leader: their proposals will never commit here and their reads
// must not be served from a deposed leader's stale state.
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
			// A committed entry always decodes; a failure is a programming
			// error, but panicking here would take down a node recovering a
			// valid log, so we skip it (the waiter times out and retries).
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
