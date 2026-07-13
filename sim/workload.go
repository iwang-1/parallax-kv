package sim

import (
	"encoding/binary"

	"github.com/iwang-1/parallax-kv/kv"
	"github.com/iwang-1/parallax-kv/raft"
)

// client is one closed-loop virtual client: it issues a single operation,
// waits for its result, thinks for a random interval, then issues the next.
// "Closed-loop" means at most one operation is in flight per client, which
// is exactly the (ClientID, Seq) discipline the kv session table dedups on.
type client struct {
	id         uint64 // kv ClientID (distinct from node IDs)
	seq        uint64 // last issued sequence number
	leaderHint uint64 // node the client currently believes is leader

	// outstanding tracks the in-flight operation (if any) so applied
	// commits and released reads can be correlated back to it.
	pending bool
	cmd     kv.Command
	histID  int
	// attempt counts submissions of the current op so a stale retry timer
	// (from an earlier attempt) does not fire a redundant resubmission.
	attempt uint64
}

// startWorkload creates the virtual clients and schedules each one's first
// operation, staggered so they do not all fire at the same virtual instant
// (which would still be deterministic but less representative).
func (s *Simulator) startWorkload() {
	n := s.cfg.Workload.Clients
	s.clients = make([]*client, 0, n)
	for i := 0; i < n; i++ {
		c := &client{id: uint64(i + 1)}
		s.clients = append(s.clients, c)
		// Stagger first issue across [0, ThinkMax].
		s.scheduleClientOp(c, s.drawThink())
	}
}

// scheduleClientOp schedules a client to issue its next operation after the
// given delay.
func (s *Simulator) scheduleClientOp(c *client, after VirtualTime) {
	s.schedule(after, "client", c.id, "", func() {
		s.issue(c)
	})
}

// issue generates and submits the client's next operation.
func (s *Simulator) issue(c *client) {
	if c.pending {
		return // already have one in flight; retry path handles progress
	}
	c.seq++
	c.attempt = 0
	c.cmd = s.genCommand(c)
	c.histID = s.hist.Invoke(c.id, c.cmd, int64(s.now))
	c.pending = true
	s.recordControl("invoke", c.id, renderCommand(c.cmd))
	s.submit(c)
}

// submit routes the client's current operation to a node, following leader
// redirects, and arms a retry timer so a lost proposal or a leaderless
// interval eventually makes progress.
func (s *Simulator) submit(c *client) {
	c.attempt++
	if c.cmd.Op == kv.OpGet {
		s.submitRead(c)
	} else {
		s.submitWrite(c)
	}
	// Arm a retry: if the op has not completed by the deadline, try again
	// (idempotent thanks to the fixed Seq and the session table).
	seqAtSubmit := c.seq
	attemptAtSubmit := c.attempt
	s.schedule(s.retryTimeout(), "client-retry", c.id, "", func() {
		if c.pending && c.seq == seqAtSubmit && c.attempt == attemptAtSubmit {
			s.submit(c)
		}
	})
}

// submitWrite steps a MsgPropose into a candidate leader, chasing redirect
// hints across nodes until one accepts (returns nil) or all reject.
func (s *Simulator) submitWrite(c *client) {
	data := kv.EncodeCommand(c.cmd)
	for _, id := range s.candidateOrder(c) {
		ns := s.nodes[id]
		if ns == nil || ns.crashed || ns.node == nil {
			continue
		}
		err := ns.node.Step(raft.Message{
			Type:    raft.MsgPropose,
			Entries: []raft.Entry{{Type: raft.EntryNormal, Data: data}},
		})
		if err == nil {
			c.leaderHint = id
			s.drain(ns)
			return
		}
		// Redirect: adopt this node's leader hint for the next candidate.
		if lh := ns.node.Leader(); lh != 0 {
			c.leaderHint = lh
		}
	}
	// No node accepted; the retry timer will try again after a new election.
}

// submitRead steps a MsgReadIndex into a candidate leader. The read is
// correlated by an opaque token echoed back in the released ReadState.
func (s *Simulator) submitRead(c *client) {
	token := make([]byte, 16)
	binary.BigEndian.PutUint64(token[0:8], c.id)
	binary.BigEndian.PutUint64(token[8:16], c.seq)
	for _, id := range s.candidateOrder(c) {
		ns := s.nodes[id]
		if ns == nil || ns.crashed || ns.node == nil {
			continue
		}
		err := ns.node.Step(raft.Message{Type: raft.MsgReadIndex, Context: token})
		if err == nil {
			c.leaderHint = id
			s.drain(ns)
			return
		}
		if lh := ns.node.Leader(); lh != 0 {
			c.leaderHint = lh
		}
	}
}

// candidateOrder returns node IDs to try in order: the leader hint first,
// then the remaining nodes in ascending ID order. Deterministic by
// construction (no map iteration).
func (s *Simulator) candidateOrder(c *client) []uint64 {
	order := make([]uint64, 0, len(s.peers))
	if c.leaderHint != 0 {
		order = append(order, c.leaderHint)
	}
	for _, id := range s.peers {
		if id != c.leaderHint {
			order = append(order, id)
		}
	}
	return order
}

// completeIfLeader completes the matching client's write when the current
// leader applies its committed command. Only the leader answers clients, so
// a follower applying the same entry for its own state does not complete the
// operation, and once completed the cleared pending flag prevents any
// double-completion from a later duplicate apply.
func (s *Simulator) completeIfLeader(ns *nodeState, cmd kv.Command, res kv.Result) {
	if ns.node == nil || ns.node.State() != raft.StateLeader {
		return
	}
	if cmd.ClientID == 0 || cmd.ClientID > uint64(len(s.clients)) {
		return
	}
	c := s.clients[cmd.ClientID-1]
	if !c.pending || c.cmd.Op == kv.OpGet || c.seq != cmd.Seq {
		return
	}
	s.finish(c, res)
}

// releaseRead records a released ReadState. If the node's applied index has
// already reached the read index the read is served immediately; otherwise
// it is buffered and served when applied catches up (servePendingReads).
func (s *Simulator) releaseRead(ns *nodeState, rs raft.ReadState) {
	if ns.applied >= rs.Index {
		s.serveRead(ns, rs)
		return
	}
	ns.pendingReads = append(ns.pendingReads, rs)
}

// servePendingReads serves any buffered reads whose index the node's
// applied index now covers. Called whenever applied advances.
func (s *Simulator) servePendingReads(ns *nodeState) {
	if len(ns.pendingReads) == 0 {
		return
	}
	kept := ns.pendingReads[:0]
	for _, rs := range ns.pendingReads {
		if ns.applied >= rs.Index {
			s.serveRead(ns, rs)
		} else {
			kept = append(kept, rs)
		}
	}
	ns.pendingReads = kept
}

// serveRead answers a linearizable read from local state and completes the
// matching client's Get.
func (s *Simulator) serveRead(ns *nodeState, rs raft.ReadState) {
	if ns.node == nil || ns.node.State() != raft.StateLeader {
		return // only the confirmed leader serves the read
	}
	if len(rs.RequestCtx) != 16 {
		return
	}
	clientID := binary.BigEndian.Uint64(rs.RequestCtx[0:8])
	seq := binary.BigEndian.Uint64(rs.RequestCtx[8:16])
	if clientID == 0 || clientID > uint64(len(s.clients)) {
		return
	}
	c := s.clients[clientID-1]
	if !c.pending || c.cmd.Op != kv.OpGet || c.seq != seq {
		return
	}
	res := ns.sm.Read(c.cmd.Key)
	s.finish(c, res)
}

// finish completes a client's outstanding operation: it records the return
// in the history and schedules the client's next operation after a think
// interval.
func (s *Simulator) finish(c *client, res kv.Result) {
	s.hist.Complete(c.histID, res, int64(s.now))
	s.recordControl("return", c.id, renderResult(res))
	c.pending = false
	s.scheduleClientOp(c, s.drawThink())
}
