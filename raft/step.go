package raft

// Step is the single entry point for all inputs. It first applies the
// term-comparison rules from the Raft paper (§3.3, §4.2.3 PreVote), then
// dispatches to the role-specific handler.
func (r *raft) Step(m Message) error {
	// Handle local messages (no term) directly.
	switch m.Type {
	case MsgPropose:
		return r.stepPropose(m)
	case MsgReadIndex:
		return r.stepReadIndex(m)
	}

	// Leader-stickiness disruption check (§4.2.3). If we are currently
	// backing a leader and our election timer has not expired, we have an
	// effective lease: reject any vote or pre-vote before the term rules can
	// step us down. This is what stops a node that rejoins from a partition
	// with an inflated term — even with an up-to-date log — from deposing a
	// healthy leader. A repeat vote for the SAME candidate is still allowed.
	if (m.Type == MsgVote || m.Type == MsgPreVote) &&
		r.lead != None && r.Vote != m.From &&
		r.electionElapsed < r.cfg.ElectionTicks {
		respType := MsgVoteResp
		if m.Type == MsgPreVote {
			respType = MsgPreVoteResp
		}
		resp := Message{To: m.From, Type: respType, Reject: true}
		if m.Type == MsgPreVote {
			resp.Term = m.Term
		} else {
			resp.Term = r.Term
		}
		r.send(resp)
		return nil
	}

	// Term rules for messages carrying a term.
	switch {
	case m.Term == 0:
		// Local/response messages without a term; fall through.
	case m.Term > r.Term:
		if m.Type == MsgPreVote {
			// Do NOT bump our term for a PreVote probe; a PreVote never
			// changes term. We answer it below without stepping down.
			break
		}
		if m.Type == MsgPreVoteResp && !m.Reject {
			// A granted PreVoteResp carries the future term; do not adopt
			// it — the candidate transition raises the term itself.
			break
		}
		// A higher term from a leader/candidate: step down.
		lead := m.From
		if m.Type == MsgVote {
			lead = None
		}
		r.becomeFollower(m.Term, lead)
	case m.Term < r.Term:
		// Stale message. Reply to append/heartbeat so the stale leader
		// learns the new term; otherwise drop silently.
		switch m.Type {
		case MsgAppend, MsgHeartbeat, MsgInstallSnapshot:
			r.send(Message{To: m.From, Type: msgRespType(m.Type)})
		case MsgPreVote:
			// Tell the pre-candidate our real (higher) term so it aborts and
			// catches up rather than campaigning forever.
			r.send(Message{To: m.From, Type: MsgPreVoteResp, Term: r.Term, Reject: true})
		}
		return nil
	}

	// Vote handling is common across roles.
	switch m.Type {
	case MsgPreVote, MsgVote:
		r.handleVoteRequest(m)
		return nil
	}

	switch r.state {
	case StateFollower:
		r.stepFollower(m)
	case StatePreCandidate, StateCandidate:
		r.stepCandidate(m)
	case StateLeader:
		r.stepLeader(m)
	}
	return nil
}

// msgRespType maps a request type to the response type used for stale-term
// rejections.
func msgRespType(t MessageType) MessageType {
	switch t {
	case MsgAppend:
		return MsgAppendResp
	case MsgHeartbeat:
		return MsgHeartbeatResp
	case MsgInstallSnapshot:
		return MsgInstallSnapshotResp
	default:
		return MsgUnknown
	}
}

// handleVoteRequest answers a MsgVote or MsgPreVote. It grants when the
// candidate's log is at least as up-to-date and (for real votes) the node has
// not already voted for someone else this term.
func (r *raft) handleVoteRequest(m Message) {
	respType := MsgVoteResp
	if m.Type == MsgPreVote {
		respType = MsgPreVoteResp
	}

	upToDate := r.raftLog.isUpToDate(m.LogIndex, m.LogTerm)
	var grant bool
	switch m.Type {
	case MsgPreVote:
		// PreVote grant condition: the probe would campaign at a term ahead
		// of ours with an at-least-as-up-to-date log. A PreVote never mutates
		// our state.
		grant = upToDate && m.Term > r.Term
	case MsgVote:
		canVote := r.Vote == m.From || (r.Vote == None && r.lead == None) || m.Term > r.Term
		grant = canVote && upToDate
		if grant {
			r.Vote = m.From
			r.electionElapsed = 0
		}
	}

	resp := Message{To: m.From, Type: respType, Reject: !grant}
	if m.Type == MsgPreVote {
		// Echo the campaign term so the pre-candidate can match the reply.
		resp.Term = m.Term
	}
	r.send(resp)
}

func (r *raft) stepPropose(m Message) error {
	if r.state != StateLeader {
		return ErrNotLeader
	}
	if len(m.Entries) == 0 {
		return nil
	}
	es := make([]Entry, len(m.Entries))
	copy(es, m.Entries)
	for i := range es {
		es[i].Type = EntryNormal
	}
	r.appendEntry(es...)
	r.bcastAppend()
	return nil
}

func (r *raft) stepReadIndex(m Message) error {
	if r.state != StateLeader {
		return ErrNotLeader
	}
	if !r.committedEntryInCurrentTerm() {
		r.pendingReadIndexMessages = append(r.pendingReadIndexMessages, m)
		return nil
	}
	r.sendReadIndex(m)
	return nil
}

func (r *raft) committedEntryInCurrentTerm() bool {
	term, err := r.raftLog.term(r.raftLog.committed)
	return err == nil && term == r.Term
}

func (r *raft) releasePendingReadIndexMessages() {
	if r.state != StateLeader || !r.committedEntryInCurrentTerm() {
		return
	}
	pending := r.pendingReadIndexMessages
	r.pendingReadIndexMessages = nil
	for _, m := range pending {
		r.sendReadIndex(m)
	}
}

func (r *raft) sendReadIndex(m Message) {
	// A single-node cluster can serve immediately: it is trivially a
	// quorum, so record the read at the current commit index.
	if len(r.peers) == 1 {
		r.readStates = append(r.readStates, ReadState{Index: r.raftLog.committed, RequestCtx: m.Context})
		return
	}
	// Record the request at the current commit index and broadcast a
	// confirming heartbeat carrying its context.
	r.readOnly.addRequest(r.raftLog.committed, m)
	r.readOnly.recvAck(r.id, m.Context)
	r.bcastHeartbeatWithCtx(m.Context)
}
