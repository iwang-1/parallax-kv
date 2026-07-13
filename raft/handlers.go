package raft

// stepFollower handles messages while a follower.
func (r *raft) stepFollower(m Message) {
	switch m.Type {
	case MsgAppend:
		r.electionElapsed = 0
		r.lead = m.From
		r.handleAppend(m)
	case MsgHeartbeat:
		r.electionElapsed = 0
		r.lead = m.From
		r.handleHeartbeat(m)
	case MsgInstallSnapshot:
		r.electionElapsed = 0
		r.lead = m.From
		r.handleSnapshot(m)
	}
}

// stepCandidate handles messages while a (pre)candidate.
func (r *raft) stepCandidate(m Message) {
	// A candidate expects the vote-response type matching its phase.
	myVoteResp := MsgVoteResp
	if r.state == StatePreCandidate {
		myVoteResp = MsgPreVoteResp
	}
	switch m.Type {
	case MsgAppend:
		r.becomeFollower(m.Term, m.From)
		r.handleAppend(m)
	case MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)
	case MsgInstallSnapshot:
		r.becomeFollower(m.Term, m.From)
		r.handleSnapshot(m)
	case myVoteResp:
		r.recordVote(m.From, !m.Reject)
		granted := r.tallyGranted()
		rejected := r.tallyRejected()
		switch {
		case granted >= r.quorum():
			if r.state == StatePreCandidate {
				r.becomeCandidate()
				r.solicitVotes(MsgVote, r.Term)
			} else {
				r.becomeLeader()
				r.bcastAppend()
			}
		case rejected >= r.quorum():
			// Lost the election; wait out the timer as a follower.
			r.becomeFollower(r.Term, None)
		}
	}
}

// stepLeader handles messages while leader.
func (r *raft) stepLeader(m Message) {
	switch m.Type {
	case MsgAppendResp:
		r.handleAppendResp(m)
	case MsgHeartbeatResp:
		r.handleHeartbeatResp(m)
	case MsgInstallSnapshotResp:
		// Treat as an append ack at the snapshot index.
		pr := r.prs[m.From]
		if pr != nil {
			pr.maybeUpdate(m.LogIndex)
			r.sendAppend(m.From)
		}
	}
}

// handleAppend processes an AppendEntries RPC and replies.
func (r *raft) handleAppend(m Message) {
	if m.LogIndex < r.raftLog.committed {
		// The leader is behind our commit index; tell it our commit point.
		r.send(Message{To: m.From, Type: MsgAppendResp, LogIndex: r.raftLog.committed})
		return
	}
	if mlastIndex, ok := r.raftLog.maybeAppend(m.LogIndex, m.LogTerm, m.Commit, m.Entries); ok {
		r.send(Message{To: m.From, Type: MsgAppendResp, LogIndex: mlastIndex})
		return
	}
	// Rejected: compute a conflict hint for fast backtracking.
	hintIndex := min(m.LogIndex, r.raftLog.lastIndex())
	hintIndex, hintTerm := r.findConflictByTerm(hintIndex, m.LogTerm)
	r.send(Message{
		To:         m.From,
		Type:       MsgAppendResp,
		LogIndex:   m.LogIndex,
		Reject:     true,
		RejectHint: hintIndex,
		HintTerm:   hintTerm,
	})
}

// findConflictByTerm walks back from index to the first entry whose term is
// at most term, returning that index and its term — the follower's best guess
// at where the logs could agree.
func (r *raft) findConflictByTerm(index, term uint64) (uint64, uint64) {
	for index > 0 {
		t, err := r.raftLog.term(index)
		if err != nil {
			// Compacted; stop here.
			break
		}
		if t <= term {
			return index, t
		}
		index--
	}
	return index, 0
}

// handleHeartbeat advances the follower's commit index and echoes the
// ReadIndex context.
func (r *raft) handleHeartbeat(m Message) {
	r.raftLog.commitTo(min(m.Commit, r.raftLog.lastIndex()))
	r.send(Message{To: m.From, Type: MsgHeartbeatResp, Context: m.Context})
}

// handleSnapshot installs a received snapshot when it is ahead of the log.
func (r *raft) handleSnapshot(m Message) {
	if m.Snapshot == nil {
		return
	}
	sindex := m.Snapshot.Metadata.Index
	sterm := m.Snapshot.Metadata.Term
	if r.restore(*m.Snapshot) {
		r.send(Message{To: m.From, Type: MsgInstallSnapshotResp, LogIndex: r.raftLog.lastIndex()})
	} else {
		// Snapshot already covered; ack at our commit index.
		_ = sindex
		_ = sterm
		r.send(Message{To: m.From, Type: MsgInstallSnapshotResp, LogIndex: r.raftLog.committed})
	}
}

// restore installs snap into the log if it is newer than what we have.
func (r *raft) restore(snap Snapshot) bool {
	if snap.Metadata.Index <= r.raftLog.committed {
		return false
	}
	// If we already have the covered entry with a matching term, fast-forward
	// the commit index instead of discarding the log.
	if r.raftLog.matchTerm(snap.Metadata.Index, snap.Metadata.Term) {
		r.raftLog.commitTo(snap.Metadata.Index)
		return false
	}
	r.raftLog.restore(snap)
	return true
}

// handleAppendResp processes a follower's AppendEntries reply.
func (r *raft) handleAppendResp(m Message) {
	pr := r.prs[m.From]
	if pr == nil {
		return
	}
	if m.Reject {
		if pr.maybeDecrTo(m.LogIndex, r.decrHint(m)) {
			r.sendAppend(m.From)
		}
		return
	}
	if pr.maybeUpdate(m.LogIndex) {
		if r.maybeCommit() {
			// Commit advanced; tell followers so they can apply.
			r.bcastAppend()
		}
	}
}

// decrHint derives the Next index to retry from a rejection's conflict hint.
func (r *raft) decrHint(m Message) uint64 {
	hint := m.RejectHint
	if m.HintTerm > 0 {
		// Scan our log back to the last index whose term is <= HintTerm.
		li := r.raftLog.lastIndex()
		if hint > li {
			hint = li
		}
		for hint > 0 {
			t, err := r.raftLog.term(hint)
			if err != nil || t <= m.HintTerm {
				break
			}
			hint--
		}
	}
	return hint
}

// handleHeartbeatResp records a heartbeat ack, releasing any ReadIndex
// requests whose leadership is now confirmed by a quorum, and nudges lagging
// followers.
func (r *raft) handleHeartbeatResp(m Message) {
	pr := r.prs[m.From]
	if pr != nil && pr.Match < r.raftLog.lastIndex() {
		r.sendAppend(m.From)
	}
	if len(m.Context) == 0 {
		return
	}
	if r.readOnly.recvAck(m.From, m.Context) < r.quorum() {
		return
	}
	for _, rs := range r.readOnly.advance(m.Context) {
		r.readStates = append(r.readStates, ReadState{Index: rs.index, RequestCtx: rs.req.Context})
	}
}
