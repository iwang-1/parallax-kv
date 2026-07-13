package raft

// bcastAppend sends replication traffic to every follower.
func (r *raft) bcastAppend() {
	for _, id := range r.peers {
		if id == r.id {
			continue
		}
		r.sendAppend(id)
	}
}

// sendAppend sends the appropriate replication message to the follower id:
// an InstallSnapshot when the needed entries are compacted, otherwise an
// AppendEntries carrying entries from pr.Next onward.
func (r *raft) sendAppend(id uint64) {
	pr := r.prs[id]
	if pr == nil {
		return
	}
	prevIndex := pr.Next - 1
	prevTerm, err := r.raftLog.term(prevIndex)
	if err != nil {
		// The entry preceding Next has been compacted; ship a snapshot.
		r.sendSnapshot(id)
		return
	}

	ents, err := r.entriesToSend(pr.Next)
	if err != nil {
		r.sendSnapshot(id)
		return
	}
	r.send(Message{
		To:       id,
		Type:     MsgAppend,
		LogIndex: prevIndex,
		LogTerm:  prevTerm,
		Entries:  ents,
		Commit:   r.raftLog.committed,
	})
}

// entriesToSend gathers entries in [lo, lastIndex+1), capped by
// MaxEntriesPerAppend.
func (r *raft) entriesToSend(lo uint64) ([]Entry, error) {
	last := r.raftLog.lastIndex()
	if lo > last {
		return nil, nil
	}
	hi := last + 1
	if limit := r.cfg.MaxEntriesPerAppend; limit > 0 && hi-lo > uint64(limit) {
		hi = lo + uint64(limit)
	}
	return r.raftLog.slice(lo, hi)
}

// sendSnapshot ships the storage snapshot to a lagging follower.
func (r *raft) sendSnapshot(id uint64) {
	snap, err := r.raftLog.storage.Snapshot()
	if err != nil || snap.Metadata.Index == 0 {
		// No snapshot available; nothing to send this round.
		return
	}
	s := snap
	r.send(Message{
		To:       id,
		Type:     MsgInstallSnapshot,
		Snapshot: &s,
	})
}

// bcastHeartbeat sends an empty heartbeat to every follower.
func (r *raft) bcastHeartbeat() {
	ctx := r.readOnly.lastPendingRequestCtx()
	r.bcastHeartbeatWithCtx(ctx)
}

// bcastHeartbeatWithCtx sends a heartbeat carrying the given ReadIndex
// confirmation context to every follower.
func (r *raft) bcastHeartbeatWithCtx(ctx []byte) {
	for _, id := range r.peers {
		if id == r.id {
			continue
		}
		commit := min(r.prs[id].Match, r.raftLog.committed)
		r.send(Message{
			To:      id,
			Type:    MsgHeartbeat,
			Commit:  commit,
			Context: ctx,
		})
	}
}
