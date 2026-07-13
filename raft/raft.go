package raft

import "sort"

// None is the sentinel node ID meaning "no node" (no leader, no vote).
const None uint64 = 0

// raft is the internal consensus state machine. Node is a thin facade over
// it. It is pure: no goroutines, no clocks, no I/O, and no map-iteration
// nondeterminism (peers are iterated in sorted order). All randomness comes
// from cfg.Rand.
type raft struct {
	cfg Config

	id   uint64
	Term uint64
	Vote uint64

	raftLog *raftLog

	state StateType
	lead  uint64

	// peers is the sorted membership, used for all quorum iteration so the
	// core never depends on map order.
	peers []uint64
	// prs is the leader's per-follower replication progress.
	prs map[uint64]*progress
	// votes records ballots received in the current (pre)election.
	votes map[uint64]bool

	// msgs is the outbound queue drained by Ready.
	msgs []Message
	// readStates holds ReadIndex results released to the driver.
	readStates []ReadState
	// readOnly tracks in-flight ReadIndex requests awaiting a heartbeat
	// quorum confirmation.
	readOnly *readOnly

	// election/heartbeat timers, measured in ticks.
	electionElapsed  int
	heartbeatElapsed int
	heartbeatTimeout int
	// randomizedElectionTimeout is drawn from cfg.Rand in
	// [ElectionTicks, 2*ElectionTicks) on every reset.
	randomizedElectionTimeout int

	// prevHardSt is the last HardState surfaced in a Ready, used to detect
	// changes worth reporting.
	prevHardSt HardState
}

func newRaft(cfg Config, storage LogStorage) (*raft, error) {
	rlog, err := newLog(storage)
	if err != nil {
		return nil, err
	}
	hs, err := storage.HardState()
	if err != nil {
		return nil, err
	}
	peers := append([]uint64(nil), cfg.Peers...)
	sort.Slice(peers, func(i, j int) bool { return peers[i] < peers[j] })

	r := &raft{
		cfg:              cfg,
		id:               cfg.ID,
		raftLog:          rlog,
		peers:            peers,
		prs:              make(map[uint64]*progress),
		votes:            make(map[uint64]bool),
		readOnly:         newReadOnly(),
		lead:             None,
		heartbeatTimeout: cfg.HeartbeatTicks,
	}
	// Recover persisted term/vote/commit.
	r.Term = hs.Term
	r.Vote = hs.Vote
	if hs.Commit > r.raftLog.committed {
		r.raftLog.commitTo(hs.Commit)
	}
	r.prevHardSt = r.hardState()
	r.becomeFollower(r.Term, None)
	return r, nil
}

// quorum is the number of nodes forming a majority.
func (r *raft) quorum() int { return len(r.peers)/2 + 1 }

// hardState snapshots the durable state.
func (r *raft) hardState() HardState {
	return HardState{Term: r.Term, Vote: r.Vote, Commit: r.raftLog.committed}
}

// send stamps the message with the sender ID and current term (except for
// PreVote messages, whose term is set explicitly by the caller) and queues it
// for the next Ready.
func (r *raft) send(m Message) {
	m.From = r.id
	// Requests and responses that carry a term always use the node's term,
	// except MsgPreVote/MsgPreVoteResp which set Term explicitly.
	if m.Term == 0 && m.Type != MsgPropose {
		switch m.Type {
		case MsgPreVote, MsgPreVoteResp:
			// Term set explicitly by caller.
		default:
			m.Term = r.Term
		}
	}
	r.msgs = append(r.msgs, m)
}

// reset re-initializes per-term volatile state on a term change or role
// transition and re-draws the randomized election timeout.
func (r *raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.lead = None
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout()
	r.votes = make(map[uint64]bool)
	r.prs = make(map[uint64]*progress)
	last := r.raftLog.lastIndex()
	for _, id := range r.peers {
		pr := &progress{Next: last + 1}
		if id == r.id {
			pr.Match = last
		}
		r.prs[id] = pr
	}
	r.readOnly = newReadOnly()
}

func (r *raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.cfg.ElectionTicks + r.cfg.Rand.Intn(r.cfg.ElectionTicks)
}

// pastElectionTimeout reports whether the (randomized) election timeout has
// elapsed for a follower or candidate.
func (r *raft) pastElectionTimeout() bool {
	return r.electionElapsed >= r.randomizedElectionTimeout
}

// promotable reports whether this node is a voting member and so may start an
// election.
func (r *raft) promotable() bool {
	_, ok := r.prs[r.id]
	return ok
}

// appendEntry stamps es with the current term and contiguous indices, adds
// them to the log, and advances the leader's own progress.
func (r *raft) appendEntry(es ...Entry) {
	li := r.raftLog.lastIndex()
	for i := range es {
		es[i].Term = r.Term
		es[i].Index = li + 1 + uint64(i)
	}
	r.raftLog.append(es...)
	r.prs[r.id].maybeUpdate(r.raftLog.lastIndex())
	r.maybeCommit()
}

// maybeCommit recomputes the commit index from the median matched index and
// applies the Figure-8 current-term restriction.
func (r *raft) maybeCommit() bool {
	matches := make([]uint64, 0, len(r.peers))
	for _, id := range r.peers {
		matches = append(matches, r.prs[id].Match)
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i] < matches[j] })
	// The commit index is the highest index replicated on a majority: with
	// the list sorted ascending, that is the element at position len-quorum.
	mci := matches[len(matches)-r.quorum()]
	return r.raftLog.maybeCommit(mci, r.Term)
}

func (r *raft) becomeFollower(term uint64, lead uint64) {
	r.reset(term)
	r.state = StateFollower
	r.lead = lead
}

func (r *raft) becomePreCandidate() {
	if r.state == StateLeader {
		panic("raft: invalid transition leader -> pre-candidate")
	}
	// PreCandidate does not increment Term or reset Vote; it only campaigns
	// at Term+1 hypothetically. Reset the timer and vote tally.
	r.state = StatePreCandidate
	r.votes = make(map[uint64]bool)
	r.lead = None
	r.electionElapsed = 0
	r.resetRandomizedElectionTimeout()
}

func (r *raft) becomeCandidate() {
	if r.state == StateLeader {
		panic("raft: invalid transition leader -> candidate")
	}
	r.reset(r.Term + 1)
	r.Vote = r.id
	r.state = StateCandidate
}

func (r *raft) becomeLeader() {
	if r.state == StateFollower {
		panic("raft: invalid transition follower -> leader")
	}
	r.reset(r.Term)
	r.lead = r.id
	r.state = StateLeader
	// Append the term's no-op entry to establish the commit rule for
	// prior-term entries (Figure-8).
	r.appendEntry(Entry{Type: EntryNoop})
}

// tick advances the logical clock by one tick.
func (r *raft) tick() {
	switch r.state {
	case StateLeader:
		r.tickHeartbeat()
	default:
		r.tickElection()
	}
}

func (r *raft) tickElection() {
	r.electionElapsed++
	if r.promotable() && r.pastElectionTimeout() {
		r.electionElapsed = 0
		r.campaign()
	}
}

func (r *raft) tickHeartbeat() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.bcastHeartbeat()
	}
}

// campaign starts an election, using PreVote first when enabled.
func (r *raft) campaign() {
	if r.cfg.PreVote {
		r.becomePreCandidate()
		r.solicitVotes(MsgPreVote, r.Term+1)
	} else {
		r.becomeCandidate()
		r.solicitVotes(MsgVote, r.Term)
	}
}

// solicitVotes votes for itself and sends vote requests to peers. voteTerm is
// the term the (pre)candidate campaigns at.
func (r *raft) solicitVotes(t MessageType, voteTerm uint64) {
	// Vote for itself; a single-node cluster wins immediately.
	r.recordVote(r.id, true)
	if r.tallyGranted() >= r.quorum() {
		if t == MsgPreVote {
			r.becomeCandidate()
			r.solicitVotes(MsgVote, r.Term)
		} else {
			r.becomeLeader()
		}
		return
	}
	li := r.raftLog.lastIndex()
	lt := r.raftLog.lastTerm()
	for _, id := range r.peers {
		if id == r.id {
			continue
		}
		r.send(Message{
			Type:     t,
			To:       id,
			Term:     voteTerm,
			LogIndex: li,
			LogTerm:  lt,
		})
	}
}

// recordVote records a ballot from id and returns (granted, firstTime).
func (r *raft) recordVote(id uint64, granted bool) (bool, bool) {
	if _, ok := r.votes[id]; !ok {
		r.votes[id] = granted
		return granted, true
	}
	return r.votes[id], false
}

func (r *raft) tallyGranted() int {
	n := 0
	for _, g := range r.votes {
		if g {
			n++
		}
	}
	return n
}

func (r *raft) tallyRejected() int {
	n := 0
	for _, g := range r.votes {
		if !g {
			n++
		}
	}
	return n
}
