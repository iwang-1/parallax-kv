package raft

import "testing"

// TestSingleNodeElectsSelf verifies a single-node cluster becomes leader on
// its first election tick without any messages.
func TestSingleNodeElectsSelf(t *testing.T) {
	r := newTestRaft(t, 1, []uint64{1}, false)
	for i := 0; i < r.randomizedElectionTimeout; i++ {
		r.tick()
	}
	if r.state != StateLeader {
		t.Fatalf("single node state = %s, want leader", r.state)
	}
	if r.Term != 1 {
		t.Fatalf("term = %d, want 1", r.Term)
	}
}

// TestElectionBasic verifies a 3-node cluster elects exactly one leader.
func TestElectionBasic(t *testing.T) {
	for _, preVote := range []bool{false, true} {
		nw := newNetwork(t, preVote, 1, 2, 3)
		nw.tickUntilLeader(1)
		if got := nw.leaders(); len(got) != 1 || got[0] != 1 {
			t.Fatalf("preVote=%v leaders = %v, want [1]", preVote, got)
		}
		if nw.peers[2].state != StateFollower || nw.peers[3].state != StateFollower {
			t.Fatalf("preVote=%v followers not in follower state", preVote)
		}
		// Followers must have adopted the leader's term and identity.
		for _, id := range []uint64{2, 3} {
			if nw.peers[id].lead != 1 {
				t.Fatalf("preVote=%v node %d lead = %d, want 1", preVote, id, nw.peers[id].lead)
			}
		}
	}
}

// TestLeaderReelectionAfterPartition verifies liveness and election safety
// across a partition: isolate the leader, a new leader emerges among the
// remaining majority at a higher term, and when the old leader rejoins it
// steps down — leaving exactly one leader.
func TestLeaderReelectionAfterPartition(t *testing.T) {
	nw := newNetwork(t, true, 1, 2, 3)
	nw.tickUntilLeader(1)
	if got := nw.leaders(); len(got) != 1 || got[0] != 1 {
		t.Fatalf("initial leaders = %v, want [1]", got)
	}
	oldTerm := nw.peers[1].Term

	// Partition the leader away from the majority {2,3}.
	nw.isolate(1)

	// The surviving majority must elect a new leader at a higher term. Both
	// survivors tick so their leader leases (stickiness) expire and a
	// PreVote can succeed.
	var majorityLeader uint64
	for i := 0; i < 6*nw.peers[2].cfg.ElectionTicks && majorityLeader == 0; i++ {
		nw.peers[2].tick()
		nw.peers[3].tick()
		nw.deliverAll()
		for _, id := range []uint64{2, 3} {
			if nw.peers[id].state == StateLeader {
				majorityLeader = id
			}
		}
	}
	if majorityLeader == 0 {
		t.Fatalf("no new leader among majority; leaders = %v", nw.leaders())
	}
	if nw.peers[majorityLeader].Term <= oldTerm {
		t.Fatalf("new leader term %d not > old term %d", nw.peers[majorityLeader].Term, oldTerm)
	}

	// Heal the partition; ticking drives heartbeats that depose the stale
	// leader (node 1) once it sees the higher term.
	nw.recover(1)
	for i := 0; i < 3*nw.peers[majorityLeader].cfg.ElectionTicks; i++ {
		for _, id := range nw.ids {
			nw.peers[id].tick()
		}
		nw.deliverAll()
	}
	if got := nw.leaders(); len(got) != 1 {
		t.Fatalf("after heal, leaders = %v, want exactly one", got)
	}
	if nw.peers[1].state == StateLeader {
		t.Fatalf("stale leader (node 1) did not step down")
	}
}

// TestElectionSafetyOneLeaderPerTerm asserts the core never grants two votes
// in the same term, so at most one leader can exist per term.
func TestElectionSafetyOneLeaderPerTerm(t *testing.T) {
	r := newTestRaft(t, 1, []uint64{1, 2, 3}, false)
	r.becomeFollower(5, None)

	// First vote request in term 5 from node 2 with an up-to-date log.
	r.Step(Message{Type: MsgVote, From: 2, To: 1, Term: 5, LogIndex: 0, LogTerm: 0})
	resp := r.drainMsgs()
	if len(resp) != 1 || resp[0].Reject {
		t.Fatalf("first vote should be granted, got %+v", resp)
	}
	if r.Vote != 2 {
		t.Fatalf("vote = %d, want 2", r.Vote)
	}

	// Second, competing vote request in the same term from node 3 must be
	// rejected (already voted for 2).
	r.Step(Message{Type: MsgVote, From: 3, To: 1, Term: 5, LogIndex: 0, LogTerm: 0})
	resp = r.drainMsgs()
	if len(resp) != 1 || !resp[0].Reject {
		t.Fatalf("second vote should be rejected, got %+v", resp)
	}
}

// TestVoteRejectStaleLog verifies the up-to-date check: a candidate with a
// shorter/older log is denied.
func TestVoteRejectStaleLog(t *testing.T) {
	r := newTestRaft(t, 1, []uint64{1, 2, 3}, false)
	// Give node 1 a log ending at term 3, index 2.
	r.becomeFollower(3, None)
	r.raftLog.append(Entry{Term: 2, Index: 1}, Entry{Term: 3, Index: 2})

	tests := []struct {
		name          string
		lastIdx, term uint64
		wantGrant     bool
	}{
		{"older term", 5, 2, false},
		{"same term shorter log", 1, 3, false},
		{"same term equal log", 2, 3, true},
		{"newer term", 1, 4, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r.Vote = None
			r.lead = None
			r.Step(Message{Type: MsgVote, From: 2, To: 1, Term: 3, LogIndex: tc.lastIdx, LogTerm: tc.term})
			resp := r.drainMsgs()
			if len(resp) != 1 {
				t.Fatalf("want 1 response, got %d", len(resp))
			}
			granted := !resp[0].Reject
			if granted != tc.wantGrant {
				t.Fatalf("granted = %v, want %v", granted, tc.wantGrant)
			}
		})
	}
}

// TestPreVoteDoesNotBumpTerm verifies that receiving a PreVote at a higher
// term does not change the recipient's term (the whole point of PreVote).
func TestPreVoteDoesNotBumpTerm(t *testing.T) {
	r := newTestRaft(t, 1, []uint64{1, 2, 3}, true)
	r.becomeFollower(5, 2) // follower with a live leader at term 5

	r.Step(Message{Type: MsgPreVote, From: 3, To: 1, Term: 6, LogIndex: 0, LogTerm: 0})
	if r.Term != 5 {
		t.Fatalf("term after PreVote = %d, want 5 (unchanged)", r.Term)
	}
	resp := r.drainMsgs()
	if len(resp) != 1 || resp[0].Type != MsgPreVoteResp {
		t.Fatalf("want a PreVoteResp, got %+v", resp)
	}
	// Grant is allowed because the probe term (6) exceeds ours and the log is
	// up to date; the key invariant is that our term stayed at 5.
}

// TestPreVoteRejectsPartitionedNode is the canonical PreVote scenario: a node
// partitioned away has inflated its term via repeated pre-campaigns; on
// rejoin it must NOT disrupt the stable leader. With PreVote, its high term
// never reaches peers (PreVote carries Term+1 but never bumps anyone), and
// peers reject the probe because they are backing a live leader at a term the
// candidate cannot beat on log freshness.
func TestPreVoteRejectsPartitionedNode(t *testing.T) {
	r := newTestRaft(t, 1, []uint64{1, 2, 3}, true)
	// Node 1 is a healthy follower with a live leader (node 2) at term 4 and
	// some committed log.
	r.becomeFollower(4, 2)
	r.raftLog.append(Entry{Term: 4, Index: 1})
	r.electionElapsed = 0

	// A partitioned node 3 pre-campaigns at term 99 but with an EMPTY log.
	r.Step(Message{Type: MsgPreVote, From: 3, To: 1, Term: 99, LogIndex: 0, LogTerm: 0})
	resp := r.drainMsgs()
	if len(resp) != 1 || !resp[0].Reject {
		t.Fatalf("stale-log pre-candidate should be rejected, got %+v", resp)
	}
	if r.Term != 4 {
		t.Fatalf("term = %d, want 4 (undisturbed)", r.Term)
	}
}
