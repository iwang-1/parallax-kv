package raft

import (
	"bytes"
	"testing"
)

// bootstrapLeader elects node 1 leader of a fresh 3-node cluster and returns
// the network with all traffic (including the no-op entry) settled.
func bootstrapLeader(t *testing.T) *network {
	t.Helper()
	nw := newNetwork(t, false, 1, 2, 3)
	nw.tickUntilLeader(1)
	if got := nw.leaders(); len(got) != 1 || got[0] != 1 {
		t.Fatalf("bootstrap leaders = %v, want [1]", got)
	}
	return nw
}

// TestReplicationHappyPath proposes a command and verifies it is replicated
// and committed on a majority.
func TestReplicationHappyPath(t *testing.T) {
	nw := bootstrapLeader(t)
	leader := nw.peers[1]

	if err := leader.Step(Message{Type: MsgPropose, Entries: []Entry{{Data: []byte("x=1")}}}); err != nil {
		t.Fatalf("propose: %v", err)
	}
	nw.deliverAll()

	// The proposed entry sits above the leader's term no-op (index 1), so it
	// is at index 2.
	for _, id := range nw.ids {
		r := nw.peers[id]
		if r.raftLog.lastIndex() != 2 {
			t.Fatalf("node %d lastIndex = %d, want 2", id, r.raftLog.lastIndex())
		}
		if r.raftLog.committed != 2 {
			t.Fatalf("node %d committed = %d, want 2", id, r.raftLog.committed)
		}
		ents, err := r.raftLog.slice(2, 3)
		if err != nil {
			t.Fatalf("node %d slice: %v", id, err)
		}
		if len(ents) != 1 || !bytes.Equal(ents[0].Data, []byte("x=1")) {
			t.Fatalf("node %d entry = %+v, want x=1", id, ents)
		}
	}
}

// TestAppendConflictBacktracking verifies a follower with a divergent suffix
// is repaired: the leader backtracks Next until the logs agree, then
// overwrites the follower's stale entries.
func TestAppendConflictBacktracking(t *testing.T) {
	// Follower with a stale divergent log: three entries at term 1..1..1.
	f := newTestRaft(t, 2, []uint64{1, 2, 3}, false)
	f.becomeFollower(1, None)
	f.raftLog.append(
		Entry{Term: 1, Index: 1},
		Entry{Term: 1, Index: 2},
		Entry{Term: 1, Index: 3},
	)

	// Leader at term 3 with a different log: entries at term 1, then 3.
	lead := newTestRaft(t, 1, []uint64{1, 2, 3}, false)
	lead.becomeCandidate() // term 1
	lead.becomeCandidate() // term 2
	lead.becomeLeader()    // term... reset keeps term, appends no-op at term 2
	// Force a known leader log: index1 term1, index2 term2(noop already), add
	// index3 term2.
	lead.Term = 3
	lead.raftLog = mustLog(t)
	lead.raftLog.append(
		Entry{Term: 1, Index: 1},
		Entry{Term: 3, Index: 2},
	)
	lead.prs = map[uint64]*progress{
		1: {Match: 2, Next: 3},
		2: {Match: 0, Next: 4}, // optimistically points past follower's log
		3: {Match: 0, Next: 4},
	}
	lead.state = StateLeader
	lead.lead = 1

	// Drive the repair: leader sends append, follower rejects with a hint,
	// leader backtracks, until follower matches.
	for round := 0; round < 10; round++ {
		lead.sendAppend(2)
		ms := lead.drainMsgs()
		if len(ms) == 0 {
			break
		}
		for _, m := range ms {
			f.Step(m)
		}
		for _, m := range f.drainMsgs() {
			lead.Step(m)
		}
		if lead.prs[2].Match == lead.raftLog.lastIndex() {
			break
		}
	}

	if f.raftLog.lastIndex() != 2 {
		t.Fatalf("follower lastIndex = %d, want 2", f.raftLog.lastIndex())
	}
	tm, _ := f.raftLog.term(2)
	if tm != 3 {
		t.Fatalf("follower term@2 = %d, want 3 (leader's entry)", tm)
	}
	if lead.prs[2].Match != 2 {
		t.Fatalf("leader Match[2] = %d, want 2", lead.prs[2].Match)
	}
}

func mustLog(t *testing.T) *raftLog {
	t.Helper()
	l, err := newLog(newTestStorage())
	if err != nil {
		t.Fatalf("newLog: %v", err)
	}
	return l
}

// TestProposeOnFollowerRejected verifies proposals to a non-leader return
// ErrNotLeader.
func TestProposeOnFollowerRejected(t *testing.T) {
	r := newTestRaft(t, 2, []uint64{1, 2, 3}, false)
	err := r.Step(Message{Type: MsgPropose, Entries: []Entry{{Data: []byte("x")}}})
	if err != ErrNotLeader {
		t.Fatalf("propose on follower err = %v, want ErrNotLeader", err)
	}
}
