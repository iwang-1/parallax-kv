package raft

import (
	"bytes"
	"testing"
)

// TestReadIndexSingleNode verifies a single-node leader releases a read
// immediately at its commit index (it is trivially a quorum).
func TestReadIndexSingleNode(t *testing.T) {
	r := newTestRaft(t, 1, []uint64{1}, false)
	for i := 0; i < r.randomizedElectionTimeout; i++ {
		r.tick()
	}
	if r.state != StateLeader {
		t.Fatalf("state = %s, want leader", r.state)
	}
	ctx := []byte("read-1")
	if err := r.Step(Message{Type: MsgReadIndex, Context: ctx}); err != nil {
		t.Fatalf("readindex: %v", err)
	}
	if len(r.readStates) != 1 {
		t.Fatalf("readStates = %d, want 1", len(r.readStates))
	}
	if !bytes.Equal(r.readStates[0].RequestCtx, ctx) {
		t.Fatalf("read ctx = %q, want %q", r.readStates[0].RequestCtx, ctx)
	}
	if r.readStates[0].Index != r.raftLog.committed {
		t.Fatalf("read index = %d, want committed %d", r.readStates[0].Index, r.raftLog.committed)
	}
}

// TestReadIndexQuorumConfirm verifies a read is released only after a
// heartbeat quorum confirms leadership.
func TestReadIndexQuorumConfirm(t *testing.T) {
	nw := bootstrapLeader(t)
	leader := nw.peers[1]
	// Drain any residual heartbeat traffic.
	nw.deliverAll()

	ctx := []byte("read-A")
	if err := leader.Step(Message{Type: MsgReadIndex, Context: ctx}); err != nil {
		t.Fatalf("readindex: %v", err)
	}
	// The read must NOT be released yet: only the leader has acked.
	if len(leader.readStates) != 0 {
		t.Fatalf("read released before quorum confirm")
	}
	// Deliver the confirming heartbeats and their responses.
	nw.deliverAll()

	if len(leader.readStates) != 1 {
		t.Fatalf("read not released after quorum, readStates = %d", len(leader.readStates))
	}
	if !bytes.Equal(leader.readStates[0].RequestCtx, ctx) {
		t.Fatalf("read ctx = %q, want %q", leader.readStates[0].RequestCtx, ctx)
	}
}

// TestReadIndexOnFollowerRejected verifies ReadIndex on a non-leader returns
// ErrNotLeader.
func TestReadIndexOnFollowerRejected(t *testing.T) {
	r := newTestRaft(t, 2, []uint64{1, 2, 3}, false)
	err := r.Step(Message{Type: MsgReadIndex, Context: []byte("x")})
	if err != ErrNotLeader {
		t.Fatalf("readindex on follower err = %v, want ErrNotLeader", err)
	}
}

// TestReadIndexNotReleasedAfterLeadershipLoss verifies that a leader which
// loses leadership (steps down) before the confirming quorum never releases
// the stale read. This is the core linearizability guard of ReadIndex.
func TestReadIndexNotReleasedAfterLeadershipLoss(t *testing.T) {
	nw := bootstrapLeader(t)
	leader := nw.peers[1]
	nw.deliverAll()

	// Issue a read but DO NOT deliver the confirming heartbeats.
	if err := leader.Step(Message{Type: MsgReadIndex, Context: []byte("stale")}); err != nil {
		t.Fatalf("readindex: %v", err)
	}
	leader.drainMsgs() // drop the confirming heartbeats (partition)

	// A higher-term leader emerges; the old leader steps down on contact.
	leader.Step(Message{Type: MsgAppend, From: 2, To: 1, Term: leader.Term + 5, LogIndex: leader.raftLog.lastIndex(), LogTerm: leader.raftLog.lastTerm(), Commit: 0})
	if leader.state == StateLeader {
		t.Fatalf("old leader did not step down")
	}
	// It must never have released the read.
	if len(leader.readStates) != 0 {
		t.Fatalf("stale read released after leadership loss: %+v", leader.readStates)
	}
}
