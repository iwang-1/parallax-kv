package raft

import (
	"bytes"
	"testing"
)

// TestReadIndexWaitsForCurrentTermCommit verifies a newly elected leader does
// not begin quorum confirmation until its current-term no-op is committed.
func TestReadIndexWaitsForCurrentTermCommit(t *testing.T) {
	r := newUncommittedLeader(t)
	ctx := []byte("before-noop-commit")

	if err := r.Step(Message{Type: MsgReadIndex, Context: ctx}); err != nil {
		t.Fatalf("readindex: %v", err)
	}
	if len(r.readStates) != 0 {
		t.Fatalf("read released before current-term commit: %+v", r.readStates)
	}
	if len(r.pendingReadIndexMessages) != 1 {
		t.Fatalf("pending reads = %d, want 1", len(r.pendingReadIndexMessages))
	}
	if len(r.readOnly.readIndexQueue) != 0 {
		t.Fatalf("read entered quorum confirmation before current-term commit")
	}
	if len(r.msgs) != 0 {
		t.Fatalf("read sent confirmation messages before current-term commit: %+v", r.msgs)
	}
}

// TestReadIndexReleasedAfterCurrentTermCommit verifies queued reads enter
// quorum confirmation at the commit index that establishes the leader's term.
func TestReadIndexReleasedAfterCurrentTermCommit(t *testing.T) {
	r := newUncommittedLeader(t)
	ctx := []byte("after-noop-commit")

	if err := r.Step(Message{Type: MsgReadIndex, Context: ctx}); err != nil {
		t.Fatalf("readindex: %v", err)
	}
	if err := r.Step(Message{
		Type: MsgAppendResp, From: 2, To: 1, Term: r.Term, LogIndex: 1,
	}); err != nil {
		t.Fatalf("append response: %v", err)
	}
	if r.raftLog.committed != 1 {
		t.Fatalf("committed = %d, want 1", r.raftLog.committed)
	}
	if len(r.pendingReadIndexMessages) != 0 {
		t.Fatalf("pending reads not released after current-term commit")
	}
	if len(r.readStates) != 0 {
		t.Fatalf("read released before heartbeat quorum: %+v", r.readStates)
	}

	if err := r.Step(Message{
		Type: MsgHeartbeatResp, From: 2, To: 1, Term: r.Term, Context: ctx,
	}); err != nil {
		t.Fatalf("heartbeat response: %v", err)
	}
	if len(r.readStates) != 1 {
		t.Fatalf("readStates = %d, want 1", len(r.readStates))
	}
	if r.readStates[0].Index != 1 {
		t.Fatalf("read index = %d, want current-term commit 1", r.readStates[0].Index)
	}
	if !bytes.Equal(r.readStates[0].RequestCtx, ctx) {
		t.Fatalf("read ctx = %q, want %q", r.readStates[0].RequestCtx, ctx)
	}
}

// TestQueuedReadIndexDroppedAfterLeadershipLoss verifies a request still
// behind the current-term gate cannot survive into a later leadership term.
func TestQueuedReadIndexDroppedAfterLeadershipLoss(t *testing.T) {
	r := newUncommittedLeader(t)
	ctx := []byte("stale-before-noop")

	if err := r.Step(Message{Type: MsgReadIndex, Context: ctx}); err != nil {
		t.Fatalf("readindex: %v", err)
	}
	if err := r.Step(Message{
		Type: MsgHeartbeat, From: 2, To: 1, Term: r.Term + 1,
	}); err != nil {
		t.Fatalf("higher-term heartbeat: %v", err)
	}
	if r.state != StateFollower {
		t.Fatalf("state = %s, want follower", r.state)
	}
	if len(r.pendingReadIndexMessages) != 0 {
		t.Fatalf("queued read survived leadership loss")
	}

	r.becomeCandidate()
	r.becomeLeader()
	if err := r.Step(Message{
		Type: MsgAppendResp, From: 2, To: 1, Term: r.Term, LogIndex: r.raftLog.lastIndex(),
	}); err != nil {
		t.Fatalf("append response in new term: %v", err)
	}
	if err := r.Step(Message{
		Type: MsgHeartbeatResp, From: 2, To: 1, Term: r.Term, Context: ctx,
	}); err != nil {
		t.Fatalf("stale heartbeat response: %v", err)
	}
	if len(r.readStates) != 0 {
		t.Fatalf("stale queued read released in a later term: %+v", r.readStates)
	}
}

func TestQueuedReadIndexReleaseReadyOrdersPersistenceBeforeHeartbeat(t *testing.T) {
	r := newUncommittedLeader(t)
	n := &Node{r: r}

	// Persist the current-term no-op before simulating a follower's
	// acknowledgement, exactly as a real driver processes Ready.
	initial := n.Ready()
	if !initial.MustSync || len(initial.Entries) != 1 || initial.Entries[0].Term != r.Term {
		t.Fatalf("initial Ready does not durably publish current-term no-op: %+v", initial)
	}
	if initial.HardState != nil {
		if err := r.raftLog.storage.SetHardState(*initial.HardState); err != nil {
			t.Fatalf("persist initial hard state: %v", err)
		}
	}
	if err := r.raftLog.storage.AppendEntries(initial.Entries); err != nil {
		t.Fatalf("persist current-term no-op: %v", err)
	}
	n.Advance(PersistAck{})

	ctx := []byte("ready-order")
	if err := n.Step(Message{Type: MsgReadIndex, Context: ctx}); err != nil {
		t.Fatalf("readindex: %v", err)
	}
	if n.HasReady() {
		t.Fatal("queued read became externally visible before current-term commit")
	}
	if err := n.Step(Message{
		Type: MsgAppendResp, From: 2, To: 1, Term: r.Term, LogIndex: 1,
	}); err != nil {
		t.Fatalf("append response: %v", err)
	}

	released := n.Ready()
	if released.HardState == nil || released.HardState.Commit != 1 {
		t.Fatalf("released Ready HardState = %+v, want commit 1", released.HardState)
	}
	foundContext := false
	for _, msg := range released.Messages {
		if msg.Type == MsgHeartbeat && bytes.Equal(msg.Context, ctx) {
			foundContext = true
		}
	}
	if !foundContext {
		t.Fatalf("released Ready has no confirming heartbeat for %q: %+v", ctx, released.Messages)
	}
	if len(released.ReadStates) != 0 {
		t.Fatalf("read released before heartbeat quorum: %+v", released.ReadStates)
	}
}

func newUncommittedLeader(t *testing.T) *raft {
	t.Helper()
	r := newTestRaft(t, 1, []uint64{1, 2, 3}, false)
	r.becomeCandidate()
	r.becomeLeader()
	if r.raftLog.committed != 0 {
		t.Fatalf("new leader committed = %d, want 0", r.raftLog.committed)
	}
	return r
}
