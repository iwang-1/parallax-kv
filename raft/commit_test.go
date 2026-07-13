package raft

import "testing"

// TestFigure8CommitRule is the regression test for the Raft Figure-8 hazard:
// a leader must NOT consider an entry from a PRIOR term committed merely
// because it is stored on a majority; it may only be committed indirectly,
// once an entry from the leader's CURRENT term reaches a majority.
func TestFigure8CommitRule(t *testing.T) {
	r := newTestRaft(t, 1, []uint64{1, 2, 3}, false)

	// Term 1: a single entry exists at index 1, replicated to a majority
	// (nodes 1 and 2). The leader is now at term 2.
	r.becomeCandidate() // term 1
	r.becomeLeader()    // appends noop at index 1, term 1
	// Simulate advancing to a new term without committing index 1.
	r.Term = 2
	r.prs = map[uint64]*progress{
		1: {Match: 1, Next: 2},
		2: {Match: 1, Next: 2}, // index 1 (term 1) on a majority
		3: {Match: 0, Next: 2},
	}
	r.raftLog.committed = 0

	// A majority stores the term-1 entry, but maybeCommit MUST refuse to
	// commit it because it is from a prior term.
	if r.maybeCommit() {
		t.Fatalf("prior-term entry must not be committed by replica count")
	}
	if r.raftLog.committed != 0 {
		t.Fatalf("committed = %d, want 0 (Figure-8 restriction)", r.raftLog.committed)
	}

	// Now the leader appends a current-term (term 2) entry at index 2 and it
	// reaches a majority. Committing index 2 (current term) also carries
	// index 1 forward — the safe, indirect commit.
	r.appendEntry(Entry{Type: EntryNormal, Data: []byte("cur")})
	r.prs[2].maybeUpdate(2) // node 2 stores index 2
	if !r.maybeCommit() {
		t.Fatalf("current-term entry on majority must commit")
	}
	if r.raftLog.committed != 2 {
		t.Fatalf("committed = %d, want 2", r.raftLog.committed)
	}
}

// TestCommitAdvancesByMedianMatch verifies the commit index tracks the
// majority-matched index (the median of Match values) for current-term
// entries.
func TestCommitAdvancesByMedianMatch(t *testing.T) {
	r := newTestRaft(t, 1, []uint64{1, 2, 3, 4, 5}, false)
	r.becomeCandidate()
	r.becomeLeader() // noop at index 1, term 1
	r.appendEntry(Entry{Type: EntryNormal})
	r.appendEntry(Entry{Type: EntryNormal}) // indices 2,3 term 1

	// Matches: leader has 3; two followers have 3, two have 1. Median (3rd of
	// 5 sorted) = 3, so commit should reach 3.
	r.prs[2].maybeUpdate(3)
	r.prs[3].maybeUpdate(3)
	r.prs[4].maybeUpdate(1)
	r.prs[5].maybeUpdate(1)
	if !r.maybeCommit() {
		t.Fatalf("expected commit to advance")
	}
	if r.raftLog.committed != 3 {
		t.Fatalf("committed = %d, want 3", r.raftLog.committed)
	}

	// Drop one of the higher followers: only leader + one follower have 3.
	// Median is now 1 (already committed 3, so no regression, but a fresh
	// leader computation would cap at min majority). Verify commit never
	// regresses.
	before := r.raftLog.committed
	r.prs[3] = &progress{Match: 1, Next: 2}
	r.maybeCommit()
	if r.raftLog.committed != before {
		t.Fatalf("commit regressed from %d to %d", before, r.raftLog.committed)
	}
}
