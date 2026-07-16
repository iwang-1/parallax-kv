package sim

import (
	"strings"
	"testing"

	"github.com/iwang-1/parallax-kv/raft"
)

func newTrackedTestStorage(t *testing.T, entries []raft.Entry, commitTerm, commit uint64) *trackedStorage {
	t.Helper()
	storage := trackStorage(defaultStorageFactory(0)).(*trackedStorage)
	if len(entries) > 0 {
		if err := storage.AppendEntries(entries); err != nil {
			t.Fatalf("append entries: %v", err)
		}
	}
	if commit > 0 {
		if err := storage.SetHardState(raft.HardState{Term: commitTerm, Commit: commit}); err != nil {
			t.Fatalf("set hard state: %v", err)
		}
	}
	return storage
}

func invariantTestNode(id, leader, term uint64, storage raft.LogStorage) *nodeState {
	return &nodeState{
		id:      id,
		storage: storage,
		node:    &mockNode{id: id, leader: leader, term: term},
	}
}

func committedFixture(t *testing.T) (*invariants, map[uint64]*nodeState, *trackedStorage) {
	t.Helper()
	committed := []raft.Entry{{Index: 1, Term: 1, Data: []byte("committed")}}
	source := newTrackedTestStorage(t, committed, 1, 1)
	leader := newTrackedTestStorage(t, nil, 0, 0)
	nodes := map[uint64]*nodeState{
		1: invariantTestNode(1, 2, 2, source),
		2: invariantTestNode(2, 2, 2, leader),
	}
	return newInvariants([]uint64{1, 2}), nodes, leader
}

func TestLeaderCompletenessMissingEntryFails(t *testing.T) {
	iv, nodes, _ := committedFixture(t)

	err := iv.checkLeaderCompleteness(nodes)
	if err == nil {
		t.Fatal("missing committed entry passed leader completeness")
	}
	if !strings.Contains(err.Error(), "missing committed index 1") {
		t.Fatalf("unexpected error: %v", err)
	}
	if nodes[2].applied != 0 {
		t.Fatalf("test leader unexpectedly applied through %d", nodes[2].applied)
	}
}

func TestLeaderCompletenessConflictingEntryFails(t *testing.T) {
	iv, nodes, leader := committedFixture(t)
	if err := leader.AppendEntries([]raft.Entry{{
		Index: 1,
		Term:  1,
		Data:  []byte("conflict"),
	}}); err != nil {
		t.Fatalf("append conflicting entry: %v", err)
	}

	err := iv.checkLeaderCompleteness(nodes)
	if err == nil {
		t.Fatal("conflicting committed entry passed leader completeness")
	}
	if !strings.Contains(err.Error(), "conflicting index 1") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLeaderCompletenessSnapshotCoveredAbsencePasses(t *testing.T) {
	iv, nodes, leader := committedFixture(t)
	if err := leader.ApplySnapshot(raft.Snapshot{
		Metadata: raft.SnapshotMetadata{Index: 1, Term: 1},
	}); err != nil {
		t.Fatalf("apply snapshot: %v", err)
	}

	if err := iv.checkLeaderCompleteness(nodes); err != nil {
		t.Fatalf("snapshot-covered committed entry failed leader completeness: %v", err)
	}
}

func TestSnapshotOnlyCommitIsTrackedAndTermBackfilled(t *testing.T) {
	source := newTrackedTestStorage(t, nil, 0, 0)
	if err := source.ApplySnapshot(raft.Snapshot{
		Metadata: raft.SnapshotMetadata{Index: 3, Term: 2},
		Data:     []byte("committed-state"),
	}); err != nil {
		t.Fatalf("apply source snapshot: %v", err)
	}
	if err := source.SetHardState(raft.HardState{Term: 4, Commit: 3}); err != nil {
		t.Fatalf("persist snapshot hard state: %v", err)
	}
	records := storageCommitted(source)
	if len(records) != 6 {
		t.Fatalf("snapshot commit records = %d, want 6 (three initial and three enriched)", len(records))
	}
	for i, rec := range records[3:] {
		if rec.index != uint64(i+1) || rec.commitTerm != 4 {
			t.Fatalf("enriched record %d = %+v, want index %d committed in term 4", i, rec, i+1)
		}
	}

	leader := newTrackedTestStorage(t, nil, 0, 0)
	nodes := map[uint64]*nodeState{
		1: invariantTestNode(1, 2, 4, source),
		2: invariantTestNode(2, 2, 4, leader),
	}
	err := newInvariants([]uint64{1, 2}).checkLeaderCompleteness(nodes)
	if err == nil || !strings.Contains(err.Error(), "missing committed index 1") {
		t.Fatalf("snapshot-only committed prefix was not enforced: %v", err)
	}
}

func TestSnapshotAgreementRejectsDivergentState(t *testing.T) {
	left := newTrackedTestStorage(t, nil, 0, 0)
	right := newTrackedTestStorage(t, nil, 0, 0)
	fixtures := []struct {
		storage *trackedStorage

		data []byte
	}{
		{storage: left, data: []byte("left-state")},
		{storage: right, data: []byte("right-state")},
	}
	for _, fixture := range fixtures {
		if err := fixture.storage.ApplySnapshot(raft.Snapshot{
			Metadata: raft.SnapshotMetadata{Index: 3, Term: 2},
			Data:     fixture.data,
		}); err != nil {
			t.Fatalf("apply snapshot: %v", err)
		}
	}
	nodes := map[uint64]*nodeState{
		1: invariantTestNode(1, 0, 0, left),
		2: invariantTestNode(2, 0, 0, right),
	}

	err := newInvariants([]uint64{1, 2}).checkLogMatching(nodes)
	if err == nil || !strings.Contains(err.Error(), "snapshot agreement violated") {
		t.Fatalf("divergent snapshots were not rejected: %v", err)
	}
}

func TestStorageGenerationInvalidatesInteriorRewrite(t *testing.T) {
	entries := []raft.Entry{
		{Index: 1, Term: 1, Data: []byte("one")},
		{Index: 2, Term: 1, Data: []byte("two")},
		{Index: 3, Term: 1, Data: []byte("three")},
	}
	source := newTrackedTestStorage(t, entries, 1, 3)
	leader := newTrackedTestStorage(t, entries, 0, 0)
	nodes := map[uint64]*nodeState{
		1: invariantTestNode(1, 2, 2, source),
		2: invariantTestNode(2, 2, 2, leader),
	}
	iv := newInvariants([]uint64{1, 2})

	if err := iv.check(0, nodes); err != nil {
		t.Fatalf("initial check: %v", err)
	}
	before := storageGeneration(leader)
	if err := leader.AppendEntries([]raft.Entry{
		{Index: 2, Term: 9, Data: []byte("rewritten")},
		{Index: 3, Term: 1, Data: []byte("three")},
	}); err != nil {
		t.Fatalf("rewrite interior entry: %v", err)
	}
	if after := storageGeneration(leader); after <= before {
		t.Fatalf("storage generation did not advance: before=%d after=%d", before, after)
	}

	err := iv.check(1, nodes)
	if err == nil {
		t.Fatal("same-length interior rewrite was hidden by checker caches")
	}
	if !strings.Contains(err.Error(), "leader completeness violated") ||
		!strings.Contains(err.Error(), "conflicting index 2") {
		t.Fatalf("unexpected error after interior rewrite: %v", err)
	}
}

func TestStorageGenerationInvalidatesUncommittedLogRewrite(t *testing.T) {
	entries := []raft.Entry{
		{Index: 1, Term: 1, Data: []byte("one")},
		{Index: 2, Term: 1, Data: []byte("two")},
	}
	left := newTrackedTestStorage(t, entries, 0, 0)
	right := newTrackedTestStorage(t, entries, 0, 0)
	nodes := map[uint64]*nodeState{
		1: invariantTestNode(1, 0, 0, left),
		2: invariantTestNode(2, 0, 0, right),
	}
	iv := newInvariants([]uint64{1, 2})
	if err := iv.checkLogMatching(nodes); err != nil {
		t.Fatalf("initial log matching: %v", err)
	}

	if err := right.AppendEntries([]raft.Entry{
		{Index: 1, Term: 1, Data: []byte("rewritten")},
		{Index: 2, Term: 1, Data: []byte("two")},
	}); err != nil {
		t.Fatalf("rewrite log: %v", err)
	}
	err := iv.checkLogMatching(nodes)
	if err == nil || !strings.Contains(err.Error(), "log matching violated") {
		t.Fatalf("same-length uncommitted rewrite was hidden by cache: %v", err)
	}
}
