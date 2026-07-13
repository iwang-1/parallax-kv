package mem

import (
	"errors"
	"reflect"
	"testing"

	"github.com/iwang-1/parallax-kv/raft"
)

func ents(idxTerms ...[2]uint64) []raft.Entry {
	out := make([]raft.Entry, 0, len(idxTerms))
	for _, it := range idxTerms {
		out = append(out, raft.Entry{Index: it[0], Term: it[1]})
	}
	return out
}

func TestEmptyBounds(t *testing.T) {
	s := New()
	if fi, _ := s.FirstIndex(); fi != 1 {
		t.Fatalf("FirstIndex = %d, want 1", fi)
	}
	if li, _ := s.LastIndex(); li != 0 {
		t.Fatalf("LastIndex = %d, want 0", li)
	}
	if hs, _ := s.HardState(); (hs != raft.HardState{}) {
		t.Fatalf("HardState = %+v, want zero", hs)
	}
}

func TestAppendAndRead(t *testing.T) {
	s := New()
	if err := s.AppendEntries(ents([2]uint64{1, 1}, [2]uint64{2, 1}, [2]uint64{3, 2})); err != nil {
		t.Fatal(err)
	}
	if li, _ := s.LastIndex(); li != 3 {
		t.Fatalf("LastIndex = %d, want 3", li)
	}
	got, err := s.Entries(1, 4)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, ents([2]uint64{1, 1}, [2]uint64{2, 1}, [2]uint64{3, 2})) {
		t.Fatalf("Entries = %+v", got)
	}
	if tm, _ := s.Term(3); tm != 2 {
		t.Fatalf("Term(3) = %d, want 2", tm)
	}
}

func TestAppendTruncatesConflict(t *testing.T) {
	s := New()
	s.AppendEntries(ents([2]uint64{1, 1}, [2]uint64{2, 1}, [2]uint64{3, 1}))
	// Overwrite from index 2 with a higher-term suffix.
	if err := s.AppendEntries(ents([2]uint64{2, 2}, [2]uint64{3, 2})); err != nil {
		t.Fatal(err)
	}
	if li, _ := s.LastIndex(); li != 3 {
		t.Fatalf("LastIndex = %d, want 3", li)
	}
	if tm, _ := s.Term(2); tm != 2 {
		t.Fatalf("Term(2) = %d, want 2", tm)
	}
}

func TestEntriesErrors(t *testing.T) {
	s := New()
	s.AppendEntries(ents([2]uint64{1, 1}, [2]uint64{2, 1}))
	if _, err := s.Entries(3, 5); !errors.Is(err, raft.ErrUnavailable) {
		t.Fatalf("Entries past end err = %v, want ErrUnavailable", err)
	}
	if _, err := s.Term(9); !errors.Is(err, raft.ErrUnavailable) {
		t.Fatalf("Term past end err = %v, want ErrUnavailable", err)
	}
}

func TestSnapshotAndCompact(t *testing.T) {
	s := New()
	s.AppendEntries(ents([2]uint64{1, 1}, [2]uint64{2, 1}, [2]uint64{3, 2}, [2]uint64{4, 2}))
	snap := raft.Snapshot{Metadata: raft.SnapshotMetadata{Index: 2, Term: 1}, Data: []byte("x")}
	if err := s.ApplySnapshot(snap); err != nil {
		t.Fatal(err)
	}
	if fi, _ := s.FirstIndex(); fi != 3 {
		t.Fatalf("FirstIndex = %d, want 3", fi)
	}
	if _, err := s.Entries(2, 3); !errors.Is(err, raft.ErrCompacted) {
		t.Fatalf("Entries into snapshot err = %v, want ErrCompacted", err)
	}
	// Term at the snapshot boundary is served from metadata.
	if tm, _ := s.Term(2); tm != 1 {
		t.Fatalf("Term(2) = %d, want 1", tm)
	}
	if tm, _ := s.Term(4); tm != 2 {
		t.Fatalf("Term(4) = %d, want 2", tm)
	}
	// Stale snapshot rejected.
	if err := s.ApplySnapshot(snap); !errors.Is(err, raft.ErrCompacted) {
		t.Fatalf("stale ApplySnapshot err = %v, want ErrCompacted", err)
	}
	// Compact within the log.
	if err := s.Compact(3); err != nil {
		t.Fatal(err)
	}
	if fi, _ := s.FirstIndex(); fi != 4 {
		t.Fatalf("after Compact FirstIndex = %d, want 4", fi)
	}
}

func TestApplySnapshotBeyondLog(t *testing.T) {
	s := New()
	s.AppendEntries(ents([2]uint64{1, 1}, [2]uint64{2, 1}))
	snap := raft.Snapshot{Metadata: raft.SnapshotMetadata{Index: 5, Term: 3}}
	if err := s.ApplySnapshot(snap); err != nil {
		t.Fatal(err)
	}
	if fi, _ := s.FirstIndex(); fi != 6 {
		t.Fatalf("FirstIndex = %d, want 6", fi)
	}
	if li, _ := s.LastIndex(); li != 5 {
		t.Fatalf("LastIndex = %d, want 5", li)
	}
}
