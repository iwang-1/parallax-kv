package disk

import (
	"errors"
	"testing"

	"github.com/iwang-1/parallax-kv/raft"
)

func TestAppendEntriesEmptyIsNoop(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	defer s.Close()
	if err := s.AppendEntries(nil); err != nil {
		t.Fatal(err)
	}
	if li, _ := s.LastIndex(); li != 0 {
		t.Fatalf("LastIndex = %d, want 0", li)
	}
}

func TestSyncNoopWhenNothingBuffered(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	defer s.Close()
	if err := s.Sync(); err != nil {
		t.Fatalf("empty Sync: %v", err)
	}
}

func TestAppendGapReturnsUnavailable(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	defer s.Close()
	s.AppendEntries([]raft.Entry{mkEntry(1, 1, "a")})
	// Index 3 leaves a hole at 2.
	if err := s.AppendEntries([]raft.Entry{mkEntry(3, 1, "c")}); !errors.Is(err, raft.ErrUnavailable) {
		t.Fatalf("gap append err = %v, want ErrUnavailable", err)
	}
}

func TestEntriesLoEqualsHiEmpty(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	defer s.Close()
	s.AppendEntries([]raft.Entry{mkEntry(1, 1, "a"), mkEntry(2, 1, "b")})
	got, err := s.Entries(2, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("Entries(2,2) = %+v, want empty", got)
	}
}

func TestEntriesReversedRangeUnavailable(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	defer s.Close()
	s.AppendEntries([]raft.Entry{mkEntry(1, 1, "a"), mkEntry(2, 1, "b")})
	if _, err := s.Entries(3, 2); !errors.Is(err, raft.ErrUnavailable) {
		t.Fatalf("reversed range err = %v, want ErrUnavailable", err)
	}
}

func TestAppendPrefixBelowSnapshotDropped(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	defer s.Close()
	s.AppendEntries([]raft.Entry{mkEntry(1, 1, "a"), mkEntry(2, 1, "b"), mkEntry(3, 1, "c")})
	s.Sync()
	if err := s.ApplySnapshot(raft.Snapshot{Metadata: raft.SnapshotMetadata{Index: 2, Term: 1}}); err != nil {
		t.Fatal(err)
	}
	// Re-append entries [2,3,4]: index 2 is covered by the snapshot and must
	// be dropped, leaving a contiguous log with 3 overwritten and 4 added.
	if err := s.AppendEntries([]raft.Entry{mkEntry(2, 1, "b"), mkEntry(3, 2, "C"), mkEntry(4, 2, "d")}); err != nil {
		t.Fatal(err)
	}
	if tm, _ := s.Term(3); tm != 2 {
		t.Fatalf("Term(3) = %d, want 2", tm)
	}
	if li, _ := s.LastIndex(); li != 4 {
		t.Fatalf("LastIndex = %d, want 4", li)
	}
}

func TestAppendFullyBelowSnapshotNoop(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	defer s.Close()
	s.AppendEntries([]raft.Entry{mkEntry(1, 1, "a"), mkEntry(2, 1, "b")})
	s.Sync()
	s.ApplySnapshot(raft.Snapshot{Metadata: raft.SnapshotMetadata{Index: 5, Term: 3}})
	if err := s.AppendEntries([]raft.Entry{mkEntry(3, 1, "c")}); err != nil {
		t.Fatalf("append below snapshot: %v", err)
	}
	if li, _ := s.LastIndex(); li != 5 {
		t.Fatalf("LastIndex = %d, want 5 (append below snapshot ignored)", li)
	}
}

func TestTermCompactedAndSnapshotBoundary(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	defer s.Close()
	s.AppendEntries([]raft.Entry{mkEntry(1, 1, "a"), mkEntry(2, 1, "b"), mkEntry(3, 2, "c")})
	s.Sync()
	s.ApplySnapshot(raft.Snapshot{Metadata: raft.SnapshotMetadata{Index: 2, Term: 1}})
	if _, err := s.Term(1); !errors.Is(err, raft.ErrCompacted) {
		t.Fatalf("Term(1) err = %v, want ErrCompacted", err)
	}
	if tm, _ := s.Term(2); tm != 1 {
		t.Fatalf("Term(2) = %d, want 1 (snapshot boundary)", tm)
	}
	if _, err := s.Term(9); !errors.Is(err, raft.ErrUnavailable) {
		t.Fatalf("Term(9) err = %v, want ErrUnavailable", err)
	}
}

func TestSetHardStateOnlySync(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	s.SetHardState(raft.HardState{Term: 5, Vote: 2, Commit: 0})
	s.Sync()
	s.Close()
	s2 := mustOpen(t, dir)
	defer s2.Close()
	if hs, _ := s2.HardState(); hs != (raft.HardState{Term: 5, Vote: 2}) {
		t.Fatalf("HardState = %+v", hs)
	}
}

func TestDecodeTornBodies(t *testing.T) {
	if _, err := decodeEntry([]byte{1, 2, 3}); err != errTornFrame {
		t.Fatalf("short entry body err = %v, want errTornFrame", err)
	}
	if _, err := decodeHardState([]byte{1, 2, 3}); err != errTornFrame {
		t.Fatalf("short hardstate body err = %v, want errTornFrame", err)
	}
	// Entry whose declared dataLen disagrees with the body length.
	bad := encodeEntry(mkEntry(1, 1, "abc"))[1:]
	bad = bad[:len(bad)-1] // drop a data byte without fixing dataLen
	if _, err := decodeEntry(bad); err != errTornFrame {
		t.Fatalf("dataLen mismatch err = %v, want errTornFrame", err)
	}
}

func TestParseSnapPayloadTorn(t *testing.T) {
	if _, err := parseSnapPayload([]byte{1, 2, 3}); err != errTornFrame {
		t.Fatalf("short snap payload err = %v, want errTornFrame", err)
	}
}

func TestSnapshotPayloadTruncationIntentAndLegacyCompatibility(t *testing.T) {
	snap := raft.Snapshot{
		Metadata: raft.SnapshotMetadata{Index: 5, Term: 3},
		Data:     []byte("state"),
	}
	payload := snapPayload(snap, true)
	stored, err := parseSnapPayload(payload)
	if err != nil {
		t.Fatal(err)
	}
	if stored.snapshot.Metadata != snap.Metadata || string(stored.snapshot.Data) != "state" || !stored.truncateSuffix {
		t.Fatalf("stored snapshot = %+v, truncate=%v", stored.snapshot, stored.truncateSuffix)
	}

	legacy, err := parseSnapPayload(payload[:len(payload)-1])
	if err != nil {
		t.Fatal(err)
	}
	if legacy.snapshot.Metadata != snap.Metadata || string(legacy.snapshot.Data) != "state" {
		t.Fatalf("legacy snapshot = %+v, want %+v", legacy.snapshot, snap)
	}
	if legacy.truncateSuffix {
		t.Fatal("legacy snapshot unexpectedly requested suffix truncation")
	}
}

func TestParseSegmentSeqBad(t *testing.T) {
	if _, err := parseSegmentSeq("not-a-segment"); err == nil {
		t.Fatal("expected error for bad segment name")
	}
}
