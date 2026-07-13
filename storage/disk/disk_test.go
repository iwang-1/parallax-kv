package disk

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/iwang-1/parallax-kv/raft"
)

func mkEntry(idx, term uint64, data string) raft.Entry {
	return raft.Entry{Term: term, Index: idx, Type: raft.EntryNormal, Data: []byte(data)}
}

func mustOpen(t *testing.T, dir string) *Storage {
	t.Helper()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open(%s): %v", dir, err)
	}
	return s
}

func TestOpenEmpty(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	defer s.Close()
	if fi, _ := s.FirstIndex(); fi != 1 {
		t.Fatalf("FirstIndex = %d, want 1", fi)
	}
	if li, _ := s.LastIndex(); li != 0 {
		t.Fatalf("LastIndex = %d, want 0", li)
	}
}

func TestAppendSyncReopen(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	if err := s.SetHardState(raft.HardState{Term: 3, Vote: 1, Commit: 2}); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendEntries([]raft.Entry{
		mkEntry(1, 1, "a"), mkEntry(2, 2, "b"), mkEntry(3, 3, "c"),
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	s.Close()

	// Reopen: durable state must survive.
	s2 := mustOpen(t, dir)
	defer s2.Close()
	if hs, _ := s2.HardState(); hs != (raft.HardState{Term: 3, Vote: 1, Commit: 2}) {
		t.Fatalf("HardState = %+v", hs)
	}
	if li, _ := s2.LastIndex(); li != 3 {
		t.Fatalf("LastIndex = %d, want 3", li)
	}
	got, err := s2.Entries(1, 4)
	if err != nil {
		t.Fatal(err)
	}
	want := []raft.Entry{mkEntry(1, 1, "a"), mkEntry(2, 2, "b"), mkEntry(3, 3, "c")}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Entries = %+v, want %+v", got, want)
	}
}

func TestUnsyncedWritesLostOnCrash(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	s.AppendEntries([]raft.Entry{mkEntry(1, 1, "a")})
	s.Sync()
	// Buffer more, but do NOT Sync — simulate a crash by abandoning s.
	s.AppendEntries([]raft.Entry{mkEntry(2, 1, "b")})
	s.Close()

	s2 := mustOpen(t, dir)
	defer s2.Close()
	if li, _ := s2.LastIndex(); li != 1 {
		t.Fatalf("LastIndex = %d, want 1 (unsynced write must be lost)", li)
	}
}

func TestTruncationOnReplay(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	s.AppendEntries([]raft.Entry{mkEntry(1, 1, "a"), mkEntry(2, 1, "b"), mkEntry(3, 1, "c")})
	s.Sync()
	// Overwrite suffix from index 2 with a higher term (a conflicting append).
	s.AppendEntries([]raft.Entry{mkEntry(2, 2, "B"), mkEntry(3, 2, "C")})
	s.Sync()
	s.Close()

	s2 := mustOpen(t, dir)
	defer s2.Close()
	if tm, _ := s2.Term(2); tm != 2 {
		t.Fatalf("Term(2) = %d, want 2 after conflict replay", tm)
	}
	got, _ := s2.Entries(1, 4)
	want := []raft.Entry{mkEntry(1, 1, "a"), mkEntry(2, 2, "B"), mkEntry(3, 2, "C")}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Entries = %+v, want %+v", got, want)
	}
}

// activeSegmentPath returns the single WAL segment in dir (tests write few
// enough records to stay in one segment).
func activeSegmentPath(t *testing.T, dir string) string {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var segs []string
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), "wal-") && strings.HasSuffix(e.Name(), ".seg") {
			segs = append(segs, e.Name())
		}
	}
	sort.Strings(segs)
	if len(segs) == 0 {
		t.Fatal("no segment file found")
	}
	return filepath.Join(dir, segs[len(segs)-1])
}

func TestTornTailPartialRecord(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	s.AppendEntries([]raft.Entry{mkEntry(1, 1, "a"), mkEntry(2, 1, "b")})
	s.Sync()
	s.Close()

	// Append garbage bytes (a torn frame from a mid-write crash) to the tail.
	seg := activeSegmentPath(t, dir)
	f, err := os.OpenFile(seg, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	// Frame header claiming a large payload, but only a few bytes follow.
	f.Write([]byte{0x00, 0x00, 0x10, 0x00, 0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02})
	f.Close()

	s2 := mustOpen(t, dir)
	defer s2.Close()
	if li, _ := s2.LastIndex(); li != 2 {
		t.Fatalf("LastIndex = %d, want 2 (torn tail must be truncated)", li)
	}
	// After truncation the segment must be appendable and durable again.
	if err := s2.AppendEntries([]raft.Entry{mkEntry(3, 1, "c")}); err != nil {
		t.Fatal(err)
	}
	if err := s2.Sync(); err != nil {
		t.Fatal(err)
	}
	s2.Close()
	s3 := mustOpen(t, dir)
	defer s3.Close()
	if li, _ := s3.LastIndex(); li != 3 {
		t.Fatalf("after recovery+append LastIndex = %d, want 3", li)
	}
}

func TestTornTailCorruptCRC(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	s.AppendEntries([]raft.Entry{mkEntry(1, 1, "a"), mkEntry(2, 1, "b"), mkEntry(3, 1, "c")})
	s.Sync()
	s.Close()

	// Flip the final byte of the last record: its CRC now fails, so recovery
	// must treat it as a torn tail and drop only that record.
	seg := activeSegmentPath(t, dir)
	data, err := os.ReadFile(seg)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 0xFF
	if err := os.WriteFile(seg, data, 0o644); err != nil {
		t.Fatal(err)
	}

	s2 := mustOpen(t, dir)
	defer s2.Close()
	if li, _ := s2.LastIndex(); li != 2 {
		t.Fatalf("LastIndex = %d, want 2 (corrupt last record dropped)", li)
	}
}

func TestSnapshotAtomicRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	s.AppendEntries([]raft.Entry{mkEntry(1, 1, "a"), mkEntry(2, 1, "b"), mkEntry(3, 2, "c"), mkEntry(4, 2, "d")})
	s.Sync()
	snap := raft.Snapshot{Metadata: raft.SnapshotMetadata{Index: 2, Term: 1}, Data: []byte("statemachine")}
	if err := s.ApplySnapshot(snap); err != nil {
		t.Fatal(err)
	}
	if fi, _ := s.FirstIndex(); fi != 3 {
		t.Fatalf("FirstIndex = %d, want 3", fi)
	}
	s.Close()

	s2 := mustOpen(t, dir)
	defer s2.Close()
	got, err := s2.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if got.Metadata != snap.Metadata || string(got.Data) != "statemachine" {
		t.Fatalf("Snapshot = %+v, want %+v", got, snap)
	}
	if fi, _ := s2.FirstIndex(); fi != 3 {
		t.Fatalf("after reopen FirstIndex = %d, want 3", fi)
	}
	// Entries beyond the snapshot survive replay.
	if tm, _ := s2.Term(4); tm != 2 {
		t.Fatalf("Term(4) = %d, want 2", tm)
	}
	if _, err := s2.Entries(2, 3); !errors.Is(err, raft.ErrCompacted) {
		t.Fatalf("Entries into snapshot err = %v, want ErrCompacted", err)
	}
}

func TestStaleSnapshotRejected(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	defer s.Close()
	if err := s.ApplySnapshot(raft.Snapshot{Metadata: raft.SnapshotMetadata{Index: 5, Term: 2}}); err != nil {
		t.Fatal(err)
	}
	if err := s.ApplySnapshot(raft.Snapshot{Metadata: raft.SnapshotMetadata{Index: 3, Term: 1}}); !errors.Is(err, raft.ErrCompacted) {
		t.Fatalf("stale ApplySnapshot err = %v, want ErrCompacted", err)
	}
}

func TestTornSnapshotFallsBack(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	if err := s.ApplySnapshot(raft.Snapshot{Metadata: raft.SnapshotMetadata{Index: 2, Term: 1}, Data: []byte("first")}); err != nil {
		t.Fatal(err)
	}
	if err := s.ApplySnapshot(raft.Snapshot{Metadata: raft.SnapshotMetadata{Index: 4, Term: 2}, Data: []byte("second")}); err != nil {
		t.Fatal(err)
	}
	s.Close()

	// Corrupt the newest snapshot file; recovery must fall back to the older
	// intact snapshot rather than losing the base entirely.
	newest := filepath.Join(dir, snapshotName(4))
	data, err := os.ReadFile(newest)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 0xFF
	if err := os.WriteFile(newest, data, 0o644); err != nil {
		t.Fatal(err)
	}

	s2 := mustOpen(t, dir)
	defer s2.Close()
	got, _ := s2.Snapshot()
	if got.Metadata.Index != 2 || string(got.Data) != "first" {
		t.Fatalf("Snapshot = %+v, want fallback to index 2", got)
	}
}

func TestNoTmpFileLeftBehind(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	defer s.Close()
	if err := s.ApplySnapshot(raft.Snapshot{Metadata: raft.SnapshotMetadata{Index: 2, Term: 1}, Data: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	ents, _ := os.ReadDir(dir)
	for _, e := range ents {
		if strings.HasSuffix(e.Name(), ".tmp") {
			t.Fatalf("leftover tmp file: %s", e.Name())
		}
	}
}
