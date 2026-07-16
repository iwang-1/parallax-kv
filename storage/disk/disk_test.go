package disk

import (
	"errors"
	"math/rand"
	"os"
	"os/exec"
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

func seedSnapshotLog(t *testing.T, s *Storage) {
	t.Helper()
	if err := s.AppendEntries([]raft.Entry{
		mkEntry(1, 1, "a"),
		mkEntry(2, 2, "b"),
		mkEntry(3, 2, "c"),
		mkEntry(4, 3, "d"),
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
}

func requireSnapshotBoundaryOnly(t *testing.T, s *Storage, index uint64) {
	t.Helper()
	if last, _ := s.LastIndex(); last != index {
		t.Fatalf("LastIndex = %d, want snapshot boundary %d", last, index)
	}
	if _, err := s.Term(index + 1); !errors.Is(err, raft.ErrUnavailable) {
		t.Fatalf("Term(%d) err = %v, want ErrUnavailable", index+1, err)
	}
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

// TestDisableSyncStillWritesRecords covers the UNSAFE benchmark mode: with the
// fsync disabled, Sync must still write the buffered records to the segment (so
// a graceful reopen recovers them) — it only skips the durability barrier. This
// is the [W2] path; it is never used in production.
func TestDisableSyncStillWritesRecords(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	s.DisableSync()
	if err := s.SetHardState(raft.HardState{Term: 2, Vote: 1, Commit: 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendEntries([]raft.Entry{mkEntry(1, 1, "x"), mkEntry(2, 2, "y")}); err != nil {
		t.Fatal(err)
	}
	if err := s.Sync(); err != nil {
		t.Fatalf("Sync (no fsync): %v", err)
	}
	s.Close()

	// A graceful close flushes the file; the records are in the segment even
	// though no fsync was issued, so a reopen recovers them.
	s2 := mustOpen(t, dir)
	defer s2.Close()
	if li, _ := s2.LastIndex(); li != 2 {
		t.Fatalf("LastIndex = %d, want 2 (records must reach the segment even without fsync)", li)
	}
	if hs, _ := s2.HardState(); hs != (raft.HardState{Term: 2, Vote: 1, Commit: 1}) {
		t.Fatalf("HardState = %+v, want {2 1 1}", hs)
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

func TestApplySnapshotMatchingBoundaryRetainsSuffix(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	defer s.Close()
	seedSnapshotLog(t, s)

	if err := s.ApplySnapshot(raft.Snapshot{
		Metadata: raft.SnapshotMetadata{Index: 2, Term: 2},
	}); err != nil {
		t.Fatal(err)
	}

	got, err := s.Entries(3, 5)
	if err != nil {
		t.Fatal(err)
	}
	want := []raft.Entry{mkEntry(3, 2, "c"), mkEntry(4, 3, "d")}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Entries = %+v, want %+v", got, want)
	}
}

func TestApplySnapshotMismatchingBoundaryDropsSuffix(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	defer s.Close()
	seedSnapshotLog(t, s)

	if err := s.ApplySnapshot(raft.Snapshot{
		Metadata: raft.SnapshotMetadata{Index: 2, Term: 7},
	}); err != nil {
		t.Fatal(err)
	}

	requireSnapshotBoundaryOnly(t, s, 2)
}

func TestSnapshotMismatchTruncationSurvivesImmediateReopen(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	seedSnapshotLog(t, s)
	if err := s.ApplySnapshot(raft.Snapshot{
		Metadata: raft.SnapshotMetadata{Index: 2, Term: 7},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := mustOpen(t, dir)
	defer reopened.Close()
	requireSnapshotBoundaryOnly(t, reopened, 2)
}

func TestSnapshotMismatchReplacementSuffixSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	seedSnapshotLog(t, s)
	if err := s.ApplySnapshot(raft.Snapshot{
		Metadata: raft.SnapshotMetadata{Index: 2, Term: 7},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendEntries([]raft.Entry{mkEntry(3, 7, "replacement")}); err != nil {
		t.Fatal(err)
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := mustOpen(t, dir)
	defer reopened.Close()
	if last, _ := reopened.LastIndex(); last != 3 {
		t.Fatalf("LastIndex = %d, want replacement index 3", last)
	}
	if term, err := reopened.Term(3); err != nil || term != 7 {
		t.Fatalf("Term(3) = %d, %v; want 7, nil", term, err)
	}
}

const snapshotCrashHelperDir = "PARALLAX_SNAPSHOT_CRASH_HELPER_DIR"

func TestSnapshotMismatchTruncationSurvivesCrashRecovery(t *testing.T) {
	if dir := os.Getenv(snapshotCrashHelperDir); dir != "" {
		s := mustOpen(t, dir)
		seedSnapshotLog(t, s)
		if err := s.ApplySnapshot(raft.Snapshot{
			Metadata: raft.SnapshotMetadata{Index: 2, Term: 7},
		}); err != nil {
			t.Fatal(err)
		}
		os.Exit(0) // Simulate abrupt process loss without Storage.Close.
	}

	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestSnapshotMismatchTruncationSurvivesCrashRecovery$", "-test.count=1")
	cmd.Env = append(os.Environ(), snapshotCrashHelperDir+"="+dir)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("crash helper: %v\n%s", err, output)
	}

	recovered := mustOpen(t, dir)
	defer recovered.Close()
	requireSnapshotBoundaryOnly(t, recovered, 2)
}

const snapshotOrderingCrashHelperDir = "PARALLAX_SNAPSHOT_ORDERING_CRASH_HELPER_DIR"

func TestSnapshotMismatchCrashBetweenSnapshotAndWALDoesNotResurrect(t *testing.T) {
	if dir := os.Getenv(snapshotOrderingCrashHelperDir); dir != "" {
		s := mustOpen(t, dir)
		seedSnapshotLog(t, s)
		s.testHooks.afterSnapshotFileDurable = func() {
			os.Exit(0) // Crash before the WAL truncation record is buffered.
		}
		if err := s.ApplySnapshot(raft.Snapshot{
			Metadata: raft.SnapshotMetadata{Index: 2, Term: 7},
		}); err != nil {
			t.Fatal(err)
		}
		t.Fatal("snapshot durability hook did not terminate helper")
	}

	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestSnapshotMismatchCrashBetweenSnapshotAndWALDoesNotResurrect$", "-test.count=1")
	cmd.Env = append(os.Environ(), snapshotOrderingCrashHelperDir+"="+dir)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("crash helper: %v\n%s", err, output)
	}

	recovered := mustOpen(t, dir)
	requireSnapshotBoundaryOnly(t, recovered, 2)
	if err := recovered.Close(); err != nil {
		t.Fatal(err)
	}

	// Recovery durably completes the missing marker before returning.
	reopened := mustOpen(t, dir)
	defer reopened.Close()
	requireSnapshotBoundaryOnly(t, reopened, 2)
}

func TestApplySnapshotFailureKeepsMirrorCoherent(t *testing.T) {
	snap := raft.Snapshot{Metadata: raft.SnapshotMetadata{Index: 2, Term: 7}}

	t.Run("before snapshot durability", func(t *testing.T) {
		dir := t.TempDir()
		s := mustOpen(t, dir)
		seedSnapshotLog(t, s)
		injected := errors.New("injected snapshot write failure")
		s.testHooks.beforeSnapshotWrite = func() error { return injected }

		if err := s.ApplySnapshot(snap); !errors.Is(err, injected) {
			t.Fatalf("ApplySnapshot err = %v, want injected failure", err)
		}
		if got, _ := s.Snapshot(); got.Metadata != (raft.SnapshotMetadata{}) {
			t.Fatalf("Snapshot = %+v, want old zero snapshot", got)
		}
		if last, _ := s.LastIndex(); last != 4 {
			t.Fatalf("LastIndex = %d, want unchanged 4", last)
		}
		if term, err := s.Term(4); err != nil || term != 3 {
			t.Fatalf("Term(4) = %d, %v; want 3, nil", term, err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}

		reopened := mustOpen(t, dir)
		defer reopened.Close()
		if last, _ := reopened.LastIndex(); last != 4 {
			t.Fatalf("reopened LastIndex = %d, want unchanged 4", last)
		}
	})

	t.Run("after snapshot durability before WAL sync", func(t *testing.T) {
		dir := t.TempDir()
		s := mustOpen(t, dir)
		seedSnapshotLog(t, s)
		injected := errors.New("injected truncation sync failure")
		s.testHooks.beforeTruncationSync = func() error { return injected }

		if err := s.ApplySnapshot(snap); !errors.Is(err, injected) {
			t.Fatalf("ApplySnapshot err = %v, want injected failure", err)
		}
		if got, _ := s.Snapshot(); got.Metadata != snap.Metadata {
			t.Fatalf("Snapshot = %+v, want durable snapshot %+v", got, snap)
		}
		requireSnapshotBoundaryOnly(t, s, 2)
		if !s.snapshotTruncationPending {
			t.Fatal("snapshot truncation should remain pending after sync failure")
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}

		reopened := mustOpen(t, dir)
		defer reopened.Close()
		requireSnapshotBoundaryOnly(t, reopened, 2)
	})
}

func TestRecoveredSnapshotMismatchCannotCampaignWithRemovedSuffix(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	seedSnapshotLog(t, s)
	if err := s.ApplySnapshot(raft.Snapshot{
		Metadata: raft.SnapshotMetadata{Index: 2, Term: 7},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	recovered := mustOpen(t, dir)
	defer recovered.Close()
	node, err := raft.NewNode(raft.Config{
		ID:             1,
		Peers:          []uint64{1, 2, 3},
		ElectionTicks:  2,
		HeartbeatTicks: 1,
		PreVote:        true,
		Rand:           rand.New(rand.NewSource(1)),
	}, recovered)
	if err != nil {
		t.Fatal(err)
	}

	foundCampaign := false
	for tick := 0; tick < 2*2 && !foundCampaign; tick++ {
		node.Tick()
		for node.HasReady() {
			rd := node.Ready()
			for _, msg := range rd.Messages {
				if msg.Type != raft.MsgPreVote && msg.Type != raft.MsgVote {
					continue
				}
				foundCampaign = true
				if msg.LogIndex != 2 || msg.LogTerm != 7 {
					t.Fatalf("campaign advertised (%d,%d), want snapshot boundary (2,7)", msg.LogIndex, msg.LogTerm)
				}
			}
			node.Advance(raft.PersistAck{})
		}
	}
	if !foundCampaign {
		t.Fatal("node did not campaign")
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
