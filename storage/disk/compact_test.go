package disk

import (
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"

	"github.com/iwang-1/parallax-kv/raft"
)

func countSnapshotFiles(t *testing.T, dir string) int {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), "snap-") && strings.HasSuffix(e.Name(), ".snap") {
			n++
		}
	}
	return n
}

// appendSynced appends entries lo..hi (term 1), one Sync per entry.
func appendSynced(t *testing.T, s *Storage, lo, hi uint64) {
	t.Helper()
	for i := lo; i <= hi; i++ {
		if err := s.AppendEntries([]raft.Entry{mkEntry(i, 1, fmt.Sprintf("payload-%d", i))}); err != nil {
			t.Fatal(err)
		}
		if err := s.Sync(); err != nil {
			t.Fatal(err)
		}
	}
}

// compactAt snapshots at index (term 1, as a state machine would after applying it) and compacts the WAL.
func compactAt(t *testing.T, s *Storage, index uint64) (int, int) {
	t.Helper()
	snap := raft.Snapshot{
		Metadata: raft.SnapshotMetadata{Index: index, Term: 1},
		Data:     []byte(fmt.Sprintf("state@%d", index)),
	}
	if err := s.ApplySnapshot(snap); err != nil {
		t.Fatal(err)
	}
	segs, snaps, err := s.CompactWAL()
	if err != nil {
		t.Fatal(err)
	}
	return segs, snaps
}

func assertEntries(t *testing.T, s *Storage, lo, hi uint64) {
	t.Helper()
	got, err := s.Entries(lo, hi+1)
	if err != nil {
		t.Fatalf("Entries(%d, %d): %v", lo, hi+1, err)
	}
	if uint64(len(got)) != hi-lo+1 {
		t.Fatalf("got %d entries, want %d", len(got), hi-lo+1)
	}
	for i, e := range got {
		want := lo + uint64(i)
		if e.Index != want || string(e.Data) != fmt.Sprintf("payload-%d", want) {
			t.Fatalf("entry %d = index %d data %q", want, e.Index, e.Data)
		}
	}
}

func TestCompactWALRemovesCoveredSegmentsAndRecovers(t *testing.T) {
	withSmallSegments(t, 64)
	dir := t.TempDir()
	s := mustOpen(t, dir)
	if err := s.SetHardState(raft.HardState{Term: 7, Vote: 2, Commit: 1}); err != nil {
		t.Fatal(err)
	}
	appendSynced(t, s, 1, 40)
	before := countSegments(t, dir)

	removed, _ := compactAt(t, s, 30)
	if removed == 0 {
		t.Fatalf("expected segments covered by index 30 to be removed (had %d segments)", before)
	}
	if after := countSegments(t, dir); after != before-removed {
		t.Fatalf("segments: before %d, removed %d, after %d", before, removed, after)
	}
	appendSynced(t, s, 41, 45)
	s.Close()

	s2 := mustOpen(t, dir)
	defer s2.Close()
	if fi, _ := s2.FirstIndex(); fi != 31 {
		t.Fatalf("FirstIndex = %d, want 31", fi)
	}
	if li, _ := s2.LastIndex(); li != 45 {
		t.Fatalf("LastIndex = %d, want 45", li)
	}
	assertEntries(t, s2, 31, 45)
	// The hard state was written once, before every entry, so its only original copy was in the first
	// segment, which compaction deleted.
	hs, _ := s2.HardState()
	if hs != (raft.HardState{Term: 7, Vote: 2, Commit: 1}) {
		t.Fatalf("hard state after compaction = %+v", hs)
	}
}

func TestCompactWALKeepsSegmentsHoldingEntriesPastTheSnapshot(t *testing.T) {
	withSmallSegments(t, 1<<20) // one segment for everything
	dir := t.TempDir()
	s := mustOpen(t, dir)
	appendSynced(t, s, 1, 20)
	if removed, _ := compactAt(t, s, 10); removed != 0 {
		t.Fatalf("removed %d segments, but the only segment is active and holds 11..20", removed)
	}
	s.Close()
	s2 := mustOpen(t, dir)
	defer s2.Close()
	assertEntries(t, s2, 11, 20)
}

func TestCompactWALSkipsWhileSnapshotNeedsTruncationMarker(t *testing.T) {
	withSmallSegments(t, 64)
	dir := t.TempDir()
	s := mustOpen(t, dir)
	appendSynced(t, s, 1, 30)
	// A leader's snapshot whose term conflicts with the local entry at its index: it relies on the WAL
	// truncation marker, so no segment may be deleted while it is the newest snapshot.
	snap := raft.Snapshot{Metadata: raft.SnapshotMetadata{Index: 20, Term: 9}, Data: []byte("x")}
	if err := s.ApplySnapshot(snap); err != nil {
		t.Fatal(err)
	}
	before := countSegments(t, dir)
	if removed, _, err := s.CompactWAL(); err != nil || removed != 0 {
		t.Fatalf("CompactWAL after a conflicting snapshot removed %d (err %v)", removed, err)
	}
	if countSegments(t, dir) != before {
		t.Fatal("segment count changed")
	}
	s.Close()
}

func TestCompactWALKeepsTwoNewestSnapshots(t *testing.T) {
	withSmallSegments(t, 64)
	dir := t.TempDir()
	s := mustOpen(t, dir)
	defer s.Close()
	appendSynced(t, s, 1, 50)
	for _, idx := range []uint64{10, 20, 30, 40} {
		compactAt(t, s, idx)
	}
	if n := countSnapshotFiles(t, dir); n != snapshotsKept {
		t.Fatalf("%d snapshot files on disk, want %d", n, snapshotsKept)
	}
}

// Randomized: interleave appends, compaction snapshots, and reopen-from-disk, and check after every reopen
// that the log is exactly the entries above the newest snapshot, with nothing lost or invented.
func TestCompactWALRandomizedReopen(t *testing.T) {
	withSmallSegments(t, 96)
	for seed := int64(1); seed <= 20; seed++ {
		rng := rand.New(rand.NewSource(seed))
		dir := t.TempDir()
		s := mustOpen(t, dir)
		var last, snapIdx uint64
		for step := 0; step < 60; step++ {
			switch r := rng.Intn(10); {
			case r < 6:
				n := uint64(1 + rng.Intn(4))
				appendSynced(t, s, last+1, last+n)
				last += n
			case r < 9:
				if last > snapIdx {
					idx := snapIdx + 1 + uint64(rng.Int63n(int64(last-snapIdx)))
					compactAt(t, s, idx)
					snapIdx = idx
				}
			default:
				s.Close()
				s = mustOpen(t, dir)
			}
		}
		s.Close()
		s = mustOpen(t, dir)
		if fi, _ := s.FirstIndex(); fi != snapIdx+1 {
			t.Fatalf("seed %d: FirstIndex %d, want %d", seed, fi, snapIdx+1)
		}
		if li, _ := s.LastIndex(); li != last {
			t.Fatalf("seed %d: LastIndex %d, want %d", seed, li, last)
		}
		if last > snapIdx {
			assertEntries(t, s, snapIdx+1, last)
		}
		s.Close()
	}
}
