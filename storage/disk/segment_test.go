package disk

import (
	"os"
	"strings"
	"testing"

	"github.com/iwang-1/parallax-kv/raft"
)

func countSegments(t *testing.T, dir string) int {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), "wal-") && strings.HasSuffix(e.Name(), ".seg") {
			n++
		}
	}
	return n
}

// withSmallSegments shrinks maxSegmentSize for the duration of a test so a
// handful of records force rotation.
func withSmallSegments(t *testing.T, size int64) {
	t.Helper()
	orig := maxSegmentSize
	maxSegmentSize = size
	t.Cleanup(func() { maxSegmentSize = orig })
}

func TestSegmentRotationAndReplay(t *testing.T) {
	withSmallSegments(t, 32) // rotate almost every Sync
	dir := t.TempDir()
	s := mustOpen(t, dir)
	for i := uint64(1); i <= 6; i++ {
		if err := s.AppendEntries([]raft.Entry{mkEntry(i, 1, "payload")}); err != nil {
			t.Fatal(err)
		}
		if err := s.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	s.Close()
	if countSegments(t, dir) < 2 {
		t.Fatalf("expected multiple segments, got %d", countSegments(t, dir))
	}

	s2 := mustOpen(t, dir)
	defer s2.Close()
	if li, _ := s2.LastIndex(); li != 6 {
		t.Fatalf("LastIndex = %d, want 6 across segments", li)
	}
	got, err := s2.Entries(1, 7)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 6 {
		t.Fatalf("got %d entries, want 6", len(got))
	}
}

func TestTornTailInEarlierSegmentDropsLaterSegments(t *testing.T) {
	withSmallSegments(t, 32)
	dir := t.TempDir()
	s := mustOpen(t, dir)
	for i := uint64(1); i <= 6; i++ {
		s.AppendEntries([]raft.Entry{mkEntry(i, 1, "payload")})
		s.Sync()
	}
	s.Close()
	if countSegments(t, dir) < 3 {
		t.Skipf("need >=3 segments to test orphan drop, got %d", countSegments(t, dir))
	}

	// Corrupt the LAST byte of the FIRST segment. Recovery must stop there and
	// discard every later segment (they are orphaned by the crash), so only
	// entries in the first segment survive.
	seg := activeSegmentPathN(t, dir, 0)
	data, err := os.ReadFile(seg)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 0xFF
	if err := os.WriteFile(seg, data, 0o644); err != nil {
		t.Fatal(err)
	}

	before := countSegments(t, dir)
	s2 := mustOpen(t, dir)
	defer s2.Close()
	after := countSegments(t, dir)
	if after >= before {
		t.Fatalf("expected orphaned segments removed: before=%d after=%d", before, after)
	}
	// The log must still be a clean contiguous prefix and appendable.
	li, _ := s2.LastIndex()
	if err := s2.AppendEntries([]raft.Entry{mkEntry(li+1, 2, "next")}); err != nil {
		t.Fatalf("append after orphan recovery: %v", err)
	}
	if err := s2.Sync(); err != nil {
		t.Fatal(err)
	}
}

// activeSegmentPathN returns the n-th (0-based, sorted) WAL segment in dir.
func activeSegmentPathN(t *testing.T, dir string, n int) string {
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
	if len(segs) <= n {
		t.Fatalf("want segment %d, only %d present", n, len(segs))
	}
	// os.ReadDir already returns sorted names; zero-padded => numeric order.
	return dir + string(os.PathSeparator) + segs[n]
}
