// Package disk is the production LogStorage: a segmented write-ahead log
// plus snapshot files.
//
// WAL format: append-only segment files (wal-<seq>.seg) of length-prefixed,
// CRC32C-framed records (frame = {payloadLen, crc32c} header + payload). A
// record's payload is a type tag followed by a type-specific body; the log
// carries entry appends, hard-state updates, and explicit suffix truncations.
// The WAL is a logical redo log: replaying every record in order — an entry
// record installs its entry at its index (truncating any conflicting suffix),
// a hard-state record overwrites the hard state, and a truncation record drops
// entries above its index — reconstructs the durable state. Conflicting
// appends truncate implicitly; snapshot boundary mismatches use an explicit
// record because no replacement entries may follow immediately.
//
// Durability is group commit: writes are buffered in memory and flushed by a
// single fsync per Ready batch (Sync), covering every record the batch wrote
// — this is where write throughput comes from (see docs/DESIGN_NOTES.md).
//
// Recovery scans segments in order, verifying CRCs; a torn tail (a partial or
// corrupt final record from a mid-write crash) marks the logical end of the
// log and is truncated, never trusted. Snapshots are written atomically: tmp
// file, fsync, rename, fsync parent directory.
package disk

import (
	"fmt"
	"os"

	"github.com/iwang-1/parallax-kv/raft"
)

// Storage is the durable raft.LogStorage. Not safe for concurrent use; the
// server's drive loop owns it.
type Storage struct {
	dir string

	// In-memory mirror of durable state, the source of truth for reads.
	hardState raft.HardState
	snapshot  raft.Snapshot
	// snapshotTruncationPending is set while recovery still needs the WAL
	// marker named by a mismatch snapshot file.
	snapshotTruncationPending bool
	// entries[i] holds the entry at index snapshot.Metadata.Index + 1 + i.
	entries   []raft.Entry
	testHooks snapshotTestHooks

	// buf accumulates framed records written since the last Sync.
	buf []byte

	// Active segment being appended to.
	seg     *os.File
	segSize int64
	segSeq  uint64 // sequence number of the active segment

	// noSync, when set, makes Sync write the buffered records to the segment
	// but skip the durability fsync. This is the UNSAFE benchmark mode: it
	// measures the throughput ceiling with the durability barrier removed, to
	// quantify what group-commit fsync costs. It is NEVER used in production;
	// a crash can lose acknowledged writes.
	noSync bool
}

// DisableSync switches the storage into the UNSAFE no-fsync mode described on
// the noSync field. Intended only for the [W2] benchmark variant.
func (s *Storage) DisableSync() { s.noSync = true }

var _ raft.LogStorage = (*Storage)(nil)

// maxSegmentSize triggers rotation to a fresh segment on the next Sync once
// the active segment reaches this size. It is a var (not a const) only so
// tests can shrink it to exercise multi-segment rotation and recovery; it is
// never mutated in production.
var maxSegmentSize int64 = 16 << 20 // 16 MiB

// offset is the index of the last entry covered by the snapshot.
func (s *Storage) offset() uint64 {
	return s.snapshot.Metadata.Index
}

// Close releases file handles. The storage is unusable afterwards.
func (s *Storage) Close() error {
	if s.seg == nil {
		return nil
	}
	err := s.seg.Close()
	s.seg = nil
	return err
}

// AppendEntries implements raft.LogStorage. It updates the in-memory log and
// buffers a record per entry; the records are not durable until Sync. Entries
// must be contiguous with the existing log; a conflicting suffix is truncated
// first. Entries at or below the snapshot index are dropped as already
// durable.
func (s *Storage) AppendEntries(entries []raft.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	off := s.offset()
	first := entries[0].Index
	last := entries[len(entries)-1].Index
	if last <= off {
		return nil
	}
	if first <= off {
		entries = entries[off+1-first:]
		first = entries[0].Index
	}
	pos := first - (off + 1)
	if pos > uint64(len(s.entries)) {
		return raft.ErrUnavailable
	}
	s.entries = append(s.entries[:pos], entries...)
	for i := range entries {
		s.buf = appendFrame(s.buf, encodeEntry(entries[i]))
	}
	return nil
}

// SetHardState implements raft.LogStorage. Buffered until Sync.
func (s *Storage) SetHardState(st raft.HardState) error {
	s.hardState = st
	s.buf = appendFrame(s.buf, encodeHardState(st))
	return nil
}

// HardState implements raft.LogStorage.
func (s *Storage) HardState() (raft.HardState, error) {
	return s.hardState, nil
}

// Entries implements raft.LogStorage, returning entries in [lo, hi).
func (s *Storage) Entries(lo, hi uint64) ([]raft.Entry, error) {
	off := s.offset()
	if lo <= off {
		return nil, raft.ErrCompacted
	}
	last := off + uint64(len(s.entries))
	if hi > last+1 {
		return nil, raft.ErrUnavailable
	}
	if lo > hi {
		return nil, raft.ErrUnavailable
	}
	if lo == hi {
		return nil, nil
	}
	out := make([]raft.Entry, hi-lo)
	copy(out, s.entries[lo-off-1:hi-off-1])
	return out, nil
}

// Term implements raft.LogStorage.
func (s *Storage) Term(i uint64) (uint64, error) {
	off := s.offset()
	if i == off {
		return s.snapshot.Metadata.Term, nil
	}
	if i < off {
		return 0, raft.ErrCompacted
	}
	if i > off+uint64(len(s.entries)) {
		return 0, raft.ErrUnavailable
	}
	return s.entries[i-off-1].Term, nil
}

// FirstIndex implements raft.LogStorage.
func (s *Storage) FirstIndex() (uint64, error) {
	return s.offset() + 1, nil
}

// LastIndex implements raft.LogStorage.
func (s *Storage) LastIndex() (uint64, error) {
	return s.offset() + uint64(len(s.entries)), nil
}

// segmentName returns the file name for segment sequence seq.
func segmentName(seq uint64) string {
	return fmt.Sprintf("wal-%020d.seg", seq)
}
