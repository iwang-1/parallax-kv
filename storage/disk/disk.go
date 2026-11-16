// Package disk is the production LogStorage: a segmented write-ahead log plus snapshot files.
package disk

import (
	"fmt"
	"os"
	"sync/atomic"

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

	// NoSync, when set, makes Sync write the buffered records to the segment but skip the durability fsync.
	noSync bool

	// walSyncs counts successful WAL segment fsyncs. Atomic so tests and metrics can read it while the
	// drive loop writes.
	walSyncs atomic.Uint64

	// segMaxIndex maps a segment sequence number to the highest entry index recorded in it (absent: no
	// entries), so CompactWAL can tell which segments a snapshot fully covers.
	segMaxIndex map[uint64]uint64
	// bufMaxIndex is the highest entry index in buf, credited to whichever segment buf is written to.
	bufMaxIndex uint64
	// replaySeq is the segment being replayed during Open.
	replaySeq uint64
	// segLimit is the size at which the active segment rotates on the next Sync (0: maxSegmentSize).
	segLimit int64
	// latestSnapshotTruncated is set when the newest snapshot conflicted with the local log and so relies
	// on a WAL truncation marker; CompactWAL leaves segments alone while it is set.
	latestSnapshotTruncated bool
}

// WALSyncs returns how many WAL segment fsyncs have completed. Safe to call from any goroutine.
func (s *Storage) WALSyncs() uint64 { return s.walSyncs.Load() }

// SetSegmentLimit sets the WAL segment size at which the active segment rotates on its next Sync. Smaller
// segments let CompactWAL reclaim space sooner after a snapshot, at the cost of more files. Zero restores
// the default (16 MiB).
func (s *Storage) SetSegmentLimit(bytes int64) { s.segLimit = bytes }

func (s *Storage) segmentLimit() int64 {
	if s.segLimit > 0 {
		return s.segLimit
	}
	return maxSegmentSize
}

// DisableSync switches the storage into the UNSAFE no-fsync mode described on
// the noSync field. Intended only for the [W2] benchmark variant.
func (s *Storage) DisableSync() { s.noSync = true }

var _ raft.LogStorage = (*Storage)(nil)

// MaxSegmentSize triggers rotation to a fresh segment on the next Sync once the active segment reaches this
// size.
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

// AppendEntries implements raft.LogStorage. It updates the in-memory log and buffers a record per entry;
// the records are not durable until Sync.
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
	if last > s.bufMaxIndex {
		s.bufMaxIndex = last
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
