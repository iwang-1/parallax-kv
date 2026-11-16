package disk

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// snapshotsKept is how many of the newest snapshot files CompactWAL leaves on disk. loadLatestSnapshot falls
// back to an older file when the newest is unreadable, so one older snapshot is kept alongside it.
const snapshotsKept = 2

// CompactWAL deletes WAL segments and snapshot files that the current snapshot makes unnecessary, and
// reports how many of each it removed. It reclaims disk space after a snapshot; without it, segments
// accumulate forever even though the in-memory log is trimmed.
//
// Recovery replays every remaining segment after loading the newest snapshot, and treats a gap between the
// snapshot index and the first replayed entry as a torn log. So CompactWAL deletes only a prefix of
// segments, never the active one, and only segments whose every entry record is at or below the snapshot
// index. Two more records live in segments besides entries, and both are handled first:
//
//   - The hard state (term, vote, commit) is re-appended to the active segment and fsynced, so the newest
//     hard state survives the deletion of whatever older segment held it.
//   - A snapshot that conflicted with the local log is paired with a truncation marker in the WAL, and
//     recovery clears the log suffix if the marker is missing. CompactWAL does nothing while the newest
//     snapshot is of that kind (or its marker is not yet durable); a later compaction snapshot, which never
//     needs a marker, re-enables it.
func (s *Storage) CompactWAL() (segmentsRemoved, snapshotsRemoved int, err error) {
	if s.snapshotTruncationPending || s.latestSnapshotTruncated {
		return 0, 0, nil
	}
	snapIndex := s.snapshot.Metadata.Index
	if snapIndex == 0 {
		return 0, 0, nil
	}
	segs, err := s.listSegments()
	if err != nil {
		return 0, 0, err
	}
	var doomed []string
	for _, name := range segs {
		seq, err := parseSegmentSeq(name)
		if err != nil {
			return 0, 0, err
		}
		if seq >= s.segSeq || s.segMaxIndex[seq] > snapIndex {
			break // the active segment, or the first segment still holding entries past the snapshot
		}
		doomed = append(doomed, name)
	}

	snaps, err := s.listSnapshotFiles()
	if err != nil {
		return 0, 0, err
	}
	var oldSnaps []string
	if len(snaps) > snapshotsKept {
		oldSnaps = snaps[:len(snaps)-snapshotsKept]
	}
	if len(doomed) == 0 && len(oldSnaps) == 0 {
		return 0, 0, nil
	}

	if len(doomed) > 0 {
		// Make the newest hard state durable in the active segment before any older copy can be deleted.
		s.buf = appendFrame(s.buf, encodeHardState(s.hardState))
		if err := s.syncBuffered(true); err != nil {
			return 0, 0, err
		}
	}
	for _, name := range doomed {
		if err := os.Remove(filepath.Join(s.dir, name)); err != nil {
			return 0, 0, fmt.Errorf("disk: remove compacted segment %s: %w", name, err)
		}
		seq, _ := parseSegmentSeq(name)
		delete(s.segMaxIndex, seq)
	}
	for _, name := range oldSnaps {
		if err := os.Remove(filepath.Join(s.dir, name)); err != nil {
			return 0, 0, fmt.Errorf("disk: remove old snapshot %s: %w", name, err)
		}
	}
	if err := fsyncDir(s.dir); err != nil {
		return 0, 0, err
	}
	return len(doomed), len(oldSnaps), nil
}

// listSnapshotFiles returns snapshot file names, oldest first.
func (s *Storage) listSnapshotFiles() ([]string, error) {
	ents, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("disk: readdir %s: %w", s.dir, err)
	}
	var names []string
	for _, e := range ents {
		n := e.Name()
		if !e.IsDir() && strings.HasPrefix(n, "snap-") && strings.HasSuffix(n, ".snap") {
			names = append(names, n)
		}
	}
	sort.Strings(names) // zero-padded index => lexical == numeric order
	return names, nil
}

// noteSegmentEntry records that segment seq holds an entry record at index.
func (s *Storage) noteSegmentEntry(seq, index uint64) {
	if s.segMaxIndex == nil {
		s.segMaxIndex = make(map[uint64]uint64)
	}
	if index > s.segMaxIndex[seq] {
		s.segMaxIndex[seq] = index
	}
}
