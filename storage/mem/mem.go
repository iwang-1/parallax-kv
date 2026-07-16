// Package mem provides the in-memory LogStorage used by the deterministic
// simulator. It models durability: state written through the LogStorage
// interface is "persisted" and survives a simulated Crash/Restart, while
// everything else about the node is discarded — exactly the split a real
// crash imposes.
package mem

import (
	"github.com/iwang-1/parallax-kv/raft"
)

// Storage is an in-memory raft.LogStorage. It is not safe for concurrent
// use; the simulator is single-goroutine by construction.
type Storage struct {
	hardState raft.HardState
	snapshot  raft.Snapshot
	// entries[i] holds the entry at index snapshot.Metadata.Index + 1 + i.
	entries []raft.Entry
}

var _ raft.LogStorage = (*Storage)(nil)

// New returns an empty Storage.
func New() *Storage {
	return &Storage{}
}

// offset is the index of the last entry covered by the snapshot; the first
// individually available index is offset+1.
func (s *Storage) offset() uint64 {
	return s.snapshot.Metadata.Index
}

// AppendEntries implements raft.LogStorage. Incoming entries must be
// contiguous with the existing log; a conflicting suffix (an incoming index
// that already exists) is truncated before the new entries are stored.
// Entries at or below the snapshot index are silently dropped as already
// durable.
func (s *Storage) AppendEntries(entries []raft.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	off := s.offset()
	first := entries[0].Index
	last := entries[len(entries)-1].Index

	// Everything is already covered by the snapshot: nothing to do.
	if last <= off {
		return nil
	}
	// Drop any prefix already covered by the snapshot.
	if first <= off {
		entries = entries[off+1-first:]
		first = entries[0].Index
	}
	// pos is the slot in s.entries where the first incoming entry belongs.
	pos := first - (off + 1)
	if pos > uint64(len(s.entries)) {
		return raft.ErrUnavailable
	}
	// Truncate any conflicting suffix and append.
	s.entries = append(s.entries[:pos], entries...)
	return nil
}

// SetHardState implements raft.LogStorage.
func (s *Storage) SetHardState(st raft.HardState) error {
	s.hardState = st
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

// ApplySnapshot implements raft.LogStorage. It installs snap, discarding
// entries it covers. A stale snapshot (index <= the current snapshot) is
// rejected with raft.ErrCompacted.
func (s *Storage) ApplySnapshot(snap raft.Snapshot) error {
	if snap.Metadata.Index <= s.offset() {
		return raft.ErrCompacted
	}
	// A matching (index, term) boundary proves the existing suffix belongs to
	// the same log. A term mismatch means the entire suffix is divergent.
	off := s.offset()
	last := off + uint64(len(s.entries))
	if snap.Metadata.Index < last &&
		s.entries[snap.Metadata.Index-off-1].Term == snap.Metadata.Term {
		s.entries = append([]raft.Entry(nil), s.entries[snap.Metadata.Index-off:]...)
	} else {
		s.entries = nil
	}
	s.snapshot = snap
	return nil
}

// Snapshot implements raft.LogStorage.
func (s *Storage) Snapshot() (raft.Snapshot, error) {
	return s.snapshot, nil
}

// Compact discards entries through index (they remain represented by the
// snapshot). It is a simulator hook for compaction scenarios and only
// updates the snapshot metadata; the snapshot Data is left untouched, so
// callers that need snapshot content must ApplySnapshot instead.
func (s *Storage) Compact(index uint64) error {
	off := s.offset()
	if index <= off {
		return raft.ErrCompacted
	}
	last := off + uint64(len(s.entries))
	if index > last {
		return raft.ErrUnavailable
	}
	term := s.entries[index-off-1].Term
	s.entries = append([]raft.Entry(nil), s.entries[index-off:]...)
	s.snapshot.Metadata.Index = index
	s.snapshot.Metadata.Term = term
	return nil
}
