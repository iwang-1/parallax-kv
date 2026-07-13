package raft

import "errors"

// Sentinel errors returned by LogStorage implementations and by Node.Step.
var (
	// ErrCompacted is returned when a requested index has been compacted
	// into a snapshot and is no longer individually available.
	ErrCompacted = errors.New("raft: requested index is compacted")
	// ErrUnavailable is returned when a requested index is beyond the
	// last index in the log.
	ErrUnavailable = errors.New("raft: requested index is unavailable")
	// ErrNotLeader is returned by Step for proposals and ReadIndex
	// requests stepped into a node that is not the leader.
	ErrNotLeader = errors.New("raft: node is not the leader")
)

// LogStorage is the durable state the Raft core reads from and the driver
// writes to. The core only READS through this interface during operation;
// all writes (AppendEntries, SetHardState, ApplySnapshot) are performed by
// the driver while processing a Ready batch, keeping the core free of I/O.
//
// Implementations: storage/disk (segmented WAL + snapshot files) for
// production, storage/mem for the deterministic simulator.
//
// Index conventions: the log conceptually starts at index 1. FirstIndex is
// the first index still individually available (everything below it lives
// only in the snapshot); LastIndex is the highest index written. Term(i)
// is defined for i in [FirstIndex-1, LastIndex] — FirstIndex-1 is served
// from snapshot metadata.
type LogStorage interface {
	// AppendEntries persists new log entries. Entries must be
	// contiguous with the existing log; on conflict (an incoming entry's
	// index already exists with a different term) the implementation
	// truncates the old suffix first.
	AppendEntries(entries []Entry) error
	// SetHardState persists the hard state.
	SetHardState(st HardState) error
	// HardState returns the last persisted hard state (zero value if
	// none), used to recover a node after restart.
	HardState() (HardState, error)
	// Entries returns entries in [lo, hi). It returns ErrCompacted if lo
	// has been compacted, ErrUnavailable if hi-1 is past the last index.
	Entries(lo, hi uint64) ([]Entry, error)
	// Term returns the term of the entry at index i (ErrCompacted /
	// ErrUnavailable as for Entries).
	Term(i uint64) (uint64, error)
	// FirstIndex returns the first individually available index.
	FirstIndex() (uint64, error)
	// LastIndex returns the last written index (0 for an empty log).
	LastIndex() (uint64, error)
	// ApplySnapshot replaces the log prefix with a snapshot, discarding
	// entries covered by it.
	ApplySnapshot(snap Snapshot) error
	// Snapshot returns the most recent snapshot (zero value if none).
	Snapshot() (Snapshot, error)
}
