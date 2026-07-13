// Package mem provides the in-memory LogStorage used by the deterministic
// simulator. It models durability: state written through the LogStorage
// interface is "persisted" and survives a simulated Crash/Restart, while
// everything else about the node is discarded — exactly the split a real
// crash imposes.
package mem

import "github.com/iwang-1/parallax-kv/raft"

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

// AppendEntries implements raft.LogStorage.
func (s *Storage) AppendEntries(entries []raft.Entry) error {
	// TODO(S1)
	panic("storage/mem: AppendEntries not implemented (stage S1)")
}

// SetHardState implements raft.LogStorage.
func (s *Storage) SetHardState(st raft.HardState) error {
	// TODO(S1)
	panic("storage/mem: SetHardState not implemented (stage S1)")
}

// HardState implements raft.LogStorage.
func (s *Storage) HardState() (raft.HardState, error) {
	// TODO(S1)
	panic("storage/mem: HardState not implemented (stage S1)")
}

// Entries implements raft.LogStorage.
func (s *Storage) Entries(lo, hi uint64) ([]raft.Entry, error) {
	// TODO(S1)
	panic("storage/mem: Entries not implemented (stage S1)")
}

// Term implements raft.LogStorage.
func (s *Storage) Term(i uint64) (uint64, error) {
	// TODO(S1)
	panic("storage/mem: Term not implemented (stage S1)")
}

// FirstIndex implements raft.LogStorage.
func (s *Storage) FirstIndex() (uint64, error) {
	// TODO(S1)
	panic("storage/mem: FirstIndex not implemented (stage S1)")
}

// LastIndex implements raft.LogStorage.
func (s *Storage) LastIndex() (uint64, error) {
	// TODO(S1)
	panic("storage/mem: LastIndex not implemented (stage S1)")
}

// ApplySnapshot implements raft.LogStorage.
func (s *Storage) ApplySnapshot(snap raft.Snapshot) error {
	// TODO(S1)
	panic("storage/mem: ApplySnapshot not implemented (stage S1)")
}

// Snapshot implements raft.LogStorage.
func (s *Storage) Snapshot() (raft.Snapshot, error) {
	// TODO(S1)
	panic("storage/mem: Snapshot not implemented (stage S1)")
}

// Compact discards entries through index (they remain represented by the
// snapshot). Simulator hook for compaction scenarios.
func (s *Storage) Compact(index uint64) error {
	// TODO(S1)
	panic("storage/mem: Compact not implemented (stage S1)")
}
