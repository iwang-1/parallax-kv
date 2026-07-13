// Package disk is the production LogStorage: a segmented write-ahead log
// plus snapshot files.
//
// WAL format: append-only segment files (wal-<firstIndex>.seg) of
// length-prefixed, CRC32-framed records (record = frame header {length,
// crc32c} + payload). Records carry hard-state updates, entry appends, and
// truncation marks. Durability is group commit: one fsync per Ready batch,
// covering every record the batch wrote — this is where write throughput
// comes from (see docs/DESIGN_NOTES.md).
//
// Recovery scans segments in order, verifying CRCs; a torn tail (partial
// or corrupt final record from a mid-write crash) is truncated, never
// trusted. Snapshots are written atomically: tmp file, fsync, rename,
// fsync parent directory.
package disk

import "github.com/iwang-1/parallax-kv/raft"

// Storage is the durable raft.LogStorage. Not safe for concurrent use;
// the server's drive loop owns it.
type Storage struct {
	dir string
}

var _ raft.LogStorage = (*Storage)(nil)

// Open opens (creating if needed) the WAL and snapshot state in dir and
// runs recovery.
func Open(dir string) (*Storage, error) {
	// TODO(S1): segment discovery, CRC scan, torn-tail truncation.
	panic("storage/disk: Open not implemented (stage S1)")
}

// Sync fsyncs all records written since the last Sync — the group-commit
// point, called once per Ready batch (when Ready.MustSync) before messages
// are sent.
func (s *Storage) Sync() error {
	// TODO(S1)
	panic("storage/disk: Sync not implemented (stage S1)")
}

// Close releases file handles. The storage is unusable afterwards.
func (s *Storage) Close() error {
	// TODO(S1)
	panic("storage/disk: Close not implemented (stage S1)")
}

// AppendEntries implements raft.LogStorage. Writes are buffered until Sync.
func (s *Storage) AppendEntries(entries []raft.Entry) error {
	// TODO(S1)
	panic("storage/disk: AppendEntries not implemented (stage S1)")
}

// SetHardState implements raft.LogStorage. Writes are buffered until Sync.
func (s *Storage) SetHardState(st raft.HardState) error {
	// TODO(S1)
	panic("storage/disk: SetHardState not implemented (stage S1)")
}

// HardState implements raft.LogStorage.
func (s *Storage) HardState() (raft.HardState, error) {
	// TODO(S1)
	panic("storage/disk: HardState not implemented (stage S1)")
}

// Entries implements raft.LogStorage.
func (s *Storage) Entries(lo, hi uint64) ([]raft.Entry, error) {
	// TODO(S1)
	panic("storage/disk: Entries not implemented (stage S1)")
}

// Term implements raft.LogStorage.
func (s *Storage) Term(i uint64) (uint64, error) {
	// TODO(S1)
	panic("storage/disk: Term not implemented (stage S1)")
}

// FirstIndex implements raft.LogStorage.
func (s *Storage) FirstIndex() (uint64, error) {
	// TODO(S1)
	panic("storage/disk: FirstIndex not implemented (stage S1)")
}

// LastIndex implements raft.LogStorage.
func (s *Storage) LastIndex() (uint64, error) {
	// TODO(S1)
	panic("storage/disk: LastIndex not implemented (stage S1)")
}

// ApplySnapshot implements raft.LogStorage (atomic snapshot file write +
// WAL compaction).
func (s *Storage) ApplySnapshot(snap raft.Snapshot) error {
	// TODO(S1)
	panic("storage/disk: ApplySnapshot not implemented (stage S1)")
}

// Snapshot implements raft.LogStorage.
func (s *Storage) Snapshot() (raft.Snapshot, error) {
	// TODO(S1)
	panic("storage/disk: Snapshot not implemented (stage S1)")
}
