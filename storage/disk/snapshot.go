package disk

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/iwang-1/parallax-kv/raft"
)

// Snapshot file format: a single framed record whose payload is
//
//	[index uint64][term uint64][dataLen uint32][data ...] + crc32c
//
// wrapped in the same {payloadLen, crc32c} frame as WAL records, so a torn
// snapshot write is detected on load and ignored. Files are named
// snap-<index>.snap; the highest intact index wins on recovery.

// snapPayload serializes snapshot metadata + data into a record payload
// (no type tag; snapshot files hold exactly one record).
func snapPayload(snap raft.Snapshot) []byte {
	buf := make([]byte, 8+8+4+len(snap.Data))
	binary.BigEndian.PutUint64(buf[0:8], snap.Metadata.Index)
	binary.BigEndian.PutUint64(buf[8:16], snap.Metadata.Term)
	binary.BigEndian.PutUint32(buf[16:20], uint32(len(snap.Data)))
	copy(buf[20:], snap.Data)
	return buf
}

// parseSnapPayload is the inverse of snapPayload.
func parseSnapPayload(payload []byte) (raft.Snapshot, error) {
	if len(payload) < 20 {
		return raft.Snapshot{}, errTornFrame
	}
	var snap raft.Snapshot
	snap.Metadata.Index = binary.BigEndian.Uint64(payload[0:8])
	snap.Metadata.Term = binary.BigEndian.Uint64(payload[8:16])
	dataLen := binary.BigEndian.Uint32(payload[16:20])
	if uint64(20)+uint64(dataLen) != uint64(len(payload)) {
		return raft.Snapshot{}, errTornFrame
	}
	if dataLen > 0 {
		snap.Data = make([]byte, dataLen)
		copy(snap.Data, payload[20:])
	}
	return snap, nil
}

func snapshotName(index uint64) string {
	return fmt.Sprintf("snap-%020d.snap", index)
}

// Snapshot implements raft.LogStorage.
func (s *Storage) Snapshot() (raft.Snapshot, error) {
	return s.snapshot, nil
}

// ApplySnapshot implements raft.LogStorage. It writes snap to a new snapshot
// file atomically (tmp + fsync + rename + dir-fsync), then updates the
// in-memory mirror, discarding entries the snapshot covers. A stale snapshot
// (index <= the current snapshot) is rejected with raft.ErrCompacted.
//
// The WAL is left intact: obsolete entry records are shadowed by the higher
// snapshot on the next recovery and reclaimed when their segments are pruned.
func (s *Storage) ApplySnapshot(snap raft.Snapshot) error {
	if snap.Metadata.Index <= s.offset() {
		return raft.ErrCompacted
	}
	if err := s.writeSnapshotFile(snap); err != nil {
		return err
	}
	last := s.offset() + uint64(len(s.entries))
	if snap.Metadata.Index < last {
		s.entries = append([]raft.Entry(nil), s.entries[snap.Metadata.Index-s.offset():]...)
	} else {
		s.entries = nil
	}
	s.snapshot = snap
	return nil
}

// writeSnapshotFile durably writes snap using the atomic tmp+fsync+rename+
// dir-fsync sequence.
func (s *Storage) writeSnapshotFile(snap raft.Snapshot) error {
	final := filepath.Join(s.dir, snapshotName(snap.Metadata.Index))
	tmp := final + ".tmp"

	framed := appendFrame(nil, snapPayload(snap))

	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("disk: create snapshot tmp: %w", err)
	}
	if _, err := f.Write(framed); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("disk: write snapshot tmp: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("disk: fsync snapshot tmp: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("disk: close snapshot tmp: %w", err)
	}
	if err := os.Rename(tmp, final); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("disk: rename snapshot: %w", err)
	}
	return fsyncDir(s.dir)
}

// loadLatestSnapshot finds the newest intact snapshot file and loads it into
// the in-memory mirror. Torn/corrupt snapshot files are skipped in favor of
// the next-newest intact one. It is called once, at Open, before WAL replay.
func (s *Storage) loadLatestSnapshot() error {
	ents, err := os.ReadDir(s.dir)
	if err != nil {
		return fmt.Errorf("disk: readdir %s: %w", s.dir, err)
	}
	var names []string
	for _, e := range ents {
		if e.IsDir() {
			continue
		}
		n := e.Name()
		if strings.HasPrefix(n, "snap-") && strings.HasSuffix(n, ".snap") {
			names = append(names, n)
		}
	}
	sort.Strings(names) // newest (highest index) last
	for i := len(names) - 1; i >= 0; i-- {
		snap, err := readSnapshotFile(filepath.Join(s.dir, names[i]))
		if err != nil {
			continue // torn/corrupt: fall back to an older snapshot
		}
		s.snapshot = snap
		return nil
	}
	return nil
}

// readSnapshotFile reads and validates a single snapshot file.
func readSnapshotFile(path string) (raft.Snapshot, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return raft.Snapshot{}, err
	}
	payload, _, ferr := readFrame(data, 0)
	if ferr != nil {
		return raft.Snapshot{}, ferr
	}
	return parseSnapPayload(payload)
}
