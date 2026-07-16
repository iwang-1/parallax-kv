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

const snapshotFlagTruncateSuffix byte = 1

type storedSnapshot struct {
	snapshot       raft.Snapshot
	truncateSuffix bool
}

type snapshotTestHooks struct {
	beforeSnapshotWrite      func() error
	afterSnapshotFileDurable func()
	beforeTruncationSync     func() error
}

// Snapshot file format: a single framed record whose payload is
//
//	[index uint64][term uint64][dataLen uint32][data ...][flags uint8]
//
// wrapped in the same {payloadLen, crc32c} frame as WAL records, so a torn
// snapshot write is detected on load and ignored. Files are named
// snap-<index>.snap; the highest intact index wins on recovery. Legacy files
// without flags remain readable.

// snapPayload serializes snapshot metadata + data into a record payload
// (no type tag; snapshot files hold exactly one record).
func snapPayload(snap raft.Snapshot, truncateSuffix bool) []byte {
	dataEnd := 8 + 8 + 4 + len(snap.Data)
	buf := make([]byte, dataEnd+1)
	binary.BigEndian.PutUint64(buf[0:8], snap.Metadata.Index)
	binary.BigEndian.PutUint64(buf[8:16], snap.Metadata.Term)
	binary.BigEndian.PutUint32(buf[16:20], uint32(len(snap.Data)))
	copy(buf[20:], snap.Data)
	if truncateSuffix {
		buf[dataEnd] = snapshotFlagTruncateSuffix
	}
	return buf
}

// parseSnapPayload is the inverse of snapPayload.
func parseSnapPayload(payload []byte) (storedSnapshot, error) {
	if len(payload) < 20 {
		return storedSnapshot{}, errTornFrame
	}
	var snap raft.Snapshot
	snap.Metadata.Index = binary.BigEndian.Uint64(payload[0:8])
	snap.Metadata.Term = binary.BigEndian.Uint64(payload[8:16])
	dataLen := binary.BigEndian.Uint32(payload[16:20])
	dataEnd := uint64(20) + uint64(dataLen)
	if uint64(len(payload)) != dataEnd && uint64(len(payload)) != dataEnd+1 {
		return storedSnapshot{}, errTornFrame
	}
	if dataLen > 0 {
		snap.Data = make([]byte, dataLen)
		copy(snap.Data, payload[20:int(dataEnd)])
	}
	stored := storedSnapshot{snapshot: snap}
	if uint64(len(payload)) == dataEnd+1 {
		flags := payload[dataEnd]
		if flags & ^snapshotFlagTruncateSuffix != 0 {
			return storedSnapshot{}, errTornFrame
		}
		stored.truncateSuffix = flags&snapshotFlagTruncateSuffix != 0
	}
	return stored, nil
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
// in-memory mirror. Entries above the snapshot are retained only when the
// existing entry at its boundary has the same term. A stale snapshot (index <=
// the current snapshot) is rejected with raft.ErrCompacted.
//
// A boundary mismatch is recorded and fsynced in the WAL after the snapshot
// file is durable, preventing recovery from replaying the divergent suffix.
func (s *Storage) ApplySnapshot(snap raft.Snapshot) error {
	if snap.Metadata.Index <= s.offset() {
		return raft.ErrCompacted
	}
	off := s.offset()
	last := off + uint64(len(s.entries))
	retainSuffix := snap.Metadata.Index < last &&
		s.entries[snap.Metadata.Index-off-1].Term == snap.Metadata.Term
	truncateSuffix := snap.Metadata.Index < last && !retainSuffix

	if s.testHooks.beforeSnapshotWrite != nil {
		if err := s.testHooks.beforeSnapshotWrite(); err != nil {
			return err
		}
	}
	if err := s.writeSnapshotFile(snap, truncateSuffix); err != nil {
		return err
	}
	if s.testHooks.afterSnapshotFileDurable != nil {
		s.testHooks.afterSnapshotFileDurable()
	}

	if retainSuffix {
		s.entries = append([]raft.Entry(nil), s.entries[snap.Metadata.Index-off:]...)
	} else {
		s.entries = nil
	}
	s.snapshot = snap
	s.snapshotTruncationPending = truncateSuffix

	if truncateSuffix {
		s.buf = appendFrame(s.buf, encodeTruncateSuffix(snap.Metadata))
		if s.testHooks.beforeTruncationSync != nil {
			if err := s.testHooks.beforeTruncationSync(); err != nil {
				return err
			}
		}
		if err := s.syncBuffered(true); err != nil {
			return err
		}
		s.snapshotTruncationPending = false
	}
	return nil
}

// writeSnapshotFile durably writes snap using the atomic tmp+fsync+rename+
// dir-fsync sequence.
func (s *Storage) writeSnapshotFile(snap raft.Snapshot, truncateSuffix bool) error {
	final := filepath.Join(s.dir, snapshotName(snap.Metadata.Index))
	tmp := final + ".tmp"

	framed := appendFrame(nil, snapPayload(snap, truncateSuffix))

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
		stored, err := readSnapshotFile(filepath.Join(s.dir, names[i]))
		if err != nil {
			continue // torn/corrupt: fall back to an older snapshot
		}
		s.snapshot = stored.snapshot
		s.snapshotTruncationPending = stored.truncateSuffix
		return nil
	}
	return nil
}

// readSnapshotFile reads and validates a single snapshot file.
func readSnapshotFile(path string) (storedSnapshot, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return storedSnapshot{}, err
	}
	payload, _, ferr := readFrame(data, 0)
	if ferr != nil {
		return storedSnapshot{}, ferr
	}
	return parseSnapPayload(payload)
}
