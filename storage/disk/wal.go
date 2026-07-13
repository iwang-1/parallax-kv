package disk

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/iwang-1/parallax-kv/raft"
)

// Open opens (creating dir if needed) the WAL and snapshot state and runs
// recovery, returning a Storage whose in-memory mirror reflects the durable
// state on disk.
func Open(dir string) (*Storage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("disk: mkdir %s: %w", dir, err)
	}
	s := &Storage{dir: dir}

	// Load the newest snapshot (if any) to establish the log's base index.
	if err := s.loadLatestSnapshot(); err != nil {
		return nil, err
	}

	// Replay WAL segments in order to rebuild hard state and the log tail.
	segs, err := s.listSegments()
	if err != nil {
		return nil, err
	}
	for i, name := range segs {
		torn, err := s.replaySegment(name)
		if err != nil {
			return nil, err
		}
		if torn {
			// The log logically ends inside this segment; any later
			// segments are orphaned by the crash and must be discarded so
			// their records are never replayed.
			if err := s.removeSegments(segs[i+1:]); err != nil {
				return nil, err
			}
			segs = segs[:i+1]
			break
		}
	}

	// Open (or create) the active segment for appending. Recovery may have
	// found a torn tail and rewritten a segment; either way we append to the
	// highest-numbered segment, creating the first one if the dir was empty.
	if err := s.openActiveSegment(segs); err != nil {
		return nil, err
	}
	return s, nil
}

// removeSegments deletes orphaned WAL segments left after a torn tail.
func (s *Storage) removeSegments(names []string) error {
	for _, name := range names {
		if err := os.Remove(filepath.Join(s.dir, name)); err != nil {
			return fmt.Errorf("disk: remove orphaned segment %s: %w", name, err)
		}
	}
	if len(names) > 0 {
		return fsyncDir(s.dir)
	}
	return nil
}

// listSegments returns WAL segment file names sorted by sequence number.
func (s *Storage) listSegments() ([]string, error) {
	ents, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("disk: readdir %s: %w", s.dir, err)
	}
	var segs []string
	for _, e := range ents {
		if e.IsDir() {
			continue
		}
		n := e.Name()
		if strings.HasPrefix(n, "wal-") && strings.HasSuffix(n, ".seg") {
			segs = append(segs, n)
		}
	}
	sort.Strings(segs) // zero-padded sequence => lexical == numeric order
	return segs, nil
}

// replaySegment applies every intact record in a segment to the in-memory
// mirror. On encountering a torn/corrupt frame it truncates the file at the
// last good boundary, reports torn=true, and stops (that frame is the logical
// end of the log).
func (s *Storage) replaySegment(name string) (torn bool, err error) {
	path := filepath.Join(s.dir, name)
	data, err := os.ReadFile(path)
	if err != nil {
		return false, fmt.Errorf("disk: read segment %s: %w", name, err)
	}
	off := 0
	for off < len(data) {
		payload, n, ferr := readFrame(data, off)
		if ferr != nil {
			// Torn tail: truncate the file here and stop replaying.
			return true, s.truncateSegment(path, int64(off))
		}
		if aerr := s.applyRecord(payload); aerr != nil {
			// A record that frames correctly (CRC-valid) but fails to
			// decode indicates real corruption, not a torn tail; still
			// treat it as end-of-log to stay available.
			return true, s.truncateSegment(path, int64(off))
		}
		off += n
	}
	return false, nil
}

// applyRecord updates the in-memory mirror from one decoded record payload.
func (s *Storage) applyRecord(payload []byte) error {
	if len(payload) == 0 {
		return errTornFrame
	}
	body := payload[1:]
	switch recType(payload[0]) {
	case recEntry:
		e, err := decodeEntry(body)
		if err != nil {
			return err
		}
		return s.applyEntryRecord(e)
	case recHardState:
		st, err := decodeHardState(body)
		if err != nil {
			return err
		}
		s.hardState = st
		return nil
	default:
		return errTornFrame
	}
}

// applyEntryRecord installs a replayed entry at its index, truncating any
// conflicting suffix (log truncation is implicit in the redo stream).
func (s *Storage) applyEntryRecord(e raft.Entry) error {
	off := s.offset()
	if e.Index <= off {
		// Covered by the snapshot; ignore.
		return nil
	}
	pos := e.Index - (off + 1)
	if pos > uint64(len(s.entries)) {
		// Gap in the log: corrupt/non-contiguous stream.
		return errTornFrame
	}
	s.entries = append(s.entries[:pos], e)
	return nil
}

// truncateSegment truncates path to size bytes and fsyncs it, discarding a
// torn tail so a subsequent append starts from a clean boundary.
func (s *Storage) truncateSegment(path string, size int64) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("disk: reopen for truncate %s: %w", path, err)
	}
	defer f.Close()
	if err := f.Truncate(size); err != nil {
		return fmt.Errorf("disk: truncate %s: %w", path, err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("disk: sync truncate %s: %w", path, err)
	}
	return nil
}

// openActiveSegment opens the highest-numbered existing segment for appending,
// or creates the first segment if none exist.
func (s *Storage) openActiveSegment(segs []string) error {
	if len(segs) == 0 {
		return s.rotateSegment(1)
	}
	name := segs[len(segs)-1]
	seq, err := parseSegmentSeq(name)
	if err != nil {
		return err
	}
	path := filepath.Join(s.dir, name)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("disk: open active segment %s: %w", name, err)
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return fmt.Errorf("disk: stat active segment %s: %w", name, err)
	}
	s.seg = f
	s.segSize = info.Size()
	s.segSeq = seq
	return nil
}

// rotateSegment creates a fresh empty segment with sequence seq and makes it
// active, fsyncing the directory so the new file is durable.
func (s *Storage) rotateSegment(seq uint64) error {
	if s.seg != nil {
		if err := s.seg.Close(); err != nil {
			return fmt.Errorf("disk: close segment: %w", err)
		}
		s.seg = nil
	}
	path := filepath.Join(s.dir, segmentName(seq))
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("disk: create segment %s: %w", path, err)
	}
	s.seg = f
	s.segSize = 0
	s.segSeq = seq
	return fsyncDir(s.dir)
}

// Sync flushes all records buffered since the last Sync with a single fsync —
// the group-commit point, called once per Ready batch (when Ready.MustSync)
// before messages are sent. It rotates to a fresh segment first if the active
// one has grown past maxSegmentSize.
func (s *Storage) Sync() error {
	if len(s.buf) == 0 {
		return nil
	}
	if s.segSize >= maxSegmentSize {
		if err := s.rotateSegment(s.segSeq + 1); err != nil {
			return err
		}
	}
	if _, err := s.seg.Write(s.buf); err != nil {
		return fmt.Errorf("disk: append records: %w", err)
	}
	if !s.noSync {
		if err := s.seg.Sync(); err != nil {
			return fmt.Errorf("disk: fsync segment: %w", err)
		}
	}
	s.segSize += int64(len(s.buf))
	s.buf = s.buf[:0]
	return nil
}

// parseSegmentSeq extracts the sequence number from a segment file name.
func parseSegmentSeq(name string) (uint64, error) {
	var seq uint64
	if _, err := fmt.Sscanf(name, "wal-%020d.seg", &seq); err != nil {
		return 0, fmt.Errorf("disk: bad segment name %q: %w", name, err)
	}
	return seq, nil
}

// fsyncDir fsyncs a directory so that a create/rename within it is durable.
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("disk: open dir %s: %w", dir, err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("disk: fsync dir %s: %w", dir, err)
	}
	return nil
}
