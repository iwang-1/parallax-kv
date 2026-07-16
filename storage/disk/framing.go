package disk

import (
	"encoding/binary"
	"errors"
	"hash/crc32"

	"github.com/iwang-1/parallax-kv/raft"
)

// WAL record framing.
//
// Each record on disk is a frame:
//
//	[payloadLen uint32][crc32c uint32][payload ...payloadLen bytes]
//
// payloadLen and crc32c are big-endian. crc32c is the Castagnoli CRC of the
// payload bytes only. The payload's first byte is a record-type tag; the
// remainder is the type-specific body.
//
// A frame whose header cannot be fully read, whose payload is shorter than
// payloadLen, or whose CRC does not match is a "torn tail": the product of a
// crash mid-write. Because the WAL is an append-only single-writer log, only
// the very end can be torn, so recovery treats the first bad frame as the
// logical end of the log and truncates there.

const frameHeaderSize = 8 // payloadLen (4) + crc32c (4)

// sanityMaxPayload bounds a single record's payload so a corrupt length field
// cannot make recovery attempt a giant allocation; it is far larger than any
// legitimate record (entry data is application-bounded, kilobytes at most).
const sanityMaxPayload = 64 << 20 // 64 MiB

// castagnoli is the CRC table shared by all framing operations.
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// recType tags a WAL record.
type recType byte

const (
	recEntry          recType = 1 // one raft.Entry appended to the log
	recHardState      recType = 2 // a persisted raft.HardState
	recTruncateSuffix recType = 3 // discard entries above an index
)

// errTornFrame signals that a frame is incomplete or corrupt and marks the
// logical end of the log during recovery.
var errTornFrame = errors.New("disk: torn or corrupt frame")

// encodeEntry serializes a log entry into a record payload (type tag +
// body). Body layout: term(8) index(8) etype(1) dataLen(4) data.
func encodeEntry(e raft.Entry) []byte {
	buf := make([]byte, 1+8+8+1+4+len(e.Data))
	buf[0] = byte(recEntry)
	binary.BigEndian.PutUint64(buf[1:9], e.Term)
	binary.BigEndian.PutUint64(buf[9:17], e.Index)
	buf[17] = byte(e.Type)
	binary.BigEndian.PutUint32(buf[18:22], uint32(len(e.Data)))
	copy(buf[22:], e.Data)
	return buf
}

// decodeEntry parses an entry record body (payload without the type tag).
func decodeEntry(body []byte) (raft.Entry, error) {
	if len(body) < 8+8+1+4 {
		return raft.Entry{}, errTornFrame
	}
	var e raft.Entry
	e.Term = binary.BigEndian.Uint64(body[0:8])
	e.Index = binary.BigEndian.Uint64(body[8:16])
	e.Type = raft.EntryType(body[16])
	dataLen := binary.BigEndian.Uint32(body[17:21])
	if uint64(21)+uint64(dataLen) != uint64(len(body)) {
		return raft.Entry{}, errTornFrame
	}
	if dataLen > 0 {
		e.Data = make([]byte, dataLen)
		copy(e.Data, body[21:])
	}
	return e, nil
}

// encodeHardState serializes a hard state into a record payload.
// Body layout: term(8) vote(8) commit(8).
func encodeHardState(st raft.HardState) []byte {
	buf := make([]byte, 1+8+8+8)
	buf[0] = byte(recHardState)
	binary.BigEndian.PutUint64(buf[1:9], st.Term)
	binary.BigEndian.PutUint64(buf[9:17], st.Vote)
	binary.BigEndian.PutUint64(buf[17:25], st.Commit)
	return buf
}

// decodeHardState parses a hard-state record body.
func decodeHardState(body []byte) (raft.HardState, error) {
	if len(body) != 8+8+8 {
		return raft.HardState{}, errTornFrame
	}
	return raft.HardState{
		Term:   binary.BigEndian.Uint64(body[0:8]),
		Vote:   binary.BigEndian.Uint64(body[8:16]),
		Commit: binary.BigEndian.Uint64(body[16:24]),
	}, nil
}

// encodeTruncateSuffix serializes the snapshot boundary that owns the
// truncation.
func encodeTruncateSuffix(meta raft.SnapshotMetadata) []byte {
	buf := make([]byte, 1+8+8)
	buf[0] = byte(recTruncateSuffix)
	binary.BigEndian.PutUint64(buf[1:9], meta.Index)
	binary.BigEndian.PutUint64(buf[9:17], meta.Term)
	return buf
}

// decodeTruncateSuffix parses a suffix-truncation record body.
func decodeTruncateSuffix(body []byte) (raft.SnapshotMetadata, error) {
	if len(body) != 8+8 {
		return raft.SnapshotMetadata{}, errTornFrame
	}
	return raft.SnapshotMetadata{
		Index: binary.BigEndian.Uint64(body[0:8]),
		Term:  binary.BigEndian.Uint64(body[8:16]),
	}, nil
}

// appendFrame appends a framed record (header + payload) to dst and returns
// the grown slice.
func appendFrame(dst, payload []byte) []byte {
	var hdr [frameHeaderSize]byte
	binary.BigEndian.PutUint32(hdr[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(hdr[4:8], crc32.Checksum(payload, castagnoli))
	dst = append(dst, hdr[:]...)
	dst = append(dst, payload...)
	return dst
}

// readFrame reads one frame starting at buf[off]. It returns the payload and
// the number of bytes consumed. It returns errTornFrame if the frame is
// incomplete or fails its CRC — the caller treats that as end-of-log.
func readFrame(buf []byte, off int) (payload []byte, n int, err error) {
	if off+frameHeaderSize > len(buf) {
		return nil, 0, errTornFrame
	}
	payloadLen := binary.BigEndian.Uint32(buf[off : off+4])
	wantCRC := binary.BigEndian.Uint32(buf[off+4 : off+8])
	if payloadLen == 0 || payloadLen > sanityMaxPayload {
		return nil, 0, errTornFrame
	}
	start := off + frameHeaderSize
	end := start + int(payloadLen)
	if end > len(buf) || end < start { // second clause guards int overflow
		return nil, 0, errTornFrame
	}
	p := buf[start:end]
	if crc32.Checksum(p, castagnoli) != wantCRC {
		return nil, 0, errTornFrame
	}
	return p, end - off, nil
}
