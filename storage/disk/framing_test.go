package disk

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"testing"

	"github.com/iwang-1/parallax-kv/raft"
)

func TestEntryRoundTrip(t *testing.T) {
	cases := []raft.Entry{
		{Term: 1, Index: 1, Type: raft.EntryNormal, Data: []byte("hello")},
		{Term: 7, Index: 42, Type: raft.EntryNoop},
		{Term: 0, Index: 0, Data: []byte{}},
	}
	for _, e := range cases {
		payload := encodeEntry(e)
		if recType(payload[0]) != recEntry {
			t.Fatalf("tag = %d, want recEntry", payload[0])
		}
		got, err := decodeEntry(payload[1:])
		if err != nil {
			t.Fatalf("decodeEntry(%+v): %v", e, err)
		}
		// Normalize nil vs empty data for comparison.
		if len(got.Data) == 0 && len(e.Data) == 0 {
			got.Data, e.Data = nil, nil
		}
		if !reflect.DeepEqual(got, e) {
			t.Fatalf("round trip = %+v, want %+v", got, e)
		}
	}
}

func TestHardStateRoundTrip(t *testing.T) {
	st := raft.HardState{Term: 9, Vote: 3, Commit: 100}
	payload := encodeHardState(st)
	if recType(payload[0]) != recHardState {
		t.Fatalf("tag = %d, want recHardState", payload[0])
	}
	got, err := decodeHardState(payload[1:])
	if err != nil {
		t.Fatal(err)
	}
	if got != st {
		t.Fatalf("round trip = %+v, want %+v", got, st)
	}
}

func TestFrameRoundTrip(t *testing.T) {
	payload := []byte("some record payload")
	buf := appendFrame(nil, payload)
	got, n, err := readFrame(buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(buf) {
		t.Fatalf("consumed %d, want %d", n, len(buf))
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload = %q, want %q", got, payload)
	}
}

func TestReadFrameTruncatedHeader(t *testing.T) {
	if _, _, err := readFrame([]byte{1, 2, 3}, 0); err != errTornFrame {
		t.Fatalf("err = %v, want errTornFrame", err)
	}
}

func TestReadFrameTruncatedPayload(t *testing.T) {
	buf := appendFrame(nil, []byte("payload"))
	// Chop the last byte off the payload.
	if _, _, err := readFrame(buf[:len(buf)-1], 0); err != errTornFrame {
		t.Fatalf("err = %v, want errTornFrame", err)
	}
}

func TestReadFrameBadCRC(t *testing.T) {
	buf := appendFrame(nil, []byte("payload"))
	buf[len(buf)-1] ^= 0xFF // corrupt a payload byte
	if _, _, err := readFrame(buf, 0); err != errTornFrame {
		t.Fatalf("err = %v, want errTornFrame", err)
	}
}

func TestReadFrameInsaneLength(t *testing.T) {
	var buf [frameHeaderSize]byte
	binary.BigEndian.PutUint32(buf[0:4], sanityMaxPayload+1)
	if _, _, err := readFrame(buf[:], 0); err != errTornFrame {
		t.Fatalf("err = %v, want errTornFrame", err)
	}
}
