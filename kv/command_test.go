package kv

import (
	"bytes"
	"math/rand"
	"reflect"
	"testing"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	cmds := []Command{
		{ClientID: 1, Seq: 1, Op: OpGet, Key: "k"},
		{ClientID: 7, Seq: 42, Op: OpPut, Key: "key", Value: []byte("value")},
		{ClientID: 7, Seq: 43, Op: OpPut, Key: "", Value: nil},
		{ClientID: 7, Seq: 44, Op: OpPut, Key: "empty-value", Value: []byte{}},
		{ClientID: 2, Seq: 9, Op: OpDelete, Key: "gone"},
		// nil Expect (create-if-absent) vs empty Expect (expect empty value)
		// must survive the round trip distinctly.
		{ClientID: 3, Seq: 1, Op: OpCAS, Key: "a", Value: []byte("v"), Expect: nil},
		{ClientID: 3, Seq: 2, Op: OpCAS, Key: "a", Value: []byte("v"), Expect: []byte{}},
		{ClientID: 3, Seq: 3, Op: OpCAS, Key: "a", Value: nil, Expect: []byte("old")},
		{ClientID: ^uint64(0), Seq: ^uint64(0), Op: OpPut, Key: "max", Value: bytes.Repeat([]byte{0xff}, 300)},
	}
	for _, want := range cmds {
		enc := EncodeCommand(want)
		got, err := DecodeCommand(enc)
		if err != nil {
			t.Fatalf("DecodeCommand(%+v): %v", want, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("round trip mismatch:\n got %#v\nwant %#v", got, want)
		}
		// nil-vs-empty must be preserved exactly.
		if (got.Value == nil) != (want.Value == nil) {
			t.Errorf("Value nil-ness changed: got nil=%v want nil=%v", got.Value == nil, want.Value == nil)
		}
		if (got.Expect == nil) != (want.Expect == nil) {
			t.Errorf("Expect nil-ness changed: got nil=%v want nil=%v", got.Expect == nil, want.Expect == nil)
		}
	}
}

func TestEncodeDeterministic(t *testing.T) {
	c := Command{ClientID: 5, Seq: 6, Op: OpCAS, Key: "k", Value: []byte("v"), Expect: []byte("e")}
	if !bytes.Equal(EncodeCommand(c), EncodeCommand(c)) {
		t.Fatal("EncodeCommand is not deterministic for equal inputs")
	}
}

func TestDecodeCommandErrors(t *testing.T) {
	valid := EncodeCommand(Command{ClientID: 1, Seq: 2, Op: OpPut, Key: "k", Value: []byte("v")})

	t.Run("empty input", func(t *testing.T) {
		if _, err := DecodeCommand(nil); err == nil {
			t.Error("want error for empty input")
		}
	})
	t.Run("bad version", func(t *testing.T) {
		bad := append([]byte{}, valid...)
		bad[0] = 0xff
		if _, err := DecodeCommand(bad); err == nil {
			t.Error("want error for unknown version")
		}
	})
	t.Run("truncated at every prefix length", func(t *testing.T) {
		for n := 0; n < len(valid); n++ {
			if _, err := DecodeCommand(valid[:n]); err == nil {
				t.Errorf("want error for %d-byte prefix of a %d-byte command", n, len(valid))
			}
		}
	})
	t.Run("trailing garbage", func(t *testing.T) {
		if _, err := DecodeCommand(append(append([]byte{}, valid...), 0x00)); err == nil {
			t.Error("want error for trailing bytes")
		}
	})
	t.Run("invalid op", func(t *testing.T) {
		for _, op := range []byte{byte(OpInvalid), byte(OpCAS) + 1, 0xff} {
			bad := append([]byte{}, valid...)
			bad[1+8+8] = op // op sits after version + ClientID + Seq
			if _, err := DecodeCommand(bad); err == nil {
				t.Errorf("want error for op byte %d", op)
			}
		}
	})
	t.Run("invalid presence flag", func(t *testing.T) {
		bad := append([]byte{}, valid...)
		bad[1+8+8+1+4+1] = 2 // Value presence flag after version+ids+op+keyLen+key("k")
		if _, err := DecodeCommand(bad); err == nil {
			t.Error("want error for invalid presence flag")
		}
	})
}

// TestEncodeDecodeRandomized round-trips randomly generated commands with a
// fixed seed, covering length and nil/empty combinations.
func TestEncodeDecodeRandomized(t *testing.T) {
	rng := rand.New(rand.NewSource(0x51CA5E))
	randBytes := func() []byte {
		switch rng.Intn(4) {
		case 0:
			return nil
		case 1:
			return []byte{}
		default:
			b := make([]byte, rng.Intn(64))
			rng.Read(b)
			return b
		}
	}
	ops := []OpType{OpGet, OpPut, OpDelete, OpCAS}
	for i := 0; i < 500; i++ {
		want := Command{
			ClientID: rng.Uint64(),
			Seq:      rng.Uint64(),
			Op:       ops[rng.Intn(len(ops))],
			Key:      string(rune('a' + rng.Intn(26))),
			Value:    randBytes(),
			Expect:   randBytes(),
		}
		got, err := DecodeCommand(EncodeCommand(want))
		if err != nil {
			t.Fatalf("iteration %d: decode: %v", i, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("iteration %d: round trip mismatch:\n got %#v\nwant %#v", i, got, want)
		}
	}
}
