package kv

import (
	"encoding/binary"
	"fmt"
)

// Low-level binary codec helpers shared by the Command encoding and the
// StateMachine snapshot format. All integers are big-endian. Byte slices
// are encoded with a presence flag so nil and empty round-trip distinctly
// (the distinction is semantic for Command.Expect: nil means "key must be
// absent", empty means "value must be empty").

const (
	flagAbsent  byte = 0
	flagPresent byte = 1
)

func appendUint64(buf []byte, v uint64) []byte {
	return binary.BigEndian.AppendUint64(buf, v)
}

func appendUint32(buf []byte, v uint32) []byte {
	return binary.BigEndian.AppendUint32(buf, v)
}

// appendString encodes a length-prefixed string (no presence flag; keys are
// always present).
func appendString(buf []byte, s string) []byte {
	buf = appendUint32(buf, uint32(len(s)))
	return append(buf, s...)
}

// appendOptBytes encodes b with a presence flag distinguishing nil from
// empty.
func appendOptBytes(buf []byte, b []byte) []byte {
	if b == nil {
		return append(buf, flagAbsent)
	}
	buf = append(buf, flagPresent)
	buf = appendUint32(buf, uint32(len(b)))
	return append(buf, b...)
}

// decoder is a strict cursor over an encoded buffer. Every read checks
// bounds and returns an error on truncation; decoding never panics.
type decoder struct {
	buf []byte
	off int
}

func (d *decoder) remaining() int { return len(d.buf) - d.off }

func (d *decoder) byte() (byte, error) {
	if d.remaining() < 1 {
		return 0, fmt.Errorf("kv: truncated input at offset %d: want 1 byte, have 0", d.off)
	}
	b := d.buf[d.off]
	d.off++
	return b, nil
}

func (d *decoder) uint64() (uint64, error) {
	if d.remaining() < 8 {
		return 0, fmt.Errorf("kv: truncated input at offset %d: want 8 bytes, have %d", d.off, d.remaining())
	}
	v := binary.BigEndian.Uint64(d.buf[d.off:])
	d.off += 8
	return v, nil
}

func (d *decoder) uint32() (uint32, error) {
	if d.remaining() < 4 {
		return 0, fmt.Errorf("kv: truncated input at offset %d: want 4 bytes, have %d", d.off, d.remaining())
	}
	v := binary.BigEndian.Uint32(d.buf[d.off:])
	d.off += 4
	return v, nil
}

func (d *decoder) bytesN(n int) ([]byte, error) {
	if d.remaining() < n {
		return nil, fmt.Errorf("kv: truncated input at offset %d: want %d bytes, have %d", d.off, n, d.remaining())
	}
	b := d.buf[d.off : d.off+n]
	d.off += n
	return b, nil
}

func (d *decoder) string() (string, error) {
	n, err := d.uint32()
	if err != nil {
		return "", err
	}
	b, err := d.bytesN(int(n))
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// optBytes reads a presence-flagged byte slice. The returned slice is a
// copy, never aliasing the input buffer.
func (d *decoder) optBytes() ([]byte, error) {
	flag, err := d.byte()
	if err != nil {
		return nil, err
	}
	switch flag {
	case flagAbsent:
		return nil, nil
	case flagPresent:
		n, err := d.uint32()
		if err != nil {
			return nil, err
		}
		b, err := d.bytesN(int(n))
		if err != nil {
			return nil, err
		}
		out := make([]byte, n)
		copy(out, b)
		return out, nil
	default:
		return nil, fmt.Errorf("kv: invalid presence flag %d at offset %d", flag, d.off-1)
	}
}

// finish returns an error if any input remains unconsumed.
func (d *decoder) finish() error {
	if d.remaining() != 0 {
		return fmt.Errorf("kv: %d trailing bytes after decoded value", d.remaining())
	}
	return nil
}
