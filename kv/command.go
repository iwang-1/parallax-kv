package kv

import "fmt"

// OpType is the operation carried by a Command.
type OpType uint8

const (
	// OpInvalid is the zero value and never a valid operation.
	OpInvalid OpType = iota
	// OpGet reads a key. Gets normally take the ReadIndex path and never
	// enter the log; OpGet exists for through-the-log reads in tests.
	OpGet
	// OpPut sets Key to Value.
	OpPut
	// OpDelete removes Key.
	OpDelete
	// OpCAS sets Key to Value iff the current value equals Expect.
	// A nil Expect means "iff the key does not exist" (create-if-absent).
	OpCAS
)

// Command is the unit that enters the Raft log: one client operation,
// tagged with the (ClientID, Seq) pair the session table dedups on.
type Command struct {
	// ClientID uniquely identifies a client session. Positive.
	ClientID uint64
	// Seq is the client's command sequence number, strictly increasing
	// per ClientID, at most one in flight.
	Seq uint64

	Op    OpType
	Key   string
	Value []byte
	// Expect is the compare value for OpCAS (nil = key must be absent).
	Expect []byte
}

// Status classifies a Result.
type Status uint8

const (
	// StatusOK: the operation succeeded.
	StatusOK Status = iota
	// StatusNotFound: Get/Delete on an absent key.
	StatusNotFound
	// StatusCASMismatch: OpCAS found a current value != Expect.
	StatusCASMismatch
	// StatusStaleSeq: the command's Seq is below the session's LastSeq;
	// the definitive result was already returned to a newer retry.
	StatusStaleSeq
)

// Result is the outcome of applying (or reading) a command.
type Result struct {
	Status Status
	// Value is the read value (Get) or the current value that defeated a
	// CAS (CASMismatch), else nil.
	Value []byte
	// Version is the key's version after the operation (0 if absent).
	Version uint64
}

// commandEncodingVersion is the first byte of every encoded Command,
// allowing the wire format to evolve without ambiguity.
const commandEncodingVersion byte = 1

// EncodeCommand serializes a Command for storage in a raft.Entry's Data.
// The encoding is deterministic: equal Commands produce equal bytes.
//
// Layout (big-endian): version(1) | ClientID(8) | Seq(8) | Op(1) |
// keyLen(4)+key | Value presence(1)[+len(4)+bytes] |
// Expect presence(1)[+len(4)+bytes]. The presence flags preserve the
// nil-vs-empty distinction that OpCAS's Expect depends on.
func EncodeCommand(c Command) []byte {
	buf := make([]byte, 0, 1+8+8+1+4+len(c.Key)+(1+4+len(c.Value))+(1+4+len(c.Expect)))
	buf = append(buf, commandEncodingVersion)
	buf = appendUint64(buf, c.ClientID)
	buf = appendUint64(buf, c.Seq)
	buf = append(buf, byte(c.Op))
	buf = appendString(buf, c.Key)
	buf = appendOptBytes(buf, c.Value)
	buf = appendOptBytes(buf, c.Expect)
	return buf
}

// DecodeCommand parses bytes produced by EncodeCommand. It is strict:
// truncated input, an unknown version or op, or trailing bytes all fail.
func DecodeCommand(data []byte) (Command, error) {
	d := &decoder{buf: data}
	version, err := d.byte()
	if err != nil {
		return Command{}, err
	}
	if version != commandEncodingVersion {
		return Command{}, fmt.Errorf("kv: unknown command encoding version %d", version)
	}
	var c Command
	if c.ClientID, err = d.uint64(); err != nil {
		return Command{}, err
	}
	if c.Seq, err = d.uint64(); err != nil {
		return Command{}, err
	}
	op, err := d.byte()
	if err != nil {
		return Command{}, err
	}
	c.Op = OpType(op)
	if c.Op < OpGet || c.Op > OpCAS {
		return Command{}, fmt.Errorf("kv: invalid op %d in encoded command", op)
	}
	if c.Key, err = d.string(); err != nil {
		return Command{}, err
	}
	if c.Value, err = d.optBytes(); err != nil {
		return Command{}, err
	}
	if c.Expect, err = d.optBytes(); err != nil {
		return Command{}, err
	}
	if err := d.finish(); err != nil {
		return Command{}, err
	}
	return c, nil
}
