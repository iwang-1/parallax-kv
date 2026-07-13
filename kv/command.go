package kv

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

// EncodeCommand serializes a Command for storage in a raft.Entry's Data.
// The encoding is deterministic: equal Commands produce equal bytes.
func EncodeCommand(c Command) []byte {
	// TODO(S1): length-prefixed binary encoding.
	panic("kv: EncodeCommand not implemented (stage S1)")
}

// DecodeCommand parses bytes produced by EncodeCommand.
func DecodeCommand(data []byte) (Command, error) {
	// TODO(S1)
	panic("kv: DecodeCommand not implemented (stage S1)")
}
