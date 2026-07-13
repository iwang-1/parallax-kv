// Package kv is the replicated application state machine: a string-keyed
// store with Get/Put/Delete/CAS, plus the client session table that gives
// commands exactly-once apply semantics.
//
// The state machine is deterministic and does no I/O: Apply(cmd) -> Result
// is a pure transition, so every replica that applies the same command
// sequence reaches the same state. Sessions are part of that state and are
// serialized into snapshots with it — a replica restored from a snapshot
// dedups retried commands exactly like one that applied the whole log.
package kv

// ValueRecord is the stored value for a key. Version increments on every
// successful mutation of the key (starting at 1) and is returned to
// clients for observability; CAS compares by value bytes, not version.
type ValueRecord struct {
	Value   []byte
	Version uint64
}

// Session is the per-client dedup record: the highest sequence number
// applied for the client and the result it produced, replayed verbatim
// when a retry of that sequence arrives.
type Session struct {
	LastSeq    uint64
	LastResult Result
}

// StateMachine is the KV store plus session table. It is not safe for
// concurrent use; the apply loop serializes access (reads under ReadIndex
// are also funneled through the apply-loop owner).
type StateMachine struct {
	data     map[string]ValueRecord
	sessions map[uint64]Session
}

// NewStateMachine returns an empty state machine.
func NewStateMachine() *StateMachine {
	return &StateMachine{
		data:     make(map[string]ValueRecord),
		sessions: make(map[uint64]Session),
	}
}

// Apply executes one committed command and returns its result.
//
// Dedup contract: if cmd.Seq equals the client's session LastSeq, the
// recorded LastResult is returned without re-executing; if cmd.Seq is
// lower it is a stale retry and a StatusStaleSeq result is returned;
// otherwise the command executes and the session advances. Clients issue
// strictly increasing Seq per ClientID with at most one command in flight.
func (m *StateMachine) Apply(cmd Command) Result {
	// TODO(S1): execute op, maintain sessions.
	panic("kv: Apply not implemented (stage S1)")
}

// Read serves a read-only Get against current state, bypassing the session
// table (reads are idempotent; linearizability comes from ReadIndex, not
// dedup).
func (m *StateMachine) Read(key string) Result {
	// TODO(S1)
	panic("kv: Read not implemented (stage S1)")
}

// Snapshot serializes the full state (data + sessions) deterministically:
// identical states produce identical bytes (keys emitted in sorted order).
func (m *StateMachine) Snapshot() ([]byte, error) {
	// TODO(S1)
	panic("kv: Snapshot not implemented (stage S1)")
}

// Restore replaces the state machine's contents from a Snapshot payload.
func (m *StateMachine) Restore(snapshot []byte) error {
	// TODO(S1)
	panic("kv: Restore not implemented (stage S1)")
}

// Len returns the number of live keys (for tests and invariant checks).
func (m *StateMachine) Len() int { return len(m.data) }
