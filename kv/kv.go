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

import (
	"bytes"
	"fmt"
	"sort"
)

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
	if sess, ok := m.sessions[cmd.ClientID]; ok {
		switch {
		case cmd.Seq == sess.LastSeq:
			// Retry of the last applied command: replay the recorded
			// result without touching state.
			return sess.LastResult.clone()
		case cmd.Seq < sess.LastSeq:
			return Result{Status: StatusStaleSeq}
		}
	}
	res := m.execute(cmd)
	m.sessions[cmd.ClientID] = Session{LastSeq: cmd.Seq, LastResult: res}
	return res.clone()
}

// execute performs cmd against the store, unconditionally (dedup has
// already been resolved by Apply). The returned Result owns its Value
// slice: it never aliases caller input or store state.
func (m *StateMachine) execute(cmd Command) Result {
	switch cmd.Op {
	case OpGet:
		return m.get(cmd.Key)
	case OpPut:
		rec := ValueRecord{
			Value:   bytes.Clone(cmd.Value),
			Version: m.data[cmd.Key].Version + 1,
		}
		m.data[cmd.Key] = rec
		return Result{Status: StatusOK, Version: rec.Version}
	case OpDelete:
		if _, ok := m.data[cmd.Key]; !ok {
			return Result{Status: StatusNotFound}
		}
		delete(m.data, cmd.Key)
		return Result{Status: StatusOK}
	case OpCAS:
		return m.cas(cmd)
	default:
		// Commands enter Apply only from committed log entries that
		// DecodeCommand validated; an unknown op here is a programming
		// error, and panicking is deterministic across replicas.
		panic(fmt.Sprintf("kv: invalid op %d in committed command", cmd.Op))
	}
}

func (m *StateMachine) get(key string) Result {
	rec, ok := m.data[key]
	if !ok {
		return Result{Status: StatusNotFound}
	}
	return Result{Status: StatusOK, Value: bytes.Clone(rec.Value), Version: rec.Version}
}

func (m *StateMachine) cas(cmd Command) Result {
	cur, exists := m.data[cmd.Key]
	if cmd.Expect == nil {
		// nil Expect = create-if-absent.
		if exists {
			return Result{Status: StatusCASMismatch, Value: bytes.Clone(cur.Value), Version: cur.Version}
		}
	} else {
		if !exists {
			return Result{Status: StatusCASMismatch}
		}
		if !bytes.Equal(cur.Value, cmd.Expect) {
			return Result{Status: StatusCASMismatch, Value: bytes.Clone(cur.Value), Version: cur.Version}
		}
	}
	rec := ValueRecord{Value: bytes.Clone(cmd.Value), Version: cur.Version + 1}
	m.data[cmd.Key] = rec
	return Result{Status: StatusOK, Version: rec.Version}
}

// clone returns a copy of r whose Value does not alias r's.
func (r Result) clone() Result {
	r.Value = bytes.Clone(r.Value)
	return r
}

// Read serves a read-only Get against current state, bypassing the session
// table (reads are idempotent; linearizability comes from ReadIndex, not
// dedup).
func (m *StateMachine) Read(key string) Result {
	return m.get(key)
}

// snapshotEncodingVersion is the first byte of every snapshot payload.
const snapshotEncodingVersion byte = 1

// Snapshot serializes the full state (data + sessions) deterministically:
// identical states produce identical bytes (keys emitted in sorted order).
//
// Layout (big-endian): version(1) |
// sessionCount(8) | per session (clientID ascending): clientID(8),
// lastSeq(8), result{status(1), value presence(1)[+len(4)+bytes],
// version(8)} |
// keyCount(8) | per key (key ascending): keyLen(4)+key,
// value presence(1)[+len(4)+bytes], version(8).
func (m *StateMachine) Snapshot() ([]byte, error) {
	buf := []byte{snapshotEncodingVersion}

	clientIDs := make([]uint64, 0, len(m.sessions))
	for id := range m.sessions {
		clientIDs = append(clientIDs, id)
	}
	sort.Slice(clientIDs, func(i, j int) bool { return clientIDs[i] < clientIDs[j] })
	buf = appendUint64(buf, uint64(len(clientIDs)))
	for _, id := range clientIDs {
		sess := m.sessions[id]
		buf = appendUint64(buf, id)
		buf = appendUint64(buf, sess.LastSeq)
		buf = append(buf, byte(sess.LastResult.Status))
		buf = appendOptBytes(buf, sess.LastResult.Value)
		buf = appendUint64(buf, sess.LastResult.Version)
	}

	keys := make([]string, 0, len(m.data))
	for k := range m.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	buf = appendUint64(buf, uint64(len(keys)))
	for _, k := range keys {
		rec := m.data[k]
		buf = appendString(buf, k)
		buf = appendOptBytes(buf, rec.Value)
		buf = appendUint64(buf, rec.Version)
	}
	return buf, nil
}

// Restore replaces the state machine's contents from a Snapshot payload.
// On error the state machine is left unchanged.
func (m *StateMachine) Restore(snapshot []byte) error {
	d := &decoder{buf: snapshot}
	version, err := d.byte()
	if err != nil {
		return err
	}
	if version != snapshotEncodingVersion {
		return fmt.Errorf("kv: unknown snapshot encoding version %d", version)
	}

	nSessions, err := d.uint64()
	if err != nil {
		return err
	}
	// Each session occupies >= 26 bytes; reject counts the input cannot hold
	// before sizing the map (guards against corrupt-count allocations).
	if nSessions > uint64(d.remaining())/26 {
		return fmt.Errorf("kv: snapshot session count %d exceeds input size", nSessions)
	}
	sessions := make(map[uint64]Session, nSessions)
	for i := uint64(0); i < nSessions; i++ {
		id, err := d.uint64()
		if err != nil {
			return err
		}
		var sess Session
		if sess.LastSeq, err = d.uint64(); err != nil {
			return err
		}
		status, err := d.byte()
		if err != nil {
			return err
		}
		sess.LastResult.Status = Status(status)
		if sess.LastResult.Value, err = d.optBytes(); err != nil {
			return err
		}
		if sess.LastResult.Version, err = d.uint64(); err != nil {
			return err
		}
		sessions[id] = sess
	}

	nKeys, err := d.uint64()
	if err != nil {
		return err
	}
	// Each key entry occupies >= 13 bytes; see the session-count guard.
	if nKeys > uint64(d.remaining())/13 {
		return fmt.Errorf("kv: snapshot key count %d exceeds input size", nKeys)
	}
	data := make(map[string]ValueRecord, nKeys)
	for i := uint64(0); i < nKeys; i++ {
		key, err := d.string()
		if err != nil {
			return err
		}
		var rec ValueRecord
		if rec.Value, err = d.optBytes(); err != nil {
			return err
		}
		if rec.Version, err = d.uint64(); err != nil {
			return err
		}
		data[key] = rec
	}
	if err := d.finish(); err != nil {
		return err
	}

	m.data = data
	m.sessions = sessions
	return nil
}

// Len returns the number of live keys (for tests and invariant checks).
func (m *StateMachine) Len() int { return len(m.data) }
