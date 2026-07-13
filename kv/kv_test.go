package kv

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
)

// stateHash is the test's notion of "state hash": SHA-256 of the
// deterministic snapshot encoding.
func stateHash(t *testing.T, m *StateMachine) [32]byte {
	t.Helper()
	snap, err := m.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	return sha256.Sum256(snap)
}

func TestPutGetDelete(t *testing.T) {
	m := NewStateMachine()

	res := m.Apply(Command{ClientID: 1, Seq: 1, Op: OpGet, Key: "a"})
	if res.Status != StatusNotFound {
		t.Fatalf("Get absent: got status %d, want StatusNotFound", res.Status)
	}

	res = m.Apply(Command{ClientID: 1, Seq: 2, Op: OpPut, Key: "a", Value: []byte("v1")})
	if res.Status != StatusOK || res.Version != 1 {
		t.Fatalf("Put: got %+v, want OK version 1", res)
	}

	res = m.Apply(Command{ClientID: 1, Seq: 3, Op: OpGet, Key: "a"})
	if res.Status != StatusOK || string(res.Value) != "v1" || res.Version != 1 {
		t.Fatalf("Get: got %+v, want OK v1 version 1", res)
	}

	res = m.Apply(Command{ClientID: 1, Seq: 4, Op: OpPut, Key: "a", Value: []byte("v2")})
	if res.Status != StatusOK || res.Version != 2 {
		t.Fatalf("second Put: got %+v, want OK version 2", res)
	}

	res = m.Apply(Command{ClientID: 1, Seq: 5, Op: OpDelete, Key: "a"})
	if res.Status != StatusOK || res.Version != 0 {
		t.Fatalf("Delete: got %+v, want OK version 0", res)
	}
	if m.Len() != 0 {
		t.Fatalf("Len after delete: got %d, want 0", m.Len())
	}

	res = m.Apply(Command{ClientID: 1, Seq: 6, Op: OpDelete, Key: "a"})
	if res.Status != StatusNotFound {
		t.Fatalf("Delete absent: got status %d, want StatusNotFound", res.Status)
	}

	// Recreating a deleted key restarts its version at 1.
	res = m.Apply(Command{ClientID: 1, Seq: 7, Op: OpPut, Key: "a", Value: []byte("v3")})
	if res.Status != StatusOK || res.Version != 1 {
		t.Fatalf("Put after delete: got %+v, want OK version 1", res)
	}
}

func TestCASSemantics(t *testing.T) {
	t.Run("create-if-absent succeeds on absent key", func(t *testing.T) {
		m := NewStateMachine()
		res := m.Apply(Command{ClientID: 1, Seq: 1, Op: OpCAS, Key: "k", Value: []byte("v"), Expect: nil})
		if res.Status != StatusOK || res.Version != 1 {
			t.Fatalf("got %+v, want OK version 1", res)
		}
		if got := m.Read("k"); string(got.Value) != "v" {
			t.Fatalf("Read after CAS: got %+v", got)
		}
	})

	t.Run("create-if-absent fails on existing key", func(t *testing.T) {
		m := NewStateMachine()
		m.Apply(Command{ClientID: 1, Seq: 1, Op: OpPut, Key: "k", Value: []byte("cur")})
		res := m.Apply(Command{ClientID: 1, Seq: 2, Op: OpCAS, Key: "k", Value: []byte("v"), Expect: nil})
		if res.Status != StatusCASMismatch || string(res.Value) != "cur" || res.Version != 1 {
			t.Fatalf("got %+v, want CASMismatch with current value cur version 1", res)
		}
		if got := m.Read("k"); string(got.Value) != "cur" || got.Version != 1 {
			t.Fatalf("state changed by failed CAS: %+v", got)
		}
	})

	t.Run("matching expect swaps and bumps version", func(t *testing.T) {
		m := NewStateMachine()
		m.Apply(Command{ClientID: 1, Seq: 1, Op: OpPut, Key: "k", Value: []byte("old")})
		res := m.Apply(Command{ClientID: 1, Seq: 2, Op: OpCAS, Key: "k", Value: []byte("new"), Expect: []byte("old")})
		if res.Status != StatusOK || res.Version != 2 {
			t.Fatalf("got %+v, want OK version 2", res)
		}
		if got := m.Read("k"); string(got.Value) != "new" || got.Version != 2 {
			t.Fatalf("Read after CAS: got %+v", got)
		}
	})

	t.Run("mismatched expect fails and returns current", func(t *testing.T) {
		m := NewStateMachine()
		m.Apply(Command{ClientID: 1, Seq: 1, Op: OpPut, Key: "k", Value: []byte("cur")})
		res := m.Apply(Command{ClientID: 1, Seq: 2, Op: OpCAS, Key: "k", Value: []byte("new"), Expect: []byte("wrong")})
		if res.Status != StatusCASMismatch || string(res.Value) != "cur" || res.Version != 1 {
			t.Fatalf("got %+v, want CASMismatch with current value cur version 1", res)
		}
		if got := m.Read("k"); string(got.Value) != "cur" {
			t.Fatalf("state changed by failed CAS: %+v", got)
		}
	})

	t.Run("non-nil expect fails on absent key", func(t *testing.T) {
		m := NewStateMachine()
		res := m.Apply(Command{ClientID: 1, Seq: 1, Op: OpCAS, Key: "k", Value: []byte("v"), Expect: []byte("x")})
		if res.Status != StatusCASMismatch || res.Value != nil || res.Version != 0 {
			t.Fatalf("got %+v, want CASMismatch on absent key", res)
		}
		if m.Len() != 0 {
			t.Fatal("failed CAS created a key")
		}
	})

	t.Run("empty expect matches empty value, not absent key", func(t *testing.T) {
		m := NewStateMachine()
		// Empty (non-nil) Expect against an absent key must fail: only nil
		// Expect means create-if-absent.
		res := m.Apply(Command{ClientID: 1, Seq: 1, Op: OpCAS, Key: "k", Value: []byte("v"), Expect: []byte{}})
		if res.Status != StatusCASMismatch {
			t.Fatalf("empty expect on absent key: got %+v, want CASMismatch", res)
		}
		m.Apply(Command{ClientID: 1, Seq: 2, Op: OpPut, Key: "k", Value: []byte{}})
		res = m.Apply(Command{ClientID: 1, Seq: 3, Op: OpCAS, Key: "k", Value: []byte("v"), Expect: []byte{}})
		if res.Status != StatusOK || res.Version != 2 {
			t.Fatalf("empty expect on empty value: got %+v, want OK version 2", res)
		}
	})
}

func TestDedupReplay(t *testing.T) {
	m := NewStateMachine()

	put := Command{ClientID: 9, Seq: 1, Op: OpPut, Key: "k", Value: []byte("v")}
	first := m.Apply(put)
	if first.Status != StatusOK || first.Version != 1 {
		t.Fatalf("first apply: got %+v", first)
	}
	hashAfterFirst := stateHash(t, m)

	// Replay of the same (ClientID, Seq): recorded result, no re-execution.
	replay := m.Apply(put)
	if !reflect.DeepEqual(replay, first) {
		t.Fatalf("replay result differs: got %+v, want %+v", replay, first)
	}
	if got := stateHash(t, m); got != hashAfterFirst {
		t.Fatal("replay mutated state")
	}
	if got := m.Read("k"); got.Version != 1 {
		t.Fatalf("replay bumped version: %+v", got)
	}

	// A CAS replay must not re-execute either: re-running it would fail
	// (the expect no longer matches) — the recorded success must be replayed.
	cas := Command{ClientID: 9, Seq: 2, Op: OpCAS, Key: "k", Value: []byte("v2"), Expect: []byte("v")}
	casFirst := m.Apply(cas)
	if casFirst.Status != StatusOK || casFirst.Version != 2 {
		t.Fatalf("CAS first apply: got %+v", casFirst)
	}
	casReplay := m.Apply(cas)
	if !reflect.DeepEqual(casReplay, casFirst) {
		t.Fatalf("CAS replay: got %+v, want recorded %+v", casReplay, casFirst)
	}
	if got := m.Read("k"); string(got.Value) != "v2" || got.Version != 2 {
		t.Fatalf("CAS replay re-executed: %+v", got)
	}

	// A stale seq (below the session's LastSeq) is rejected.
	stale := m.Apply(Command{ClientID: 9, Seq: 1, Op: OpPut, Key: "k", Value: []byte("old")})
	if stale.Status != StatusStaleSeq {
		t.Fatalf("stale seq: got status %d, want StatusStaleSeq", stale.Status)
	}
	if got := m.Read("k"); string(got.Value) != "v2" {
		t.Fatalf("stale command mutated state: %+v", got)
	}
}

func TestDedupIsPerClient(t *testing.T) {
	m := NewStateMachine()
	m.Apply(Command{ClientID: 1, Seq: 5, Op: OpPut, Key: "k", Value: []byte("a")})

	// A different client with the same seq is not a duplicate.
	res := m.Apply(Command{ClientID: 2, Seq: 5, Op: OpPut, Key: "k", Value: []byte("b")})
	if res.Status != StatusOK || res.Version != 2 {
		t.Fatalf("other client same seq: got %+v, want OK version 2", res)
	}
	// Client 1's session is unaffected: seq 5 still replays its own result.
	replay := m.Apply(Command{ClientID: 1, Seq: 5, Op: OpPut, Key: "k", Value: []byte("a")})
	if replay.Status != StatusOK || replay.Version != 1 {
		t.Fatalf("client 1 replay: got %+v, want recorded OK version 1", replay)
	}
}

func TestReadBypassesSessions(t *testing.T) {
	m := NewStateMachine()
	m.Apply(Command{ClientID: 1, Seq: 1, Op: OpPut, Key: "k", Value: []byte("v")})
	before := stateHash(t, m)

	res := m.Read("k")
	if res.Status != StatusOK || string(res.Value) != "v" || res.Version != 1 {
		t.Fatalf("Read: got %+v", res)
	}
	if m.Read("absent").Status != StatusNotFound {
		t.Fatal("Read absent: want StatusNotFound")
	}
	if got := stateHash(t, m); got != before {
		t.Fatal("Read mutated state (session table must not record reads)")
	}
}

func TestResultValueDoesNotAliasState(t *testing.T) {
	m := NewStateMachine()
	val := []byte("orig")
	m.Apply(Command{ClientID: 1, Seq: 1, Op: OpPut, Key: "k", Value: val})

	// Mutating the caller's Put buffer must not change stored state.
	val[0] = 'X'
	if got := m.Read("k"); string(got.Value) != "orig" {
		t.Fatalf("stored value aliases caller buffer: %q", got.Value)
	}

	// Mutating a returned read value must not change stored state.
	res := m.Read("k")
	res.Value[0] = 'Y'
	if got := m.Read("k"); string(got.Value) != "orig" {
		t.Fatalf("returned value aliases stored state: %q", got.Value)
	}

	// Mutating a replayed result must not corrupt the session record.
	get := Command{ClientID: 1, Seq: 2, Op: OpGet, Key: "k"}
	first := m.Apply(get)
	first.Value[0] = 'Z'
	replay := m.Apply(get)
	if string(replay.Value) != "orig" {
		t.Fatalf("session record aliases returned result: %q", replay.Value)
	}
}

func TestSnapshotDeterministicAcrossInsertionOrder(t *testing.T) {
	// Same logical state built in different orders (and with different
	// map-internal history) must snapshot to identical bytes.
	a := NewStateMachine()
	b := NewStateMachine()

	keys := []string{"zebra", "apple", "mango", "kiwi", "pear"}
	for i, k := range keys {
		a.Apply(Command{ClientID: uint64(i + 1), Seq: 1, Op: OpPut, Key: k, Value: []byte(k)})
	}
	for i := len(keys) - 1; i >= 0; i-- {
		k := keys[i]
		b.Apply(Command{ClientID: uint64(i + 1), Seq: 1, Op: OpPut, Key: k, Value: []byte(k)})
	}
	// Churn b's map with keys that are later deleted.
	b.Apply(Command{ClientID: 100, Seq: 1, Op: OpPut, Key: "temp", Value: []byte("x")})
	b.Apply(Command{ClientID: 100, Seq: 2, Op: OpDelete, Key: "temp"})
	// ... and remove the churn client's session by matching a's sessions:
	// a has no client 100, so give a the same session history.
	a.Apply(Command{ClientID: 100, Seq: 1, Op: OpPut, Key: "temp", Value: []byte("x")})
	a.Apply(Command{ClientID: 100, Seq: 2, Op: OpDelete, Key: "temp"})

	snapA, err := a.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot a: %v", err)
	}
	snapB, err := b.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot b: %v", err)
	}
	if !bytes.Equal(snapA, snapB) {
		t.Fatal("snapshots of identical logical state differ")
	}
}

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	m := NewStateMachine()
	m.Apply(Command{ClientID: 1, Seq: 1, Op: OpPut, Key: "a", Value: []byte("va")})
	m.Apply(Command{ClientID: 1, Seq: 2, Op: OpPut, Key: "b", Value: nil})
	m.Apply(Command{ClientID: 2, Seq: 7, Op: OpPut, Key: "c", Value: []byte{}})
	m.Apply(Command{ClientID: 2, Seq: 8, Op: OpCAS, Key: "c", Value: []byte("vc"), Expect: []byte{}})
	m.Apply(Command{ClientID: 3, Seq: 1, Op: OpGet, Key: "a"})

	snap, err := m.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	restored := NewStateMachine()
	// Pre-populate to prove Restore replaces, not merges.
	restored.Apply(Command{ClientID: 99, Seq: 1, Op: OpPut, Key: "junk", Value: []byte("junk")})
	if err := restored.Restore(snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	if restored.Len() != m.Len() {
		t.Fatalf("Len: got %d, want %d", restored.Len(), m.Len())
	}
	if restored.Read("junk").Status != StatusNotFound {
		t.Fatal("Restore merged instead of replacing")
	}
	for _, key := range []string{"a", "b", "c"} {
		got, want := restored.Read(key), m.Read(key)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("Read(%q): got %+v, want %+v", key, got, want)
		}
	}

	// Snapshot of the restored machine is byte-identical.
	snap2, err := restored.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot after restore: %v", err)
	}
	if !bytes.Equal(snap, snap2) {
		t.Fatal("snapshot -> restore -> snapshot is not byte-identical")
	}
}

func TestDedupSurvivesSnapshotRestore(t *testing.T) {
	// The exactly-once story: a replica restored from a snapshot dedups a
	// retried command exactly like one that applied the whole log.
	m := NewStateMachine()
	cas := Command{ClientID: 5, Seq: 3, Op: OpCAS, Key: "k", Value: []byte("v"), Expect: nil}
	recorded := m.Apply(cas)
	if recorded.Status != StatusOK {
		t.Fatalf("CAS: got %+v", recorded)
	}

	snap, err := m.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	restored := NewStateMachine()
	if err := restored.Restore(snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	replay := restored.Apply(cas)
	if !reflect.DeepEqual(replay, recorded) {
		t.Fatalf("post-restore replay: got %+v, want recorded %+v", replay, recorded)
	}
	if got := restored.Read("k"); got.Version != 1 {
		t.Fatalf("post-restore replay re-executed: %+v", got)
	}
	stale := restored.Apply(Command{ClientID: 5, Seq: 2, Op: OpPut, Key: "k", Value: []byte("x")})
	if stale.Status != StatusStaleSeq {
		t.Fatalf("post-restore stale seq: got %+v, want StatusStaleSeq", stale)
	}
}

func TestRestoreErrors(t *testing.T) {
	valid, err := func() ([]byte, error) {
		m := NewStateMachine()
		m.Apply(Command{ClientID: 1, Seq: 1, Op: OpPut, Key: "k", Value: []byte("v")})
		return m.Snapshot()
	}()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	cases := map[string][]byte{
		"empty":            nil,
		"bad version":      append([]byte{0xff}, valid[1:]...),
		"trailing garbage": append(append([]byte{}, valid...), 0x00),
	}
	// Every strict prefix of a valid snapshot is truncated input. The
	// count guards use minimum entry sizes, so a snapshot whose sessions
	// and values are much larger than the minimum ("fat") is also swept:
	// its prefixes pass the count guards and truncate mid-field instead.
	fat, err := func() ([]byte, error) {
		m := NewStateMachine()
		big := bytes.Repeat([]byte{0xab}, 64)
		for client := uint64(1); client <= 2; client++ {
			key := fmt.Sprintf("key-%d", client)
			m.Apply(Command{ClientID: client, Seq: 1, Op: OpPut, Key: key, Value: big})
			// Get records the 64-byte value in the session's LastResult.
			m.Apply(Command{ClientID: client, Seq: 2, Op: OpGet, Key: key})
		}
		return m.Snapshot()
	}()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	for _, snap := range [][]byte{valid, fat} {
		for n := 0; n < len(snap); n++ {
			cases[fmt.Sprintf("len-%d snapshot truncated to %d bytes", len(snap), n)] = snap[:n]
		}
	}
	// Absurd counts must be rejected before allocation.
	hugeSessions := []byte{snapshotEncodingVersion}
	hugeSessions = appendUint64(hugeSessions, ^uint64(0))
	cases["huge session count"] = hugeSessions
	hugeKeys := []byte{snapshotEncodingVersion}
	hugeKeys = appendUint64(hugeKeys, 0) // zero sessions
	hugeKeys = appendUint64(hugeKeys, ^uint64(0))
	cases["huge key count"] = hugeKeys

	for name, snap := range cases {
		t.Run(name, func(t *testing.T) {
			m := NewStateMachine()
			m.Apply(Command{ClientID: 1, Seq: 1, Op: OpPut, Key: "keep", Value: []byte("keep")})
			before := stateHash(t, m)
			if err := m.Restore(snap); err == nil {
				t.Fatal("want error")
			}
			if got := stateHash(t, m); got != before {
				t.Fatal("failed Restore mutated state")
			}
		})
	}
}

// TestApplyPanicsOnInvalidOp pins the contract that an invalid op in a
// committed command is a programming error surfaced by a deterministic
// panic, never a silent no-op (DecodeCommand rejects such commands before
// they can reach the log).
func TestApplyPanicsOnInvalidOp(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("Apply with OpInvalid: want panic")
		}
	}()
	NewStateMachine().Apply(Command{ClientID: 1, Seq: 1, Op: OpInvalid, Key: "k"})
}

// TestDeterministicApply is the determinism property: two fresh machines
// applying the same randomly generated command sequence in the same order
// reach byte-identical snapshots (hence identical state hashes), and a
// third machine that restores from a mid-sequence snapshot and applies the
// remainder converges to the same hash.
func TestDeterministicApply(t *testing.T) {
	const nCmds = 2000
	rng := rand.New(rand.NewSource(0xDE7E12A))

	keys := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
	randVal := func() []byte {
		switch rng.Intn(3) {
		case 0:
			return nil
		default:
			b := make([]byte, rng.Intn(16))
			rng.Read(b)
			return b
		}
	}
	cmds := make([]Command, nCmds)
	seqs := make(map[uint64]uint64)
	for i := range cmds {
		clientID := uint64(1 + rng.Intn(5))
		// Mostly advance the session; sometimes replay the last seq.
		seq := seqs[clientID] + 1
		if seqs[clientID] > 0 && rng.Intn(10) == 0 {
			seq = seqs[clientID]
		} else {
			seqs[clientID] = seq
		}
		cmds[i] = Command{
			ClientID: clientID,
			Seq:      seq,
			Op:       []OpType{OpGet, OpPut, OpDelete, OpCAS}[rng.Intn(4)],
			Key:      keys[rng.Intn(len(keys))],
			Value:    randVal(),
			Expect:   randVal(),
		}
	}

	m1, m2 := NewStateMachine(), NewStateMachine()
	var m3 *StateMachine // restored mid-stream from m1's snapshot
	for i, cmd := range cmds {
		r1 := m1.Apply(cmd)
		r2 := m2.Apply(cmd)
		if !reflect.DeepEqual(r1, r2) {
			t.Fatalf("cmd %d (%+v): results diverge: %+v vs %+v", i, cmd, r1, r2)
		}
		if i == nCmds/2 {
			snap, err := m1.Snapshot()
			if err != nil {
				t.Fatalf("mid-stream Snapshot: %v", err)
			}
			m3 = NewStateMachine()
			if err := m3.Restore(snap); err != nil {
				t.Fatalf("mid-stream Restore: %v", err)
			}
		} else if m3 != nil {
			if r3 := m3.Apply(cmd); !reflect.DeepEqual(r1, r3) {
				t.Fatalf("cmd %d: restored machine diverges: %+v vs %+v", i, r1, r3)
			}
		}
	}

	h1, h2, h3 := stateHash(t, m1), stateHash(t, m2), stateHash(t, m3)
	if h1 != h2 {
		t.Fatal("identical command sequences produced different state hashes")
	}
	if h1 != h3 {
		t.Fatal("snapshot-restored machine diverged from log-applied machine")
	}
}
