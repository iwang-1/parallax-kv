package raft

import (
	"math/rand"
	"sort"
	"testing"
)

// testStorage is a minimal in-memory LogStorage for core unit tests. It lives
// in the raft test package to avoid an import cycle with storage/mem (which
// itself imports raft). Index 1 is the first log index; there is no snapshot
// support here (snapshot storage is exercised in storage/mem's own tests).
type testStorage struct {
	hard    HardState
	ents    []Entry // ents[i] has index i+1
	snap    Snapshot
	hasSnap bool
}

func newTestStorage() *testStorage { return &testStorage{} }

func (s *testStorage) base() uint64 {
	if s.hasSnap {
		return s.snap.Metadata.Index
	}
	return 0
}

func (s *testStorage) AppendEntries(entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}
	base := s.base()
	first := entries[0].Index
	pos := first - base - 1
	if pos > uint64(len(s.ents)) {
		return ErrUnavailable
	}
	s.ents = append(append([]Entry(nil), s.ents[:pos]...), entries...)
	return nil
}

func (s *testStorage) SetHardState(st HardState) error { s.hard = st; return nil }
func (s *testStorage) HardState() (HardState, error)   { return s.hard, nil }

func (s *testStorage) Entries(lo, hi uint64) ([]Entry, error) {
	base := s.base()
	if lo <= base {
		return nil, ErrCompacted
	}
	if hi > base+uint64(len(s.ents))+1 {
		return nil, ErrUnavailable
	}
	if lo >= hi {
		return nil, nil
	}
	out := make([]Entry, hi-lo)
	copy(out, s.ents[lo-base-1:hi-base-1])
	return out, nil
}

func (s *testStorage) Term(i uint64) (uint64, error) {
	base := s.base()
	if i == base {
		if s.hasSnap {
			return s.snap.Metadata.Term, nil
		}
		return 0, nil
	}
	if i < base {
		return 0, ErrCompacted
	}
	if i > base+uint64(len(s.ents)) {
		return 0, ErrUnavailable
	}
	return s.ents[i-base-1].Term, nil
}

func (s *testStorage) FirstIndex() (uint64, error) { return s.base() + 1, nil }
func (s *testStorage) LastIndex() (uint64, error)  { return s.base() + uint64(len(s.ents)), nil }

func (s *testStorage) ApplySnapshot(snap Snapshot) error {
	s.snap = snap
	s.hasSnap = true
	s.ents = nil
	return nil
}

func (s *testStorage) Snapshot() (Snapshot, error) { return s.snap, nil }

// testConfig builds a Config with a fixed-seed Rand so tests are
// deterministic. Election/heartbeat ticks are small round numbers.
func testConfig(id uint64, peers []uint64, preVote bool, seed int64) Config {
	return Config{
		ID:             id,
		Peers:          peers,
		ElectionTicks:  10,
		HeartbeatTicks: 1,
		PreVote:        preVote,
		Rand:           rand.New(rand.NewSource(seed)),
	}
}

// newTestRaft constructs a raft over a fresh mem storage.
func newTestRaft(t *testing.T, id uint64, peers []uint64, preVote bool) *raft {
	t.Helper()
	r, err := newRaft(testConfig(id, peers, preVote, int64(id)), newTestStorage())
	if err != nil {
		t.Fatalf("newRaft(%d): %v", id, err)
	}
	return r
}

// drainMsgs returns and clears the outbound queue.
func (r *raft) drainMsgs() []Message {
	ms := r.msgs
	r.msgs = nil
	return ms
}

// network is a minimal, single-goroutine message router for multi-node
// tests. It is deterministic: messages are delivered in send order and peers
// iterated in sorted ID order.
type network struct {
	t     *testing.T
	peers map[uint64]*raft
	ids   []uint64
	// dropped IDs neither send nor receive (partition simulation).
	isolated map[uint64]bool
}

func newNetwork(t *testing.T, preVote bool, ids ...uint64) *network {
	t.Helper()
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	nw := &network{
		t:        t,
		peers:    make(map[uint64]*raft),
		ids:      append([]uint64(nil), ids...),
		isolated: make(map[uint64]bool),
	}
	for _, id := range ids {
		r, err := newRaft(testConfig(id, ids, preVote, int64(id)), newTestStorage())
		if err != nil {
			t.Fatalf("newRaft(%d): %v", id, err)
		}
		nw.peers[id] = r
	}
	return nw
}

// isolate removes a node from the network (drops all its traffic both ways).
func (nw *network) isolate(id uint64) { nw.isolated[id] = true }

// recover reconnects a previously isolated node.
func (nw *network) recover(id uint64) { delete(nw.isolated, id) }

// deliverAll routes every queued message to its destination until the
// network quiesces (no node has pending output). Isolated nodes' traffic is
// dropped.
func (nw *network) deliverAll() {
	for step := 0; step < 10000; step++ {
		var batch []Message
		for _, id := range nw.ids {
			r := nw.peers[id]
			if nw.isolated[id] {
				r.msgs = nil
				continue
			}
			batch = append(batch, r.drainMsgs()...)
		}
		if len(batch) == 0 {
			return
		}
		for _, m := range batch {
			if nw.isolated[m.To] || nw.isolated[m.From] {
				continue
			}
			dst, ok := nw.peers[m.To]
			if !ok {
				continue
			}
			if err := dst.Step(m); err != nil {
				nw.t.Fatalf("Step(%v): %v", m.Type, err)
			}
		}
	}
	nw.t.Fatal("deliverAll did not quiesce (possible message storm)")
}

// tickUntilElection ticks node id until it starts (or completes) an election,
// then delivers messages until quiescence.
func (nw *network) tickUntilLeader(id uint64) {
	r := nw.peers[id]
	for i := 0; i < 3*r.cfg.ElectionTicks; i++ {
		r.tick()
		nw.deliverAll()
		if r.state == StateLeader {
			return
		}
	}
}

// leaders returns the IDs of all nodes currently believing they are leader.
func (nw *network) leaders() []uint64 {
	var ls []uint64
	for _, id := range nw.ids {
		if nw.peers[id].state == StateLeader {
			ls = append(ls, id)
		}
	}
	return ls
}
