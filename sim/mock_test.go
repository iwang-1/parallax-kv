package sim

import (
	"sort"

	"github.com/iwang-1/parallax-kv/raft"
)

// mockNode is a deterministic stand-in for the real Raft core used to unit
// test the harness itself. It is NOT Raft: it is a fixed-leader replicator
// whose only job is to make the harness exercise every path — proposals,
// replication messages over the network, retransmission, acks, commits,
// committed-entry apply, ReadIndex release, and crash/restart recovery —
// without depending on the (separately built) consensus core.
//
// Model: the lowest-ID node is the permanent leader. A proposal appends an
// entry to the leader's log and commits it immediately (single-node-quorum
// simplification). On every Tick the leader (re)sends, to each follower,
// the next entry the follower is missing, so dropped/reordered deliveries
// and post-partition catch-up all resolve exactly as real replication would
// at the harness's level of abstraction. A follower appends the exactly-next
// entry and acks its new last index; the leader advances that follower's
// nextIndex on the ack. Everything is a pure function of inputs and
// persisted storage, so replays are bit-identical.
type mockNode struct {
	id      uint64
	peers   []uint64
	leader  uint64
	storage raft.LogStorage
	term    uint64

	// log is the leader's authoritative entry list (index i-1 holds entry
	// i). Followers keep only nextIndex; their entries live in storage.
	log       []raft.Entry
	nextIndex uint64            // next index to assign (leader) / to accept (follower)
	peerNext  map[uint64]uint64 // leader's view of each follower's next needed index

	// pending outputs, drained by Ready.
	msgs       []raft.Message
	entries    []raft.Entry
	committed  []raft.Entry
	readStates []raft.ReadState
	hs         *raft.HardState
}

func newMockNode(cfg raft.Config, storage raft.LogStorage) (raftNode, error) {
	peers := append([]uint64(nil), cfg.Peers...)
	sort.Slice(peers, func(i, j int) bool { return peers[i] < peers[j] })
	m := &mockNode{
		id:       cfg.ID,
		peers:    peers,
		leader:   peers[0],
		storage:  storage,
		term:     1,
		peerNext: make(map[uint64]uint64),
	}
	last, _ := storage.LastIndex()
	m.nextIndex = last + 1
	if m.nextIndex == 0 {
		m.nextIndex = 1
	}
	// Rebuild the leader's in-memory log from storage after a restart.
	if m.isLeader() && last > 0 {
		if ents, err := storage.Entries(1, last+1); err == nil {
			m.log = ents
		}
	}
	for _, p := range peers {
		if p != m.id {
			m.peerNext[p] = 1
		}
	}
	return m, nil
}

func (m *mockNode) isLeader() bool { return m.id == m.leader }

// Tick drives leader retransmission: send each follower its next missing
// entry. This is what lets followers catch up after drops, reorders, or a
// healed partition.
func (m *mockNode) Tick() {
	if !m.isLeader() {
		return
	}
	for _, p := range m.peers {
		if p == m.id {
			continue
		}
		next := m.peerNext[p]
		if next >= 1 && next <= uint64(len(m.log)) {
			e := m.log[next-1]
			m.msgs = append(m.msgs, raft.Message{
				Type:    raft.MsgAppend,
				From:    m.id,
				To:      p,
				Term:    m.term,
				Entries: []raft.Entry{e},
			})
		}
	}
}

func (m *mockNode) Step(msg raft.Message) error {
	switch msg.Type {
	case raft.MsgPropose:
		if !m.isLeader() {
			return raft.ErrNotLeader
		}
		for _, e := range msg.Entries {
			e.Index = m.nextIndex
			e.Term = m.term
			m.nextIndex++
			m.log = append(m.log, e)
			m.entries = append(m.entries, e)
			m.committed = append(m.committed, e)
		}
		m.hs = &raft.HardState{Term: m.term, Commit: m.nextIndex - 1}
	case raft.MsgReadIndex:
		if !m.isLeader() {
			return raft.ErrNotLeader
		}
		m.readStates = append(m.readStates, raft.ReadState{
			Index:      m.nextIndex - 1,
			RequestCtx: msg.Context,
		})
	case raft.MsgAppend:
		// Follower: accept only the exactly-next entry so duplicate or
		// reordered deliveries never create a storage gap.
		advanced := false
		for _, e := range msg.Entries {
			if e.Index != m.nextIndex {
				continue
			}
			m.entries = append(m.entries, e)
			m.committed = append(m.committed, e)
			m.nextIndex = e.Index + 1
			advanced = true
		}
		if advanced {
			m.hs = &raft.HardState{Term: m.term, Commit: m.nextIndex - 1}
		}
		// Always ack our current last index so the leader learns where we
		// are (an ack after a duplicate is harmless and idempotent).
		m.msgs = append(m.msgs, raft.Message{
			Type:     raft.MsgAppendResp,
			From:     m.id,
			To:       msg.From,
			Term:     m.term,
			LogIndex: m.nextIndex - 1,
		})
	case raft.MsgAppendResp:
		if !m.isLeader() {
			return nil
		}
		// Advance our view of the follower to the entry after its last.
		m.peerNext[msg.From] = msg.LogIndex + 1
	}
	return nil
}

func (m *mockNode) HasReady() bool {
	return len(m.msgs) > 0 || len(m.entries) > 0 || len(m.committed) > 0 ||
		len(m.readStates) > 0 || m.hs != nil
}

func (m *mockNode) Ready() raft.Ready {
	return raft.Ready{
		Messages:         m.msgs,
		Entries:          m.entries,
		CommittedEntries: m.committed,
		ReadStates:       m.readStates,
		HardState:        m.hs,
		MustSync:         len(m.entries) > 0 || m.hs != nil,
	}
}

func (m *mockNode) Advance(ack raft.PersistAck) {
	m.msgs = nil
	m.entries = nil
	m.committed = nil
	m.readStates = nil
	m.hs = nil
}

func (m *mockNode) State() raft.StateType {
	if m.isLeader() {
		return raft.StateLeader
	}
	return raft.StateFollower
}

func (m *mockNode) Leader() uint64 { return m.leader }
