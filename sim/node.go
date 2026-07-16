package sim

import (
	"fmt"

	"github.com/iwang-1/parallax-kv/kv"
	"github.com/iwang-1/parallax-kv/raft"
	"github.com/iwang-1/parallax-kv/storage/mem"
)

// raftNode is the slice of *raft.Node the simulator drives. Depending on
// the interface rather than the concrete type lets the harness be unit
// tested against a trivial mock node (an echo state machine) without the
// real consensus core — the real core is integrated in stage S2. The
// method set is exactly that of *raft.Node, so *raft.Node satisfies it.
type raftNode interface {
	Tick()
	Step(m raft.Message) error
	HasReady() bool
	Ready() raft.Ready
	Advance(ack raft.PersistAck)
	State() raft.StateType
	Leader() uint64
	// Term reports the node's current raft term. The invariant checker keys
	// election safety on it: a leader's role is only meaningful paired with
	// the term it leads, and the last-applied-entry term is a stale proxy (a
	// freshly elected leader has not yet applied a current-term entry).
	Term() uint64
}

// nodeFactory constructs a node from its raft.Config and durable storage.
// The default wraps raft.NewNode; tests inject a mock.
type nodeFactory func(cfg raft.Config, storage raft.LogStorage) (raftNode, error)

// storageFactory constructs the durable storage for a node. The object it
// returns must survive Crash/Restart (that is what models power-loss
// durability), so the simulator creates it once per node and never on
// restart. The default is an in-memory storage.
type storageFactory func(id uint64) raft.LogStorage

func defaultNodeFactory(cfg raft.Config, storage raft.LogStorage) (raftNode, error) {
	return raft.NewNode(cfg, storage)
}

func defaultStorageFactory(id uint64) raft.LogStorage { return mem.New() }

// committedRec records what became durably committed when HardState.Commit
// advanced. Snapshot-covered entries may lack an individual term or digest,
// but their indexes still matter to leader completeness.
type committedRec struct {
	index      uint64
	term       uint64
	digest     uint64
	commitTerm uint64
	termKnown  bool
	dataKnown  bool
}

// trackedStorage wraps the simulator's durable storage. The Raft core still
// sees exactly LogStorage, while the checker gets two simulator-local facts
// that LogStorage does not expose: an exact mutation generation and a durable
// record of commit advances. Embedding preserves the read-only API and keeps
// this wrapper independent of a concrete storage implementation.
type trackedStorage struct {
	raft.LogStorage

	generation    uint64
	durableCommit uint64
	committed     []committedRec
}

func trackStorage(storage raft.LogStorage) raft.LogStorage {
	if _, ok := storage.(*trackedStorage); ok {
		return storage
	}
	return &trackedStorage{LogStorage: storage}
}

func (s *trackedStorage) AppendEntries(entries []raft.Entry) error {
	if err := s.LogStorage.AppendEntries(entries); err != nil {
		return err
	}
	s.generation++
	return nil
}

func (s *trackedStorage) ApplySnapshot(snapshot raft.Snapshot) error {
	if err := s.LogStorage.ApplySnapshot(snapshot); err != nil {
		return err
	}
	s.generation++
	if snapshot.Metadata.Index > s.durableCommit {
		// A snapshot proves the covered prefix is committed, but only its
		// boundary term is available. Emit records before moving the frontier
		// so a later HardState update cannot make snapshot-only commitments
		// invisible to the checker.
		for index := s.durableCommit + 1; index <= snapshot.Metadata.Index; index++ {
			rec := committedRec{index: index}
			if index == snapshot.Metadata.Index {
				rec.term = snapshot.Metadata.Term
				rec.termKnown = true
			}
			s.committed = append(s.committed, rec)
		}
		s.durableCommit = snapshot.Metadata.Index
	}
	return nil
}

func (s *trackedStorage) SetHardState(hs raft.HardState) error {
	if err := s.LogStorage.SetHardState(hs); err != nil {
		return err
	}
	s.generation++
	return s.recordCommitAdvance(hs.Commit, hs.Term)
}

func (s *trackedStorage) recordCommitAdvance(commit, commitTerm uint64) error {
	if commit <= s.durableCommit {
		// ApplySnapshot runs before SetHardState in a Ready batch. Enrich the
		// snapshot-derived records once that batch's persisted term arrives,
		// and append copies so a checker that already folded them sees the
		// stronger attribution on its next pass.
		var enriched []committedRec
		for i := range s.committed {
			if s.committed[i].index <= commit && s.committed[i].commitTerm == 0 && commitTerm != 0 {
				s.committed[i].commitTerm = commitTerm
				enriched = append(enriched, s.committed[i])
			}
		}
		s.committed = append(s.committed, enriched...)
		return nil
	}

	snapshot, _ := s.LogStorage.Snapshot()
	records := make([]committedRec, 0, commit-s.durableCommit)
	for index := s.durableCommit + 1; index <= commit; index++ {
		rec := committedRec{index: index, commitTerm: commitTerm}
		if entries, err := s.LogStorage.Entries(index, index+1); err == nil && len(entries) == 1 {
			rec.term = entries[0].Term
			rec.digest = fnv64(entries[0].Data)
			rec.termKnown = true
			rec.dataKnown = true
		} else {
			if term, err := s.LogStorage.Term(index); err == nil {
				rec.term = term
				rec.termKnown = true
			}
			if snapshot.Metadata.Index < index {
				return fmt.Errorf("durable commit advanced to unavailable index %d", index)
			}
		}
		records = append(records, rec)
	}
	s.committed = append(s.committed, records...)
	s.durableCommit = commit
	return nil
}

func storageGeneration(storage raft.LogStorage) uint64 {
	if tracked, ok := storage.(*trackedStorage); ok {
		return tracked.generation
	}
	return 0
}

func storageCommitted(storage raft.LogStorage) []committedRec {
	if tracked, ok := storage.(*trackedStorage); ok {
		return tracked.committed
	}
	return nil
}

// appliedRec is a compact record of one entry applied by a node, kept so
// the applied-prefix invariant can compare nodes without inspecting their
// internal logs.
type appliedRec struct {
	index uint64
	term  uint64
	// digest is a cheap content hash of the entry data; two nodes that
	// applied the same index/term must have applied identical data.
	digest uint64
}

// nodeState is the simulator's per-node bookkeeping. The durable half
// (storage) survives a Crash; the volatile half (node, sm, applied,
// appliedLog) is discarded on Crash and rebuilt on Restart, exactly the
// split a real crash imposes.
type nodeState struct {
	id      uint64
	storage raft.LogStorage

	// Volatile state (nil / empty while crashed).
	node    raftNode
	sm      *kv.StateMachine
	applied uint64
	// appliedLog is the ordered list of entries this node has applied,
	// used by the applied-prefix invariant checker.
	appliedLog []appliedRec
	// pendingReads are ReadIndex releases whose read index has not yet been
	// reached by applied; they are served once applied catches up.
	pendingReads []raft.ReadState

	crashed bool
}

// fnv64 is a small deterministic content digest (FNV-1a) used for the
// applied-entry records. It avoids pulling entry payloads into the
// invariant checker while still detecting content divergence.
func fnv64(b []byte) uint64 {
	const (
		offset uint64 = 14695981039346656037
		prime  uint64 = 1099511628211
	)
	h := offset
	for _, c := range b {
		h ^= uint64(c)
		h *= prime
	}
	return h
}
