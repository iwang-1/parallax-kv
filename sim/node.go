package sim

import (
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
