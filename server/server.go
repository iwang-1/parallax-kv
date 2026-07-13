// Package server is the production runtime around the pure raft core: it
// owns the drive loop that the simulator owns in tests.
//
// Drive loop contract (per Ready batch, order is load-bearing):
//
//	persist HardState/Entries/Snapshot (group-commit fsync if MustSync)
//	  -> send Messages
//	  -> apply Snapshot + CommittedEntries to the kv state machine
//	  -> release ReadStates whose index has been applied
//	  -> Advance
//
// Persist-BEFORE-send is the safety-critical edge: a vote or log promise
// sent before it is durable can be un-made by a crash. The integration
// tests exercise this ordering explicitly.
package server

import "context"

// Config parameterizes a server node.
type Config struct {
	// ID is this node's raft ID (positive).
	ID uint64
	// Peers maps every node ID in the cluster (including ID) to its
	// peer-RPC listen address.
	Peers map[uint64]string
	// ClientAddrs maps node IDs to client-facing addresses, used for
	// leader redirect hints. May be nil if redirects should carry only
	// the leader ID.
	ClientAddrs map[uint64]string
	// DataDir is the WAL + snapshot directory.
	DataDir string
	// ListenPeer and ListenClient are this node's bind addresses for the
	// RaftTransport and KVService services.
	ListenPeer   string
	ListenClient string
	// TickInterval is the real-time duration of one raft tick.
	TickIntervalMillis int
	// ElectionTicks/HeartbeatTicks feed raft.Config.
	ElectionTicks  int
	HeartbeatTicks int
}

// Server is one production node: raft core + disk storage + gRPC
// transport + kv state machine + client service.
type Server struct {
	cfg Config
}

// New builds a server, opening (and recovering) its on-disk state.
func New(cfg Config) (*Server, error) {
	// TODO(S4)
	panic("server: New not implemented (stage S4)")
}

// Run starts the node and blocks until ctx is cancelled or a fatal error
// occurs, then shuts down cleanly.
func (s *Server) Run(ctx context.Context) error {
	// TODO(S4)
	panic("server: Run not implemented (stage S4)")
}
