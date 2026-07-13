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

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"

	stdgrpc "google.golang.org/grpc"

	"github.com/iwang-1/parallax-kv/kv"
	"github.com/iwang-1/parallax-kv/proto/kvpb"
	"github.com/iwang-1/parallax-kv/raft"
	"github.com/iwang-1/parallax-kv/storage/disk"
	transportgrpc "github.com/iwang-1/parallax-kv/transport/grpc"
)

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

func (c *Config) withDefaults() {
	if c.TickIntervalMillis <= 0 {
		c.TickIntervalMillis = 20
	}
	if c.HeartbeatTicks <= 0 {
		c.HeartbeatTicks = 5
	}
	if c.ElectionTicks <= c.HeartbeatTicks {
		c.ElectionTicks = 10 * c.HeartbeatTicks
	}
}

// clientKey identifies a client command for waiter matching.
type clientKey struct {
	clientID uint64
	seq      uint64
}

// opResult is the drive loop's answer to a client request.
type opResult struct {
	res       kv.Result
	notLeader bool
	leaderID  uint64
}

type reqKind uint8

const (
	reqPropose reqKind = iota
	reqRead
)

// request is a client operation routed into the single-threaded drive loop.
type request struct {
	kind reqKind
	cmd  kv.Command // reqPropose
	key  string     // reqRead
	resp chan opResult
}

type readWaiter struct {
	key  string
	resp chan opResult
}

// Server is one production node: raft core + disk storage + gRPC transport
// + kv state machine + client service.
type Server struct {
	cfg  Config
	node *raft.Node
	stor *disk.Storage
	sm   *kv.StateMachine
	tr   *transportgrpc.PeerTransport

	reqCh chan *request

	// Fields below are owned exclusively by the drive loop goroutine.
	applied      uint64
	prevLeader   bool
	readToken    uint64
	pendingReads []raft.ReadState
	writeWaiters map[clientKey]chan opResult
	readWaiters  map[string]readWaiter

	clientSrv *stdgrpc.Server
	clientLis net.Listener

	closeOnce sync.Once
}

// New builds a server, opening (and recovering) its on-disk state.
func New(cfg Config) (*Server, error) {
	if cfg.ID == 0 {
		return nil, fmt.Errorf("server: Config.ID must be positive")
	}
	if _, ok := cfg.Peers[cfg.ID]; !ok {
		return nil, fmt.Errorf("server: Config.Peers must include self ID %d", cfg.ID)
	}
	cfg.withDefaults()

	stor, err := disk.Open(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("server: open storage: %w", err)
	}

	peerIDs := make([]uint64, 0, len(cfg.Peers))
	for id := range cfg.Peers {
		peerIDs = append(peerIDs, id)
	}

	rc := raft.Config{
		ID:             cfg.ID,
		Peers:          peerIDs,
		ElectionTicks:  cfg.ElectionTicks,
		HeartbeatTicks: cfg.HeartbeatTicks,
		PreVote:        true,
		Rand:           rand.New(rand.NewSource(time.Now().UnixNano() ^ int64(cfg.ID))),
	}
	node, err := raft.NewNode(rc, stor)
	if err != nil {
		_ = stor.Close()
		return nil, fmt.Errorf("server: new node: %w", err)
	}

	s := &Server{
		cfg:          cfg,
		node:         node,
		stor:         stor,
		sm:           kv.NewStateMachine(),
		reqCh:        make(chan *request, 256),
		writeWaiters: make(map[clientKey]chan opResult),
		readWaiters:  make(map[string]readWaiter),
	}

	// Recover applied state from any persisted snapshot so a restarted node
	// resumes from its compacted prefix rather than index 0.
	if snap, err := stor.Snapshot(); err == nil && snap.Metadata.Index > 0 {
		if len(snap.Data) > 0 {
			if err := s.sm.Restore(snap.Data); err != nil {
				_ = stor.Close()
				return nil, fmt.Errorf("server: restore snapshot: %w", err)
			}
		}
		s.applied = snap.Metadata.Index
	}
	return s, nil
}

// Run starts the node and blocks until ctx is cancelled or a fatal error
// occurs, then shuts down cleanly.
func (s *Server) Run(ctx context.Context) error {
	tr, err := transportgrpc.NewPeerTransport(s.cfg.ID, s.cfg.Peers)
	if err != nil {
		return fmt.Errorf("server: start transport: %w", err)
	}
	s.tr = tr

	// Client-facing KVService.
	lis, err := net.Listen("tcp", s.cfg.ListenClient)
	if err != nil {
		s.shutdown()
		return fmt.Errorf("server: listen client %s: %w", s.cfg.ListenClient, err)
	}
	s.clientLis = lis
	s.clientSrv = stdgrpc.NewServer()
	kvpb.RegisterKVServiceServer(s.clientSrv, &kvService{s: s})
	go func() { _ = s.clientSrv.Serve(lis) }()

	err = s.driveLoop(ctx)
	s.shutdown()
	return err
}

// ClientAddr reports the bound client-facing address (useful when the
// configured address used port 0).
func (s *Server) ClientAddr() string {
	if s.clientLis == nil {
		return s.cfg.ListenClient
	}
	return s.clientLis.Addr().String()
}

func (s *Server) shutdown() {
	s.closeOnce.Do(func() {
		if s.clientSrv != nil {
			s.clientSrv.Stop()
		}
		if s.tr != nil {
			_ = s.tr.Close()
		}
		if s.stor != nil {
			_ = s.stor.Close()
		}
	})
}
