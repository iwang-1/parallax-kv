// Package grpc implements the production transport: the peer-to-peer
// RaftTransport service (a single Step RPC carrying batched Messages) and
// the client-facing KVService (Get/Put/Delete/Cas with session dedup
// fields and leader redirect). Wire types live in proto/raftpb and
// proto/kvpb; this package owns the conversions to and from the raft
// package's native types.
package grpc

import (
	"github.com/iwang-1/parallax-kv/raft"
	"github.com/iwang-1/parallax-kv/transport"
)

// PeerTransport is the gRPC transport.Transport: it maintains one client
// connection per peer for outbound sends and serves the RaftTransport
// service for inbound messages.
type PeerTransport struct {
	self  uint64
	peers map[uint64]string // node ID -> address
}

var _ transport.Transport = (*PeerTransport)(nil)

// NewPeerTransport creates a transport for node self, dialing the given
// peer addresses lazily.
func NewPeerTransport(self uint64, peers map[uint64]string) (*PeerTransport, error) {
	// TODO(S1)
	panic("transport/grpc: NewPeerTransport not implemented (stage S1)")
}

// Send implements transport.Transport. It is fire-and-forget: messages to
// unreachable peers are dropped (raft tolerates loss), and it never blocks
// the drive loop.
func (t *PeerTransport) Send(msgs []raft.Message) {
	// TODO(S1)
	panic("transport/grpc: Send not implemented (stage S1)")
}

// Recv implements transport.Transport.
func (t *PeerTransport) Recv() <-chan raft.Message {
	// TODO(S1)
	panic("transport/grpc: Recv not implemented (stage S1)")
}

// Close shuts down peer connections and the inbound service; Recv's
// channel is closed.
func (t *PeerTransport) Close() error {
	// TODO(S1)
	panic("transport/grpc: Close not implemented (stage S1)")
}
