// Package grpc implements the production transport: the peer-to-peer
// RaftTransport service (a single Step RPC carrying batched Messages) and
// the client-facing KVService (Get/Put/Delete/Cas with session dedup
// fields and leader redirect). Wire types live in proto/raftpb and
// proto/kvpb; this package owns the conversions to and from the raft
// package's native types.
package grpc

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/iwang-1/parallax-kv/proto/raftpb"
	"github.com/iwang-1/parallax-kv/raft"
	"github.com/iwang-1/parallax-kv/transport"
)

// outboundQueue bounds the per-peer send backlog. Raft tolerates message
// loss by design, so a peer that falls behind has its excess sends dropped
// rather than blocking the sender's drive loop.
const outboundQueue = 256

// recvQueue bounds the inbound message buffer handed to the drive loop.
const recvQueue = 1024

// PeerTransport is the gRPC transport.Transport: it maintains one client
// connection per peer for outbound sends and serves the RaftTransport
// service for inbound messages.
type PeerTransport struct {
	raftpb.UnimplementedRaftTransportServer

	self  uint64
	peers map[uint64]string // node ID -> peer RPC address

	srv *grpc.Server
	lis net.Listener

	recvCh chan raft.Message

	mu      sync.Mutex
	senders map[uint64]*peerSender
	closed  bool
}

var _ transport.Transport = (*PeerTransport)(nil)

// peerSender owns one peer's outbound path: a buffered queue drained by a
// single worker goroutine that lazily dials and sends. Isolating each peer
// means a slow or unreachable peer never blocks sends to the others.
type peerSender struct {
	addr string
	ch   chan []raft.Message
	done chan struct{}
}

// NewPeerTransport creates a transport for node self, binds the inbound
// RaftTransport service on the self entry of peers, and dials the remaining
// peers lazily on first send.
func NewPeerTransport(self uint64, peers map[uint64]string) (*PeerTransport, error) {
	addr, ok := peers[self]
	if !ok {
		return nil, fmt.Errorf("transport/grpc: peers map missing self ID %d", self)
	}
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("transport/grpc: listen %s: %w", addr, err)
	}
	t := &PeerTransport{
		self:    self,
		peers:   peers,
		lis:     lis,
		recvCh:  make(chan raft.Message, recvQueue),
		senders: make(map[uint64]*peerSender),
	}
	t.srv = grpc.NewServer()
	raftpb.RegisterRaftTransportServer(t.srv, t)
	go func() { _ = t.srv.Serve(lis) }()
	return t, nil
}

// Addr reports the transport's bound peer-RPC address (useful when the
// configured address used port 0).
func (t *PeerTransport) Addr() string { return t.lis.Addr().String() }

// Step implements raftpb.RaftTransportServer: it delivers every message in
// the request to the local drive loop via Recv. Delivery is best-effort — a
// full inbound buffer drops rather than blocks, since raft tolerates loss.
func (t *PeerTransport) Step(_ context.Context, req *raftpb.StepRequest) (*raftpb.StepResponse, error) {
	for _, pm := range req.GetMessages() {
		m := fromProto(pm)
		select {
		case t.recvCh <- m:
		default:
			// Inbound buffer full: drop. The sender will retransmit on the
			// next heartbeat/append cycle.
		}
	}
	return &raftpb.StepResponse{}, nil
}

// Send implements transport.Transport. It groups messages by destination and
// hands each group to that peer's sender queue. It is fire-and-forget and
// never blocks the drive loop: a full per-peer queue drops the batch.
func (t *PeerTransport) Send(msgs []raft.Message) {
	if len(msgs) == 0 {
		return
	}
	byPeer := make(map[uint64][]raft.Message)
	for _, m := range msgs {
		if m.To == 0 || m.To == t.self {
			continue
		}
		byPeer[m.To] = append(byPeer[m.To], m)
	}
	for to, batch := range byPeer {
		s := t.sender(to)
		if s == nil {
			continue
		}
		select {
		case s.ch <- batch:
		default:
			// Peer backlog full: drop this batch.
		}
	}
}

// sender returns (creating if needed) the peerSender for peer id, or nil if
// the transport is closed or the peer is unknown.
func (t *PeerTransport) sender(id uint64) *peerSender {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil
	}
	if s, ok := t.senders[id]; ok {
		return s
	}
	addr, ok := t.peers[id]
	if !ok {
		return nil
	}
	s := &peerSender{
		addr: addr,
		ch:   make(chan []raft.Message, outboundQueue),
		done: make(chan struct{}),
	}
	t.senders[id] = s
	go s.run()
	return s
}

// run is the per-peer worker: it dials lazily (redialing on failure) and
// forwards each queued batch in one Step RPC.
func (s *peerSender) run() {
	var conn *grpc.ClientConn
	var client raftpb.RaftTransportClient
	defer func() {
		if conn != nil {
			_ = conn.Close()
		}
	}()
	for {
		select {
		case <-s.done:
			return
		case batch := <-s.ch:
			if client == nil {
				c, err := grpc.NewClient(s.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					continue // drop batch; retry dial on next send
				}
				conn, client = c, raftpb.NewRaftTransportClient(c)
			}
			req := &raftpb.StepRequest{Messages: make([]*raftpb.Message, len(batch))}
			for i := range batch {
				req.Messages[i] = toProto(batch[i])
			}
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			if _, err := client.Step(ctx, req); err != nil {
				// Transient failure: drop this batch and drop the conn so the
				// next send redials. Raft's retransmission covers the loss.
				if conn != nil {
					_ = conn.Close()
				}
				conn, client = nil, nil
			}
			cancel()
		}
	}
}

// Recv implements transport.Transport.
func (t *PeerTransport) Recv() <-chan raft.Message { return t.recvCh }

// Close shuts down peer connections and the inbound service; Recv's channel
// is closed.
func (t *PeerTransport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	senders := t.senders
	t.senders = nil
	t.mu.Unlock()

	for _, s := range senders {
		close(s.done)
	}
	t.srv.Stop()
	close(t.recvCh)
	return nil
}
