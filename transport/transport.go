// Package transport defines how production nodes exchange raft Messages.
// The deterministic simulator does NOT use this interface — its network is
// part of the single-goroutine event loop (see package sim); Transport
// exists only for the real-process runtime.
package transport

import "github.com/iwang-1/parallax-kv/raft"

// Transport moves raft messages between peers.
//
// Send is fire-and-forget and must not block the caller's drive loop:
// Raft tolerates message loss by design, so a transport under pressure
// drops rather than stalls. Recv yields messages from all peers; the
// channel is closed when the transport shuts down.
type Transport interface {
	Send(msgs []raft.Message)
	Recv() <-chan raft.Message
}
