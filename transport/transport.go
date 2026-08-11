// Package transport defines how production nodes exchange raft Messages.
package transport

import "github.com/iwang-1/parallax-kv/raft"

// Transport moves raft messages between peers. Send is fire-and-forget and must not block the caller's
// drive loop: Raft tolerates message loss by design, so a transport under pressure drops rather than stalls.
type Transport interface {
	Send(msgs []raft.Message)
	Recv() <-chan raft.Message
}
