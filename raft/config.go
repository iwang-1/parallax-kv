package raft

import (
	"errors"
	"math/rand"
)

// Config parameterizes a Node. The core is a pure deterministic state
// machine: it never reads the wall clock, spawns goroutines, or performs
// I/O. Time advances only via Tick, and ALL randomness (election timeout
// jitter) is drawn from the injected Rand — two nodes constructed with
// equal Configs and identical seeds behave identically.
type Config struct {
	// ID is this node's ID. Must be positive; 0 is reserved for "none".
	ID uint64
	// Peers is the fixed cluster membership, including ID itself.
	// Membership change is out of scope for this project.
	Peers []uint64
	// ElectionTicks is the base election timeout in ticks. Each node
	// randomizes its actual timeout uniformly in
	// [ElectionTicks, 2*ElectionTicks) using Rand, re-drawn on every
	// timer reset. Must be greater than HeartbeatTicks.
	ElectionTicks int
	// HeartbeatTicks is the leader's heartbeat interval in ticks.
	HeartbeatTicks int
	// MaxEntriesPerAppend caps the entries carried by one MsgAppend.
	// 0 means no limit.
	MaxEntriesPerAppend int
	// PreVote enables the PreVote phase: a would-be candidate first
	// probes a quorum without incrementing terms, so a node rejoining
	// from a partition with an inflated term cannot depose a healthy
	// leader.
	PreVote bool
	// Rand is the sole source of randomness. Required.
	Rand *rand.Rand
}

func (c *Config) validate() error {
	switch {
	case c.ID == 0:
		return errors.New("raft: Config.ID must be positive")
	case c.ElectionTicks <= c.HeartbeatTicks:
		return errors.New("raft: ElectionTicks must exceed HeartbeatTicks")
	case c.HeartbeatTicks <= 0:
		return errors.New("raft: HeartbeatTicks must be positive")
	case c.Rand == nil:
		return errors.New("raft: Config.Rand is required")
	}
	found := false
	seen := make(map[uint64]struct{}, len(c.Peers))
	for _, p := range c.Peers {
		if p == 0 {
			return errors.New("raft: peer IDs must be positive")
		}
		if _, ok := seen[p]; ok {
			return errors.New("raft: peer IDs must be unique")
		}
		seen[p] = struct{}{}
		if p == c.ID {
			found = true
		}
	}
	if !found {
		return errors.New("raft: Config.Peers must include Config.ID")
	}
	return nil
}
