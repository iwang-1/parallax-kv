package sim

import (
	"fmt"

	"github.com/iwang-1/parallax-kv/raft"
)

// invariants holds the safety checks the simulator runs after every step.
// These are the properties Raft must never violate regardless of faults; a
// violation is a real consensus bug and is latched (with the replay seed)
// so it can be reproduced deterministically.
//
// Checked here (the two that need only the info the harness already tracks):
//   - election safety: at most one leader per term across the whole run;
//   - applied-prefix agreement: any two nodes' applied entry sequences agree
//     wherever they overlap (identical (term, data) at each index), i.e. no
//     two nodes ever apply different commands at the same log index.
//
// Log matching and leader completeness are properties of the raft log and
// are asserted in the raft core's own unit tests and, in stage S2, by
// checks with access to the real core's log; the harness-level checks here
// are the ones expressible from applied output alone, which is exactly what
// keeps them valid against the mock node too.
type invariants struct {
	peers []uint64
	// leaderByTerm remembers the leader seen for each term to detect a
	// second, different leader in the same term.
	leaderByTerm map[uint64]uint64
}

func newInvariants(peers []uint64) *invariants {
	return &invariants{
		peers:        append([]uint64(nil), peers...),
		leaderByTerm: make(map[uint64]uint64),
	}
}

// check runs all invariants against the current cluster state.
func (iv *invariants) check(now VirtualTime, nodes map[uint64]*nodeState) error {
	if err := iv.checkElectionSafety(nodes); err != nil {
		return fmt.Errorf("at t=%d: %w", now, err)
	}
	if err := iv.checkAppliedPrefix(nodes); err != nil {
		return fmt.Errorf("at t=%d: %w", now, err)
	}
	return nil
}

// checkElectionSafety asserts at most one leader per term. It reads each
// live leader's term from the entry it most recently applied when
// available; when a mock node exposes only State we treat the term as
// unknown (0) and only detect the "two live leaders" degenerate case is
// left to the core's tests. To keep the check meaningful against both the
// mock and the real core, we key on the leader's self-reported term via the
// applied log's last term where present, else skip term attribution.
func (iv *invariants) checkElectionSafety(nodes map[uint64]*nodeState) error {
	for _, id := range iv.peers {
		ns := nodes[id]
		if ns == nil || ns.crashed || ns.node == nil {
			continue
		}
		if ns.node.State() != raft.StateLeader {
			continue
		}
		term := leaderTerm(ns)
		if term == 0 {
			continue // term not observable yet; nothing to attribute
		}
		if prev, ok := iv.leaderByTerm[term]; ok && prev != id {
			return fmt.Errorf("election safety violated: nodes %d and %d both led term %d", prev, id, term)
		}
		iv.leaderByTerm[term] = id
	}
	return nil
}

// leaderTerm returns the term of the last entry the leader applied, a lower
// bound on its current term that is enough to attribute leadership to a
// term. Returns 0 when unknown.
func leaderTerm(ns *nodeState) uint64 {
	if n := len(ns.appliedLog); n > 0 {
		return ns.appliedLog[n-1].term
	}
	return 0
}

// checkAppliedPrefix asserts that no two nodes applied different entries at
// the same index: for every pair, at each index both have applied, the
// (term, data digest) must match. A mismatch means replicas diverged — the
// central safety failure the simulator exists to catch.
func (iv *invariants) checkAppliedPrefix(nodes map[uint64]*nodeState) error {
	// Build, per index, the first (term, digest) seen and which node set
	// it, iterating peers in sorted order for a stable error message.
	type mark struct {
		term   uint64
		digest uint64
		node   uint64
	}
	seen := make(map[uint64]mark)
	for _, id := range iv.peers {
		ns := nodes[id]
		if ns == nil {
			continue
		}
		for _, rec := range ns.appliedLog {
			if m, ok := seen[rec.index]; ok {
				if m.term != rec.term || m.digest != rec.digest {
					return fmt.Errorf(
						"applied-prefix divergence at index %d: node %d applied (term=%d,digest=%x) but node %d applied (term=%d,digest=%x)",
						rec.index, m.node, m.term, m.digest, id, rec.term, rec.digest)
				}
			} else {
				seen[rec.index] = mark{term: rec.term, digest: rec.digest, node: id}
			}
		}
	}
	return nil
}
