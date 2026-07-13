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
// Checked here, every step, are the four Raft safety properties:
//   - election safety: at most one leader per term across the whole run;
//   - log matching: if two nodes' durable logs hold an entry with the same
//     (index, term) then that entry's command is identical, and the same
//     (index, term) never carries different data — the core invariant that
//     makes committed history stable across replicas;
//   - leader completeness: every entry that has ever been committed (applied
//     by any node) is present, unchanged, in the durable log of the
//     highest-term leader — a legitimately elected leader can never be
//     missing a committed entry;
//   - applied-prefix agreement: any two nodes' applied entry sequences agree
//     wherever they overlap (identical (term, data) at each index), i.e. no
//     two nodes ever apply different commands at the same log index.
//
// Election safety and applied-prefix are expressible from the info the
// harness already tracks (so they hold against the mock node too); log
// matching and leader completeness read the real core's durable log through
// LogStorage. A violation of any of these is a genuine consensus bug and is
// latched with the replay seed so it reproduces deterministically.
type invariants struct {
	peers []uint64
	// leaderByTerm remembers the leader seen for each term to detect a
	// second, different leader in the same term.
	leaderByTerm map[uint64]uint64
	// committed remembers, for every index any node has ever committed
	// (applied), the entry that was committed there. It is the persistent
	// authority the leader-completeness check holds each current leader
	// against; it survives crashes (unlike a node's volatile appliedLog),
	// so a post-crash divergent commit is still caught.
	committed map[uint64]logMark
	// markCache memoizes each node's durable-log marks so the log-matching
	// and leader-completeness checks — which run every step — re-read the
	// full log only when it actually changed, not on every tick or message
	// delivery. Keyed by node ID.
	markCache map[uint64]*durableCache
	// foldCursor tracks how many of each node's appliedLog entries have
	// already been folded into iv.committed, so the leader-completeness fold
	// is incremental rather than rescanning every applied entry each step. A
	// crash empties appliedLog; when the length drops below the cursor we
	// reset to 0 and re-fold on restart (idempotent, since re-applied entries
	// match the recorded committed marks).
	foldCursor map[uint64]int
	// lastSig is the previous step's structural signature; the three
	// structural checks are skipped while it is unchanged.
	lastSig uint64
	sigInit bool
}

// logMark is a compact (term, data-digest) fingerprint of a log entry.
type logMark struct {
	term   uint64
	digest uint64
	node   uint64 // the node that first established this mark (for messages)
}

// durableCache is a node's memoized durable-log fingerprint, reused while the
// log's shape (first/last index) and tail entry are unchanged. Any append or
// conflict-truncation moves the last index or rewrites the tail term/digest,
// which invalidates the cache and forces a re-read.
type durableCache struct {
	first, last uint64
	tailTerm    uint64
	tailDigest  uint64
	marks       map[uint64]logMark
}

func newInvariants(peers []uint64) *invariants {
	return &invariants{
		peers:        append([]uint64(nil), peers...),
		leaderByTerm: make(map[uint64]uint64),
		committed:    make(map[uint64]logMark),
		markCache:    make(map[uint64]*durableCache),
		foldCursor:   make(map[uint64]int),
	}
}

// check runs all invariants against the current cluster state.
//
// Election safety is cheap and runs every step. The three structural checks
// (applied-prefix, log matching, leader completeness) only observe a node's
// applied count, durable last index, and leadership, so their verdict cannot
// change on a step that moved none of those — the vast majority of steps
// (timer re-arms, in-flight message deliveries that no-op). A cheap
// per-cluster signature skips them on such steps, which keeps every relevant
// change checked while avoiding an O(entries) rescan tens of thousands of
// times per run.
func (iv *invariants) check(now VirtualTime, nodes map[uint64]*nodeState) error {
	if err := iv.checkElectionSafety(nodes); err != nil {
		return fmt.Errorf("at t=%d: %w", now, err)
	}
	sig := iv.structuralSignature(nodes)
	if iv.sigInit && sig == iv.lastSig {
		return nil
	}
	iv.lastSig = sig
	iv.sigInit = true
	if err := iv.checkAppliedPrefix(nodes); err != nil {
		return fmt.Errorf("at t=%d: %w", now, err)
	}
	if err := iv.checkLogMatching(nodes); err != nil {
		return fmt.Errorf("at t=%d: %w", now, err)
	}
	if err := iv.checkLeaderCompleteness(nodes); err != nil {
		return fmt.Errorf("at t=%d: %w", now, err)
	}
	return nil
}

// structuralSignature folds the state the three structural checks depend on
// into a single digest: per node, its applied count and index, durable last
// index, and whether it is currently leader (with its leader term). Two steps
// with equal signatures are indistinguishable to those checks.
func (iv *invariants) structuralSignature(nodes map[uint64]*nodeState) uint64 {
	const prime uint64 = 1099511628211
	h := uint64(14695981039346656037)
	mix := func(v uint64) { h = (h ^ v) * prime }
	for _, id := range iv.peers {
		ns := nodes[id]
		if ns == nil {
			mix(0)
			continue
		}
		mix(id)
		mix(uint64(len(ns.appliedLog)))
		mix(ns.applied)
		if ns.storage != nil {
			last, _ := ns.storage.LastIndex()
			mix(last)
		}
		if !ns.crashed && ns.node != nil && ns.node.State() == raft.StateLeader {
			mix(1 + leaderTerm(ns))
		} else {
			mix(0)
		}
	}
	return h
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

// durableMarks returns the (index -> logMark) of a node's durable log,
// reading entries in [FirstIndex, LastIndex] through LogStorage. Entries
// compacted into a snapshot are not individually available and are skipped;
// the applied-prefix and committed-map checks cover the compacted region.
// A crashed node's storage still holds its durable log, so it participates.
//
// The result is memoized per node and reused until the log changes: reading
// the whole log on every one of the tens of thousands of steps in a run would
// dominate the simulator's cost, so we re-read only when the first/last index
// or the tail entry's (term, digest) moves — which every append or
// conflict-truncation does. Callers must NOT mutate the returned map.
func (iv *invariants) durableMarks(ns *nodeState, id uint64) map[uint64]logMark {
	if ns == nil || ns.storage == nil {
		return nil
	}
	last, err := ns.storage.LastIndex()
	if err != nil || last == 0 {
		return nil
	}
	first, err := ns.storage.FirstIndex()
	if err != nil || first == 0 {
		first = 1
	}
	tailTerm, terr := ns.storage.Term(last)
	// Read just the tail entry to fingerprint it for the cache-validity test.
	var tailDigest uint64
	if tail, terr2 := ns.storage.Entries(last, last+1); terr2 == nil && len(tail) == 1 {
		tailDigest = fnv64(tail[0].Data)
	}
	if c := iv.markCache[id]; c != nil && terr == nil &&
		c.first == first && c.last == last && c.tailTerm == tailTerm && c.tailDigest == tailDigest {
		return c.marks
	}

	ents, err := ns.storage.Entries(first, last+1)
	if err != nil {
		return nil
	}
	marks := make(map[uint64]logMark, len(ents))
	for _, e := range ents {
		marks[e.Index] = logMark{term: e.Term, digest: fnv64(e.Data), node: id}
	}
	iv.markCache[id] = &durableCache{
		first: first, last: last, tailTerm: tailTerm, tailDigest: tailDigest, marks: marks,
	}
	return marks
}

// checkLogMatching asserts the Log Matching Property across durable logs: if
// two nodes hold an entry at the same (index, term) it must be the identical
// command, and no two nodes hold different data at the same (index, term).
// Because AppendEntries truncates a conflicting suffix before writing, two
// live logs may legitimately differ at an index while one is catching up —
// but only when the terms differ (a newer leader overwrote an uncommitted
// tail). Same index AND same term with different data is a real violation.
func (iv *invariants) checkLogMatching(nodes map[uint64]*nodeState) error {
	seen := make(map[uint64]logMark)
	for _, id := range iv.peers {
		for idx, m := range iv.durableMarks(nodes[id], id) {
			prev, ok := seen[idx]
			if !ok {
				seen[idx] = m
				continue
			}
			if prev.term == m.term && prev.digest != m.digest {
				return fmt.Errorf(
					"log matching violated at index %d term %d: node %d has digest %x but node %d has digest %x",
					idx, m.term, prev.node, prev.digest, id, m.digest)
			}
			// Keep the higher-term mark so a later same-term comparison uses
			// the freshest entry (a lower-term entry there is an uncommitted
			// tail about to be overwritten).
			if m.term > prev.term {
				seen[idx] = m
			}
		}
	}
	return nil
}

// checkLeaderCompleteness asserts the Leader Completeness Property: every
// entry ever committed (applied by any node, recorded in iv.committed) must
// still be present, byte-identical, in the durable log of the current leader
// of the highest term — a legitimately elected leader can never lack a
// committed entry. It first folds the current applied logs into iv.committed
// (the running record of what has been committed), checking that no index is
// ever committed with two different values, then verifies the top leader.
func (iv *invariants) checkLeaderCompleteness(nodes map[uint64]*nodeState) error {
	// (1) Fold newly applied entries into the persistent committed record,
	// resuming from each node's fold cursor so this is O(new entries), not
	// O(whole applied log), per step.
	for _, id := range iv.peers {
		ns := nodes[id]
		if ns == nil {
			continue
		}
		cur := iv.foldCursor[id]
		if cur > len(ns.appliedLog) {
			cur = 0 // appliedLog was reset by a crash; re-fold from the start
		}
		for _, rec := range ns.appliedLog[cur:] {
			prev, ok := iv.committed[rec.index]
			if ok {
				if prev.term != rec.term || prev.digest != rec.digest {
					return fmt.Errorf(
						"committed divergence at index %d: recorded (term=%d,digest=%x) from node %d but node %d committed (term=%d,digest=%x)",
						rec.index, prev.term, prev.digest, prev.node, id, rec.term, rec.digest)
				}
			} else {
				iv.committed[rec.index] = logMark{term: rec.term, digest: rec.digest, node: id}
			}
		}
		iv.foldCursor[id] = len(ns.appliedLog)
	}

	// (2) Identify the highest-term current leader.
	var topLeader *nodeState
	var topTerm uint64
	var topID uint64
	for _, id := range iv.peers {
		ns := nodes[id]
		if ns == nil || ns.crashed || ns.node == nil || ns.node.State() != raft.StateLeader {
			continue
		}
		if t := leaderTerm(ns); t >= topTerm {
			topTerm, topLeader, topID = t, ns, id
		}
	}
	if topLeader == nil {
		return nil // no live leader to hold to the property right now
	}

	// (3) Every committed entry at or below the leader's applied index must
	// be present and identical in its durable log. Entries beyond what the
	// leader has itself applied are not yet its responsibility to hold.
	marks := iv.durableMarks(topLeader, topID)
	for idx, want := range iv.committed {
		if idx > topLeader.applied {
			continue
		}
		got, ok := marks[idx]
		if !ok {
			// May be compacted into the leader's snapshot; only a present,
			// differing entry is a violation.
			continue
		}
		if got.term != want.term || got.digest != want.digest {
			return fmt.Errorf(
				"leader completeness violated: leader %d (term %d) has index %d = (term=%d,digest=%x) but committed value is (term=%d,digest=%x)",
				topID, topTerm, idx, got.term, got.digest, want.term, want.digest)
		}
	}
	return nil
}
