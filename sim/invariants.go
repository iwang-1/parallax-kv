package sim

import (
	"fmt"
	"sort"

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
//   - leader completeness: every entry covered by a durable commit advance is
//     present, unchanged, in each relevant leader's durable log, or is covered
//     by that leader's snapshot — a legitimately elected leader can never be
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
	// committed remembers every durable HardState.Commit advance and the
	// entry fingerprint stored at that index. It is independent of volatile
	// application progress and survives node crashes.
	committed map[uint64]logMark
	// markCache memoizes each node's durable-log marks so the log-matching
	// and leader-completeness checks — which run every step — re-read the
	// full log only when it actually changed, not on every tick or message
	// delivery. Keyed by node ID.
	markCache map[uint64]*durableCache
	// commitCursor makes folding each storage's durable commit records
	// incremental.
	commitCursor map[uint64]int
	// sortedCommitted is the committed index set in ascending order, cached
	// between steps. iv.committed only ever gains keys (values are enriched in
	// place, never removed), so the cache is valid whenever its length still
	// matches the map's — it is rebuilt only when a new committed index
	// appears, not on every step.
	sortedCommitted    []uint64
	sortedCommittedLen int
	// committedVersion bumps whenever iv.committed changes in any way that can
	// affect a leader-completeness verdict: a new index added, OR an existing
	// mark enriched in place (term/digest/commitTerm filled in from a later
	// durable record). Enrichment does not change the map's length, so a
	// length check alone cannot detect it — this version does.
	committedVersion uint64
	// leaderCheckState memoizes the inputs at which each leader last satisfied
	// leader completeness. The verdict is a pure function of the leader's
	// durable log (fixed by its storage generation), the committed set (fixed
	// by committedVersion), and the leader's term (used to filter which
	// commitments it is answerable for). An unchanged triple cannot change the
	// verdict, so the O(committed) scan is skipped. Keyed by node ID.
	leaderCheckState map[uint64]leaderCheck
	// lastStructural is an exact per-node observation. Unlike a hash over log
	// lengths and tail signatures, storage generations cannot hide an
	// interior same-length rewrite.
	lastStructural []structuralState
}

// leaderCheck is the memo key for a leader-completeness pass: the inputs that
// fully determine its outcome.
type leaderCheck struct {
	generation       uint64
	committedVersion uint64
	term             uint64
}

// logMark is a compact fingerprint of a committed or durable log entry.
type logMark struct {
	term       uint64
	digest     uint64
	commitTerm uint64
	node       uint64 // the node that first established this mark (for messages)
	termKnown  bool
	dataKnown  bool
}

type structuralState struct {
	present         bool
	applied         uint64
	appliedEntries  int
	storageGen      uint64
	committedEvents int
	crashed         bool
	leader          bool
	leaderTerm      uint64
}

// durableCache is reused only for an exact simulator-observed storage
// generation. It also carries snapshot metadata used to distinguish a truly
// missing entry from one compacted into the snapshot.
type durableCache struct {
	generation    uint64
	marks         map[uint64]logMark
	snapshotIndex uint64
	snapshotTerm  uint64
	snapshotData  uint64
}

func newInvariants(peers []uint64) *invariants {
	return &invariants{
		peers:            append([]uint64(nil), peers...),
		leaderByTerm:     make(map[uint64]uint64),
		committed:        make(map[uint64]logMark),
		markCache:        make(map[uint64]*durableCache),
		commitCursor:     make(map[uint64]int),
		leaderCheckState: make(map[uint64]leaderCheck),
	}
}

// check runs all invariants against the current cluster state.
//
// Election safety is cheap and runs every step. The three structural checks
// are skipped only when every exact observation is unchanged. In particular,
// the storage wrapper's generation changes on every successful mutation, so
// a same-length rewrite cannot be skipped or served from a stale cache.
func (iv *invariants) check(now VirtualTime, nodes map[uint64]*nodeState) error {
	if err := iv.checkElectionSafety(nodes); err != nil {
		return fmt.Errorf("at t=%d: %w", now, err)
	}
	if !iv.structuralChanged(nodes) {
		return nil
	}
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

func (iv *invariants) structuralChanged(nodes map[uint64]*nodeState) bool {
	current := make([]structuralState, len(iv.peers))
	for i, id := range iv.peers {
		ns := nodes[id]
		if ns == nil {
			continue
		}
		state := structuralState{
			present:         true,
			applied:         ns.applied,
			appliedEntries:  len(ns.appliedLog),
			storageGen:      storageGeneration(ns.storage),
			committedEvents: len(storageCommitted(ns.storage)),
			crashed:         ns.crashed,
		}
		if !ns.crashed && ns.node != nil && ns.node.State() == raft.StateLeader {
			state.leader = true
			state.leaderTerm = leaderTerm(ns)
		}
		current[i] = state
	}
	if len(current) == len(iv.lastStructural) {
		equal := true
		for i := range current {
			if current[i] != iv.lastStructural[i] {
				equal = false
				break
			}
		}
		if equal {
			return false
		}
	}
	iv.lastStructural = current
	return true
}

// checkElectionSafety asserts at most one leader per term. Leaders expose
// their current term directly; term zero means the test double cannot
// attribute leadership to a term yet.
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

// leaderTerm returns the node's current raft term, the term it leads while in
// StateLeader. It reads the node's own term directly (not the last-applied
// entry's term, which lags: a leader elected in term T campaigns and wins
// before it applies any term-T entry, so the applied term can still be T-1
// while the node genuinely leads T). Election safety must key on the real
// term, or a newly elected leader and the stale leader it replaced would be
// mis-attributed to the same term and reported as a false violation. Returns
// 0 when the node is not live.
func leaderTerm(ns *nodeState) uint64 {
	if ns == nil || ns.crashed || ns.node == nil {
		return 0
	}
	return ns.node.Term()
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

// durableMarks returns a generation-cached view of a node's durable entries
// and snapshot boundary. A crashed node's storage still participates.
func (iv *invariants) durableMarks(ns *nodeState, id uint64) (*durableCache, error) {
	if ns == nil || ns.storage == nil {
		return &durableCache{marks: make(map[uint64]logMark)}, nil
	}
	generation := storageGeneration(ns.storage)
	if cached := iv.markCache[id]; cached != nil && cached.generation == generation {
		return cached, nil
	}

	snapshot, err := ns.storage.Snapshot()
	if err != nil {
		return nil, fmt.Errorf("node %d read snapshot metadata: %w", id, err)
	}
	view := &durableCache{
		generation:    generation,
		marks:         make(map[uint64]logMark),
		snapshotIndex: snapshot.Metadata.Index,
		snapshotTerm:  snapshot.Metadata.Term,
		snapshotData:  fnv64(snapshot.Data),
	}

	last, err := ns.storage.LastIndex()
	if err != nil {
		return nil, fmt.Errorf("node %d read last index: %w", id, err)
	}
	first, err := ns.storage.FirstIndex()
	if err != nil {
		return nil, fmt.Errorf("node %d read first index: %w", id, err)
	}
	if first > 0 && first <= last {
		entries, err := ns.storage.Entries(first, last+1)
		if err != nil {
			return nil, fmt.Errorf("node %d read entries [%d,%d): %w", id, first, last+1, err)
		}
		for _, entry := range entries {
			view.marks[entry.Index] = logMark{
				term: entry.Term, digest: fnv64(entry.Data), node: id,
				termKnown: true, dataKnown: true,
			}
		}
	}
	iv.markCache[id] = view
	return view, nil
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
	snapshots := make(map[uint64]logMark)
	for _, id := range iv.peers {
		view, err := iv.durableMarks(nodes[id], id)
		if err != nil {
			return err
		}
		if view.snapshotIndex > 0 {
			mark := logMark{
				term: view.snapshotTerm, digest: view.snapshotData, node: id,
				termKnown: true, dataKnown: true,
			}
			if prev, ok := snapshots[view.snapshotIndex]; ok &&
				(prev.term != mark.term || prev.digest != mark.digest) {
				return fmt.Errorf(
					"snapshot agreement violated at index %d: node %d has (term=%d,digest=%x), node %d has (term=%d,digest=%x)",
					view.snapshotIndex, prev.node, prev.term, prev.digest, id, mark.term, mark.digest)
			}
			snapshots[view.snapshotIndex] = mark
		}
		for idx, m := range view.marks {
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
// durably committed entry must be present and unchanged in every later leader.
// Commitment comes from persisted HardState.Commit advances, not from the
// volatile applied index. A missing individual entry is acceptable only when
// that leader's snapshot metadata covers its index.
func (iv *invariants) checkLeaderCompleteness(nodes map[uint64]*nodeState) error {
	// Fold newly persisted commit advances into the run-wide authority.
	for _, id := range iv.peers {
		ns := nodes[id]
		if ns == nil {
			continue
		}
		records := storageCommitted(ns.storage)
		cur := iv.commitCursor[id]
		if cur > len(records) {
			cur = 0
		}
		for _, rec := range records[cur:] {
			prev, ok := iv.committed[rec.index]
			if ok {
				if (prev.termKnown && rec.termKnown && prev.term != rec.term) ||
					(prev.dataKnown && rec.dataKnown && prev.digest != rec.digest) {
					return fmt.Errorf(
						"committed divergence at index %d: recorded (term=%d,digest=%x) from node %d but node %d committed (term=%d,digest=%x)",
						rec.index, prev.term, prev.digest, prev.node, id, rec.term, rec.digest)
				}
				changed := false
				if !prev.termKnown && rec.termKnown {
					prev.term = rec.term
					prev.termKnown = true
					changed = true
				}
				if !prev.dataKnown && rec.dataKnown {
					prev.digest = rec.digest
					prev.dataKnown = true
					changed = true
				}
				if prev.commitTerm == 0 || (rec.commitTerm != 0 && rec.commitTerm < prev.commitTerm) {
					prev.commitTerm = rec.commitTerm
					changed = true
				}
				if changed {
					iv.committed[rec.index] = prev
					iv.committedVersion++
				}
			} else {
				iv.committed[rec.index] = logMark{
					term: rec.term, digest: rec.digest, commitTerm: rec.commitTerm, node: id,
					termKnown: rec.termKnown, dataKnown: rec.dataKnown,
				}
				iv.committedVersion++
			}
		}
		iv.commitCursor[id] = len(records)
	}

	// iv.committed only ever gains keys, so the ascending index list is stable
	// between steps until a new index appears. Rebuild it only then, instead
	// of re-sorting the whole set on every step.
	if len(iv.committed) != iv.sortedCommittedLen {
		iv.sortedCommitted = iv.sortedCommitted[:0]
		for index := range iv.committed {
			iv.sortedCommitted = append(iv.sortedCommitted, index)
		}
		sort.Slice(iv.sortedCommitted, func(i, j int) bool {
			return iv.sortedCommitted[i] < iv.sortedCommitted[j]
		})
		iv.sortedCommittedLen = len(iv.committed)
	}
	indexes := iv.sortedCommitted

	// Check each live leader. Filtering by the term in which the commit was
	// persisted avoids holding a stale lower-term leader responsible for a
	// commitment made only after a higher-term leader took over.
	for _, id := range iv.peers {
		ns := nodes[id]
		if ns == nil || ns.crashed || ns.node == nil || ns.node.State() != raft.StateLeader {
			continue
		}
		term := leaderTerm(ns)
		if term == 0 {
			continue
		}
		// The verdict is a pure function of the leader's durable log (fixed by
		// its storage generation), the committed set (fixed by committedVersion),
		// and this leader's term (which selects the commitments it answers for).
		// If none changed since this leader last passed, re-scanning cannot find
		// a new violation — skip it.
		key := leaderCheck{
			generation:       storageGeneration(ns.storage),
			committedVersion: iv.committedVersion,
			term:             term,
		}
		if prev, ok := iv.leaderCheckState[id]; ok && prev == key {
			continue
		}
		view, err := iv.durableMarks(ns, id)
		if err != nil {
			return err
		}
		for _, index := range indexes {
			want := iv.committed[index]
			if want.commitTerm != 0 && want.commitTerm > term {
				continue
			}
			if got, ok := view.marks[index]; ok {
				if (want.termKnown && got.term != want.term) ||
					(want.dataKnown && got.digest != want.digest) {
					return fmt.Errorf(
						"leader completeness violated: leader %d (term %d) has conflicting index %d = (term=%d,digest=%x), committed value is (term=%d,digest=%x)",
						id, term, index, got.term, got.digest, want.term, want.digest)
				}
				continue
			}
			if view.snapshotIndex >= index {
				if view.snapshotIndex == index && want.termKnown && view.snapshotTerm != want.term {
					return fmt.Errorf(
						"leader completeness violated: leader %d (term %d) snapshot has conflicting index %d term %d, committed term is %d",
						id, term, index, view.snapshotTerm, want.term)
				}
				continue
			}
			return fmt.Errorf(
				"leader completeness violated: leader %d (term %d) is missing committed index %d; snapshot covers through %d",
				id, term, index, view.snapshotIndex)
		}
		// This leader satisfied completeness for the current inputs; record
		// them so an unchanged (generation, committedVersion, term) skips the
		// scan next step.
		iv.leaderCheckState[id] = key
	}
	return nil
}
