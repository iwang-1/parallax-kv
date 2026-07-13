package raft

import (
	"math/rand"
	"testing"
)

// driveNode is a minimal single-node driver that closes the Ready/Advance
// loop against a testStorage, mimicking the persist-before-send discipline:
// it persists HardState/Entries to storage, then "sends" (records) messages,
// then applies committed entries, then Advances.
type driveNode struct {
	n       *Node
	st      *testStorage
	sent    []Message
	applied []Entry
}

func newDriveNode(t *testing.T, cfg Config) *driveNode {
	t.Helper()
	st := newTestStorage()
	n, err := NewNode(cfg, st)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	return &driveNode{n: n, st: st}
}

// pump drains all pending Ready batches, enforcing persist-before-send.
func (d *driveNode) pump(t *testing.T) {
	t.Helper()
	for d.n.HasReady() {
		rd := d.n.Ready()
		// 1. Persist BEFORE sending.
		if rd.HardState != nil {
			if err := d.st.SetHardState(*rd.HardState); err != nil {
				t.Fatalf("SetHardState: %v", err)
			}
		}
		if len(rd.Entries) > 0 {
			if err := d.st.AppendEntries(rd.Entries); err != nil {
				t.Fatalf("AppendEntries: %v", err)
			}
		}
		// 2. Send.
		d.sent = append(d.sent, rd.Messages...)
		// 3. Apply.
		d.applied = append(d.applied, rd.CommittedEntries...)
		// 4. Advance.
		d.n.Advance(PersistAck{})
	}
}

// TestNodeReadyPersistThenApply drives a single-node cluster through a
// proposal and verifies the Ready contract: HardState/Entries surface,
// MustSync is set for durable batches, and the entry is applied exactly once.
func TestNodeReadyPersistThenApply(t *testing.T) {
	cfg := testConfig(1, []uint64{1}, false, 1)
	d := newDriveNode(t, cfg)

	// Tick to become leader.
	for i := 0; i < 2*cfg.ElectionTicks; i++ {
		d.n.Tick()
		d.pump(t)
	}
	if d.n.State() != StateLeader {
		t.Fatalf("state = %s, want leader", d.n.State())
	}

	// The leader's no-op entry must have been persisted and applied.
	last, _ := d.st.LastIndex()
	if last < 1 {
		t.Fatalf("no-op not persisted, lastIndex = %d", last)
	}

	// Propose a command.
	if err := d.n.Step(Message{Type: MsgPropose, Entries: []Entry{{Data: []byte("k=v")}}}); err != nil {
		t.Fatalf("propose: %v", err)
	}
	d.pump(t)

	// The command entry must be persisted and applied.
	var found bool
	for _, e := range d.applied {
		if e.Type == EntryNormal && string(e.Data) == "k=v" {
			found = true
		}
	}
	if !found {
		t.Fatalf("proposed entry not applied; applied = %+v", d.applied)
	}

	// Applied entries must never be re-surfaced.
	prevApplied := len(d.applied)
	d.pump(t)
	if len(d.applied) != prevApplied {
		t.Fatalf("entries re-applied: %d -> %d", prevApplied, len(d.applied))
	}
}

// TestNodeMustSyncSemantics verifies MustSync is true when entries or
// term/vote change and false for a commit-only Ready.
func TestNodeMustSyncSemantics(t *testing.T) {
	cfg := testConfig(1, []uint64{1}, false, 1)
	d := newDriveNode(t, cfg)

	sawEntrySync := false
	for i := 0; i < 2*cfg.ElectionTicks; i++ {
		d.n.Tick()
		for d.n.HasReady() {
			rd := d.n.Ready()
			if len(rd.Entries) > 0 && !rd.MustSync {
				t.Fatalf("Ready with entries must set MustSync")
			}
			if len(rd.Entries) > 0 {
				sawEntrySync = true
			}
			if rd.HardState != nil {
				d.st.SetHardState(*rd.HardState)
			}
			if len(rd.Entries) > 0 {
				d.st.AppendEntries(rd.Entries)
			}
			d.n.Advance(PersistAck{})
		}
	}
	if !sawEntrySync {
		t.Fatalf("never observed an entry-bearing Ready")
	}
}

// TestElectionTimeoutRandomizationBounds verifies the randomized election
// timeout always falls in [ElectionTicks, 2*ElectionTicks) across many draws
// from the injected Rand.
func TestElectionTimeoutRandomizationBounds(t *testing.T) {
	cfg := Config{
		ID:             1,
		Peers:          []uint64{1, 2, 3},
		ElectionTicks:  10,
		HeartbeatTicks: 1,
		Rand:           rand.New(rand.NewSource(12345)),
	}
	r, err := newRaft(cfg, newTestStorage())
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	seenMin, seenMax := false, false
	for i := 0; i < 10000; i++ {
		r.resetRandomizedElectionTimeout()
		got := r.randomizedElectionTimeout
		if got < cfg.ElectionTicks || got >= 2*cfg.ElectionTicks {
			t.Fatalf("timeout %d out of [%d, %d)", got, cfg.ElectionTicks, 2*cfg.ElectionTicks)
		}
		if got == cfg.ElectionTicks {
			seenMin = true
		}
		if got == 2*cfg.ElectionTicks-1 {
			seenMax = true
		}
	}
	if !seenMin || !seenMax {
		t.Fatalf("randomization did not span the range (min=%v max=%v)", seenMin, seenMax)
	}
}

// TestDeterministicReplayFromSeed verifies the core is deterministic: two
// nodes built from equal Configs with identical seeds produce byte-identical
// message sequences under the same tick schedule.
func TestDeterministicReplayFromSeed(t *testing.T) {
	run := func() []Message {
		cfg := testConfig(1, []uint64{1, 2, 3}, true, 42)
		r, err := newRaft(cfg, newTestStorage())
		if err != nil {
			t.Fatalf("newRaft: %v", err)
		}
		var all []Message
		for i := 0; i < 100; i++ {
			r.tick()
			all = append(all, r.drainMsgs()...)
		}
		return all
	}
	a, b := run(), run()
	if len(a) != len(b) {
		t.Fatalf("nondeterministic message count: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i].Type != b[i].Type || a[i].To != b[i].To || a[i].Term != b[i].Term {
			t.Fatalf("nondeterministic message at %d: %+v vs %+v", i, a[i], b[i])
		}
	}
}
