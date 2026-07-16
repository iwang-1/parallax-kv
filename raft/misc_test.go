package raft

import (
	"math/rand"
	"testing"
)

// TestConfigValidate covers the Config validation rules.
func TestConfigValidate(t *testing.T) {
	base := func() Config {
		return Config{ID: 1, Peers: []uint64{1, 2, 3}, ElectionTicks: 10, HeartbeatTicks: 1, Rand: rand.New(rand.NewSource(1))}
	}
	tests := []struct {
		name    string
		mutate  func(c *Config)
		wantErr bool
	}{
		{"valid", func(c *Config) {}, false},
		{"zero id", func(c *Config) { c.ID = 0 }, true},
		{"election not gt heartbeat", func(c *Config) { c.ElectionTicks = 1; c.HeartbeatTicks = 1 }, true},
		{"zero heartbeat", func(c *Config) { c.HeartbeatTicks = 0 }, true},
		{"nil rand", func(c *Config) { c.Rand = nil }, true},
		{"zero peer id", func(c *Config) { c.Peers = []uint64{1, 0, 3} }, true},
		{"duplicate peer id", func(c *Config) { c.Peers = []uint64{1, 2, 2} }, true},
		{"duplicate self id", func(c *Config) { c.Peers = []uint64{1, 1, 2} }, true},
		{"peers missing self", func(c *Config) { c.Peers = []uint64{2, 3} }, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := base()
			tc.mutate(&c)
			err := c.validate()
			if (err != nil) != tc.wantErr {
				t.Fatalf("validate err = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}

// TestTermBasedBacktracking verifies decrHint uses HintTerm to skip an entire
// conflicting term in one round trip rather than decrementing by one.
func TestTermBasedBacktracking(t *testing.T) {
	// Leader log: index 1..5 at terms [1,1,1,4,4]; current term 5.
	lead := newTestRaft(t, 1, []uint64{1, 2, 3}, false)
	lead.Term = 5
	lead.state = StateLeader
	lead.lead = 1
	lead.raftLog.append(
		Entry{Term: 1, Index: 1}, Entry{Term: 1, Index: 2}, Entry{Term: 1, Index: 3},
		Entry{Term: 4, Index: 4}, Entry{Term: 4, Index: 5},
	)
	lead.prs = map[uint64]*progress{
		1: {Match: 5, Next: 6},
		2: {Match: 0, Next: 6},
		3: {Match: 0, Next: 6},
	}

	// Follower rejects at index 5 with a hint that its highest term is 2 at
	// index 3 (its log diverged with a shorter/older term-2 tail).
	resp := Message{
		Type: MsgAppendResp, From: 2, To: 1, Reject: true,
		LogIndex: 5, RejectHint: 3, HintTerm: 2,
	}
	lead.handleAppendResp(resp)

	// decrHint should have scanned back past the leader's term-4 entries to
	// the first index whose term is <= 2 (index 3, term 1), so Next becomes 4.
	if lead.prs[2].Next != 4 {
		t.Fatalf("Next after term-based backtrack = %d, want 4", lead.prs[2].Next)
	}
}

// TestSnapshotRestoreExtensionPoint exercises the InstallSnapshot receive
// path (an S3 extension point wired in S1): a follower far behind installs a
// snapshot that advances its commit index and last index.
func TestSnapshotRestoreExtensionPoint(t *testing.T) {
	f := newTestRaft(t, 2, []uint64{1, 2, 3}, false)
	f.becomeFollower(4, 1)

	snap := Snapshot{
		Metadata: SnapshotMetadata{Index: 10, Term: 4},
		Data:     []byte("state"),
	}
	f.Step(Message{Type: MsgInstallSnapshot, From: 1, To: 2, Term: 4, Snapshot: &snap})

	if f.raftLog.committed != 10 {
		t.Fatalf("committed after snapshot = %d, want 10", f.raftLog.committed)
	}
	if f.raftLog.lastIndex() != 10 {
		t.Fatalf("lastIndex after snapshot = %d, want 10", f.raftLog.lastIndex())
	}
	// The follower must ack installation at the snapshot's last index.
	ms := f.drainMsgs()
	if len(ms) != 1 || ms[0].Type != MsgInstallSnapshotResp || ms[0].LogIndex != 10 {
		t.Fatalf("snapshot ack = %+v, want InstallSnapshotResp@10", ms)
	}
}

// TestStaleSnapshotIgnored verifies a snapshot at or below the commit index is
// not installed (no log rewrite), but is still acknowledged.
func TestStaleSnapshotIgnored(t *testing.T) {
	f := newTestRaft(t, 2, []uint64{1, 2, 3}, false)
	f.becomeFollower(4, 1)
	f.raftLog.append(Entry{Term: 4, Index: 1}, Entry{Term: 4, Index: 2})
	f.raftLog.commitTo(2)

	snap := Snapshot{Metadata: SnapshotMetadata{Index: 1, Term: 4}}
	f.Step(Message{Type: MsgInstallSnapshot, From: 1, To: 2, Term: 4, Snapshot: &snap})

	if f.raftLog.lastIndex() != 2 {
		t.Fatalf("stale snapshot altered log: lastIndex = %d, want 2", f.raftLog.lastIndex())
	}
	ms := f.drainMsgs()
	if len(ms) != 1 || ms[0].Type != MsgInstallSnapshotResp {
		t.Fatalf("stale snapshot ack = %+v", ms)
	}
}

// TestNodeAccessors covers the Node facade accessors.
func TestNodeAccessors(t *testing.T) {
	cfg := testConfig(1, []uint64{1}, false, 1)
	n, err := NewNode(cfg, newTestStorage())
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if n.Leader() != None {
		t.Fatalf("initial Leader = %d, want 0", n.Leader())
	}
	if n.Term() != 0 {
		t.Fatalf("initial Term = %d, want 0", n.Term())
	}
	for i := 0; i < 2*cfg.ElectionTicks; i++ {
		n.Tick()
	}
	if n.Leader() != 1 || n.State() != StateLeader {
		t.Fatalf("after election: Leader=%d State=%s", n.Leader(), n.State())
	}
}

// TestStateTypeString covers the StateType stringer.
func TestStateTypeString(t *testing.T) {
	cases := map[StateType]string{
		StateFollower:     "follower",
		StatePreCandidate: "pre-candidate",
		StateCandidate:    "candidate",
		StateLeader:       "leader",
		StateType(99):     "unknown",
	}
	for st, want := range cases {
		if got := st.String(); got != want {
			t.Fatalf("StateType(%d).String() = %q, want %q", st, got, want)
		}
	}
}
