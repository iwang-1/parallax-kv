package raft

// MessageType identifies the kind of a Message. Every input to the state
// machine — peer RPCs, client proposals, and read requests — is a Message
// stepped through Node.Step; every output is a Message emitted in Ready.
type MessageType uint8

const (
	// MsgUnknown is the zero value and is never a valid message type.
	MsgUnknown MessageType = iota

	// Local input messages (never sent over the wire).

	// MsgPropose asks the leader to append Entries[i].Data as new log
	// entries. Followers reject proposals with ErrNotLeader.
	MsgPropose
	// MsgReadIndex requests a linearizable read point. Context carries an
	// opaque correlation token echoed back in the released ReadState.
	MsgReadIndex

	// Election messages.

	// MsgPreVote probes whether an election could be won without
	// disturbing a live leader; it does not increment terms.
	MsgPreVote
	// MsgPreVoteResp answers a MsgPreVote. Reject reports the verdict.
	MsgPreVoteResp
	// MsgVote is a RequestVote RPC. LogIndex/LogTerm carry the
	// candidate's last log entry for the up-to-date check.
	MsgVote
	// MsgVoteResp answers a MsgVote. Reject reports the verdict.
	MsgVoteResp

	// Replication messages.

	// MsgAppend is an AppendEntries RPC. LogIndex/LogTerm identify the
	// entry immediately preceding Entries; Commit is the leader's commit
	// index.
	MsgAppend
	// MsgAppendResp answers a MsgAppend. On success LogIndex is the last
	// index now matched; on rejection RejectHint/HintTerm carry the
	// follower's conflict hint for fast backtracking.
	MsgAppendResp
	// MsgHeartbeat is an empty leader heartbeat. Context carries pending
	// ReadIndex tokens awaiting quorum confirmation.
	MsgHeartbeat
	// MsgHeartbeatResp acknowledges a heartbeat, echoing Context.
	MsgHeartbeatResp
	// MsgInstallSnapshot ships a Snapshot to a follower whose needed
	// entries have been compacted away.
	MsgInstallSnapshot
	// MsgInstallSnapshotResp acknowledges snapshot installation; LogIndex
	// is the follower's new last index.
	MsgInstallSnapshotResp
)

// Message is the single envelope for all state-machine inputs and outputs.
// Unused fields are left at their zero values; which fields are meaningful
// depends on Type (see the MessageType constants).
type Message struct {
	Type MessageType
	// From and To are node IDs. Node IDs are positive; 0 means "none".
	// Local messages (MsgPropose, MsgReadIndex) leave both at 0.
	From uint64
	To   uint64
	// Term is the sender's current term. For MsgPreVote it is the term
	// the candidate would campaign at, not its current term.
	Term uint64
	// LogIndex and LogTerm identify a log position: the candidate's last
	// entry (votes), the entry preceding Entries (appends), or the last
	// matched index (append responses).
	LogIndex uint64
	LogTerm  uint64
	// Entries are the log entries carried by MsgPropose and MsgAppend.
	Entries []Entry
	// Commit is the sender's commit index (MsgAppend, MsgHeartbeat).
	Commit uint64
	// Reject reports a negative verdict on response messages.
	Reject bool
	// RejectHint and HintTerm implement conflict backtracking: the
	// follower's guess at the highest possibly-matching index and the
	// term of the entry at that index.
	RejectHint uint64
	HintTerm   uint64
	// Snapshot is set on MsgInstallSnapshot.
	Snapshot *Snapshot
	// Context is an opaque correlation token for ReadIndex requests,
	// threaded through heartbeats and their responses.
	Context []byte
}

// EntryType distinguishes application commands from internal entries.
type EntryType uint8

const (
	// EntryNormal carries an application command in Data.
	EntryNormal EntryType = iota
	// EntryNoop is the empty entry a new leader appends at the start of
	// its term to establish the commit rule for prior-term entries.
	EntryNoop
)

// Entry is a single Raft log entry.
type Entry struct {
	Term  uint64
	Index uint64
	Type  EntryType
	Data  []byte
}

// HardState is the state that must be persisted (fsynced) before any
// message reflecting it is sent to a peer: the current term, the vote cast
// in that term (0 = none), and the commit index.
type HardState struct {
	Term   uint64
	Vote   uint64
	Commit uint64
}

// SnapshotMetadata locates a snapshot in the log: it covers all entries
// through Index, whose entry had term Term.
type SnapshotMetadata struct {
	Index uint64
	Term  uint64
}

// Snapshot is a point-in-time serialization of the application state
// machine, replacing the log prefix through Metadata.Index.
type Snapshot struct {
	Metadata SnapshotMetadata
	Data     []byte
}

// ReadState is a released linearizable read point: once the driver's
// applied index reaches Index it may serve the read identified by
// RequestCtx from local state.
type ReadState struct {
	Index      uint64
	RequestCtx []byte
}

// Ready is a batch of outputs the driver must process, in order:
//
//  1. Persist HardState, Entries, and Snapshot durably (fsync) — BEFORE
//     step 2. Sending a message that promises state which is then lost in
//     a crash violates safety.
//  2. Send Messages to peers.
//  3. Apply Snapshot (if any) then CommittedEntries to the application
//     state machine; serve ReadStates whose Index has been applied.
//  4. Call Node.Advance to acknowledge the batch.
type Ready struct {
	// HardState is non-nil when term, vote, or commit changed.
	HardState *HardState
	// Entries are new log entries to persist.
	Entries []Entry
	// Snapshot is a received snapshot to persist and apply.
	Snapshot *Snapshot
	// CommittedEntries are entries known committed but not yet applied.
	CommittedEntries []Entry
	// Messages are outbound messages. They must not be sent until
	// HardState and Entries are durable.
	Messages []Message
	// ReadStates are ReadIndex requests whose leadership has been
	// confirmed by a heartbeat quorum.
	ReadStates []ReadState
	// MustSync reports whether the persistence in this batch requires an
	// fsync before Messages may be sent (term/vote changes and new
	// entries do; a commit-index-only change does not).
	MustSync bool
}

// PersistAck acknowledges that the previous Ready batch was fully
// processed: state persisted, messages handed to the transport, and
// committed entries applied. It is a struct (not a bare method) so that
// partial-acknowledgement fields can be added without breaking callers.
type PersistAck struct{}

// StateType is the role a node currently plays.
type StateType uint8

const (
	StateFollower StateType = iota
	StatePreCandidate
	StateCandidate
	StateLeader
)

// String returns a human-readable role name.
func (s StateType) String() string {
	switch s {
	case StateFollower:
		return "follower"
	case StatePreCandidate:
		return "pre-candidate"
	case StateCandidate:
		return "candidate"
	case StateLeader:
		return "leader"
	default:
		return "unknown"
	}
}
