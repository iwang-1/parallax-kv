# Design notes

This document explains the implemented design and the tradeoffs behind
each non-obvious choice. parallax-kv is a single replicated state
machine: one static Raft group, one leader, and one ordered log. It does not
implement sharding.

## 1. One pure core, two drivers

`raft.Node` is a deterministic state machine. Its public loop is:

```text
Tick() or Step(input)
        |
        v
Ready() -> persist -> send -> apply/read -> Advance()
```

The core contains no goroutines, wall-clock reads, network calls, or disk I/O.
Peers are iterated in sorted order, and election randomness comes from an
injected `*rand.Rand`. These constraints let two drivers reuse the exact same
consensus implementation:

- `sim/` owns virtual time, a seeded event queue, a fault-injecting network,
  in-memory storage, workload generation, and correctness checks.
- `server/` owns a real ticker, gRPC, the disk WAL, the KV state machine, and
  client waiters.

The production drive loop serializes every tick, peer message, and client
request through one goroutine because `raft.Node` is intentionally not
concurrency-safe. Keeping scheduling outside the core makes the simulator a
pure function of `(scenario, seed)` and keeps race-prone coordination out of
the consensus algorithm.

### Why `Ready` instead of channels?

A channel-based core would put goroutine scheduling inside the algorithm and
make exact replay dependent on the Go runtime. `Ready` exposes effects as
data. The driver chooses how to perform them while preserving one
safety-critical order:

1. Persist `HardState`, new entries, and any snapshot. Call `Sync` when
   `Ready.MustSync` is true.
2. Send outbound messages.
3. Apply the snapshot and committed entries; release `ReadState`s only after
   their index is applied.
4. Acknowledge the batch with `Advance(PersistAck{})`.

`MustSync` is true for term/vote changes, entries, and snapshots. A
commit-index-only update need not force an immediate fsync for Raft safety.
`PersistAck` is a struct so the API can later add partial acknowledgements
without changing every driver call site.

## 2. Election safety and PreVote

Election timeouts are randomized in
`[ElectionTicks, 2 * ElectionTicks)`. A candidate must have an
at-least-as-up-to-date log, and a node persists its term and vote before its
vote response is sent.

PreVote is enabled in the real runtime. A timeout first asks whether a quorum
would support an election in `term + 1`; the probe does not change any node's
term. This prevents a partitioned node from repeatedly increasing its term
and then disrupting a healthy leader when it rejoins.

The implementation also rejects a vote or pre-vote while a follower is still
hearing from a leader and its election timer has not expired. This is leader
stickiness for election disruption control, not the clock-based lease used
for reads.

Tradeoff: PreVote adds one message round before a real election, increasing
failover latency slightly. It buys much better stability when stale or
partitioned nodes rejoin.

Focused evidence:

- `TestPreVoteDoesNotBumpTerm`
- `TestPreVoteRejectsPartitionedNode`
- `TestLeaderReelectionAfterPartition`
- `TestElectionSafetyOneLeaderPerTerm`

## 3. Replication and the Figure-8 rule

The leader tracks `Match` and `Next` for each follower. An AppendEntries
rejection carries a conflict index and term so the leader can skip over an
incompatible suffix instead of backing up one entry per round trip.

The commit index is the highest index replicated on a quorum, but only if the
entry at that index belongs to the leader's current term. This is the Raft
Figure-8 restriction. Counting replicas alone is unsafe for an older-term
entry because that entry can still be replaced by a later leader. Each new
leader appends a current-term no-op; once that no-op reaches a quorum, earlier
entries become committed indirectly.

Focused evidence:

- `TestFigure8CommitRule`
- `TestCommitAdvancesByMedianMatch`
- `TestAppendConflictBacktracking`
- `TestTermBasedBacktracking`

## 4. Linearizable reads: ReadIndex, not read leases

A Get does not append to the log. The leader:

1. Records the current commit index with a unique request context.
2. Sends that context on heartbeats.
3. Waits for a quorum to acknowledge the heartbeat.
4. Returns a `ReadState` to the driver.
5. Serves the read only after the local applied index reaches the confirmed
   index.

The quorum round trip confirms that the node is still the leader before it
serves local state. If it loses leadership first, both the Raft read state and
the server's waiter are discarded.

The full Raft ReadIndex protocol also requires a newly elected leader to know
that it has committed an entry in its current term before serving a read. The
core appends that current-term no-op on election, but `stepReadIndex` does not
currently defer requests until the no-op commits. That explicit gate is a
remaining correctness-hardening item; the finite soak did not produce an
`Illegal` history for this schedule, but does not prove the schedule absent.

A lease read would be faster because it could avoid that quorum exchange, but
its safety depends on bounded clock drift and timing assumptions. ReadIndex
keeps the safety argument in terms of quorum intersection and message order,
which also maps cleanly onto a virtual-time simulator.

Tradeoff: ReadIndex adds a quorum round trip to an otherwise local read.
Pending requests are queued, and confirmation of a later context releases
earlier requests too, but there is no follower-read path.

Focused evidence:

- `TestReadIndexQuorumConfirm`
- `TestReadIndexNotReleasedAfterLeadershipLoss`
- `TestReadIndexOnFollowerRejected`

## 5. Client retries and exactly-once apply

The client tries its last-known leader first, follows redirect hints, and
round-robins after hard RPC errors. Every mutation carries
`(clientID, sequence)`, and retries reuse the same pair.

The replicated state machine stores, per client, the highest sequence and its
result:

- equal sequence: return the cached result without reapplying;
- lower sequence: reject it as stale;
- higher sequence: execute and advance the session.

This is exactly-once **apply** for clients that issue at most one mutation at a
time. It is not exactly-once network delivery. The session table is part of
the replicated state and snapshot payload, so deduplication survives
compaction and restore.

CAS compares value bytes rather than a separate version token. A nil expected
value means create-if-absent; a non-nil value means compare against that exact
value.

Tradeoffs:

- One in-flight mutation per client keeps sequence handling simple.
- Session state grows with the number of client IDs; there is no session
  expiry protocol.
- Exactly-once apply depends on retaining session state in snapshots.

Focused evidence:

- `TestDedupReplay`
- `TestDedupSurvivesSnapshotRestore`
- `TestExactlyOnceRetry`

## 6. WAL, group commit, and recovery

The disk store is a logical redo log. The active segment rotates on the next
`Sync` after reaching 16 MiB. Each record is:

```text
[payload length: uint32][CRC32C: uint32][payload]
```

Entry records install an entry at its index and implicitly truncate a
conflicting suffix during replay. Hard-state records replace the durable
term, vote, and commit index.

`AppendEntries` and `SetHardState` buffer framed records in memory. `Sync`
writes the full buffer and performs one fsync for the entire `Ready` batch.
This is the group-commit boundary: one barrier covers every record already in
that batch, and no proposal is acknowledged before it succeeds. The production
loop currently drains after each client request, so it does not deliberately
queue several leader proposals into one `Ready`.

Recovery scans segments in order. A partial header, short payload, invalid
length, or CRC mismatch marks a torn tail. The active segment is truncated to
the last valid frame, and later segments are discarded because records after
the first tear cannot belong to the durable prefix.

The ordering argument is more important than the file format:

```text
durable term/vote/log -> dependent network message -> client-visible apply
```

Sending first would allow a crash to erase a vote or log promise that another
node already relied on.

### What the benchmark says

The measured 4 KiB fsync p50 was approximately 0.89 ms. Measured throughput
rises with concurrency to the C=8 knee:

| Clients | Durable writes/s | p99 |
|---:|---:|---:|
| 1 | 138 | 7.8 ms |
| 8 | 272 | 41 ms |
| 64 | 279 | 352 ms |
| 256 | 285 | 991 ms |

C=8 is the useful knee. More clients do not materially improve throughput;
they wait longer behind the same serialized log and fsync path. The unsafe
no-fsync mode is a benchmark control only and can lose acknowledged writes.

Focused evidence:

- `TestNodeReadyPersistThenApply`
- `TestNodeMustSyncSemantics`
- `TestAppendSyncReopen`
- `TestTornTailPartialRecord`
- `TestTornTailCorruptCRC`
- `TestClusterFailoverNoAckedWriteLoss`

## 7. Deterministic simulation

The simulator is a single-goroutine discrete-event system. It owns:

- a virtual clock;
- a stable event queue;
- one seeded RNG;
- node ticks and restart;
- delay, loss, duplication, and partitions;
- client invocation/response timestamps; and
- a canonical trace hash.

There is no wall clock on the simulation path. Every randomized timing choice
draws from the run RNG, and stable peer iteration removes Go map-order
nondeterminism. CI double-runs every scenario over fixed seeds and compares
trace hashes and completed-operation counts.

The seven soak scenarios are:

1. `partition-leader`
2. `partition-random-half`
3. `crash-leader-loop`
4. `30pct-loss`
5. `dup+delay`
6. `mixed-chaos`
7. `snapshot-under-partition`

Structural faults preserve a quorum over time so the workload can keep making
progress. Each run uses a three-node cluster, six clients, four contended
keys, and an 8-second virtual-time workload containing Get, Put, Delete, and
CAS.

## 8. Two layers of correctness checking

### Per-step Raft invariants

After every event, the simulator checks:

- **Election safety:** at most one observed leader for a real Raft term.
- **Log matching:** equal `(index, term)` entries carry identical commands.
- **Leader completeness:** the highest-term leader does not conflict with an
  entry already observed committed.
- **Applied-prefix agreement:** replicas never apply different commands at
  the same index.

### End-of-run client history

The harness records call and return virtual times for completed operations.
Operations with no response remain pending and are modeled as either having
taken effect or not taken effect. Porcupine then searches for a legal
linearization of the sequential KV specification. Histories are partitioned
by key because linearizability is local to each independent object.

The Porcupine search is bounded to 20 seconds per history:

- `Ok`: a legal linearization was found.
- `Illegal`: no legal linearization exists; the run fails with a replay
  command.
- `Unknown`: the bounded search timed out; this is inconclusive and the
  current harness does not fail the run.

The soak report therefore says **zero `Illegal` verdicts**, not that every
history received a proof-like `Ok`. Current batch output does not retain an
`Ok`/`Unknown` count. See
[`CONSISTENCY_REPORT.md`](../CONSISTENCY_REPORT.md).

### Non-vacuity

The completed 200-seed soak executed 1,400 scenario runs and recorded
2,914,245 completed client operations. That makes the overall result
non-empty by a wide margin. Smoke and fixed-regression tests additionally
fail any individual scenario run that completes zero operations; the
fresh-seed soak currently stores aggregate batch counts rather than a
per-run minimum.

## 9. The checker-precision incident

The bug ledger records zero consensus bugs. During nemesis bring-up, the
election-safety checker reported two leaders in one term. Investigation showed
that the nodes led different terms, which is legal during a partition.

The checker had keyed election safety on the term of a leader's last applied
entry. That value can lag the node's current term immediately after an
election, so an old leader and a new leader were assigned the same stale term.
The fix exposed `raft.Node.Term()` and keyed the invariant on the actual Raft
term.

This was a checker-precision bug, not a Raft bug. The distinction matters:
the invariant was strengthened to measure the property it claimed to measure;
the consensus algorithm was not changed to silence the failure.

## 10. Snapshots and compaction

Snapshot policy stays outside the pure core:

- The KV state machine serializes keys, versions, sessions, and cached results
  in deterministic order.
- The simulator creates a snapshot after `SnapshotEntries` newly applied
  entries and applies it to storage, which truncates the covered log prefix.
- A restart restores the snapshot before applying the remaining committed
  suffix.
- If a follower's `Next` index falls below the leader's compacted prefix,
  replication switches from AppendEntries to `InstallSnapshot`.
- Disk snapshots use a framed CRC, temporary file, file fsync, rename, and
  parent-directory fsync. Recovery chooses the newest intact snapshot.

The `snapshot-under-partition` scenario isolates a follower long enough for
the majority to compact past it, then heals the partition and forces
leader-to-follower snapshot catch-up.

### Deliberate scope limits

- The production `/server` runtime does **not** schedule snapshot compaction.
  Compaction is currently driver-triggered only in the simulator. Disk
  snapshot persistence and restore exist and are unit-tested, but periodic
  production scheduling is not implemented.
- A snapshot is sent as one Raft message. Chunked snapshot streaming,
  resumable transfer, flow control, and per-chunk verification are not
  implemented.

## 11. RPC boundary

Peer transport is a single gRPC `Step` endpoint carrying batches of Raft
messages. Responses in the Raft protocol are ordinary messages sent through
the same path, not gRPC return values. This avoids smuggling RPC request/return
ordering into an asynchronous consensus protocol.

Client RPCs are separate Get/Put/Delete/CAS methods. Followers return leader
hints; clients chase those hints and preserve mutation sequence numbers across
retries.

Generated protobuf code is committed, so building and testing do not require
`protoc`.

## 12. Known production gaps

- One static Raft group only: no sharding and no online membership changes.
- ReadIndex does not yet explicitly wait for the leader's current-term no-op
  to commit before confirming a newly elected leader's read.
- The production driver has a `Ready`-level fsync boundary but no deliberate
  leader-side proposal coalescing.
- No production snapshot compaction scheduler and no chunked snapshot
  streaming.
- No TLS, authentication, authorization, quotas, or admission control.
- No follower reads, multi-key transactions, secondary indexes, or watch
  API.
- One serialized apply/consensus loop per node; this is simple and
  deterministic but is also the throughput ceiling for a group.
- The consistency campaign is empirical testing under a defined fault model,
  not formal verification.
- Benchmark results are single-host localhost measurements and should not be
  generalized to a networked deployment.
