# Design notes

Seed notes from stage S0 (scaffold). Each area grows a full section as it
is built; decisions recorded here at freeze time are the contract later
stages build against.

## The load-bearing decision: one pure core, two drivers

`/raft` is a deterministic state machine: `Tick()` advances a logical
clock, `Step(Message)` ingests every input (peer RPCs, proposals,
ReadIndex requests), `Ready()` emits a batch of outputs (state to persist,
messages to send, entries to apply), `Advance(PersistAck)` acknowledges
the batch. No goroutines, no `time.Now`, no I/O, no map-iteration-order
dependence; all randomness comes from the `*rand.Rand` injected via
`Config.Rand`.

Everything with effects lives in a driver:

- `/sim` — single-goroutine event loop, virtual clock, seeded
  fault-injecting network. Because the core is pure and the harness owns
  every source of nondeterminism behind one seed, a whole distributed
  failure replays bit-for-bit from a `uint64`.
- `/server` — real time, disk WAL, gRPC. Same core, byte for byte.

Determinism is what makes seed-replay honest; one stray `time.Now()`
breaks it. CI enforces it mechanically (trace-hash determinism gate,
stage S4).

The message-batch (`Ready`) API is deliberately chosen over a
channel-based API: channels would put goroutine scheduling — a source of
nondeterminism — inside the core.

## Interface freeze decisions (S0)

- **`LogStorage` lives in package `raft`**, not `/storage`: the consumer
  defines the interface (Go convention), and defining it in `/storage`
  would create an import cycle (its methods traffic in `raft.Entry` /
  `raft.HardState`). `/storage` re-exports it as a type alias.
- **`LogStorage` gained a 9th method, `HardState()`**, beyond the spec's
  8: a restarting node must be able to read back the term/vote/commit it
  persisted, or recovery is impossible.
- **`Ready.MustSync`**: term/vote changes and new entries require fsync
  before sending messages; a commit-index-only update does not. The flag
  lets the WAL skip fsyncs safely.
- **`PersistAck` is an empty struct, not a bare `Advance()`**, so
  partial-acknowledgement fields (e.g. async apply progress) can be added
  later without breaking every driver.
- **Heartbeats are a distinct message type** (`MsgHeartbeat`), not empty
  `MsgAppend`: ReadIndex threads its correlation context through
  heartbeat/response pairs, and separating them keeps that path free of
  log-matching bookkeeping.
- **CAS compares values, not versions** (`Expect []byte`, nil = "must be
  absent"): keeps the client API self-contained (no read-before-write to
  learn a version) and gives the linearizability model a clean sequential
  spec.
- **Session dedup is `(clientID, seq)` with one command in flight per
  client**; the session table is part of state-machine state and is
  serialized into snapshots, so restore-from-snapshot preserves
  exactly-once semantics.

## RPC decision (S0)

gRPC + protobuf, as planned — no fallback needed. protoc v29.3 (official
linux-x86_64 release binary) runs on the glibc-2.26 dev host; generated
code is committed so no one else ever needs protoc (see
docs/DEV_NOTES.md). Peer RPC is a single `Step(StepRequest)` carrying
batched `Message`s — the wire mirrors the core's own input API, so the
transport stays a dumb pipe. `StepResponse` is empty: Raft responses are
first-class messages traveling the same one-way path, never RPC return
values (RPC-level request/response pairing would smuggle in ordering
assumptions the algorithm does not make).

## Planned sections (filled in as stages land)

- Election safety and PreVote (S1)
- Log replication, conflict backtracking, and the Figure-8 commit rule (S1)
- ReadIndex, and why lease-based reads were rejected (S1/S2)
- WAL format, group commit, and torn-tail recovery (S1)
- The simulator's event loop and fault model (S1/S2)
- Linearizability checking: history recording and how WGL/Porcupine
  searches for a linearization (S2)
- Snapshots and InstallSnapshot (S3)
