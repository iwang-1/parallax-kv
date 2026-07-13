# parallax-kv

> **Under construction** — the interfaces are frozen and the skeleton
> compiles; the consensus core, simulator, and storage engines land next.

Distributed key-value store in Go — Raft from scratch, verified by
deterministic simulation: every failure replays from its seed, every
history checked for linearizability.

parallax-kv implements the Raft consensus algorithm as a pure deterministic
state machine (no goroutines, no clocks, no I/O inside the core) and runs
that one core under two drivers: a single-goroutine simulator with a
virtual clock, a seeded fault-injecting network (partitions,
crash-restarts, message loss/reorder/duplication), and
Porcupine-checked linearizability — and a production runtime with a
group-commit fsync WAL and gRPC transport. The product is not just the
store; it is the evidence that the store is correct under an explicit
fault model, with every claim linked to a reproducible artifact.

Status: stage S0 (scaffold). See [docs/DESIGN_NOTES.md](docs/DESIGN_NOTES.md)
for the architecture and [docs/DEV_NOTES.md](docs/DEV_NOTES.md) for
building on the dev host.

## Snapshots (scope)

Snapshotting is implemented in the deterministic simulator: log compaction
with truncation, state-machine + session-table serialization, restore on
restart, and the leader→follower `InstallSnapshot` catch-up flow (validated by
the `snapshot-under-partition` nemesis scenario). Limitations: the whole
snapshot ships in a single message (no chunked streaming), and the production
`/server` runtime does not yet schedule periodic compaction — the disk
snapshot files exist and are unit-tested, but compaction is currently
driver-triggered only in the simulator. See the Snapshots section of
[docs/DESIGN_NOTES.md](docs/DESIGN_NOTES.md).

License: [MIT](LICENSE)
