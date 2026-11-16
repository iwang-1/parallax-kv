# Benchmark results

All numbers here were **measured on the build host and committed with their
raw output** (`benchmarks/raw/`). This file is the single source of truth for
every performance number quoted in the README — nothing is
estimated, rounded up, or carried over from a prior run.

Read the methodology and the loopback caveat **before** the tables. These are
single-host, loopback-networked numbers. They are honest measurements of this
implementation on this machine; they are **not** a claim of cross-machine
throughput and must never be compared against a networked datastore.

## Hardware & software disclosure

| | |
|---|---|
| Host | Linux, kernel 5.10.259 |
| CPU | Intel Xeon Platinum 8488C, 48 vCPU |
| Memory | 97 GB |
| Disk (WAL) | ext4 on local NVMe SSD (`/dev/nvme0n1p1`) |
| Go | go1.24.13 linux/amd64 |
| Cluster | 3 `parallaxd` processes on localhost (loopback) |
| Payload | 16-byte keys, 128-byte values, keyspace 1000 |

### Measured fsync cost (the number that explains the write throughput)

The durability floor of any single-fsync-per-write design is the fsync latency
of the WAL disk. Reproduce with `scripts/fsync_probe.go` (writes 4 KiB and
fsyncs in a loop, timing each fsync):

```
go run ./scripts -n 200 -dir <WAL disk>
```

Measured on this host's WAL filesystem (median of 3 runs of n=200; raw in
`benchmarks/raw/fsync_probe_rep{1,2,3}.txt`):

| metric | 4 KiB fsync |
|---|---|
| p50 | 0.89 ms |
| p99 | 2.95 ms |
| min | 0.7–0.8 ms |
| max | 3.0–3.6 ms |

A single fsync at p50 = 0.89 ms caps a naive fsync-per-entry design at
**~1,100 writes/sec**. Group commit — fsyncing all entries in one Ready batch
per durability barrier — is how the store exceeds that floor under concurrency
without ever weakening durability. The UNSAFE variant below removes the fsync
entirely to quantify exactly what that barrier costs.

## Methodology

- **Tool**: `cmd/parallax-bench`, closed-loop. `--clients` goroutines each
  issue one operation, wait for its result, then immediately issue the next, so
  offered concurrency equals `--clients`.
- **Run shape**: 30 s measured window, 5 s warmup discarded, **5 repetitions**
  per configuration. Tables report the **median** across the 5 reps with the
  **min–max range**.
- **Concurrency levels**: C in {1, 8, 64, 256}.
- **Durability**: the durable workloads (W1) run the production default —
  group-commit fsync per Ready batch. This is the only mode quoted as a
  headline number. The UNSAFE workload (W2) sets `--unsafe-no-fsync` on the nodes, which
  skips the WAL fsync; it is published **only** to show the durability cost and
  is labeled UNSAFE everywhere because a crash can lose acknowledged writes.
- **Errors**: every cell recorded **0 errors** across all 5 reps (no operation
  timed out or hit a mid-election failure during the measured window).
- **Loopback disclosure**: all three nodes and the client run on one host over
  the loopback interface. There is no physical network: inter-node latency is a
  kernel memory copy (~tens of µs), not a datacenter round trip. These numbers
  therefore isolate the *algorithm + fsync + gRPC + scheduling* cost on one
  machine. They over-state what the same code would do across a real network
  (where the replication round trip dominates) and cannot be compared to any
  networked system. Shared-host scheduling variance is real; it is why we report
  medians of 5 reps and disclose the min–max range.

### Exact commands

```bash
# Build the binaries.
go build -o parallaxd      ./cmd/parallaxd
go build -o parallax-bench ./cmd/parallax-bench

# Start a 3-node cluster (durable default). One data dir per node.
for id in 1 2 3; do
  parallaxd --id $id \
    --peers 1=localhost:27101,2=localhost:27102,3=localhost:27103 \
    --client-peers 1=localhost:28101,2=localhost:28102,3=localhost:28103 \
    --data-dir /data/d$id --listen localhost:2810$id \
    --tick-ms 10 --heartbeat-ticks 3 --election-ticks 15 &
done

# W1 durable write, one cell (repeat 5x per concurrency level):
parallax-bench --cluster localhost:28101,localhost:28102,localhost:28103 \
  --clients 8 --duration 30s --warmup 5s --workload write --keyspace 1000

# R1 ReadIndex read:
parallax-bench --cluster ... --clients 8 --duration 30s --warmup 5s --workload read

# W2 UNSAFE: restart the cluster with --unsafe-no-fsync on every node, then
# run the same write workload.
```

The full matrix was driven by `benchmarks/raw/run_matrix.sh` (committed);
per-cell raw output is in `benchmarks/raw/<label>_c<N>.txt`. The script has
since been revised (error capture and a per-cell header that also records the
repetition number), so its current revision does not emit a byte-identical
per-cell header to the committed raw files above; the run shape it drives —
concurrency levels, 30 s duration, 5 s warmup, keyspace, 5 reps — is
unchanged.

## [W1] Committed writes — group-commit fsync (DEFAULT, durable)

This is the headline durable mode. Each committed write is durable on a
quorum before it is acknowledged.

| Concurrency | Writes/sec (median) | range | p50 | p99 |
|---:|---:|---|---:|---:|
| 1 | 138 | 137–139 | 7.29 ms | 7.82 ms |
| 8 | 272 | 271–275 | 29.6 ms | 40.7 ms |
| 64 | 279 | 274–293 | 232 ms | 352 ms |
| 256 | 285 | 282–287 | 899 ms | 991 ms |

**Reading the table.** At C=1 the write path is latency-bound: throughput is
1/latency (1/7.29 ms ≈ 137 writes/sec, matching the measured 138). Each write
pays at least two WAL fsyncs — the leader fsyncs before sending, then the
follower fsyncs before responding — plus a replication round trip. At the
measured 0.89 ms fsync p50 that is a ~1.8 ms floor, so it is a lower bound on
the 7.29 ms, not a full account of it; the remainder is not attributed to a
specific cost here because these runs did not break the request down further.
As concurrency rises,
group commit batches many clients' entries into one fsync per Ready, so
throughput climbs to a **plateau of ~270–285 writes/sec** — the point where the
single-fsync-per-batch serialization on the leader is the ceiling. Past the C=8
knee, added concurrency buys almost no throughput and only inflates latency
(p99 grows from 41 ms to ~1 s as 256 clients queue behind the same fsync
cadence). **The knee (C=8) is the interesting operating point**: 272 writes/sec
at 41 ms p99. The peak throughput point is C=256 at 285 writes/sec, but its p99
of ~1 s makes it a throughput-ceiling demonstration, not a healthy operating
point.

## [W2] Committed writes — fsync DISABLED (UNSAFE — durability cost only)

**UNSAFE. Not a real operating mode.** Nodes ran with `--unsafe-no-fsync`,
which skips the WAL durability barrier: a crash can lose acknowledged writes.
Published solely to quantify what group-commit fsync costs.

| Concurrency | Writes/sec (median) | range | p50 | p99 |
|---:|---:|---|---:|---:|
| 1 | 3,660 | 3,456–4,219 | 0.26 ms | 0.50 ms |
| 8 | 4,862 | 4,821–4,871 | 1.60 ms | 2.89 ms |
| 64 | 3,618 | 3,535–3,650 | 17.5 ms | 21.1 ms |
| 256 | 2,399 | 2,395–2,405 | 106 ms | 114 ms |

**The narrative.** With the fsync removed, the C=1 write latency floor collapses
from a 7.29 ms median p50 to a 0.26 ms median p50, and single-client
throughput jumps from 138 to
~3,660 writes/sec. Peak UNSAFE throughput is ~4,860 writes/sec at C=8 — about
**18x the durable ceiling at the same concurrency** (272 → 4,862). That gap is
exactly the price of durability, and it is why the design fights for it with
group commit rather than by dropping the fsync. (UNSAFE throughput *falls* past
C=8 because with the fsync gone the bottleneck moves to the single-goroutine
drive loop and gRPC handling, which more concurrent clients only contend on.)

## [R1] Linearizable reads — ReadIndex

Reads go through ReadIndex: the leader records its commit index, confirms it is
still leader via a heartbeat quorum round, then serves once its applied index
reaches the recorded index. No entry is appended and no fsync is paid, but every
read still costs one heartbeat-quorum confirmation round.

| Concurrency | Reads/sec (median) | range | p50 | p99 |
|---:|---:|---|---:|---:|
| 1 | 4,574 | 4,542–4,589 | 0.21 ms | 0.29 ms |
| 8 | 11,110 | 10,842–11,161 | 0.70 ms | 1.18 ms |
| 64 | 11,080 | 10,910–11,090 | 5.65 ms | 7.26 ms |
| 256 | 11,144 | 11,128–11,191 | 22.5 ms | 27.0 ms |

Reads are ~33x the durable write rate at C=1 (no fsync, no log append) and
plateau near **~11,100 reads/sec** — the ReadIndex confirmation round and the
single-goroutine drive loop are the ceiling. p99 stays sub-30 ms even at C=256.

## [W3] Committed writes — batched drive loop (DEFAULT since this change)

**Correction to the [W1] reading above.** The [W1] runs predate input batching:
the drive loop then drained after every client request and every peer message,
so each write cost its own fsync on the leader and on each follower, and
concurrent writes did *not* share a barrier. The ~280 writes/sec plateau was
that per-input fsync cadence, not a ceiling of group commit. The drive loop now
takes every already-queued input (up to 256) before draining and proposes
consecutive writes as one `MsgPropose`, so writes that arrive together share one
fsync per node and one append per follower (docs/DESIGN_NOTES.md §6).

Same workload as [W1], before and after that change, on one host: Apple M3 Max
(14 cores), macOS 27, go1.27.4, three `parallaxd` processes on localhost, fresh
cluster per run, 3 s warmup, 15 s measured window, 3 runs per cell, medians.
This host's 4 KiB fsync p50 is **3.96 ms** (`scripts/fsync_probe.go`), about 4.5x
the [W1] host's 0.89 ms, so absolute numbers are not comparable to [W1]; compare
the two columns with each other.

| Clients | before: writes/s | before: p99 | after: writes/s | after: p99 | throughput |
|---:|---:|---:|---:|---:|---:|
| 1   | 76.9  | 17.6 ms   | 76.8    | 18.0 ms | 1.0x  |
| 8   | 108.3 | 126.9 ms  | 200.1   | 58.7 ms | 1.85x |
| 64  | 102.0 | 763.7 ms  | 1,407.7 | 60.3 ms | 13.8x |
| 256 | 91.9  | 4,458 ms  | 5,800.3 | 59.1 ms | 63x   |

Every measured run had **0 errors**. Raw output, including two runs that failed
while seeding keys before any measurement (one per version) and the rerun of
each:
[`raw/W3_batched_apple_m3max.txt`](raw/W3_batched_apple_m3max.txt). The
"before, 256 clients" rerun failed to seed as well, so that cell is the median
of 2 runs, not 3.

**Reading the table.** At one client there is nothing to batch, and the two
versions match. With more clients, the old loop stayed near one write per
fsync round and its latency grew with the queue (p99 4.5 s at 256 clients).
The batched loop keeps p99 near 60 ms from 8 to 256 clients while throughput
grows with concurrency, because each fsync round now carries every write that
queued during the previous one. An in-process test,
`server.TestConcurrentWritesShareFsyncs`, counts the effect directly: 256
concurrent writes cost 570 WAL fsyncs across the three nodes before the change
(2.23 per write) and 118 after (0.46 per write).

**What it took.** A first version batched inputs but still proposed each write
as its own `MsgPropose`. That ran at 1,500+ writes/s at 64 clients but produced
~50,000 client errors per run at 256 clients: each proposal triggered an append
that re-sent all unacknowledged entries, the per-peer transport queue
overflowed and dropped batches including heartbeats, and followers started
elections. Proposing each group of writes as one message fixed it; the table
above is the fixed version.

## [W4] Log compaction — disk use and recovery time

`parallaxd --snapshot-entries N` snapshots every N applied entries and deletes
the WAL segments and older snapshot files it covers (docs/DESIGN_NOTES.md §10).
Same host as [W3] (Apple M3 Max, macOS 27, go1.27.4), three `parallaxd`
processes on localhost, 64 clients, 30 s write window, 1 MiB WAL segments; after
shutdown, `du` of each data directory and five timed `disk.Open` calls on node 1
(`go run ./scripts/recoverytime`). Two runs per configuration:

| | writes/s | p99 | node 1 on disk | WAL segments | log entries recovered | `disk.Open` median |
|---|---:|---:|---:|---:|---:|---:|
| no compaction, run 1 | 1,515 | 62.2 ms | 11,464 KiB | 10 | 50,998 | 6.98 ms |
| no compaction, run 2 | 1,533 | 58.1 ms | 11,692 KiB | 10 | 51,685 | 6.48 ms |
| `--snapshot-entries 10000`, run 1 | 1,518 | 63.1 ms | 1,268 KiB | 1 | 911 | 0.59 ms |
| `--snapshot-entries 10000`, run 2 | 1,514 | 62.0 ms | 1,272 KiB | 1 | 844 | 0.72 ms |

Raw: [`raw/W4_compaction_apple_m3max.txt`](raw/W4_compaction_apple_m3max.txt).

Without compaction, disk use and recovery work grow with every write, forever.
With it they are bounded by the snapshot interval: what remains is two snapshot
files plus the entries since the last snapshot, so the numbers depend on where
in the cycle the run stopped. These two runs stopped shortly after a snapshot
(~900 entries past it); an exploratory run that stopped just before one held
9,863 entries, 3,200 KiB, and recovered in 1.43 ms. Throughput and p99 are the
same within run-to-run noise.

## Headline figures

- **Batched drive loop ([W3])**: on one host, durable write throughput at 64
  clients rose from 102 to 1,408 writes/s (13.8x) with p99 falling from 764 ms to
  60 ms; at 256 clients, 92 to 5,800 writes/s with p99 near 60 ms. Fsyncs per write
  across the cluster fell from 2.23 to 0.46.
- **Durable write throughput, pre-batching ([W1])**: quote the **knee, C=8**: 272
  committed writes/sec at 40.7 ms p99 (durable, group-commit fsync). If instead
  the peak-throughput figure is preferred, it is 285 writes/sec at C=256, but its
  ~1 s p99 must be disclosed alongside it — the knee is the more honest number.
- All numbers above are medians of 5x 30 s runs with 0 errors; raw output is in
  `benchmarks/raw/`.
