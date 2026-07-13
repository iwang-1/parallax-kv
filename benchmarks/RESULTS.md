# Benchmark results

All numbers here were **measured on the build host and committed with their
raw output** (`benchmarks/raw/`). This file is the single source of truth for
every performance number quoted on the resume or in the README — nothing is
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
| p99 | 2.9 ms |
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
  group-commit fsync per Ready batch. This is the only mode quoted on the
  resume. The UNSAFE workload (W2) sets `--unsafe-no-fsync` on the nodes, which
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
per-cell raw output is in `benchmarks/raw/<label>_c<N>.txt`.

## [W1] Committed writes — group-commit fsync (DEFAULT, durable)

This is the mode quoted on the resume. Each committed write is durable on a
quorum before it is acknowledged.

| Concurrency | Writes/sec (median) | range | p50 | p99 |
|---:|---:|---|---:|---:|
| 1 | 138 | 137–139 | 7.29 ms | 7.82 ms |
| 8 | 272 | 271–275 | 29.6 ms | 40.7 ms |
| 64 | 279 | 274–293 | 232 ms | 352 ms |
| 256 | 285 | 282–287 | 899 ms | 991 ms |

**Reading the table.** At C=1 the write path is latency-bound: p50 = 7.3 ms is
roughly two fsyncs (leader WAL + follower WAL, in parallel) plus a replication
round trip, and throughput is 1/latency ≈ 138 writes/sec. As concurrency rises,
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
from 7.3 ms to 0.26 ms (26x) and single-client throughput jumps from 138 to
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

## Resume feed

- **Bullet 1** (durable write throughput): quote the **knee, C=8**: 272
  committed writes/sec at 40.7 ms p99 (durable, group-commit fsync). If instead
  the peak-throughput figure is preferred, it is 285 writes/sec at C=256, but its
  ~1 s p99 must be disclosed alongside it — the knee is the more honest number.
- All numbers above are medians of 5x 30 s runs with 0 errors; raw output is in
  `benchmarks/raw/`.
