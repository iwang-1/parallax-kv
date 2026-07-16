# Consistency report

Report date: 2026-07-14 UTC

## Result

The deterministic nemesis soak completed all seeds in `[0, 200)` across all
seven scenarios:

| Measure | Result |
|---|---:|
| Seeds | 200 |
| Scenarios per seed | 7 |
| Scenario runs | 1,400 |
| Completed client operations | 2,914,245 |
| Per-step invariant violations | 0 |
| Porcupine `Illegal` verdicts | 0 |
| Consensus bugs discovered | 0 |

All eight 25-seed `go test` commands returned `PASS`. This is empirical
evidence for the implemented fault model, not a formal proof of Raft or of all
possible executions.

## Reproduction

Run from the repository root:

```sh
for lo in 0 25 50 75 100 125 150 175; do
  hi=$((lo + 25))
  go test ./sim -run 'TestScenarioSoak$' -timeout 10m \
    -soak-lo="$lo" -soak-hi="$hi" -count=1 -v
done
```

Each command covers 25 seeds x 7 scenarios = 175 scenario runs. A single
failure can be replayed without rerunning the matrix:

```sh
go test ./sim -run 'TestScenarioReplay$' \
  -scenario=mixed-chaos -seed=0x1234 -count=1 -v
```

The committed raw summary is
[`benchmarks/raw/soak_200.txt`](benchmarks/raw/soak_200.txt).

## Batch results

| Seed range | Scenario runs | Client operations | Test time | Result |
|---|---:|---:|---:|---|
| `[0, 25)` | 175 | 363,407 | 239.12 s | PASS |
| `[25, 50)` | 175 | 363,408 | 233.87 s | PASS |
| `[50, 75)` | 175 | 361,746 | 234.64 s | PASS |
| `[75, 100)` | 175 | 364,123 | 236.52 s | PASS |
| `[100, 125)` | 175 | 366,087 | 252.24 s | PASS |
| `[125, 150)` | 175 | 366,660 | 252.94 s | PASS |
| `[150, 175)` | 175 | 362,657 | 249.68 s | PASS |
| `[175, 200)` | 175 | 366,157 | 252.38 s | PASS |
| **Total** | **1,400** | **2,914,245** | **1,951.39 s** | **PASS** |

The final two batches completed on July 14. The first six timings and counts
come from `benchmarks/raw/soak.log`; the final two counts and timings complete
that interrupted parent run.

## Scope

Every scenario builds the same base workload:

- three Raft nodes;
- six concurrent simulated clients;
- four contended keys;
- Get, Put, Delete, and CAS operations;
- 8 seconds of virtual time per scenario run; and
- seeded delay, loss, duplication, partitions, crashes, and restarts.

The seven named fault programs are:

| Scenario | Stress applied |
|---|---|
| `partition-leader` | Repeatedly isolate the current leader, then heal |
| `partition-random-half` | Repeatedly isolate a random minority, then heal |
| `crash-leader-loop` | Crash, restart, and replace leaders under load |
| `30pct-loss` | 30% loss plus low-rate duplication |
| `dup+delay` | 30% duplication with wide delay and reordering |
| `mixed-chaos` | Loss, duplication, delay, leader partitions, and crashes |
| `snapshot-under-partition` | Compact past an isolated follower, then force `InstallSnapshot` catch-up |

Structural faults preserve a quorum over time. The purpose is to exercise
leadership changes, stale replicas, recovery, and reordering while still
allowing client operations to complete.

## What was checked

After every simulated event, the harness checks four Raft safety invariants:

1. **Election safety:** no two observed nodes lead the same actual Raft term.
2. **Log matching:** entries sharing `(index, term)` have identical command
   data.
3. **Leader completeness:** the highest-term leader does not conflict with an
   entry already observed committed.
4. **Applied-prefix agreement:** no two replicas apply different commands at
   the same index.

At the end of every scenario run, the harness passes the client history to
Porcupine. The model checks the observed Get/Put/Delete/CAS results against a
single-copy sequential KV specification. Histories are partitioned by key,
and requests that never returned are modeled as possibly having taken effect.

## Non-vacuity

The soak recorded **2,914,245 completed client operations**, averaging about
2,081.6 operations per scenario run. Every 25-seed batch completed between
361,746 and 366,660 operations, so the campaign did not pass merely because
faults prevented the workload from producing observations.

There are two narrower guards:

- `TestScenarioSmoke` fails if any named scenario completes zero operations.
- `TestScenarioRegression` fails if any fixed scenario/seed run completes
  zero operations.

The fresh-seed soak records aggregate operations per batch, not the minimum
for each individual scenario run. This report therefore makes the supported
claim that the overall campaign and every batch were non-vacuous; it does not
claim a retained per-run minimum that the harness did not log.

## Porcupine verdict semantics

Porcupine's search is bounded to 20 seconds per history:

- `Ok` means a valid linearization was found.
- `Illegal` means no valid linearization exists and fails the test.
- `Unknown` means the bounded search timed out and is inconclusive.

The current harness treats `Unknown` as non-failing and does not emit an
`Ok`/`Unknown` counter in the batch summary. The exact supported result is
therefore **zero Porcupine `Illegal` verdicts**, not "1,400 histories formally
proved linearizable." This distinction does not weaken the zero-violation
count; it prevents an inconclusive search from being mislabeled as proof.

## Checker-precision issue

The campaign found **zero consensus bugs**. During earlier nemesis bring-up,
the election-safety checker did fire, but root-cause analysis showed a bug in
the checker:

- It used the term of a leader's last applied entry as a proxy for the
  leader's current Raft term.
- Immediately after election, the applied-entry term can lag the current
  term.
- An isolated old leader and a newly elected leader in different terms were
  therefore mislabeled as leaders in the same stale term.
- The fix exposed `raft.Node.Term()` and keyed election safety on the actual
  current term.

No Raft behavior was changed to resolve this incident. It is recorded as a
checker-precision correction, not as a consensus defect. The full attribution
is in [docs/BUG_LEDGER.md](docs/BUG_LEDGER.md).

## Limits of the result

- The explored state space is finite: seven authored scenarios and 200 seeds.
- The simulator is deterministic and single-goroutine; production scheduling
  and kernel/network behavior are covered separately by race, integration,
  and real-process failover tests.
- The Porcupine checker can return `Unknown`, as described above.
- Production snapshot compaction scheduling is not implemented.
- Snapshot transfer is a single message; chunked streaming is not
  implemented.
- The system is one static Raft group. The report makes no sharding or dynamic
  membership claim.
