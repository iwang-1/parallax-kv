# Bug ledger

Every consensus bug caught **organically** by the deterministic simulation
harness during development gets an entry here: the replay seed, the
scenario, the observed symptom, the root cause, and the fix commit.

**Rules of this ledger (the honesty contract):**

1. Organic bugs only. A bug qualifies only if the harness caught it while
   it was genuinely unnoticed — a mistake actually made, found by the
   machinery this repo exists to showcase. Bugs planted to make the
   harness look good do not qualify and will never appear here.
2. Every entry must be reproducible: `REPLAY:` line with the exact seed
   and test invocation, verified to fail at the pre-fix commit.
3. If the count is zero when the project ships, this file says so plainly
   and documents the harness-validation evidence (mutation-style checks
   that the invariants and checker actually detect injected faults)
   instead of inventing entries.

---

## Consensus bugs found: 0 (to date)

Through the S2c nemesis soak — six scenarios (partition-leader,
partition-random-half, crash-leader-loop, 30pct-loss, dup+delay, mixed-chaos)
across 200 fresh seeds plus a fixed regression corpus, ~1.3M+ client
operations, every run checked for four per-step safety invariants (election
safety, log matching, leader completeness, applied-prefix agreement) and
end-of-run Porcupine linearizability — the harness has found **zero genuine
consensus bugs** in the Raft core.

Per rule 3, this is stated plainly rather than padded. The core was built
table-driven-test-first (see `raft/*_test.go`: Figure-8 commit rule, ReadIndex
under leadership loss, PreVote disruption, conflict backtracking), which is why
the integration soak surfaces no new core defects. The value the harness
delivers is documented below as harness-validation evidence.

Should a fresh soak seed ever fail, its entry lands here with the `REPLAY:`
line and its seed is committed to `regressionSeeds` in
`sim/scenario_test.go` so the fix is guarded forever.

### Harness-validation evidence (the checker actually bites)

A safety checker that never fires proves nothing. Evidence that these do:

- **Negative controls in the checker's own unit tests.** `sim/invariants`
  is exercised against deliberately divergent state:
  `TestReplaySeedInError` feeds the applied-prefix checker two nodes that
  applied different data at the same index and asserts it reports the
  divergence (and that the error carries the `REPLAY:` seed).
- **The linearizability model rejects illegal histories.** `sim/lin`'s
  `TestModelRejectsStaleRead` proves the Porcupine KV model returns
  `Illegal` on a hand-built stale read, so an `Ok` verdict on a real run is
  meaningful, not vacuous.
- **The checker caught a real defect — in itself.** During S2c bring-up the
  election-safety check fired on every fault-injecting scenario (see the
  checker-precision note below). Investigation proved the fault was in the
  *checker*, not the core: it keyed election safety on a stale proxy for the
  leader's term. That is exactly the discipline this ledger enforces — a
  fired invariant is investigated to root cause and attributed honestly to
  the component actually at fault, never silenced.

---

## Checker-precision notes (harness fixes, NOT consensus bugs)

These are corrections to the *verification harness* — recorded for honesty
and to show the invariants were pressure-tested, but they are explicitly
**not** Raft bugs and are kept separate from the (empty) consensus-bug list
above.

### C1. Election-safety check keyed on a stale term proxy

- **Surfaced**: S2c nemesis bring-up; every fault-injecting scenario at
  seed 0x1 (partition-leader, partition-random-half, crash-leader-loop,
  mixed-chaos) reported `election safety violated: nodes X and Y both led
  term 1`.
- **Why it was NOT a consensus bug**: two nodes reporting `StateLeader` at
  *different* terms is normal Raft — during a partition the majority elects a
  new leader at term T+1 while the isolated old leader still believes it
  leads term T until it learns otherwise. Election safety forbids two leaders
  at the *same* term, which never actually happened.
- **Root cause (in the checker)**: `leaderTerm` used the term of the leader's
  last *applied* entry as a proxy for its current term. That proxy lags: a
  leader elected in term T campaigns and wins before it applies any term-T
  entry, so its applied-term can still read T-1 while it genuinely leads T.
  The stale old leader (applied-term T-1) and the fresh new leader
  (applied-term still T-1) were both attributed to term T-1 and flagged.
- **Fix**: expose the node's real current term via `raftNode.Term()` and key
  election safety on it. This *strengthens* the check (it now compares real
  raft terms, the actual definition of the property) rather than weakening
  it. Verified: all six scenarios pass the 200-seed soak after the fix.

<!-- Entry template for a genuine consensus bug:
## N. <one-line title>
- **Found**: <date>, scenario <name>, seed 0x<seed>
- **Replay**: `go test ./sim -run TestScenarioReplay -scenario=<name> -seed=0x...` at commit <sha>
- **Symptom**: <invariant violated / linearizability failure observed>
- **Root cause**: <the actual mistake>
- **Fix**: commit <sha>
-->
