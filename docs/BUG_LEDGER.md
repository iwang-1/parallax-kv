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

No entries yet — the harness and core land in stages S1–S2.

<!-- Entry template:
## N. <one-line title>
- **Found**: <date>, scenario <name>, seed 0x<seed>
- **Replay**: `go test ./sim -run TestSim... -seed=0x...` at commit <sha>
- **Symptom**: <invariant violated / linearizability failure observed>
- **Root cause**: <the actual mistake>
- **Fix**: commit <sha>
-->
