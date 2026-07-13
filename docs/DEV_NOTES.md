# Dev notes (build host specifics)

Notes for developing on the primary build host (48-core Linux,
glibc 2.26, no root). None of this applies to CI, which runs stock
`ubuntu-latest` with Go module defaults.

## Go toolchain

Go is installed via mise, not on the default PATH:

```sh
export PATH="/home/dev/.local/share/mise/installs/go/1.24.13/bin:$PATH"
go version   # go1.24.13 linux/amd64
```

## Module proxy workaround (host-only)

`proxy.golang.org` is sinkholed on this host's network. Module fetches must
bypass the proxy and the checksum database:

```sh
export GOPROXY=direct
export GOSUMDB=off
```

Direct fetches are verified working. `go.sum` is committed as usual, so CI
(which uses the default proxy and sum DB) still fully verifies every
dependency; `GOSUMDB=off` only skips verification for the local fetch step
on this host.

Also set `GOTOOLCHAIN=local` when adding dependencies: without it, `go get`
may try to download a newer toolchain, which both fights the proxy
workaround and silently raises the repo's Go requirement.

## Dependency pins

- `google.golang.org/grpc v1.73.0` — deliberately not latest: grpc >= 1.82
  requires Go 1.25, and this repo builds with Go 1.24.
- `google.golang.org/protobuf v1.36.6` — minimum required by grpc v1.73.0.
- `github.com/anishathalye/porcupine v1.0.0` — the linearizability checker.

## protoc (generated code is committed)

Generated `.pb.go` files under `proto/` are committed, so neither CI nor a
fresh clone needs protoc. To regenerate after editing a `.proto` file, the
host toolchain lives in `.tools/` (gitignored):

- `.tools/protoc-29.3/` — protoc v29.3, official linux-x86_64 release
  binary (runs fine on glibc 2.26)
- `.tools/bin/protoc-gen-go` v1.36.5, `.tools/bin/protoc-gen-go-grpc` v1.5.1

```sh
cd "$(git rev-parse --show-toplevel)"
PATH="$PWD/.tools/bin:$PWD/.tools/protoc-29.3/bin:$PATH" \
protoc --proto_path=proto \
  --go_out=. --go_opt=module=github.com/iwang-1/parallax-kv \
  --go-grpc_out=. --go-grpc_opt=module=github.com/iwang-1/parallax-kv \
  proto/raft.proto proto/kv.proto
```

## CI layout (`.github/workflows/ci.yml`)

CI runs stock `ubuntu-latest` with the default module proxy and checksum DB —
none of the host-only proxy workaround above applies there. Five jobs:

- **lint** — `gofmt -l`, `go vet`, `go build`, and `golangci-lint` (pinned to
  v1.64.8 via `.golangci.yml`). The pin matters: an unpinned lint version can
  silently redefine "clean" between runs.
- **unit-race** — `go test ./... -race`, skipping only the two dedicated heavy
  matrices (`TestScenarioSoak`, `TestScenarioDeterminism`). This keeps the pure
  core, kv, storage, the server drive loop, the gRPC transport, the real-process
  e2e failover test, AND the fixed-seed nemesis regression corpus under the race
  detector.
- **determinism** — the gate: 10 seeds x every scenario, each run twice, trace
  hashes byte-compared (`-run TestScenarioDeterminism -det-lo=0 -det-hi=10`).
- **sim-soak** — the fixed regression corpus, then ~200 FRESH random seeds
  (`TestScenarioSoak -soak-lo/-soak-hi`); a failure prints its `REPLAY:` line and
  the seed is then committed to `regressionSeeds`.
- **bench-smoke** — builds the real binaries, stands up a 3-node localhost
  cluster, runs `parallax-bench --smoke`. Asserts NOTHING about throughput; host
  numbers live only in `benchmarks/RESULTS.md`.

The sim matrices run WITHOUT `-race`: the harness is single-goroutine, so the
race detector adds no coverage there and would roughly double wall time. Job
sizing was measured on the build host at ~1.3 s per scenario-run and budgeted
~2-3x for a slower 2-core runner.

## fsync probe (`scripts/fsync_probe.go`)

The disk disclosure in `benchmarks/RESULTS.md` is reproduced by a standalone
probe: it overwrites 4KiB and fsyncs in a loop, timing each fsync, and prints a
p50/p99 table.

```sh
go run ./scripts               # 200 samples (matches RESULTS.md), temp dir
go run ./scripts -n 1000       # more samples
go run ./scripts -dir /path    # probe a specific filesystem (e.g. the WAL disk)
```

Probe on a temp dir does not necessarily match the WAL-disk numbers quoted in
RESULTS.md — point `-dir` at the same filesystem the WAL uses to reproduce them.
`scripts/` is its own `main` package, excluded from the library build surface.

## Pre-commit checklist

```sh
test -z "$(gofmt -l .)" && go vet ./... && go build ./... && go test ./... -race
```
