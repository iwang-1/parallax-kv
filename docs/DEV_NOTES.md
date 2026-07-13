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

## Pre-commit checklist

```sh
test -z "$(gofmt -l .)" && go vet ./... && go build ./... && go test ./... -race
```
