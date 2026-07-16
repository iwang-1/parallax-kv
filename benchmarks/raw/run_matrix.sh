#!/usr/bin/env bash
# Portable benchmark matrix runner for parallax-kv.
#
# The script builds all required binaries, starts an isolated three-process
# localhost cluster, and writes each run to a new directory. It never appends to
# the committed evidence files. Override RAW_DIR or PARALLAX_BENCH_WORK_ROOT when
# a specific destination is required.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd -- "$SCRIPT_DIR/../.." && pwd)"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
if [[ -n "${RAW_DIR:-}" ]]; then
  RAW="$RAW_DIR"
  if ! mkdir -- "$RAW"; then
    echo "ERROR: output path must not already exist: $RAW" >&2
    exit 1
  fi
else
  RAW="$(mktemp -d "$SCRIPT_DIR/run-$RUN_ID.XXXXXX")"
fi

WORKROOT_PARENT="${PARALLAX_BENCH_WORK_ROOT:-${TMPDIR:-/tmp}}"
mkdir -p -- "$WORKROOT_PARENT"
WORKROOT="$(mktemp -d "$WORKROOT_PARENT/parallax-bench.XXXXXX")"
BIN="$WORKROOT/bin"
DATAROOT="$WORKROOT/data"

PEER="1=localhost:27101,2=localhost:27102,3=localhost:27103"
CLIENTMAP="1=localhost:28101,2=localhost:28102,3=localhost:28103"
CLUSTER="localhost:28101,localhost:28102,localhost:28103"

PIDS=()
CURRENT_CONTEXT=setup

stop_cluster() {
  local pid running
  if ((${#PIDS[@]} == 0)); then
    return
  fi

  kill "${PIDS[@]}" 2>/dev/null || true
  for _ in {1..20}; do
    running=0
    for pid in "${PIDS[@]}"; do
      if kill -0 "$pid" 2>/dev/null; then
        running=1
      fi
    done
    if ((running == 0)); then
      break
    fi
    sleep 0.1
  done
  for pid in "${PIDS[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill -KILL "$pid" 2>/dev/null || true
    fi
    wait "$pid" 2>/dev/null || true
  done
  PIDS=()
}

cleanup() {
  local status=$?
  local failure_dir

  trap - EXIT INT TERM
  stop_cluster

  if ((status != 0)) && [[ -d "$DATAROOT" ]]; then
    failure_dir="$RAW/failure-$CURRENT_CONTEXT"
    if mkdir -- "$failure_dir" &&
      cp -a -- "$DATAROOT/." "$failure_dir/"; then
      {
        printf 'status=%q\n' "$status"
        printf 'context=%q\n' "$CURRENT_CONTEXT"
        printf 'captured=%q\n' "$(date -u +%FT%TZ)"
      } >"$failure_dir/FAILURE.txt"
      echo "failure artifacts: $failure_dir" >&2
    else
      echo "ERROR: could not preserve failure artifacts under $failure_dir" >&2
    fi
  fi

  rm -rf -- "$WORKROOT" || true
  exit "$status"
}

ports_available() {
  local port
  for port in 27101 27102 27103 28101 28102 28103; do
    if (exec 3<>"/dev/tcp/localhost/$port") >/dev/null 2>&1; then
      echo "ERROR: localhost port $port is already in use" >&2
      return 1
    fi
  done
}

cluster_processes_running() {
  local pid
  for pid in "${PIDS[@]}"; do
    if ! kill -0 "$pid" 2>/dev/null; then
      return 1
    fi
  done
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

ports_available
mkdir -p "$BIN"

go -C "$REPO" build -o "$BIN/parallaxd" ./cmd/parallaxd
go -C "$REPO" build -o "$BIN/parallaxctl" ./cmd/parallaxctl
go -C "$REPO" build -o "$BIN/parallax-bench" ./cmd/parallax-bench

STARTED="$(date -u +%FT%TZ)"
COMMIT="$(git -C "$REPO" rev-parse HEAD)"
GO_VERSION="$(go version)"
{
  printf 'started=%q\n' "$STARTED"
  printf 'commit=%q\n' "$COMMIT"
  printf 'go_version=%q\n' "$GO_VERSION"
  printf 'raw_dir=%q\n' "$RAW"
} >"$RAW/METADATA.txt"

start_cluster() {
  local mode="$1"
  local cmd

  CURRENT_CONTEXT="startup_${mode}"
  ports_available
  rm -rf -- "$DATAROOT"
  mkdir -p "$DATAROOT"
  PIDS=()
  for id in 1 2 3; do
    mkdir -p "$DATAROOT/d$id"
    cmd=("$BIN/parallaxd" --id "$id" --peers "$PEER" --client-peers "$CLIENTMAP" \
      --data-dir "$DATAROOT/d$id" --listen "localhost:$((28100 + id))" \
      --tick-ms 10 --heartbeat-ticks 3 --election-ticks 15)
    case "$mode" in
      durable) ;;
      unsafe) cmd+=(--unsafe-no-fsync) ;;
      *)
        echo "ERROR: unknown cluster mode: $mode" >&2
        return 2
        ;;
    esac
    "${cmd[@]}" >"$DATAROOT/d$id.log" 2>&1 &
    PIDS+=("$!")
  done

  sleep 0.1
  for _ in {1..60}; do
    if ! cluster_processes_running; then
      echo "ERROR: a cluster process exited for mode=$mode; logs are under $DATAROOT" >&2
      return 1
    fi
    if "$BIN/parallaxctl" --cluster "$CLUSTER" --timeout 2s \
      put __leaderprobe__ x >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.5
  done
  echo "ERROR: no leader for mode=$mode; logs are under $DATAROOT" >&2
  return 1
}

run_cell() {
  local label="$1" workload="$2" clients="$3" repetition="$4"
  local out="$RAW/${label}_c${clients}.txt"

  CURRENT_CONTEXT="${label}_c${clients}_rep${repetition}"
  printf '### label=%q workload=%q clients=%q repetition=%q started=%q\n' \
    "$label" "$workload" "$clients" "$repetition" "$(date -u +%FT%TZ)" \
    >>"$out"
  if "$BIN/parallax-bench" --cluster "$CLUSTER" --clients "$clients" \
    --duration 30s --warmup 5s --workload "$workload" --keyspace 1000 \
    >>"$out" 2>&1; then
    printf '\n' >>"$out"
  else
    local status=$?
    printf 'ERROR: benchmark exited with status %d\n' "$status" >>"$out"
    return "$status"
  fi
}

CONCURRENCY=(1 8 64 256)
REPS=5

echo "=== MATRIX START $(date -u +%FT%TZ) ==="

start_cluster durable
for clients in "${CONCURRENCY[@]}"; do
  for ((rep = 1; rep <= REPS; rep++)); do
    run_cell W1_durable write "$clients" "$rep"
  done
done
for clients in "${CONCURRENCY[@]}"; do
  for ((rep = 1; rep <= REPS; rep++)); do
    run_cell R1_read read "$clients" "$rep"
  done
done
stop_cluster

start_cluster unsafe
for clients in "${CONCURRENCY[@]}"; do
  for ((rep = 1; rep <= REPS; rep++)); do
    run_cell W2_unsafe write "$clients" "$rep"
  done
done
stop_cluster

echo "=== MATRIX DONE $(date -u +%FT%TZ) ==="
echo "raw output: $RAW"
