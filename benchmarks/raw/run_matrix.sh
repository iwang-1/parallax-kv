#!/usr/bin/env bash
# Benchmark matrix orchestrator for parallax-kv (S5).
# Stands up a real 3-process localhost cluster, runs parallax-bench closed-loop,
# and appends raw output to benchmarks/raw/. Honors the spec methodology:
# 30s measured window, 5s warmup discarded, 5 repetitions per config.
set -u

BIN=/tmp/parallax-bench
REPO=/home/dev/github-profile-work/parallax-kv
RAW="$REPO/benchmarks/raw"
DATAROOT=/home/dev/parallax-bench-data
mkdir -p "$RAW"

PEER="1=localhost:27101,2=localhost:27102,3=localhost:27103"
CLIENTMAP="1=localhost:28101,2=localhost:28102,3=localhost:28103"
CLUSTER="localhost:28101,localhost:28102,localhost:28103"

PIDS=()

start_cluster() {
  local mode="$1"   # "durable" or "unsafe"
  local extra=""
  if [ "$mode" = "unsafe" ]; then extra="--unsafe-no-fsync"; fi
  rm -rf "$DATAROOT"
  PIDS=()
  for id in 1 2 3; do
    mkdir -p "$DATAROOT/d$id"
    "$BIN/parallaxd" --id "$id" --peers "$PEER" --client-peers "$CLIENTMAP" \
      --data-dir "$DATAROOT/d$id" --listen "localhost:281$(printf '%02d' $id)" \
      --tick-ms 10 --heartbeat-ticks 3 --election-ticks 15 $extra \
      > "$DATAROOT/d$id.log" 2>&1 &
    PIDS+=($!)
  done
  # Wait for a leader by retrying a put until it succeeds.
  for _ in $(seq 1 60); do
    if "$BIN/parallaxctl" --cluster "$CLUSTER" --timeout 2s put __leaderprobe__ x >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.5
  done
  echo "ERROR: no leader for mode=$mode" >&2
  return 1
}

stop_cluster() {
  for p in "${PIDS[@]}"; do kill -9 "$p" 2>/dev/null; wait "$p" 2>/dev/null; done
  PIDS=()
}

run_cell() {
  local label="$1" workload="$2" clients="$3"
  local out="$RAW/${label}_c${clients}.txt"
  echo "### $label workload=$workload clients=$clients  $(date -u +%FT%TZ)" >> "$out"
  "$BIN/parallax-bench" --cluster "$CLUSTER" --clients "$clients" \
    --duration 30s --warmup 5s --workload "$workload" --keyspace 1000 >> "$out" 2>&1
  echo "" >> "$out"
}

CONC="1 8 64 256"
REPS=5

echo "=== MATRIX START $(date -u +%FT%TZ) ==="

# ---- DURABLE cluster: W1 (writes) + R1 (reads) ----
start_cluster durable || exit 1
for c in $CONC; do
  for r in $(seq 1 $REPS); do run_cell "W1_durable" write "$c"; done
done
for c in $CONC; do
  for r in $(seq 1 $REPS); do run_cell "R1_read" read "$c"; done
done
stop_cluster

# ---- UNSAFE cluster: W2 (writes, no fsync) ----
start_cluster unsafe || exit 1
for c in $CONC; do
  for r in $(seq 1 $REPS); do run_cell "W2_unsafe" write "$c"; done
done
stop_cluster

echo "=== MATRIX DONE $(date -u +%FT%TZ) ==="
