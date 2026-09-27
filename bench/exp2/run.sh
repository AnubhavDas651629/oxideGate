#!/usr/bin/env bash
# Experiment 2 — in-flight concurrency limit. See WRITEUP-1.md.
#
# Usage:  bench/exp2/run.sh [outdir]           (default: bench/exp2/out)
#
# Part A: closed loop, c=20, limit 0(unlimited)/1/2/4/8/16/32, 3 interleaved
#         reps, 300 streamed requests each. Where does the wait live?
# Part B: open loop at 10 req/s (~1.7x the mock's capacity), 300 requests:
#         unlimited vs limit 4 with queue depth 100 (default) vs 12 (sized).
#         Does admission control shed load, and what does it buy?
#
# Mock (capacity 4) on :9100, gateway on :8100. ~40 min total.
set -euo pipefail
cd "$(dirname "$0")/../.."
OUT=${1:-bench/exp2/out}
mkdir -p "$OUT"

cargo build --release --quiet

MOCK_PORT=9100 python3 tools/mock_backend.py > "$OUT/mock.log" 2>&1 &
MOCK=$!
trap 'kill $MOCK 2>/dev/null || true' EXIT
until curl -sf localhost:9100/stats > /dev/null; do sleep 0.1; done

# run TAG LIMIT QUEUE_DEPTH -- loadgen args...
run() {
  local tag=$1 limit=$2 depth=$3; shift 4
  OXIDEGATE_BIND=127.0.0.1:8100 OXIDEGATE_BACKEND=http://127.0.0.1:9100/v1 \
    OXIDEGATE_MAX_INFLIGHT=$limit OXIDEGATE_QUEUE_DEPTH=$depth RUST_LOG=warn \
    ./target/release/oxidegate > "$OUT/gw_$tag.log" 2>&1 &
  local gw=$!
  until curl -sf localhost:8100/health > /dev/null; do sleep 0.1; done
  ./target/release/loadgen --url http://127.0.0.1:8100 --stream "$@" > "$OUT/lg_$tag.txt" || true
  curl -s localhost:8100/metrics \
    | grep -E "oxidegate_(queue_wait_seconds|requests_total)(_sum|_count)?[ {]" \
    > "$OUT/metrics_$tag.txt"
  kill "$gw"; wait "$gw" 2>/dev/null || true
  grep '^| ' "$OUT/lg_$tag.txt" | tail -1
}

for rep in 1 2 3; do
  for limit in 0 1 2 4 8 16 32; do
    run "A_l${limit}_r${rep}" "$limit" 100 -- \
      --concurrency 20 --requests 300 --warmup 40 --label "A limit=$limit rep$rep"
  done
done

for rep in 1 2 3; do
  for cfg in "0 100" "4 100" "4 12"; do
    set -- $cfg
    run "B_l$1_q$2_r${rep}" "$1" "$2" -- \
      --rps 10 --concurrency 400 --requests 300 --warmup 20 \
      --label "B limit=$1 queue=$2 rep$rep"
  done
done
