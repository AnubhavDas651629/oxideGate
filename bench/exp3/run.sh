#!/usr/bin/env bash
# Experiment 3 — does a free-tier flood move paid-tier latency? See WRITEUP-1.md.
#
# Usage:  bench/exp3/run.sh [outdir]           (default: bench/exp3/out)
#
# Mock capacity 4, gateway limit 4, global queue 200. Quotas are set high
# (tenants.json) so only the queueing policy is under test; each tenant is
# capped at 60 requests in the system.
#   paid: open loop 2 req/s for 60s (~1/3 of capacity)
#   free: open loop 10 req/s, started 5s earlier so a backlog exists
# Conditions: paid alone | flood + FIFO | flood + DRR. 3 interleaved reps.
set -euo pipefail
cd "$(dirname "$0")/../.."
OUT=${1:-bench/exp3/out}
mkdir -p "$OUT"

cargo build --release --quiet

MOCK_PORT=9100 python3 tools/mock_backend.py > "$OUT/mock.log" 2>&1 &
MOCK=$!
trap 'kill $MOCK 2>/dev/null || true' EXIT
until curl -sf localhost:9100/stats > /dev/null; do sleep 0.1; done

LG=./target/release/loadgen
URL=http://127.0.0.1:8100

for rep in 1 2 3; do
  for cond in alone fifo drr; do
    fair=1; [ "$cond" = fifo ] && fair=0
    tag="${cond}_r${rep}"
    OXIDEGATE_BIND=127.0.0.1:8100 OXIDEGATE_BACKEND=http://127.0.0.1:9100/v1 \
      OXIDEGATE_MAX_INFLIGHT=4 OXIDEGATE_QUEUE_DEPTH=200 OXIDEGATE_FAIR_QUEUE=$fair \
      OXIDEGATE_TENANTS=bench/exp3/tenants.json RUST_LOG=warn \
      ./target/release/oxidegate > "$OUT/gw_$tag.log" 2>&1 &
    gw=$!
    until curl -sf localhost:8100/health > /dev/null; do sleep 0.1; done

    flood=""
    if [ "$cond" != alone ]; then
      $LG --url $URL --api-key key-free --stream --rps 10 --concurrency 400 \
        --requests 800 --warmup 0 --label "free $cond rep$rep" > "$OUT/lg_free_$tag.txt" &
      flood=$!
      sleep 5
    fi
    $LG --url $URL --api-key key-paid --stream --rps 2 --concurrency 100 \
      --requests 120 --warmup 0 --label "paid $cond rep$rep" > "$OUT/lg_paid_$tag.txt"
    [ -n "$flood" ] && { wait "$flood" || true; }

    curl -s localhost:8100/metrics | grep -E "^oxidegate_(requests_total|queue_wait_seconds_(sum|count))" \
      > "$OUT/metrics_$tag.txt"
    kill "$gw"; wait "$gw" 2>/dev/null || true
    grep -h '^| ' "$OUT/lg_paid_$tag.txt" | tail -1
    [ -n "$flood" ] && grep -h '^| ' "$OUT/lg_free_$tag.txt" | tail -1
  done
done
