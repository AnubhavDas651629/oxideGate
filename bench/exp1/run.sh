#!/usr/bin/env bash
# Experiment 1 — batching window sweep. See WRITEUP-1.md for what this shows.
#
# Usage:  bench/exp1/run.sh [outdir]           (default: bench/exp1/out)
#
# Runs windows 0/5/10/20ms at --concurrency 20 and 4, three interleaved
# repetitions each (24 runs, ~35 min). The mock backend stays up for the
# whole sweep; the gateway is restarted for every run so each /metrics
# scrape covers exactly one run (plus its warmup).
#
# Ports 8100/9100 are used so a dev gateway on :8000 can keep running.
# Numbers are only meaningful from --release builds.
set -euo pipefail
cd "$(dirname "$0")/../.."
OUT=${1:-bench/exp1/out}
mkdir -p "$OUT"

cargo build --release --quiet

MOCK_PORT=9100 python3 tools/mock_backend.py > "$OUT/mock.log" 2>&1 &
MOCK=$!
trap 'kill $MOCK 2>/dev/null || true' EXIT
until curl -sf localhost:9100/stats > /dev/null; do sleep 0.1; done

for rep in 1 2 3; do
  for c in 20 4; do
    for w in 0 5 10 20; do
      tag="c${c}_w${w}_r${rep}"
      OXIDEGATE_BIND=127.0.0.1:8100 OXIDEGATE_BACKEND=http://127.0.0.1:9100/v1 \
        OXIDEGATE_BATCH_WINDOW_MS=$w RUST_LOG=warn \
        ./target/release/oxidegate > "$OUT/gw_$tag.log" 2>&1 &
      GW=$!
      until curl -sf localhost:8100/health > /dev/null; do sleep 0.1; done

      ./target/release/loadgen --url http://127.0.0.1:8100 --stream \
        --concurrency "$c" --requests 500 --warmup 40 \
        --label "c=$c window=${w}ms rep$rep" > "$OUT/lg_$tag.txt"

      # The gateway's own view: how long requests sat in *our* queue, and
      # how many requests each dispatch actually gathered.
      curl -s localhost:8100/metrics \
        | grep -E "oxidegate_(queue_wait_seconds|batch_size)(_sum|_count)?[ {]" \
        > "$OUT/metrics_$tag.txt"

      kill "$GW"; wait "$GW" 2>/dev/null || true
      grep '^| c=' "$OUT/lg_$tag.txt"
    done
  done
done
