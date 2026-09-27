# I built a batching layer and measured it into the ground

*Draft 1 — Sep 27, 2026. Experiment 1 of oxideGate. Raw data and the script
that produced it are in [`bench/exp1/`](bench/exp1/).*

## The idea I started with

The original spec for this gateway had the scheduler hold incoming requests
for 5–20 ms and then "dispatch them as a batch". That's standard advice for
throughput systems: amortise fixed costs by grouping work.

Before building it I'd already argued myself out of it (ROADMAP D1).
`POST /v1/chat/completions` carries **one** conversation per HTTP request,
so a gateway "batch" is really just N separate requests fired at the same
instant. And the backends this gateway fronts (vLLM, and to a lesser extent
Ollama) already batch *continuously* inside the engine, admitting new
requests into the running GPU batch as slots free up. They don't want
pre-batched input and couldn't use it anyway.

So the prediction was: **a gateway-side window is pure added latency, and
buys nothing.**

A prediction you're confident in is still just a prediction, so I built the
window anyway (`OXIDEGATE_BATCH_WINDOW_MS`, default 0) and measured it.
The prediction turned out to be right, but only in one of the two regimes I
tested. In the other, the cost disappeared from the client-side numbers
completely. Why it disappeared is more interesting than the headline, and
it's what set up the next piece of work.

## Setup

**Gateway.** `--release` build. The scheduler task pulls the first request
off a bounded `mpsc` queue, then keeps collecting until `window` has passed
since that first request, then spawns one dispatch task per request. With
`window = 0` it dispatches each request as soon as it arrives. Every run
starts a fresh gateway process, so its `/metrics` cover exactly one run.

**Backend: a mock, on purpose** (`tools/mock_backend.py`, ROADMAP D3). A
real model's run-to-run variance is tens to hundreds of milliseconds, which
would swamp a 5 ms effect. The mock serves deterministic timing:

- 200 ms "prefill" before the first token
- then 20 tokens at 20 ms each (~23 ms in practice; Python's `sleep`
  overshoots)
- **a hard concurrency limit of 4**: a fifth concurrent request blocks until
  a slot frees

One request takes ~677 ms end to end, whether it's alone or one of four.
So the mock already hands batching's benefit out for free: four concurrent
requests finish in the same time as one. A window therefore has a real
opportunity to help, by lining requests up so they fill all four slots
together. The test could have come out the other way. That was one of the
conditions I set before running it (ROADMAP D1, "honesty conditions").

**Load.** `src/bin/loadgen.rs`, closed loop: N workers, each sending its
next streamed request as soon as the previous one finishes. Each run is
500 measured requests after 40 warmup requests. Warmup goes through the
same worker pool so every pooled connection is open before the clock
starts. TTFT is time to the first body byte, E2E is time to the last, and
both are recorded per request into an HDR histogram. Every configuration
ran **three times**, interleaved across configurations to spread any drift
on the machine evenly.

**Two concurrency levels**, and the reason for picking two is the core of
this writeup:

- **c = 20**: five times the backend's capacity. The backend is saturated
  and requests queue inside it.
- **c = 4**: exactly the backend's capacity. Every request gets a slot
  immediately and nothing queues downstream.

Machine: Apple M5 (10 cores), macOS. Gateway, mock and load generator on
the same host.

## Results

Means of 3 runs, with the min–max across runs in brackets. All 24 runs:
500/500 requests OK, 0 failures.

### c = 4 (backend exactly at capacity) — the window's cost is fully visible

| window | req/s | TTFT p50 (ms) | TTFT p99 | E2E p50 (ms) | E2E p99 | gateway queue wait, mean | requests per dispatch |
|---|---|---|---|---|---|---|---|
| **0 (control)** | **5.90** (5.88–5.92) | **204.8** (204.5–205.1) | 206.3 | **677.5** (675.3–679.9) | 696.3 | 0.02 ms | 1.00 |
| 5 ms | 5.81 (5.75–5.84) | 211.1 (210.9–211.3) | 215.1 | 684.5 (684.0–685.1) | 812.2* | 6.3 ms | 1.13 |
| 10 ms | 5.81 (5.79–5.83) | 216.0 (215.8–216.3) | 219.1 | 686.9 (683.0–691.7) | 707.9 | 10.7 ms | 1.34 |
| 20 ms | 5.67 (5.56–5.76) | 225.5 (224.9–226.2) | 232.1 | 699.5 (692.2–705.5) | 743.4 | 18.6 ms | 1.81 |

\* One run (rep 3) had a single stall that took E2E p99 to 1030 ms. The
other two reps were 705 and 701. I'm reporting it, not dropping it.

Each millisecond of window turns into roughly a millisecond of TTFT, at
every percentile: +6.3, +11.2 and +20.7 ms at p50. Throughput falls
because a closed-loop client can't send its next request until the
current one returns, so every added millisecond is a millisecond the
backend's slots sit empty. A one-line model predicts this:
`throughput(W) ≈ throughput(0) × 677 / (677 + W_eff)`. That gives 5.73 req/s
at 20 ms; I measured 5.67.

This is the regime where the prediction holds exactly: **pure cost, no
benefit.**

### c = 20 (backend saturated) — the cost vanishes from the client's numbers

| window | req/s | TTFT p50 (ms) | TTFT p99 | E2E p50 (ms) | E2E p99 | gateway queue wait, mean | requests per dispatch |
|---|---|---|---|---|---|---|---|
| **0 (control)** | **5.90** (5.87–5.94) | **2911.6** (2891.8–2928.6) | 2953.2 | **3384.0** (3358.7–3405.8) | 3431.8 | 0.02 ms | 1.00 |
| 5 ms | 5.90 (5.89–5.91) | 2914.3 (2908.2–2924.5) | 2959.4 | 3387.4 (3381.2–3399.7) | 3438.6 | 6.3 ms | 1.22 |
| 10 ms | 5.89 (5.87–5.92) | 2919.8 (2904.1–2928.6) | 2960.0 | 3394.9 (3375.1–3405.8) | 3438.6 | 11.0 ms | 1.34 |
| 20 ms | 5.93 (5.92–5.94) | 2904.1 (2904.1–2904.1) | 2950.5 | 3374.4 (3373.1–3375.1) | 3424.3 | 19.5 ms | 1.45 |

No difference at any window. The spread between windows is smaller than
the spread between repeats of the same window.

This isn't because the window stopped costing anything. The gateway's
own metric shows every request still sits in its queue for 6–20 ms. The
cost is being paid, just not where the client can see it.

## Why: Little's law, and a queue in the wrong place

At c = 20 the system always holds exactly 20 requests (closed loop). The
backend serves 4 at a time, so at any moment about 16 are waiting. With a
window, one or two of those 16 wait in the gateway's queue instead of the
mock's. The backend never runs short of work, because there are always a
dozen requests queued behind the window, so throughput doesn't move. And
by Little's law (`latency = requests in system / throughput`), if the
number in the system is fixed and throughput is fixed, latency is fixed
too. The window moves the wait to a different place. It doesn't add to it.

That's a real result, but I don't think the right conclusion is "at scale
the window is free". What the c = 20 table actually shows is:

1. **Clients wait ~2.9 s for a first token, and the gateway's queue-wait
   metric reports 0.02 ms.** With `window = 0` the gateway forwards
   everything immediately, and all of that waiting happens inside the
   backend. The gateway can't observe that queue, reorder it, or shed load
   from it. A request that has already been handed to the backend can't be
   de-prioritised when a paid-tier request arrives behind it. This is the
   second half of D1's argument, now with a number on it.
2. **Admission control never fired.** The gateway's queue (depth 100) was
   empty the whole time, so there was nothing to reject from. In a separate
   check, sending 60 requests open loop at 20 req/s (about 3× what the mock
   can serve) gave **zero** 429s; p50 latency just climbed to 3.5 s. That's
   D5: the 429 path is correct code with nothing to act on until the gateway
   stops forwarding everything it receives.

So the question isn't whether to hold requests at the gateway. It's
**how many requests to let through to the backend at once**. Too few and
the backend idles, which is the c = 4 case with a window. Too many and the
queue moves somewhere we can't manage it, which is the c = 20 case. That's
the in-flight concurrency limiter, and it's Experiment 2.

## Second surprise: the window hardly batches anything

Even at c = 20 with a 20 ms window, each dispatch gathered on average
**1.45 requests**. I had pictured requests piling up behind the window.
In a closed loop they don't. The backend finishes requests one at a time
at staggered moments, each finished client immediately sends its next
request, and so new arrivals are spaced out by the backend's completion
rhythm (~170 ms apart here). A 20 ms window catches one arrival, maybe two.
It would only form real batches if arrivals were bursty, and a backend that
batches continuously doesn't need them grouped in the first place.

## A smaller detail: the window is ~1 ms longer than configured

At window = 5 ms the mean queue wait is 6.3 ms; at 10 ms it's 11.0 ms.
Tokio's timer has millisecond granularity and rounds deadlines up, so a
single-request "batch" waits the whole window plus up to ~1 ms. At 20 ms the
mean dips below the window, because more requests join partway through and
wait less than the full time. It's a small effect, but it would matter if
someone tried to set a sub-millisecond window.

## Does it hold outside the simulator? (Ollama, `gemma3:270m`)

As a check, I ran window = 0 vs 20 ms against a real model: Ollama 0.33
with `gemma3:270m`, on the Mac's GPU. One thing I didn't expect: the Ollama
server here runs with `OLLAMA_NUM_PARALLEL=1`. It generates for one request
at a time, so its capacity is 1, not 4.

That meant my first comparison, at c = 4 and c = 20, put both runs in the
*saturated* regime, where the model above predicts no visible difference.
That's what happened. Six pairs at each level showed no consistent
difference: mean paired TTFT change of +0.7 ms at c = 4, with ±15% swings
between runs. There was also a slow drift during the session: at a fixed
configuration, Ollama's throughput fell from ~19 to ~15 req/s over the course of
the session. macOS reported no thermal or performance warnings, and I don't
yet know the cause. A 20 ms effect can't be resolved against drift that
large, which is exactly why all the measurement here uses the mock (D3).
An early read of the first three c = 20 pairs looked like the window cost
~44 ms. Reversing the run order made the pattern vanish: in one reversed
pair the window run was 92 ms *faster*. It was an ordering artifact, and
it's why the order alternates.

The unsaturated regime for a capacity-1 backend is **c = 1**. Six pairs,
alternating which window went first:

| window | req/s | TTFT p50 (ms) | TTFT p99 | E2E p50 (ms) | E2E p99 |
|---|---|---|---|---|---|
| 0 | 13.52 (13.35–13.79) | 13.5 (13.1–13.7) | 19.8 | 72.4 (70.0–74.5) | 109.5 |
| 20 ms | 10.31 (10.14–10.55) | 36.8 (36.6–37.3) | 39.8 | 97.1 (96.1–98.0) | 124.0 |

The window costs +23 ms of TTFT and **−24% throughput**, in all 6 pairs,
with no overlap between the ranges. Throughput falls much more than on the
mock because Ollama's service time (~72 ms) is much shorter than the
mock's, so 20 ms is a bigger share of each cycle. The same one-line model
predicts 10.5 req/s; I measured 10.3. The mechanism holds against a real
model.

## Limitations — read these before quoting the numbers

- **The mock's capacity curve isn't a GPU's.** The mock is flat up to 4
  concurrent requests and then blocks completely. A real GPU running
  continuous batching degrades *gradually*: each extra concurrent sequence
  makes every step a little slower, well before any hard limit. On real
  hardware the saturated regime won't be as clean as the c = 20 table.
  There, holding requests back at the gateway changes how many sequences
  share each GPU step, so the window probably has some small effect on
  per-token speed that the mock can't show. I expect the c = 4 conclusion
  (pure cost when not saturated) to carry over directly. I expect the
  c = 20 conclusion (no visible effect) to carry over only approximately.
  I haven't measured this on a GPU; vLLM needs CUDA and this is a Mac.
- **Closed-loop load.** A closed-loop generator sends more slowly when the
  system slows down, which can hide tail latency (coordinated omission).
  For *this* experiment that's what I wanted: fixed N in the system is what
  makes the Little's-law argument clean. `loadgen --rps` exists for
  open-loop runs, and Experiment 2 and the Phase 4 load suite should use it.
- **One host.** Gateway, backend and load generator share the machine's
  CPU. Absolute numbers include that contention, but it's the same across
  all windows, so the comparisons hold.
- **Three repetitions per mock configuration.** The run-to-run spread was
  under ±1% for E2E p50 and under ±0.3 ms for TTFT p50 at c = 4. That's
  well inside the project's ±5% reproducibility target, but three samples
  can't describe rare stalls like the one at c = 4, window = 5.

## What this changes

- The batching window stays in the code as an experiment switch, defaulted
  to 0. It's a measured negative result, not a feature.
- The real control is the **in-flight concurrency limit** (D1). Its purpose
  is to move the c = 20 queue out of the backend and into the gateway,
  where admission control (D5) and fair queueing (Phase 3) can act on it.
  Experiment 2 sweeps that limit at 1/2/4/8/16/32 against the same mock.
  The prediction: throughput rises until the limit matches the backend's
  capacity (4), then stays flat, while TTFT at the backend stays flat until
  the limit passes 4 and then climbs. That means the "knee" should sit at
  the backend's capacity, and the whole point of the limiter is to operate
  at that knee.

## Reproduce

```sh
bench/exp1/run.sh            # ~35 min; mock on :9100, gateway on :8100
```

Raw per-run output (loadgen reports plus gateway `/metrics` scrapes) for
every number above, including the Ollama runs, is in `bench/exp1/raw/`.
