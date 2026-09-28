# oxideGate — Roadmap

**Authoritative plan.** Where this and any other document disagree, this one
wins. Kept as the live status board: checkboxes here are the source of truth
for "how much is done" — update them as work lands, don't let this drift the
way it did once already (see the Sep 27 note below).

- Start: 2026-08-27 · Original target: 2026-10-22 (8 weeks)
- **Revised target: 2026-10-15.** Full scope kept — see "Compressed schedule."
- Status as of 2026-09-28: Phases 1, 1.5, **2 and 3 complete** (Blocks A–C,
  all ahead of target). Experiments 1–4 run and written up in
  `WRITEUP-1.md`. Next: Block D (reliability + optimisation).

---

## 2026-09-27 status check — what actually happened

An 18-day gap (personal emergency) landed between Sep 9 and Sep 27. During
that time Phase 1 and most of Phase 2a were rebuilt by hand from an earlier
local checkout — real, working, tested code, matching the design below. But
that checkout predated the doc-reconciliation pass, so `claude.md` and this
file had drifted to contradict the code (claiming Redis, describing batching
as a feature rather than an experiment). Fixed as part of this update. Also
fixed: a typo'd error code (`queu_full` → `queue_full`), and removed a
`trial/` directory of inert scratch files that had been committed by mistake.

Lesson for later phases: when picking work back up after a gap, diff the
actual repo against this file before assuming either is right.

---

## 2026-09-27 Block A notes — things that didn't match the plan

- The "green state" at `798c91e` wasn't green on this machine: clippy 1.98
  flags `while_let_loop` in the batching loop. Fixed (`b777ca9`). No
  toolchain is pinned, so clippy's lint set moves with whatever is installed.
- Experiment 1's prediction ("pure cost") held only below saturation. At
  saturation the cost is invisible end-to-end. That strengthens D1 rather
  than contradicting it, but it is a different result from the one written
  down in advance, and the writeup says so.
- D5 now has a number: 3x over-capacity open-loop load produced **zero** 429s.

---

## Decisions

Recorded so they don't get relitigated. Each one is a claim that has to survive
questioning, so the reasoning matters more than the choice.

### D1 — Batching is not a gateway concern. Measure it, then replace it.

The original spec had the scheduler hold requests for 5–20ms and "dispatch a
batch". That cannot work as written:

- `POST /v1/chat/completions` accepts **one** conversation per HTTP request.
  There is no batch endpoint. "Dispatching a batch" can only mean N separate
  HTTP requests sent at the same instant.
- vLLM already performs **continuous batching internally**, admitting new
  requests into a running GPU batch as slots free. It does not want, and cannot
  use, pre-batched input.

So a gateway-side window adds 0–window ms of latency to every request and buys
nothing. Batching is the right idea at the wrong layer.

**What replaces it:** an **in-flight concurrency limit** — how many requests the
gateway allows at the backend simultaneously. That is a real knob:

- too few in flight → GPU idles, throughput below capacity
- too many in flight → requests queue *inside* vLLM where we cannot see or
  reorder them; TTFT degrades and fairness becomes impossible, because a request
  already handed to the backend can no longer be deprioritised

Holding the queue at our layer rather than the backend's is what makes admission
control and fairness possible at all.

**Plan:** build the window anyway, measure that it hurts, publish the negative
result, then build the limiter and measure the knee. That arc — hypothesis,
measurement, surprise, redesign — is the spine of Writeup 1.

#### Honesty conditions for Experiment 1

The result is only worth publishing if the setup could have proved the opposite:

- `window = 0` is the control, always run.
- The mock already gives batching's benefit for free — 4 concurrent requests
  complete in the same time as 1, up to its concurrency limit. So a window has
  a real opportunity to add value on top. It is not a rigged test.
- The mock's throughput is flat up to its limit and then blocks; a real GPU
  degrades gradually instead. State that limitation in the writeup rather than
  waiting for a reader to find it.

### D2 — No Redis in V1. Quotas and counters are in-process.

V1 is a single process; Redis adds a network hop and nothing else. Worse, it
would destroy the Phase 3 experiment: the question there is what lock contention
on shared counters costs at high concurrency, and a Redis round-trip (~0.5ms)
swamps a contended atomic (~50ns) by four orders of magnitude.

Measure the contention in-process, then argue for Redis in a writeup with
numbers. Revisit only when V2 needs multiple gateway instances.

### D3 — Measurement runs against a mock backend, not a model.

The experiments target 5–20ms effects. A real model's per-request variance is
hundreds of ms and swamps them; the reproducibility target (±5%) is
unreachable that way.

The mock must **saturate**, or nothing queues and every curve is flat. It needs
fixed service time and a **hard concurrency limit**, so requests beyond the limit
genuinely wait. (Already built and confirmed to saturate — see `tools/mock_backend.py`.)

| Purpose | Backend |
|---|---|
| Day-to-day development | Ollama, `qwen2.5:0.5b` |
| **All measurement** | **`tools/mock_backend.py`, finite concurrency** |
| Realism / conformance checks | Ollama, occasionally OpenAI |
| Deployment story | vLLM on a GPU host |

vLLM is CUDA-only and will not run on the development Mac. This is why the
backend URL is configuration, not code.

### D4 — Queued requests get their response over a oneshot channel.

Once a scheduler sits between handler and backend, the handler must enqueue and
then wait. Each `QueuedRequest` carries a oneshot sender; the scheduler dispatches
and sends the `reqwest::Response` back through it; the handler streams from
there. Keeps SSE working through the queue. (Implemented in `src/scheduler.rs`.)

### D5 — Admission control needs D1's concurrency limiter to mean anything.

The bounded queue + `try_send` → `QueueFull` (429) exists and is correct code,
but right now it almost never fires: the scheduler drains the queue instantly
because dispatch is `tokio::spawn`ed with no cap on how many can run at once.
An empty queue has nothing to reject from. The 429 path only becomes meaningful
once the in-flight concurrency limiter (2b) creates real backpressure. Don't
read "admission control is done" as "admission control does anything yet" —
track it as one feature landing in two parts.

### D6 — Multi-tenancy design (decided Sep 28, before Block C)

Chosen explicitly by the author from laid-out alternatives:

- **Deficit round robin** for fairness, over virtual-time WFQ (tighter
  bounds, O(log n), harder to defend) and strict priority (starves free
  tier; not "weighted"). Per-tenant queues; paid weight 1.0, free 0.5, so
  a backlogged paid tenant gets 2 dispatches for every 1 free. Cost unit
  is one request — the mock's requests are identical; token-weighted cost
  is the obvious refinement if request sizes vary.
- **Token budgets reserve, then refund.** Admission reserves `max_tokens`
  (or a default) and rejects if it doesn't fit; completion refunds the
  unused part. Never overshoots, even under concurrency — "check then
  charge after" lets N concurrent requests all pass and overshoot by up to
  N x max_tokens.
- **Budgets are tokens-per-minute**, refilling continuously (a token
  bucket), like hosted LLM APIs — a throughput limit, not a billing cap.

Defaults taken without asking (standard, reversible): tenants from a JSON
file at startup, `Authorization: Bearer <key>` resolves the tenant; no file
= single anonymous tenant, so every earlier benchmark still runs unchanged.
Request rate = token bucket. Per-tenant cap on requests in the system
(queued + at backend), so one tenant's flood fills only its own share and
not the global queue. Counted tokens = completion tokens only (prompt
tokens would need a tokenizer — out of scope); streamed replies are
counted by SSE frames, ~1 token each on vLLM/Ollama, approximate.

---

## Compressed schedule — full scope, Oct 15

18 days as of Sep 27. Every phase in the original plan stays in scope, at full
depth — no phase is being cut. That means near-daily shipping from here, and
weekends are working days. Push order follows dependency, not the calendar:
each block below assumes the one above it is done, so slipping one shifts
everything after it — flag that here immediately if it happens, don't quietly
absorb it.

| Block | Target | Contains |
|---|---|---|
| **A** ✅ | Sep 30 — done Sep 27 | Cleanup (done, see Sep 27 note) + load generator + **Experiment 1** run + findings written up |
| **B** ✅ | Oct 3 — done Sep 27 | In-flight concurrency limiter + **Experiment 2** + admission control actually sized and meaningful (closes D5) — **Phase 2 done** |
| **C** ✅ | Oct 7 — done Sep 28 | Tenant model, quotas, weighted fair queue + **Experiment 3** (fairness) + **Experiment 4** (contention cost, D2) — **Phase 3 done** |
| **D** | Oct 11 | Health probes, circuit breaker, no-retry-after-stream, fault injection harness, load suite at 10/50/100/500, flamegraph + one measured optimisation — **Phase 4 done** |
| **E** | Oct 14 | Both writeups, README with latency table — **Phase 5 done** |
| — | Oct 15 | Ship |

If Block D runs long, the optimisation pass (not the fault injection or load
suite — those are the measurements) is the piece to compress first: a smaller,
well-explained improvement beats a bigger unmeasured one, per the project's own
"cut features, never cut measurements" rule.

---

## Phases

### Phase 1 — Proxy skeleton · ✅ complete

axum server; OpenAI-compatible `/v1/chat/completions`; forwarding to a
configurable backend over reqwest; SSE pass-through with TTFT measurement;
typed errors mapped to 502; Docker + Compose.

### Phase 1.5 — Pre-flight · ✅ complete

- [x] Reconcile the docs; this file is authoritative
- [x] Mock backend: finite concurrency, configurable service time
- [x] `/metrics` endpoint (Prometheus)
- [x] Router extracted behind a lib target for integration tests
- [x] Integration test harness (11 tests, passing)

### Phase 2 — Scheduler · Block A + B

#### 2a — get the answer (Block A)

- [x] `QueuedRequest` + oneshot response channel (D4)
- [x] Queue + scheduler task; batching window configurable, `0` disables it
- [x] Streaming preserved through the queue (regression-tested)
- [x] Load generator (`src/bin/loadgen.rs`): concurrency/RPS knobs, TTFT + E2E
      percentiles via hdrhistogram *(Sep 27. Closed loop by default; `--rps`
      is open loop, measured from scheduled send time to avoid coordinated
      omission. Non-2xx and truncated streams count as failures — see commit.)*
- [x] **Experiment 1** — window 0/5/10/20ms against the mock, `0` as control
      *(Sep 27. Run at c=20 as planned **and** c=4. c=4: pure cost, TTFT
      +window ms, throughput −4% at 20ms. c=20: zero visible effect — Little's
      law hides it because the backend is saturated; gateway queue-wait shows
      the cost is still paid. Not the flat "pure cost" predicted; explained in
      the writeup. 3 reps each, spread <±1%. Data: `bench/exp1/`.)*
- [x] Confirmation run against real Ollama
      *(Sep 27, `gemma3:270m` — `qwen2.5:0.5b` not pulled locally. Surprise:
      this Ollama runs `NUM_PARALLEL=1`, so c=4/c=20 were both saturated and
      showed nothing through ±15% run-to-run drift. Confirmed at c=1: window
      20ms costs +23ms TTFT, −24% throughput, 6/6 alternating pairs.)*
- [x] Writeup 1 draft: `WRITEUP-1.md` *(the window is kept as a default-0
      experiment switch rather than deleted — it's the control for the story)*

#### 2b — build on it (Block B)

- [x] In-flight concurrency limiter (replaces the window as the real knob)
      *(Sep 27, `8824cef`. `OXIDEGATE_MAX_INFLIGHT`, 0 = unlimited. Slot is
      taken before dequeue, so a full backend stops the queue draining; the
      slot is held until a stream's last byte. 5 new load tests, 16/16.)*
- [x] **Experiment 2** — limit 1/2/4/8/16/32, expect a knee
      *(Sep 27. Knee exactly at the mock's capacity (4): throughput
      1.48/2.94/5.92 then flat. Client latency identical for every limit >=4
      (Little's law again); what changes is where the wait lives — 75% in our
      queue at limit 4, 0% unlimited. Data: `bench/exp2/`.)*
- [x] Admission control sized by Experiment 2's result (closes D5)
      *(Open loop at 1.7x capacity: unlimited = 0 rejections, TTFT p99 20.7s;
      limit 4 + queue 12 (= 2s budget x 5.9 req/s) = 37% rejected, accepted
      TTFT p99 2.25s, same throughput. Default queue depth 100 = 17s budget,
      too loose to matter. Code defaults left at off/100 because the right
      values are backend-specific; startup now warns when the limiter is
      off. **Open item for Block E:** docker-compose must set these for
      whatever backend it ships with, or `compose up` reproduces D5.)*

**Exit:** both curves plotted and explainable; queue rejection actually happens
under load, not just in a unit test.

### Phase 3 — Multi-tenancy · Block C

- [x] Tenant model: id, tier, token budget, rate limit, concurrency cap *(Sep 28, `f547fad`; design in D6)*
- [x] Quota checks at admission; token accounting on completion *(reserve/refund; streamed tokens counted by SSE frame)*
- [x] Weighted fair queueing (free 0.5x, paid 1.0x) *(deficit round robin; `OXIDEGATE_FAIR_QUEUE=0` = FIFO control)*
- [x] **Experiment 3** — free-tier flood must not move paid-tier p99
      *(Sep 28. **Target not met literally, and that's the finding.** Paid
      TTFT p99: alone 209ms, flood+FIFO 14.9s, flood+DRR 0.70s. The +0.49s
      residual is non-preemptive head-of-line blocking at the backend —
      bounded by one service time, scales with it (330ms service -> +177ms).
      Removing it needs reserved paid capacity or preemption: a policy
      choice, open for the author. Data: `bench/exp3/`.)*
- [x] **Experiment 4** — cost of counter contention at high concurrency (D2)
      *(Sep 28. Worst case, 16 threads on one tenant's lock: ~270ns/op,
      ceiling ~3.7M admissions/s. Per-tenant locks scale (81->19ns), a
      global lock doesn't (~170ns flat). Loopback TCP RTT 14.2us = ~50x the
      worst contended admission; D2 holds. Found: Lease::drop allocates a
      metric-label String per request — Block D optimisation candidate.)*

**Exit:** fairness demonstrated with numbers, and its cost quantified.

### Phase 4 — Reliability + optimisation · Block D

- [ ] Health probes; circuit breaker
- [ ] Timeouts, backoff, **never retry a stream that emitted tokens**
- [ ] Fault injection: kill backends mid-stream, inject latency, saturate queues
- [ ] Load suite at 10/50/100/500 concurrency
- [ ] Flamegraph profiling; one optimisation pass with before/after numbers

**Exit:** degradation curves, and a measured improvement.

### Phase 5 — Writeups · Block E

- [ ] Writeup 1 — "What surprised me": D1's batching result is the centrepiece
- [ ] Writeup 2 — "Failure modes and graceful degradation"
- [ ] README opens with a latency table and links to both

---

## Out of scope (V1)

Postgres persistence · web dashboards · multi-region · custom model support ·
advanced analytics · Kafka · service mesh · gRPC · custom storage · WASM.

If a task appears to need one of these, it is out of scope. Cut it.

## Definition of done

Clone, `docker compose up`, fire the load generator, reproduce the numbers in the
README within ±5%. Every design choice has data behind it.
