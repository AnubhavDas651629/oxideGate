# oxideGate — Roadmap

**Authoritative plan.** Where this and any other document disagree, this one wins.

- Start: 2026-08-27 · Target ship: 2026-10-22 (**8 weeks**, four 2-week phases)
- Status as of 2026-09-05: **Phase 1 complete**, 5 days ahead of its Sep 10 target.

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
genuinely wait.

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
there. Keeps SSE working through the queue.

---

## Phases

### Phase 1 — Proxy skeleton · Aug 27 – Sep 10 · ✅ complete Sep 5

axum server; OpenAI-compatible `/v1/chat/completions`; forwarding to a
configurable backend over reqwest; SSE pass-through with TTFT measurement;
typed errors mapped to 502; Docker + Compose.

### Phase 1.5 — Pre-flight · Sep 5 – Sep 10 · ✅ complete Sep 5

Groundwork the scheduler lands into. Not in the original plan; bought with the
slack from finishing Phase 1 early.

- [x] Reconcile the docs; this file becomes authoritative
- [x] Mock backend: finite concurrency, configurable service time
- [x] `/metrics` endpoint (Prometheus) — needed before a queue exists to observe
- [x] Extract the router behind a lib target so integration tests can drive it
- [x] First integration tests, so scheduler work lands into a harness

### Phase 2 — Scheduler · Sep 11 – Sep 24

Ordered so the batching result lands **first**. D1 decides what the scheduler's
core abstraction should be, so building admission control and fairness before
knowing the answer means reworking them afterwards.

The experiment cannot run before the scheduler exists — a batching window *is*
scheduler machinery. So this is Phase 2 reordered, not work moved ahead of it.

#### 2a — get the answer · Sep 11 – 17

- [ ] `QueuedRequest` + oneshot response channel (D4)
- [ ] Queue + scheduler task; batching window configurable, `0` disables it
- [ ] Streaming preserved through the queue (regression risk: the existing SSE
      test must keep passing)
- [ ] Load generator (`src/bin/loadgen.rs`): configurable concurrency and RPS,
      TTFT and E2E percentiles
- [ ] **Experiment 1** — window 0/5/10/20ms against the mock. `0` is the
      control; without it the numbers mean nothing
- [ ] Confirmation run against real Ollama — noisier, but it shows the
      mechanism holds outside the simulator
- [ ] **Writeup 1 draft** — "I built a batching layer and deleted it"

#### 2b — build on it · Sep 18 – 24

- [ ] In-flight concurrency limiter (replaces the window)
- [ ] **Experiment 2** — limit 1/2/4/8/16/32 (expect: a knee)
- [ ] Admission control: queue-depth threshold sized by what Experiment 2
      showed, 429 on overflow

**Exit:** both curves plotted and explainable, and a draft writeup.

#### Honesty conditions for Experiment 1

The result is only worth publishing if the setup could have proved the opposite:

- `window = 0` is the control, always run.
- The mock already gives batching's benefit for free — 4 concurrent requests
  complete in the same time as 1, up to its concurrency limit. So a window has
  a real opportunity to add value on top. It is not a rigged test.
- The mock's throughput is flat up to its limit and then blocks; a real GPU
  degrades gradually instead. State that limitation in the writeup rather than
  waiting for a reader to find it.

### Phase 3 — Multi-tenancy · Sep 25 – Oct 8

- [ ] Tenant model: id, tier, token budget, rate limit, concurrency cap
- [ ] Quota checks at admission; token accounting on completion
- [ ] Weighted fair queueing (free 0.5x, paid 1.0x)
- [ ] **Experiment 3** — free-tier flood must not move paid-tier p99
- [ ] **Experiment 4** — cost of counter contention at high concurrency (D2)

**Exit:** fairness demonstrated with numbers, and its cost quantified.

### Phase 4 — Reliability + optimisation · Oct 9 – Oct 22

- [ ] Health probes; circuit breaker
- [ ] Timeouts, backoff, **never retry a stream that emitted tokens**
- [ ] Fault injection: kill backends mid-stream, inject latency, saturate queues
- [ ] Load suite at 10/50/100/500 concurrency
- [ ] Flamegraph profiling; one optimisation pass with before/after numbers

**Exit:** degradation curves, and a measured improvement.

### Phase 5 — Writeups · Oct 23 – Nov 2

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
