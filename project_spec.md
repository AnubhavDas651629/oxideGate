# Project Spec: oxideGate

## Part 1: Product Requirements

### Definition
A minimal, highly measurable LLM inference gateway that sits between clients and model-serving backends (vLLM/llama.cpp). The gateway owns everything *around* inference: queueing, admission control, batching, routing, multi-tenant quotas, and failure handling.

**Key principle**: Backends are black boxes. We do not implement or modify inference logic.

### Product Purpose
To **demonstrate deep systems thinking and performance optimization** through a real, measured artifact. The deliverable is not features; it's **measurements, analysis, and defensible design decisions**.

### Who Is This For?
- **Primary user**: You (fresher building a portfolio)
- **Secondary audience**: YC founders/infrastructure teams hiring systems engineers
- **Validation metric**: "Can I explain every architectural choice under interview pressure?"

### Problems It Solves
1. **Unknown performance behavior under load** — Most inference gateways lack realistic load testing. We measure everything.
2. **Opaque latency tradeoffs** — "Batching is good" vs. "batching adds tail latency" — we quantify the curve.
3. **Multi-tenant fairness** — How do you prevent a free-tier flood from starving a paid tenant? We measure the cost.
4. **Async/concurrency knowledge gap** — Most junior engineers don't deeply understand non-blocking I/O, futures, backpressure. Building this teaches it.

### Functionality (V1)

#### Core flows:
1. **Request admission** → Client sends OpenAI-compatible `/v1/chat/completions` request with API key
2. **Tenant resolution** → API key maps to tenant with quota (token budget, rate limit, concurrency cap)
3. **Queueing** → Request enters fair queue or rejected with 429
4. **Batching** → Scheduler waits 5-20ms (configurable), collects requests, dispatches batch
5. **Backend routing** → Select healthiest backend by current load
6. **Streaming** → Forward tokens back to client via SSE
7. **Accounting** → Track token usage against tenant quota
8. **Failure handling** → Timeout, retry (only if no tokens emitted), circuit break unhealthy backends

#### Not included in V1:
- Persistence (no Postgres logging yet)
- Advanced analytics dashboards
- Multi-region federation
- Custom model selection logic

### Jobs to Be Done

**For the portfolio builder (you):**
- Understand how systems behave under realistic multi-tenant load
- Master Rust async/concurrency at depth
- Learn performance profiling and optimization
- Build something defensible in interviews

**For hypothetical YC users (later):**
- Run multiple tenants on one backend without mutual interference
- Measure and optimize inference latency under load
- Detect and isolate backend failures gracefully

### User Experience
V1 is **not** about ease of use. It's about measurement.
- Admin can spin up gateway in Docker Compose
- Admin can configure batching window, queue depth, tenant quotas
- Admin fires load generator and sees real-time latency/throughput curves
- Admin reads flamegraph output and understands bottlenecks
- Admin writes up findings in a blog post

**Explicitly not V1**: Web dashboards, self-serve tenant onboarding, mobile apps.

---

## Part 2: Technical Design

### Tech Stack (Fixed)
| Layer | Choice | Why |
|---|---|---|
| Language | Rust 1.70+ | Type safety, performance, async story. Shows depth. |
| Runtime | Tokio | Industry standard async runtime. |
| HTTP server | axum | Minimal, composable, good for streaming (SSE). |
| Middleware | tower | Standardized middleware layer for auth, metrics. |
| Serialization | serde / serde_json | OpenAI API compatibility. |
| Outbound HTTP | reqwest | Async HTTP client for backend calls. |
| Load testing | criterion + custom Rust harness | Measurement is the deliverable. |
| Profiling | flamegraph + perf | Identify bottlenecks. |
| Metrics | Prometheus format + local logging | Track latency histograms, throughput. |
| Queueing state | In-memory (crossbeam channels + Arc<Mutex>) | V1 single-process. Redis in V2. |
| Tenant quotas | In-memory HashMap with atomic counters | Single-process V1. |
| Backends | vLLM (2 instances) + llama.cpp CPU fallback | Realistic multi-backend scenario. |
| Deploy | Docker Compose | Easy local setup. |

**Explicitly out of scope**: Kafka, service mesh, gRPC, custom DBs, WASM, new frameworks.

### Engineering Requirements

#### Non-functional:
1. **Latency measurement** — Track p50/p95/p99 for time-to-first-token (TTFT) and end-to-end (E2E). Must support 1000+ req/s load.
2. **Async bounds** — No blocking I/O on the hot path. No `std::thread::sleep` in request handlers.
3. **Correctness under failure** — Partial response handling: if a backend dies mid-stream, surface the error, don't retry.
4. **Zero unwrap() in hot paths** — Request handling must use `?` and proper error propagation. Tests can panic.
5. **Load testing reproducibility** — Every design change measured with before/after curves. Flamegraph diffs.

#### Functional:
1. **OpenAI API compatibility** — `POST /v1/chat/completions` request/response match OpenAI schema (subset).
2. **Per-tenant quotas** — Token budget, request rate limit, max concurrent requests. Enforced at admission.
3. **Admission control** — Reject with 429 (Too Many Requests) when queue depth > threshold.
4. **Batching window** — Configurable 5-20ms. Collect requests, dispatch as batch.
5. **Fair queueing** — Weighted by tenant tier (free tier weighted at 0.5x, paid at 1.0x). Free-tier flood doesn't starve paid.
6. **Backend health checking** — Periodic probe. Mark unhealthy, remove from rotation, probe again.
7. **SSE streaming** — Forward tokens from backend to client as they arrive. Partial responses on error.
8. **Prometheus metrics** — Expose `/metrics` endpoint with histograms for latency, counters for requests/errors, gauges for queue depth.

---

## Architecture Overview

### Four layers (built sequentially):

```
┌─────────────────────────────────────────┐
│  1. Proxy Layer                         │
│  OpenAI-compatible API endpoint         │
│  Auth → tenant resolution               │
└──────────────┬──────────────────────────┘
               │
┌──────────────▼──────────────────────────┐
│  2. Scheduler (Core)                    │
│  Queue + admission control              │
│  Batching window + routing              │
│  Weighted fair queueing                 │
└──────────────┬──────────────────────────┘
               │
┌──────────────▼──────────────────────────┐
│  3. Multi-Tenancy                       │
│  Per-tenant quotas (tokens, rate, conc.)│
│  Token accounting per request           │
└──────────────┬──────────────────────────┘
               │
┌──────────────▼──────────────────────────┐
│  4. Backend Management                  │
│  Circuit breaking + health checks       │
│  Timeout/retry (no-retry-after-stream)  │
│  Error handling + graceful degradation  │
└──────────────┬──────────────────────────┘
               │
               ▼ (outbound HTTP)
        [vLLM backends]
```

### Key Components

#### 1. Proxy Layer (`src/proxy.rs`)
- **HTTP handler** for `POST /v1/chat/completions`
- **Auth middleware** — Extract API key, resolve to tenant
- **SSE response stream** — Forward tokens from scheduler to client
- **Error handling** — Return proper OpenAI error responses

**Interaction**: Proxy → Scheduler (enqueue request) → Response stream (pull tokens)

#### 2. Scheduler (`src/scheduler.rs`) — THE CORE
- **Queue** — Crossbeam MPMC channel for incoming requests
- **Batch collector** — Timer task that wakes every N ms
- **Fair queue** — Weighted queue per tenant tier
- **Admission control** — Check: (queue_depth < threshold) AND (tenant_quota_available)
- **Routing** — Select backend by health + current load
- **Dispatch** — Send batch to selected backend, track response stream

**Interaction**: Proxy → Queue, Scheduler → Backends, Scheduler → Multi-tenancy (check quotas)

#### 3. Multi-Tenancy (`src/tenants.rs`)
- **Tenant store** — In-memory map of tenant_id → quota config
- **Quota tracking** — Token budget, request rate limit, concurrency cap
- **Atomic counters** — Current usage (lock-free reads where possible)
- **Accounting** — Deduct tokens as stream ends

**Interaction**: Scheduler checks quotas before admission, updates counters as requests complete

#### 4. Backend Management (`src/backends.rs`)
- **Health probe task** — Periodic healthcheck
- **Circuit breaker** — Track error rate, mark unhealthy
- **Load tracking** — Current requests per backend
- **Failure recovery** — Re-probe unhealthy backends

**Interaction**: Scheduler queries for healthiest backend, receives responses, reports errors

---

## System Design

### Request lifecycle:
```
Client request
    ↓
[Proxy] Auth, parse OpenAI request
    ↓
[Scheduler] Enqueue
    ↓
[Admission] Check: queue_depth < threshold? tenant_quota available?
    ├─ YES → admit, wait for batch
    ├─ NO → reject 429
    ↓
[Batch timer] 10ms elapsed (configurable)
    ↓
[Batch collector] Gather N requests
    ↓
[Fair queue] Sort by tenant tier
    ↓
[Routing] Select healthiest backend
    ↓
[Dispatch] Forward batch to backend
    ↓
[Backend] Process, stream tokens back
    ↓
[Proxy SSE] Forward tokens to client
    ↓
[Accounting] Update tenant quota
    ↓
Client receives complete response (or error)
```

### Data structures:

#### Request representation (`src/types.rs`):
```rust
struct IncomingRequest {
    tenant_id: String,
    model: String,
    messages: Vec<Message>,
    max_tokens: Option<usize>,
}

struct QueuedRequest {
    req: IncomingRequest,
    enqueued_at: Instant,
    priority: f32,  // Based on tenant tier
}
```

#### Tenant config:
```rust
struct Tenant {
    id: String,
    tier: TenantTier,  // Free, Pro, Enterprise
    token_budget: u64,
    request_rate_limit: u32,  // req/sec
    max_concurrent: u32,
}
```

#### Metrics snapshot (for load generator output):
```rust
struct LatencySnapshot {
    p50_ms: f32,
    p95_ms: f32,
    p99_ms: f32,
    throughput_rps: f32,
    queue_depth: usize,
}
```

### Concurrency model:
- **Scheduler task** — Single async task that owns the queue and batching logic. Runs continuously.
- **Proxy handlers** — Tokio task per connection. Only enqueue and forward responses, don't do heavy work.
- **Health probe task** — Background task, probes every 5s (configurable).
- **Load generator task** (in tests) — Spawns N async tasks, each fires M requests.

**Why**: Centralized scheduler avoids distributed consensus. Async I/O prevents blocking. Clear separation of concerns.

### Failure handling:
| Failure | Action |
|---------|--------|
| Backend timeout | Mark unhealthy, try next backend, fail request if all down |
| Partial stream (backend dies mid-response) | Surface the partial error; **do not retry** |
| Queue overflow | Reject new requests with 429 |
| Tenant quota exceeded | Reject with 429 |
| Health probe fails | Increment error counter, after N failures mark unhealthy |

---

## API Design

### Endpoint: `POST /v1/chat/completions`

#### Request (OpenAI-compatible subset):
```json
{
  "model": "qwen-0.5b",
  "messages": [
    {"role": "user", "content": "Hello"}
  ],
  "max_tokens": 100,
  "temperature": 0.7
}
```

#### Response (SSE stream):
```
data: {"choices": [{"delta": {"content": " Hello"}}]}
data: {"choices": [{"delta": {"content": " world"}}]}
data: [DONE]
```

#### Error response:
```json
{
  "error": {
    "message": "Queue full, try again",
    "code": "429_queue_full"
  }
}
```

### Other endpoints:

#### `GET /metrics`
Prometheus metrics:
```
http_requests_total{method="POST", endpoint="/v1/chat/completions", status="200"} 1234
http_request_duration_seconds_bucket{le="0.1"} 500
http_request_duration_seconds_bucket{le="1.0"} 1200
gateway_queue_depth 45
gateway_active_requests 23
tenant_token_usage{tenant_id="free-tier-1"} 5000
```

#### `GET /health`
```json
{
  "status": "ok",
  "backends": [
    {"id": "vllm-0", "healthy": true, "load": 12},
    {"id": "vllm-1", "healthy": false, "load": 0}
  ]
}
```

---

## V1 Milestones (Clear success criteria)

### Phase 1: Proxy skeleton (Week 1–2)
- [ ] `axum` HTTP server listening on `:8000`
- [ ] Parse OpenAI-compatible `/v1/chat/completions` request
- [ ] Forward to hardcoded vLLM backend via `reqwest`
- [ ] Stream SSE response back
- [ ] Docker Compose file with 1x vLLM container
- [ ] **Measurement**: Request works end-to-end. No latency targets yet.

### Phase 2: Scheduler + batching (Week 3–4)
- [ ] Request queue (crossbeam MPMC)
- [ ] Batch timer and collector
- [ ] Admit requests based on queue depth (hard threshold)
- [ ] Dispatch batches to backend
- [ ] **Measurement**: Latency vs. batch size curve. Graph it.

### Phase 3: Multi-tenancy + fairness (Week 4–5)
- [ ] Tenant model (id, tier, quotas)
- [ ] Quota checks at admission
- [ ] Token accounting
- [ ] Weighted fair queueing
- [ ] **Measurement**: Fairness test — free tier flood doesn't starve paid tier. Measure p99 latency delta.

### Phase 4: Reliability + full suite (Week 5–6)
- [ ] Health probes for backends
- [ ] Circuit breaker
- [ ] No-retry-after-stream rule
- [ ] Full load generator (configurable concurrency, RPS)
- [ ] Flamegraph profiling
- [ ] **Measurement**: Latency histograms at 10x, 50x, 100x concurrency. Degradation curve.

### Phase 5: Writeups (Week 6+)
- [ ] Writeup 1: "What surprised me" (batching curves, fairness costs, tail latency insights)
- [ ] Writeup 2: "Failure modes and graceful degradation" (what happens when backends die?)
- [ ] Update README with latency table + links to writeups

---

## Success Criteria for V1

1. **The system runs** — Docker Compose up, load generator fires, requests complete end-to-end.
2. **Measurements are reproducible** — Run twice, get same latency curves (within ±5%).
3. **Code is defensible** — Every architectural choice has a "why" and supporting data.
4. **Writeups are solid** — Someone reads them and understands the tradeoffs without asking questions.
5. **Rust is idiomatic** — `cargo fmt` + `cargo clippy -- -D warnings` pass. No unsafe code except where justified.

---

## Notes for Implementation

- Start **ugly**. Week 1–2 is about learning the shape of the problem, not perfection.
- **Measurements trump features**. A slow system with clear metrics beats a fast system with no data.
- **Commit history matters**. Commit often, in public. The evolution is part of the portfolio.
- **Rust learning** — Expect to hit borrow-checker walls. Document the "why" in the solution; that's the learning.
- **Load generator is critical** — Without realistic load, you can't see emergent behavior. Build it early (Week 3+).
