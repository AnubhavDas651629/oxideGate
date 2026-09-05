# CLAUDE.md

## What this project is

A multi-tenant LLM inference gateway written in Rust. It sits in front of one or
more model-serving backends (vLLM / llama.cpp) and owns everything *around* the
model: admission control, request queueing, batching, per-tenant quotas,
backend routing, health checking, and failure handling.

It does **not** implement model inference. Backends are black boxes that accept
prompts and stream tokens. Do not suggest work on model internals, quantization,
attention kernels, or training — that is out of scope permanently.

## Why it exists (this shapes every decision)

This is a portfolio project built to demonstrate systems engineering ability to
early-stage infrastructure startups. That has two consequences that override
normal "make it work" instincts:

1. **Measurements are the deliverable, not the code.** A feature without a
   benchmark attached is half-finished. Latency histograms, throughput curves,
   and before/after optimization numbers are the actual output.
2. **I must be able to defend every design decision in an interview.** See
   "How to work with me" below — this changes how you should help.

## Author context

- Background: Python/FastAPI backend, some deep learning (weather forecasting).
- **Learning Rust while building this.** Assume no deep familiarity with
  ownership, lifetimes, pinning, or async internals. Explain rather than assume.
- Self-assessment: backend / DevOps / systems focused. Not an ML engineer.
  Frame things accordingly; don't lean on ML-side reasoning.

## Tech stack

Fixed. Do not propose additions without a strong, specific reason.

| Layer | Choice |
|---|---|
| Language / runtime | Rust, Tokio |
| HTTP server | axum (SSE for token streaming) |
| Middleware | tower (auth, rate limit, metrics layers) |
| Serialization | serde / serde_json |
| Outbound HTTP | reqwest |
| Observability | tracing, tracing-subscriber, Prometheus metrics, Grafana |
| Benchmarking | criterion + a custom Rust load generator |
| Profiling | flamegraph, perf |
| Shared state | In-process: atomics + `Arc<Mutex>`. **No Redis in V1** (ROADMAP D2) |
| Config / logs (optional) | Postgres |
| Backends | 2x vLLM containers, small model (Qwen 0.5B class); llama.cpp CPU fallback |
| Deploy | Docker Compose (local), Kubernetes manifests, GitHub Actions CI |

**Explicitly out of scope:** Kafka, service mesh, gRPC, custom storage engines,
WASM, any new framework. If a task seems to need one of these, say so and stop —
don't add it.

## Architecture

Four layers, built in order. Do not jump ahead.

### 1. Proxy layer
OpenAI-compatible `POST /v1/chat/completions`. API-key auth resolves to a tenant.
Forwards to a backend and streams tokens back via SSE.

### 2. Scheduler (the core)
Requests do not go straight to a backend. They enter a queue; a scheduler task
decides what to dispatch, where, and when.

- **Admission control** — reject with 429 when queue depth exceeds threshold.
  Rejecting early is correct behaviour, not a failure.
- **Concurrency limiting** — cap how many requests are in flight at the backend
  at once. Too few idles the GPU; too many buries the queue inside vLLM where we
  cannot reorder or shed it. Finding that knee is a primary experiment.
- **Batching** — built and measured as a *negative* result, not a feature. The
  backend batches continuously on its own and takes one conversation per HTTP
  request, so a gateway-side window is pure added latency. See ROADMAP D1.
- **Fairness** — tenants have tiers; a free-tier flood must not starve a paid
  tenant. Weighted fair queueing.
- **Routing** — select backend by current load and health, not round-robin.

### 3. Multi-tenancy
Per-tenant token budgets, request-rate limits, and concurrency caps, enforced at
admission time. Counters in-process (atomics), so their contention cost is
measurable rather than hidden behind a network hop. Per-request usage accounting.

### 4. Reliability and measurement
- Circuit breaking: erroring backends leave rotation, get probed until healthy.
- Timeouts and backoff retries. **Never retry a stream that already emitted
  tokens** — surface a partial failure instead.
- Fault-injection harness: kill backends mid-stream, inject latency, saturate
  the queue. Prove graceful degradation.
- Load generator + metrics: p50/p95/p99 for time-to-first-token and end-to-end
  latency, throughput vs. concurrency, saturation behaviour.

## How to work with me

This is the most important section.

**Generate freely** for: boilerplate, type definitions, Docker/K8s configs, CI
pipelines, the load generator, test scaffolding, README/writeup drafts, and
explaining compiler errors.

**Do not hand me finished code** for: the scheduler, batching logic,
backpressure, admission control, fairness policy, or the concurrency model.
For these, instead:
- lay out the options and their tradeoffs,
- ask me which I want and why,
- let me write the first version,
- then review it and push back on what's wrong.

The rule: if I couldn't defend it under questioning, it shouldn't be in the
repo. Code I accepted because it compiled is a liability, not an asset.

**When I'm learning Rust**, prefer explanation over correction. If I hit a
borrow-checker error, explain the ownership problem before showing the fix.

**Push back on me.** If a design I propose is wrong, say so directly. Don't
soften it. If I'm about to over-engineer something, say that too — scope creep
is the main risk to this project.

## Conventions

- `cargo fmt` and `cargo clippy -- -D warnings` must pass before any commit.
- Errors: `thiserror` for library errors, `anyhow` at the binary boundary.
- No `unwrap()` / `expect()` in request-handling paths. Tests are fine.
- Instrument new async paths with `tracing` spans as they're written, not later.
- Every scheduler-behaviour change needs a test that exercises it under load.
- Commit often and in public — the commit history is part of the artifact.

## Timeline

See `ROADMAP.md` — it is authoritative for dates, scope and decisions.
Eight weeks, Aug 27 → Oct 22, in four two-week phases: proxy skeleton,
scheduler, multi-tenancy, reliability + optimisation. Then writeups.

Shipping something real and measured beats shipping something complete. If time
runs short, cut features, never cut the measurements.

## Definition of done

The README opens with a latency table and links to the writeups. Someone can
clone it, run `docker compose up`, fire the load generator, and reproduce the
numbers.