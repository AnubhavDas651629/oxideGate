# oxideGate — Project Timeline & Goals

**Project Start Date**: August 27, 2026  
**Target Ship Date**: October 22, 2026 (**8 weeks**, four 2-week phases)

> `ROADMAP.md` is authoritative for scope and design decisions. This file is the
> dated milestone breakdown that sits under it.

---

## Phase 1: Proxy Skeleton
**Duration**: Aug 27 – Sep 10 (2 weeks)  
**Goal**: Get something running end-to-end. Ugly is fine.

### Milestones:
| Date | Milestone | Success Criteria |
|------|-----------|-----------------|
| Aug 29 | Rust/Tokio setup + project scaffold | `cargo build` succeeds. Understand basic project structure. |
| Aug 31 | Basic axum HTTP server | Server listens on `:8000`, responds to GET `/health` with JSON. |
| Sep 2 | Parse OpenAI request + mock response | POST `/v1/chat/completions` accepts JSON, returns hardcoded response. |
| Sep 4 | Forward to real vLLM backend | `reqwest` forwards request to vLLM, gets real response back. |
| Sep 6 | SSE streaming | Forward tokens as they arrive. Client sees tokens streamed in real-time. |
| Sep 8 | Docker Compose setup | `docker-compose up` brings up gateway + 1x vLLM. End-to-end request works. |
| Sep 10 | ✅ Phase 1 complete | Code compiles (`cargo fmt`, `cargo clippy`). Single request works. |

### Rust Learning Focus:
- Basic Tokio async/await
- axum handlers and extractors
- `reqwest` client setup
- Error handling with `?` operator and `anyhow`
- Struct definitions and trait implementations

---

## Phase 2: Scheduler
**Duration**: Sep 11 – Sep 24 (2 weeks)  
**Goal**: Build the queue and batch collector. Start measuring latency.

### Milestones:
| Date | Milestone | Success Criteria |
|------|-----------|-----------------|
| Sep 11 | Queue structure (crossbeam MPMC) | Requests enqueue without blocking. No deadlocks. |
| Sep 13 | Batch timer task | Background task wakes every 10ms, collects pending requests. |
| Sep 15 | In-flight concurrency limit | Cap concurrent backend requests; queue the rest. See ROADMAP D1. |
| Sep 17 | Admission control (hard threshold) | Reject with 429 if queue_depth > 100. Test it. |
| Sep 19 | Load generator v1 | Simple Rust harness fires N concurrent requests, measures response times. |
| Sep 22 | Latency measurement | Two curves: batching window 0/5/10/20ms (expect pure cost), and in-flight limit 1-32 (expect a knee). |
| Sep 24 | ✅ Phase 2 complete | Both curves exist. Can explain why a gateway-side window only adds latency against a continuously-batching backend, and where the concurrency knee is. |

### Rust Learning Focus:
- Crossbeam channels (MPMC)
- Arc + Mutex for shared state
- Background task spawning (tokio::spawn)
- Timing and Instant
- Basic trait objects (for generics)
- Lifetimes in function signatures

---

## Phase 3: Multi-Tenancy + Fairness
**Duration**: Sep 25 – Oct 8 (2 weeks)  
**Goal**: Per-tenant quotas, fair queueing, prove fairness under load.

### Milestones:
| Date | Milestone | Success Criteria |
|------|-----------|-----------------|
| Sep 25 | Tenant model + in-memory store | Define tenant struct (id, tier, quotas). Store in HashMap. |
| Sep 27 | Quota checks at admission | Before admitting, check: token_budget, rate_limit, max_concurrent. Deduct on admission. |
| Sep 29 | Weighted fair queue | Sort queue by tenant tier. Free tier at 0.5x weight, paid at 1.0x. |
| Oct 1 | Load generator multi-tenant test | Fire N free-tier requests, M paid-tier requests concurrently. Measure latency per tier. |
| Oct 4 | Fairness validation | Prove: paid tier p99 latency doesn't increase when free tier floods. Measure cost (if any). |
| Oct 6 | Token accounting | Track tokens used per request, deduct from tenant budget, validate accounting. |
| Oct 8 | ✅ Phase 3 complete | Fairness is proven with measurements. Understand the tradeoff cost (if lock contention exists). |

### Rust Learning Focus:
- HashMap and custom key/value types
- Atomic operations (Arc<AtomicU64>) for lock-free counters
- Enum types for TenantTier
- Pattern matching
- Interior mutability (Mutex vs. AtomicU64 tradeoff)
- Option/Result error propagation

---

## Phase 4: Reliability + Load Testing
**Duration**: Oct 9 – Oct 22 (2 weeks)  
**Goal**: Circuit breaking, health checks, full load suite, flamegraph profiling.

### Milestones:
| Date | Milestone | Success Criteria |
|------|-----------|-----------------|
| Oct 9 | Health probe task | Background task pings backends every 5s, tracks health status. |
| Oct 11 | Circuit breaker | Track error rate. Mark unhealthy after N consecutive errors. |
| Oct 13 | Retry logic (with no-retry-after-stream rule) | Retry if no tokens emitted. Don't retry if stream started. |
| Oct 15 | Load generator v2 (full harness) | Configurable concurrency (10, 50, 100, 500). Run at each level, collect metrics. |
| Oct 17 | Flamegraph profiling | Build with `--release`, profile with flamegraph, identify bottlenecks. |
| Oct 19 | Before/after optimization | Find bottleneck (lock contention? allocation?), optimize, measure 10%+ improvement. |
| Oct 22 | ✅ Phase 4 complete | Latency histograms at 10x/50x/100x concurrency. Flamegraph diffs show improvements. Graceful degradation proven. |

### Rust Learning Focus:
- Background tasks and coordination (channels, Arc sharing)
- Error types (custom errors with thiserror)
- Performance profiling (understanding flamegraph output)
- Optimization patterns (reduce allocations, lock contention)
- Benchmarking mindset

---

## Phase 5: Writeups + Polish
**Duration**: Oct 23 – Nov 2 (ongoing)  
**Goal**: Document findings, write defensible narratives.

### Milestones:
| Date | Milestone | Success Criteria |
|------|-----------|-----------------|
| Oct 25 | Writeup 1: "What Surprised Me" | Batching curves, fairness costs, tail latency insights. 2000+ words. |
| Oct 28 | Writeup 2: "Failure Modes" | How does it degrade? Circuit breaking, partial streams, graceful shutdown. 1500+ words. |
| Nov 1 | README + latency table | Someone clones repo, reads README, understands what they're looking at. Table shows numbers. |
| Nov 2 | ✅ Ship | Push to GitHub, share with YC contacts. |

### Documentation Focus:
- Clear explanation of every design choice
- Data-backed claims (always include numbers)
- Honest about tradeoffs (nothing is free)
- Interview-ready narratives

---

## Weekly Check-ins

Every Friday (Sep 5, 12, 19, 26, Oct 3, 10, 17, 24, 31):
- [ ] What shipped this week?
- [ ] What surprised you?
- [ ] What Rust concepts did you learn (or struggle with)?
- [ ] Measurements on the wall (latency curves, throughput, etc.)
- [ ] Is timeline still realistic?

---

## Hard Stops (Don't Add Scope)
- ❌ Postgres persistence
- ❌ Web dashboards
- ❌ Multi-region federation
- ❌ Custom model support
- ❌ Advanced analytics

If a task looks like it needs one of these, it's out of scope. Cut it and move on.

---

## Rust Learning Pacing

### Week 1–2 (Proxy):
- Basic async/await (Tokio)
- Structs, impl blocks, trait bounds
- Error handling (`?` operator)

### Week 3–4 (Scheduler):
- Channels (crossbeam MPMC)
- Arc + Mutex (shared mutable state)
- Lifetimes in structs and functions
- Trait objects (if needed)

### Week 5–6 (Multi-tenancy):
- Atomic operations (Arc<AtomicU64>)
- Interior mutability patterns
- Custom error types

### Week 7–8 (Reliability):
- Performance profiling (flamegraph)
- Unsafe code (justified use only)
- Optimization patterns

---

## Success Looks Like

At the end:
- [ ] System runs end-to-end (`docker compose up` → load generator → metrics)
- [ ] Code is idiomatic Rust (`cargo fmt` + `cargo clippy -- -D warnings` pass)
- [ ] No `unwrap()` / `expect()` in request paths
- [ ] Latency curves are reproducible (±5%)
- [ ] Every design choice has supporting data
- [ ] Writeups are interview-ready
- [ ] Commit history tells a story
- [ ] You can defend every line of code under questioning
