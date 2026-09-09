//! Load generator. Fires many requests at the gateway and reports the
//! latency distribution.
//!
//! Averages are useless here. If 99 requests take 100ms and one takes 10s,
//! the average is ~200ms — a number describing nobody's experience. p99 is
//! what tells you how bad your worst experiences are, and it is what real
//! gateways are judged on, so we keep every sample and report percentiles.
//!
//! Two clocks per request:
//!   TTFT — time to the FIRST byte of the answer. What the user perceives.
//!   E2E  — time to the LAST byte. What the machine actually spent.
//! Only streaming has a meaningful TTFT; buffered requests get one number.
//!
//! ⚠️ Delete this attribute once the TODOs are filled.
#![allow(unused_variables, unused_mut, dead_code, unused_imports)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use clap::Parser;
use futures_util::StreamExt;
use hdrhistogram::Histogram;
use serde_json::json;

#[derive(Parser, Debug)]
#[command(about = "Load generator for oxideGate")]
struct Args {
    /// Gateway base URL.
    #[arg(long, default_value = "http://127.0.0.1:8000")]
    url: String,

    /// How many requests to keep in flight at once.
    #[arg(long, default_value_t = 10)]
    concurrency: usize,

    /// Total requests to send (not counting warmup).
    #[arg(long, default_value_t = 200)]
    requests: usize,

    /// Model name to put in the request body.
    #[arg(long, default_value = "mock-model")]
    model: String,

    /// Ask for a streamed reply, so TTFT is meaningful.
    #[arg(long)]
    stream: bool,

    /// Throwaway requests before measuring. The first few requests pay for
    /// TCP setup and lazy initialisation and would skew the tail.
    #[arg(long, default_value_t = 20)]
    warmup: usize,
}

/// Everything the workers collect. Behind one Mutex: locking briefly per
/// request is far cheaper than the request itself, so contention here does
/// not distort the numbers.
struct Stats {
    ttft: Histogram<u64>,
    e2e: Histogram<u64>,
    ok: u64,
    failed: u64,
}

impl Stats {
    fn new() -> Self {
        // Range 1µs .. 60s, 3 significant figures.
        Stats {
            ttft: Histogram::new_with_bounds(1, 60_000_000, 3).expect("histogram bounds"),
            e2e: Histogram::new_with_bounds(1, 60_000_000, 3).expect("histogram bounds"),
            ok: 0,
            failed: 0,
        }
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(120))
        .build()
        .expect("build client");

    println!(
        "target {} · {} requests · {} concurrent · stream={}",
        args.url, args.requests, args.concurrency, args.stream
    );

    // ┌── TODO 1 ── warmup ──────────────────────────────────────────────┐
    // │ Fire args.warmup requests and throw the results away.            │
    // │ Sequential is fine; it is only a handful.                        │
    // │ Without this the first samples include connection setup and      │
    // │ land in the tail, making p99 a lie.                              │
    // └──────────────────────────────────────────────────────────────────┘

    // ┌── TODO 2 ── run the load ────────────────────────────────────────┐
    // │ The worker-pool shape:                                           │
    // │                                                                  │
    // │   let stats = Arc::new(Mutex::new(Stats::new()));                │
    // │   let counter = Arc::new(AtomicUsize::new(0));                   │
    // │   let started = Instant::now();                                  │
    // │                                                                  │
    // │   spawn `args.concurrency` tasks. Each one loops:                │
    // │     - claim a slot: counter.fetch_add(1, Ordering::Relaxed)      │
    // │     - if the claimed number >= args.requests, break              │
    // │     - call one_request(...)                                      │
    // │     - lock stats and record the result                           │
    // │                                                                  │
    // │ fetch_add returns the value BEFORE adding, and is atomic, so no  │
    // │ two workers ever claim the same slot. This is the lock-free      │
    // │ counter idea from your Phase 3 notes, arriving early.            │
    // │                                                                  │
    // │ Collect the JoinHandles and await them all, or use               │
    // │ tokio::task::JoinSet. Then take `started.elapsed()` for          │
    // │ throughput.                                                      │
    // │                                                                  │
    // │ Everything shared needs Arc::clone before it moves into a task — │
    // │ the ownership rule from lesson 2, in real code.                  │
    // └──────────────────────────────────────────────────────────────────┘

    // ┌── TODO 3 ── report ──────────────────────────────────────────────┐
    // │ Print, in a shape you can paste into the writeup:                │
    // │                                                                  │
    // │   requests   200 ok, 0 failed                                    │
    // │   throughput 47.3 req/s                                          │
    // │   TTFT       p50 204ms  p95 219ms  p99 241ms                     │
    // │   E2E        p50 312ms  p95 338ms  p99 402ms                     │
    // │                                                                  │
    // │ Percentiles: h.value_at_quantile(0.50) etc, in microseconds.     │
    // │ Throughput: ok as f64 / elapsed.as_secs_f64().                   │
    // │ Skip the TTFT line entirely when !args.stream — printing zeros   │
    // │ there would invite a false comparison.                           │
    // └──────────────────────────────────────────────────────────────────┘
}

/// One request. Returns (time to first byte, time to last byte).
/// TTFT is None for buffered replies, where it has no meaning.
async fn one_request(
    client: &reqwest::Client,
    url: &str,
    model: &str,
    stream: bool,
) -> Result<(Option<Duration>, Duration), reqwest::Error> {
    let body = json!({
        "model": model,
        "stream": stream,
        "messages": [{"role": "user", "content": "hello"}],
    });

    let started = Instant::now();

    // ┌── TODO 4 ── send it and time it ─────────────────────────────────┐
    // │ POST {url}/v1/chat/completions with that body.                   │
    // │                                                                  │
    // │ Buffered (stream == false):                                      │
    // │   read the whole body — resp.bytes().await? — then return        │
    // │   (None, started.elapsed()).                                     │
    // │                                                                  │
    // │ Streamed (stream == true):                                       │
    // │   let mut s = resp.bytes_stream();                               │
    // │   while let Some(chunk) = s.next().await { ... }                 │
    // │   Record started.elapsed() on the FIRST chunk -> that is TTFT.   │
    // │   Keep draining to the end -> that is E2E.                       │
    // │                                                                  │
    // │ You MUST drain the whole stream even though you only need the    │
    // │ timings. Dropping it early kills the connection mid-response,    │
    // │ which the gateway sees as a client hang-up — and you would be    │
    // │ measuring your own load generator giving up.                     │
    // └──────────────────────────────────────────────────────────────────┘

    todo!("one_request")
}
