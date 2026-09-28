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
//! Two load shapes:
//!   closed loop (default) — `--concurrency` workers, each sends its next
//!     request as soon as the previous one finishes. Offered load adapts to
//!     how fast the gateway answers.
//!   open loop (`--rps N`) — requests are *scheduled* at a fixed rate and
//!     latency is measured from the scheduled time, not the actual send time.
//!     If every worker is busy when a request is due, the lateness counts
//!     against the gateway. This avoids "coordinated omission": a closed-loop
//!     client that slows down whenever the server does quietly stops sending
//!     exactly when things are worst, and under-reports the tail.
//!
//! Example:
//!   cargo run --release --bin loadgen -- --stream --concurrency 20 --requests 500

use std::time::{Duration, Instant};

use anyhow::{anyhow, Context};
use clap::Parser;
use futures_util::StreamExt;
use hdrhistogram::Histogram;
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// `#[derive(Parser)]` makes clap generate the argument parser from this
/// struct: each field becomes a `--flag`, the doc comment becomes its help
/// text, and the field's type decides how the string gets parsed.
#[derive(Parser, Debug, Clone)]
#[command(about = "Load generator for oxideGate")]
struct Args {
    /// Gateway base URL.
    #[arg(long, default_value = "http://127.0.0.1:8000")]
    url: String,

    /// Maximum requests in flight at once (number of workers).
    #[arg(long, default_value_t = 10)]
    concurrency: usize,

    /// Total requests to measure (not counting warmup).
    #[arg(long, default_value_t = 200)]
    requests: usize,

    /// Model name to put in the request body.
    #[arg(long, default_value = "mock-model")]
    model: String,

    /// Ask for a streamed reply, so TTFT is meaningful.
    #[arg(long)]
    stream: bool,

    /// Throwaway requests before measuring, sent through the same worker
    /// pool so that every pooled connection gets opened before the clock
    /// starts, not just the first one.
    #[arg(long, default_value_t = 20)]
    warmup: usize,

    /// Open-loop mode: schedule requests at this fixed rate (req/s) instead
    /// of sending back-to-back. Still capped at --concurrency in flight.
    #[arg(long)]
    rps: Option<f64>,

    /// Label for the markdown result row, e.g. "window=10ms".
    #[arg(long, default_value = "run")]
    label: String,

    /// Per-request timeout in seconds.
    #[arg(long, default_value_t = 120)]
    timeout_secs: u64,

    /// Sent as `Authorization: Bearer <key>`; selects the tenant.
    #[arg(long)]
    api_key: Option<String>,
}

/// How a single request ended. Failures are split by kind because they
/// mean different things: a 429 is the gateway correctly shedding load, a
/// 5xx is something broken, a transport error is the connection dying.
enum Outcome {
    Ok {
        ttft: Option<Duration>,
        e2e: Duration,
    },
    /// 429 Too Many Requests — admission control said no.
    Rejected,
    /// Any other non-2xx status.
    HttpError,
    /// Connection refused/reset, timeout, or a stream that broke midway.
    Transport,
    /// Stream ended without error but never sent `data: [DONE]`.
    Truncated,
}

/// Everything one worker collected.
///
/// Each worker owns its own `Stats` and returns it when it finishes; main
/// merges them at the end. No sharing while the test runs means no Mutex,
/// and so nothing on the measurement path that could contend or panic.
struct Stats {
    ttft: Histogram<u64>,
    e2e: Histogram<u64>,
    ok: u64,
    rejected: u64,
    http_error: u64,
    transport: u64,
    truncated: u64,
}

/// Histograms record microseconds, from 1µs up to 5 minutes, at 3
/// significant figures (i.e. within 0.1% of the true value).
const HIST_MAX_US: u64 = 300_000_000;

impl Stats {
    fn new() -> anyhow::Result<Self> {
        let hist = || {
            Histogram::new_with_bounds(1, HIST_MAX_US, 3)
                .map_err(|e| anyhow!("histogram bounds: {e:?}"))
        };
        Ok(Stats {
            ttft: hist()?,
            e2e: hist()?,
            ok: 0,
            rejected: 0,
            http_error: 0,
            transport: 0,
            truncated: 0,
        })
    }

    fn record(&mut self, outcome: Outcome) {
        match outcome {
            Outcome::Ok { ttft, e2e } => {
                self.ok += 1;
                // saturating_record clamps anything above HIST_MAX_US
                // instead of returning an error.
                self.e2e.saturating_record(micros(e2e));
                if let Some(t) = ttft {
                    self.ttft.saturating_record(micros(t));
                }
            }
            Outcome::Rejected => self.rejected += 1,
            Outcome::HttpError => self.http_error += 1,
            Outcome::Transport => self.transport += 1,
            Outcome::Truncated => self.truncated += 1,
        }
    }

    fn merge(&mut self, other: &Stats) -> anyhow::Result<()> {
        self.ttft
            .add(&other.ttft)
            .map_err(|e| anyhow!("merge ttft: {e:?}"))?;
        self.e2e
            .add(&other.e2e)
            .map_err(|e| anyhow!("merge e2e: {e:?}"))?;
        self.ok += other.ok;
        self.rejected += other.rejected;
        self.http_error += other.http_error;
        self.transport += other.transport;
        self.truncated += other.truncated;
        Ok(())
    }

    fn failed(&self) -> u64 {
        self.rejected + self.http_error + self.transport + self.truncated
    }
}

fn micros(d: Duration) -> u64 {
    u64::try_from(d.as_micros()).unwrap_or(u64::MAX)
}

/// Percentile in milliseconds, one decimal. Integer milliseconds would hide
/// exactly the 5ms-scale effects the experiments are looking for.
fn pct_ms(h: &Histogram<u64>, q: f64) -> f64 {
    h.value_at_quantile(q) as f64 / 1000.0
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    if args.concurrency == 0 {
        return Err(anyhow!("--concurrency must be at least 1"));
    }
    if let Some(r) = args.rps {
        if !(r.is_finite() && r > 0.0) {
            return Err(anyhow!("--rps must be a positive number"));
        }
    }

    // reqwest::Client holds a connection pool internally and is cheap to
    // clone: every clone is a handle to the same pool (it is an Arc inside).
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(args.timeout_secs))
        .pool_max_idle_per_host(args.concurrency)
        .build()
        .context("build HTTP client")?;

    println!(
        "target {} · {} requests · {} concurrent · stream={} · {}",
        args.url,
        args.requests,
        args.concurrency,
        args.stream,
        match args.rps {
            Some(r) => format!("open loop @ {r} req/s"),
            None => "closed loop".to_string(),
        }
    );

    // Warmup uses the same pool of workers as the real run, so all
    // `concurrency` connections get their TCP handshake done now. A
    // sequential warmup would open exactly one connection and leave the
    // other N-1 to be opened inside the measured window, inflating the tail.
    // Warmup is always closed loop: its only job is to open connections.
    if args.warmup > 0 {
        let warm = run_phase(&client, &args, args.warmup, None).await?;
        println!(
            "warmup done ({} requests discarded, {} failed)",
            args.warmup,
            warm.failed()
        );
    }

    let started = Instant::now();
    let stats = run_phase(&client, &args, args.requests, args.rps).await?;
    let elapsed = started.elapsed();

    report(&args, &stats, elapsed);

    if stats.ok == 0 {
        return Err(anyhow!("every request failed — is the gateway up?"));
    }
    Ok(())
}

/// Send `total` requests through `concurrency` workers and return the merged
/// stats.
///
/// Worker-pool pattern: rather than pre-assigning "worker 0 does requests
/// 0-49", every worker loops claiming the next request number from a shared
/// atomic counter. A worker that gets fast responses simply claims more, so
/// nobody sits idle while there is work left.
async fn run_phase(
    client: &reqwest::Client,
    args: &Args,
    total: usize,
    rps: Option<f64>,
) -> anyhow::Result<Stats> {
    // `fetch_add(1)` atomically returns the old value and stores old + 1,
    // so no two workers can ever claim the same number. It is lock-free.
    //
    // Arc because tokio::spawn needs the task to own ('static) everything it
    // touches; each worker gets its own Arc handle to the one counter.
    let next = Arc::new(AtomicUsize::new(0));

    // For open loop: request i is due at `phase_start + i / rps`.
    let phase_start = tokio::time::Instant::now();
    let interval = rps.map(|r| Duration::from_secs_f64(1.0 / r));

    let mut handles = Vec::with_capacity(args.concurrency);
    for _ in 0..args.concurrency {
        let client = client.clone();
        let args = args.clone();
        let next = Arc::clone(&next);

        handles.push(tokio::spawn(async move {
            let mut stats = Stats::new()?;
            loop {
                // Relaxed ordering is enough: we need each number handed out
                // exactly once, not ordering relative to other memory.
                let i = next.fetch_add(1, Ordering::Relaxed);
                if i >= total {
                    break;
                }

                // Closed loop: measure from now. Open loop: wait for this
                // request's scheduled slot, then measure from the *slot*,
                // so any time spent waiting for a free worker is charged.
                let measure_from = match interval {
                    None => Instant::now(),
                    Some(iv) => {
                        let due = phase_start + iv.mul_f64(i as f64);
                        tokio::time::sleep_until(due).await;
                        due.into_std()
                    }
                };

                let outcome = one_request(&client, &args, measure_from).await;
                stats.record(outcome);
            }
            Ok::<Stats, anyhow::Error>(stats)
        }));
    }

    let mut merged = Stats::new()?;
    for h in handles {
        // Outer `?`: the task panicked. Inner `?`: the task returned Err.
        let worker_stats = h.await.context("worker task panicked")??;
        merged.merge(&worker_stats)?;
    }
    Ok(merged)
}

/// One request, timed from `started`.
///
/// Returns an Outcome rather than a Result: a failed request is data for
/// the report, not an error that should stop the run.
async fn one_request(client: &reqwest::Client, args: &Args, started: Instant) -> Outcome {
    let body = json!({
        "model": args.model,
        "stream": args.stream,
        "messages": [{"role": "user", "content": "hello"}],
    });

    let mut request = client
        .post(format!("{}/v1/chat/completions", args.url))
        .json(&body);
    if let Some(key) = &args.api_key {
        request = request.bearer_auth(key);
    }
    let resp = match request.send().await {
        Ok(r) => r,
        Err(_) => return Outcome::Transport,
    };

    // send() only fails on transport problems. A 429 or 502 still comes
    // back as Ok(resp), so the status has to be checked explicitly, or
    // every rejection would be recorded as a very fast success.
    let status = resp.status();
    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        // Read the (small) body so the connection goes back to the pool.
        let _ = resp.bytes().await;
        return Outcome::Rejected;
    }
    if !status.is_success() {
        let _ = resp.bytes().await;
        return Outcome::HttpError;
    }

    if !args.stream {
        return match resp.bytes().await {
            Ok(_) => Outcome::Ok {
                ttft: None,
                e2e: started.elapsed(),
            },
            Err(_) => Outcome::Transport,
        };
    }

    // Streaming: drain every chunk, even after TTFT is captured. Dropping
    // the stream early closes the connection mid-response; the gateway
    // sees that as a client hang-up, and we would be measuring the load
    // generator giving up rather than the gateway finishing its work.
    let mut chunks = resp.bytes_stream();
    let mut ttft = None;
    // The last few bytes seen, to check the stream ended with [DONE]
    // rather than just stopping. Chunk boundaries are arbitrary, so the
    // marker can be split across two chunks; keeping a short tail handles it.
    let mut tail: Vec<u8> = Vec::with_capacity(64);

    while let Some(chunk) = chunks.next().await {
        let bytes = match chunk {
            Ok(b) => b,
            Err(_) => return Outcome::Transport,
        };
        if ttft.is_none() && !bytes.is_empty() {
            ttft = Some(started.elapsed());
        }
        tail.extend_from_slice(&bytes);
        if tail.len() > 64 {
            tail.drain(..tail.len() - 64);
        }
    }
    let e2e = started.elapsed();

    if !tail.windows(6).any(|w| w == b"[DONE]") {
        return Outcome::Truncated;
    }
    Outcome::Ok { ttft, e2e }
}

fn report(args: &Args, s: &Stats, elapsed: Duration) {
    let throughput = s.ok as f64 / elapsed.as_secs_f64();

    println!(
        "\n── results: {} ──────────────────────────────────────",
        args.label
    );
    println!(
        "requests    {} ok · {} failed ({} rejected 429 · {} http error · {} transport · {} truncated)",
        s.ok,
        s.failed(),
        s.rejected,
        s.http_error,
        s.transport,
        s.truncated
    );
    println!("wall time   {:.2} s", elapsed.as_secs_f64());
    println!("throughput  {throughput:.2} req/s");
    println!(
        "            {:>9} {:>9} {:>9} {:>9}",
        "p50", "p95", "p99", "max"
    );
    if args.stream {
        println!(
            "TTFT (ms)   {:>9.1} {:>9.1} {:>9.1} {:>9.1}",
            pct_ms(&s.ttft, 0.50),
            pct_ms(&s.ttft, 0.95),
            pct_ms(&s.ttft, 0.99),
            s.ttft.max() as f64 / 1000.0
        );
    }
    println!(
        "E2E (ms)    {:>9.1} {:>9.1} {:>9.1} {:>9.1}",
        pct_ms(&s.e2e, 0.50),
        pct_ms(&s.e2e, 0.95),
        pct_ms(&s.e2e, 0.99),
        s.e2e.max() as f64 / 1000.0
    );

    // One markdown row, so several runs paste straight into a results table.
    // TTFT is left blank for buffered runs rather than printed as 0: a zero
    // would read as "instant", which is a lie, not a measurement.
    let ttft_cols = if args.stream {
        format!(
            "{:.1} | {:.1} | {:.1}",
            pct_ms(&s.ttft, 0.50),
            pct_ms(&s.ttft, 0.95),
            pct_ms(&s.ttft, 0.99)
        )
    } else {
        "– | – | –".to_string()
    };
    println!("\n| label | ok | failed | req/s | TTFT p50 | TTFT p95 | TTFT p99 | E2E p50 | E2E p95 | E2E p99 |");
    println!("|---|---|---|---|---|---|---|---|---|---|");
    println!(
        "| {} | {} | {} | {:.2} | {} | {:.1} | {:.1} | {:.1} |",
        args.label,
        s.ok,
        s.failed(),
        throughput,
        ttft_cols,
        pct_ms(&s.e2e, 0.50),
        pct_ms(&s.e2e, 0.95),
        pct_ms(&s.e2e, 0.99)
    );
}
