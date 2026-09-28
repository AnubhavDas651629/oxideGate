//! Integration tests: a real gateway on a real socket, talking to a stub
//! backend on another. Nothing is mocked at the HTTP layer, so these exercise
//! routing, extraction, serialisation and error mapping the way a client does.

use axum::{routing::post, Json, Router};
use futures_util::StreamExt;
use oxidegate::scheduler::{self, SchedulerConfig};
use oxidegate::tenants::{TenantConfig, Tenants, Tier};
use oxidegate::{build_router, telemetry, AppState, Backend};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

/// The metrics recorder is process-wide and can only be installed once, but
/// every test wants a gateway. Install on first use and hand out clones.
fn metrics_handle() -> metrics_exporter_prometheus::PrometheusHandle {
    static HANDLE: OnceLock<metrics_exporter_prometheus::PrometheusHandle> = OnceLock::new();
    HANDLE
        .get_or_init(|| telemetry::install().expect("install recorder"))
        .clone()
}

/// Spawn a stub backend. Returns its base URL.
async fn spawn_backend(router: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    format!("http://{addr}/v1")
}

/// Spawn the gateway pointed at `backend_url`, batching off. Returns its URL.
async fn spawn_gateway(backend_url: String) -> String {
    spawn_gateway_with(backend_url, Duration::ZERO, 100).await
}

/// Spawn the gateway with an explicit batching window and queue depth.
async fn spawn_gateway_with(backend_url: String, window: Duration, queue_depth: usize) -> String {
    spawn_gateway_cfg(
        backend_url,
        SchedulerConfig {
            queue_depth,
            window,
            max_inflight: 0,
            fair: true,
        },
    )
    .await
}

/// Spawn the gateway with a full scheduler config, single anonymous tenant.
async fn spawn_gateway_cfg(backend_url: String, cfg: SchedulerConfig) -> String {
    spawn_gateway_full(backend_url, cfg, Tenants::anonymous()).await
}

/// Spawn the gateway with a scheduler config and a tenant registry.
async fn spawn_gateway_full(backend_url: String, cfg: SchedulerConfig, tenants: Tenants) -> String {
    let backend = Arc::new(Backend {
        http: reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap(),
        url: backend_url,
    });
    let scheduler = scheduler::spawn(backend, cfg);
    let state = Arc::new(AppState {
        scheduler,
        metrics: metrics_handle(),
        tenants,
        default_max_tokens: 256,
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, build_router(state)).await.unwrap();
    });
    format!("http://{addr}")
}

fn completion_body(content: &str) -> Value {
    json!({
        "id": "chatcmpl-stub",
        "object": "chat.completion",
        "created": 1,
        "model": "stub-model",
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": content},
            "finish_reason": "stop"
        }],
        "usage": {"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3}
    })
}

fn request_body() -> Value {
    json!({"model": "stub-model", "messages": [{"role": "user", "content": "hi"}]})
}

#[tokio::test]
async fn health_returns_ok() {
    let gw = spawn_gateway("http://127.0.0.1:1/v1".to_string()).await;
    let body: Value = reqwest::get(format!("{gw}/health"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["status"], "ok");
}

#[tokio::test]
async fn forwards_request_and_returns_backend_reply() {
    let backend = spawn_backend(Router::new().route(
        "/v1/chat/completions",
        post(|Json(v): Json<Value>| async move {
            // The gateway must pass the client's model through untouched.
            assert_eq!(v["model"], "stub-model");
            Json(completion_body("hello from stub"))
        }),
    ))
    .await;
    let gw = spawn_gateway(backend).await;

    let body: Value = reqwest::Client::new()
        .post(format!("{gw}/v1/chat/completions"))
        .json(&request_body())
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(body["choices"][0]["message"]["content"], "hello from stub");
    assert_eq!(body["usage"]["total_tokens"], 3);
}

#[tokio::test]
async fn malformed_request_is_rejected_before_the_backend_is_called() {
    // If the extractor lets a bad body through, this flips and the test fails.
    let called = Arc::new(AtomicBool::new(false));
    let seen = called.clone();

    let backend = spawn_backend(Router::new().route(
        "/v1/chat/completions",
        post(move |Json(_): Json<Value>| {
            let seen = seen.clone();
            async move {
                seen.store(true, Ordering::SeqCst);
                Json(completion_body("should not happen"))
            }
        }),
    ))
    .await;
    let gw = spawn_gateway(backend).await;

    // No `messages` field.
    let resp = reqwest::Client::new()
        .post(format!("{gw}/v1/chat/completions"))
        .json(&json!({"model": "stub-model"}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 422);
    assert!(!called.load(Ordering::SeqCst), "backend must not be called");
}

#[tokio::test]
async fn backend_error_becomes_502() {
    let backend = spawn_backend(Router::new().route(
        "/v1/chat/completions",
        post(|| async {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "upstream boom",
            )
        }),
    ))
    .await;
    let gw = spawn_gateway(backend).await;

    let resp = reqwest::Client::new()
        .post(format!("{gw}/v1/chat/completions"))
        .json(&request_body())
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 502);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["code"], "backend_error");
}

#[tokio::test]
async fn unreachable_backend_becomes_502() {
    // Port 1 is reserved and nothing listens there.
    let gw = spawn_gateway("http://127.0.0.1:1/v1".to_string()).await;

    let resp = reqwest::Client::new()
        .post(format!("{gw}/v1/chat/completions"))
        .json(&request_body())
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 502);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["code"], "backend_unreachable");
}

#[tokio::test]
async fn streaming_passes_sse_frames_through_unchanged() {
    let backend = spawn_backend(Router::new().route(
        "/v1/chat/completions",
        post(|Json(v): Json<Value>| async move {
            // The gateway must forward the stream flag, or the backend would
            // reply buffered and streaming would silently never be exercised.
            assert_eq!(v["stream"], true);
            (
                [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
                "data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n\n\
                 data: {\"choices\":[{\"delta\":{\"content\":\"b\"}}]}\n\n\
                 data: [DONE]\n\n",
            )
        }),
    ))
    .await;
    let gw = spawn_gateway(backend).await;

    let mut body = request_body();
    body["stream"] = json!(true);

    let resp = reqwest::Client::new()
        .post(format!("{gw}/v1/chat/completions"))
        .json(&body)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers()
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()),
        Some("text/event-stream")
    );

    let text = resp.text().await.unwrap();
    assert!(text.contains(r#""content":"a""#), "got: {text}");
    assert!(text.contains(r#""content":"b""#), "got: {text}");
    assert!(text.trim().ends_with("data: [DONE]"), "got: {text}");
}

#[tokio::test]
async fn metrics_endpoint_reports_request_counts() {
    let backend = spawn_backend(Router::new().route(
        "/v1/chat/completions",
        post(|| async { Json(completion_body("counted")) }),
    ))
    .await;
    let gw = spawn_gateway(backend).await;

    reqwest::Client::new()
        .post(format!("{gw}/v1/chat/completions"))
        .json(&request_body())
        .send()
        .await
        .unwrap();

    let text = reqwest::get(format!("{gw}/metrics"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    assert!(
        text.contains(telemetry::REQUESTS_TOTAL),
        "missing {} in:\n{text}",
        telemetry::REQUESTS_TOTAL
    );
    assert!(
        text.contains(r#"outcome="ok""#),
        "missing ok outcome:\n{text}"
    );
}

// ── scheduler ────────────────────────────────────────────────────────────

/// A backend that replies instantly, so any measured delay is ours.
async fn spawn_instant_backend() -> String {
    spawn_backend(Router::new().route(
        "/v1/chat/completions",
        post(|| async { Json(completion_body("instant")) }),
    ))
    .await
}

#[tokio::test]
async fn window_zero_does_not_delay_a_request() {
    let gw = spawn_gateway_with(spawn_instant_backend().await, Duration::ZERO, 100).await;

    let start = std::time::Instant::now();
    let resp = reqwest::Client::new()
        .post(format!("{gw}/v1/chat/completions"))
        .json(&request_body())
        .send()
        .await
        .unwrap();
    let elapsed = start.elapsed();

    assert_eq!(resp.status(), 200);
    assert!(
        elapsed < Duration::from_millis(150),
        "control case should be fast, took {elapsed:?}"
    );
}

/// The heart of Experiment 1, as a test: a lone request still pays the full
/// window, because the scheduler waits for companions that never arrive.
#[tokio::test]
async fn batching_window_delays_a_lone_request_by_the_full_window() {
    let window = Duration::from_millis(200);
    let gw = spawn_gateway_with(spawn_instant_backend().await, window, 100).await;

    let start = std::time::Instant::now();
    let resp = reqwest::Client::new()
        .post(format!("{gw}/v1/chat/completions"))
        .json(&request_body())
        .send()
        .await
        .unwrap();
    let elapsed = start.elapsed();

    assert_eq!(resp.status(), 200);
    assert!(
        elapsed >= window,
        "a lone request must wait out the whole window; took {elapsed:?}, window {window:?}"
    );
}

/// Streaming has to survive the trip through the queue. This is the
/// regression that would otherwise be found by hand, late.
#[tokio::test]
async fn streaming_still_works_through_the_scheduler() {
    let backend = spawn_backend(Router::new().route(
        "/v1/chat/completions",
        post(|| async {
            (
                [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
                "data: {\"choices\":[{\"delta\":{\"content\":\"q\"}}]}\n\ndata: [DONE]\n\n",
            )
        }),
    ))
    .await;
    let gw = spawn_gateway_with(backend, Duration::from_millis(20), 100).await;

    let mut body = request_body();
    body["stream"] = json!(true);

    let resp = reqwest::Client::new()
        .post(format!("{gw}/v1/chat/completions"))
        .json(&body)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let text = resp.text().await.unwrap();
    assert!(text.contains(r#""content":"q""#), "got: {text}");
    assert!(text.trim().ends_with("data: [DONE]"), "got: {text}");
}

/// The queue-wait histogram must actually be populated, or Experiment 1 has
/// no data to plot.
#[tokio::test]
async fn queue_wait_is_recorded() {
    let gw = spawn_gateway_with(
        spawn_instant_backend().await,
        Duration::from_millis(30),
        100,
    )
    .await;

    reqwest::Client::new()
        .post(format!("{gw}/v1/chat/completions"))
        .json(&request_body())
        .send()
        .await
        .unwrap();

    let text = reqwest::get(format!("{gw}/metrics"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    assert!(
        text.contains(telemetry::QUEUE_WAIT),
        "missing {} in:\n{text}",
        telemetry::QUEUE_WAIT
    );
    assert!(
        text.contains(telemetry::BATCH_SIZE),
        "missing {}",
        telemetry::BATCH_SIZE
    );
}

// ── in-flight limiter + admission control (Block B) ──────────────────────

/// Counts what the backend actually sees: calls, and the peak number of
/// requests it was working on at the same moment.
#[derive(Default)]
struct BackendLoad {
    current: AtomicUsize,
    peak: AtomicUsize,
    calls: AtomicUsize,
}

impl BackendLoad {
    fn enter(&self) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let now = self.current.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
    }
    fn leave(&self) {
        self.current.fetch_sub(1, Ordering::SeqCst);
    }
}

/// A buffered backend that takes `delay` per request and records its load.
async fn spawn_slow_backend(delay: Duration) -> (String, Arc<BackendLoad>) {
    let load = Arc::new(BackendLoad::default());
    let seen = load.clone();
    let url = spawn_backend(Router::new().route(
        "/v1/chat/completions",
        post(move || {
            let seen = seen.clone();
            async move {
                seen.enter();
                tokio::time::sleep(delay).await;
                seen.leave();
                Json(completion_body("slow"))
            }
        }),
    ))
    .await;
    (url, load)
}

fn limited(queue_depth: usize, max_inflight: usize) -> SchedulerConfig {
    SchedulerConfig {
        queue_depth,
        window: Duration::ZERO,
        max_inflight,
        fair: true,
    }
}

/// Fire `n` requests at once; return their status codes.
async fn burst(gw: &str, n: usize) -> Vec<u16> {
    let client = reqwest::Client::new();
    let reqs = (0..n).map(|_| {
        let client = client.clone();
        let url = format!("{gw}/v1/chat/completions");
        async move {
            client
                .post(url)
                .json(&request_body())
                .send()
                .await
                .unwrap()
                .status()
                .as_u16()
        }
    });
    futures_util::future::join_all(reqs).await
}

/// 30 concurrent clients, limit 3: the backend must never see more than 3.
#[tokio::test]
async fn limiter_caps_backend_concurrency_under_load() {
    let (backend, load) = spawn_slow_backend(Duration::from_millis(50)).await;
    let gw = spawn_gateway_cfg(backend, limited(100, 3)).await;

    let codes = burst(&gw, 30).await;

    assert!(codes.iter().all(|&c| c == 200), "codes: {codes:?}");
    assert_eq!(load.calls.load(Ordering::SeqCst), 30);
    let peak = load.peak.load(Ordering::SeqCst);
    assert!(
        peak <= 3,
        "backend saw {peak} concurrent requests, limit is 3"
    );
    assert_eq!(
        peak, 3,
        "limiter should run the backend at its limit, not below"
    );
}

/// Fire `n` requests `gap` apart, each on its own task; return status codes
/// in send order. Spacing them out means the scheduler gets to run between
/// arrivals, so this measures sustained overload rather than a race
/// between a same-instant burst and the scheduler draining the queue.
async fn paced(gw: &str, n: usize, gap: Duration) -> Vec<u16> {
    let client = reqwest::Client::new();
    let mut tasks = Vec::new();
    for _ in 0..n {
        let client = client.clone();
        let url = format!("{gw}/v1/chat/completions");
        tasks.push(tokio::spawn(async move {
            client
                .post(url)
                .json(&request_body())
                .send()
                .await
                .unwrap()
                .status()
                .as_u16()
        }));
        tokio::time::sleep(gap).await;
    }
    let mut codes = Vec::new();
    for t in tasks {
        codes.push(t.await.unwrap());
    }
    codes
}

/// D5, closed. Sustained overload: 10 requests at 50 req/s against a
/// backend that serves ~3/s. With the limiter (1 in flight + 2 queued) the
/// gateway accepts exactly 3 and rejects the rest with 429.
///
/// Without the limiter the SAME load is never rejected: the scheduler
/// forwards everything instantly, the queue stays empty, and the overload
/// piles up inside the backend instead. That half is the bug D5 describes.
#[tokio::test]
async fn sustained_overload_is_rejected_only_with_the_limiter() {
    let gap = Duration::from_millis(20);

    let (backend, load) = spawn_slow_backend(Duration::from_millis(300)).await;
    let gw = spawn_gateway_cfg(backend, limited(2, 1)).await;
    let codes = paced(&gw, 10, gap).await;
    assert_eq!(
        codes,
        [200, 200, 200, 429, 429, 429, 429, 429, 429, 429],
        "1 in flight + 2 queued should be accepted, in arrival order"
    );
    assert_eq!(load.peak.load(Ordering::SeqCst), 1);

    let (backend, load) = spawn_slow_backend(Duration::from_millis(300)).await;
    let gw = spawn_gateway_cfg(backend, limited(2, 0)).await;
    let codes = paced(&gw, 10, gap).await;
    assert!(codes.iter().all(|&c| c == 200), "codes: {codes:?}");
    assert!(
        load.peak.load(Ordering::SeqCst) >= 8,
        "without a limiter the overload lands on the backend, peak was {}",
        load.peak.load(Ordering::SeqCst)
    );
}

#[tokio::test]
async fn rejection_is_a_429_with_queue_full_code() {
    let (backend, _) = spawn_slow_backend(Duration::from_millis(300)).await;
    let gw = spawn_gateway_cfg(backend, limited(1, 1)).await;

    let client = reqwest::Client::new();
    let reqs = (0..6).map(|_| {
        client
            .post(format!("{gw}/v1/chat/completions"))
            .json(&request_body())
            .send()
    });
    let resps = futures_util::future::join_all(reqs).await;

    let mut saw_429 = false;
    for resp in resps {
        let resp = resp.unwrap();
        if resp.status() == 429 {
            let body: Value = resp.json().await.unwrap();
            assert_eq!(body["error"]["code"], "queue_full");
            saw_429 = true;
        }
    }
    assert!(saw_429, "a burst of 6 against capacity 2 must be rejected");
}

/// A streamed reply occupies its backend slot until the LAST byte, not
/// until the headers arrive. With limit 1 and two streaming clients, the
/// second must not even get headers until the first stream has finished.
/// If the slot were released at headers, both would start immediately.
#[tokio::test]
async fn streaming_holds_its_slot_until_the_stream_ends() {
    let stream_time = Duration::from_millis(300);
    let backend = spawn_backend(Router::new().route(
        "/v1/chat/completions",
        post(move || async move {
            let frames = futures_util::stream::iter([false, true]).then(move |last| async move {
                if last {
                    tokio::time::sleep(stream_time).await;
                    Ok::<_, std::convert::Infallible>("data: [DONE]\n\n")
                } else {
                    Ok("data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n")
                }
            });
            (
                [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
                axum::body::Body::from_stream(frames),
            )
        }),
    ))
    .await;
    let gw = spawn_gateway_cfg(backend, limited(10, 1)).await;

    let mut body = request_body();
    body["stream"] = json!(true);
    let client = reqwest::Client::new();
    let start = std::time::Instant::now();

    let one = |client: reqwest::Client, body: Value| {
        let url = format!("{gw}/v1/chat/completions");
        async move {
            let resp = client.post(url).json(&body).send().await.unwrap();
            let headers_at = start.elapsed();
            let text = resp.text().await.unwrap();
            assert!(text.trim().ends_with("data: [DONE]"), "got: {text}");
            headers_at
        }
    };
    let (a, b) = tokio::join!(
        one(client.clone(), body.clone()),
        one(client.clone(), body.clone())
    );

    let later = a.max(b);
    assert!(
        later >= stream_time,
        "second stream got headers after {later:?}; it should have waited for \
         the first stream ({stream_time:?}) to release the only slot"
    );
}

/// A client that gives up while queued must not cost a backend call.
#[tokio::test]
async fn request_abandoned_in_queue_is_never_sent_to_the_backend() {
    let (backend, load) = spawn_slow_backend(Duration::from_millis(400)).await;
    let gw = spawn_gateway_cfg(backend, limited(10, 1)).await;

    // A occupies the only slot for 400ms.
    let url = format!("{gw}/v1/chat/completions");
    let a = tokio::spawn({
        let url = url.clone();
        async move {
            reqwest::Client::new()
                .post(url)
                .json(&request_body())
                .send()
                .await
                .unwrap()
                .status()
        }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    // B queues behind A, then its client times out and hangs up.
    let b = reqwest::Client::builder()
        .timeout(Duration::from_millis(100))
        .build()
        .unwrap()
        .post(&url)
        .json(&request_body())
        .send()
        .await;
    assert!(b.is_err(), "B should have timed out while queued");

    assert_eq!(a.await.unwrap(), 200);
    // Give the scheduler time to pull B off the queue after A's slot frees.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        load.calls.load(Ordering::SeqCst),
        1,
        "B's client was gone; the scheduler should have skipped it"
    );
}

// ── multi-tenancy (Block C, D6) ──────────────────────────────────────────

fn tenant(id: &str, tier: Tier, tpm: u64, rps: f64, conc: u32) -> TenantConfig {
    TenantConfig {
        id: id.into(),
        api_key: format!("key-{id}"),
        tier,
        tokens_per_minute: tpm,
        requests_per_second: rps,
        max_concurrent: conc,
    }
}

async fn post_as(gw: &str, key: Option<&str>, body: &Value) -> reqwest::Response {
    let mut req = reqwest::Client::new()
        .post(format!("{gw}/v1/chat/completions"))
        .json(body);
    if let Some(k) = key {
        req = req.bearer_auth(k);
    }
    req.send().await.unwrap()
}

async fn error_code(resp: reqwest::Response) -> (u16, String) {
    let status = resp.status().as_u16();
    let body: Value = resp.json().await.unwrap();
    (
        status,
        body["error"]["code"].as_str().unwrap_or("").to_string(),
    )
}

#[tokio::test]
async fn keyed_mode_requires_a_valid_api_key() {
    let tenants = Tenants::from_configs(&[tenant("a", Tier::Paid, 1_000_000, 100.0, 10)]).unwrap();
    let gw = spawn_gateway_full(spawn_instant_backend().await, limited(10, 0), tenants).await;

    let (s, code) = error_code(post_as(&gw, None, &request_body()).await).await;
    assert_eq!((s, code.as_str()), (401, "invalid_api_key"));
    let (s, _) = error_code(post_as(&gw, Some("wrong"), &request_body()).await).await;
    assert_eq!(s, 401);
    assert_eq!(
        post_as(&gw, Some("key-a"), &request_body()).await.status(),
        200
    );

    // Health and metrics stay unauthenticated.
    assert_eq!(
        reqwest::get(format!("{gw}/health")).await.unwrap().status(),
        200
    );
}

#[tokio::test]
async fn tenant_rate_limit_rejects_the_excess_of_a_burst() {
    // 5 req/s, burst 5: 12 at once -> 5 accepted, 7 rate-limited.
    let tenants = Tenants::from_configs(&[tenant("a", Tier::Free, 1_000_000, 5.0, 100)]).unwrap();
    let gw = spawn_gateway_full(spawn_instant_backend().await, limited(100, 0), tenants).await;

    let mut codes = Vec::new();
    for _ in 0..12 {
        codes.push(error_or_ok(post_as(&gw, Some("key-a"), &request_body()).await).await);
    }
    assert_eq!(codes.iter().filter(|c| *c == "ok").count(), 5, "{codes:?}");
    assert_eq!(
        codes.iter().filter(|c| *c == "rate_limited").count(),
        7,
        "{codes:?}"
    );
}

async fn error_or_ok(resp: reqwest::Response) -> String {
    if resp.status() == 200 {
        let _ = resp.bytes().await;
        "ok".into()
    } else {
        error_code(resp).await.1
    }
}

#[tokio::test]
async fn tenant_concurrency_cap_holds_under_a_burst() {
    let tenants = Tenants::from_configs(&[tenant("a", Tier::Paid, 1_000_000, 1000.0, 2)]).unwrap();
    let (backend, load) = spawn_slow_backend(Duration::from_millis(200)).await;
    let gw = spawn_gateway_full(backend, limited(100, 0), tenants).await;

    let reqs = (0..8)
        .map(|_| async { error_or_ok(post_as(&gw, Some("key-a"), &request_body()).await).await });
    let codes = futures_util::future::join_all(reqs).await;

    assert_eq!(codes.iter().filter(|c| *c == "ok").count(), 2, "{codes:?}");
    assert_eq!(
        codes.iter().filter(|c| *c == "concurrency_limited").count(),
        6,
        "{codes:?}"
    );
    assert!(load.peak.load(Ordering::SeqCst) <= 2);
}

#[tokio::test]
async fn token_budget_reserves_max_tokens_then_refunds_actual_usage() {
    // Budget 600/min. The stub reports 2 completion tokens per reply.
    let cfg = tenant("a", Tier::Paid, 600, 1000.0, 100);
    let tenants = Tenants::from_configs(&[cfg]).unwrap();
    let gw = spawn_gateway_full(spawn_instant_backend().await, limited(100, 0), tenants).await;

    // A request that could cost more than the whole budget is refused up
    // front, before the backend does any work.
    let mut big = request_body();
    big["max_tokens"] = json!(1000);
    let (s, code) = error_code(post_as(&gw, Some("key-a"), &big).await).await;
    assert_eq!((s, code.as_str()), (429, "token_budget_exhausted"));

    // 500 reserved, 2 used: after it completes, nearly the whole 600 is
    // back — so a second 500-token request fits. With charge-on-reserve and
    // no refund, it would not.
    let mut req = request_body();
    req["max_tokens"] = json!(500);
    assert_eq!(post_as(&gw, Some("key-a"), &req).await.status(), 200);
    assert_eq!(post_as(&gw, Some("key-a"), &req).await.status(), 200);
}

#[tokio::test]
async fn streamed_tokens_are_counted_against_the_budget() {
    // 20 token frames per reply; budget 100/min; reservation 50 per request.
    let frames: String = (0..20)
        .map(|i| format!("data: {{\"choices\":[{{\"delta\":{{\"content\":\"t{i}\"}}}}]}}\n\n"))
        .chain(std::iter::once("data: [DONE]\n\n".to_string()))
        .collect();
    let backend = spawn_backend(Router::new().route(
        "/v1/chat/completions",
        post(move || {
            let frames = frames.clone();
            async move {
                (
                    [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
                    frames,
                )
            }
        }),
    ))
    .await;
    let tenants = Tenants::from_configs(&[tenant("a", Tier::Paid, 100, 1000.0, 100)]).unwrap();
    let gw = spawn_gateway_full(backend, limited(100, 0), tenants).await;

    let mut body = request_body();
    body["stream"] = json!(true);
    body["max_tokens"] = json!(50);

    // Each reply really costs 20. Reserve 50 needs 50 available:
    // 100 -> 80 -> 60 -> 40 (refused). If streamed tokens weren't counted
    // (charged 0), every request would be refunded to 100 and all would pass.
    let mut codes = Vec::new();
    for _ in 0..4 {
        codes.push(error_or_ok(post_as(&gw, Some("key-a"), &body).await).await);
    }
    assert_eq!(codes, ["ok", "ok", "ok", "token_budget_exhausted"]);
}

/// Experiment 3 in miniature. The backend serves one request at a time.
/// A free tenant floods the queue with 20 requests; then a paid request
/// arrives. With fair queueing it is served within a couple of slots.
/// With FIFO it waits behind the entire flood.
#[tokio::test]
async fn paid_request_is_not_starved_by_a_free_flood() {
    async fn paid_latency(fair: bool) -> Duration {
        let tenants = Tenants::from_configs(&[
            tenant("free", Tier::Free, 10_000_000, 1000.0, 100),
            tenant("paid", Tier::Paid, 10_000_000, 1000.0, 100),
        ])
        .unwrap();
        let (backend, _) = spawn_slow_backend(Duration::from_millis(50)).await;
        let cfg = SchedulerConfig {
            fair,
            ..limited(100, 1)
        };
        let gw = spawn_gateway_full(backend, cfg, tenants).await;

        let flood: Vec<_> = (0..20)
            .map(|_| {
                let gw = gw.clone();
                tokio::spawn(async move {
                    post_as(&gw, Some("key-free"), &request_body())
                        .await
                        .status()
                })
            })
            .collect();
        tokio::time::sleep(Duration::from_millis(30)).await; // flood is queued

        let start = std::time::Instant::now();
        let status = post_as(&gw, Some("key-paid"), &request_body())
            .await
            .status();
        let took = start.elapsed();
        assert_eq!(status, 200);
        for f in flood {
            assert_eq!(f.await.unwrap(), 200);
        }
        took
    }

    let fair = paid_latency(true).await;
    let fifo = paid_latency(false).await;
    // FIFO: behind ~19 x 50ms. Fair: at most ~2-3 slots.
    assert!(
        fair < Duration::from_millis(250),
        "fair queueing should serve paid quickly, took {fair:?}"
    );
    assert!(
        fifo > Duration::from_millis(700),
        "control: FIFO should make paid wait behind the flood, took {fifo:?}"
    );
}
