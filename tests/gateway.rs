//! Integration tests: a real gateway on a real socket, talking to a stub
//! backend on another. Nothing is mocked at the HTTP layer, so these exercise
//! routing, extraction, serialisation and error mapping the way a client does.

use axum::{routing::post, Json, Router};
use futures_util::StreamExt;
use oxidegate::scheduler::{self, SchedulerConfig};
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
        },
    )
    .await
}

/// Spawn the gateway with a full scheduler config.
async fn spawn_gateway_cfg(backend_url: String, cfg: SchedulerConfig) -> String {
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
