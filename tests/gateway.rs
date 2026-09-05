//! Integration tests: a real gateway on a real socket, talking to a stub
//! backend on another. Nothing is mocked at the HTTP layer, so these exercise
//! routing, extraction, serialisation and error mapping the way a client does.

use axum::{routing::post, Json, Router};
use oxidegate::{build_router, telemetry, AppState};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, Ordering};
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

/// Spawn the gateway pointed at `backend_url`. Returns its base URL.
async fn spawn_gateway(backend_url: String) -> String {
    let state = Arc::new(AppState {
        http: reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap(),
        backend_url,
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
