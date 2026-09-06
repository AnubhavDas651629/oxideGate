pub mod error;
pub mod telemetry;
pub mod types;

use axum::{
    body::Body,
    extract::State,
    http::header,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use futures_util::StreamExt;
use metrics::{counter, gauge, histogram};
use metrics_exporter_prometheus::PrometheusHandle;
use serde_json::json;
use std::sync::Arc;
use std::time::Instant;
use tracing::info;

use error::GatewayError;
use types::{ChatCompletionRequest, ChatCompletionResponse};

/// Shared, read-only state handed to every request handler.
pub struct AppState {
    pub http: reqwest::Client,
    pub backend_url: String,
    pub metrics: PrometheusHandle,
}

pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/health", get(health_handler))
        .route("/metrics", get(metrics_handler))
        .route("/v1/chat/completions", post(chat_completions_handler))
        .with_state(state)
}

async fn health_handler() -> Json<serde_json::Value> {
    Json(json!({ "status": "ok" }))
}

async fn metrics_handler(State(state): State<Arc<AppState>>) -> Response {
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        state.metrics.render(),
    )
        .into_response()
}

/// One entry point, two shapes of reply: streamed or buffered.
async fn chat_completions_handler(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ChatCompletionRequest>,
) -> Result<Response, GatewayError> {
    info!(
        model = %req.model,
        messages = req.messages.len(),
        stream = req.stream,
        "forwarding to backend"
    );

    gauge!(telemetry::INFLIGHT).increment(1.0);
    let result = if req.stream {
        stream_completion(&state, req).await
    } else {
        buffered_completion(&state, req).await
    };
    gauge!(telemetry::INFLIGHT).decrement(1.0);

    let outcome = if result.is_ok() { "ok" } else { "error" };
    counter!(telemetry::REQUESTS_TOTAL, "outcome" => outcome).increment(1);

    result
}

/// Wait for the whole reply, parse it, hand it back.
async fn buffered_completion(
    state: &AppState,
    req: ChatCompletionRequest,
) -> Result<Response, GatewayError> {
    let started = Instant::now();
    let resp = send_to_backend(state, &req).await?;
    let parsed: ChatCompletionResponse = resp.json().await?;

    let elapsed = started.elapsed();
    histogram!(telemetry::REQUEST_DURATION).record(elapsed.as_secs_f64());

    info!(
        e2e_ms = elapsed.as_millis(),
        completion_tokens = parsed.usage.completion_tokens,
        "backend responded"
    );

    Ok(Json(parsed).into_response())
}

/// Pipe the backend's SSE frames straight through to the client.
async fn stream_completion(
    state: &AppState,
    req: ChatCompletionRequest,
) -> Result<Response, GatewayError> {
    let started = Instant::now();
    let resp = send_to_backend(state, &req).await?;

    // Bytes are forwarded untouched. We only observe them to record
    // time-to-first-token, which is the metric Phase 2 is built around.
    let mut seen_first = false;
    let byte_stream = resp.bytes_stream().map(move |chunk| {
        if !seen_first {
            seen_first = true;
            let ttft = started.elapsed();
            histogram!(telemetry::TTFT).record(ttft.as_secs_f64());
            info!(ttft_ms = ttft.as_millis(), "first token");
        }
        chunk
    });

    Ok((
        [
            (header::CONTENT_TYPE, "text/event-stream"),
            (header::CACHE_CONTROL, "no-cache"),
            (header::CONNECTION, "keep-alive"),
        ],
        Body::from_stream(byte_stream),
    )
        .into_response())
}

/// Send the request upstream and reject any non-2xx reply.
async fn send_to_backend(
    state: &AppState,
    req: &ChatCompletionRequest,
) -> Result<reqwest::Response, GatewayError> {
    let url = format!("{}/chat/completions", state.backend_url);
    let resp = state.http.post(&url).json(req).send().await?;

    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(GatewayError::BackendStatus { status, body });
    }

    Ok(resp)
}
