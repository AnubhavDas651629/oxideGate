pub mod error;
pub mod scheduler;
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
use scheduler::SchedulerHandle;
use types::{ChatCompletionRequest, ChatCompletionResponse};

/// Everything needed to talk to the model backend.
///
/// Kept separate from AppState to break a cycle: the scheduler needs this to
/// do its work, and AppState needs the scheduler. Splitting the backend out
/// means each is built once, in order, with no chicken-and-egg.
pub struct Backend {
    pub http: reqwest::Client,
    pub url: String,
}

impl Backend {
    /// Send one request upstream and reject any non-2xx reply.
    pub async fn send(
        &self,
        req: &ChatCompletionRequest,
    ) -> Result<reqwest::Response, GatewayError> {
        let url = format!("{}/chat/completions", self.url);
        let resp = self.http.post(&url).json(req).send().await?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(GatewayError::BackendStatus { status, body });
        }

        Ok(resp)
    }
}

/// Shared, read-only state handed to every request handler.
///
/// Handlers no longer hold the backend: everything goes through the queue.
pub struct AppState {
    pub scheduler: SchedulerHandle,
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

async fn chat_completions_handler(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ChatCompletionRequest>,
) -> Result<Response, GatewayError> {
    // The clock starts HERE, before the request enters the queue.
    //
    // Starting it after submit() returns would hide the queue wait entirely,
    // and the batching window's cost is exactly what Experiment 1 measures.
    // A stopwatch started after the wait would report a flat line and look
    // like a result rather than a bug.
    let arrived = Instant::now();
    let wants_stream = req.stream;

    info!(
        model = %req.model,
        messages = req.messages.len(),
        stream = wants_stream,
        "request received"
    );

    gauge!(telemetry::INFLIGHT).increment(1.0);
    let result = serve(&state, req, wants_stream, arrived).await;
    gauge!(telemetry::INFLIGHT).decrement(1.0);

    let outcome = if result.is_ok() { "ok" } else { "error" };
    counter!(telemetry::REQUESTS_TOTAL, "outcome" => outcome).increment(1);

    result
}

/// Queue the request, then shape the reply.
async fn serve(
    state: &AppState,
    req: ChatCompletionRequest,
    wants_stream: bool,
    arrived: Instant,
) -> Result<Response, GatewayError> {
    // Everything now goes through the scheduler. This await covers the queue
    // wait as well as the backend call.
    let resp = state.scheduler.submit(req).await?;

    if wants_stream {
        Ok(stream_response(resp, arrived))
    } else {
        buffered_response(resp, arrived).await
    }
}

/// Wait for the whole reply, parse it, hand it back.
async fn buffered_response(
    resp: reqwest::Response,
    arrived: Instant,
) -> Result<Response, GatewayError> {
    let parsed: ChatCompletionResponse = resp.json().await?;

    let elapsed = arrived.elapsed();
    histogram!(telemetry::REQUEST_DURATION).record(elapsed.as_secs_f64());

    info!(
        e2e_ms = elapsed.as_millis(),
        completion_tokens = parsed.usage.completion_tokens,
        "backend responded"
    );

    Ok(Json(parsed).into_response())
}

/// Pipe the backend's SSE frames straight through to the client.
fn stream_response(resp: reqwest::Response, arrived: Instant) -> Response {
    // Bytes are forwarded untouched. We only observe them to record
    // time-to-first-token, measured from arrival so the queue wait counts.
    let mut seen_first = false;
    let byte_stream = resp.bytes_stream().map(move |chunk| {
        if !seen_first {
            seen_first = true;
            let ttft = arrived.elapsed();
            histogram!(telemetry::TTFT).record(ttft.as_secs_f64());
            info!(ttft_ms = ttft.as_millis(), "first token");
        }
        chunk
    });

    (
        [
            (header::CONTENT_TYPE, "text/event-stream"),
            (header::CACHE_CONTROL, "no-cache"),
            (header::CONNECTION, "keep-alive"),
        ],
        Body::from_stream(byte_stream),
    )
        .into_response()
}
