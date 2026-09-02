mod error;
mod types;

use anyhow::Context;
use axum::{
    body::Body,
    extract::State,
    http::header,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use futures_util::StreamExt;
use serde_json::json;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::info;

use error::GatewayError;
use types::{ChatCompletionRequest, ChatCompletionResponse};

/// Shared, read-only state handed to every request handler.
struct AppState {
    http: reqwest::Client,
    backend_url: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    // 0.0.0.0 inside a container; override for local-only runs.
    let bind_addr =
        std::env::var("OXIDEGATE_BIND").unwrap_or_else(|_| "127.0.0.1:8000".to_string());

    let backend_url = std::env::var("OXIDEGATE_BACKEND")
        .unwrap_or_else(|_| "http://127.0.0.1:11434/v1".to_string());

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(300))
        .connect_timeout(Duration::from_secs(5))
        .build()
        .context("failed to build HTTP client")?;

    let state = Arc::new(AppState { http, backend_url });

    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/v1/chat/completions", post(chat_completions_handler))
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind(&bind_addr)
        .await
        .with_context(|| format!("failed to bind {bind_addr}"))?;

    info!(backend = %state.backend_url, "oxideGate listening on {bind_addr}");

    axum::serve(listener, app).await.context("server error")?;

    Ok(())
}

async fn health_handler() -> Json<serde_json::Value> {
    Json(json!({ "status": "ok" }))
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

    if req.stream {
        stream_completion(state, req).await
    } else {
        buffered_completion(state, req).await
    }
}

/// Wait for the whole reply, parse it, hand it back.
async fn buffered_completion(
    state: Arc<AppState>,
    req: ChatCompletionRequest,
) -> Result<Response, GatewayError> {
    let started = Instant::now();
    let resp = send_to_backend(&state, &req).await?;
    let parsed: ChatCompletionResponse = resp.json().await?;

    info!(
        e2e_ms = started.elapsed().as_millis(),
        completion_tokens = parsed.usage.completion_tokens,
        "backend responded"
    );

    Ok(Json(parsed).into_response())
}

/// Pipe the backend's SSE frames straight through to the client.
async fn stream_completion(
    state: Arc<AppState>,
    req: ChatCompletionRequest,
) -> Result<Response, GatewayError> {
    let started = Instant::now();
    let resp = send_to_backend(&state, &req).await?;

    // Bytes are forwarded untouched. We only observe them to record
    // time-to-first-token, which is the metric Phase 2 is built around.
    let mut seen_first = false;
    let byte_stream = resp.bytes_stream().map(move |chunk| {
        if !seen_first {
            seen_first = true;
            info!(ttft_ms = started.elapsed().as_millis(), "first token");
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
