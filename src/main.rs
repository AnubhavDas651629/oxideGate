mod error;
mod types;

use anyhow::Context;
use axum::{
    extract::State,
    routing::{get, post},
    Json, Router,
};
use serde_json::json;
use std::net::SocketAddr;
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

    let backend_url = std::env::var("OXIDEGATE_BACKEND")
        .unwrap_or_else(|_| "http://127.0.0.1:11434/v1".to_string());

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .context("failed to build HTTP client")?;

    let state = Arc::new(AppState { http, backend_url });

    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/v1/chat/completions", post(chat_completions_handler))
        .with_state(state.clone());

    let addr = SocketAddr::from(([127, 0, 0, 1], 8000));

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to bind {addr}"))?;

    info!(backend = %state.backend_url, "oxideGate listening on {addr}");

    axum::serve(listener, app).await.context("server error")?;

    Ok(())
}

async fn health_handler() -> Json<serde_json::Value> {
    Json(json!({ "status": "ok" }))
}

/// Forward the request to the backend and hand the answer back unchanged.
async fn chat_completions_handler(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ChatCompletionRequest>,
) -> Result<Json<ChatCompletionResponse>, GatewayError> {
    let started = Instant::now();
    let url = format!("{}/chat/completions", state.backend_url);

    info!(
        model = %req.model,
        messages = req.messages.len(),
        "forwarding to backend"
    );

    let resp = state.http.post(&url).json(&req).send().await?;

    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(GatewayError::BackendStatus { status, body });
    }

    let parsed: ChatCompletionResponse = resp.json().await?;

    info!(
        elapsed_ms = started.elapsed().as_millis(),
        completion_tokens = parsed.usage.completion_tokens,
        "backend responded"
    );

    Ok(Json(parsed))
}
