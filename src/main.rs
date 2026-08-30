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
use types::{ChatCompletionsRequest, ChatCompletionsResponse};

struct AppState {
    http: reqwest::Client,
    backend_url: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // in proffesional rust apps we donnot use "println!" for server logs, instead we use tracing, this turns on logging system, without this none of "info!", "error!", "debug!" will work
    tracing_subscriber::fmt::init();

    let backend_url = std::env::var("OXIDEGATE_BACKEND")
        .unwrap_or_else(|_| "http://127.0.0.1:11434/v1".to_string());

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .context("failed to build HTTP client")?;

    let state = Arc::new(AppState { http, backend_url });

    //build router with routes
    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/v1/chat/completions", post(chat_completions_handler))
        .with_state(state.clone());

    //Bind to localhost:8000
    let addr = SocketAddr::from(([127, 0, 0, 1], 8000));

    //run the server
    let listener = tokio::net::TcpListener::bind(addr) //listener will open its ear to port 8000, if laready in use, expect(...) is error handling
        .await
        .with_context(|| format!("failed to bind {addr}"))?; // we use "|| format!" so that rust only wastes time formatting that string if an error actually occurs

    info!(backend = %state.backend_url, "oxideGate listening on {addr}");

    // .await: The server runs. Let's pretend it suddenly fails and generates a raw, confusing HyperNetworkError.
    // .context("server error"): This grabs the HyperNetworkError and wraps it in a nice bow so it now reads "server error: HyperNetworkError".
    // ?: The question mark looks at it, sees that it's an error, and instantly hits the eject button. It forces the main() function to stop whatever it was doing and return that beautifully formatted error out to the terminal, where the program shuts down cleanly.
    axum::serve(listener, app).await.context("server error")?; // axum will take the listener and our app and actually starts the web server

    Ok(())
}

// json!({"status": "ok"}) -> creates a tiny peice of json
// Json(...) to package it up nicely with the correct HTTP headers so the web browser knows it's receiving JSON
async fn health_handler() -> Json<serde_json::Value> {
    Json(json!({"status": "ok"}))
}

async fn chat_completions_handler(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ChatCompletionsRequest>,
) -> Result<Json<ChatCompletionsResponse>, GatewayError> {
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

    let parsed: ChatCompletionsResponse = resp.json().await?;

    info!(
        elapsed_ms = started.elapsed().as_millis(),
        completion_tokens = parsed.usage.completion_tokens,
        "backend responded"
    );

    Ok(Json(parsed))
}
