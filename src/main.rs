mod types;

use anyhow::Context;
use axum::{
    routing::{get, post},
    Json, Router,
};
use serde_json::json;
use std::net::SocketAddr;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::info;

use types::{ChatCompletionRequest, ChatCompletionResponse, Choice, Message, Usage};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/v1/chat/completions", post(chat_completions_handler));

    let addr = SocketAddr::from(([127, 0, 0, 1], 8000));

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to bind {addr}"))?;

    info!("oxideGate listening on {addr}");

    axum::serve(listener, app).await.context("server error")?;

    Ok(())
}

async fn health_handler() -> Json<serde_json::Value> {
    Json(json!({ "status": "ok" }))
}

/// Mock for now: parse the request, ignore it, return a canned reply.
async fn chat_completions_handler(
    Json(req): Json<ChatCompletionRequest>,
) -> Json<ChatCompletionResponse> {
    info!(
        model = %req.model,
        messages = req.messages.len(),
        max_tokens = ?req.max_tokens,
        temperature = ?req.temperature,
        stream = req.stream,
        "received chat completion request"
    );

    let reply = Message {
        role: "assistant".to_string(),
        content: format!(
            "mock reply from oxideGate (model={}, {} message(s) received)",
            req.model,
            req.messages.len()
        ),
    };

    Json(ChatCompletionResponse {
        id: "chatcmpl-oxidegate-mock".to_string(),
        object: "chat.completion".to_string(),
        created: unix_timestamp(),
        model: req.model,
        choices: vec![Choice {
            index: 0,
            message: reply,
            finish_reason: "stop".to_string(),
        }],
        usage: Usage {
            prompt_tokens: 0,
            completion_tokens: 0,
            total_tokens: 0,
        },
    })
}

fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
