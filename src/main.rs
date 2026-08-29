mod types;

use anyhow::{Context, Ok};
use axum::{
    routing::{get, post},
    Json, Router,
};
use serde_json::json;
use std::net::SocketAddr;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::info;
use types::{ChatCompletionsRequest, ChatCompletionsResponse, Choice, Message, Usage};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // in proffesional rust apps we donnot use "println!" for server logs, instead we use tracing, this turns on logging system, without this none of "info!", "error!", "debug!" will work
    tracing_subscriber::fmt::init();

    //build router with routes
    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/v1/chat/completions", post(chat_completions_handler));

    //Bind to localhost:8000
    let addr = SocketAddr::from(([127, 0, 0, 1], 8000));

    //run the server
    let listener = tokio::net::TcpListener::bind(addr) //listener will open its ear to port 8000, if laready in use, expect(...) is error handling
        .await
        .with_context(|| format!("failed to bind {addr}"))?; // we use "|| format!" so that rust only wastes time formatting that string if an error actually occurs

    info!("Oxidegate listening on {addr}");

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
    Json(req): Json<ChatCompletionsRequest>,
) -> Json<ChatCompletionsResponse> {
    info!(
        model = %req.model,
        messages = req.messages.len(),
        max_tokens = ?req.max_tokens,
        temperature = ?req.temperature,
        stream = req.stream,
        "Received chat completion request"
    );

    let reply = Message {
        role: "assistant".to_string(),
        content: format!(
            "mock reply from the oxideGate(model = {}, {} message(s) received",
            req.model,
            req.messages.len()
        ),
    };

    Json(ChatCompletionsResponse {
        id: "chatcmpl-oxideGate-mock".to_string(),
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
