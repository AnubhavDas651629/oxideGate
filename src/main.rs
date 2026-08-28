use anyhow::Context;
use axum::{routing::get, Json, Router};
use serde_json::json;
use std::net::SocketAddr;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Send tracing events to stdout. RUST_LOG controls the level.
    tracing_subscriber::fmt::init();

    // Build the router: one route for now.
    let app = Router::new().route("/health", get(health_handler));

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
