use anyhow::Context;
use oxidegate::{build_router, telemetry, AppState};
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    // 0.0.0.0 inside a container; override for local-only runs.
    let bind_addr =
        std::env::var("OXIDEGATE_BIND").unwrap_or_else(|_| "127.0.0.1:8000".to_string());

    let backend_url = std::env::var("OXIDEGATE_BACKEND")
        .unwrap_or_else(|_| "http://127.0.0.1:11434/v1".to_string());

    let metrics = telemetry::install()?;

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(300))
        .connect_timeout(Duration::from_secs(5))
        .build()
        .context("failed to build HTTP client")?;

    let state = Arc::new(AppState {
        http,
        backend_url,
        metrics,
    });

    let app = build_router(state.clone());

    let listener = tokio::net::TcpListener::bind(&bind_addr)
        .await
        .with_context(|| format!("failed to bind {bind_addr}"))?;

    info!(backend = %state.backend_url, "oxideGate listening on {bind_addr}");

    axum::serve(listener, app).await.context("server error")?;

    Ok(())
}
