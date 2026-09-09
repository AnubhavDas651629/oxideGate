use anyhow::Context;
use oxidegate::scheduler::{self, SchedulerConfig};
use oxidegate::{build_router, telemetry, AppState, Backend};
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

/// Read a number from the environment, falling back to `default`.
fn env_num<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    // 0.0.0.0 inside a container; override for local-only runs.
    let bind_addr =
        std::env::var("OXIDEGATE_BIND").unwrap_or_else(|_| "127.0.0.1:8000".to_string());

    let backend_url = std::env::var("OXIDEGATE_BACKEND")
        .unwrap_or_else(|_| "http://127.0.0.1:11434/v1".to_string());

    // Default window is 0: batching off. That is the control case for
    // Experiment 1, so the honest default is the one that adds nothing.
    let window_ms: u64 = env_num("OXIDEGATE_BATCH_WINDOW_MS", 0);
    let queue_depth: usize = env_num("OXIDEGATE_QUEUE_DEPTH", 100);

    let metrics = telemetry::install()?;

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(300))
        .connect_timeout(Duration::from_secs(5))
        .build()
        .context("failed to build HTTP client")?;

    let backend = Arc::new(Backend {
        http,
        url: backend_url.clone(),
    });

    let cfg = SchedulerConfig {
        queue_depth,
        window: Duration::from_millis(window_ms),
    };
    let scheduler = scheduler::spawn(backend, cfg);

    let state = Arc::new(AppState { scheduler, metrics });
    let app = build_router(state);

    let listener = tokio::net::TcpListener::bind(&bind_addr)
        .await
        .with_context(|| format!("failed to bind {bind_addr}"))?;

    info!(
        backend = %backend_url,
        batch_window_ms = window_ms,
        queue_depth,
        "oxideGate listening on {bind_addr}"
    );

    axum::serve(listener, app).await.context("server error")?;

    Ok(())
}
