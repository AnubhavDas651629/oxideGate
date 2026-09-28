use anyhow::Context;
use oxidegate::scheduler::{self, SchedulerConfig};
use oxidegate::tenants::Tenants;
use oxidegate::{build_router, telemetry, AppState, Backend};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

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
    // 0 = no limit: every request goes straight to the backend, which is
    // the pre-limiter behaviour and Experiment 2's control.
    let max_inflight: usize = env_num("OXIDEGATE_MAX_INFLIGHT", 0);
    // 1 = deficit round robin across tenants (D6); 0 = one FIFO, the
    // control for Experiment 3.
    let fair = env_num::<u8>("OXIDEGATE_FAIR_QUEUE", 1) != 0;
    let default_max_tokens: u32 = env_num("OXIDEGATE_DEFAULT_MAX_TOKENS", 256);

    // No tenants file = single anonymous tenant: no auth, no limits.
    let tenants = match std::env::var("OXIDEGATE_TENANTS") {
        Ok(path) => Tenants::load(std::path::Path::new(&path))
            .with_context(|| format!("loading tenants from {path}"))?,
        Err(_) => Tenants::anonymous(),
    };
    if tenants.is_anonymous() {
        warn!("OXIDEGATE_TENANTS not set: single anonymous tenant, no auth, no quotas");
    } else {
        info!(tenants = tenants.len(), "tenants loaded");
    }

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
        max_inflight,
        fair,
    };
    if max_inflight == 0 {
        // Not an error — it is Experiment 2's control — but it should never
        // be silent: with no limit, the backlog forms inside the backend,
        // the queue never fills, and admission control cannot shed load.
        warn!(
            "OXIDEGATE_MAX_INFLIGHT=0: in-flight limiter disabled; the 429 \
             path will not engage under load. Set it to the backend's \
             measured capacity (see WRITEUP-1.md, Experiment 2)."
        );
    }
    let scheduler = scheduler::spawn(backend, cfg);

    let state = Arc::new(AppState {
        scheduler,
        metrics,
        tenants,
        default_max_tokens,
    });
    let app = build_router(state);

    let listener = tokio::net::TcpListener::bind(&bind_addr)
        .await
        .with_context(|| format!("failed to bind {bind_addr}"))?;

    info!(
        backend = %backend_url,
        batch_window_ms = window_ms,
        queue_depth,
        max_inflight,
        fair,
        "oxideGate listening on {bind_addr}"
    );

    axum::serve(listener, app).await.context("server error")?;

    Ok(())
}
