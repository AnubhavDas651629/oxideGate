use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot};

use crate::error::GatewayError;
use crate::types::ChatCompletionRequest;
use crate::{telemetry, Backend};

pub type Reply = Result<reqwest::Response, GatewayError>;

pub struct QueuedRequest {
    pub req: ChatCompletionRequest,
    pub respond_to: oneshot::Sender<Reply>,
    pub enqueued_at: Instant,
}

#[derive(Clone, Copy, Debug)]
pub struct SchedulerConfig {
    pub queue_depth: usize,
    pub window: Duration,
}

#[derive(Clone)]
pub struct SchedulerHandle {
    tx: mpsc::Sender<QueuedRequest>,
}

impl SchedulerHandle {
    pub async fn submit(&self, req: ChatCompletionRequest) -> Reply {
        let (tx, rx) = oneshot::channel();

        let queued_req = QueuedRequest {
            req,
            respond_to: tx,
            enqueued_at: Instant::now(),
        };

        if self.tx.try_send(queued_req).is_err() {
            return Err(GatewayError::QueueFull);
        }

        match rx.await {
            Ok(reply) => reply,
            Err(_) => Err(GatewayError::SchedulerGone),
        }
    }
}

pub fn spawn(backend: Arc<Backend>, cfg: SchedulerConfig) -> SchedulerHandle {
    let (tx, rx) = mpsc::channel(cfg.queue_depth);
    tokio::spawn(run(rx, backend, cfg));
    SchedulerHandle { tx }
}

async fn run(mut rx: mpsc::Receiver<QueuedRequest>, backend: Arc<Backend>, cfg: SchedulerConfig) {
    loop {
        let first = match rx.recv().await {
            Some(req) => req,
            None => break,
        };

        let mut batch = vec![first];

        if cfg.window != Duration::ZERO {
            let deadline = Instant::now() + cfg.window;
            loop {
                let Some(left) = deadline.checked_duration_since(Instant::now()) else {
                    break;
                };

                match tokio::time::timeout(left, rx.recv()).await {
                    Ok(Some(req)) => batch.push(req),
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
        }

        let batch_size = batch.len();
        let queue_wait = batch[0].enqueued_at.elapsed();

        metrics::histogram!(telemetry::BATCH_SIZE).record(batch_size as f64);

        tracing::info!(
            batch_size = batch_size,
            first_waited_ms = queue_wait.as_millis(),
            "dispatching batch"
        );
        for item in batch {
            tokio::spawn(dispatch(backend.clone(), item));
        }
    }
}

async fn dispatch(backend: Arc<Backend>, item: QueuedRequest) {
    // Per-request queue wait. batch[0] waited longest; recording it here,
    // once per request, is what gives usable percentiles rather than a
    // worst-case log line.
    metrics::histogram!(telemetry::QUEUE_WAIT).record(item.enqueued_at.elapsed().as_secs_f64());

    let result = backend.send(&item.req).await;

    if item.respond_to.send(result).is_err() {
        tracing::debug!("client hung up before response was delivered");
    }
}
