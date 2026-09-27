use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};
use tracing::Instrument;

use crate::error::GatewayError;
use crate::types::ChatCompletionRequest;
use crate::{telemetry, Backend};

pub type Reply = Result<Dispatched, GatewayError>;

/// What the scheduler hands back to the handler: the backend's response,
/// plus the slot it is occupying at the backend.
///
/// The slot has to travel with the response. For a streamed reply the
/// backend is still busy until the *last* token, not when the headers
/// arrive, so the handler keeps the slot alive for as long as it is still
/// forwarding bytes and drops it when the stream ends.
pub struct Dispatched {
    pub resp: reqwest::Response,
    pub slot: BackendSlot,
}

/// One unit of backend concurrency. Dropping it frees the slot.
///
/// Rust detail: there is no explicit `release()` call anywhere. The
/// semaphore permit inside is returned automatically when this value is
/// dropped — whether the stream finished normally, errored, or the client
/// hung up and axum threw the body away. Tying the release to ownership
/// means there is no code path that can forget to do it.
pub struct BackendSlot {
    /// None when the limiter is disabled (max_inflight = 0).
    _permit: Option<OwnedSemaphorePermit>,
}

impl BackendSlot {
    fn new(permit: Option<OwnedSemaphorePermit>) -> Self {
        metrics::gauge!(telemetry::BACKEND_INFLIGHT).increment(1.0);
        BackendSlot { _permit: permit }
    }
}

impl Drop for BackendSlot {
    fn drop(&mut self) {
        // The permit field is dropped right after this runs, releasing it.
        metrics::gauge!(telemetry::BACKEND_INFLIGHT).decrement(1.0);
    }
}

pub struct QueuedRequest {
    pub req: ChatCompletionRequest,
    pub respond_to: oneshot::Sender<Reply>,
    pub enqueued_at: Instant,
}

#[derive(Clone, Copy, Debug)]
pub struct SchedulerConfig {
    /// Requests allowed to wait in our queue. Past this, submit() fails
    /// with QueueFull (429).
    pub queue_depth: usize,
    /// Batching window. Measured in Experiment 1 as pure cost; kept only
    /// as an experiment switch. 0 disables it.
    pub window: Duration,
    /// Maximum requests in flight at the backend at once. 0 = unlimited,
    /// which is the pre-limiter behaviour and Experiment 2's control.
    pub max_inflight: usize,
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

        // try_send, never send().await: if the queue is full we answer 429
        // immediately instead of making the client wait for a place in the
        // line to wait in. This only fires once something upstream stops
        // draining the queue — which is what the in-flight limiter does.
        if self.tx.try_send(queued_req).is_err() {
            return Err(GatewayError::QueueFull);
        }
        metrics::gauge!(telemetry::QUEUE_DEPTH)
            .set((self.tx.max_capacity() - self.tx.capacity()) as f64);

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

/// Take a backend slot, waiting if all of them are in use.
///
/// Returns None only if the semaphore was closed, which nothing in this
/// program does; the scheduler treats that as "shut down".
async fn acquire_slot(limiter: &Option<Arc<Semaphore>>) -> Option<BackendSlot> {
    match limiter {
        None => Some(BackendSlot::new(None)),
        Some(sem) => {
            // acquire_owned needs an Arc<Semaphore> (not a reference) so the
            // permit can outlive this function and be moved to another task.
            let permit = Arc::clone(sem).acquire_owned().await.ok()?;
            Some(BackendSlot::new(Some(permit)))
        }
    }
}

async fn run(mut rx: mpsc::Receiver<QueuedRequest>, backend: Arc<Backend>, cfg: SchedulerConfig) {
    let limiter = (cfg.max_inflight > 0).then(|| Arc::new(Semaphore::new(cfg.max_inflight)));

    loop {
        // Order matters: take a backend slot FIRST, then a request.
        //
        // While every slot is busy the scheduler sits here and does not
        // touch the queue, so the queue fills and submit() starts returning
        // 429. That is the whole mechanism by which admission control gets
        // teeth (ROADMAP D5). Receiving first and then waiting for a slot
        // would also work, but would park one request in limbo — out of the
        // queue, not at the backend, invisible to both.
        let Some(first_slot) = acquire_slot(&limiter).await else {
            break;
        };
        let Some(first) = rx.recv().await else {
            break;
        };

        let mut batch = vec![(first, Some(first_slot))];

        if cfg.window != Duration::ZERO {
            let deadline = Instant::now() + cfg.window;
            // Keep collecting until the deadline passes. checked_duration_since
            // returns None once `now` is past the deadline, which ends the loop.
            while let Some(left) = deadline.checked_duration_since(Instant::now()) {
                match tokio::time::timeout(left, rx.recv()).await {
                    // Slots for these are taken just before dispatch, below.
                    Ok(Some(req)) => batch.push((req, None)),
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
        }

        metrics::gauge!(telemetry::QUEUE_DEPTH).set(rx.len() as f64);

        let batch_size = batch.len();
        let queue_wait = batch[0].0.enqueued_at.elapsed();

        metrics::histogram!(telemetry::BATCH_SIZE).record(batch_size as f64);

        tracing::debug!(
            batch_size = batch_size,
            first_waited_ms = queue_wait.as_millis(),
            "dispatching batch"
        );
        for (item, slot) in batch {
            // A client that hung up while queued gets skipped: sending its
            // request to the backend would burn a slot producing tokens
            // nobody will read. The slot (if any) is dropped and freed here.
            if item.respond_to.is_closed() {
                metrics::counter!(telemetry::ABANDONED_TOTAL).increment(1);
                continue;
            }
            let slot = match slot {
                Some(s) => s,
                None => match acquire_slot(&limiter).await {
                    Some(s) => s,
                    None => return,
                },
            };
            let span = tracing::info_span!("dispatch", model = %item.req.model);
            tokio::spawn(dispatch(backend.clone(), item, slot).instrument(span));
        }
    }
}

async fn dispatch(backend: Arc<Backend>, item: QueuedRequest, slot: BackendSlot) {
    // Per-request queue wait: enqueue until a backend slot was granted and
    // the request left the queue. This is the gateway-side half of latency.
    metrics::histogram!(telemetry::QUEUE_WAIT).record(item.enqueued_at.elapsed().as_secs_f64());

    // On error, `slot` is dropped at the end of this function (the backend
    // is no longer working on it); on success it rides along to the handler.
    let result = backend
        .send(&item.req)
        .await
        .map(|resp| Dispatched { resp, slot });

    if item.respond_to.send(result).is_err() {
        // The Reply (and the slot inside it) is dropped right here, so a
        // client that vanished mid-dispatch doesn't leak a slot.
        tracing::debug!("client hung up before response was delivered");
    }
}
