//! The scheduler: requests wait in *our* queue instead of the backend's.
//!
//! Without this, every request goes straight to the backend and piles up
//! inside it, where we can neither see it nor reorder it. Holding the line
//! ourselves is what makes admission control and fairness possible at all.
//!
//! ── HOW THE PIECES FIT ───────────────────────────────────────────────
//!
//!   handler  --submit()-->  [ queue ]  -->  scheduler loop
//!      ^                                          |
//!      |                                     tokio::spawn
//!      +--------- oneshot "buzzer" <---------  dispatch --> backend
//!
//! The handler waits on the buzzer while the scheduler does the work.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot};

use crate::error::GatewayError;
use crate::types::ChatCompletionRequest;
use crate::AppState;

/// What travels back through the buzzer: the live backend connection, so the
/// handler can still stream tokens off it. Buffering here would kill SSE.
pub type Reply = Result<reqwest::Response, GatewayError>;

/// One request waiting in line.
pub struct QueuedRequest {
    pub req: ChatCompletionRequest,
    /// The buzzer. Send exactly one Reply through it, then it is used up.
    pub respond_to: oneshot::Sender<Reply>,
    /// When it joined the queue. `enqueued_at.elapsed()` at dispatch time is
    /// the queue wait, which is the number Experiment 1 turns on.
    pub enqueued_at: Instant,
}

#[derive(Clone, Copy, Debug)]
pub struct SchedulerConfig {
    /// How many requests may wait before we start rejecting with 429.
    pub queue_depth: usize,
    /// The batching window. `Duration::ZERO` disables it — that is the
    /// control case for Experiment 1, and the default.
    pub window: Duration, //Duration -> is a span of time, Instant -> is an instant of time
}

/// Handed to request handlers so they can submit work. Cloning is cheap:
/// it only clones the sending end of the channel.
#[derive(Clone)]
pub struct SchedulerHandle {
    tx: mpsc::Sender<QueuedRequest>, // multi producer - single consumer, where multiple many tasks can send, but only one could read
}

impl SchedulerHandle {
    /// Put a request in the queue and wait for its answer.
    pub async fn submit(&self, req: ChatCompletionRequest) -> Reply {
        // ┌── TODO 1 ────────────────────────────────────────────────────┐
        // │ Make a buzzer:                                               │
        // │     let (tx, rx) = oneshot::channel();                       │
        // │ `tx` goes into the queue with the request; you keep `rx`.    │
        // └──────────────────────────────────────────────────────────────┘

        // rx effectivly sleeps while tx is in the queue and wakes up when tx returns after getting the response from AI
        let (tx, rx) = oneshot::channel();

        // ┌── TODO 2 ────────────────────────────────────────────────────┐
        // │ Build a QueuedRequest and put it on the queue.               │
        // │                                                              │
        // │ Use `self.tx.try_send(...)`, NOT `send().await`.             │
        // │   try_send  -> fails immediately if the queue is full        │
        // │   send      -> waits for space, which is the opposite of     │
        // │                what admission control means                  │
        // │                                                              │
        // │ On failure return Err(GatewayError::QueueFull) -> 429.       │
        // └──────────────────────────────────────────────────────────────┘

        let queued_req = QueuedRequest {
            // queued_req is the package that gets dropped into the one shared queue
            req,
            respond_to: tx,
            enqueued_at: Instant::now(),
        };

        if let Err(_) = self.tx.try_send(queued_req) {
            return Err(GatewarError::QueueFull);
        }

        // ┌── TODO 3 ────────────────────────────────────────────────────┐
        // │ Wait on the buzzer: `rx.await`.                              │
        // │                                                              │
        // │ It gives you a Result. The Err case means the sender was     │
        // │ dropped without sending — the scheduler died or shut down    │
        // │ while holding your request. Map that to                      │
        // │ GatewayError::SchedulerGone.                                 │
        // │                                                              │
        // │ Note it is a Result inside a Result: the outer is "did the   │
        // │ buzzer work", the inner is "did the request succeed".        │
        // └──────────────────────────────────────────────────────────────┘

        match rx.await {
            //rx.await gives us a "Result"defined above, which could be Ok(reply), or and error
            Ok(reply) => reply,
            Err(_) => Err(GatewarError::SchedulerGone),
        }
    }
}

/// Start the scheduler. Returns the handle that handlers use.
pub fn spawn(state: Arc<AppState>, cfg: SchedulerConfig) -> SchedulerHandle {
    // Bounded on purpose: an unbounded queue grows until the process dies.
    let (tx, rx) = mpsc::channel(cfg.queue_depth);
    tokio::spawn(run(rx, state, cfg));
    SchedulerHandle { tx }
}

/// The scheduler loop. Runs forever, one ticket rail, one chef.
async fn run(mut rx: mpsc::Receiver<QueuedRequest>, state: Arc<AppState>, cfg: SchedulerConfig) {
    loop {
        // ┌── TODO 4 ────────────────────────────────────────────────────┐
        // │ Wait for the first request.                                  │
        // │                                                              │
        // │     let first = match rx.recv().await { ... };               │
        // │                                                              │
        // │ `recv()` returns None when every sender is gone, which means │
        // │ the gateway is shutting down. Break the loop on None, or     │
        // │ this spins forever burning CPU.                              │
        // └──────────────────────────────────────────────────────────────┘
        let first = match rx.recv().await {
            // recv is used for retreiving messages(only Some and None, not Ok and Err) from mpsc::
            Some(req) => req,
            None => break,
        };

        // ┌── TODO 5 ────────────────────────────────────────────────────┐
        // │ Collect a batch.                                             │
        // │                                                              │
        // │ Start a Vec holding `first`. Then, if cfg.window is not zero,│
        // │ keep pulling requests until the window expires:              │
        // │                                                              │
        // │     let deadline = Instant::now() + cfg.window;              │
        // │     loop {                                                   │
        // │         let left = deadline - Instant::now();  // careful:   │
        // │              // subtracting past a deadline panics. Use      │
        // │              // checked_duration_since or compare first.     │
        // │         match tokio::time::timeout(left, rx.recv()).await {  │
        // │             ... push it, or stop when time is up             │
        // │         }                                                    │
        // │     }                                                        │
        // │                                                              │
        // │ The timer starts when the FIRST request arrives (decision 3),│
        // │ not on a fixed tick. That is the version that gives batching │
        // │ its best shot, so the experiment is a fair test.             │
        // │                                                              │
        // │ With window == ZERO, skip all of this: batch is just [first].│
        // └──────────────────────────────────────────────────────────────┘

        let mut batch = vec![first];

        if cfg.window != Duration::ZERO {
            let deadline = Instant::now() + cfg.window;
            loop {
                // check how much time is left before deadline
                // checked_duration_since -> standard rust fn to find time between instant::now()
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

        // ┌── TODO 6 ──── the one from the puzzle ───────────────────────┐
        // │ Dispatch the batch.                                          │
        // │                                                              │
        // │ For each request:  tokio::spawn(dispatch(state.clone(), it)) │
        // │                                                              │
        // │ Do NOT write `dispatch(...).await` here. That parks this     │
        // │ loop until the backend replies, and the whole gateway goes   │
        // │ one-request-at-a-time. See examples/serial_vs_spawn.rs —     │
        // │ 2004ms vs 501ms for the same work.                           │
        // │                                                              │
        // │ Log the batch size and the queue wait of the first request;  │
        // │ you need both for the writeup.                               │
        // └──────────────────────────────────────────────────────────────┘

        let batch_size = batch.len();
        let queue_wait = batch[0].enqueued_at.elapsed(); // how long the first req waited

        tracing::info!(
            batch_size = batch_size,
            queue_wait_ms = queue_wait.as_millis(),
            "dispatching batch"
        );
        for item in batch {
            tokio::spawn(dispatch(state.clone(), item))
        }
    }
}

/// Send one request to the backend and buzz the answer back.
/// Runs as its own task, so several of these are in flight at once.
async fn dispatch(state: Arc<AppState>, item: QueuedRequest) {
    // ┌── TODO 7 ────────────────────────────────────────────────────────┐
    // │ Call crate::send_to_backend(&state, &item.req).await             │
    // │ then send the result through item.respond_to.                    │
    // │                                                                  │
    // │ `send` on a oneshot CONSUMES it, so this only works once — which │
    // │ is exactly right for one request, one answer.                    │
    // │                                                                  │
    // │ It returns a Result you cannot use the `?` operator on. An Err   │
    // │ means the client hung up while waiting. That is normal, not a    │
    // │ bug: log it at debug and move on. Do not panic.                  │
    // └──────────────────────────────────────────────────────────────────┘

    // Step 1: Call the backend and get either a live Response or an error
    let result = crate::send_to_backend(&state, &item.req).await;

    // Step 2: Buzz the result back through the oneshot to the sleeping HTTP handler.
    // .send() consumes item.respond_to — it can fire exactly once, which is correct.
    // Err(_) here means the client disconnected while waiting — normal, not a bug.
    if let Err(_) = item.respond_to.send(result) {
        tracing::debug!("client hung up before response was delivered");
    }
}
