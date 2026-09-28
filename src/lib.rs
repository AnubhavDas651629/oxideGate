pub mod error;
pub mod fairqueue;
pub mod scheduler;
pub mod telemetry;
pub mod tenants;
pub mod types;

use axum::{
    body::Body,
    extract::{Request, State},
    http::{header, HeaderMap},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Extension, Json, Router,
};
use futures_util::StreamExt;
use metrics::{counter, gauge, histogram};
use metrics_exporter_prometheus::PrometheusHandle;
use serde_json::json;
use std::sync::Arc;
use std::time::Instant;
use tracing::{info, Instrument};

use error::GatewayError;
use scheduler::{BackendSlot, Dispatched, QueueKey, SchedulerHandle};
use tenants::{Lease, Tenant, Tenants};
use types::{ChatCompletionRequest, ChatCompletionResponse};

/// Everything needed to talk to the model backend.
///
/// Kept separate from AppState to break a cycle: the scheduler needs this to
/// do its work, and AppState needs the scheduler. Splitting the backend out
/// means each is built once, in order, with no chicken-and-egg.
pub struct Backend {
    pub http: reqwest::Client,
    pub url: String,
}

impl Backend {
    /// Send one request upstream and reject any non-2xx reply.
    pub async fn send(
        &self,
        req: &ChatCompletionRequest,
    ) -> Result<reqwest::Response, GatewayError> {
        let url = format!("{}/chat/completions", self.url);
        let resp = self.http.post(&url).json(req).send().await?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(GatewayError::BackendStatus { status, body });
        }

        Ok(resp)
    }
}

/// Shared, read-only state handed to every request handler.
///
/// Handlers no longer hold the backend: everything goes through the queue.
pub struct AppState {
    pub scheduler: SchedulerHandle,
    pub metrics: PrometheusHandle,
    pub tenants: Tenants,
    /// Tokens reserved for a request that doesn't set max_tokens. Also
    /// forwarded to the backend as max_tokens, so the reservation is an
    /// actual ceiling rather than a guess (D6).
    pub default_max_tokens: u32,
}

pub fn build_router(state: Arc<AppState>) -> Router {
    // Auth wraps only the inference route; /health and /metrics stay open.
    let api = Router::new()
        .route("/v1/chat/completions", post(chat_completions_handler))
        .route_layer(middleware::from_fn_with_state(state.clone(), authenticate));

    Router::new()
        .route("/health", get(health_handler))
        .route("/metrics", get(metrics_handler))
        .merge(api)
        .with_state(state)
}

/// Resolve `Authorization: Bearer <key>` to a tenant, before the body is
/// even parsed. The tenant rides along to the handler as a request
/// extension — axum's typed per-request storage.
async fn authenticate(
    State(state): State<Arc<AppState>>,
    mut req: Request,
    next: Next,
) -> Result<Response, GatewayError> {
    let key = bearer(req.headers());
    let tenant = state.tenants.resolve(key).ok_or_else(|| {
        counter!(telemetry::REQUESTS_TOTAL, "outcome" => "unauthorized").increment(1);
        GatewayError::Unauthorized
    })?;
    req.extensions_mut().insert(tenant);
    Ok(next.run(req).await)
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
        .map(str::trim)
}

async fn health_handler() -> Json<serde_json::Value> {
    Json(json!({ "status": "ok" }))
}

async fn metrics_handler(State(state): State<Arc<AppState>>) -> Response {
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        state.metrics.render(),
    )
        .into_response()
}

async fn chat_completions_handler(
    State(state): State<Arc<AppState>>,
    Extension(tenant): Extension<Arc<Tenant>>,
    Json(req): Json<ChatCompletionRequest>,
) -> Result<Response, GatewayError> {
    // The clock starts HERE, before the request enters the queue.
    //
    // Starting it after submit() returns would hide the queue wait entirely,
    // and the batching window's cost is exactly what Experiment 1 measures.
    // A stopwatch started after the wait would report a flat line and look
    // like a result rather than a bug.
    let arrived = Instant::now();
    let wants_stream = req.stream;

    let span = tracing::info_span!("request", tenant = %tenant.id, stream = wants_stream);
    span.in_scope(|| info!(model = %req.model, messages = req.messages.len(), "request received"));

    gauge!(telemetry::INFLIGHT).increment(1.0);
    let result = serve(&state, &tenant, req, wants_stream, arrived)
        .instrument(span)
        .await;
    gauge!(telemetry::INFLIGHT).decrement(1.0);

    // Rejections are split out from errors: a 429 is admission control
    // doing its job, not something broken, and the two need separate
    // curves. The reason label says which limit fired.
    let tenant_label = tenant.id.to_string();
    match &result {
        Ok(_) => counter!(telemetry::REQUESTS_TOTAL, "outcome" => "ok", "tenant" => tenant_label)
            .increment(1),
        Err(e @ (GatewayError::QueueFull | GatewayError::Admission(_))) => {
            let (_, reason) = e.parts();
            counter!(telemetry::REQUESTS_TOTAL,
                "outcome" => "rejected", "reason" => reason, "tenant" => tenant_label)
            .increment(1)
        }
        Err(_) => {
            counter!(telemetry::REQUESTS_TOTAL, "outcome" => "error", "tenant" => tenant_label)
                .increment(1)
        }
    }

    result
}

/// Admit, queue, then shape the reply.
async fn serve(
    state: &AppState,
    tenant: &Arc<Tenant>,
    mut req: ChatCompletionRequest,
    wants_stream: bool,
    arrived: Instant,
) -> Result<Response, GatewayError> {
    // Reserve the most this request could cost. If the client didn't cap
    // it, we do, so the backend can't produce more than was reserved.
    let reserve = *req.max_tokens.get_or_insert(state.default_max_tokens);

    // Every tenant limit, checked before the request takes a queue place.
    // `?` turns an AdmitError into GatewayError::Admission via #[from].
    let lease = tenant.admit(u64::from(reserve))?;

    let key = QueueKey {
        tenant: Arc::clone(&tenant.id),
        weight: tenant.tier.weight(),
        tier: tenant.tier.as_str(),
    };

    // This await covers the queue wait as well as the backend call. If it
    // fails, `lease` is dropped on the way out: slot freed, full refund.
    let Dispatched { resp, slot } = state.scheduler.submit(req, key).await?;

    if wants_stream {
        Ok(stream_response(resp, slot, lease, arrived))
    } else {
        buffered_response(resp, slot, lease, arrived).await
    }
}

/// Wait for the whole reply, parse it, hand it back.
async fn buffered_response(
    resp: reqwest::Response,
    slot: BackendSlot,
    mut lease: Lease,
    arrived: Instant,
) -> Result<Response, GatewayError> {
    let parsed: ChatCompletionResponse = resp.json().await?;
    // The whole body is in hand; the backend is done with this request.
    drop(slot);
    lease.record_tokens(u64::from(parsed.usage.completion_tokens));
    drop(lease);

    let elapsed = arrived.elapsed();
    histogram!(telemetry::REQUEST_DURATION).record(elapsed.as_secs_f64());

    info!(
        e2e_ms = elapsed.as_millis(),
        completion_tokens = parsed.usage.completion_tokens,
        "backend responded"
    );

    Ok(Json(parsed).into_response())
}

/// Pipe the backend's SSE frames straight through to the client.
fn stream_response(
    resp: reqwest::Response,
    slot: BackendSlot,
    mut lease: Lease,
    arrived: Instant,
) -> Response {
    let tier = lease.tenant().tier.as_str();
    let mut frames = FrameCounter::default();
    // Bytes are forwarded untouched. We only observe them to record
    // time-to-first-token, measured from arrival so the queue wait counts,
    // and to count tokens for the tenant's budget.
    let mut seen_first = false;
    let byte_stream = resp.bytes_stream().map(move |chunk| {
        // Keeps the backend slot alive for exactly as long as this stream.
        //
        // A `move` closure only captures variables it mentions, so without
        // this line `slot` would be dropped as soon as this function
        // returned — freeing the slot while tokens are still flowing, and
        // letting the limiter over-admit. Mentioning it moves it into the
        // closure; the closure lives inside the response body; the body is
        // dropped when the last byte is sent or the client disconnects.
        // (`lease` needs no such line: it is used below, so it is captured.)
        let _ = &slot;
        if let Ok(bytes) = &chunk {
            if !seen_first {
                seen_first = true;
                let ttft = arrived.elapsed();
                histogram!(telemetry::TTFT, "tier" => tier).record(ttft.as_secs_f64());
                info!(ttft_ms = ttft.as_millis(), "first token");
            }
            lease.record_tokens(frames.feed(bytes));
        }
        chunk
    });

    (
        [
            (header::CONTENT_TYPE, "text/event-stream"),
            (header::CACHE_CONTROL, "no-cache"),
            (header::CONNECTION, "keep-alive"),
        ],
        Body::from_stream(byte_stream),
    )
        .into_response()
}

/// Counts SSE `data:` frames that carry a token, across arbitrary chunk
/// boundaries. vLLM and Ollama send about one token per frame, so this is
/// an approximation of completion tokens — good enough for a budget,
/// without parsing JSON on the hot path or needing a tokenizer.
#[derive(Default)]
struct FrameCounter {
    /// Bytes of an incomplete line carried over from the previous chunk.
    partial: Vec<u8>,
}

impl FrameCounter {
    /// Longest partial line we bother keeping; SSE lines are short.
    const MAX_PARTIAL: usize = 64 * 1024;

    fn feed(&mut self, bytes: &[u8]) -> u64 {
        let mut n = 0;
        let mut rest = bytes;
        while let Some(pos) = rest.iter().position(|&b| b == b'\n') {
            let (line_end, after) = rest.split_at(pos);
            if self.partial.is_empty() {
                n += count_line(line_end);
            } else {
                self.partial.extend_from_slice(line_end);
                n += count_line(&self.partial);
                self.partial.clear();
            }
            rest = &after[1..];
        }
        if self.partial.len() + rest.len() <= Self::MAX_PARTIAL {
            self.partial.extend_from_slice(rest);
        }
        n
    }
}

fn count_line(line: &[u8]) -> u64 {
    let line = line.strip_suffix(b"\r").unwrap_or(line);
    match line.strip_prefix(b"data:") {
        Some(payload) if payload.trim_ascii() != b"[DONE]" => 1,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::FrameCounter;

    #[test]
    fn counts_token_frames_not_done() {
        let mut c = FrameCounter::default();
        let n = c.feed(b"data: {\"a\":1}\n\ndata: {\"a\":2}\n\ndata: [DONE]\n\n");
        assert_eq!(n, 2);
    }

    #[test]
    fn frames_split_across_chunks_are_counted_once() {
        let mut c = FrameCounter::default();
        let mut n = c.feed(b"da");
        n += c.feed(b"ta: {\"x\":1}");
        n += c.feed(b"\n\ndata: [DO");
        n += c.feed(b"NE]\n\n");
        assert_eq!(n, 1);
    }
}
