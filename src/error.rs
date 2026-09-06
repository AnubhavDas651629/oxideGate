use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;
use thiserror::Error;

/// Everything that can go wrong while serving a request.
#[derive(Debug, Error)]
pub enum GatewayError {
    #[error("could not reach backend: {0}")]
    BackendUnreachable(#[from] reqwest::Error),

    #[error("backend returned {status}: {body}")]
    BackendStatus { status: StatusCode, body: String },

    #[error("queue is full, try again later")]
    QueueFull,

    #[error("scheduler stopped before answering")]
    SchedulerGone,
}

impl GatewayError {
    /// The status code we show the client, and the short code in the body.
    fn parts(&self) -> (StatusCode, &'static str) {
        match self {
            // We could not talk to the backend at all -> 502 Bad Gateway.
            GatewayError::BackendUnreachable(_) => (StatusCode::BAD_GATEWAY, "backend_unreachable"),
            // The backend answered, but unhappily -> 502 as well.
            GatewayError::BackendStatus { .. } => (StatusCode::BAD_GATEWAY, "backend_error"),
            // The queue is full. Turning work away is correct behaviour here,
            // not a failure: 429 tells the client to come back.
            GatewayError::QueueFull => (StatusCode::TOO_MANY_REQUESTS, "queue_full"),
            // The scheduler died or shut down mid-request. That is our bug,
            // not the client's, so 500 rather than 502.
            GatewayError::SchedulerGone => {
                (StatusCode::INTERNAL_SERVER_ERROR, "scheduler_unavailable")
            }
        }
    }
}

/// Lets us return GatewayError straight out of a handler.
impl IntoResponse for GatewayError {
    fn into_response(self) -> Response {
        let (status, code) = self.parts();
        let message = self.to_string();

        tracing::error!(error = %message, "request failed");

        let body = Json(json!({
            "error": {
                "message": message,
                "code": code,
            }
        }));

        (status, body).into_response()
    }
}
