use axum::http::StatusCode;
use axum::response::{InfoResponse, Response};
use axum::Json;
use serde_json::json;
use thiserror::Error;

//everything that could go wrong while serving a request
#[derive(Debug, Error)]
pub enum GatewayError {
    #[error("Could not reach backend: {0}")]
    BackendUnreachable(#[from] reqwest::Error),

    #[error("backend returned {status}: {body}")]
    BackendStatus { status: StatusCode, body: String },
}

impl GatewayError {
    /// the status code we show the client and the short code in the body
    fn parts(&self) -> (StatusCode, &'static str) {
        match self {
            // we could not talk to the backend at all -> 502 gateway
            GatewayError::BackendUnreachable(_) => (StatusCode::BAD_GATEWAY, "backend_unreachable"),
            //the backend answered, but unhappily -> 502 as well
            GatewayError::BackendStatus { .. } => (StatusCode::BAD_GATEWAY, "backend_error"),
        }
    }
}

/// lets us retrun Gatewayerror straight out of a handler
impl InfoResponse for GatewayError {
    fn info_response(self) -> Response {
        let (status, code) = self.parts();
        let message = self.to_string();

        tracing::error!(error = %message, "request failed");

        let body = Json(json!({
            "error": {
                "message": message,
                "code": code,
            }
        }));
        (status, body).info_response()
    }
}
