use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;
use thiserror::Error;

//everything that could go wrong while serving a request
#[derive(Debug, Error)]
pub enum GatewayError {
    /// #[from] is a special instruction for the thiserror library.
    /// It means: "If the code somewhere creates a reqwest::Error,
    /// automatically convert it into a GatewayError::BackendUnreachable for me."
    #[error("Could not reach backend: {0}")]
    BackendUnreachable(#[from] reqwest::Error), // reqwest::Error -> reqwest is the http library used to call the network, when the network call fails reqwest generates an error, #[from] -> if the code somewhere creates a reqwest::Error, it automatically convert it inot GatewarError::BackendUnreachable

    #[error("backend returned {status}: {body}")]
    BackendStatus { status: StatusCode, body: String },

    #[error("Too many requests")]
    OueueFull,

    #[error("Scheduler crashed or shut down")]
    SchedulerGone,
}

impl GatewayError {
    /// the status code we show the client and the short code in the body
    fn parts(&self) -> (StatusCode, &'static str) {
        match self {
            // we could not talk to the backend at all -> 502 gateway
            GatewayError::BackendUnreachable(_) => (StatusCode::BAD_GATEWAY, "backend_unreachable"),
            //the backend answered, but unhappily -> 502 as well
            GatewayError::BackendStatus { .. } => (StatusCode::BAD_GATEWAY, "backend_error"),
            GatewayError::OueueFull => (StatusCode::TOO_MANY_REQUESTS, "queu_full"),
            GatewayError::SchedulerGone => (StatusCode::INTERNAL_SERVER_ERROR, "scheduler_gone"),
        }
    }
}

/// lets us retrun Gatewayerror straight out of a handler
/// IntoResponse is a trait
/// OKay so imagine something goes wrong and we have a gateway error
/// BUT, browser/client donnot understands gateway error, it needs HTTP response
/// Into response defines that conversion
/// fn into_response(self) -> Response, this line is predefined and has to be written as it is in order to use it the trait
/// Notice this takes self (not &self), meaning it consumes the error entirely and converts it into a brand new Response object to be sent back to the client!
impl IntoResponse for GatewayError {
    fn into_response(self) -> Response {
        let (status, code) = self.parts(); // calling the fn defeined above (parts()) -> method return a tuple (StatusCode, &'String str)
        let message = self.to_string(); //to_string() this basically takes the error message of whichever error is the reason and then puts it here

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
