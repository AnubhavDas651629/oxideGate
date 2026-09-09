mod error;
mod types;

use anyhow::Context;
use axum::{
    body::Body,
    extract::State,
    http::header,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use futures_util::StreamExt;
use serde_json::json;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::info;

use error::GatewayError;
use types::{ChatCompletionRequest, ChatCompletionResponse};

/// Shared, read-only state handed to every request handler.
struct AppState {
    http: reqwest::Client,
    backend_url: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // in proffesional rust apps we donnot use "println!" for server logs, instead we use tracing, this turns on logging system, without this none of "info!", "error!", "debug!" will work
    tracing_subscriber::fmt::init();

    // bind_addr -> IP address where and port that your server will "listen" on for incoming traffic
    // std::env -> This accesses Rust's standard library module for dealing with the operating system's environment
    // ::var("OXIDEGATE_BIND") ->This looks for an Environment Variable named OXIDEGATE_BIND on your computer -> returns either SUCCESS or an ERROR meaning that the variable wasnt set locally
    // .unwrap_or_else() -> If the environment variable exists, give me its value. Or else, if it's an error (like it doesn't exist), run the code inside these parentheses instead
    // |_| ->  This is Rust syntax for a tiny, unnamed function, The underscore _ specifically means "I am receiving an error object here, but I am going to completely ignore it."
    // summary -> Create a variable named bind_addr. Check my computer for an environment variable named OXIDEGATE_BIND. If you find it, use it! If you don't find it (or if there's an error reading it), ignore the error and just use "127.0.0.1:8000" instead
    let bind_addr =
        std::env::var("OXIDEGATE_BIND").unwrap_or_else(|_| "127.0.0.1:8000".to_string()); //this

    let backend_url = std::env::var("OXIDEGATE_BACKEND")
        .unwrap_or_else(|_| "http://127.0.0.1:11434/v1".to_string());

    // reqwest::client::builder() -> I want to create a new HTTP client, but I want to configure some custom rules before you finalize it
    // .timeout(Duration::from_secs(300)) -> sets a hard limit on any request this client makes, if taking longer than 300 secs to complete its response, will show timeout error
    // .connect_timeout(Duration::from_secs(5)) -> if initial condition is not established in 5 secs give up
    // .build() ->I'm done giving you settings. Take everything I just said and actually build the Client for me now.
    // .context("") -> message on failure
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(300))
        .connect_timeout(Duration::from_secs(5))
        .build()
        .context("failed to build HTTP client")?;

    // we assign Arc to structs or enums, the benefit of it is that, we could use .clone() after it to use the struct or enum over and over again
    // then why not use the .clone() directly, because .clone() copies entire files and that is memory heavy, Arc store the pointers of where the data is stored
    // the true owner is now ARC and not state
    let state = Arc::new(AppState { http, backend_url });

    //build router with routes
    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/v1/chat/completions", post(chat_completions_handler))
        .with_state(state.clone());

    // tokio -> engine powering the server
    // tcplistener::bind ->  This is you asking your operating system (Mac/Windows/Linux): "Hey, can I reserve this IP address and port exclusively for my app?"
    // .with_context(|| format!(...)) -> You are telling Rust: "Here is a set of instructions on how to build an error message. ONLY run these instructions if an error actually happens!"
    let listener = tokio::net::TcpListener::bind(&bind_addr) //listener will open its ear to port 8000, if laready in use, expect(...) is error handling
        .await
        .with_context(|| format!("failed to bind {bind_addr}"))?; // we use "|| format!" so that rust only wastes time formatting that string if an error actually occurs

    // info belogns to the tracing library and uses structed outputs
    info!(backend = %state.backend_url, bind_addr = %bind_addr, "oxideGate listening");

    // .await: The server runs. Let's pretend it suddenly fails and generates a raw, confusing HyperNetworkError.
    // .context("server error"): This grabs the HyperNetworkError and wraps it in a nice bow so it now reads "server error: HyperNetworkError".
    // ?: The question mark looks at it, sees that it's an error, and instantly hits the eject button. It forces the main() function to stop whatever it was doing and return that beautifully formatted error out to the terminal, where the program shuts down cleanly.
    axum::serve(listener, app).await.context("server error")?; // axum will take the listener and our app and actually starts the web server

    Ok(())
}

// json!({"status": "ok"}) -> creates a tiny peice of json
// Json(...) to package it up nicely with the correct HTTP headers so the web browser knows it's receiving JSON
async fn health_handler() -> Json<serde_json::Value> {
    Json(json!({"status": "ok"}))
}

async fn chat_completions_handler(
    State(state): State<Arc<AppState>>, // we are using axum::extract::State ->axum automatically reacher into the router, calls .clone() on your Arc and hands this fn the pointer to your HTTP client
    Json(req): Json<ChatCompletionRequest>,
) -> Result<Response, GatewayError> {
    // the result would either be a succesfull http response or an Gateway Error
    info!(
        model = %req.model,
        messages = req.messages.len(),
        stream = req.stream,
        "forwarding to backend"
    );

    if req.stream {
        // if the client gave stream = True then we stream the resposne like chatGPT or else at one reply
        stream_completion(state, req).await
    } else {
        buffered_completion(state, req).await
    }
}

async fn buffered_completion(
    state: Arc<AppState>,
    req: ChatCompletionRequest,
) -> Result<Response, GatewayError> {
    let started = Instant::now();
    let resp = send_to_backend(&state, &req).await?; // passes the HTTP Client(state) and the user's chat(req) to a helper function "Send_to_backend", this makes the web request to the backend AI

    let parsed: ChatCompletionResponse = resp.json().await?; // AI model generates the text and repies with raw JSON text and the text is converted into struct called ChatCompletion resposne

    info!(
        e2e_ms = started.elapsed().as_millis(), // e2e_ms: looking at the stopwatch we called earlier, .elapsed() -> get the time in ms
        completion_tokens = parsed.usage.completion_tokens, // parsed-> just defined above, now parsed is a struct so we are just getting the info anout exact number of tokens(words) the AI generated
        "backend responded"
    );

    Ok(Json(parsed).into_response()) // result is to be returned, ok means success no erros
                                     // into_reponse defined in types -> put the correct http error headers( for ex 201 error)
}

/// Pipe the backend's SSE frames straight through to the client.
async fn stream_completion(
    state: Arc<AppState>,
    req: ChatCompletionRequest,
) -> Result<Response, GatewayError> {
    let started = Instant::now();
    let resp = send_to_backend(&state, &req).await?;

    // Bytes are forwarded untouched. We only observe them to record
    // time-to-first-token, which is the metric Phase 2 is built around.
    let mut seen_first = false;
    let byte_stream = resp.bytes_stream().map(move |chunk| {
        if !seen_first {
            seen_first = true;
            info!(ttft_ms = started.elapsed().as_millis(), "first token");
        }
        chunk
    });

    Ok((
        [
            (header::CONTENT_TYPE, "text/event-stream"),
            (header::CACHE_CONTROL, "no-cache"),
            (header::CONNECTION, "keep-alive"),
        ],
        Body::from_stream(byte_stream),
    )
        .into_response())
}

/// Send the request upstream and reject any non-2xx reply.
async fn send_to_backend(
    state: &AppState,
    req: &ChatCompletionRequest,
) -> Result<reqwest::Response, GatewayError> {
    let url = format!("{}/chat/completions", state.backend_url);
    let resp = state.http.post(&url).json(req).send().await?;

    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(GatewayError::BackendStatus { status, body });
    }

    Ok(resp)
}
