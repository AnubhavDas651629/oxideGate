use axum::{routing::get, Json, Router};
use serde_json::json;
use std::net::SocketAddr;

#[tokio::main]
async fn main() {
    //build router with routes
    let app = Router::new().route("/health", get(health_handler));

    //Bind to localhost:8000
    let addr = SocketAddr::from(([127, 0, 0, 1], 8000));
    println!("listening on {}", addr);

    //run the server
    let listener = tokio::net::TcpListener::bind(addr) //listener will open its ear to port 8000, if laready in use, expect(...) is error handling
        .await
        .expect("failed to bind");

    axum::serve(listener, app).await.expect("server error"); // axum will take the listener and our app and actually starts the web server
}

// json!({"status": "ok"}) -> creates a tiny peice of json
// Json(...) to package it up nicely with the correct HTTP headers so the web browser knows it's receiving JSON
async fn health_handler() -> Json<serde_json::Value> {
    Json(json!({"status": "ok"}))
}
