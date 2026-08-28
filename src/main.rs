use axum::{routing::get, Json, Router};
use serde_json::json;
use std::net::SocketAddr;

// tokio::main
async fn main() {
    //build router with routes
    let app = Router::new().route("/heath", get(health_handler));

    //Bind to localhost:8000
    let addr = SocketAddr::from(([127, 0, 0, 1], 8000));
    println!("listening on {}", addr);

    //run the server
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("failed to bind");

    axum::serve(listener, app).await.expect("server error");
}

async fn health_handler() -> Json<serde_json::Value> {
    Json(json!({"status": "ok"}))
}
