//! Two schedulers. Same work. One is 4x slower.
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

async fn backend_call(id: u32, start: Instant, done: mpsc::Sender<u32>) {
    tokio::time::sleep(Duration::from_millis(500)).await;
    println!(
        "      request {id} done at {} ms",
        start.elapsed().as_millis()
    );
    let _ = done.send(id).await;
}

/// Version A: the scheduler awaits each call itself.
async fn scheduler_awaits(mut rx: mpsc::Receiver<(u32, Instant, mpsc::Sender<u32>)>) {
    while let Some((id, start, done)) = rx.recv().await {
        backend_call(id, start, done).await; // <-- the loop stops here
    }
}

/// Version B: the scheduler hands the call to a new task and moves on.
async fn scheduler_spawns(mut rx: mpsc::Receiver<(u32, Instant, mpsc::Sender<u32>)>) {
    while let Some((id, start, done)) = rx.recv().await {
        tokio::spawn(backend_call(id, start, done)); // <-- loop keeps going
    }
}

#[tokio::main]
async fn main() {
    for (name, spawns) in [
        ("A: scheduler AWAITS", false),
        ("B: scheduler SPAWNS", true),
    ] {
        println!("\n--- {name} ---  (4 requests, backend takes 500ms each)");
        let (tx, rx) = mpsc::channel(100);
        if spawns {
            tokio::spawn(scheduler_spawns(rx));
        } else {
            tokio::spawn(scheduler_awaits(rx));
        }

        let (done_tx, mut done_rx) = mpsc::channel(100);
        let start = Instant::now();
        for id in 1..=4 {
            tx.send((id, start, done_tx.clone())).await.unwrap();
        }
        drop(tx);
        drop(done_tx);

        for _ in 0..4 {
            done_rx.recv().await;
        }
        println!("      ALL DONE in {} ms", start.elapsed().as_millis());
    }
}
