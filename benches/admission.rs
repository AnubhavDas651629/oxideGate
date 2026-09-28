//! Experiment 4 — what does in-process quota state cost under contention?
//! (ROADMAP D2: "measure the contention in-process, then argue for Redis
//! with numbers".)
//!
//!   cargo bench --bench admission
//!
//! Each benchmark runs one admission + release (the work every request does
//! once) on T threads at the same time, and reports wall time per operation
//! across all threads — i.e. 1 / throughput. If contention didn't matter,
//! the number would fall as T rises; where a lock serialises, it doesn't.
//!
//! Variants:
//! - tenant_mutex/same_tenant — the real code, every thread on ONE tenant:
//!   worst case, everyone fights over one lock.
//! - tenant_mutex/own_tenant — the real code, one tenant per thread: how
//!   the per-tenant design behaves with many tenants.
//! - global_mutex/own_tenant — the same arithmetic, but all tenants behind a
//!   single Mutex<HashMap>: the design we didn't pick.
//! - atomic_counter/same — a bare fetch_add/fetch_sub, no buckets: the
//!   floor for any correct shared counter.
//! - loopback_tcp_rtt — one byte to a local echo server and back: the floor
//!   for ANY networked counter store (Redis, etc.) before it does any work.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use oxidegate::tenants::{Tenant, TenantConfig, Tenants, Tier};

const THREADS: &[usize] = &[1, 2, 4, 8, 16];

/// Run `iters` operations split across `threads` threads, all released at
/// once by a barrier. Returns wall time from release to the last finish.
fn contended<F>(threads: usize, iters: u64, op: F) -> Duration
where
    F: Fn(usize) + Sync,
{
    let per_thread = (iters / threads as u64).max(1);
    let barrier = Barrier::new(threads + 1);
    let start = std::thread::scope(|s| {
        for t in 0..threads {
            let (barrier, op) = (&barrier, &op);
            s.spawn(move || {
                barrier.wait();
                for _ in 0..per_thread {
                    op(t);
                }
            });
        }
        barrier.wait();
        Instant::now()
    }); // scope joins every thread before returning
    start.elapsed()
}

fn unlimited(id: &str) -> TenantConfig {
    TenantConfig {
        id: id.into(),
        api_key: id.into(),
        tier: Tier::Paid,
        tokens_per_minute: 1_000_000_000_000_000,
        requests_per_second: 1e15,
        max_concurrent: u32::MAX,
    }
}

fn tenants(n: usize) -> Vec<Arc<Tenant>> {
    let configs: Vec<_> = (0..n).map(|i| unlimited(&format!("t{i}"))).collect();
    let registry = Tenants::from_configs(&configs).expect("configs");
    configs
        .iter()
        .map(|c| registry.resolve(Some(&c.api_key)).expect("tenant"))
        .collect()
}

/// Admit + release, exactly what one request costs the quota system.
fn admit_release(t: &Arc<Tenant>) {
    let mut lease = t.admit(64).expect("unlimited tenant admits");
    lease.record_tokens(20);
    drop(lease);
}

/// The rejected alternative: every tenant's state behind one lock. Same
/// arithmetic as Tenant::admit + Lease::drop — two lock acquisitions, two
/// lazy bucket refills, compare, adjust.
struct Counters {
    active: u32,
    req_level: f64,
    tok_level: f64,
    last: Instant,
}

fn global_admit_release(map: &Mutex<HashMap<usize, Counters>>, key: usize) {
    let refill = |c: &mut Counters| {
        let now = Instant::now();
        let dt = now.saturating_duration_since(c.last).as_secs_f64();
        c.req_level = (c.req_level + dt * 1e15).min(1e15);
        c.tok_level = (c.tok_level + dt * 1e15).min(1e15);
        c.last = now;
    };
    {
        let mut m = map.lock().expect("lock");
        let c = m.get_mut(&key).expect("tenant");
        refill(c);
        if c.req_level >= 1.0 && c.tok_level >= 64.0 {
            c.req_level -= 1.0;
            c.tok_level -= 64.0;
            c.active += 1;
        }
    }
    {
        let mut m = map.lock().expect("lock");
        let c = m.get_mut(&key).expect("tenant");
        refill(c);
        c.active -= 1;
        c.tok_level += 44.0;
    }
}

fn bench_admission(c: &mut Criterion) {
    let mut g = c.benchmark_group("admission");
    g.sample_size(30).measurement_time(Duration::from_secs(4));

    for &threads in THREADS {
        let one = tenants(1);
        g.bench_with_input(
            BenchmarkId::new("tenant_mutex/same_tenant", threads),
            &threads,
            |b, &t| b.iter_custom(|iters| contended(t, iters, |_| admit_release(&one[0]))),
        );

        let many = tenants(threads);
        g.bench_with_input(
            BenchmarkId::new("tenant_mutex/own_tenant", threads),
            &threads,
            |b, &t| b.iter_custom(|iters| contended(t, iters, |i| admit_release(&many[i]))),
        );

        let map: Mutex<HashMap<usize, Counters>> = Mutex::new(
            (0..threads)
                .map(|i| {
                    let c = Counters {
                        active: 0,
                        req_level: 1e15,
                        tok_level: 1e15,
                        last: Instant::now(),
                    };
                    (i, c)
                })
                .collect(),
        );
        g.bench_with_input(
            BenchmarkId::new("global_mutex/own_tenant", threads),
            &threads,
            |b, &t| b.iter_custom(|iters| contended(t, iters, |i| global_admit_release(&map, i))),
        );

        let counter = AtomicU32::new(0);
        g.bench_with_input(
            BenchmarkId::new("atomic_counter/same", threads),
            &threads,
            |b, &t| {
                b.iter_custom(|iters| {
                    contended(t, iters, |_| {
                        counter.fetch_add(1, Ordering::AcqRel);
                        counter.fetch_sub(1, Ordering::AcqRel);
                    })
                })
            },
        );
    }
    g.finish();
}

/// One byte over loopback TCP and back. Nagle is disabled on both ends, or
/// the kernel would batch tiny writes and add ~40ms of artificial delay.
fn bench_loopback(c: &mut Criterion) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    std::thread::spawn(move || {
        if let Ok((mut s, _)) = listener.accept() {
            let _ = s.set_nodelay(true);
            let mut buf = [0u8; 1];
            while s.read_exact(&mut buf).is_ok() {
                if s.write_all(&buf).is_err() {
                    break;
                }
            }
        }
    });
    let mut client = TcpStream::connect(addr).expect("connect");
    client.set_nodelay(true).expect("nodelay");

    let mut g = c.benchmark_group("network_floor");
    g.sample_size(30).measurement_time(Duration::from_secs(4));
    g.bench_function("loopback_tcp_rtt", |b| {
        let mut buf = [0u8; 1];
        b.iter(|| {
            client.write_all(&[7]).expect("write");
            client.read_exact(&mut buf).expect("read");
        })
    });
    g.finish();
}

criterion_group!(benches, bench_admission, bench_loopback);
criterion_main!(benches);
