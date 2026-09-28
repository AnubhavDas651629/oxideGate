//! Tenants: who is calling, and what they are allowed to do (ROADMAP D6).
//!
//! Every limit is checked at admission, before a request is queued:
//! - request rate: token bucket, `requests_per_second`
//! - concurrency: requests this tenant has in the system (queued or at the
//!   backend), `max_concurrent`
//! - token budget: tokens per minute, reserve-then-refund
//!
//! State is in-process (ROADMAP D2): one small Mutex per tenant. Tenants
//! never share a lock, so contention only happens between requests of the
//! *same* tenant. What that costs is Experiment 4.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Instant;

use serde::Deserialize;
use thiserror::Error;

use crate::telemetry;

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Tier {
    Free,
    Paid,
}

impl Tier {
    /// Share of dispatches when tenants are competing: a backlogged paid
    /// tenant is served twice for every once a free one is.
    pub fn weight(self) -> f64 {
        match self {
            Tier::Free => 0.5,
            Tier::Paid => 1.0,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Tier::Free => "free",
            Tier::Paid => "paid",
        }
    }
}

/// One entry in the tenants file.
#[derive(Debug, Clone, Deserialize)]
pub struct TenantConfig {
    pub id: String,
    pub api_key: String,
    pub tier: Tier,
    /// Completion tokens per minute, refilled continuously.
    pub tokens_per_minute: u64,
    pub requests_per_second: f64,
    /// Requests this tenant may have queued or in flight at once.
    pub max_concurrent: u32,
}

#[derive(Debug, Deserialize)]
struct TenantsFile {
    tenants: Vec<TenantConfig>,
}

#[derive(Debug, Error)]
pub enum TenantsError {
    #[error("could not read tenants file: {0}")]
    Io(#[from] std::io::Error),
    #[error("could not parse tenants file: {0}")]
    Parse(#[from] serde_json::Error),
    #[error("duplicate api_key for tenant {0}")]
    DuplicateKey(String),
    #[error("tenant {0}: requests_per_second must be positive")]
    BadRate(String),
}

/// Why a tenant's request was refused at admission. All become 429s.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum AdmitError {
    #[error("tenant request rate exceeded")]
    RateLimited,
    #[error("tenant concurrency limit reached")]
    ConcurrencyLimited,
    #[error("tenant token budget exhausted")]
    TokenBudget,
}

/// A token bucket: holds up to `capacity`, refills at `per_sec`.
///
/// Refill is computed lazily from elapsed time whenever the bucket is
/// touched — no background task ticking every bucket.
#[derive(Debug)]
struct Bucket {
    capacity: f64,
    per_sec: f64,
    level: f64,
    last: Instant,
}

impl Bucket {
    fn full(capacity: f64, per_sec: f64, now: Instant) -> Self {
        Bucket {
            capacity,
            per_sec,
            level: capacity,
            last: now,
        }
    }

    /// A bucket that never runs out. f64 infinity survives + and - intact.
    fn unlimited(now: Instant) -> Self {
        Bucket::full(f64::INFINITY, f64::INFINITY, now)
    }

    fn refill(&mut self, now: Instant) {
        let dt = now.saturating_duration_since(self.last).as_secs_f64();
        if dt > 0.0 {
            self.level = (self.level + dt * self.per_sec).min(self.capacity);
        }
        self.last = now;
    }
}

#[derive(Debug)]
struct TenantState {
    requests: Bucket,
    tokens: Bucket,
    active: u32,
}

#[derive(Debug)]
pub struct Tenant {
    pub id: Arc<str>,
    pub tier: Tier,
    max_concurrent: u32,
    state: Mutex<TenantState>,
}

impl Tenant {
    fn from_config(cfg: &TenantConfig, now: Instant) -> Self {
        let tpm = cfg.tokens_per_minute as f64;
        Tenant {
            id: Arc::from(cfg.id.as_str()),
            tier: cfg.tier,
            max_concurrent: cfg.max_concurrent,
            state: Mutex::new(TenantState {
                // Burst of one second's worth, and at least one request.
                requests: Bucket::full(
                    cfg.requests_per_second.max(1.0),
                    cfg.requests_per_second,
                    now,
                ),
                tokens: Bucket::full(tpm, tpm / 60.0, now),
                active: 0,
            }),
        }
    }

    fn unlimited(id: &str, now: Instant) -> Self {
        Tenant {
            id: Arc::from(id),
            tier: Tier::Paid,
            max_concurrent: u32::MAX,
            state: Mutex::new(TenantState {
                requests: Bucket::unlimited(now),
                tokens: Bucket::unlimited(now),
                active: 0,
            }),
        }
    }

    /// std Mutex, not tokio's: the critical section is a few arithmetic
    /// operations and never awaits, and a std lock is cheaper. (A tokio
    /// Mutex exists for locks held *across* an .await.)
    ///
    /// If a thread panicked while holding the lock, it's "poisoned". The
    /// state is plain counters that can't be left half-updated in a way
    /// that matters, so we take it anyway rather than failing every
    /// request for this tenant forever.
    fn lock(&self) -> MutexGuard<'_, TenantState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Check every limit; if all pass, take from each and return a Lease.
    ///
    /// All checks happen before anything is taken, under one lock, so a
    /// refused request has no side effects: a rate-limited request must
    /// not also eat token budget.
    ///
    /// `self: &Arc<Self>` lets the Lease hold its own Arc to this tenant,
    /// so it can hand things back when dropped, wherever that happens.
    pub fn admit(self: &Arc<Self>, reserve_tokens: u64) -> Result<Lease, AdmitError> {
        let now = Instant::now();
        let mut st = self.lock();

        if st.active >= self.max_concurrent {
            return Err(AdmitError::ConcurrencyLimited);
        }
        st.requests.refill(now);
        if st.requests.level < 1.0 {
            return Err(AdmitError::RateLimited);
        }
        st.tokens.refill(now);
        if st.tokens.level < reserve_tokens as f64 {
            return Err(AdmitError::TokenBudget);
        }

        st.requests.level -= 1.0;
        st.tokens.level -= reserve_tokens as f64;
        st.active += 1;

        Ok(Lease {
            tenant: Arc::clone(self),
            reserved: reserve_tokens,
            used: 0,
        })
    }

    /// Tokens available right now. For tests and diagnostics.
    pub fn tokens_available(&self) -> f64 {
        let mut st = self.lock();
        st.tokens.refill(Instant::now());
        st.tokens.level
    }

    /// Requests currently holding a lease. For tests and diagnostics.
    pub fn active(&self) -> u32 {
        self.lock().active
    }
}

/// A tenant's claim on its limits for one request: one concurrency slot and
/// `reserved` tokens. Dropping it returns the slot and refunds whatever
/// part of the reservation wasn't used.
///
/// Like BackendSlot, release is tied to Drop, so every exit path — success,
/// error, client disconnect mid-stream — settles the account exactly once.
#[derive(Debug)]
pub struct Lease {
    tenant: Arc<Tenant>,
    reserved: u64,
    used: u64,
}

impl Lease {
    pub fn tenant(&self) -> &Arc<Tenant> {
        &self.tenant
    }

    /// Count completion tokens as they are forwarded.
    pub fn record_tokens(&mut self, n: u64) {
        self.used += n;
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        let now = Instant::now();
        let mut st = self.tenant.lock();
        st.active = st.active.saturating_sub(1);
        st.tokens.refill(now);
        // Refund the unused part of the reservation. If the backend
        // produced MORE than reserved, the excess is charged as debt: the
        // level can go negative and the tenant waits for it to refill.
        let delta = self.reserved as f64 - self.used as f64;
        st.tokens.level = (st.tokens.level + delta).min(st.tokens.capacity);
        drop(st);

        metrics::counter!(telemetry::TOKENS_TOTAL, "tenant" => self.tenant.id.to_string())
            .increment(self.used);
    }
}

/// All tenants, looked up by API key. Built once at startup, then only
/// read, so the map itself needs no lock.
#[derive(Debug)]
pub struct Tenants {
    by_key: HashMap<String, Arc<Tenant>>,
    /// Single-tenant mode: no tenants file, no auth, no limits. Keeps every
    /// pre-multi-tenancy benchmark reproducible unchanged.
    anonymous: Option<Arc<Tenant>>,
}

impl Tenants {
    pub fn anonymous() -> Self {
        Tenants {
            by_key: HashMap::new(),
            anonymous: Some(Arc::new(Tenant::unlimited("anonymous", Instant::now()))),
        }
    }

    pub fn from_configs(configs: &[TenantConfig]) -> Result<Self, TenantsError> {
        let now = Instant::now();
        let mut by_key = HashMap::new();
        for cfg in configs {
            let rps = cfg.requests_per_second;
            if rps.is_nan() || rps <= 0.0 {
                return Err(TenantsError::BadRate(cfg.id.clone()));
            }
            let tenant = Arc::new(Tenant::from_config(cfg, now));
            if by_key.insert(cfg.api_key.clone(), tenant).is_some() {
                return Err(TenantsError::DuplicateKey(cfg.id.clone()));
            }
        }
        Ok(Tenants {
            by_key,
            anonymous: None,
        })
    }

    pub fn load(path: &Path) -> Result<Self, TenantsError> {
        let text = std::fs::read_to_string(path)?;
        let file: TenantsFile = serde_json::from_str(&text)?;
        Self::from_configs(&file.tenants)
    }

    pub fn is_anonymous(&self) -> bool {
        self.anonymous.is_some()
    }

    pub fn len(&self) -> usize {
        self.by_key.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_key.is_empty()
    }

    /// Resolve a bearer token to a tenant. In anonymous mode every caller
    /// is the anonymous tenant, key or no key.
    pub fn resolve(&self, api_key: Option<&str>) -> Option<Arc<Tenant>> {
        if let Some(anon) = &self.anonymous {
            return Some(Arc::clone(anon));
        }
        api_key.and_then(|k| self.by_key.get(k)).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn cfg(tpm: u64, rps: f64, conc: u32) -> TenantConfig {
        TenantConfig {
            id: "t".into(),
            api_key: "k".into(),
            tier: Tier::Free,
            tokens_per_minute: tpm,
            requests_per_second: rps,
            max_concurrent: conc,
        }
    }

    fn tenant(c: TenantConfig) -> Arc<Tenant> {
        Arc::new(Tenant::from_config(&c, Instant::now()))
    }

    #[test]
    fn concurrency_cap_is_released_on_drop() {
        let t = tenant(cfg(1_000_000, 1000.0, 2));
        let a = t.admit(1).unwrap();
        let _b = t.admit(1).unwrap();
        assert_eq!(t.admit(1).unwrap_err(), AdmitError::ConcurrencyLimited);
        drop(a);
        assert!(t.admit(1).is_ok());
    }

    #[test]
    fn rate_limit_allows_burst_then_refuses() {
        let t = tenant(cfg(1_000_000, 3.0, 100));
        let leases: Vec<_> = (0..3).map(|_| t.admit(1).unwrap()).collect();
        assert_eq!(t.admit(1).unwrap_err(), AdmitError::RateLimited);
        drop(leases);
        // Releasing leases must NOT give back rate: that's time-based only.
        assert_eq!(t.admit(1).unwrap_err(), AdmitError::RateLimited);
    }

    #[test]
    fn rate_refills_over_time() {
        let t = tenant(cfg(1_000_000, 100.0, 1000));
        for _ in 0..100 {
            drop(t.admit(1).unwrap());
        }
        assert_eq!(t.admit(1).unwrap_err(), AdmitError::RateLimited);
        std::thread::sleep(Duration::from_millis(30)); // ~3 requests' worth
        assert!(t.admit(1).is_ok());
    }

    #[test]
    fn reservation_is_taken_up_front_and_unused_part_refunded() {
        let t = tenant(cfg(600, 1000.0, 100)); // 600/min = 10 tokens/s refill
        let mut lease = t.admit(500).unwrap();
        assert!(
            t.tokens_available() < 101.0,
            "500 must be reserved at admission"
        );
        // A second request that would overshoot is refused, even though
        // nothing has actually been generated yet.
        assert_eq!(t.admit(200).unwrap_err(), AdmitError::TokenBudget);

        lease.record_tokens(20);
        drop(lease);
        let avail = t.tokens_available();
        assert!(
            (580.0..=600.0).contains(&avail),
            "only the 20 used should be charged, available={avail}"
        );
    }

    #[test]
    fn overuse_beyond_reservation_becomes_debt() {
        let t = tenant(cfg(600, 1000.0, 100));
        let mut lease = t.admit(100).unwrap();
        lease.record_tokens(1000);
        drop(lease);
        assert!(t.tokens_available() < 0.0, "excess must be charged");
        assert_eq!(t.admit(1).unwrap_err(), AdmitError::TokenBudget);
    }

    #[test]
    fn refused_request_has_no_side_effects() {
        let t = tenant(cfg(100, 1000.0, 100));
        let before = t.tokens_available();
        assert_eq!(t.admit(500).unwrap_err(), AdmitError::TokenBudget);
        assert_eq!(t.active(), 0);
        assert!(t.tokens_available() >= before - 0.01);
    }

    #[test]
    fn anonymous_mode_resolves_anything_and_never_limits() {
        let tenants = Tenants::anonymous();
        let t = tenants.resolve(None).unwrap();
        let leases: Vec<_> = (0..10_000).map(|_| t.admit(1_000_000).unwrap()).collect();
        assert_eq!(t.active(), 10_000);
        drop(leases);
    }

    #[test]
    fn keyed_mode_rejects_unknown_and_missing_keys() {
        let tenants = Tenants::from_configs(&[cfg(1, 1.0, 1)]).unwrap();
        assert!(tenants.resolve(Some("k")).is_some());
        assert!(tenants.resolve(Some("nope")).is_none());
        assert!(tenants.resolve(None).is_none());
    }

    #[test]
    fn duplicate_keys_are_a_config_error() {
        let err = Tenants::from_configs(&[cfg(1, 1.0, 1), cfg(1, 1.0, 1)]).unwrap_err();
        assert!(matches!(err, TenantsError::DuplicateKey(_)));
    }
}
