use anyhow::Context;
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};

/// Metric names, in one place so the handler and the tests agree.
pub const REQUESTS_TOTAL: &str = "oxidegate_requests_total";
pub const REQUEST_DURATION: &str = "oxidegate_request_duration_seconds";
pub const TTFT: &str = "oxidegate_ttft_seconds";
pub const INFLIGHT: &str = "oxidegate_inflight_requests";

/// Explicit buckets. The defaults are tuned for millisecond web handlers;
/// inference spans milliseconds to minutes, so the range has to be wider or
/// every interesting completion lands in +Inf.
const LATENCY_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0,
];

/// Installs the global recorder. Must be called once, before any metric is
/// emitted; a second call fails because the recorder is process-wide.
pub fn install() -> anyhow::Result<PrometheusHandle> {
    PrometheusBuilder::new()
        .set_buckets_for_metric(Matcher::Full(REQUEST_DURATION.to_string()), LATENCY_BUCKETS)
        .context("invalid duration buckets")?
        .set_buckets_for_metric(Matcher::Full(TTFT.to_string()), LATENCY_BUCKETS)
        .context("invalid ttft buckets")?
        .install_recorder()
        .context("failed to install prometheus recorder")
}
