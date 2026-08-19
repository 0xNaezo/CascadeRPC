//! Every metric the balancer emits, and the only place their names are spelled.
//!
//! Gathered into one module because the names are a contract with whatever
//! scrapes them: a dashboard, an alert rule and a recording rule all break
//! together when one is renamed, and finding every emission site first should
//! not be part of that. The `description:`/`unit:` metadata the exporter
//! publishes is here for the same reason.
//!
//! Nothing here knows what a node or a request is — each function takes the
//! labels already resolved, so no layer has to depend on this one to be
//! measurable.

// `::metrics` and not `metrics`: this module has the same name as the crate it
// wraps, and a bare path would be ambiguous.
use ::metrics::{Gauge, Unit, counter, gauge, histogram};

/// Holds a gauge up for as long as the guard lives.
///
/// For counting things that are *currently* happening: the increment and the
/// matching decrement cannot drift apart, because the decrement is `Drop` and
/// runs on every path out — including a cancelled future.
pub struct GaugeGuard(Gauge);

impl GaugeGuard {
    /// Increments the gauge; it goes back down when the guard is dropped.
    #[must_use]
    fn new(gauge: Gauge) -> Self {
        gauge.increment(1);

        Self(gauge)
    }
}

impl Drop for GaugeGuard {
    fn drop(&mut self) {
        self.0.decrement(1);
    }
}

/// Records one attempt against one upstream node: how it ended, and how long
/// it took.
///
/// Every path out of an attempt goes through here, so the `outcome` label
/// partitions the attempts rather than sampling them.
pub fn record_upstream(node: &str, outcome: &'static str, duration_seconds: f64) {
    counter!(
        description: "Attempts sent to upstream RPC nodes",
        "rpc_upstream_attempts",
        "node" => node.to_owned(),
        "outcome" => outcome,
    )
    .increment(1);
    histogram!(
        description: "Upstream RPC attempt duration",
        unit: Unit::Seconds,
        "rpc_upstream_duration",
        "node" => node.to_owned(),
        "outcome" => outcome,
    )
    .record(duration_seconds);
}

/// Records the end-to-end result of one client request, however many upstream
/// attempts it took.
pub fn record_request(outcome: &'static str, duration_seconds: f64) {
    counter!(
        description: "Client requests handled by the RPC load balancer",
        "rpc_requests",
        "outcome" => outcome,
    )
    .increment(1);
    histogram!(
        description: "End-to-end RPC load balancer request duration",
        unit: Unit::Seconds,
        "rpc_request_duration",
        "outcome" => outcome,
    )
    .record(duration_seconds);
}

/// Counts the requests parked because every node is rate-limited, for as long
/// as the returned guard lives.
#[must_use]
pub fn sleeping_on_rate_limit() -> GaugeGuard {
    GaugeGuard::new(gauge!(
        description: "Number of requests currently sleeping while all RPC nodes are rate-limited",
        "rpc_sleep_queue_size"
    ))
}

/// Records one health probe: how long it took, and the verdict it reached.
///
/// The verdict is a gauge and not a counter because that is the question asked
/// of it — "is this node up right now" — and the histogram beside it is what
/// says whether a node about to be marked down is merely slow.
pub fn record_probe(node: &str, tier: u8, healthy: bool, duration_seconds: f64) {
    let outcome = if healthy { "healthy" } else { "unhealthy" };

    histogram!(
        description: "Time spent completing an RPC node healthcheck",
        unit: Unit::Seconds,
        "rpc_healthcheck_duration",
        "node" => node.to_owned(),
        "outcome" => outcome,
    )
    .record(duration_seconds);
    gauge!(
        description: "Whether an RPC node passed its latest healthcheck",
        "rpc_node_healthy",
        "node" => node.to_owned(),
        "tier" => tier.to_string(),
    )
    .set(if healthy { 1.0 } else { 0.0 });
}

/// Records how many nodes the latest probe round left the router to choose
/// from.
pub fn set_healthy_nodes(count: usize) {
    gauge!(
        description: "Number of RPC nodes that passed the latest healthcheck",
        "rpc_healthy_nodes",
    )
    .set(u32::try_from(count).unwrap_or(u32::MAX));
}

/// Counts one node passed over without a request being sent, and why.
///
/// A counter with no histogram beside it: nothing was sent, so there is no
/// duration to record, and the `0.0` a shared `record_upstream` would have to
/// pass lands in the latency histogram as a real observation of zero.
///
/// `quota_exhausted` and `method_unsupported` fire at most once per request per
/// node — the router marks such a node tried and never returns to it.
/// `rate_limit` fires once per retry round, because a rate-limited node is
/// exactly the one it comes back for.
pub fn record_skip(node: &str, reason: &'static str) {
    counter!(
        description: "Nodes passed over without sending a request",
        "rpc_upstream_skips",
        "node" => node.to_owned(),
        "reason" => reason,
    )
    .increment(1);
}

/// Publishes what one node has spent in the current billing period, against the
/// usage the router stops admitting it at.
///
/// Two gauges rather than one ratio: the ratio is what an alert fires on, but
/// the absolute spend is what a provider's bill is reconciled against, and
/// dividing here throws it away. `PromQL` can take the ratio back out.
///
/// A node with no real quota reports a threshold near `u64::MAX`, which leaves
/// its ratio pinned at zero — the right shape for a node that never spills.
pub fn set_node_quota(node: &str, used: u64, threshold: u64) {
    // Past 2^53 an f64 stops resolving single units. The only quota that large
    // is the unlimited node's, where the absolute value is not the reading
    // anyone takes.
    #[allow(clippy::cast_precision_loss)]
    let (used, threshold) = (used as f64, threshold as f64);

    gauge!(
        description: "Usage a node has booked in the current billing period",
        "rpc_node_quota_used",
        "node" => node.to_owned(),
    )
    .set(used);
    gauge!(
        description: "Usage past which the router stops routing to a node",
        "rpc_node_quota_threshold",
        "node" => node.to_owned(),
    )
    .set(threshold);
}
