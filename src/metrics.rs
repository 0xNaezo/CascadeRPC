//! Every metric the balancer emits, and the only place their names are spelled.
//!
//! Gathered into one module because the names are a contract with whatever
//! scrapes them: a dashboard, an alert rule and a recording rule all break
//! together when one is renamed, and finding every emission site first should
//! not be part of that. The `description`/`unit` metadata the exporter
//! publishes is here for the same reason, in [`describe_all`].
//!
//! Everything on the request path emits through a resolved handle rather than
//! through the `counter!`/`histogram!` macros. A macro call re-registers the
//! metric every time: it re-describes it behind one process-wide `Mutex` in the
//! exporter, allocates a `Vec` for the labels and a `String` per label value,
//! then looks the key up in the registry. At request rates that mutex is the
//! serialization point of the whole server. A `Counter` is an `Arc`-like handle
//! straight to the storage — `increment` touches neither the registry nor the
//! lock. What stays on the macros is what fires once a ranking round or once a
//! minute, where the cost is not worth a handle to hold.
//!
//! Nothing here knows what a node or a request is — [`NodeMetrics`] is built
//! from a name, so no layer has to depend on this one to be measurable.

use std::sync::LazyLock;

// `::metrics` and not `metrics`: this module has the same name as the crate it
// wraps, and a bare path would be ambiguous.
use ::metrics::{
    Counter, Gauge, Histogram, Unit, counter, describe_counter, describe_gauge, describe_histogram,
    gauge, histogram,
};

/// How one attempt against an upstream node ended.
///
/// Value of the `outcome` label on `rpc_upstream_attempts` and
/// `rpc_upstream_duration`, and the index of that label's handles in
/// [`NodeMetrics`]. An enum and not a string literal per call site so the
/// compiler is the one keeping `outcome` a partition of the attempts, rather
/// than two literals that happen to match.
#[derive(Clone, Copy)]
pub enum Outcome {
    Success,
    ForwardedHttpError,
    ForwardedRpcError,
    RetryableRpcError,
    InvalidJson,
    BodyError,
    TransportError,
}

impl Outcome {
    /// Every variant, in discriminant order — see the `const` block below.
    const ALL: [Self; 7] = [
        Self::Success,
        Self::ForwardedHttpError,
        Self::ForwardedRpcError,
        Self::RetryableRpcError,
        Self::InvalidJson,
        Self::BodyError,
        Self::TransportError,
    ];
    const COUNT: usize = Self::ALL.len();

    const fn as_label(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::ForwardedHttpError => "forwarded_http_error",
            Self::ForwardedRpcError => "forwarded_rpc_error",
            Self::RetryableRpcError => "retryable_rpc_error",
            Self::InvalidJson => "invalid_json",
            Self::BodyError => "body_error",
            Self::TransportError => "transport_error",
        }
    }
}

/// Why a node was passed over without a request being sent.
///
/// Value of the `reason` label on `rpc_upstream_skips`, and the index of its
/// counter in [`NodeMetrics`].
#[derive(Clone, Copy)]
pub enum SkipReason {
    RateLimit,
    QuotaExhausted,
    MethodUnsupported,
    /// Node was at its concurrency cap, so the request went to the next node
    /// instead of queueing on this one.
    Saturated,
    /// Node is serving a penalty from a failed attempt and is not being sent
    /// traffic until it expires.
    ///
    /// Counted per routing round, not per request: a request that walks the
    /// table again — waiting out a rate limit, or failing open over a set where
    /// every node is penalized — counts one skip per round, and the fail-open
    /// round then does dial the node it just skipped.
    Penalized,
}

impl SkipReason {
    const ALL: [Self; 5] = [
        Self::RateLimit,
        Self::QuotaExhausted,
        Self::MethodUnsupported,
        Self::Saturated,
        Self::Penalized,
    ];
    const COUNT: usize = Self::ALL.len();

    const fn as_label(self) -> &'static str {
        match self {
            Self::RateLimit => "rate_limit",
            Self::QuotaExhausted => "quota_exhausted",
            Self::MethodUnsupported => "method_unsupported",
            Self::Saturated => "saturated",
            Self::Penalized => "penalized",
        }
    }
}

/// How one client request ended, however many upstream attempts it took.
///
/// Value of the `outcome` label on `rpc_requests` and `rpc_request_duration`,
/// and the index of its handles in [`REQUEST`].
#[derive(Clone, Copy)]
pub enum RequestOutcome {
    Forwarded,
    BadRequest,
    BadGateway,
    Timeout,
}

impl RequestOutcome {
    const ALL: [Self; 4] = [
        Self::Forwarded,
        Self::BadRequest,
        Self::BadGateway,
        Self::Timeout,
    ];
    const COUNT: usize = Self::ALL.len();

    const fn as_label(self) -> &'static str {
        match self {
            Self::Forwarded => "forwarded",
            Self::BadRequest => "bad_request",
            Self::BadGateway => "bad_gateway",
            Self::Timeout => "timeout",
        }
    }
}

// Every handle array below is built by mapping over `ALL` and read back by
// casting a variant to `usize`. The two agree only while `ALL` is in
// discriminant order, and a variant inserted in the middle would otherwise
// silently start counting under its neighbour's label.
const _: () = {
    let mut i = 0;
    while i < Outcome::COUNT {
        assert!(Outcome::ALL[i] as usize == i);
        i += 1;
    }
    i = 0;
    while i < SkipReason::COUNT {
        assert!(SkipReason::ALL[i] as usize == i);
        i += 1;
    }
    i = 0;
    while i < RequestOutcome::COUNT {
        assert!(RequestOutcome::ALL[i] as usize == i);
        i += 1;
    }
};

/// The counter and histogram pair for each end-to-end request outcome.
///
/// Resolved on first use, which is after the recorder is installed:
/// `main` installs it before it serves anything, and nothing else in the crate
/// records a request. Resolved against no recorder these would be no-ops for
/// the life of the process.
static REQUEST: LazyLock<[(Counter, Histogram); RequestOutcome::COUNT]> = LazyLock::new(|| {
    RequestOutcome::ALL.map(|outcome| {
        (
            counter!("rpc_requests", "outcome" => outcome.as_label()),
            histogram!("rpc_request_duration", "outcome" => outcome.as_label()),
        )
    })
});

/// The gauge counting requests parked on a rate limit. See [`REQUEST`] for why
/// it is resolved once.
static SLEEP_QUEUE: LazyLock<Gauge> = LazyLock::new(|| gauge!("rpc_sleep_queue_size"));

/// Every metric handle one node emits, resolved once when the node is built.
///
/// Held by [`crate::core::node::RpcNode`], so the request path reaches a handle
/// by field access instead of by re-registering a name. The node's own metrics
/// are the ones that fire per attempt and per retry round, which is what makes
/// them worth resolving up front.
#[derive(Clone)]
pub struct NodeMetrics {
    attempts: [Counter; Outcome::COUNT],
    durations: [Histogram; Outcome::COUNT],
    skips: [Counter; SkipReason::COUNT],
}

impl NodeMetrics {
    /// Resolves every handle for one node, against whatever recorder is
    /// installed *now* — see the ordering note in `main`.
    #[must_use]
    pub fn new(node: &str) -> Self {
        Self {
            attempts: Outcome::ALL.map(|outcome| {
                counter!(
                    "rpc_upstream_attempts",
                    "node" => node.to_owned(),
                    "outcome" => outcome.as_label(),
                )
            }),
            durations: Outcome::ALL.map(|outcome| {
                histogram!(
                    "rpc_upstream_duration",
                    "node" => node.to_owned(),
                    "outcome" => outcome.as_label(),
                )
            }),
            skips: SkipReason::ALL.map(|reason| {
                counter!(
                    "rpc_upstream_skips",
                    "node" => node.to_owned(),
                    "reason" => reason.as_label(),
                )
            }),
        }
    }

    /// Records one attempt against this node: how it ended, and how long it
    /// took.
    ///
    /// Every path out of an attempt goes through here, so the `outcome` label
    /// partitions the attempts rather than sampling them.
    pub fn record_attempt(&self, outcome: Outcome, duration_seconds: f64) {
        self.attempts[outcome as usize].increment(1);
        self.durations[outcome as usize].record(duration_seconds);
    }

    /// Counts this node passed over without a request being sent, and why.
    ///
    /// A counter with no histogram beside it: nothing was sent, so there is no
    /// duration to record, and the `0.0` a shared [`Self::record_attempt`]
    /// would have to pass lands in the latency histogram as a real observation
    /// of zero.
    ///
    /// `QuotaExhausted` and `MethodUnsupported` fire at most once per request
    /// per node — the router marks such a node tried and never returns to it.
    /// `RateLimit` fires once per retry round, because a rate-limited node is
    /// exactly the one it comes back for.
    pub fn record_skip(&self, reason: SkipReason) {
        self.skips[reason as usize].increment(1);
    }
}

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

/// Records the end-to-end result of one client request, however many upstream
/// attempts it took.
pub fn record_request(outcome: RequestOutcome, duration_seconds: f64) {
    let (count, duration) = &REQUEST[outcome as usize];

    count.increment(1);
    duration.record(duration_seconds);
}

/// Counts the requests parked because every node is rate-limited, for as long
/// as the returned guard lives.
#[must_use]
pub fn sleeping_on_rate_limit() -> GaugeGuard {
    GaugeGuard::new(SLEEP_QUEUE.clone())
}

/// Publishes what real traffic currently says about one node: whether it is
/// serving a penalty, and the latency average its answers have built up.
///
/// Gauges and not counters because that is the question asked of them — "is
/// this node taking traffic right now, and how fast is it" — and the two are
/// published together so a node marked down can be read against how slow it had
/// become first.
///
/// `ema_us` is reported in seconds, the base unit a Prometheus histogram of the
/// same latency is already in, so the two are comparable without a conversion
/// in the query. `None` — a node nothing has answered for — is published as
/// `NaN`; the caller owns the sentinel that means it, so this module still does
/// not need to know what a node is.
///
/// On the macros and not on held handles: once per node per ranking round is
/// nowhere near the rate that makes the registry hurt.
pub fn set_node_state(node: &str, tier: u8, penalized: bool, ema_us: Option<u32>) {
    gauge!(
        "rpc_node_healthy",
        "node" => node.to_owned(),
        "tier" => tier.to_string(),
    )
    .set(if penalized { 0.0 } else { 1.0 });

    // `NaN` and not "skip the series": a gauge already published keeps its last
    // value forever, so a node that goes back to unmeasured would otherwise
    // leave a stale latency on the dashboard indefinitely. `NaN` renders as a
    // gap, which is what "nothing has measured this" looks like.
    gauge!("rpc_node_latency_ema", "node" => node.to_owned(), "tier" => tier.to_string())
        .set(ema_us.map_or(f64::NAN, |us| f64::from(us) / 1_000_000.0));
}

/// Records how many nodes the latest ranking round left the router free to
/// choose from.
pub fn set_healthy_nodes(count: usize) {
    gauge!("rpc_healthy_nodes").set(u32::try_from(count).unwrap_or(u32::MAX));
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

    gauge!("rpc_node_quota_used", "node" => node.to_owned()).set(used);
    gauge!("rpc_node_quota_threshold", "node" => node.to_owned()).set(threshold);
}

/// Publishes the description and unit of every metric this module emits.
///
/// Once at startup instead of on every emission: the `description:`/`unit:`
/// arguments of `counter!` and friends are re-evaluated on each call, and the
/// exporter takes a process-wide `Mutex` to decide it already has them.
///
/// Called from [`crate::server::install_metrics_recorder`] rather than left to
/// each caller, so a metric cannot end up described against a recorder that is
/// not the one rendering it — and so nothing has to remember to call it.
pub fn describe_all() {
    describe_counter!(
        "rpc_upstream_attempts",
        "Attempts sent to upstream RPC nodes"
    );
    describe_histogram!(
        "rpc_upstream_duration",
        Unit::Seconds,
        "Upstream RPC attempt duration"
    );
    describe_counter!(
        "rpc_upstream_skips",
        "Nodes passed over without sending a request"
    );
    describe_counter!("rpc_requests", "Client requests handled by CascadeRPC");
    describe_histogram!(
        "rpc_request_duration",
        Unit::Seconds,
        "End-to-end CascadeRPC request duration"
    );
    describe_gauge!(
        "rpc_sleep_queue_size",
        "Number of requests currently sleeping while all RPC nodes are rate-limited"
    );
    describe_gauge!(
        "rpc_node_healthy",
        "Whether an RPC node is free of penalties from its recent traffic"
    );
    describe_gauge!(
        "rpc_healthy_nodes",
        "Number of RPC nodes not currently serving a penalty"
    );
    describe_gauge!(
        "rpc_node_latency_ema",
        Unit::Seconds,
        "Moving average of an RPC node's answered attempts"
    );
    describe_gauge!(
        "rpc_node_quota_used",
        "Usage a node has booked in the current billing period"
    );
    describe_gauge!(
        "rpc_node_quota_threshold",
        "Usage past which the router stops routing to a node"
    );
}
