//! One upstream RPC node: its address, the limits it is served under, and the
//! health the probe loop measures for it.
//!
//! A node is built once per config load and never mutated afterwards. The
//! request path only touches its atomics and its permits, so a reload can
//! republish the whole set while requests are in flight.
//!
//! Nothing here reads a file or knows the shape of the config: the config layer
//! fills in a [`NewNode`] and this turns it into a node. See
//! [`crate::provider::load_config::build_nodes`].

use anyhow::{Context, Result};
use governor::{
    Quota, RateLimiter,
    clock::{Clock, DefaultClock},
    middleware::NoOpMiddleware,
    state::{InMemoryState, NotKeyed},
};
use std::{
    num::NonZeroU32,
    sync::LazyLock,
    sync::atomic::{AtomicU32, Ordering},
    time::Duration,
};
use tokio::sync::{Semaphore, SemaphorePermit};
use tokio::time::Instant;
use url::Url;

use crate::metrics::NodeMetrics;
use crate::protocol::cost_table::ProviderCostTable;

/// A `governor` limiter with every knob at its default: one unkeyed bucket,
/// state in memory, no middleware. Named because the full path appears in
/// [`RpcNode`]'s type.
pub type DefaultDirectRateLimiter<MW = NoOpMiddleware<<DefaultClock as Clock>::Instant>> =
    RateLimiter<NotKeyed, InMemoryState, DefaultClock, MW>;

/// The clock every rate limiter is built on and measured against.
///
/// One per process: `DefaultClock` is a `QuantaClock`, and building one per
/// rate-limited attempt was calibration work repeated on the hot path for a
/// value that never changes. Handing the same clock to the limiter keeps the
/// bucket's own timestamps and the wait time reported for it on one source
/// instead of two instances that merely happen to agree.
static CLOCK: LazyLock<DefaultClock> = LazyLock::new(DefaultClock::default);

/// The instant every penalty deadline is measured from.
///
/// Deadlines are seconds since this point and not wall-clock time: a penalty is
/// an interval, and `SystemTime` is not monotonic — an NTP step backwards would
/// strand a node in its penalty for hours, one forwards would clear it at once.
///
/// Seconds and not milliseconds because nothing reads finer: the penalty is
/// [`PENALTY_SECS`] long and `Retry-After` is delta-seconds by specification.
/// It also puts the `u32` wrap 136 years out instead of 49.7 days.
static EPOCH: LazyLock<Instant> = LazyLock::new(Instant::now);

/// How long a node stays out of rotation after one failed attempt.
///
/// Fixed, not exponential: a node that is still broken fails its first
/// re-admitted attempt and is penalized again, so the backoff falls out of the
/// cycle for free. A permanently dead node costs 12 attempts a minute, which is
/// nothing next to the counter and the reset that an explicit exponent needs.
const PENALTY_SECS: u32 = 5;

/// The EMA's smoothing shift: `alpha = 1 / 2^LATENCY_SHIFT`.
///
/// A shift and not a float multiply — this runs on every answered attempt.
const LATENCY_SHIFT: u32 = 3;

/// Latency of a node nothing has measured yet. Sorts last in
/// [`crate::core::topology::Topology::rank`], so an unknown node is offered the
/// request only once every measured one has turned it down.
const UNMEASURED: u32 = u32::MAX;

/// Seconds from [`EPOCH`] to `at`, the unit every penalty deadline is in.
///
/// Takes the instant rather than reading the clock: both callers already have
/// one — the router the instant it stamped the request with, the upstream layer
/// the instant it started the attempt at — and a clock read per request is not
/// free at the rates this balancer is built for.
#[must_use]
pub fn seconds_since_start(at: Instant) -> u32 {
    u32::try_from(at.saturating_duration_since(*EPOCH).as_secs()).unwrap_or(u32::MAX)
}

/// What [`RpcNode::new`] needs to build a node, gathered into one struct
/// because the list is long enough that positional arguments stop being
/// readable.
pub struct NewNode {
    pub name: String,
    pub url: String,
    pub rps_limit: u32,
    pub max_concurrent: usize,
    pub tier: u8,
    pub method_costs: ProviderCostTable,
    /// Quota for one billing period, in whatever unit the provider bills in.
    /// Only used to derive [`RpcNode::spillover_threshold`].
    pub monthly_limit: u64,
    /// Share of `monthly_limit` the node may spend before the router starts
    /// skipping it, in percent.
    pub spillover_percent: u8,
    pub reset_day: u8,
}

/// The two limiters every attempt goes through, kept on a cache line of their
/// own.
///
/// The alignment is the point: unpadded, the allocator is free to lay a node's
/// bucket next to another node's health flag, and one health round then
/// invalidates the line the router reads its limiter from.
#[repr(align(64))]
pub struct NodeLimits {
    pub rate_limiting: DefaultDirectRateLimiter,
    pub concurrency: Semaphore,
}

/// The latency a node's own traffic has measured for it, on a cache line of
/// its own.
///
/// Write-hot: every answered attempt updates it. Read only by the ranking loop,
/// once a second. It sits apart from [`NodePenalty`] for exactly that reason —
/// sharing a line would invalidate the penalty on every answer, and the penalty
/// is read by every request.
#[repr(align(64))]
pub struct NodeLatency {
    /// Exponential moving average of answered attempts, in microseconds.
    /// [`UNMEASURED`] until the node answers for the first time.
    pub ema_us: AtomicU32,
}

/// How long a node is held out of rotation, on a cache line of its own.
///
/// Read-mostly, the opposite traffic to [`NodeLatency`]: every request reads it
/// while only a failed attempt writes it, so the line stays shared across cores
/// and the read costs nothing beyond an L1 hit.
#[repr(align(64))]
pub struct NodePenalty {
    /// Seconds from [`EPOCH`] until which the node is skipped. `0` means the
    /// node has never failed.
    pub until_s: AtomicU32,
}

/// Why a node cannot take an attempt right now.
///
/// Both are temporary by construction — the router treats neither as a failure
/// of the node.
#[derive(Debug)]
pub enum Unavailable {
    /// Rate limiter has no token; the duration is how long until it does.
    RateLimited(Duration),
    /// Every concurrency permit is in use. Also covers a closed semaphore,
    /// which nothing in the crate ever does, and which would read as a node
    /// that is permanently busy rather than as a panic.
    Saturated,
}

/// A node is always shared as `Arc<RpcNode>` (see [`crate::core::topology`]),
/// so nothing inside it carries an `Arc` of its own: the fields are inline and
/// share the node's single allocation.
pub struct RpcNode {
    /// Index of this node's counter in [`crate::quotas::state::GlobalQuotaState`],
    /// assigned by `assign_ids` and tied to [`Self::name`], not to the node's
    /// position in the config.
    pub id: usize,
    pub name: String,
    pub url: Url,
    pub limits: NodeLimits,
    pub tier: u8,
    pub latency: NodeLatency,
    pub penalty: NodePenalty,
    pub method_costs: ProviderCostTable,
    /// Usage past which the router stops routing to this node: `monthly_limit`
    /// scaled by `spillover_percent`. Traffic spills to the next tier a little
    /// before the provider's quota is actually gone.
    pub spillover_threshold: u64,
    /// Day of the month this node's usage counter is zeroed on. See
    /// [`crate::quotas::period`].
    pub reset_day: u8,
    /// This node's metric handles, resolved once here so the request path never
    /// goes back through the registry. See [`NodeMetrics`].
    pub metrics: NodeMetrics,
}

impl RpcNode {
    /// Creates a node with no quota slot yet, no measured latency and no
    /// penalty against it.
    ///
    /// # Errors
    ///
    /// Returns an error if `rps_limit` is 0 or `url` is not a valid URL.
    pub fn new(config: NewNode) -> Result<Self> {
        let non_zero_rate_limiting = NonZeroU32::new(config.rps_limit).ok_or_else(|| {
            anyhow::anyhow!("Fatal error: RPS for node '{}' cannot be 0", config.name)
        })?;

        let quota = Quota::per_second(non_zero_rate_limiting);
        let url = Url::parse(&config.url).with_context(|| {
            format!(
                "Fatal error: invalid URL for node '{}': {}",
                config.name, config.url
            )
        })?;

        // u128 multiply avoids overflow when monthly_limit == u64::MAX (unlimited nodes).
        let spillover_threshold =
            ((config.monthly_limit as u128) * u128::from(config.spillover_percent) / 100) as u64;

        Ok(Self {
            // Placeholder: the real slot is handed out by `assign_ids`, which
            // sees the whole node set and can keep a name on the counter it
            // was already spending.
            id: 0,
            metrics: NodeMetrics::new(&config.name),
            name: config.name,
            url,
            limits: NodeLimits {
                rate_limiting: RateLimiter::direct_with_clock(quota, CLOCK.clone()),
                concurrency: Semaphore::new(config.max_concurrent),
            },
            tier: config.tier,
            latency: NodeLatency {
                ema_us: AtomicU32::new(UNMEASURED),
            },
            penalty: NodePenalty {
                until_s: AtomicU32::new(0),
            },
            method_costs: config.method_costs,
            spillover_threshold,
            reset_day: config.reset_day,
        })
    }

    /// Takes a concurrency permit for one attempt, if the node can serve one
    /// right now. The permit is held for as long as the attempt runs.
    ///
    /// Never waits. A node at its concurrency cap is a node that is not
    /// accepting the request, and the router's job is to offer it to the next
    /// node rather than to queue on this one — see [`Unavailable::Saturated`].
    ///
    /// # Errors
    ///
    /// Returns why the node cannot take the attempt: out of permits, or out of
    /// rate-limit tokens together with how long until one returns.
    pub fn try_admit(&self) -> Result<SemaphorePermit<'_>, Unavailable> {
        // Permit before token: a rate-limit token taken for an attempt that
        // then finds no permit is spent on nothing, and a saturated node would
        // drain its own bucket while it is too busy to use it.
        let permit = self
            .limits
            .concurrency
            .try_acquire()
            .map_err(|_| Unavailable::Saturated)?;

        // Dropping `permit` on this path hands it straight back: the attempt it
        // was taken for is not happening.
        self.check_rate()
            .map_err(Unavailable::RateLimited)
            .map(|()| permit)
    }

    /// Takes a rate-limit token, or reports how long until one returns.
    ///
    /// Separate from [`Self::try_admit`] because a caller that already waited
    /// for a permit still has to pass the rate limiter, and must not go back
    /// through the permit half to do it.
    ///
    /// # Errors
    ///
    /// Returns how long the bucket needs to refill. The token is consumed on
    /// success, so every `Ok` here is an attempt that has to happen.
    pub fn check_rate(&self) -> Result<(), Duration> {
        self.limits
            .rate_limiting
            .check()
            .map_err(|err| err.wait_time_from(CLOCK.now()))
    }

    /// Whether the node is serving out a penalty at `now_s`.
    ///
    /// One relaxed load of a read-mostly cache line, and the only thing the
    /// request path asks about a node's health. `now_s` comes in from the
    /// caller — see [`seconds_since_start`].
    #[must_use]
    pub fn is_penalized(&self, now_s: u32) -> bool {
        now_s < self.penalty.until_s.load(Ordering::Relaxed)
    }

    /// Folds one answered attempt into the node's latency, and clears any
    /// penalty standing against it.
    ///
    /// `started` is the instant the attempt began, which the caller already
    /// holds for its own metrics — measuring the round trip costs no extra
    /// clock read.
    ///
    /// The read-modify-write is three relaxed operations and not a CAS loop: a
    /// lost update under concurrency drops one sample out of an average, which
    /// is the kind of error an average is made of. Paying a retry loop per
    /// answer to avoid it would be the more expensive mistake.
    pub fn observe_answer(&self, started: Instant) {
        let sample_us = u32::try_from(started.elapsed().as_micros()).unwrap_or(UNMEASURED - 1);

        let ema_us = self.latency.ema_us.load(Ordering::Relaxed);
        let next_us = if ema_us == UNMEASURED {
            sample_us
        } else {
            (ema_us - (ema_us >> LATENCY_SHIFT)).saturating_add(sample_us >> LATENCY_SHIFT)
        };

        self.latency.ema_us.store(next_us, Ordering::Relaxed);

        // Guarded so the common path leaves the read-mostly penalty line
        // shared: a node that is not penalized — every node, nearly always —
        // reads a zero and writes nothing. The store is for the node that just
        // answered while serving a penalty, which is how a recovered node
        // returns to rotation without waiting the deadline out.
        if self.penalty.until_s.load(Ordering::Relaxed) != 0 {
            self.penalty.until_s.store(0, Ordering::Relaxed);
        }
    }

    /// Holds the node out of rotation after a failed attempt.
    ///
    /// `retry_after_s` is what the upstream asked for on a 429, or `0` when it
    /// asked for nothing; the longer of it and [`PENALTY_SECS`] wins, so a
    /// provider can extend its own cooldown but never shorten it below what the
    /// balancer would have applied anyway.
    ///
    /// The latency EMA is deliberately left alone: a node coming out of a
    /// penalty keeps the estimate it had when it broke, so it re-enters at the
    /// back of its tier and is offered a request only once the nodes ahead of it
    /// are busy. That is a probation period at the cost of not writing a field.
    pub fn penalize(&self, at: Instant, retry_after_s: u32) {
        let until_s = seconds_since_start(at).saturating_add(PENALTY_SECS.max(retry_after_s));

        self.penalty.until_s.store(until_s, Ordering::Relaxed);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn dummy_node(monthly_limit: u64, spillover_percent: u8) -> RpcNode {
        RpcNode::new(NewNode {
            name: "test".into(),
            url: "http://localhost:9".into(),
            rps_limit: 1,
            max_concurrent: 1,
            tier: 0,
            method_costs: ProviderCostTable::default(),
            monthly_limit,
            spillover_percent,
            reset_day: 1,
        })
        .unwrap()
    }

    #[test]
    fn spillover_threshold_basic() {
        assert_eq!(dummy_node(1000, 95).spillover_threshold, 950);
        assert_eq!(dummy_node(1000, 100).spillover_threshold, 1000);
        assert_eq!(dummy_node(1000, 1).spillover_threshold, 10);
    }

    #[test]
    fn spillover_threshold_no_overflow_at_max() {
        // Regression: monthly_limit == u64::MAX * 95 panicked in the old
        // `monthly_limit * 95` code. u128 path must not overflow.
        let node = dummy_node(u64::MAX, 95);
        assert!(node.spillover_threshold < u64::MAX);
    }

    /// A node with every limit given, for the tests that exercise the limits
    /// rather than the quota arithmetic.
    fn node_with(rps_limit: u32, max_concurrent: usize, url: &str) -> Result<RpcNode> {
        RpcNode::new(NewNode {
            name: "test".into(),
            url: url.into(),
            rps_limit,
            max_concurrent,
            tier: 0,
            method_costs: ProviderCostTable::default(),
            monthly_limit: 1000,
            spillover_percent: 95,
            reset_day: 1,
        })
    }

    #[test]
    fn zero_rps_is_rejected() {
        // `Quota::per_second` takes a `NonZeroU32`; without the guard this is a
        // panic at startup instead of a config error.
        assert!(node_with(0, 1, "http://localhost:9").is_err());
    }

    #[test]
    fn an_invalid_url_is_rejected() {
        assert!(node_with(1, 1, "not a url").is_err());
    }

    #[test]
    fn a_url_without_a_scheme_is_rejected() {
        // The most likely typo in a config file, and one that would otherwise
        // only surface as a failed request.
        assert!(node_with(1, 1, "127.0.0.1:8899").is_err());
    }

    #[test]
    fn a_new_node_carries_no_measurement_and_no_penalty() {
        let node = node_with(1, 1, "http://localhost:9").unwrap();

        // Nothing has measured it, so `Topology::rank` sorts it last and the
        // request path offers it a request only once the measured nodes have
        // turned one down.
        assert_eq!(node.latency.ema_us.load(Ordering::Relaxed), UNMEASURED);
        assert!(!node.is_penalized(0));
        assert_eq!(node.id, 0, "the real slot is handed out by assign_ids");
    }

    #[test]
    fn the_first_answer_sets_the_average_rather_than_averaging_the_sentinel() {
        // Folding `UNMEASURED` into the average would leave the node reading as
        // multi-second for its first several answers, i.e. dead last in the
        // ranking long after it started answering in microseconds.
        let node = node_with(1, 1, "http://localhost:9").unwrap();

        node.observe_answer(Instant::now());

        let first = node.latency.ema_us.load(Ordering::Relaxed);
        assert!(first < 1_000_000, "first sample was averaged in: {first}");
    }

    #[test]
    fn the_average_converges_on_a_steady_latency() {
        let node = node_with(1, 1, "http://localhost:9").unwrap();
        node.latency.ema_us.store(80_000, Ordering::Relaxed);

        // A stream of near-instant answers has to pull the average down; with
        // `alpha = 1/8` thirty samples is well past the time constant.
        for _ in 0..30 {
            node.observe_answer(Instant::now());
        }

        let ema = node.latency.ema_us.load(Ordering::Relaxed);
        assert!(ema < 8_000, "average did not converge downwards: {ema}");
    }

    #[test]
    fn a_penalty_expires_on_its_own() {
        let node = node_with(1, 1, "http://localhost:9").unwrap();
        let at = Instant::now();
        let now_s = seconds_since_start(at);

        node.penalize(at, 0);

        assert!(node.is_penalized(now_s), "penalty did not take effect");
        assert!(
            !node.is_penalized(now_s + PENALTY_SECS),
            "penalty outlived its deadline"
        );
    }

    #[test]
    fn retry_after_extends_a_penalty_but_cannot_cut_it_short() {
        let node = node_with(1, 1, "http://localhost:9").unwrap();
        let at = Instant::now();
        let now_s = seconds_since_start(at);

        node.penalize(at, PENALTY_SECS * 4);
        assert!(node.is_penalized(now_s + PENALTY_SECS * 3), "upstream asked for longer and was ignored");

        // The other direction: a provider asking for one second must not buy
        // itself a shorter cooldown than the balancer would have applied.
        node.penalty.until_s.store(0, Ordering::Relaxed);
        node.penalize(at, 1);
        assert!(node.is_penalized(now_s + PENALTY_SECS - 1));
    }

    #[test]
    fn an_answer_clears_a_standing_penalty() {
        // Reached when every node is penalized at once: the router fails open,
        // offers the request anyway, and the node answers. Leaving the penalty
        // standing would keep the whole balancer failing open until it expired.
        let node = node_with(1, 1, "http://localhost:9").unwrap();
        let at = Instant::now();

        node.penalize(at, 0);
        node.observe_answer(at);

        assert!(!node.is_penalized(seconds_since_start(at)));
    }

    #[tokio::test]
    async fn try_admit_reports_the_wait_when_the_bucket_is_empty() {
        let node = node_with(1, 10, "http://localhost:9").unwrap();

        let _first = node.try_admit().expect("first call has a token");

        let Err(Unavailable::RateLimited(wait)) = node.try_admit() else {
            panic!("one token per second, so the second call is limited");
        };

        // The router sleeps on this value, so a zero would spin.
        assert!(
            wait > Duration::ZERO,
            "wait time must be positive: {wait:?}"
        );
        assert!(
            wait <= Duration::from_secs(1),
            "wait longer than the quota window: {wait:?}"
        );
    }

    #[tokio::test]
    async fn a_permit_is_released_when_the_attempt_is_dropped() {
        let node = node_with(100, 1, "http://localhost:9").unwrap();

        let permit = node.try_admit().expect("first attempt admitted");
        assert_eq!(node.limits.concurrency.available_permits(), 0);

        drop(permit);

        // Without this the node would be permanently at capacity after one
        // request.
        assert_eq!(node.limits.concurrency.available_permits(), 1);
    }

    #[test]
    fn the_hot_cells_sit_on_cache_lines_of_their_own() {
        // Three different traffic patterns, three lines. `limits` is written by
        // every admission, `latency` by every answer, `penalty` is read by every
        // request and written almost never. Sharing a line, the answers would
        // invalidate the penalty on every core that is only reading it.
        // Dropping the attribute, or putting a field back behind an `Arc`,
        // breaks this silently.
        let node = node_with(1, 1, "http://localhost:9").unwrap();

        let cells = [
            ("limits", std::ptr::from_ref(&node.limits).addr()),
            ("latency", std::ptr::from_ref(&node.latency).addr()),
            ("penalty", std::ptr::from_ref(&node.penalty).addr()),
        ];

        for (name, address) in cells {
            assert_eq!(address % 64, 0, "{name} is not on a line boundary");
        }

        for (i, (left, right)) in cells.iter().zip(&cells[1..]).enumerate() {
            assert!(
                left.1.abs_diff(right.1) >= 64,
                "{} and {} share a cache line (pair {i})",
                left.0,
                right.0
            );
        }
    }

    #[tokio::test]
    async fn concurrency_is_capped_at_max_concurrent() {
        let node = node_with(100, 2, "http://localhost:9").unwrap();

        let _a = node.try_admit().unwrap();
        let _b = node.try_admit().unwrap();

        assert!(
            matches!(node.try_admit(), Err(Unavailable::Saturated)),
            "a third attempt must be turned away, not queued"
        );
        assert_eq!(node.limits.concurrency.available_permits(), 0);
    }

    /// A node with no permits left must not spend a rate-limit token being
    /// turned away — it would drain its own bucket while too busy to use it.
    #[tokio::test]
    async fn a_saturated_node_keeps_its_rate_limit_tokens() {
        let node = node_with(2, 1, "http://localhost:9").unwrap();

        let held = node.try_admit().expect("first attempt admitted");

        assert!(matches!(node.try_admit(), Err(Unavailable::Saturated)));

        drop(held);

        drop(
            node.try_admit()
                .expect("the second token is still there once a permit frees up"),
        );
    }
}
