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
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32},
    },
    time::Duration,
};
use tokio::sync::{Semaphore, SemaphorePermit};
use url::Url;

use crate::protocol::cost_table::ProviderCostTable;

/// A `governor` limiter with every knob at its default: one unkeyed bucket,
/// state in memory, no middleware. Named because the full path appears in
/// [`RpcNode`]'s type.
pub type DefaultDirectRateLimiter<MW = NoOpMiddleware<<DefaultClock as Clock>::Instant>> =
    RateLimiter<NotKeyed, InMemoryState, DefaultClock, MW>;

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

#[derive(Clone)]
pub struct RpcNode {
    /// Index of this node's counter in [`crate::quotas::state::GlobalQuotaState`],
    /// assigned by `assign_ids` and tied to [`Self::name`], not to the node's
    /// position in the config.
    pub id: usize,
    pub name: String,
    pub url: Url,
    pub rate_limiting: Arc<DefaultDirectRateLimiter>,
    pub concurrency_limiting: Arc<Semaphore>,
    pub tier: u8,
    pub latency: Arc<AtomicU32>,
    pub healthy: Arc<AtomicBool>,
    pub method_costs: Arc<ProviderCostTable>,
    /// Usage past which the router stops routing to this node: `monthly_limit`
    /// scaled by `spillover_percent`. Traffic spills to the next tier a little
    /// before the provider's quota is actually gone.
    pub spillover_threshold: u64,
    /// Day of the month this node's usage counter is zeroed on. See
    /// [`crate::quotas::period`].
    pub reset_day: u8,
}

impl RpcNode {
    /// Creates a node with no quota slot yet and no measured latency, assumed
    /// healthy until the first probe says otherwise.
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
            name: config.name,
            url,
            rate_limiting: Arc::new(RateLimiter::direct(quota)),
            concurrency_limiting: Arc::new(Semaphore::new(config.max_concurrent)),
            tier: config.tier,
            latency: Arc::new(AtomicU32::new(u32::MAX)),
            healthy: Arc::new(AtomicBool::new(true)),
            method_costs: Arc::new(config.method_costs),
            spillover_threshold,
            reset_day: config.reset_day,
        })
    }

    /// Takes a concurrency permit for one attempt, once the rate limiter has a
    /// token to spare. The permit is held for as long as the attempt runs.
    ///
    /// # Errors
    ///
    /// Returns how long until the rate limiter frees up, which the router
    /// sleeps on. A closed semaphore — which cannot happen while the node is
    /// alive, nothing ever closes it — reports `Duration::MAX` so that the
    /// router treats the node as the worst option rather than the best.
    pub async fn acquire_and_check(&self) -> Result<SemaphorePermit<'_>, Duration> {
        if let Err(err) = self.rate_limiting.check() {
            let clock = DefaultClock::default();

            let time = err.wait_time_from(clock.now());

            return Err(time);
        }

        let permit = self
            .concurrency_limiting
            .acquire()
            .await
            .map_err(|_| Duration::MAX)?;

        Ok(permit)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
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
}
