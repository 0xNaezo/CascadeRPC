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

use crate::provider::cost_table::ProviderCostTable;

pub struct RoutingTable {
    pub active_nodes: Vec<Arc<RpcNode>>,
}

pub type DefaultDirectRateLimiter<MW = NoOpMiddleware<<DefaultClock as Clock>::Instant>> =
    RateLimiter<NotKeyed, InMemoryState, DefaultClock, MW>;

pub struct NewNode {
    pub name: String,
    pub url: String,
    pub rps_limit: u32,
    pub max_concurrent: usize,
    pub tier: u8,
    pub method_costs: ProviderCostTable,
    pub monthly_limit: u64,
    pub billing_type: String,
    pub spillover_percent: u8,
}

#[derive(Clone)]
pub struct RpcNode {
    pub name: String,
    pub url: Url,
    pub rate_limiting: Arc<DefaultDirectRateLimiter>,
    pub concurrency_limiting: Arc<Semaphore>,
    pub tier: u8,
    pub latency: Arc<AtomicU32>,
    pub healthy: Arc<AtomicBool>,
    pub method_costs: Arc<ProviderCostTable>,
    pub monthly_limit: u64,
    pub billing_type: String,
    pub spillover_threshold: u64,
}

impl RpcNode {
    /// Creates a new `RpcNode`.
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
            name: config.name,
            url,
            rate_limiting: Arc::new(RateLimiter::direct(quota)),
            concurrency_limiting: Arc::new(Semaphore::new(config.max_concurrent)),
            tier: config.tier,
            latency: Arc::new(AtomicU32::new(0)),
            healthy: Arc::new(AtomicBool::new(true)),
            method_costs: Arc::new(config.method_costs),
            monthly_limit: config.monthly_limit,
            billing_type: config.billing_type,
            spillover_threshold,
        })
    }

    /// Checks rate limit, then acquires concurrency permit.
    ///
    /// # Errors
    ///
    /// Returns the minimum wait time if the rate limit is exceeded.
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
            billing_type: "credits".into(),
            spillover_percent,
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
