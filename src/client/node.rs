use anyhow::{Context, Result};
use governor::{
    NotUntil, Quota, RateLimiter,
    clock::{Clock, DefaultClock, QuantaInstant},
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

use crate::config::ConfigNode;

pub struct RoutingTable {
    pub active_nodes: Vec<Arc<RpcNode>>,
}



pub type DefaultDirectRateLimiter<MW = NoOpMiddleware<<DefaultClock as Clock>::Instant>> =
    RateLimiter<NotKeyed, InMemoryState, DefaultClock, MW>;

#[derive(Clone)]
pub struct RpcNode {
    pub name: String,
    pub url: Url,
    pub rate_limiting: Arc<DefaultDirectRateLimiter>,
    pub concurrency_limiting: Arc<Semaphore>,
    pub tier: u8,
    pub latency: Arc<AtomicU32>,
    pub healthy: Arc<AtomicBool>,
}

impl RpcNode {
    /// Creates a new `RpcNode`.
    ///
    /// # Errors
    ///
    /// Returns an error if `rate_limiting` is 0 or `url_str` is not a valid URL.
    pub fn new(
        name: String,
        url_str: &str,
        rate_limiting: u32,
        concurrency_limiting: usize,
        tier: u8,
    ) -> Result<Self> {
        let non_zero_rate_limiting = NonZeroU32::new(rate_limiting)
            .ok_or_else(|| anyhow::anyhow!("Fatal error: RPS for node '{name}' cannot be 0"))?;

        let quota = Quota::per_second(non_zero_rate_limiting);
        let url = Url::parse(url_str)
            .with_context(|| format!("Fatal error: invalid URL for node '{name}': {url_str}"))?;

        Ok(Self {
            name,
            url,
            rate_limiting: Arc::new(RateLimiter::direct(quota)),
            concurrency_limiting: Arc::new(Semaphore::new(concurrency_limiting)),
            tier,
            latency: Arc::new(AtomicU32::new(0)),
            healthy: Arc::new(AtomicBool::new(true)),
        })
    }

    /// Acquires concurrency permit and checks rate limit.
    ///
    /// # Errors
    ///
    /// Returns an error if the rate limit is exceeded for this node.
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

impl TryFrom<ConfigNode> for RpcNode {
    type Error = anyhow::Error;

    fn try_from(n: ConfigNode) -> Result<Self> {
        Self::new(n.name, &n.url, n.rps_limit, n.max_concurrent, n.tier)
    }
}
