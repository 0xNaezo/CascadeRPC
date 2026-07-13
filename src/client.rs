use anyhow::{Context, Result};
use bytes::Bytes;
use governor::{
    Quota, RateLimiter,
    clock::{Clock, DefaultClock},
    middleware::NoOpMiddleware,
    state::{InMemoryState, NotKeyed},
};
use reqwest::{Client, Response};
use serde_json::json;
use std::{
    num::NonZeroU32,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32},
    },
};
use tokio::sync::{Semaphore, SemaphorePermit};
use tracing::info;
use url::Url;

use crate::structs::RpcBalanceResponse;

#[derive(Clone)]
pub struct RpcClient {
    pub client: Client,
    pub node_configs: NodeConfigs,
    pub request_counter: Arc<AtomicU32>,
}

#[derive(Clone)]
pub struct NodeConfigs {
    pub nodes: Vec<RpcNode>,
}

pub type DefaultDirectRateLimiter<MW = NoOpMiddleware<<DefaultClock as Clock>::Instant>> =
    RateLimiter<NotKeyed, InMemoryState, DefaultClock, MW>;

#[derive(Clone)]
pub struct RpcNode {
    pub name: String,
    pub url: Url,
    pub is_live: Arc<AtomicBool>,
    pub rate_limiting: Arc<DefaultDirectRateLimiter>,
    pub concurrency_limiting: Arc<Semaphore>,
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
    ) -> Result<Self> {
        let non_zero_rate_limiting = NonZeroU32::new(rate_limiting)
            .ok_or_else(|| anyhow::anyhow!("Fatal error: RPS for node '{name}' cannot be 0"))?;

        let quota = Quota::per_second(non_zero_rate_limiting);
        let url = Url::parse(url_str)
            .with_context(|| format!("Fatal error: invalid URL for node '{name}': {url_str}"))?;

        Ok(Self {
            name,
            url,
            is_live: Arc::new(AtomicBool::new(true)),
            rate_limiting: Arc::new(RateLimiter::direct(quota)),
            concurrency_limiting: Arc::new(Semaphore::new(concurrency_limiting)),
        })
    }

    /// Acquires concurrency permit and checks rate limit.
    ///
    /// # Errors
    ///
    /// Returns an error if the rate limit is exceeded for this node.
    pub async fn acquire_and_check(&self) -> Result<SemaphorePermit<'_>> {
        if self.rate_limiting.check().is_err() {
            return Err(anyhow::format_err!("Rate limit exceeded for {}", self.name));
        }

        let permit = self.concurrency_limiting.acquire().await?;

        Ok(permit)
    }
}

impl RpcClient {
    #[must_use]
    pub fn new(node_configs: NodeConfigs) -> Self {
        let client = Client::new();

        Self {
            client,
            node_configs,
            request_counter: Arc::new(AtomicU32::new(0)),
        }
    }

    /// Sends a balance request with fallback across nodes.
    ///
    /// # Errors
    ///
    /// Returns an error if all RPC nodes fail or exhaust their rate limits.
    pub async fn send_with_fallback(&self, address: String) -> Result<RpcBalanceResponse> {
        info!(address = %address, "fetching balance");

        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getBalance",
            "params": [
                address
            ]
        })
        .to_string();

        let body_bytes = Bytes::from(body);

        for node in &self.node_configs.nodes {
            let _permit = match node.acquire_and_check().await {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!("{}", e);
                    continue;
                }
            };

            tracing::info!("Sending request to {}", node.name);

            let response =
                match Self::send_request(self.client.clone(), body_bytes.clone(), node.url.clone())
                    .await
                {
                    Ok(res) => res,
                    Err(e) => {
                        tracing::error!("Node {} HTTP failed: {e}", node.name);
                        continue;
                    }
                };

            let parse_res: RpcBalanceResponse = match response.json().await {
                Ok(res) => res,
                Err(e) => {
                    tracing::error!("Node {} returned invalid JSON: {e}", node.name);
                    continue;
                }
            };

            if let Some(err) = parse_res.error {
                tracing::error!("Node {} returned error: {err:#?}", node.name);
                continue;
            }

            return Ok(parse_res);
        }

        Err(anyhow::format_err!(
            "All RPC nodes failed or exhausted rate limits"
        ))
    }

    /// Sends a JSON-RPC request to the given URL.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP request fails.
    pub async fn send_request(client: Client, body: Bytes, url: Url) -> Result<Response> {
        let result = client
            .post(url)
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await?;

        Ok(result)
    }
}
