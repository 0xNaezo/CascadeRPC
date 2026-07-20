use anyhow::Result;
use arc_swap::ArcSwap;
use bytes::Bytes;
use reqwest::{Client, Response};
use serde_json::json;
use std::sync::{Arc, atomic::AtomicU32};
use std::time::Duration;
use tracing::info;
use url::Url;

use crate::client::node::{RoutingTable, RpcNode};

use crate::structs::{RpcErrorOnly, RpcHealthResponse};

#[derive(Clone)]
pub struct RpcClient {
    pub client: Client,
    pub all_nodes: Vec<Arc<RpcNode>>,
    pub routing_table: Arc<ArcSwap<RoutingTable>>,
    pub request_counter: Arc<AtomicU32>,
}

impl RpcClient {
    /// Builds a new [`RpcClient`] with a 2-second HTTP timeout.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying HTTP client cannot be initialized
    /// (e.g. TLS backend failure).
    pub fn new(nodes: Vec<RpcNode>) -> Result<Self> {
        let client = Client::builder().timeout(Duration::new(2, 0)).build()?;
        let all_nodes: Vec<Arc<RpcNode>> = nodes.into_iter().map(Arc::new).collect();

        Ok(Self {
            client,
            routing_table: Arc::new(ArcSwap::from_pointee(RoutingTable {
                active_nodes: all_nodes.clone(),
            })),
            all_nodes,
            request_counter: Arc::new(AtomicU32::new(0)),
        })
    }

    /// Sends a balance request with fallback across nodes.
    ///
    /// # Errors
    ///
    /// Returns an error if all RPC nodes fail or exhaust their rate limits.
    pub async fn send_with_fallback(&self, body_bytes: Bytes) -> Result<Bytes> {
        info!("Get request");

        let active_nodes = self.routing_table.load();

        for node in &active_nodes.active_nodes {
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

            let is_retryable_error = Self::is_retryable_error(&response);

            if !is_retryable_error {
                return Err(anyhow::format_err!(
                    "Node {} returned non-retryable HTTP {}",
                    node.name,
                    response.status().as_u16()
                ));
            }

            let parse_byte = match response.bytes().await {
                Ok(res) => res,
                Err(e) => {
                    tracing::error!("Failed to read response body from node {}: {e}", node.name);
                    continue;
                }
            };

            let parse_error: RpcErrorOnly = match serde_json::from_slice(parse_byte.as_ref()) {
                Ok(res) => res,
                Err(e) => {
                    tracing::error!("Node {} returned invalid JSON: {e}", node.name);
                    continue;
                }
            };

            if let Some(err) = parse_error.error
                && !Self::is_retryable_json_rpc_error(err.code)
            {
                return Err(anyhow::format_err!(
                    "Node {} returned non-retryable JSON-RPC error {}",
                    node.name,
                    err.code
                ));
            }

            return Ok(parse_byte);
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

    // false = не нужно повторять, true - нужно повторять
    #[must_use]
    fn is_retryable_error(response: &Response) -> bool {
        let http_status = response.status().as_u16();
        let not_retryable = [
            400, 402, 404, 405, 406, 407, 408, 409, 410, 411, 412, 413, 414, 415, 416, 417, 418,
            421, 422, 423, 424, 425, 426, 428, 431, 451,
        ];

        if not_retryable.contains(&http_status) {
            return false;
        }

        true
    }

    // false = не нужно повторять, true - нужно повторять
    #[must_use]
    fn is_retryable_json_rpc_error(error_code: i32) -> bool {
        let not_retryable = [-32700, -32601, -32602, -32600];

        if not_retryable.contains(&error_code) {
            return false;
        }

        true
    }

    pub async fn get_health(client: Client, node: &RpcNode) -> (bool, u32) {
        info!(node = %node.name, "checking node health");

        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getHealth"
        })
        .to_string();

        let body_bytes = Bytes::from(body);

        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        for _ in 0..3 {
            interval.tick().await;

            let start_time = tokio::time::Instant::now();

            let Ok(Ok(response)) = tokio::time::timeout(
                Duration::from_millis(500),
                Self::send_request(client.clone(), body_bytes.clone(), node.url.clone()),
            )
            .await
            else {
                continue;
            };
            
            let latency = start_time.elapsed().as_millis() as u32;

            let result: RpcHealthResponse = match response.json().await {
                Ok(result) => result,
                Err(_) => continue,
            };

            return (
                result.error.is_none() && result.result.as_deref() == Some("ok"),
                latency,
            );
        }

        (false, u32::MAX)
    }
}
