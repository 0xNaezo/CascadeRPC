use anyhow::Result;
use arc_swap::ArcSwap;
use bytes::Bytes;
use metrics::{Unit, counter, histogram};
use reqwest::{Client, Response, StatusCode};
use serde_json::json;
use std::sync::{Arc, atomic::AtomicU32};
use std::time::Duration;
use tokio::time::{Instant, timeout};
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
    pub async fn send_with_fallback(
        &self,
        body_bytes: Bytes,
    ) -> Result<(StatusCode, Bytes), Bytes> {
        info!("Get request");

        let request_started = Instant::now();

        let result = timeout(Duration::from_secs(1), async {
            loop {
                let active_nodes = self.routing_table.load();

                let mut best_time: Option<Duration> = None;

                for node in &active_nodes.active_nodes {
                    let _permit = match node.acquire_and_check().await {
                        Ok(permit) => permit,
                        Err(time) => {
                            Self::record_upstream(node, "rate_limit", 0.0);
                            best_time =
                                Some(best_time.map_or(time, |current_best| current_best.min(time)));

                            continue;
                        }
                    };

                    tracing::info!("Sending request to {}", node.name);
                    let started = Instant::now();

                    let response = match Self::send_request(
                        self.client.clone(),
                        body_bytes.clone(),
                        node.url.clone(),
                    )
                    .await
                    {
                        Ok(res) => res,
                        Err(e) => {
                            Self::record_upstream(
                                node,
                                "transport_error",
                                started.elapsed().as_secs_f64(),
                            );
                            tracing::error!("Node {} HTTP failed: {e}", node.name);
                            continue;
                        }
                    };

                    let status_code = response.status();
                    let is_retryable_error = Self::is_retryable_error(status_code);

                    if !is_retryable_error {
                        match response.bytes().await {
                            Ok(body) => {
                                Self::record_upstream(
                                    node,
                                    "forwarded_http_error",
                                    started.elapsed().as_secs_f64(),
                                );
                                return Ok((status_code, body));
                            }
                            Err(e) => {
                                Self::record_upstream(
                                    node,
                                    "body_error",
                                    started.elapsed().as_secs_f64(),
                                );
                                tracing::error!(
                                    "Failed to read response body from node {}: {e}",
                                    node.name
                                );
                                continue;
                            }
                        }
                    }

                    let parse_byte = match response.bytes().await {
                        Ok(res) => res,
                        Err(e) => {
                            Self::record_upstream(
                                node,
                                "body_error",
                                started.elapsed().as_secs_f64(),
                            );
                            tracing::error!(
                                "Failed to read response body from node {}: {e}",
                                node.name
                            );
                            continue;
                        }
                    };

                    let parse_error: RpcErrorOnly =
                        match serde_json::from_slice(parse_byte.as_ref()) {
                            Ok(res) => res,
                            Err(e) => {
                                Self::record_upstream(
                                    node,
                                    "invalid_json",
                                    started.elapsed().as_secs_f64(),
                                );
                                tracing::error!("Node {} returned invalid JSON: {e}", node.name);
                                continue;
                            }
                        };

                    if let Some(err) = parse_error.error {
                        if !Self::is_retryable_json_rpc_error(err.code) {
                            Self::record_upstream(
                                node,
                                "forwarded_rpc_error",
                                started.elapsed().as_secs_f64(),
                            );
                            return Ok((status_code, parse_byte));
                        }

                        Self::record_upstream(
                            node,
                            "retryable_rpc_error",
                            started.elapsed().as_secs_f64(),
                        );
                        tracing::warn!("Retryable RPC error from {}: {}", node.name, err.code);
                        continue;
                    }

                    let outcome = if status_code.is_success() {
                        "success"
                    } else {
                        "forwarded_http_error"
                    };
                    Self::record_upstream(node, outcome, started.elapsed().as_secs_f64());
                    return Ok((status_code, parse_byte));
                }

                let Some(best_time) = best_time else {
                    return Err(Bytes::from(
                        "All nodes failed with server/network errors (no rate limits to wait for)",
                    ));
                };

                tokio::time::sleep(best_time).await;
            }
        })
        .await;

        let (result, outcome) = match result {
            Ok(Ok(response)) => (Ok(response), "forwarded"),
            Ok(Err(message)) => (Err(message), "bad_gateway"),
            Err(_) => (
                Err(Bytes::from(
                    "Global timeout (1s) exceeded while retrying nodes",
                )),
                "timeout",
            ),
        };

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
        .record(request_started.elapsed().as_secs_f64());

        result
    }

    fn record_upstream(node: &RpcNode, outcome: &'static str, duration_seconds: f64) {
        counter!(
            description: "Attempts sent to upstream RPC nodes",
            "rpc_upstream_attempts",
            "node" => node.name.clone(),
            "outcome" => outcome,
        )
        .increment(1);
        histogram!(
            description: "Upstream RPC attempt duration",
            unit: Unit::Seconds,
            "rpc_upstream_duration",
            "node" => node.name.clone(),
            "outcome" => outcome,
        )
        .record(duration_seconds);
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

    // ponytail: false = no retry, true = retry
    #[must_use]
    fn is_retryable_error(error_code: StatusCode) -> bool {
        let http_status = error_code.as_u16();
        let not_retryable = [
            400, 402, 404, 405, 406, 407, 408, 409, 410, 411, 412, 413, 414, 415, 416, 417, 418,
            421, 422, 423, 424, 425, 426, 428, 431, 451,
        ];

        if not_retryable.contains(&http_status) {
            return false;
        }

        true
    }

    // ponytail: false = no retry, true = retry
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
