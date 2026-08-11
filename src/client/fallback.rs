use anyhow::Result;
use bytes::Bytes;
use metrics::{Unit, counter, gauge, histogram};
use reqwest::StatusCode;
use std::time::Duration;
use tokio::time::{Instant, timeout};
use tracing::{debug, trace};

use crate::client::rpc::{GaugeGuard, RpcClient};
use crate::structs::balancer::RpcErrorOnly;

impl RpcClient {
    /// Sends a JSON-RPC request with fallback across nodes.
    ///
    /// # Errors
    ///
    /// Returns an error if all RPC nodes fail or exhaust their rate limits.
    pub async fn send_with_fallback(
        &self,
        body_bytes: Bytes,
    ) -> Result<(StatusCode, Bytes), Bytes> {
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

                    trace!(node = %node.name, "sending request");
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
                            debug!(node = %node.name, error = %e, "upstream HTTP request failed");
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
                                debug!(
                                    node = %node.name,
                                    error = %e,
                                    "failed to read upstream response body"
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
                            debug!(
                                node = %node.name,
                                error = %e,
                                "failed to read upstream response body"
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
                                debug!(
                                    node = %node.name,
                                    error = %e,
                                    "upstream returned invalid JSON"
                                );
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
                        debug!(
                            node = %node.name,
                            code = err.code,
                            "upstream returned retryable RPC error"
                        );
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

                let _guard = GaugeGuard::new(gauge!(
                    description: "Number of requests currently sleeping while all RPC nodes are rate-limited",
                    "rpc_sleep_queue_size"
                ));
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
}
