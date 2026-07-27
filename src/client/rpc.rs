use anyhow::Result;
use arc_swap::ArcSwap;
use bytes::Bytes;
use metrics::{Unit, counter, histogram};
use reqwest::{Client, Response, StatusCode};
use std::sync::Arc;
use std::time::Duration;
use url::Url;

use crate::client::node::{RoutingTable, RpcNode};

pub struct GaugeGuard(metrics::Gauge);

impl GaugeGuard {
    #[must_use]
    pub fn new(gauge: metrics::Gauge) -> Self {
        gauge.increment(1);
        Self(gauge)
    }
}

impl Drop for GaugeGuard {
    fn drop(&mut self) {
        self.0.decrement(1);
    }
}

#[derive(Clone)]
pub struct RpcClient {
    pub client: Client,
    pub all_nodes: Vec<Arc<RpcNode>>,
    pub routing_table: Arc<ArcSwap<RoutingTable>>,
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
        })
    }

    pub fn record_upstream(node: &RpcNode, outcome: &'static str, duration_seconds: f64) {
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
    pub fn is_retryable_error(error_code: StatusCode) -> bool {
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
    pub fn is_retryable_json_rpc_error(error_code: i32) -> bool {
        let not_retryable = [-32700, -32601, -32602, -32600];

        if not_retryable.contains(&error_code) {
            return false;
        }

        true
    }
}
