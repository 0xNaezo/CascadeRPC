use anyhow::Result;
use arc_swap::ArcSwap;
use bytes::Bytes;
use metrics::{Unit, counter, histogram};
use reqwest::{Client, Response, StatusCode};
use std::sync::Arc;
use std::time::Duration;
use url::Url;

use crate::{
    core::node::{RoutingTable, RpcNode},
    quotas::GlobalQuotaState,
};

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
    pub nodes_usage: Arc<GlobalQuotaState>,
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

        let nodes_name: Vec<String> = nodes.iter().map(|node| node.name.clone()).collect();
        let nodes_usage = Arc::new(GlobalQuotaState::new(nodes_name));

        let all_nodes: Vec<Arc<RpcNode>> = nodes.into_iter().map(Arc::new).collect();

        Ok(Self {
            client,
            routing_table: Arc::new(ArcSwap::from_pointee(RoutingTable {
                active_nodes: all_nodes.clone(),
            })),
            nodes_usage,
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

    #[must_use]
    pub fn is_retryable_json_rpc_error(error_code: i32) -> bool {
        let not_retryable = [-32700, -32601, -32602, -32600];

        if not_retryable.contains(&error_code) {
            return false;
        }

        true
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn retryable_error_true_for_5xx_and_429() {
        assert!(RpcClient::is_retryable_error(StatusCode::TOO_MANY_REQUESTS));
        assert!(RpcClient::is_retryable_error(
            StatusCode::INTERNAL_SERVER_ERROR
        ));
        assert!(RpcClient::is_retryable_error(StatusCode::BAD_GATEWAY));
        assert!(RpcClient::is_retryable_error(
            StatusCode::SERVICE_UNAVAILABLE
        ));
        assert!(RpcClient::is_retryable_error(StatusCode::GATEWAY_TIMEOUT));
    }

    #[test]
    fn retryable_error_true_for_success() {
        assert!(RpcClient::is_retryable_error(StatusCode::OK));
    }

    #[test]
    fn retryable_error_false_for_not_retryable_list() {
        let not_retryable = [
            400, 402, 404, 405, 406, 407, 408, 409, 410, 411, 412, 413, 414, 415, 416, 417, 418,
            421, 422, 423, 424, 425, 426, 428, 431, 451,
        ];
        for code in not_retryable {
            assert!(
                !RpcClient::is_retryable_error(StatusCode::from_u16(code).unwrap()),
                "expected {code} to be not-retryable"
            );
        }
    }

    #[test]
    fn retryable_jsonrpc_false_for_not_retryable_codes() {
        for code in [-32700, -32601, -32602, -32600] {
            assert!(
                !RpcClient::is_retryable_json_rpc_error(code),
                "expected {code} to be not-retryable"
            );
        }
    }

    #[test]
    fn retryable_jsonrpc_true_for_server_internal_and_zero() {
        assert!(RpcClient::is_retryable_json_rpc_error(-32000)); // server error
        assert!(RpcClient::is_retryable_json_rpc_error(-32603)); // internal error
        assert!(RpcClient::is_retryable_json_rpc_error(0)); // no error
    }
}
