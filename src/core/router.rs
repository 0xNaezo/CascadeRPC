use bytes::Bytes;
use metrics::{Unit, counter, gauge, histogram};
use reqwest::{Response, StatusCode};
use std::time::Duration;
use tokio::sync::SemaphorePermit;
use tokio::time::{Instant, timeout};
use tracing::{debug, trace};

use crate::core::node::RpcNode;
use crate::core::rpc::{GaugeGuard, RpcClient};
use crate::protocol::methods::get_standard_method_id;
use crate::protocol::rpc_payload::{MethodExtractor, RpcErrorOnly};

const GLOBAL_TIMEOUT: Duration = Duration::from_secs(1);
const PARSE_ERROR_BODY: &[u8] =
    br#"{"jsonrpc":"2.0","error":{"code":-32700,"message":"Parse error"},"id":null}"#;
const ALL_NODES_FAILED_BODY: &[u8] =
    b"All nodes failed with server/network errors (no rate limits to wait for)";
const GLOBAL_TIMEOUT_BODY: &[u8] = b"Global timeout (1s) exceeded while retrying nodes";

/// Whether a node may take the request right now.
enum Admission<'a> {
    /// Node accepted the request; the permit is held for the attempt.
    Ready(SemaphorePermit<'a>),
    /// Node cannot serve this request at all (quota spent, method unsupported).
    Skip,
    /// Node is rate-limited; the duration is how long until it frees up.
    RateLimited(Duration),
}

/// Outcome of a single attempt against one node.
enum Attempt {
    /// Response is final and goes back to the client.
    Done(StatusCode, Bytes),
    /// Attempt failed in a retryable way; move on to the next node.
    Retry,
}

impl RpcClient {
    /// Sends a JSON-RPC request with fallback across nodes.
    ///
    /// # Errors
    ///
    /// Returns an error if all RPC nodes fail or exhaust their rate limits.
    pub async fn send(&self, body_bytes: Bytes) -> Result<(StatusCode, Bytes), Bytes> {
        let request_started = Instant::now();

        let method = match Self::extract_method(&body_bytes) {
            Ok(method) => method,
            Err(response) => return Err(response),
        };
        let method_id = get_standard_method_id(method.as_bytes());

        let result = timeout(GLOBAL_TIMEOUT, self.route(&body_bytes, method_id)).await;

        let (result, outcome) = match result {
            Ok(Ok(response)) => (Ok(response), "forwarded"),
            Ok(Err(message)) => (Err(message), "bad_gateway"),
            Err(_) => (Err(Bytes::from_static(GLOBAL_TIMEOUT_BODY)), "timeout"),
        };

        Self::record_request(outcome, request_started);

        result
    }

    /// Extracts the JSON-RPC method name from the raw request body.
    fn extract_method(body_bytes: &[u8]) -> Result<&str, Bytes> {
        serde_json::from_slice::<MethodExtractor<'_>>(body_bytes)
            .map(|extractor| extractor.method)
            .map_err(|_| Bytes::from_static(PARSE_ERROR_BODY))
    }

    /// Walks the routing table, retrying across nodes until one answers or all
    /// of them are exhausted. Sleeps and retries while some node is only
    /// rate-limited.
    async fn route(
        &self,
        body_bytes: &Bytes,
        method_id: usize,
    ) -> Result<(StatusCode, Bytes), Bytes> {
        loop {
            let active_nodes = self.routing_table.load();

            let mut best_time: Option<Duration> = None;

            for node in &active_nodes.active_nodes {
                let _permit = match self.admit(node, method_id).await {
                    Admission::Ready(permit) => permit,
                    Admission::Skip => continue,
                    Admission::RateLimited(time) => {
                        best_time =
                            Some(best_time.map_or(time, |current_best| current_best.min(time)));

                        continue;
                    }
                };

                trace!(node = %node.name, "sending request");

                if let Attempt::Done(status_code, body) =
                    self.attempt_node(node, body_bytes.clone()).await
                {
                    return Ok((status_code, body));
                }
            }

            let Some(best_time) = best_time else {
                return Err(Bytes::from_static(ALL_NODES_FAILED_BODY));
            };

            Self::wait_rate_limited(best_time).await;
        }
    }

    /// Checks rate limit, remaining quota and method pricing for a node, and
    /// books the method cost against its usage once the node is accepted.
    async fn admit<'a>(&self, node: &'a RpcNode, method_id: usize) -> Admission<'a> {
        let permit = match node.acquire_and_check().await {
            Ok(permit) => permit,
            Err(time) => {
                Self::record_upstream(node, "rate_limit", 0.0);

                return Admission::RateLimited(time);
            }
        };

        let used = self.nodes_usage.get_node_usage(&node.name);

        if used.get() > node.spillover_threshold {
            return Admission::Skip;
        }

        let method_cost = node.method_costs.cost(method_id);

        if method_cost == u32::MAX {
            return Admission::Skip;
        }

        used.add(u64::from(method_cost));

        Admission::Ready(permit)
    }

    /// Performs one HTTP attempt against a node and classifies the result.
    async fn attempt_node(&self, node: &RpcNode, body_bytes: Bytes) -> Attempt {
        let started = Instant::now();

        match Self::send_request(self.client.clone(), body_bytes, node.url.clone()).await {
            Ok(response) => Self::classify_response(node, response, started).await,
            Err(e) => {
                Self::record_upstream(node, "transport_error", started.elapsed().as_secs_f64());
                debug!(node = %node.name, error = %e, "upstream HTTP request failed");

                Attempt::Retry
            }
        }
    }

    /// Decides whether an upstream response is final or the next node should be
    /// tried, recording the per-attempt metrics for either case.
    async fn classify_response(node: &RpcNode, response: Response, started: Instant) -> Attempt {
        let status_code = response.status();

        let body = match response.bytes().await {
            Ok(body) => body,
            Err(e) => {
                Self::record_upstream(node, "body_error", started.elapsed().as_secs_f64());
                debug!(
                    node = %node.name,
                    error = %e,
                    "failed to read upstream response body"
                );

                return Attempt::Retry;
            }
        };

        if !Self::is_retryable_error(status_code) {
            Self::record_upstream(
                node,
                "forwarded_http_error",
                started.elapsed().as_secs_f64(),
            );

            return Attempt::Done(status_code, body);
        }

        let parse_error: RpcErrorOnly = match serde_json::from_slice(body.as_ref()) {
            Ok(res) => res,
            Err(e) => {
                Self::record_upstream(node, "invalid_json", started.elapsed().as_secs_f64());
                debug!(
                    node = %node.name,
                    error = %e,
                    "upstream returned invalid JSON"
                );

                return Attempt::Retry;
            }
        };

        if let Some(err) = parse_error.error {
            if !Self::is_retryable_json_rpc_error(err.code) {
                Self::record_upstream(node, "forwarded_rpc_error", started.elapsed().as_secs_f64());

                return Attempt::Done(status_code, body);
            }

            Self::record_upstream(node, "retryable_rpc_error", started.elapsed().as_secs_f64());
            debug!(
                node = %node.name,
                code = err.code,
                "upstream returned retryable RPC error"
            );

            return Attempt::Retry;
        }

        let outcome = if status_code.is_success() {
            "success"
        } else {
            "forwarded_http_error"
        };
        Self::record_upstream(node, outcome, started.elapsed().as_secs_f64());

        Attempt::Done(status_code, body)
    }

    /// Waits for the first node to leave its rate limit, tracking how many
    /// requests are parked in the meantime.
    async fn wait_rate_limited(best_time: Duration) {
        let _guard = GaugeGuard::new(gauge!(
            description: "Number of requests currently sleeping while all RPC nodes are rate-limited",
            "rpc_sleep_queue_size"
        ));
        tokio::time::sleep(best_time).await;
    }

    /// Records the end-to-end metrics for one client request.
    fn record_request(outcome: &'static str, request_started: Instant) {
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
    }
}
