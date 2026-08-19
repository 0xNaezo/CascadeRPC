use bytes::Bytes;
use memchr::memmem;
use metrics::{Unit, counter, gauge, histogram};
use reqwest::{Response, StatusCode};
use std::time::Duration;
use tokio::sync::SemaphorePermit;
use tokio::time::{Instant, timeout};
use tracing::{debug, trace};

use crate::core::node::RpcNode;
use crate::core::rpc::{GaugeGuard, RpcClient};
use crate::protocol::registry::CUSTOM_METHODS;
use crate::protocol::rpc_payload::{MethodExtractor, RpcErrorOnly};

const GLOBAL_TIMEOUT: Duration = Duration::from_secs(1);

/// Upstream bodies up to this size are always parsed, larger ones only when
/// they contain an `"error"` key. Any plausible non-JSON upstream answer (a
/// gateway error page, a rate-limit notice) fits well below the limit, while
/// the payloads worth not parsing start an order of magnitude above it.
const VALIDATE_BELOW: usize = 64 * 1024;

/// Failure produced by the balancer itself, as opposed to a response forwarded
/// from an upstream node.
#[derive(Debug, Clone, Copy)]
enum RouteError {
    /// Client sent a body that is not a JSON-RPC request.
    BadRequest,
    /// Every node failed with a server/network error and none is merely
    /// rate-limited, so there is nothing to wait for.
    AllNodesFailed,
    /// The global budget expired while retrying nodes.
    Timeout,
}

impl RouteError {
    const fn status(self) -> StatusCode {
        match self {
            Self::BadRequest => StatusCode::BAD_REQUEST,
            Self::AllNodesFailed => StatusCode::BAD_GATEWAY,
            Self::Timeout => StatusCode::GATEWAY_TIMEOUT,
        }
    }

    const fn body(self) -> &'static [u8] {
        match self {
            Self::BadRequest => {
                br#"{"jsonrpc":"2.0","error":{"code":-32700,"message":"Parse error"},"id":null}"#
            }
            Self::AllNodesFailed => {
                b"All nodes failed with server/network errors (no rate limits to wait for)"
            }
            Self::Timeout => b"Global timeout (1s) exceeded while retrying nodes",
        }
    }

    /// Value of the `outcome` label in `rpc_requests` / `rpc_request_duration`.
    const fn outcome(self) -> &'static str {
        match self {
            Self::BadRequest => "bad_request",
            Self::AllNodesFailed => "bad_gateway",
            Self::Timeout => "timeout",
        }
    }

    const fn response(self) -> (StatusCode, Bytes) {
        (self.status(), Bytes::from_static(self.body()))
    }
}

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
    /// Both halves of the result carry the status to answer the client with:
    /// on success it is the upstream's own status, on failure the one the
    /// balancer picked for the reason it failed.
    ///
    /// # Errors
    ///
    /// Returns `400` with a JSON-RPC parse error if the body is not a JSON-RPC
    /// request, `502` if all RPC nodes fail, and `504` if the global timeout
    /// expires while retrying nodes.
    pub async fn send(
        &self,
        body_bytes: Bytes,
    ) -> Result<(StatusCode, Bytes), (StatusCode, Bytes)> {
        let request_started = Instant::now();

        let result = self.dispatch(&body_bytes).await;

        let outcome = match result {
            Ok(_) => "forwarded",
            Err(error) => error.outcome(),
        };
        Self::record_request(outcome, request_started);

        result.map_err(RouteError::response)
    }

    /// Resolves the method, then walks the nodes under the global time budget.
    ///
    /// The id is resolved once, here, and stays valid for every retry round
    /// below: custom-method ids are append-only, so a SIGHUP landing mid-request
    /// can add names but never renumber the one already in hand.
    async fn dispatch(&self, body_bytes: &Bytes) -> Result<(StatusCode, Bytes), RouteError> {
        let method = Self::extract_method(body_bytes)?;
        let method_id = CUSTOM_METHODS.resolve(method);

        // `?` peels off the timeout layer; what is left is the routing outcome.
        timeout(GLOBAL_TIMEOUT, self.route(body_bytes, method_id))
            .await
            .map_err(|_| RouteError::Timeout)?
    }

    /// Extracts the JSON-RPC method name from the raw request body.
    fn extract_method(body_bytes: &[u8]) -> Result<&str, RouteError> {
        serde_json::from_slice::<MethodExtractor<'_>>(body_bytes)
            .map(|extractor| extractor.method)
            .map_err(|_| RouteError::BadRequest)
    }

    /// Walks the routing table, retrying across nodes until one answers or all
    /// of them are exhausted. Sleeps and retries while some node is only
    /// rate-limited.
    ///
    /// A node serves the request at most once, however many rounds the sleeping
    /// takes: one attempt, one quota booking. Only a rate-limited node comes
    /// back around, which is what the sleep is for.
    async fn route(
        &self,
        body_bytes: &Bytes,
        method_id: usize,
    ) -> Result<(StatusCode, Bytes), RouteError> {
        // One bit per quota slot: `node.id` is below `MAX_NODES` == 64 by
        // construction, `assign_ids` rejects larger configs. Without it every
        // round would book the cost of a steadily failing node all over again.
        let mut tried: u64 = 0;

        loop {
            // Reloaded once per attempt round, so a config swap reaches the
            // retry loop without disturbing the round already in flight.
            let topology = self.topology.load();

            let mut best_time: Option<Duration> = None;

            for node in &topology.active {
                let bit = 1u64 << node.id;

                if tried & bit != 0 {
                    continue;
                }

                let _permit = match self.admit(node, method_id).await {
                    Admission::Ready(permit) => {
                        tried |= bit;

                        permit
                    }
                    Admission::Skip => {
                        tried |= bit;

                        continue;
                    }
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
                return Err(RouteError::AllNodesFailed);
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

        let used = self.nodes_usage.usage(node.id);

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

        if body.len() <= VALIDATE_BELOW || memmem::find(body.as_ref(), br#""error""#).is_some() {
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
                    Self::record_upstream(
                        node,
                        "forwarded_rpc_error",
                        started.elapsed().as_secs_f64(),
                    );

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
