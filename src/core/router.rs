//! Routing one client request: pick a node, bill it, send, and decide whether
//! the answer is final or the next node gets a turn.
//!
//! The order is not decided here — the health check loop publishes it and this
//! walks it front to back, taking the first node that will accept the request.
//! What this module owns is everything after that choice: the global time
//! budget, quota booking, and when to give up.
//!
//! What an individual answer *means* is [`crate::core::upstream`]'s job; this
//! only acts on the verdict.

use bytes::Bytes;
use reqwest::StatusCode;
use std::time::Duration;
use tokio::sync::SemaphorePermit;
use tokio::time::{Instant, timeout};
use tracing::trace;

use crate::core::node::RpcNode;
use crate::core::rpc::RpcClient;
use crate::core::upstream::{self, Attempt};
use crate::metrics;
use crate::protocol::registry::CUSTOM_METHODS;
use crate::protocol::rpc_payload::MethodExtractor;
use crate::quotas::state::MAX_NODES;

/// Wall-clock budget for one client request, retries and rate-limit sleeps
/// included.
const GLOBAL_TIMEOUT: Duration = Duration::from_secs(1);

// Shorter than the backstop on a single HTTP request, on purpose: one upstream
// may not spend the whole budget on its own. The other way round the backstop
// would fire first and this budget would never be the thing that bounds a
// request.
const _: () = assert!(GLOBAL_TIMEOUT.as_millis() <= upstream::HTTP_BACKSTOP.as_millis());

// `route` tracks the nodes a request has already tried in a `u64` bitmask, one
// bit per quota slot. The two numbers are set in different modules, so the
// agreement between them is checked here rather than trusted to a comment.
const _: () = assert!(MAX_NODES <= u64::BITS as usize);

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

    /// Always JSON-RPC, whatever the HTTP status: a client that speaks JSON-RPC
    /// to this endpoint gets a body it can parse on 502 and 504 too, not only on
    /// the 400 it shares with the upstreams.
    ///
    /// `-32000` is the first of the codes the spec reserves for
    /// implementation-defined server errors, which is what a balancer running
    /// out of nodes or out of time is.
    const fn body(self) -> &'static [u8] {
        match self {
            Self::BadRequest => {
                br#"{"jsonrpc":"2.0","error":{"code":-32700,"message":"Parse error"},"id":null}"#
            }
            Self::AllNodesFailed => {
                br#"{"jsonrpc":"2.0","error":{"code":-32000,"message":"All nodes failed with server/network errors (no rate limits to wait for)"},"id":null}"#
            }
            // No timeout value in the message: spelling out GLOBAL_TIMEOUT here
            // is something a `const fn` cannot do, and a hand-kept copy of it
            // goes stale the first time the budget is retuned.
            Self::Timeout => {
                br#"{"jsonrpc":"2.0","error":{"code":-32000,"message":"Global timeout exceeded while retrying nodes"},"id":null}"#
            }
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

/// Whether a node may take the request right now, and what to do with it if
/// not.
enum Admission<'a> {
    /// Node accepted the request; the permit is held for the attempt.
    Ready(SemaphorePermit<'a>),
    /// Node cannot serve this request at all (quota spent, method unsupported).
    Skip,
    /// Node is rate-limited; the duration is how long until it frees up.
    RateLimited(Duration),
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
        metrics::record_request(outcome, request_started.elapsed().as_secs_f64());

        result.map_err(RouteError::response)
    }

    /// Resolves the method, then walks the nodes under the global time budget.
    ///
    /// The id is resolved once, here, and stays valid for every retry round
    /// below: custom-method ids are append-only, so a SIGHUP landing mid-request
    /// can add names but never renumber the one already in hand.
    async fn dispatch(&self, body_bytes: &Bytes) -> Result<(StatusCode, Bytes), RouteError> {
        let method = extract_method(body_bytes)?;
        let method_id = CUSTOM_METHODS.resolve(method);

        // `?` peels off the timeout layer; what is left is the routing outcome.
        timeout(GLOBAL_TIMEOUT, self.route(body_bytes, method_id))
            .await
            .map_err(|_| RouteError::Timeout)?
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
        // One bit per quota slot: `node.id` is below `MAX_NODES` by
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
                    upstream::attempt(&self.client, node, body_bytes.clone()).await
                {
                    return Ok((status_code, body));
                }
            }

            let Some(best_time) = best_time else {
                return Err(RouteError::AllNodesFailed);
            };

            wait_rate_limited(best_time).await;
        }
    }

    /// Checks rate limit, remaining quota and method pricing for a node, and
    /// books the method cost against its usage once the node is accepted.
    async fn admit<'a>(&self, node: &'a RpcNode, method_id: usize) -> Admission<'a> {
        let permit = match node.acquire_and_check().await {
            Ok(permit) => permit,
            Err(time) => {
                metrics::record_upstream(&node.name, "rate_limit", 0.0);

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
}

/// Extracts the JSON-RPC method name from the raw request body.
fn extract_method(body_bytes: &[u8]) -> Result<&str, RouteError> {
    serde_json::from_slice::<MethodExtractor<'_>>(body_bytes)
        .map(|extractor| extractor.method)
        .map_err(|_| RouteError::BadRequest)
}

/// Waits for the first node to leave its rate limit, tracking how many
/// requests are parked in the meantime.
async fn wait_rate_limited(best_time: Duration) {
    let _guard = metrics::sleeping_on_rate_limit();

    tokio::time::sleep(best_time).await;
}
