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

use crate::core::node::{RpcNode, Unavailable};
use crate::core::rpc::RpcClient;
use crate::core::upstream::{self, Attempt};
use crate::metrics::{self, RequestOutcome, SkipReason};
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
    const fn outcome(self) -> RequestOutcome {
        match self {
            Self::BadRequest => RequestOutcome::BadRequest,
            Self::AllNodesFailed => RequestOutcome::BadGateway,
            Self::Timeout => RequestOutcome::Timeout,
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
    /// Node is at its concurrency cap. Nothing is wrong with it and it has not
    /// served the request, so it is neither marked tried nor counted as failed.
    Saturated,
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
            Ok(_) => RequestOutcome::Forwarded,
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
            //
            // `load_full` and not `load`: the round below awaits a full upstream
            // round-trip, and an `arc_swap` guard held across that await burns
            // one of the eight per-thread debt slots for the whole request. With
            // hundreds of tasks per worker the slots stay empty, every `load`
            // falls back to the slow path anyway, and the writer in the health
            // check loop pays for it too. One honest `Arc` clone per round is
            // cheaper than a guard nobody can afford to hold.
            let topology = self.topology.load_full();

            let mut best_time: Option<Duration> = None;
            let mut saturated = None;

            for node in &topology.active {
                let bit = 1u64 << node.id;

                if tried & bit != 0 {
                    continue;
                }

                let _permit = match self.admit(node, method_id) {
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
                    Admission::Saturated => {
                        saturated.get_or_insert(node);

                        continue;
                    }
                };

                if let Some(response) = self.attempt_on(node, body_bytes).await {
                    return Ok(response);
                }
            }

            if let Some(best_time) = best_time {
                wait_rate_limited(best_time).await;

                continue;
            }

            // Nothing here failed — whatever is left is merely busy, and busy
            // is not a reason to answer 502. Parking is the old behaviour,
            // kept for the one case it was meant for: it happens only after
            // the request has been offered to every other node, and the global
            // budget still bounds it.
            let Some(node) = saturated else {
                return Err(RouteError::AllNodesFailed);
            };

            match self.admit_waiting(node, method_id).await {
                Admission::Ready(_permit) => {
                    tried |= 1u64 << node.id;

                    if let Some(response) = self.attempt_on(node, body_bytes).await {
                        return Ok(response);
                    }
                }
                // The wait cost this round its permit and the node turned out
                // to be unusable anyway; the next round has the others.
                Admission::Skip => tried |= 1u64 << node.id,
                Admission::RateLimited(time) => wait_rate_limited(time).await,
                // Only a closed semaphore reaches here, and nothing closes one.
                Admission::Saturated => return Err(RouteError::AllNodesFailed),
            }
        }
    }

    /// Sends one attempt to a node, and reports the response if it is the one
    /// the client gets.
    ///
    /// `None` means the attempt failed in a way another node may not — see
    /// [`upstream::attempt`] for which those are.
    async fn attempt_on(&self, node: &RpcNode, body_bytes: &Bytes) -> Option<(StatusCode, Bytes)> {
        trace!(node = %node.name, "sending request");

        match upstream::attempt(&self.client, node, body_bytes.clone()).await {
            Attempt::Done(status_code, body) => Some((status_code, body)),
            Attempt::Retry => None,
        }
    }

    /// Checks rate limit, remaining quota and method pricing for a node, and
    /// books the method cost against its usage once the node is accepted.
    fn admit<'a>(&self, node: &'a RpcNode, method_id: usize) -> Admission<'a> {
        let permit = match node.try_admit() {
            Ok(permit) => permit,
            Err(Unavailable::RateLimited(time)) => {
                node.metrics.record_skip(SkipReason::RateLimit);

                return Admission::RateLimited(time);
            }
            Err(Unavailable::Saturated) => {
                node.metrics.record_skip(SkipReason::Saturated);

                return Admission::Saturated;
            }
        };

        self.book(node, method_id, permit)
    }

    /// Waits for the node to have a permit, then admits the request on it.
    ///
    /// The permit is used rather than released and re-taken: the semaphore
    /// hands a freed permit to the first waiter, so a request that dropped one
    /// to go back around the loop would find the node busy again — every time,
    /// as long as anything else is queued.
    ///
    /// Reached only when every node is saturated at once, which is the one
    /// case where there is nothing better for a request to do than wait.
    async fn admit_waiting<'a>(&self, node: &'a RpcNode, method_id: usize) -> Admission<'a> {
        let permit = {
            let _guard = metrics::sleeping_on_rate_limit();

            match node.limits.concurrency.acquire().await {
                Ok(permit) => permit,
                Err(_) => return Admission::Saturated,
            }
        };

        if let Err(time) = node.check_rate() {
            node.metrics.record_skip(SkipReason::RateLimit);

            return Admission::RateLimited(time);
        }

        self.book(node, method_id, permit)
    }

    /// Books the method cost against a node's quota, with the permit for the
    /// attempt already in hand.
    fn book<'a>(
        &self,
        node: &RpcNode,
        method_id: usize,
        permit: SemaphorePermit<'a>,
    ) -> Admission<'a> {
        let used = self.nodes_usage.usage(node.id);

        if used.get() > node.spillover_threshold {
            node.metrics.record_skip(SkipReason::QuotaExhausted);

            return Admission::Skip;
        }

        let method_cost = node.method_costs.cost(method_id);

        if method_cost == u32::MAX {
            node.metrics.record_skip(SkipReason::MethodUnsupported);

            return Admission::Skip;
        }

        used.add(u64::from(method_cost));

        Admission::Ready(permit)
    }
}

/// Extracts the JSON-RPC method name from the raw request body.
///
/// Tries a scan first and falls back to a full parse, which keeps the fast path
/// free to give up on anything it is not sure about.
fn extract_method(body_bytes: &[u8]) -> Result<&str, RouteError> {
    if let Some(method) = scan_leading_method(body_bytes) {
        return Ok(method);
    }

    serde_json::from_slice::<MethodExtractor<'_>>(body_bytes)
        .map(|extractor| extractor.method)
        .map_err(|_| RouteError::BadRequest)
}

/// Reads the method name when `method` is the object's first key, which is
/// where every real client puts it.
///
/// Deliberately narrow: only the leading key is looked at, so a `"method"`
/// nested inside `params` can never be mistaken for the real one, and anything
/// unusual — a batch, an escape sequence in the name, a different first key —
/// returns `None` and pays for a full parse instead.
///
/// One deliberate difference from the parse it replaces: the bytes after the
/// name are not looked at, so a body with a well-formed leading `method` and a
/// broken tail is now routed instead of answered as a parse error. The upstream
/// rejects it either way, and validating the tail is the whole cost this
/// avoids.
fn scan_leading_method(body: &[u8]) -> Option<&str> {
    let rest = expect_byte(skip_ws(body), b'{')?;
    let rest = expect_byte(skip_ws(rest), b'"')?;
    let rest = rest.strip_prefix(b"method\"")?;
    let rest = expect_byte(skip_ws(rest), b':')?;
    let rest = expect_byte(skip_ws(rest), b'"')?;

    let end = memchr::memchr2(b'"', b'\\', rest)?;

    if rest[end] != b'"' {
        return None;
    }

    std::str::from_utf8(&rest[..end]).ok()
}

/// Drops leading JSON whitespace.
fn skip_ws(body: &[u8]) -> &[u8] {
    let first_value = body
        .iter()
        .position(|byte| !matches!(byte, b' ' | b'\t' | b'\n' | b'\r'))
        .unwrap_or(body.len());

    &body[first_value..]
}

/// Consumes one expected byte, or gives up.
fn expect_byte(body: &[u8], byte: u8) -> Option<&[u8]> {
    body.split_first()
        .and_then(|(first, rest)| (*first == byte).then_some(rest))
}

/// Waits for the first node to leave its rate limit, tracking how many
/// requests are parked in the meantime.
async fn wait_rate_limited(best_time: Duration) {
    let _guard = metrics::sleeping_on_rate_limit();

    tokio::time::sleep(best_time).await;
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    /// The scan is only a shortcut, so on every body it accepts it must name
    /// the same method a full parse would. Bodies it declines are not listed
    /// here — they fall back to serde and are its problem.
    #[test]
    fn scan_agrees_with_the_parse_it_replaces() {
        let corpus: &[&[u8]] = &[
            br#"{"method":"getSlot","params":[],"id":1,"jsonrpc":"2.0"}"#,
            br#"{ "method" : "getSlot" , "id":1}"#,
            b"\n\t{\"method\":\"getBalance\",\"id\":1}",
            br#"{"method":"","id":1}"#,
            // "method" also appears nested, and must not win.
            br#"{"method":"outer","params":[{"method":"inner"}],"id":1}"#,
            // Declined by the scan, still routable through serde.
            br#"{"jsonrpc":"2.0","method":"getSlot","id":1}"#,
            br#"[{"method":"getSlot","id":1}]"#,
            br#"{"method":"get\u0053lot","id":1}"#,
            b"",
            b"not json at all",
        ];

        for body in corpus {
            let Some(scanned) = scan_leading_method(body) else {
                continue;
            };

            let parsed = serde_json::from_slice::<MethodExtractor<'_>>(body)
                .unwrap_or_else(|e| panic!("scan accepted a body serde rejects: {e}"));

            assert_eq!(
                scanned,
                parsed.method,
                "disagreement on {}",
                String::from_utf8_lossy(body)
            );
        }
    }

    /// The balancer's own failures go back to a client that speaks JSON-RPC and
    /// nothing else. Pinned on the bytes rather than on the message text: the
    /// integration tests match a substring, which reads the same inside and
    /// outside a JSON envelope and would not notice the envelope going away.
    #[test]
    fn every_route_error_answers_with_parseable_json_rpc() {
        for (error, expected_code) in [
            (RouteError::BadRequest, -32700),
            (RouteError::AllNodesFailed, -32000),
            (RouteError::Timeout, -32000),
        ] {
            let body: serde_json::Value = serde_json::from_slice(error.body())
                .unwrap_or_else(|e| panic!("{error:?} body is not JSON: {e}"));

            assert_eq!(body["jsonrpc"], "2.0", "{error:?}");
            assert_eq!(body["error"]["code"], expected_code, "{error:?}");
            assert!(
                body["error"]["message"]
                    .as_str()
                    .is_some_and(|m| !m.is_empty()),
                "{error:?} carries no message"
            );
        }
    }
}
