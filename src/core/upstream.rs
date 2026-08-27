//! Talking to one upstream node: send the body, decide what the answer means,
//! and record what it says about the node.
//!
//! This is the balancer's only source of health information. There is no probe
//! traffic: every attempt a client pays for is also the measurement, folded
//! into the node's latency average when it answers and turned into a penalty
//! when it does not. See [`RpcNode::observe_answer`] and [`RpcNode::penalize`].
//!
//! Nothing here chooses a node or retries — that is [`crate::core::router`].

use bytes::Bytes;
use memchr::memmem;
use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderValue, RETRY_AFTER};
use reqwest::{Client, Response, StatusCode};
use std::sync::LazyLock;
use std::time::Duration;
use tokio::time::Instant;
use tracing::debug;

use crate::core::node::RpcNode;
use crate::metrics::Outcome;
use crate::protocol::rpc_payload::RpcErrorOnly;

/// Backstop on a single HTTP request to an upstream.
///
/// Every caller wraps a shorter budget of its own around a request — the router
/// its global timeout, the probe its per-attempt one — so this only bounds the
/// case where a caller forgets to. It is deliberately the loosest of the three:
/// a backstop that fires first would silently replace the budget the caller
/// meant to enforce.
pub const HTTP_BACKSTOP: Duration = Duration::from_secs(2);

/// Upstream bodies up to this size are always parsed, larger ones only when
/// they contain an `"error"` key. Any plausible non-JSON upstream answer (a
/// gateway error page, a rate-limit notice) fits below this, so the "invalid
/// JSON means retry" guarantee still holds where invalid JSON actually shows
/// up. Real payloads are an order of magnitude larger and are only parsed when
/// the `"error"` key is present at all — parsing them costs time proportional
/// to the response size and answers a question a substring search settles.
const VALIDATE_BELOW: usize = 512;

/// The substring gate for large bodies. Built once: the finder precomputes a
/// prefilter, and rebuilding it per response was most of the cost of the
/// search.
static ERROR_KEY: LazyLock<memmem::Finder<'static>> =
    LazyLock::new(|| memmem::Finder::new(br#""error""#));

/// The only content type an upstream is ever posted.
///
/// Pre-parsed: passing the pair as `&str` made reqwest validate the name,
/// lower-case it, and validate the value again on every single request. As a
/// `HeaderValue` the header costs a clone of a refcount.
const JSON: HeaderValue = HeaderValue::from_static("application/json");

/// Outcome of a single attempt against one node.
pub enum Attempt {
    /// Response is final and goes back to the client.
    Done(StatusCode, Bytes),
    /// Attempt failed in a retryable way; move on to the next node.
    Retry,
}

/// Builds the one pooled HTTP client every upstream is reached through.
///
/// # Errors
///
/// Returns an error if the client cannot be initialized, e.g. a TLS backend
/// failure.
pub fn client() -> reqwest::Result<Client> {
    Client::builder().timeout(HTTP_BACKSTOP).build()
}

/// Performs one HTTP attempt against a node, classifies the result, and folds
/// what it says about the node into the node.
///
/// Every path out of here has recorded exactly one upstream attempt, which is
/// what makes the `outcome` label a partition of the attempts, and has either
/// updated the node's latency or penalized it.
pub async fn attempt(client: &Client, node: &RpcNode, body: Bytes) -> Attempt {
    let started = Instant::now();

    let sent = client
        .post(node.url.clone())
        .header(CONTENT_TYPE, JSON)
        .body(body)
        .send()
        .await;

    match sent {
        Ok(response) => classify_response(node, response, started).await,
        Err(e) => {
            // DNS, connection, TLS, or the client's own timeout: the node did
            // not answer at all, which is the clearest signal it gives.
            node.penalize(started, 0);
            node.metrics
                .record_attempt(Outcome::TransportError, started.elapsed().as_secs_f64());
            debug!(node = %node.name, error = %e, "upstream HTTP request failed");

            Attempt::Retry
        }
    }
}

/// Decides whether an upstream response is final or the next node should be
/// tried, recording the per-attempt metrics and the node's health for either
/// case.
///
/// Two questions are answered here and they are not the same one. Whether the
/// *request* is settled decides what the client gets; whether the *node* is
/// well decides whether it keeps taking traffic. A 404 settles the request and
/// says nothing bad about the node; a 503 says the node is unwell whether or
/// not its body is forwarded.
async fn classify_response(node: &RpcNode, response: Response, started: Instant) -> Attempt {
    let status_code = response.status();

    // Read before the body is consumed, and only on the one status that carries
    // it — a header lookup on every response buys nothing.
    let retry_after_s = if status_code == StatusCode::TOO_MANY_REQUESTS {
        retry_after_secs(response.headers())
    } else {
        0
    };

    let body = match response.bytes().await {
        Ok(body) => body,
        Err(e) => {
            // The node accepted the request and then cut the answer short.
            node.penalize(started, 0);
            node.metrics
                .record_attempt(Outcome::BodyError, started.elapsed().as_secs_f64());
            debug!(
                node = %node.name,
                error = %e,
                "failed to read upstream response body"
            );

            return Attempt::Retry;
        }
    };

    if is_final_status(status_code) {
        // The request's problem, not the node's: it answered, promptly and
        // correctly, that this body is not one it will serve.
        node.observe_answer(started);
        node.metrics
            .record_attempt(Outcome::ForwardedHttpError, started.elapsed().as_secs_f64());

        return Attempt::Done(status_code, body);
    }

    if body.len() <= VALIDATE_BELOW || ERROR_KEY.find(body.as_ref()).is_some() {
        let parse_error: RpcErrorOnly = match serde_json::from_slice(body.as_ref()) {
            Ok(res) => res,
            Err(e) => {
                // An RPC endpoint that answers with something other than JSON is
                // not serving RPC right now — a gateway error page, an auth
                // wall, a truncated body.
                node.penalize(started, retry_after_s);
                node.metrics
                    .record_attempt(Outcome::InvalidJson, started.elapsed().as_secs_f64());
                debug!(
                    node = %node.name,
                    error = %e,
                    "upstream returned invalid JSON"
                );

                return Attempt::Retry;
            }
        };

        if let Some(err) = parse_error.error {
            if !is_retryable_json_rpc_error(err.code) {
                node.observe_answer(started);
                node.metrics
                    .record_attempt(Outcome::ForwardedRpcError, started.elapsed().as_secs_f64());

                return Attempt::Done(status_code, body);
            }

            // The node itself declined: out of capacity, behind on its ledger,
            // internally broken. Whichever it is, stop sending to it.
            node.penalize(started, retry_after_s);
            node.metrics
                .record_attempt(Outcome::RetryableRpcError, started.elapsed().as_secs_f64());
            debug!(
                node = %node.name,
                code = err.code,
                "upstream returned retryable RPC error"
            );

            return Attempt::Retry;
        }
    }

    if status_code.is_success() {
        node.observe_answer(started);
        node.metrics
            .record_attempt(Outcome::Success, started.elapsed().as_secs_f64());

        return Attempt::Done(status_code, body);
    }

    // A status that is neither final nor a success, over a body carrying no
    // JSON-RPC error of its own: a 429 or a 5xx with a plain-text or empty
    // body. The body still goes to the client — retrying it on another node
    // would not improve what this one already said — but the node is held out
    // of rotation, which is the whole point of watching for these.
    node.penalize(started, retry_after_s);
    node.metrics
        .record_attempt(Outcome::ForwardedHttpError, started.elapsed().as_secs_f64());

    Attempt::Done(status_code, body)
}

/// How many seconds an upstream asked to be left alone for, or `0` when it
/// asked for nothing this can act on.
///
/// Delta-seconds only. `Retry-After` may also carry an HTTP-date, which fails
/// the parse here and falls back to the balancer's own penalty — a date is rare
/// from an RPC provider, and the fallback is the same order of magnitude.
fn retry_after_secs(headers: &HeaderMap) -> u32 {
    headers
        .get(RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(0)
}

/// Whether an HTTP status settles the request on its own, with no reason to
/// look at the body or at another node.
///
/// An allow-list of the statuses that say something about the *request*:
/// repeating it against another node would only produce the same answer more
/// slowly. Everything else — 5xx, 429, an unassigned code — is the node's
/// problem, not the request's, and a success still has a body that may carry a
/// JSON-RPC error inside it.
#[must_use]
pub const fn is_final_status(status: StatusCode) -> bool {
    // A `matches!` and not an array `contains`: the same list, but the compiler
    // turns ranges into comparisons instead of walking 26 elements per
    // response. 401 and 403 are deliberately absent — an upstream rejecting the
    // balancer's own credentials is that node's problem, and another node may
    // well accept the request.
    matches!(
        status.as_u16(),
        400 | 402 | 404..=418 | 421..=426 | 428 | 431 | 451
    )
}

/// Whether a JSON-RPC error code leaves room to try another node.
///
/// A deny-list over the four codes the spec defines for a malformed or
/// unanswerable request: parse error, invalid request, method not found,
/// invalid params. Another node would reject the identical body identically, so
/// those are forwarded to the client as they are.
#[must_use]
pub fn is_retryable_json_rpc_error(error_code: i32) -> bool {
    let not_retryable = [-32700, -32601, -32602, -32600];

    !not_retryable.contains(&error_code)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn not_final_for_5xx_and_429() {
        // The node's problem, not the request's: another node may well answer.
        assert!(!is_final_status(StatusCode::TOO_MANY_REQUESTS));
        assert!(!is_final_status(StatusCode::INTERNAL_SERVER_ERROR));
        assert!(!is_final_status(StatusCode::BAD_GATEWAY));
        assert!(!is_final_status(StatusCode::SERVICE_UNAVAILABLE));
        assert!(!is_final_status(StatusCode::GATEWAY_TIMEOUT));
    }

    #[test]
    fn not_final_for_success() {
        // A 200 is not settled by its status alone — the body still has to be
        // checked for a JSON-RPC error.
        assert!(!is_final_status(StatusCode::OK));
    }

    #[test]
    fn final_for_client_error_statuses() {
        let final_statuses = [
            400, 402, 404, 405, 406, 407, 408, 409, 410, 411, 412, 413, 414, 415, 416, 417, 418,
            421, 422, 423, 424, 425, 426, 428, 431, 451,
        ];
        for code in final_statuses {
            assert!(
                is_final_status(StatusCode::from_u16(code).unwrap()),
                "expected {code} to be final"
            );
        }
    }

    #[test]
    fn retryable_jsonrpc_false_for_not_retryable_codes() {
        for code in [-32700, -32601, -32602, -32600] {
            assert!(
                !is_retryable_json_rpc_error(code),
                "expected {code} to be not-retryable"
            );
        }
    }

    #[test]
    fn retryable_jsonrpc_true_for_server_internal_and_zero() {
        assert!(is_retryable_json_rpc_error(-32000)); // server error
        assert!(is_retryable_json_rpc_error(-32603)); // internal error
        assert!(is_retryable_json_rpc_error(0)); // no error
    }

    fn headers_with(retry_after: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, HeaderValue::from_str(retry_after).unwrap());

        headers
    }

    #[test]
    fn retry_after_reads_delta_seconds() {
        assert_eq!(retry_after_secs(&headers_with("30")), 30);
        // Providers do pad the value; a header the balancer cannot read is a
        // cooldown it silently shortens to its own default.
        assert_eq!(retry_after_secs(&headers_with(" 30 ")), 30);
    }

    #[test]
    fn retry_after_falls_back_to_zero_on_anything_else() {
        // Absent, an HTTP-date, or nonsense: the caller applies its own penalty
        // rather than reading a zero cooldown out of an unparseable header.
        assert_eq!(retry_after_secs(&HeaderMap::new()), 0);
        assert_eq!(
            retry_after_secs(&headers_with("Wed, 21 Oct 2015 07:28:00 GMT")),
            0
        );
        assert_eq!(retry_after_secs(&headers_with("-5")), 0);
    }
}
