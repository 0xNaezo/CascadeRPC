//! Talking to one upstream node: send the body, and decide what the answer
//! means.
//!
//! Shared by the router and the health probe, which is the reason it is a
//! module of free functions and not more methods on the client: neither of them
//! needs anything from the balancer's state to make one HTTP request.
//!
//! Nothing here chooses a node or retries — that is [`crate::core::router`].

use bytes::Bytes;
use memchr::memmem;
use reqwest::{Client, Response, StatusCode};
use std::sync::LazyLock;
use std::time::Duration;
use tokio::time::Instant;
use tracing::debug;
use url::Url;

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

/// Posts a body to one upstream URL, verbatim. Takes the URL rather than a node
/// because the health probe has no request to bill and no node state to touch.
///
/// # Errors
///
/// Returns an error if the request cannot be completed at all: DNS, connection,
/// TLS, or the client's own timeout. An HTTP error status is a completed
/// request and comes back as `Ok`.
pub async fn post(client: &Client, body: Bytes, url: Url) -> reqwest::Result<Response> {
    client
        .post(url)
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
}

/// Performs one HTTP attempt against a node and classifies the result.
///
/// Every path out of here has recorded exactly one upstream attempt, which is
/// what makes the `outcome` label a partition of the attempts.
pub async fn attempt(client: &Client, node: &RpcNode, body: Bytes) -> Attempt {
    let started = Instant::now();

    match post(client, body, node.url.clone()).await {
        Ok(response) => classify_response(node, response, started).await,
        Err(e) => {
            node.metrics
                .record_attempt(Outcome::TransportError, started.elapsed().as_secs_f64());
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
        node.metrics
            .record_attempt(Outcome::ForwardedHttpError, started.elapsed().as_secs_f64());

        return Attempt::Done(status_code, body);
    }

    if body.len() <= VALIDATE_BELOW || ERROR_KEY.find(body.as_ref()).is_some() {
        let parse_error: RpcErrorOnly = match serde_json::from_slice(body.as_ref()) {
            Ok(res) => res,
            Err(e) => {
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
                node.metrics
                    .record_attempt(Outcome::ForwardedRpcError, started.elapsed().as_secs_f64());

                return Attempt::Done(status_code, body);
            }

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

    let outcome = if status_code.is_success() {
        Outcome::Success
    } else {
        Outcome::ForwardedHttpError
    };
    node.metrics
        .record_attempt(outcome, started.elapsed().as_secs_f64());

    Attempt::Done(status_code, body)
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
pub fn is_final_status(status: StatusCode) -> bool {
    let final_statuses = [
        400, 402, 404, 405, 406, 407, 408, 409, 410, 411, 412, 413, 414, 415, 416, 417, 418, 421,
        422, 423, 424, 425, 426, 428, 431, 451,
    ];

    final_statuses.contains(&status.as_u16())
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
}
