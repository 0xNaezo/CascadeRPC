//! The health probe: one node, one verdict.
//!
//! Deliberately a `getHealth` RPC call and not a TCP connect or an HTTP ping —
//! a node that accepts connections while its ledger is behind is exactly the
//! node this has to catch.

use bytes::Bytes;
use reqwest::Client;
use std::time::Duration;
use tracing::debug;

use crate::core::node::RpcNode;
use crate::core::upstream;
use crate::protocol::rpc_payload::RpcHealthResponse;

/// The probe body, built once. Never changes, and `getHealth` takes no
/// parameters.
static PROBE_BODY: Bytes = Bytes::from_static(br#"{"jsonrpc":"2.0","id":1,"method":"getHealth"}"#);

/// Probes one node and reports whether it is healthy, and how long its answer
/// took in milliseconds.
///
/// Up to three attempts, one per second, each given 500ms to answer, so a node
/// that never responds costs ~2.5s. The retries are what keep a single dropped
/// packet from taking a healthy node out of the routing table for a whole
/// interval; the first clean answer wins and the rest are skipped.
///
/// Healthy means the node said so: a JSON-RPC response with no error and a
/// `result` of exactly `"ok"`. Anything else — a timeout, a transport failure, a
/// body that does not parse, a degraded node reporting its own state — is
/// unhealthy, with `u32::MAX` for the latency, the sentinel that sorts an
/// unmeasured node to the back of the routing table.
pub async fn probe(client: &Client, node: &RpcNode) -> (bool, u32) {
    debug!(node = %node.name, "checking node health");

    // The first tick is immediate, so this paces the retries without
    // delaying the first attempt.
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    for _ in 0..3 {
        interval.tick().await;

        let start_time = tokio::time::Instant::now();

        let Ok(Ok(response)) = tokio::time::timeout(
            Duration::from_millis(500),
            upstream::post(client, PROBE_BODY.clone(), node.url.clone()),
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
