#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::{Router, extract::State, response::IntoResponse, routing::post};
use bytes::Bytes;
use reqwest::StatusCode;
use rpc_load_balancer::{
    core::{
        node::{NewNode, RpcNode},
        rpc::RpcClient,
    },
    provider::cost_table::ProviderCostTable,
};

struct MockState {
    status: u16,
    body: &'static str,
    latency: Duration,
    request_count: Arc<AtomicUsize>,
}

async fn handle_rpc(State(state): State<Arc<MockState>>) -> impl IntoResponse {
    state.request_count.fetch_add(1, Ordering::Relaxed);
    tokio::time::sleep(state.latency).await;
    (StatusCode::from_u16(state.status).unwrap(), state.body)
}

/// Spawns a mock RPC node on an ephemeral port, returns its URL and a request counter.
async fn spawn_mock(status: u16, body: &'static str) -> (String, Arc<AtomicUsize>) {
    spawn_mock_latency(status, body, Duration::ZERO).await
}

async fn spawn_mock_latency(
    status: u16,
    body: &'static str,
    latency: Duration,
) -> (String, Arc<AtomicUsize>) {
    let count = Arc::new(AtomicUsize::new(0));
    let state = Arc::new(MockState {
        status,
        body,
        latency,
        request_count: count.clone(),
    });
    let app = Router::new().route("/", post(handle_rpc)).with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    // axum::serve needs a graceful-shutdown future before it impls Future (axum 0.8);
    // `pending::<()>()` never resolves, so the server lives for the test's duration.
    tokio::spawn(async move {
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(std::future::pending::<()>())
            .await;
    });
    (format!("http://{addr}"), count)
}

fn build_client(node: RpcNode) -> RpcClient {
    RpcClient::new(vec![node]).unwrap()
}

/// Cost table that prices `getBalance` (the method in `OK_BODY`), so nodes
/// aren't skipped by the u32::MAX "can't implement" guard.
fn priced_table() -> ProviderCostTable {
    ProviderCostTable::new(HashMap::from([("getBalance".to_string(), 1)]))
}

const OK_BODY: &str = r#"{"jsonrpc":"2.0","method":"getBalance","result":"ok","id":1}"#;
const INVALID_PARAMS_BODY: &str =
    r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32602,"message":"invalid params"}}"#;
const SERVER_ERROR_BODY: &str =
    r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"server error"}}"#;

#[tokio::test]
async fn success_path() {
    let (url, count) = spawn_mock(200, OK_BODY).await;
    let node = RpcNode::new(NewNode {
        name: "A".to_string(),
        url: url.clone(),
        rps_limit: 100,
        max_concurrent: 10,
        tier: 0,
        method_costs: priced_table(),
        monthly_limit: u64::MAX,
        billing_type: "requests".to_string(),
        spillover_percent: 100,
    })
    .unwrap();
    let client = build_client(node);

    let (status, body) = client.send(Bytes::from(OK_BODY)).await.unwrap();

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, Bytes::from(OK_BODY));
    assert_eq!(count.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn non_retryable_http_error_passthrough() {
    let (url, count) = spawn_mock(400, "bad request body").await;
    let node = RpcNode::new(NewNode {
        name: "A".to_string(),
        url: url.clone(),
        rps_limit: 100,
        max_concurrent: 10,
        tier: 0,
        method_costs: priced_table(),
        monthly_limit: u64::MAX,
        billing_type: "requests".to_string(),
        spillover_percent: 100,
    })
    .unwrap();
    let client = build_client(node);

    let (status, body) = client.send(Bytes::from(OK_BODY)).await.unwrap();

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body, Bytes::from("bad request body"));
    assert_eq!(count.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn non_retryable_jsonrpc_error_passthrough() {
    let (url, count) = spawn_mock(200, INVALID_PARAMS_BODY).await;
    let node = RpcNode::new(NewNode {
        name: "A".to_string(),
        url: url.clone(),
        rps_limit: 100,
        max_concurrent: 10,
        tier: 0,
        method_costs: priced_table(),
        monthly_limit: u64::MAX,
        billing_type: "requests".to_string(),
        spillover_percent: 100,
    })
    .unwrap();
    let client = build_client(node);

    let (status, body) = client.send(Bytes::from(OK_BODY)).await.unwrap();

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, Bytes::from(INVALID_PARAMS_BODY));
    assert_eq!(count.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn tier_spillover_on_rate_limit() {
    let (url_t0, count_t0) = spawn_mock(200, OK_BODY).await;
    let (url_t1, count_t1) = spawn_mock(200, OK_BODY).await;

    // Tier 0 rps=1: one token, consumed by the first request.
    // Tier 1 rps=100: ample headroom to absorb the spillover from the second request.
    let node_t0 = RpcNode::new(NewNode {
        name: "T0".to_string(),
        url: url_t0.clone(),
        rps_limit: 1,
        max_concurrent: 10,
        tier: 0,
        method_costs: priced_table(),
        monthly_limit: u64::MAX,
        billing_type: "requests".to_string(),
        spillover_percent: 100,
    })
    .unwrap();
    let node_t1 = RpcNode::new(NewNode {
        name: "T1".to_string(),
        url: url_t1.clone(),
        rps_limit: 100,
        max_concurrent: 10,
        tier: 1,
        method_costs: priced_table(),
        monthly_limit: u64::MAX,
        billing_type: "requests".to_string(),
        spillover_percent: 100,
    })
    .unwrap();
    let client = RpcClient::new(vec![node_t0, node_t1]).unwrap();

    let (s1, _) = client.send(Bytes::from(OK_BODY)).await.unwrap();
    assert_eq!(s1, StatusCode::OK);

    let (s2, _) = client.send(Bytes::from(OK_BODY)).await.unwrap();
    assert_eq!(s2, StatusCode::OK);

    // First request hit Tier 0 (consumed the lone token), second spilled to Tier 1
    // without waiting for the Tier 0 bucket to refill.
    assert_eq!(count_t0.load(Ordering::Relaxed), 1);
    assert_eq!(count_t1.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn retryable_jsonrpc_error_exhausts_nodes_then_fails() {
    // -32000 is retryable: the loop walks past it, then with no rate-limit
    // pressure to schedule a wait the for-loop ends with best_time=None,
    // surfacing the "All nodes failed" error path fast rather than retrying.
    let (url, _) = spawn_mock(200, SERVER_ERROR_BODY).await;
    let node = RpcNode::new(NewNode {
        name: "A".to_string(),
        url: url.clone(),
        rps_limit: 100,
        max_concurrent: 10,
        tier: 0,
        method_costs: priced_table(),
        monthly_limit: u64::MAX,
        billing_type: "requests".to_string(),
        spillover_percent: 100,
    })
    .unwrap();
    let client = build_client(node);

    let err = client
        .send(Bytes::from(OK_BODY))
        .await
        .expect_err("retryable error with no rate-limit wait should fail fast");
    let err_str = String::from_utf8_lossy(&err);
    assert!(
        err_str.contains("All nodes failed"),
        "unexpected error: {err_str}"
    );
}

#[tokio::test]
async fn all_transport_errors_no_wait() {
    // Bind a listener to grab an unused port, then drop it: nothing answers.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let url = format!("http://{addr}");

    let node = RpcNode::new(NewNode {
        name: "dead".to_string(),
        url: url.clone(),
        rps_limit: 100,
        max_concurrent: 10,
        tier: 0,
        method_costs: priced_table(),
        monthly_limit: u64::MAX,
        billing_type: "requests".to_string(),
        spillover_percent: 100,
    })
    .unwrap();
    let client = build_client(node);

    let err = client
        .send(Bytes::from(OK_BODY))
        .await
        .expect_err("transport failure should produce an error, not a 502");
    let err_str = String::from_utf8_lossy(&err);
    assert!(
        err_str.contains("All nodes failed"),
        "unexpected error: {err_str}"
    );
}

#[tokio::test]
async fn unsupported_method_skips_node() {
    // Default table prices nothing → cost() returns u32::MAX → node is skipped
    // before the request is sent.
    let (url, count) = spawn_mock(200, OK_BODY).await;
    let node = RpcNode::new(NewNode {
        name: "A".to_string(),
        url: url.clone(),
        rps_limit: 100,
        max_concurrent: 10,
        tier: 0,
        method_costs: ProviderCostTable::default(),
        monthly_limit: u64::MAX,
        billing_type: "requests".to_string(),
        spillover_percent: 100,
    })
    .unwrap();
    let client = build_client(node);

    let err = client
        .send(Bytes::from(OK_BODY))
        .await
        .expect_err("unsupported method should skip the node");
    let err_str = String::from_utf8_lossy(&err);
    assert!(
        err_str.contains("no rate limits"),
        "unexpected error: {err_str}"
    );
    assert_eq!(count.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn fallback_success_on_retryable_rpc_error() {
    let (url_a, count_a) = spawn_mock(200, SERVER_ERROR_BODY).await;
    let (url_b, count_b) = spawn_mock(200, OK_BODY).await;

    let node_a = RpcNode::new(NewNode {
        name: "A".to_string(),
        url: url_a.clone(),
        rps_limit: 100,
        max_concurrent: 10,
        tier: 0,
        method_costs: priced_table(),
        monthly_limit: u64::MAX,
        billing_type: "requests".to_string(),
        spillover_percent: 100,
    })
    .unwrap();
    let node_b = RpcNode::new(NewNode {
        name: "B".to_string(),
        url: url_b.clone(),
        rps_limit: 100,
        max_concurrent: 10,
        tier: 0,
        method_costs: priced_table(),
        monthly_limit: u64::MAX,
        billing_type: "requests".to_string(),
        spillover_percent: 100,
    })
    .unwrap();
    let client = RpcClient::new(vec![node_a, node_b]).unwrap();

    let (status, body) = client.send(Bytes::from(OK_BODY)).await.unwrap();

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, Bytes::from(OK_BODY));
    // Node A returned a retryable -32000, node B absorbed the retry.
    assert_eq!(count_a.load(Ordering::Relaxed), 1);
    assert_eq!(count_b.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn fallback_success_on_transport_error() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let dead_url = format!("http://{addr}");

    let (url_b, count_b) = spawn_mock(200, OK_BODY).await;

    let node_a = RpcNode::new(NewNode {
        name: "A".to_string(),
        url: dead_url.clone(),
        rps_limit: 100,
        max_concurrent: 10,
        tier: 0,
        method_costs: priced_table(),
        monthly_limit: u64::MAX,
        billing_type: "requests".to_string(),
        spillover_percent: 100,
    })
    .unwrap();
    let node_b = RpcNode::new(NewNode {
        name: "B".to_string(),
        url: url_b.clone(),
        rps_limit: 100,
        max_concurrent: 10,
        tier: 0,
        method_costs: priced_table(),
        monthly_limit: u64::MAX,
        billing_type: "requests".to_string(),
        spillover_percent: 100,
    })
    .unwrap();
    let client = RpcClient::new(vec![node_a, node_b]).unwrap();

    let (status, body) = client.send(Bytes::from(OK_BODY)).await.unwrap();

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, Bytes::from(OK_BODY));
    assert_eq!(count_b.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn sleep_and_retry_after_rate_limit_exhaustion() {
    let (url, count) = spawn_mock(200, OK_BODY).await;
    let node = RpcNode::new(NewNode {
        name: "A".to_string(),
        url: url.clone(),
        rps_limit: 10,
        max_concurrent: 10,
        tier: 0,
        method_costs: priced_table(),
        monthly_limit: u64::MAX,
        billing_type: "requests".to_string(),
        spillover_percent: 100,
    })
    .unwrap();
    let client = build_client(node.clone());

    // Burn the 10-token burst directly (no HTTP), leaving the bucket empty.
    // ponytail: waits ~100ms (1s quota / 10 rps), comfortably inside the 1s
    // global timeout; an rps=1 variant would sleep ~1s and race it.
    for _ in 0..10 {
        drop(node.acquire_and_check().await.unwrap());
    }

    let (status, _) = client.send(Bytes::from(OK_BODY)).await.unwrap();

    assert_eq!(status, StatusCode::OK);
    assert_eq!(count.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn global_timeout_fails_fast() {
    let (url, _) = spawn_mock_latency(200, OK_BODY, Duration::from_millis(1500)).await;
    let node = RpcNode::new(NewNode {
        name: "slow".to_string(),
        url: url.clone(),
        rps_limit: 100,
        max_concurrent: 10,
        tier: 0,
        method_costs: priced_table(),
        monthly_limit: u64::MAX,
        billing_type: "requests".to_string(),
        spillover_percent: 100,
    })
    .unwrap();
    let client = build_client(node);

    let err = client
        .send(Bytes::from(OK_BODY))
        .await
        .expect_err("a node slower than the 1s budget should time out");
    let err_str = String::from_utf8_lossy(&err);
    assert!(
        err_str.contains("Global timeout"),
        "unexpected error: {err_str}"
    );
}

#[tokio::test]
async fn invalid_json_upstream_fails() {
    let (url, _) = spawn_mock(200, "not json").await;
    let node = RpcNode::new(NewNode {
        name: "A".to_string(),
        url: url.clone(),
        rps_limit: 100,
        max_concurrent: 10,
        tier: 0,
        method_costs: priced_table(),
        monthly_limit: u64::MAX,
        billing_type: "requests".to_string(),
        spillover_percent: 100,
    })
    .unwrap();
    let client = build_client(node);

    let err = client
        .send(Bytes::from(OK_BODY))
        .await
        .expect_err("unparseable upstream body should fail the request");
    let err_str = String::from_utf8_lossy(&err);
    assert!(
        err_str.contains("All nodes failed"),
        "unexpected error: {err_str}"
    );
}
