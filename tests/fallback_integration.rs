//! Fallback and retry behaviour of the router: which upstream failures are
//! forwarded to the client and which send the request to the next node.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::time::Duration;

use bytes::Bytes;
use reqwest::StatusCode;

mod common;

use common::{
    BATCH_BODY, INVALID_PARAMS_BODY, NO_ERROR_BODY, OK_BODY, SERVER_ERROR_BODY,
    assert_err_contains, build_client, build_client_one, dead_url, node, spawn_mock,
    spawn_mock_latency,
};

#[tokio::test]
async fn success_path() {
    let mock = spawn_mock(200, OK_BODY).await;
    let client = build_client_one(node("A", &mock.url).build());

    let (status, body) = client.send(Bytes::from(OK_BODY)).await.unwrap();

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, Bytes::from(OK_BODY));
    assert_eq!(mock.hits(), 1);
}

#[tokio::test]
async fn non_retryable_http_error_passthrough() {
    let mock = spawn_mock(400, "bad request body").await;
    let client = build_client_one(node("A", &mock.url).build());

    let (status, body) = client.send(Bytes::from(OK_BODY)).await.unwrap();

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body, Bytes::from("bad request body"));
    assert_eq!(mock.hits(), 1);
}

#[tokio::test]
async fn non_retryable_jsonrpc_error_passthrough() {
    let mock = spawn_mock(200, INVALID_PARAMS_BODY).await;
    let client = build_client_one(node("A", &mock.url).build());

    let (status, body) = client.send(Bytes::from(OK_BODY)).await.unwrap();

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, Bytes::from(INVALID_PARAMS_BODY));
    assert_eq!(mock.hits(), 1);
}

#[tokio::test]
async fn tier_spillover_on_rate_limit() {
    let t0 = spawn_mock(200, OK_BODY).await;
    let t1 = spawn_mock(200, OK_BODY).await;

    // Tier 0 rps=1: one token, consumed by the first request.
    // Tier 1 rps=100: ample headroom to absorb the spillover from the second request.
    let client = build_client(vec![
        node("T0", &t0.url).tier(0).rps(1).build(),
        node("T1", &t1.url).tier(1).build(),
    ]);

    let (s1, _) = client.send(Bytes::from(OK_BODY)).await.unwrap();
    assert_eq!(s1, StatusCode::OK);

    let (s2, _) = client.send(Bytes::from(OK_BODY)).await.unwrap();
    assert_eq!(s2, StatusCode::OK);

    // First request hit Tier 0 (consumed the lone token), second spilled to Tier 1
    // without waiting for the Tier 0 bucket to refill.
    assert_eq!(t0.hits(), 1);
    assert_eq!(t1.hits(), 1);
}

#[tokio::test]
async fn retryable_jsonrpc_error_exhausts_nodes_then_fails() {
    // -32000 is retryable: the loop walks past it, then with no rate-limit
    // pressure to schedule a wait the for-loop ends with best_time=None,
    // surfacing the "All nodes failed" error path fast rather than retrying.
    let mock = spawn_mock(200, SERVER_ERROR_BODY).await;
    let client = build_client_one(node("A", &mock.url).build());

    let (_, err) = client
        .send(Bytes::from(OK_BODY))
        .await
        .expect_err("retryable error with no rate-limit wait should fail fast");

    assert_err_contains(&err, "All nodes failed");
}

#[tokio::test]
async fn all_transport_errors_no_wait() {
    let client = build_client_one(node("dead", &dead_url().await).build());

    let (status, err) = client
        .send(Bytes::from(OK_BODY))
        .await
        .expect_err("transport failure should produce an error, not a 502");

    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_err_contains(&err, "All nodes failed");
}

#[tokio::test]
async fn unsupported_method_skips_node() {
    // Default table prices nothing → cost() returns u32::MAX → node is skipped
    // before the request is sent.
    let mock = spawn_mock(200, OK_BODY).await;
    let client = build_client_one(node("A", &mock.url).prices_nothing().build());

    let (_, err) = client
        .send(Bytes::from(OK_BODY))
        .await
        .expect_err("unsupported method should skip the node");

    assert_err_contains(&err, "no rate limits");
    assert_eq!(mock.hits(), 0);
}

#[tokio::test]
async fn fallback_success_on_retryable_rpc_error() {
    let a = spawn_mock(200, SERVER_ERROR_BODY).await;
    let b = spawn_mock(200, OK_BODY).await;

    let client = build_client(vec![node("A", &a.url).build(), node("B", &b.url).build()]);

    let (status, body) = client.send(Bytes::from(OK_BODY)).await.unwrap();

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, Bytes::from(OK_BODY));
    // Node A returned a retryable -32000, node B absorbed the retry.
    assert_eq!(a.hits(), 1);
    assert_eq!(b.hits(), 1);
}

#[tokio::test]
async fn fallback_success_on_transport_error() {
    let b = spawn_mock(200, OK_BODY).await;

    let client = build_client(vec![
        node("A", &dead_url().await).build(),
        node("B", &b.url).build(),
    ]);

    let (status, body) = client.send(Bytes::from(OK_BODY)).await.unwrap();

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, Bytes::from(OK_BODY));
    assert_eq!(b.hits(), 1);
}

#[tokio::test]
async fn sleep_and_retry_after_rate_limit_exhaustion() {
    let mock = spawn_mock(200, OK_BODY).await;
    let rpc_node = node("A", &mock.url).rps(10).build();
    let client = build_client_one(rpc_node.clone());

    // Burn the 10-token burst directly (no HTTP), leaving the bucket empty.
    // Waits ~100ms (1s quota / 10 rps), comfortably inside the 1s global
    // timeout; an rps=1 variant would sleep ~1s and race it.
    for _ in 0..10 {
        drop(rpc_node.acquire_and_check().await.unwrap());
    }

    let (status, _) = client.send(Bytes::from(OK_BODY)).await.unwrap();

    assert_eq!(status, StatusCode::OK);
    assert_eq!(mock.hits(), 1);
}

#[tokio::test]
async fn global_timeout_fails_fast() {
    let mock = spawn_mock_latency(200, OK_BODY, Duration::from_millis(1500)).await;
    let client = build_client_one(node("slow", &mock.url).build());

    let (status, err) = client
        .send(Bytes::from(OK_BODY))
        .await
        .expect_err("a node slower than the 1s budget should time out");

    assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
    assert_err_contains(&err, "Global timeout");
}

#[tokio::test]
async fn invalid_json_upstream_fails() {
    let mock = spawn_mock(200, "not json").await;
    let client = build_client_one(node("A", &mock.url).build());

    let (_, err) = client
        .send(Bytes::from(OK_BODY))
        .await
        .expect_err("unparseable upstream body should fail the request");

    assert_err_contains(&err, "All nodes failed");
}

// ---------------------------------------------------------------------------
// Body-vs-status classification
//
// For a retryable status the router does NOT decide on the status alone — it
// parses the body and lets that decide. The next two tests pin both halves of
// that rule, because the pair is easy to get wrong when touching
// `classify_response`.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn retryable_status_with_valid_json_is_forwarded_not_retried() {
    // 500 is retryable by status, but the body parses as JSON with no `error`
    // member, so `classify_response` falls through to Done and the client gets
    // the 500 — node B is never tried. See src/core/router.rs:275-282.
    let a = spawn_mock(500, NO_ERROR_BODY).await;
    let b = spawn_mock(200, OK_BODY).await;

    let client = build_client(vec![node("A", &a.url).build(), node("B", &b.url).build()]);

    let (status, body) = client.send(Bytes::from(OK_BODY)).await.unwrap();

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(body, Bytes::from(NO_ERROR_BODY));
    assert_eq!(a.hits(), 1);
    assert_eq!(
        b.hits(),
        0,
        "a 5xx whose body is valid JSON without an error member is forwarded, not retried"
    );
}

#[tokio::test]
async fn retryable_status_with_non_json_body_falls_over() {
    // Same retryable status class, but a body that does not parse: this one
    // takes the invalid_json branch (router.rs:247) and does move on.
    let a = spawn_mock(429, "rate limited, slow down").await;
    let b = spawn_mock(200, OK_BODY).await;

    let client = build_client(vec![node("A", &a.url).build(), node("B", &b.url).build()]);

    let (status, body) = client.send(Bytes::from(OK_BODY)).await.unwrap();

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, Bytes::from(OK_BODY));
    assert_eq!(a.hits(), 1);
    assert_eq!(b.hits(), 1);
}

// ---------------------------------------------------------------------------
// Router edge cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn batch_request_returns_400() {
    // The balancer does not support JSON-RPC batches: `MethodExtractor` cannot
    // deserialize an array, so the request is rejected before routing.
    let mock = spawn_mock(200, OK_BODY).await;
    let client = build_client_one(node("A", &mock.url).build());

    let (status, err) = client
        .send(Bytes::from(BATCH_BODY))
        .await
        .expect_err("batch requests are not supported");

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_err_contains(&err, "Parse error");
    assert_eq!(mock.hits(), 0);
}

#[tokio::test]
async fn empty_routing_table_returns_502() {
    let client = build_client(vec![]);

    let (status, err) = client
        .send(Bytes::from(OK_BODY))
        .await
        .expect_err("a balancer with no nodes cannot serve anything");

    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_err_contains(&err, "All nodes failed");
}

#[tokio::test]
async fn timeout_while_waiting_out_a_rate_limit() {
    // rps=1 means the bucket refills after ~1s, which is the whole global
    // budget. The router sleeps out the rate limit and wakes with only
    // microseconds left, so the 100ms upstream cannot finish in time.
    let mock = spawn_mock_latency(200, OK_BODY, Duration::from_millis(100)).await;
    let rpc_node = node("A", &mock.url).rps(1).build();
    let client = build_client_one(rpc_node.clone());

    drop(rpc_node.acquire_and_check().await.unwrap());

    let (status, err) = client
        .send(Bytes::from(OK_BODY))
        .await
        .expect_err("a rate-limit wait that eats the whole budget must time out");

    assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
    assert_err_contains(&err, "Global timeout");
}

#[tokio::test]
async fn max_concurrent_caps_in_flight_requests() {
    // 6 requests against a node capped at 2 concurrent: the semaphore must
    // never let more than 2 reach the upstream at once. 3 waves x 100ms fits
    // inside the 1s budget.
    let mock = spawn_mock_latency(200, OK_BODY, Duration::from_millis(100)).await;
    let client = build_client_one(node("A", &mock.url).max_concurrent(2).build());

    let mut handles = Vec::new();
    for _ in 0..6 {
        let client = client.clone();
        handles.push(tokio::spawn(async move {
            client.send(Bytes::from(OK_BODY)).await
        }));
    }

    for handle in handles {
        let (status, _) = handle.await.unwrap().expect("request should succeed");
        assert_eq!(status, StatusCode::OK);
    }

    assert_eq!(mock.hits(), 6);
    assert!(
        mock.peak_concurrency() <= 2,
        "max_concurrent=2 but {} requests were in flight at once",
        mock.peak_concurrency()
    );
}
