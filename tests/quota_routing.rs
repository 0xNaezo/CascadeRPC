//! Cumulative usage accounting and the quota half of `RpcClient::admit`.
//!
//! Usage is seeded directly through `client.nodes_usage` rather than by burning
//! real requests, which keeps every test instant and exact.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use bytes::Bytes;
use reqwest::StatusCode;
use rpc_load_balancer::core::rpc::RpcClient;

mod common;

use common::{
    OK_BODY, SERVER_ERROR_BODY, assert_err_contains, build_client, build_client_one, node,
    node_handle, spawn_mock,
};

/// Quota slot of a node, resolved by name so the tests below stay readable.
fn node_id(client: &RpcClient, name: &str) -> usize {
    client
        .topology
        .load()
        .all
        .iter()
        .find(|node| node.name == name)
        .expect("node was never given to build_client")
        .id
}

/// Current booked usage for a node.
fn usage(client: &RpcClient, name: &str) -> u64 {
    client.nodes_usage.usage(node_id(client, name)).get()
}

/// Books `amount` against a node, simulating traffic already served this month.
fn seed_usage(client: &RpcClient, name: &str, amount: u64) {
    client.nodes_usage.usage(node_id(client, name)).add(amount);
}

#[tokio::test]
async fn quota_exhausted_node_is_skipped() {
    let mock = spawn_mock(200, OK_BODY).await;
    let client = build_client_one(
        node("A", &mock.url)
            .monthly_limit(1000)
            .spillover_percent(100)
            .build(),
    );

    seed_usage(&client, "A", 1001);

    let (status, err) = client
        .send(Bytes::from(OK_BODY))
        .await
        .expect_err("a node past its spillover threshold must not be used");

    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_err_contains(&err, "no rate limits");
    assert_eq!(mock.hits(), 0, "the request must not reach the upstream");
}

#[tokio::test]
async fn spillover_to_next_tier_when_quota_exhausted() {
    let cheap = spawn_mock(200, OK_BODY).await;
    let fallback = spawn_mock(200, OK_BODY).await;

    let client = build_client(vec![
        node("cheap", &cheap.url)
            .tier(0)
            .monthly_limit(1000)
            .spillover_percent(95)
            .build(),
        node("fallback", &fallback.url).tier(1).build(),
    ]);

    // Threshold is 950; push the tier-0 node past it.
    seed_usage(&client, "cheap", 951);

    let (status, body) = client.send(Bytes::from(OK_BODY)).await.unwrap();

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, Bytes::from(OK_BODY));
    assert_eq!(cheap.hits(), 0);
    assert_eq!(fallback.hits(), 1, "traffic must spill to the next tier");
}

#[tokio::test]
async fn cost_is_booked_per_request() {
    let mock = spawn_mock(200, OK_BODY).await;
    let client = build_client_one(node("A", &mock.url).priced(&[("getBalance", 10)]).build());

    for _ in 0..3 {
        client.send(Bytes::from(OK_BODY)).await.unwrap();
    }

    assert_eq!(usage(&client, "A"), 30, "3 requests at cost 10 each");
    assert_eq!(mock.hits(), 3);
}

#[tokio::test]
async fn threshold_is_strict_greater() {
    // `admit` skips on `used > threshold`, so usage sitting exactly on the
    // threshold still gets one more request through.
    let at_limit_mock = spawn_mock(200, OK_BODY).await;
    let at_limit = build_client_one(
        node("A", &at_limit_mock.url)
            .monthly_limit(1000)
            .spillover_percent(100)
            .priced(&[("getBalance", 1)])
            .build(),
    );
    seed_usage(&at_limit, "A", 1000);

    let (status, _) = at_limit.send(Bytes::from(OK_BODY)).await.unwrap();
    assert_eq!(
        status,
        StatusCode::OK,
        "usage == threshold is still admitted"
    );
    assert_eq!(at_limit_mock.hits(), 1);
    assert_eq!(usage(&at_limit, "A"), 1001);

    let over_mock = spawn_mock(200, OK_BODY).await;
    let over = build_client_one(
        node("A", &over_mock.url)
            .monthly_limit(1000)
            .spillover_percent(100)
            .build(),
    );
    seed_usage(&over, "A", 1001);

    over.send(Bytes::from(OK_BODY))
        .await
        .expect_err("one over the threshold is skipped");
    assert_eq!(over_mock.hits(), 0);
}

#[tokio::test]
async fn all_nodes_over_quota_returns_502() {
    let a = spawn_mock(200, OK_BODY).await;
    let b = spawn_mock(200, OK_BODY).await;

    let client = build_client(vec![
        node("A", &a.url).monthly_limit(100).build(),
        node("B", &b.url).monthly_limit(100).build(),
    ]);

    seed_usage(&client, "A", 101);
    seed_usage(&client, "B", 101);

    let (status, err) = client
        .send(Bytes::from(OK_BODY))
        .await
        .expect_err("nothing left to serve the request");

    // Quota exhaustion is a Skip, not a RateLimited, so there is no wait to
    // schedule and the router fails immediately.
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_err_contains(&err, "no rate limits");
    assert_eq!(a.hits(), 0);
    assert_eq!(b.hits(), 0);
}

#[tokio::test]
async fn unlimited_node_is_never_skipped() {
    // monthly_limit = u64::MAX (what config rewrites `0` into) gives a
    // threshold of u64::MAX, which no usage total can exceed.
    let mock = spawn_mock(200, OK_BODY).await;
    let rpc_node = node("A", &mock.url)
        .monthly_limit(u64::MAX)
        .spillover_percent(100)
        .build();

    assert_eq!(rpc_node.spillover_threshold, u64::MAX);

    let client = build_client_one(rpc_node);
    seed_usage(&client, "A", u64::MAX / 2);

    let (status, _) = client.send(Bytes::from(OK_BODY)).await.unwrap();

    assert_eq!(status, StatusCode::OK);
    assert_eq!(mock.hits(), 1);
}

#[tokio::test]
async fn cost_is_booked_even_when_the_node_fails() {
    // Cost is charged in `admit`, before the HTTP call, and never refunded.
    // A failing node still burns its quota, and the retry bills the next node
    // on top.
    let broken = spawn_mock(200, "not json").await;
    let good = spawn_mock(200, OK_BODY).await;

    let client = build_client(vec![
        node("broken", &broken.url)
            .priced(&[("getBalance", 10)])
            .build(),
        node("good", &good.url)
            .priced(&[("getBalance", 10)])
            .build(),
    ]);

    let (status, _) = client.send(Bytes::from(OK_BODY)).await.unwrap();

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        usage(&client, "broken"),
        10,
        "quota is spent on the failed attempt and not refunded"
    );
    assert_eq!(usage(&client, "good"), 10);
}

#[tokio::test]
async fn unpriced_method_does_not_bill() {
    // The `cost == u32::MAX` skip happens before `used.add`, so a node that
    // cannot serve the method is not charged for it.
    let mock = spawn_mock(200, OK_BODY).await;
    let client = build_client_one(node("A", &mock.url).prices_nothing().build());

    client
        .send(Bytes::from(OK_BODY))
        .await
        .expect_err("unpriced method means the node is skipped");

    assert_eq!(usage(&client, "A"), 0);
    assert_eq!(mock.hits(), 0);
}

#[tokio::test]
async fn retry_loop_bills_the_failing_node_once() {
    // One client request bills a node at most once, however many passes the
    // route loop makes: a node that already had its turn is skipped before
    // `admit`, so only the rate-limited one comes back around.
    //
    // Pass 1: A is charged and fails, B is rate-limited -> sleep ~100ms.
    // Pass 2: A is skipped, B has a token and answers.
    let a = spawn_mock(200, SERVER_ERROR_BODY).await;
    let b = spawn_mock(200, OK_BODY).await;

    let client = build_client(vec![
        node("A", &a.url).priced(&[("getBalance", 10)]).build(),
        node("B", &b.url)
            .rps(10)
            .priced(&[("getBalance", 10)])
            .build(),
    ]);

    // Drain B's 10-token burst so the first pass finds it rate-limited. Through
    // the client's own handle: the bucket the router meets is the only one that
    // counts.
    let node_b = node_handle(&client, "B");
    for _ in 0..10 {
        drop(node_b.acquire_and_check().await.unwrap());
    }

    let (status, body) = client.send(Bytes::from(OK_BODY)).await.unwrap();

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, Bytes::from(OK_BODY));
    assert_eq!(a.hits(), 1, "A is not retried on the second pass");
    assert_eq!(
        usage(&client, "A"),
        10,
        "one client request bills a node once, not once per pass"
    );
    assert_eq!(usage(&client, "B"), 10);
}
