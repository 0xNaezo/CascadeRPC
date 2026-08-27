//! Passive health: what the balancer learns about its nodes from the traffic it
//! is already serving.
//!
//! There is no probe loop to exercise here — the balancer never dials an
//! upstream to ask how it is. Every test below drives real requests through the
//! router and then asks what the node set looks like afterwards, which is
//! exactly how the production path collects the same information.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::time::Duration;

use bytes::Bytes;
use cascaderpc::core::{ranking::RankLoop, rpc::RpcClient, topology::Topology};
use reqwest::StatusCode;
use tokio::time::Instant;

mod common;

use common::{
    NOT_JSON_BODY, OK_BODY, RATE_LIMITED_BODY, RATE_LIMITED_JSON_BODY, build_client,
    build_client_one, dead_url, node, node_handle, spawn_flaky_mock, spawn_mock,
    spawn_rate_limited_mock,
};

/// One `getBalance` request, the body every mock in this file is priced for.
const fn request() -> Bytes {
    Bytes::from_static(br#"{"method":"getBalance","id":1,"jsonrpc":"2.0"}"#)
}

/// Sends one request through the router and reports the status the client sees.
async fn send(client: &RpcClient) -> StatusCode {
    match client.send(request()).await {
        Ok((status, _)) | Err((status, _)) => status,
    }
}

fn names(table: &Topology) -> Vec<&str> {
    table.ranked.iter().map(|node| node.name.as_str()).collect()
}

fn is_up(client: &RpcClient, name: &str) -> bool {
    client
        .health_snapshot()
        .into_iter()
        .find(|node| node.name == name)
        .expect("node is in the topology")
        .status
        == "up"
}

// ---------------------------------------------------------------------------
// Measuring
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_answered_request_is_the_measurement() {
    let upstream = spawn_mock(200, OK_BODY).await;
    let client = build_client_one(node("only", &upstream.url).build());

    assert_eq!(
        client.health_snapshot()[0].latency_ms,
        None,
        "nothing has answered yet, so there is nothing to report"
    );

    assert_eq!(send(&client).await, StatusCode::OK);

    // One client request, one upstream request. The balancer spent nothing of
    // its own finding out that this node works.
    assert_eq!(upstream.hits(), 1);
    assert!(
        client.health_snapshot()[0].latency_ms.is_some(),
        "the answer did not become the measurement"
    );
}

#[tokio::test]
async fn ranking_orders_by_tier_then_measured_latency() {
    // This sort *is* the load balancing algorithm: the router walks the ranked
    // list and takes the first node that will accept the request.
    let slow_t0 = spawn_mock(200, OK_BODY).await;
    let fast_t0 = spawn_mock(200, OK_BODY).await;
    let fast_t1 = spawn_mock(200, OK_BODY).await;

    // Deliberately built in the wrong order, so passing means the sort ran.
    let client = build_client(vec![
        node("fast_t1", &fast_t1.url).tier(1).build(),
        node("slow_t0", &slow_t0.url).tier(0).build(),
        node("fast_t0", &fast_t0.url).tier(0).build(),
    ]);

    // Backdating the start of the attempt is how a test states a latency: the
    // node records the round trip from the instant it was handed.
    let now = Instant::now();
    node_handle(&client, "slow_t0").observe_answer(now - Duration::from_millis(200));
    node_handle(&client, "fast_t0").observe_answer(now);
    node_handle(&client, "fast_t1").observe_answer(now);

    RankLoop::rerank(&client).await;

    assert_eq!(
        names(&client.topology.load()),
        ["fast_t0", "slow_t0", "fast_t1"]
    );
}

#[tokio::test]
async fn ranking_never_dials_an_upstream() {
    // The whole point of the feature. A ranking round reads atomics the request
    // path already wrote and publishes an order; if it ever costs a request,
    // that request is billed against a provider quota for asking a question
    // instead of answering one.
    let first = spawn_mock(200, OK_BODY).await;
    let second = spawn_mock(200, OK_BODY).await;

    let client = build_client(vec![
        node("first", &first.url).build(),
        node("second", &second.url).build(),
    ]);

    for _ in 0..5 {
        RankLoop::rerank(&client).await;
    }

    assert_eq!(first.hits(), 0, "the ranking loop probed a node");
    assert_eq!(second.hits(), 0, "the ranking loop probed a node");
}

// ---------------------------------------------------------------------------
// Penalizing
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_failing_node_is_skipped_by_the_very_next_request() {
    // The penalty is applied and read per request, not once per ranking round:
    // a node that broke a moment ago must stop taking traffic now.
    let bad = spawn_mock(500, NOT_JSON_BODY).await;
    let good = spawn_mock(200, OK_BODY).await;

    let client = build_client(vec![
        node("bad", &bad.url).build(),
        node("good", &good.url).build(),
    ]);

    assert_eq!(send(&client).await, StatusCode::OK);
    assert_eq!(bad.hits(), 1, "the first request has to find out somehow");

    for _ in 0..5 {
        assert_eq!(send(&client).await, StatusCode::OK);
    }

    assert_eq!(bad.hits(), 1, "the broken node kept taking traffic");
    assert_eq!(good.hits(), 6);
    assert!(!is_up(&client, "bad"));
}

#[tokio::test]
async fn a_429_penalizes_the_node_that_sent_it() {
    // The one signal this feature exists for. A gateway's plain-text 429 is not
    // an answer the client can use, so the request moves to the next node — and
    // the node that sent it stops taking traffic.
    let limited = spawn_mock(429, RATE_LIMITED_BODY).await;
    let good = spawn_mock(200, OK_BODY).await;

    let client = build_client(vec![
        node("limited", &limited.url).build(),
        node("good", &good.url).build(),
    ]);

    assert_eq!(send(&client).await, StatusCode::OK);
    assert!(!is_up(&client, "limited"));

    assert_eq!(send(&client).await, StatusCode::OK);
    assert_eq!(limited.hits(), 1, "the rate-limited node was asked again");
    assert_eq!(good.hits(), 2);
}

#[tokio::test]
async fn a_429_the_client_can_read_is_forwarded_and_still_penalizes() {
    // Valid JSON with no JSON-RPC error in it: there is nothing for the router
    // to improve on by retrying, so the body goes back as it came. Whether the
    // *request* is settled and whether the *node* is well are two different
    // questions, and this is the case that separates them.
    let limited = spawn_mock(429, RATE_LIMITED_JSON_BODY).await;
    let good = spawn_mock(200, OK_BODY).await;

    let client = build_client(vec![
        node("limited", &limited.url).build(),
        node("good", &good.url).build(),
    ]);

    assert_eq!(send(&client).await, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(good.hits(), 0, "the 429 settled this request");

    assert_eq!(send(&client).await, StatusCode::OK);
    assert_eq!(limited.hits(), 1, "the rate-limited node was asked again");
    assert_eq!(good.hits(), 1);
}

#[tokio::test]
async fn a_retry_after_header_is_accepted_from_the_upstream() {
    let limited = spawn_rate_limited_mock("60").await;
    let good = spawn_mock(200, OK_BODY).await;

    let client = build_client(vec![
        node("limited", &limited.url).build(),
        node("good", &good.url).build(),
    ]);

    send(&client).await;

    assert!(!is_up(&client, "limited"));
    assert_eq!(send(&client).await, StatusCode::OK);
    assert_eq!(limited.hits(), 1);
}

#[tokio::test]
async fn an_unreachable_node_is_penalized_and_reports_no_latency() {
    let good = spawn_mock(200, OK_BODY).await;
    let client = build_client(vec![
        node("gone", &dead_url().await).build(),
        node("good", &good.url).build(),
    ]);

    assert_eq!(send(&client).await, StatusCode::OK);

    assert!(!is_up(&client, "gone"));
    assert_eq!(
        client.health_snapshot()[0].latency_ms,
        None,
        "a penalized node must not report a latency a dashboard would rank on"
    );
}

#[tokio::test]
async fn a_penalized_node_stays_in_the_ranked_table() {
    // The ranking does not filter — the router skips per request. A table that
    // dropped penalized nodes could not fail open over them.
    let bad = spawn_mock(500, NOT_JSON_BODY).await;
    let good = spawn_mock(200, OK_BODY).await;

    let client = build_client(vec![
        node("bad", &bad.url).build(),
        node("good", &good.url).build(),
    ]);

    send(&client).await;
    RankLoop::rerank(&client).await;

    let table = client.topology.load();
    assert_eq!(table.ranked.len(), 2);
    assert_eq!(table.all.len(), 2);
}

// ---------------------------------------------------------------------------
// Recovering, and failing open
// ---------------------------------------------------------------------------

#[tokio::test]
async fn every_node_penalized_still_serves() {
    // Answering 502 while every node is under a penalty the balancer imposed on
    // itself is the failure mode this has to avoid: a shared outage, a DNS
    // blip or a provider-wide 429 penalizes the whole set at once. Both nodes
    // fail their first request and answer normally afterwards.
    let first = spawn_flaky_mock(OK_BODY, 1).await;
    let second = spawn_flaky_mock(OK_BODY, 1).await;

    let client = build_client(vec![
        node("first", &first.url).build(),
        node("second", &second.url).build(),
    ]);

    assert_eq!(
        send(&client).await,
        StatusCode::BAD_GATEWAY,
        "both nodes failed this one for real"
    );
    assert!(!is_up(&client, "first") && !is_up(&client, "second"));

    // Without failing open this is a 502 too, and stays one for the whole
    // penalty — with every node in the config perfectly healthy.
    assert_eq!(send(&client).await, StatusCode::OK);
}

#[tokio::test]
async fn a_node_that_answers_again_returns_to_rotation() {
    // Recovery costs nothing either: the penalty expires on its own, and an
    // answer served under fail-open clears it early.
    let recovering = spawn_flaky_mock(OK_BODY, 1).await;
    let client = build_client_one(node("recovering", &recovering.url).build());

    assert_eq!(send(&client).await, StatusCode::BAD_GATEWAY);
    assert!(!is_up(&client, "recovering"));

    assert_eq!(send(&client).await, StatusCode::OK);
    assert!(
        is_up(&client, "recovering"),
        "an answer must clear the penalty rather than wait it out"
    );
}
