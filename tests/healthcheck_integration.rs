//! The health check loop, which is where node selection actually happens: it
//! decides which nodes are in the routing table and, by sorting them, the order
//! the router walks them in.
//!
//! `tokio::time::interval` yields its first tick immediately, so spawning the
//! loop and waiting for it to publish a table exercises one full iteration.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;
use std::time::Duration;

use cascaderpc::core::{healthcheck::HealthCheckLoop, rpc::RpcClient, topology::Topology};

mod common;

use common::{
    HEALTH_ERROR_BODY, HEALTH_OK_BODY, NOT_JSON_BODY, build_client, build_client_one, dead_url,
    node, spawn_flaky_mock, spawn_health_mock, spawn_mock, spawn_mock_latency,
    spawn_unhealthy_mock,
};

/// Runs the health check until it publishes a routing table, then stops it.
///
/// A node that never answers costs ~2.5s (3 probes on a 1s interval, 500ms
/// each), so the deadline has to clear that comfortably.
async fn run_one_tick(client: &RpcClient) -> Arc<Topology> {
    let before = client.topology.load_full();
    let handle = tokio::spawn(HealthCheckLoop::run_healthcheck_loop(client.clone()));

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let published = loop {
        let current = client.topology.load_full();
        if !Arc::ptr_eq(&before, &current) {
            break current;
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "health check never published a routing table"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    };

    handle.abort();

    published
}

fn names(table: &Topology) -> Vec<&str> {
    table.active.iter().map(|node| node.name.as_str()).collect()
}

#[tokio::test]
async fn sorts_by_tier_then_latency() {
    // This sort *is* the load balancing algorithm: the router simply takes the
    // first node that will accept the request.
    let fast_t1 = spawn_health_mock(Duration::ZERO).await;
    let slow_t0 = spawn_health_mock(Duration::from_millis(200)).await;
    let fast_t0 = spawn_health_mock(Duration::ZERO).await;

    // Deliberately built in the wrong order, so passing means the sort ran.
    let client = build_client(vec![
        node("fast_t1", &fast_t1.url).tier(1).build(),
        node("slow_t0", &slow_t0.url).tier(0).build(),
        node("fast_t0", &fast_t0.url).tier(0).build(),
    ]);

    let table = run_one_tick(&client).await;

    assert_eq!(names(&table), ["fast_t0", "slow_t0", "fast_t1"]);
}

#[tokio::test]
async fn unhealthy_node_is_removed_from_the_table() {
    let good = spawn_health_mock(Duration::ZERO).await;
    let bad = spawn_unhealthy_mock().await;

    let client = build_client(vec![
        node("good", &good.url).build(),
        node("bad", &bad.url).build(),
    ]);

    let table = run_one_tick(&client).await;

    assert_eq!(names(&table), ["good"]);
    assert_eq!(table.all.len(), 2, "the full node set is left untouched");
}

#[tokio::test]
async fn fails_open_when_every_node_is_unhealthy() {
    // With no healthy node the balancer would otherwise answer 502 to
    // everything, so it puts every node back in the table and lets the router
    // find out for itself.
    let first = spawn_unhealthy_mock().await;
    let second = spawn_unhealthy_mock().await;

    let client = build_client(vec![
        node("first", &first.url).build(),
        node("second", &second.url).build(),
    ]);

    let table = run_one_tick(&client).await;

    assert_eq!(
        table.active.len(),
        2,
        "failing open must restore every node"
    );
    for rpc_node in &table.active {
        assert!(
            !rpc_node
                .status
                .healthy
                .load(std::sync::atomic::Ordering::Relaxed),
            "{} is in the table despite being unhealthy — that is the point",
            rpc_node.name
        );
    }
}

#[tokio::test]
async fn unreachable_node_sorts_last_when_failing_open() {
    // A node that answers (badly) reports a real latency; one that never
    // answers gets the u32::MAX sentinel, which parks it at the back of the
    // table so the router tries it last.
    let responding = spawn_unhealthy_mock().await;
    let unreachable = dead_url().await;

    let client = build_client(vec![
        node("unreachable", &unreachable).build(),
        node("responding", &responding.url).build(),
    ]);

    let table = run_one_tick(&client).await;

    assert_eq!(names(&table), ["responding", "unreachable"]);
    assert_eq!(
        table.active[1]
            .status
            .latency
            .load(std::sync::atomic::Ordering::Relaxed),
        u32::MAX
    );
}

// ---------------------------------------------------------------------------
// The probe itself: how many attempts it spends, and what it accepts as
// healthy. `core::health` is a private module, so these go through
// `run_once`, which is what calls it in production anyway.
// ---------------------------------------------------------------------------

/// Runs one probe round and reports how many nodes answered.
async fn probe_round(client: &RpcClient) -> usize {
    HealthCheckLoop::run_once(client).await
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

#[tokio::test]
async fn a_node_that_answers_on_the_third_probe_is_healthy() {
    // Two dropped packets are what the retries exist for; without them a node
    // leaves the routing table for a whole interval over a transient blip.
    let flaky = spawn_flaky_mock(HEALTH_OK_BODY, 2).await;
    let client = build_client_one(node("flaky", &flaky.url).build());

    assert_eq!(probe_round(&client).await, 1);
    assert_eq!(
        flaky.hits(),
        3,
        "the probe gave up before its third attempt"
    );
    assert!(is_up(&client, "flaky"));
}

#[tokio::test]
async fn a_node_that_never_answers_costs_three_attempts() {
    let broken = spawn_flaky_mock(HEALTH_OK_BODY, usize::MAX).await;
    let client = build_client_one(node("broken", &broken.url).build());

    assert_eq!(probe_round(&client).await, 0);

    // Three and no more: a node that never answers must not stall the round
    // it shares with every other node.
    assert_eq!(broken.hits(), 3);
    assert!(!is_up(&client, "broken"));
}

#[tokio::test]
async fn a_probe_slower_than_the_timeout_reads_as_unhealthy() {
    // 600ms against the probe's 500ms budget: the node answers, just not in
    // time, which is the case a plain TCP connect check would miss.
    let sluggish = spawn_mock_latency(200, HEALTH_OK_BODY, Duration::from_millis(600)).await;
    let client = build_client_one(node("sluggish", &sluggish.url).build());

    assert_eq!(probe_round(&client).await, 0);
    assert_eq!(sluggish.hits(), 3);
}

#[tokio::test]
async fn a_body_that_does_not_parse_reads_as_unhealthy() {
    let gateway_page = spawn_mock(200, NOT_JSON_BODY).await;
    let client = build_client_one(node("gateway", &gateway_page.url).build());

    assert_eq!(probe_round(&client).await, 0);
    assert_eq!(gateway_page.hits(), 3, "an unparseable body is retried");
}

#[tokio::test]
async fn an_error_member_reads_as_unhealthy() {
    // Says `result: "ok"` and carries an error beside it. Reading only the
    // result would call this node healthy.
    let contradictory = spawn_mock(200, HEALTH_ERROR_BODY).await;
    let client = build_client_one(node("contradictory", &contradictory.url).build());

    assert_eq!(probe_round(&client).await, 0);
    assert!(!is_up(&client, "contradictory"));
}

#[tokio::test]
async fn a_healthy_node_reports_a_measured_latency() {
    let good = spawn_health_mock(Duration::ZERO).await;
    let client = build_client_one(node("good", &good.url).build());

    probe_round(&client).await;

    let measured = client.health_snapshot()[0].latency_ms;

    // Not the u32::MAX sentinel: an unmeasured node sorts to the back of the
    // routing table, which a healthy node must never do.
    assert!(
        measured.is_some_and(|ms| ms < u32::MAX),
        "no latency measured: {measured:?}"
    );
}

#[tokio::test]
async fn a_recovered_node_returns_to_the_table() {
    let good = spawn_health_mock(Duration::ZERO).await;
    // Fails the first round's three attempts, answers on the next round's.
    let recovering = spawn_flaky_mock(HEALTH_OK_BODY, 3).await;

    let client = build_client(vec![
        node("good", &good.url).build(),
        node("recovering", &recovering.url).build(),
    ]);

    assert_eq!(probe_round(&client).await, 1);
    assert_eq!(names(&client.topology.load()), ["good"]);

    assert_eq!(probe_round(&client).await, 2);
    assert!(is_up(&client, "recovering"));
    assert_eq!(names(&client.topology.load()).len(), 2);
}

#[tokio::test]
async fn an_unreachable_node_never_reports_a_latency() {
    let client = build_client_one(node("gone", &dead_url().await).build());

    assert_eq!(probe_round(&client).await, 0);
    assert_eq!(
        client.health_snapshot()[0].latency_ms,
        None,
        "a down node reports no latency at all"
    );
}

#[tokio::test]
async fn a_node_that_goes_bad_leaves_the_table() {
    let good = spawn_health_mock(Duration::ZERO).await;
    let bad = spawn_unhealthy_mock().await;

    let client = build_client(vec![
        node("good", &good.url).build(),
        node("bad", &bad.url).build(),
    ]);

    probe_round(&client).await;

    assert_eq!(names(&client.topology.load()), ["good"]);
    assert_eq!(
        client.topology.load().all.len(),
        2,
        "the full node set survives; only the routing table shrinks"
    );
}
