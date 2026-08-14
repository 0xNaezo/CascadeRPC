//! The health check loop, which is where node selection actually happens: it
//! decides which nodes are in the routing table and, by sorting them, the order
//! the router walks them in.
//!
//! `tokio::time::interval` yields its first tick immediately, so spawning the
//! loop and waiting for it to publish a table exercises one full iteration.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;
use std::time::Duration;

use rpc_load_balancer::core::{healthcheck::HealthCheckLoop, node::RoutingTable, rpc::RpcClient};

mod common;

use common::{build_client, dead_url, node, spawn_health_mock, spawn_unhealthy_mock};

/// Runs the health check until it publishes a routing table, then stops it.
///
/// A node that never answers costs ~2s (3 probes on a 1s interval), so the
/// deadline has to clear that comfortably.
async fn run_one_tick(client: &RpcClient) -> Arc<RoutingTable> {
    let before = client.routing_table.load_full();
    let handle = tokio::spawn(HealthCheckLoop::run_healthcheck_loop(client.clone()));

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let published = loop {
        let current = client.routing_table.load_full();
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

fn names(table: &RoutingTable) -> Vec<&str> {
    table
        .active_nodes
        .iter()
        .map(|node| node.name.as_str())
        .collect()
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
    assert_eq!(client.all_nodes.len(), 2, "all_nodes is left untouched");
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
        table.active_nodes.len(),
        2,
        "failing open must restore every node"
    );
    for rpc_node in &table.active_nodes {
        assert!(
            !rpc_node.healthy.load(std::sync::atomic::Ordering::Relaxed),
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
        table.active_nodes[1]
            .latency
            .load(std::sync::atomic::Ordering::Relaxed),
        u32::MAX
    );
}
