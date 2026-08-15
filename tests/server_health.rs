//! The `/health` endpoint. Its JSON shape is a contract for whatever is
//! scraping it, so the field names are asserted explicitly.
//!
//! The handler reads only the per-node atomics, so these tests flip those
//! directly instead of standing up upstreams and waiting for a health check.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::atomic::Ordering;

use axum::{extract::State, response::IntoResponse};
use rpc_load_balancer::{core::rpc::RpcClient, server::health};
use serde_json::Value;

mod common;

use common::{build_client, node};

/// Three nodes, all up, with distinct latencies. No sockets involved: the
/// handler never dials the URLs.
fn client() -> RpcClient {
    build_client(vec![
        node("alpha", "http://127.0.0.1:1").tier(0).build(),
        node("beta", "http://127.0.0.1:2").tier(1).build(),
        node("gamma", "http://127.0.0.1:3").tier(2).build(),
    ])
}

fn set_state(client: &RpcClient, name: &str, up: bool, latency_ms: u32) {
    let topology = client.topology.load();
    let target = topology
        .all
        .iter()
        .find(|node| node.name == name)
        .unwrap_or_else(|| panic!("no node named {name}"));

    target.healthy.store(up, Ordering::Relaxed);
    target.latency.store(latency_ms, Ordering::Relaxed);
}

async fn health_json(client: &RpcClient) -> Value {
    let response = health(State(client.clone())).await.into_response();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();

    serde_json::from_slice(&body).expect("/health must return JSON")
}

#[tokio::test]
async fn reports_ok_when_every_node_is_up() {
    let client = client();
    let body = health_json(&client).await;

    assert_eq!(body["status"], "ok");
    assert_eq!(body["active_nodes"], 3);
    assert_eq!(body["total_nodes"], 3);
}

#[tokio::test]
async fn reports_degraded_when_some_nodes_are_down() {
    let client = client();
    set_state(&client, "beta", false, 0);

    let body = health_json(&client).await;

    assert_eq!(body["status"], "degraded");
    assert_eq!(body["active_nodes"], 2);
    assert_eq!(body["total_nodes"], 3);
}

#[tokio::test]
async fn reports_critical_when_every_node_is_down() {
    let client = client();
    for name in ["alpha", "beta", "gamma"] {
        set_state(&client, name, false, 0);
    }

    let body = health_json(&client).await;

    assert_eq!(body["status"], "critical");
    assert_eq!(body["active_nodes"], 0);
    assert_eq!(body["total_nodes"], 3);
}

#[tokio::test]
async fn latency_is_reported_for_up_nodes_and_hidden_for_down_ones() {
    // A down node keeps whatever latency it last measured (often the u32::MAX
    // sentinel), which would be misleading to publish.
    let client = client();
    set_state(&client, "alpha", true, 42);
    set_state(&client, "beta", false, u32::MAX);

    let body = health_json(&client).await;
    let nodes = body["nodes"].as_array().unwrap();

    let alpha = &nodes[0];
    assert_eq!(alpha["name"], "alpha");
    assert_eq!(alpha["status"], "up");
    assert_eq!(alpha["latency_ms"], 42);

    let beta = &nodes[1];
    assert_eq!(beta["name"], "beta");
    assert_eq!(beta["status"], "down");
    assert_eq!(beta["latency_ms"], Value::Null);
}

#[tokio::test]
async fn response_shape_is_stable() {
    let client = client();
    let body = health_json(&client).await;

    // serde_json parses objects into a BTreeMap, so these come back sorted —
    // this pins the set of keys, not the order they are serialized in.
    let top: Vec<&String> = body.as_object().unwrap().keys().collect();
    assert_eq!(top, ["active_nodes", "nodes", "status", "total_nodes"]);

    let nodes = body["nodes"].as_array().unwrap();
    assert_eq!(
        nodes.len(),
        3,
        "every configured node is listed, up or down"
    );

    let per_node: Vec<&String> = nodes[0].as_object().unwrap().keys().collect();
    assert_eq!(per_node, ["latency_ms", "name", "status", "tier"]);

    // Nodes are listed in configuration order, not routing-table order.
    let listed: Vec<&str> = nodes
        .iter()
        .map(|node| node["name"].as_str().unwrap())
        .collect();
    assert_eq!(listed, ["alpha", "beta", "gamma"]);
    assert_eq!(nodes[2]["tier"], 2);
}
