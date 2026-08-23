//! The HTTP surface as a client actually meets it: a real listener, real
//! sockets, real status codes.
//!
//! Every other test in this suite calls `RpcClient::send` or the `health`
//! handler directly, which leaves the part `init_server` owns — the route
//! table — asserted nowhere. A typo in a path or a method would pass all of
//! them and 404 in production.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use cascaderpc::core::rpc::RpcClient;

mod common;

use common::{
    NOT_JSON_BODY, OK_BODY, build_client, build_client_one, dead_url, node, spawn_mock,
    spawn_server,
};

/// The server under test, with no metrics recorder installed.
async fn serve(client: RpcClient) -> String {
    spawn_server(client, None).await
}

async fn post(base: &str, path: &str, body: &'static str) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{base}{path}"))
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .expect("request reached the balancer")
}

#[tokio::test]
async fn send_request_is_proxied_and_the_upstream_body_is_forwarded() {
    let upstream = spawn_mock(200, OK_BODY).await;
    let base = serve(build_client_one(node("only", &upstream.url).build())).await;

    let response = post(&base, "/send-request", OK_BODY).await;

    assert_eq!(response.status(), 200);
    assert_eq!(response.text().await.unwrap(), OK_BODY);
    assert_eq!(upstream.hits(), 1);
}

#[tokio::test]
async fn an_upstream_status_is_forwarded_verbatim() {
    // 404 is a final status: the balancer hands it back rather than trying the
    // next node, and the client must see the upstream's own code.
    let upstream = spawn_mock(404, OK_BODY).await;
    let base = serve(build_client_one(node("only", &upstream.url).build())).await;

    assert_eq!(post(&base, "/send-request", OK_BODY).await.status(), 404);
}

#[tokio::test]
async fn a_body_that_is_not_json_rpc_gets_400() {
    let upstream = spawn_mock(200, OK_BODY).await;
    let base = serve(build_client_one(node("only", &upstream.url).build())).await;

    let response = post(&base, "/send-request", NOT_JSON_BODY).await;

    assert_eq!(response.status(), 400);

    // A client that speaks JSON-RPC gets a body it can parse, even for the
    // balancer's own errors.
    let body: serde_json::Value = response.json().await.expect("400 body must be JSON-RPC");
    assert_eq!(body["error"]["code"], -32700);
    assert_eq!(upstream.hits(), 0, "a malformed body never reaches a node");
}

#[tokio::test]
async fn all_nodes_down_gets_502() {
    let base = serve(build_client_one(node("gone", &dead_url().await).build())).await;

    let response = post(&base, "/send-request", OK_BODY).await;

    assert_eq!(response.status(), 502);

    let body: serde_json::Value = response.json().await.expect("502 body must be JSON-RPC");
    assert_eq!(body["error"]["code"], -32000);
}

#[tokio::test]
async fn health_is_served_over_http() {
    let base = serve(build_client(vec![
        node("alpha", "http://127.0.0.1:1").build(),
        node("beta", "http://127.0.0.1:2").build(),
    ]))
    .await;

    let body: serde_json::Value = reqwest::get(format!("{base}/health"))
        .await
        .unwrap()
        .json()
        .await
        .expect("/health must return JSON");

    // Nodes start assumed-healthy, and nothing has probed them here.
    assert_eq!(body["status"], "ok");
    assert_eq!(body["total_nodes"], 2);
    assert_eq!(body["nodes"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn the_metrics_route_is_absent_without_a_handle() {
    let base = serve(build_client_one(node("only", "http://127.0.0.1:1").build())).await;

    // The scrape endpoint exposes node names and spend; without a recorder it
    // must not exist at all rather than render an empty page.
    assert_eq!(
        reqwest::get(format!("{base}/metrics"))
            .await
            .unwrap()
            .status(),
        404
    );
}

#[tokio::test]
async fn an_unknown_route_is_404() {
    let base = serve(build_client_one(node("only", "http://127.0.0.1:1").build())).await;

    // No catch-all: an unknown path must not be proxied to an upstream.
    assert_eq!(
        reqwest::get(format!("{base}/anything-else"))
            .await
            .unwrap()
            .status(),
        404
    );
}

#[tokio::test]
async fn a_get_on_send_request_is_405() {
    let base = serve(build_client_one(node("only", "http://127.0.0.1:1").build())).await;

    assert_eq!(
        reqwest::get(format!("{base}/send-request"))
            .await
            .unwrap()
            .status(),
        405
    );
}

#[tokio::test]
async fn a_post_on_health_is_405() {
    let base = serve(build_client_one(node("only", "http://127.0.0.1:1").build())).await;

    assert_eq!(post(&base, "/health", OK_BODY).await.status(), 405);
}

#[tokio::test]
async fn concurrent_clients_are_all_answered() {
    let upstream = spawn_mock(200, OK_BODY).await;
    let base = serve(build_client_one(
        node("only", &upstream.url).max_concurrent(4).build(),
    ))
    .await;

    let mut handles = Vec::new();
    for _ in 0..8 {
        let base = base.clone();
        handles.push(tokio::spawn(async move {
            post(&base, "/send-request", OK_BODY).await.status()
        }));
    }

    for handle in handles {
        assert_eq!(handle.await.unwrap(), 200);
    }
    assert_eq!(upstream.hits(), 8);
}
