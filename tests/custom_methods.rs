//! Routing and billing for methods outside the `RpcMethod` enum.
//!
//! The unit tests cover the registry and the cost table in isolation; these
//! check the part neither can: that the id a provider's table was built with is
//! the id the router resolves an incoming request to, across nodes and through
//! the quota accounting.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use bytes::Bytes;
use cascaderpc::{core::rpc::RpcClient, protocol::cost_table::CostSpec};
use reqwest::StatusCode;

mod common;

use common::{OK_BODY, build_client, build_client_one, node, spawn_mock, table_from};

/// A method no `RpcMethod` variant covers — the case the registry exists for.
const CUSTOM_BODY: &str = r#"{"jsonrpc":"2.0","method":"provider_doSomething","id":1}"#;
/// A method nothing in any config names.
const UNDECLARED_BODY: &str = r#"{"jsonrpc":"2.0","method":"nobody_declared_this","id":1}"#;

fn usage(client: &RpcClient, name: &str) -> u64 {
    let id = client
        .topology
        .load()
        .all
        .iter()
        .find(|node| node.name == name)
        .expect("node was never given to build_client")
        .id;

    client.nodes_usage.usage(id).get()
}

fn spec(routing: &[(&str, u32)], custom: &[(&str, u32)], unknown_cost: u32) -> CostSpec {
    let owned = |entries: &[(&str, u32)]| {
        entries
            .iter()
            .map(|(name, cost)| ((*name).to_string(), *cost))
            .collect()
    };

    CostSpec {
        routing: owned(routing),
        custom: owned(custom),
        unknown_cost,
    }
}

#[tokio::test]
async fn a_custom_method_is_served_and_billed_at_its_own_price() {
    let mock = spawn_mock(200, OK_BODY).await;
    let client = build_client_one(
        node("A", &mock.url)
            .costs(table_from(&spec(
                &[("getBalance", 1)],
                &[("provider_doSomething", 77)],
                u32::MAX,
            )))
            .build(),
    );

    let (status, _) = client.send(Bytes::from(CUSTOM_BODY)).await.unwrap();

    assert_eq!(status, StatusCode::OK);
    assert_eq!(mock.hits(), 1);
    assert_eq!(usage(&client, "A"), 77, "billed at the custom price, not 1");
}

#[tokio::test]
async fn an_undeclared_method_is_skipped_when_no_fallback_is_configured() {
    // Unchanged behaviour from before custom methods existed: a node that
    // cannot price a method is not sent the request.
    let mock = spawn_mock(200, OK_BODY).await;
    let client = build_client_one(
        node("A", &mock.url)
            .costs(table_from(&spec(&[("getBalance", 1)], &[], u32::MAX)))
            .build(),
    );

    client
        .send(Bytes::from(UNDECLARED_BODY))
        .await
        .expect_err("nothing prices this method");

    assert_eq!(mock.hits(), 0);
    assert_eq!(usage(&client, "A"), 0);
}

#[tokio::test]
async fn the_fallback_price_makes_an_undeclared_method_routable() {
    // `unknown_method_cost` is the "charge with a margin" knob: the node serves
    // what the operator never enumerated, billed high enough that the balancer
    // errs on the side of thinking the quota ran out early.
    let mock = spawn_mock(200, OK_BODY).await;
    let client = build_client_one(
        node("A", &mock.url)
            .costs(table_from(&spec(&[("getBalance", 1)], &[], 400)))
            .build(),
    );

    let (status, _) = client.send(Bytes::from(UNDECLARED_BODY)).await.unwrap();

    assert_eq!(status, StatusCode::OK);
    assert_eq!(mock.hits(), 1);
    assert_eq!(usage(&client, "A"), 400);
}

#[tokio::test]
async fn a_named_method_is_not_charged_the_fallback() {
    let mock = spawn_mock(200, OK_BODY).await;
    let client = build_client_one(
        node("A", &mock.url)
            .costs(table_from(&spec(&[("getBalance", 1)], &[], 400)))
            .build(),
    );

    client.send(Bytes::from(OK_BODY)).await.unwrap();

    assert_eq!(usage(&client, "A"), 1);
}

#[tokio::test]
async fn a_custom_method_spills_to_the_node_that_prices_it() {
    // The id has to mean the same method in both tables, or the request would
    // either miss the node that can serve it or bill the wrong slot on the one
    // that cannot.
    let cheap = spawn_mock(200, OK_BODY).await;
    let capable = spawn_mock(200, OK_BODY).await;

    let client = build_client(vec![
        node("cheap", &cheap.url)
            .tier(0)
            .costs(table_from(&spec(&[("getBalance", 1)], &[], u32::MAX)))
            .build(),
        node("capable", &capable.url)
            .tier(1)
            .costs(table_from(&spec(
                &[("getBalance", 5)],
                &[("provider_doSomething", 20)],
                u32::MAX,
            )))
            .build(),
    ]);

    let (status, _) = client.send(Bytes::from(CUSTOM_BODY)).await.unwrap();

    assert_eq!(status, StatusCode::OK);
    assert_eq!(cheap.hits(), 0, "tier 0 cannot price it");
    assert_eq!(capable.hits(), 1);
    assert_eq!(usage(&client, "cheap"), 0);
    assert_eq!(usage(&client, "capable"), 20);
}

#[tokio::test]
async fn two_custom_methods_do_not_share_a_price() {
    // Ids come out of one shared registry, so a table filled in HashMap order
    // has to land each name in its own slot.
    let mock = spawn_mock(200, OK_BODY).await;
    let client = build_client_one(
        node("A", &mock.url)
            .costs(table_from(&spec(
                &[],
                &[("provider_doSomething", 77), ("provider_doOther", 3)],
                u32::MAX,
            )))
            .build(),
    );

    client.send(Bytes::from(CUSTOM_BODY)).await.unwrap();
    assert_eq!(usage(&client, "A"), 77);

    client
        .send(Bytes::from(
            r#"{"jsonrpc":"2.0","method":"provider_doOther","id":1}"#,
        ))
        .await
        .unwrap();
    assert_eq!(
        usage(&client, "A"),
        80,
        "3 on top of the first request's 77"
    );
}
