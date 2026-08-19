//! Shared harness for the integration test binaries.
//!
//! Each test binary pulls in this module with `mod common;` and uses only part
//! of it, hence the crate-wide `dead_code` allow. The clippy allows mirror
//! `tests/fallback_integration.rs`: `Cargo.toml` denies `unwrap_used`,
//! `expect_used` and `panic` for the whole package, test targets included.
#![allow(
    dead_code,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::must_use_candidate,
    clippy::missing_panics_doc,
    // The builder setters could be `const fn`, but that buys nothing in test
    // setup code and makes the chain harder to extend.
    clippy::missing_const_for_fn
)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::{Router, extract::State, response::IntoResponse, routing::post};
use reqwest::StatusCode;
use rpc_load_balancer::{
    core::{
        node::{NewNode, RpcNode},
        rpc::RpcClient,
    },
    protocol::registry::CUSTOM_METHODS,
    provider::cost_table::{CostSpec, ProviderCostTable},
};

// ---------------------------------------------------------------------------
// Request/response bodies
// ---------------------------------------------------------------------------

/// A well-formed `getBalance` request. Doubles as the canned success response,
/// which is why every node in these tests must price `getBalance`.
pub const OK_BODY: &str = r#"{"jsonrpc":"2.0","method":"getBalance","result":"ok","id":1}"#;
/// JSON-RPC -32602: terminal, forwarded to the client as-is.
pub const INVALID_PARAMS_BODY: &str =
    r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32602,"message":"invalid params"}}"#;
/// JSON-RPC -32000: retryable, sends the router to the next node.
pub const SERVER_ERROR_BODY: &str =
    r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"server error"}}"#;
/// Valid JSON with no `error` member — what `classify_response` treats as final.
pub const NO_ERROR_BODY: &str = r#"{"jsonrpc":"2.0","id":1,"result":"ok"}"#;
/// What `get_health` accepts as a healthy node.
pub const HEALTH_OK_BODY: &str = r#"{"jsonrpc":"2.0","id":1,"result":"ok"}"#;
/// Parses fine but `result != "ok"`, so the node is marked unhealthy while
/// still reporting a real latency (unlike a node that never answers).
pub const HEALTH_BAD_BODY: &str = r#"{"jsonrpc":"2.0","id":1,"result":"behind"}"#;
/// A JSON-RPC batch. The balancer does not support batching.
pub const BATCH_BODY: &str = r#"[{"jsonrpc":"2.0","method":"getBalance","id":1}]"#;

// ---------------------------------------------------------------------------
// Mock upstream node
// ---------------------------------------------------------------------------

struct MockState {
    status: u16,
    body: &'static str,
    latency: Duration,
    requests: Arc<AtomicUsize>,
    in_flight: Arc<AtomicUsize>,
    max_in_flight: Arc<AtomicUsize>,
}

/// Handle to a running mock upstream.
pub struct Mock {
    pub url: String,
    requests: Arc<AtomicUsize>,
    max_in_flight: Arc<AtomicUsize>,
}

impl Mock {
    /// Total requests this mock has received.
    pub fn hits(&self) -> usize {
        self.requests.load(Ordering::Relaxed)
    }

    /// Highest number of requests this mock ever had in flight at once.
    pub fn peak_concurrency(&self) -> usize {
        self.max_in_flight.load(Ordering::Relaxed)
    }
}

async fn handle_rpc(State(state): State<Arc<MockState>>) -> impl IntoResponse {
    state.requests.fetch_add(1, Ordering::Relaxed);

    let in_flight = state.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
    state.max_in_flight.fetch_max(in_flight, Ordering::SeqCst);

    tokio::time::sleep(state.latency).await;

    state.in_flight.fetch_sub(1, Ordering::SeqCst);

    (StatusCode::from_u16(state.status).unwrap(), state.body)
}

/// Spawns a mock RPC node on an ephemeral port that answers instantly.
pub async fn spawn_mock(status: u16, body: &'static str) -> Mock {
    spawn_mock_latency(status, body, Duration::ZERO).await
}

/// Spawns a mock RPC node that sleeps `latency` before answering.
pub async fn spawn_mock_latency(status: u16, body: &'static str, latency: Duration) -> Mock {
    let requests = Arc::new(AtomicUsize::new(0));
    let max_in_flight = Arc::new(AtomicUsize::new(0));
    let state = Arc::new(MockState {
        status,
        body,
        latency,
        requests: requests.clone(),
        in_flight: Arc::new(AtomicUsize::new(0)),
        max_in_flight: max_in_flight.clone(),
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

    Mock {
        url: format!("http://{addr}"),
        requests,
        max_in_flight,
    }
}

/// Mock that answers `getHealth` with `result: "ok"` after `latency`.
///
/// `get_health` times each probe out at 500ms, so `latency` must stay well
/// under that or the node reads as unhealthy.
pub async fn spawn_health_mock(latency: Duration) -> Mock {
    spawn_mock_latency(200, HEALTH_OK_BODY, latency).await
}

/// Mock that answers, but with a `result` the health check rejects. The node
/// ends up unhealthy *with a real latency*, unlike [`dead_url`].
pub async fn spawn_unhealthy_mock() -> Mock {
    spawn_mock(200, HEALTH_BAD_BODY).await
}

/// A URL nothing is listening on: binds a port to reserve it, then drops it.
pub async fn dead_url() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    format!("http://{addr}")
}

// ---------------------------------------------------------------------------
// Cost tables
// ---------------------------------------------------------------------------

/// Prices `getBalance` (the method in [`OK_BODY`]) at 1, so nodes are not
/// skipped by the `u32::MAX` "node can't do this method" guard.
pub fn priced_table() -> ProviderCostTable {
    table_with(&[("getBalance", 1)])
}

/// Cost table with exactly the given methods priced; everything else stays
/// `u32::MAX`.
///
/// Built over the same process-wide registry the router resolves against, so a
/// custom name priced here gets the id a request for it will carry.
pub fn table_with(methods: &[(&str, u32)]) -> ProviderCostTable {
    table_from(&CostSpec {
        routing: methods
            .iter()
            .map(|(name, cost)| ((*name).to_string(), *cost))
            .collect::<HashMap<String, u32>>(),
        ..CostSpec::default()
    })
}

/// Cost table over a spec the test wrote itself — custom methods, a fallback
/// price, or both.
pub fn table_from(spec: &CostSpec) -> ProviderCostTable {
    ProviderCostTable::new(spec, &CUSTOM_METHODS)
}

// ---------------------------------------------------------------------------
// Node / client builders
// ---------------------------------------------------------------------------

/// Fluent [`RpcNode`] builder. Defaults are "unconstrained node that prices
/// `getBalance`", so each test only states the limit it actually exercises.
pub struct NodeBuilder {
    spec: NewNode,
}

/// Starts a node named `name` pointing at `url`, with ample rps (100),
/// concurrency (10) and quota (`u64::MAX`) at tier 0.
pub fn node(name: &str, url: &str) -> NodeBuilder {
    NodeBuilder {
        spec: NewNode {
            name: name.to_string(),
            url: url.to_string(),
            rps_limit: 100,
            max_concurrent: 10,
            tier: 0,
            method_costs: priced_table(),
            monthly_limit: u64::MAX,
            billing_type: "requests".to_string(),
            spillover_percent: 100,
            reset_day: 1,
        },
    }
}

impl NodeBuilder {
    pub fn tier(mut self, tier: u8) -> Self {
        self.spec.tier = tier;
        self
    }

    pub fn rps(mut self, rps_limit: u32) -> Self {
        self.spec.rps_limit = rps_limit;
        self
    }

    pub fn max_concurrent(mut self, max_concurrent: usize) -> Self {
        self.spec.max_concurrent = max_concurrent;
        self
    }

    pub fn monthly_limit(mut self, monthly_limit: u64) -> Self {
        self.spec.monthly_limit = monthly_limit;
        self
    }

    pub fn spillover_percent(mut self, spillover_percent: u8) -> Self {
        self.spec.spillover_percent = spillover_percent;
        self
    }

    pub fn costs(mut self, method_costs: ProviderCostTable) -> Self {
        self.spec.method_costs = method_costs;
        self
    }

    /// Cost table pricing exactly `methods`; shorthand for `.costs(table_with(..))`.
    pub fn priced(self, methods: &[(&str, u32)]) -> Self {
        let table = table_with(methods);
        self.costs(table)
    }

    /// Default cost table: nothing priced, so the node is skipped for every method.
    pub fn prices_nothing(self) -> Self {
        self.costs(ProviderCostTable::default())
    }

    pub fn build(self) -> RpcNode {
        RpcNode::new(self.spec).unwrap()
    }
}

/// Builds a client over the given nodes, in routing-table order.
pub fn build_client(nodes: Vec<RpcNode>) -> RpcClient {
    RpcClient::new(nodes).unwrap()
}

/// Builds a client over a single node.
pub fn build_client_one(node: RpcNode) -> RpcClient {
    build_client(vec![node])
}

// ---------------------------------------------------------------------------
// Assertions
// ---------------------------------------------------------------------------

/// Asserts a balancer-generated error body mentions `needle`, printing the
/// whole body when it does not.
pub fn assert_err_contains(body: &bytes::Bytes, needle: &str) {
    let text = String::from_utf8_lossy(body);
    assert!(text.contains(needle), "unexpected error body: {text}");
}
