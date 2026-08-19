#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;
use std::time::Duration;

use axum::{
    Router,
    extract::State,
    routing::{get, post},
};
use tokio::signal;

// Static response: no allocation per request, so the mock never competes with
// the balancer for CPU under load.
static OK_BODY: &str = r#"{"jsonrpc":"2.0","result":"ok","id":1}"#;

struct NodeState {
    latency: Duration,
}

async fn handle_rpc(State(state): State<Arc<NodeState>>) -> &'static str {
    tokio::time::sleep(state.latency).await;
    OK_BODY
}

async fn health() -> &'static str {
    r#"{"status":"up"}"#
}

async fn run_node(name: &'static str, port: u16, latency: Duration) {
    let addr = format!("127.0.0.1:{port}");
    let state = Arc::new(NodeState { latency });

    let app = Router::new()
        .route("/", post(handle_rpc))
        .route("/health", get(health))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    tracing::info!(
        "mock node `{name}` ready on {addr} (latency: {}ms)",
        latency.as_millis()
    );

    axum::serve(listener, app)
        .with_graceful_shutdown(wait_for_signal())
        .await
        .unwrap();
}

async fn wait_for_signal() {
    let ctrl_c = async {
        signal::ctrl_c().await.unwrap();
    };

    #[cfg(unix)]
    let terminate = async {
        let mut stream = signal::unix::signal(signal::unix::SignalKind::terminate()).unwrap();
        stream.recv().await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => tracing::info!("SIGINT received, shutting down"),
        () = terminate => tracing::info!("SIGTERM received, shutting down"),
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    // deterministic per-node latency in ms.
    // Set all to 0 for max-throughput benchmarking.
    let nodes: [(&str, u16, u64); 6] = [
        ("Helius-1", 8891, 3),
        ("Helius-2", 8892, 5),
        ("Alchemy-1", 8893, 7),
        ("Alchemy-2", 8894, 9),
        ("Triton-1", 8895, 12),
        ("public-mirror", 8896, 20),
    ];

    tracing::info!("starting {} mock nodes on ports 8891-8896", nodes.len());

    let mut handles = Vec::new();
    for (name, port, latency_ms) in nodes {
        handles.push(tokio::spawn(run_node(
            name,
            port,
            Duration::from_millis(latency_ms),
        )));
    }

    for h in handles {
        h.await.unwrap();
    }
}
