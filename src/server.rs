//! The HTTP surface: one endpoint that proxies RPC requests, one that reports
//! node health, and optionally a Prometheus scrape endpoint.
//!
//! Handlers hold no state of their own — everything comes from the shared
//! [`RpcClient`], which is why they can be cloned across connections freely.

use crate::core::rpc::RpcClient;
use crate::core::topology::NodeHealth;
use axum::{
    Router,
    extract::{Json, State},
    response::IntoResponse,
    routing::{get, post},
};
use bytes::Bytes;
use metrics_exporter_prometheus::PrometheusBuilder;
use reqwest::StatusCode;
use serde::Serialize;
use tokio::signal;

/// Binds the listener and serves until SIGINT or SIGTERM.
///
/// Routes `GET /health`, `POST /send-request`, and `GET /metrics` when metrics
/// are enabled — the recorder is only installed in that case, so the metrics the
/// rest of the crate emits go nowhere otherwise.
///
/// Returns once shutdown is complete, which is what lets the caller flush the
/// quota counters one last time.
///
/// # Errors
///
/// Returns an error if the metrics recorder cannot be installed, the TCP
/// listener cannot bind, or the server fails fatally while running.
pub async fn init_server(
    rpc_client: RpcClient,
    port: u16,
    host: String,
    enable_metrics: bool,
) -> anyhow::Result<()> {
    let mut app = Router::new()
        .route("/health", get(health))
        .route("/send-request", post(send_request))
        .with_state(rpc_client);

    if enable_metrics {
        let builder = PrometheusBuilder::new().with_recommended_naming(true);
        let handle = builder.install_recorder()?;
        app = app.route(
            "/metrics",
            get(move || {
                let handle = handle.clone();
                async move { handle.render() }
            }),
        );
    }

    let listener = tokio::net::TcpListener::bind(format!("{host}:{port}")).await?;

    Ok(axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?)
}

/// Proxies one JSON-RPC request. The body is forwarded verbatim, and so is the
/// answer: both halves of the router's result already carry the status to
/// reply with.
async fn send_request(State(rpc_client): State<RpcClient>, body: Bytes) -> (StatusCode, Bytes) {
    match rpc_client.send(body).await {
        Ok(response) | Err(response) => response,
    }
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    active_nodes: usize,
    total_nodes: usize,
    nodes: Vec<NodeHealth>,
}

/// Reports what the last health check round measured, per node.
///
/// Costs nothing and never dials an upstream: `up`/`down` and the latency are as
/// fresh as the last probe. `degraded` means the balancer is still serving but
/// with fewer nodes than configured; `critical` means it is failing open over
/// the whole set.
///
/// The per-node reading is [`RpcClient::health_snapshot`]; what is left here is
/// the summary the endpoint puts around it.
pub async fn health(State(rpc_client): State<RpcClient>) -> impl IntoResponse {
    let nodes = rpc_client.health_snapshot();

    let active_nodes = nodes.iter().filter(|n| n.status == "up").count();
    let total_nodes = nodes.len();

    let status = if active_nodes == 0 {
        "critical"
    } else if active_nodes < total_nodes {
        "degraded"
    } else {
        "ok"
    };

    Json(HealthResponse {
        status,
        active_nodes,
        total_nodes,
        nodes,
    })
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(err) = signal::ctrl_c().await {
            tracing::error!("Failed to install Ctrl+C handler: {}", err);
            std::future::pending::<()>().await;
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(err) => {
                tracing::error!("Failed to install SIGTERM handler: {}", err);
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {
            tracing::info!("Received Ctrl+C (SIGINT). Shutting down gracefully...");
        },
        () = terminate => {
            tracing::info!("Received SIGTERM. Shutting down gracefully...");
        },
    }
}
