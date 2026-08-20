//! The HTTP surface: one endpoint that proxies RPC requests, one that reports
//! node health, and optionally a Prometheus scrape endpoint.
//!
//! Handlers hold no state of their own — everything comes from the shared
//! [`RpcClient`].
//!
//! The state is an `Arc<RpcClient>` and not an `RpcClient`: axum clones the
//! state into every request, and the client's own `Clone` is four `Arc` bumps
//! and a `reqwest::Client` clone. Behind one `Arc` that is one bump per
//! request, in and out.

use crate::core::rpc::RpcClient;
use crate::core::topology::NodeHealth;
use axum::{
    Router,
    extract::{Json, State},
    response::IntoResponse,
    routing::{get, post},
};
use bytes::Bytes;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use reqwest::StatusCode;
use serde::Serialize;
use std::sync::Arc;
use tokio::signal;

/// Installs the process-wide Prometheus recorder and hands back the handle the
/// scrape endpoint renders from.
///
/// Separate from [`init_server`], and called before it: the recorder is global,
/// and a metric emitted before one exists is dropped rather than buffered. Doing
/// this last would silently lose everything measured during startup — the quota
/// gauges published off the restored counters, among them.
///
/// # Errors
///
/// Returns an error if a recorder is already installed in this process.
pub fn install_metrics_recorder() -> anyhow::Result<PrometheusHandle> {
    let handle = PrometheusBuilder::new()
        .with_recommended_naming(true)
        .install_recorder()?;

    // Here and not at the call site: a description registered against a
    // recorder that is not the one rendering is a metric that scrapes without
    // a HELP line, and nothing fails loudly when that happens.
    crate::metrics::describe_all();

    Ok(handle)
}

/// Binds the listener and serves until SIGINT or SIGTERM.
///
/// Routes `GET /health`, `POST /send-request`, and `GET /metrics` when a handle
/// from [`install_metrics_recorder`] is passed — without one nothing scrapes the
/// metrics the rest of the crate emits, and they go nowhere.
///
/// Returns once shutdown is complete, which is what lets the caller flush the
/// quota counters one last time.
///
/// # Errors
///
/// Returns an error if the TCP listener cannot bind, or the server fails
/// fatally while running.
pub async fn init_server(
    rpc_client: Arc<RpcClient>,
    port: u16,
    host: String,
    metrics: Option<PrometheusHandle>,
) -> anyhow::Result<()> {
    let mut app = Router::new()
        .route("/health", get(health))
        .route("/send-request", post(send_request))
        .with_state(rpc_client);

    if let Some(handle) = metrics {
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
async fn send_request(
    State(rpc_client): State<Arc<RpcClient>>,
    body: Bytes,
) -> (StatusCode, Bytes) {
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
pub async fn health(State(rpc_client): State<Arc<RpcClient>>) -> impl IntoResponse {
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
