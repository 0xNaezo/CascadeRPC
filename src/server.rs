use std::sync::atomic::Ordering;

use crate::client::rpc::RpcClient;
use axum::{
    Router,
    extract::{Json, State},
    response::IntoResponse,
    routing::{get, post},
};
use bytes::Bytes;
use reqwest::StatusCode;
use serde::Serialize;
use serde_json::json;
use tokio::signal;

#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

/// Starts the HTTP server on `0.0.0.0:3000` and serves RPC endpoints.
///
/// # Errors
///
/// Returns an error if the TCP listener fails to bind or the server encounters a fatal error.
pub async fn init_server(rpc_client: RpcClient, port: u16, host: String) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/health", get(health))
        .route("/test-speed", get(test_speed))
        .route("/send-request", post(send_request))
        .with_state(rpc_client);

    let listener = tokio::net::TcpListener::bind(format!("{host}:{port}")).await?;

    Ok(axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?)
}

async fn send_request(
    State(rpc_client): State<RpcClient>,
    body: Bytes,
) -> Result<Bytes, (StatusCode, Json<ErrorResponse>)> {
    let result = rpc_client.send_with_fallback(body).await;

    match result {
        Ok(response) => Ok(response),
        Err(err) => {
            let err_struct = ErrorResponse {
                error: format!("error: {err}"),
            };

            Err((StatusCode::BAD_GATEWAY, Json(err_struct)))
        }
    }
}

pub async fn test_speed(State(rpc_client): State<RpcClient>) -> Json<serde_json::Value> {
    rpc_client.request_counter.fetch_add(1, Ordering::Relaxed);

    Json(json!({ "request_count": rpc_client.request_counter.load(Ordering::Relaxed) }))
}

#[derive(Serialize)]
struct NodeHealth {
    name: String,
    tier: u8,
    status: &'static str,
    latency_ms: Option<u32>,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    active_nodes: usize,
    total_nodes: usize,
    nodes: Vec<NodeHealth>,
}

pub async fn health(State(rpc_client): State<RpcClient>) -> impl IntoResponse {
    let nodes: Vec<NodeHealth> = rpc_client
        .all_nodes
        .iter()
        .map(|node| {
            let is_up = node.healthy.load(Ordering::Relaxed);
            let latency = node.latency.load(Ordering::Relaxed);

            NodeHealth {
                name: node.name.clone(),
                tier: node.tier,
                status: if is_up { "up" } else { "down" },
                latency_ms: is_up.then_some(latency),
            }
        })
        .collect();

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
