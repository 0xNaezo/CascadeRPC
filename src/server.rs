use std::sync::atomic::Ordering;

use crate::client::rpc::RpcClient;
use axum::{
    Router,
    extract::{Json, State},
    response::IntoResponse,
    routing::get,
};
use serde::Serialize;
use serde_json::json;

#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

/// Starts the HTTP server on `0.0.0.0:3000` and serves RPC endpoints.
///
/// # Errors
///
/// Returns an error if the TCP listener fails to bind or the server encounters a fatal error.
pub async fn init_server(rpc_client: RpcClient) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/health", get(health))
        .route("/test-speed", get(test_speed))
        //.route("/get-balance/{address}", get(get_balance))
        .with_state(rpc_client);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;

    Ok(axum::serve(listener, app).await?)
}
/*
async fn get_balance(
    State(rpc_client): State<RpcClient>,
    Path(address): Path<String>,
) -> Result<Json<RpcBalanceResponse>, (StatusCode, Json<ErrorResponse>)> {
    let result = rpc_client.post_three_requests(address).await;

    match result {
        Ok(response) => Ok(Json(response)),
        Err(err) => {
            let err_struct = ErrorResponse {
                error: format!("error: {err}"),
            };

            Err((StatusCode::BAD_GATEWAY, Json(err_struct)))
        }
    }
}
*/
pub async fn test_speed(State(rpc_client): State<RpcClient>) -> Json<serde_json::Value> {
    rpc_client.request_counter.fetch_add(1, Ordering::Relaxed);

    Json(json!({ "request_count": rpc_client.request_counter.load(Ordering::Relaxed) }))
}

pub async fn health() -> impl IntoResponse {
    Json(json!({ "status": "ok" }))
}
