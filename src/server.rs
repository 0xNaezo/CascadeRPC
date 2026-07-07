use crate::{client::RpcClient, structs::RpcBalanceResponse};
use axum::{
    Router,
    extract::{Json, Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
};
use serde::Serialize;
use serde_json::json;

#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

async fn init_server(rpc_client: RpcClient) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/health", get(health))
        .route("/get-balance/{address}", get(get_balance))
        .with_state(rpc_client);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;

    Ok(axum::serve(listener, app).await?)
}

async fn get_balance(
    State(rpc_client): State<RpcClient>,
    Path(address): Path<String>,
) -> Result<Json<RpcBalanceResponse>, (StatusCode, Json<ErrorResponse>)> {
    let client = rpc_client.client.clone();

    let result = rpc_client.post_three_requests(client, address).await;

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

pub async fn health() -> impl IntoResponse {
    Json(json!({ "status": "ok" }))
}
