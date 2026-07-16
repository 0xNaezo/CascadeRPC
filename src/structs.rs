use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Serialize, Deserialize)]
pub struct RpcErrorOnly {
    pub error: Option<Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RpcHealthResponse {
    pub jsonrpc: String,
    pub id: Value,

    #[serde(default)]
    pub result: Option<String>,

    #[serde(default)]
    pub error: Option<RpcError>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
}
