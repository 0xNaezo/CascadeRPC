use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Serialize, Deserialize)]
pub struct RpcBalanceResponse {
    pub jsonrpc: String,
    pub id: Value,

    #[serde(default)]
    pub result: Option<BalanceResult>,

    #[serde(default)]
    pub error: Option<RpcError>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BalanceResult {
    pub context: Context,
    pub value: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Context {
    pub slot: u64,
}
