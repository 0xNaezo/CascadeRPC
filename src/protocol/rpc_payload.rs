use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

#[derive(Debug, Serialize, Deserialize)]
pub struct RpcErrorOnly {
    pub error: Option<RpcError>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RpcHealthResponse {
    #[serde(default)]
    pub result: Option<String>,

    #[serde(default)]
    pub error: Option<RpcError>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RpcError {
    pub code: i32,
    pub message: String,
}

#[derive(Deserialize)]
pub struct MethodExtractor<'a> {
    #[serde(borrow)]
    pub method: &'a str,
}

pub enum IncomingPayload<'a> {
    Single(&'a RawValue),
    Batch(Vec<&'a RawValue>),
}

/// Parses an incoming JSON-RPC payload (single or batch).
///
/// # Errors
///
/// Returns `Err` with the reason if the payload is not a valid JSON object,
/// not a valid JSON array, or does not start with `{`/`[`.
pub fn parse_incoming(payload: &[u8]) -> Result<IncomingPayload<'_>, &'static str> {
    let first_byte = payload.iter().find(|b| !b.is_ascii_whitespace());

    match first_byte {
        Some(b'{') => {
            let raw =
                serde_json::from_slice::<&RawValue>(payload).map_err(|_| "Invalid JSON object")?;
            Ok(IncomingPayload::Single(raw))
        }

        Some(b'[') => {
            let batch = serde_json::from_slice::<Vec<&RawValue>>(payload)
                .map_err(|_| "Invalid JSON array")?;
            Ok(IncomingPayload::Batch(batch))
        }

        _ => Err("Payload must start with '{' or '['"),
    }
}
