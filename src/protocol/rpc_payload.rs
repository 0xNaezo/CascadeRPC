//! The parts of a JSON-RPC message the balancer actually reads.
//!
//! Every type here is a partial view on purpose: the balancer forwards bodies
//! byte for byte and only needs the few fields it routes and classifies on, so
//! deserializing the rest would be work thrown away on every request.

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

/// An upstream response reduced to its `error` field, which is what decides
/// whether the answer is final or the next node gets a turn.
#[derive(Debug, Serialize, Deserialize)]
pub struct RpcErrorOnly {
    pub error: Option<RpcError>,
}

/// A `getHealth` response. Both fields default to `None`, so a body carrying
/// neither parses and simply reads as unhealthy.
#[derive(Debug, Serialize, Deserialize)]
pub struct RpcHealthResponse {
    #[serde(default)]
    pub result: Option<String>,

    #[serde(default)]
    pub error: Option<RpcError>,
}

/// A JSON-RPC error reduced to its code; the message is forwarded to the client
/// inside the original body and never inspected.
#[derive(Debug, Serialize, Deserialize)]
pub struct RpcError {
    pub code: i32,
}

/// A request reduced to its `method` field.
///
/// The name borrows straight out of the request buffer, so resolving a method
/// costs no allocation. That also means it cannot carry an escape sequence —
/// a `\u0067` in the name makes this fail to deserialize, and the request is
/// answered as a parse error. No real client spells a method name that way.
#[derive(Deserialize)]
pub struct MethodExtractor<'a> {
    #[serde(borrow)]
    pub method: &'a str,
}

/// A parsed request body: one call, or a batch of them.
///
/// Not routed yet — batching is not implemented, and the balancer treats a
/// batch body as one opaque request.
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
