use bytes::Bytes;
use reqwest::Client;
use serde_json::json;
use std::time::Duration;
use tracing::debug;

use crate::client::node::RpcNode;
use crate::client::rpc::RpcClient;
use crate::structs::balancer::RpcHealthResponse;

impl RpcClient {
    pub async fn get_health(client: Client, node: &RpcNode) -> (bool, u32) {
        debug!(node = %node.name, "checking node health");

        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getHealth"
        })
        .to_string();

        let body_bytes = Bytes::from(body);

        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        for _ in 0..3 {
            interval.tick().await;

            let start_time = tokio::time::Instant::now();

            let Ok(Ok(response)) = tokio::time::timeout(
                Duration::from_millis(500),
                Self::send_request(client.clone(), body_bytes.clone(), node.url.clone()),
            )
            .await
            else {
                continue;
            };

            let latency = start_time.elapsed().as_millis() as u32;

            let result: RpcHealthResponse = match response.json().await {
                Ok(result) => result,
                Err(_) => continue,
            };

            return (
                result.error.is_none() && result.result.as_deref() == Some("ok"),
                latency,
            );
        }

        (false, u32::MAX)
    }
}
