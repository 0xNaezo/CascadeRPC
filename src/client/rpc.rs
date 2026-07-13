use anyhow::Result;
use bytes::Bytes;
use reqwest::{Client, Response};
use serde_json::json;
use std::sync::{Arc, atomic::AtomicU32};
use tracing::info;
use url::Url;

use crate::client::node::NodeConfigs;

use crate::structs::RpcBalanceResponse;

#[derive(Clone)]
pub struct RpcClient {
    pub client: Client,
    pub node_configs: NodeConfigs,
    pub request_counter: Arc<AtomicU32>,
}

impl RpcClient {
    #[must_use]
    pub fn new(node_configs: NodeConfigs) -> Self {
        let client = Client::new();

        Self {
            client,
            node_configs,
            request_counter: Arc::new(AtomicU32::new(0)),
        }
    }

    /// Sends a balance request with fallback across nodes.
    ///
    /// # Errors
    ///
    /// Returns an error if all RPC nodes fail or exhaust their rate limits.
    pub async fn send_with_fallback(&self, address: String) -> Result<RpcBalanceResponse> {
        info!(address = %address, "fetching balance");

        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getBalance",
            "params": [
                address
            ]
        })
        .to_string();

        let body_bytes = Bytes::from(body);

        for node in &self.node_configs.nodes {
            let _permit = match node.acquire_and_check().await {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!("{}", e);
                    continue;
                }
            };

            tracing::info!("Sending request to {}", node.name);

            let response =
                match Self::send_request(self.client.clone(), body_bytes.clone(), node.url.clone())
                    .await
                {
                    Ok(res) => res,
                    Err(e) => {
                        tracing::error!("Node {} HTTP failed: {e}", node.name);
                        continue;
                    }
                };

            let parse_res: RpcBalanceResponse = match response.json().await {
                Ok(res) => res,
                Err(e) => {
                    tracing::error!("Node {} returned invalid JSON: {e}", node.name);
                    continue;
                }
            };

            if let Some(err) = parse_res.error {
                tracing::error!("Node {} returned error: {err:#?}", node.name);
                continue;
            }

            return Ok(parse_res);
        }

        Err(anyhow::format_err!(
            "All RPC nodes failed or exhausted rate limits"
        ))
    }

    /// Sends a JSON-RPC request to the given URL.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP request fails.
    pub async fn send_request(client: Client, body: Bytes, url: Url) -> Result<Response> {
        let result = client
            .post(url)
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await?;

        Ok(result)
    }
}
