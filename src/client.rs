use anyhow::Result;
use bytes::Bytes;
use reqwest::{Client, Response};
use serde_json::json;
use tokio::task::JoinSet;
use tracing::info;
use url::Url;

use crate::structs::RpcBalanceResponse;

#[derive(Clone)]
pub struct RpcClient {
    pub client: Client,
    pub node_configs: NodeConfigs,
}

#[derive(Clone)]
pub struct NodeConfigs {
    pub nodes: Vec<RpcNode>,
}

#[derive(Clone)]
pub struct RpcNode {
    pub name: String,
    pub url: Url,
}

impl RpcClient {
    pub fn new(node_configs: NodeConfigs) -> Self {
        let client = Client::new();

        Self {
            client,
            node_configs,
        }
    }

    pub async fn post_three_requests(&self, address: String) -> Result<RpcBalanceResponse> {
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
        let client = self.client.clone();
        let nodes = self.node_configs.nodes.clone();

        let mut set = JoinSet::new();

        for node in nodes {
            let body = body_bytes.clone();
            let client = client.clone();
            set.spawn(async move {
                tracing::info!("sending request node={}", node.name);

                let result = Self::send_request(client, body, node.url.clone()).await;

                (node.name, result)
            });
        }

        while let Some(result) = set.join_next().await {
            if let Ok((node_name, Ok(response))) = result {
                let res_struct: RpcBalanceResponse = response.json().await?;

                tracing::info!("got valid response from node={}", node_name);

                return Ok(res_struct);
            }
        }

        Err(anyhow::format_err!("all nodes failed"))
    }

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
