use anyhow::Result;
use reqwest::{Client, Response};
use serde_json::{Value, json};
use tokio::task::JoinSet;
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

    pub async fn get_balance(
        client: Client,
        address: String, // pass the body, not the address + pass in bytes
        url: Url,
    ) -> Result<RpcBalanceResponse> {
        // TODO: Avoid creating JSON every time. Do it once and convert to bytes

        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getBalance",
            "params": [
                address
            ]
        });

        let response: RpcBalanceResponse =
            Self::send_request(client, body, url).await?.json().await?;

        Ok(response)
    }

    pub async fn post_three_requests(
        &self,
        client: Client,
        address: String,
    ) -> Result<RpcBalanceResponse> {
        let mut set = JoinSet::new();

        // immediately collect the request body and convert to bytes

        let urls: Vec<&Url> = self
            .node_configs
            .nodes
            .iter()
            .map(|node| &node.url)
            .collect();

        // TODO: Unify logic, avoid two separate variables
        for url in urls {
            set.spawn(Self::get_balance(
                client.clone(),
                address.clone(),
                url.clone(),
            ));
        }

        while let Some(result) = set.join_next().await {
            match result {
                Ok(Ok(response)) => return Ok(response),
                Ok(Err(_err)) => continue,
                Err(_err) => continue,
            };
        }

        // TODO: Write a proper error message
        Err(anyhow::format_err!("rastrat"))
    }

    pub async fn send_request(client: Client, body: Value, url: Url) -> Result<Response> {
        let result = client.post(url).json(&body).send().await?;

        Ok(result)
    }
}
