use anyhow::Result;
use rpc_load_balancer::{
    client::{node::RpcNode, router::LockFreeRouter, rpc::RpcClient},
    config::Settings,
    server,
};

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let config = Settings::load()?;

    let server_config = config.server;

    let nodes: Vec<RpcNode> = config
        .nodes
        .into_iter()
        .map(RpcNode::try_from)
        .collect::<Result<Vec<RpcNode>, _>>()?;

    let rpc_client = RpcClient::new(nodes)?;

    tokio::spawn(LockFreeRouter::run_healthcheck_loop(rpc_client.clone()));

    server::init_server(
        rpc_client,
        server_config.port,
        server_config.host,
        server_config.enable_metrics,
    )
    .await?;

    Ok(())
}
