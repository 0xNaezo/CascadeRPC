use anyhow::Result;
use rpc_load_balancer::{
    core::{
        healthcheck::HealthCheckLoop, rpc::RpcClient, node::RpcNode,
    },
    provider::{load_config::Settings, pricing_parser},
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
        .map(|n| {
            let costs = pricing_parser::load_from_path(&n.provider_pricing_path)?;

            RpcNode::new(
                n.name,
                &n.url,
                n.rps_limit,
                n.max_concurrent,
                n.tier,
                costs,
                n.monthly_limit,
                n.billing_type,
            )
        })
        .collect::<Result<Vec<RpcNode>, _>>()?;

    let rpc_client = RpcClient::new(nodes)?;

    tokio::spawn(HealthCheckLoop::run_healthcheck_loop(rpc_client.clone()));

    server::init_server(
        rpc_client,
        server_config.port,
        server_config.host,
        server_config.enable_metrics,
    )
    .await?;

    Ok(())
}
