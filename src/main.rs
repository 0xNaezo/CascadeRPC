use anyhow::Result;
use rpc_load_balancer::{
    core::{
        healthcheck::HealthCheckLoop,
        node::{NewNode, RpcNode},
        rpc::RpcClient,
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
            let (costs, spillover_percent) =
                pricing_parser::load_from_path(&n.provider_pricing_path)?;

            RpcNode::new(NewNode {
                name: n.name,
                url: n.url,
                rps_limit: n.rps_limit,
                max_concurrent: n.max_concurrent,
                tier: n.tier,
                method_costs: costs,
                monthly_limit: n.monthly_limit,
                billing_type: n.billing_type,
                spillover_percent,
            })
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
