use anyhow::Result;
use rpc_load_balancer::{
    core::{healthcheck::HealthCheckLoop, node::RpcNode, reload, rpc::RpcClient},
    provider::load_config::Settings,
    quotas::persistence::{self, restore},
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

    let quotas = restore()?;

    let nodes: Vec<RpcNode> = RpcNode::build_nodes(config.nodes)?;

    let rpc_client = RpcClient::new(nodes)?;

    rpc_client.load_quotas(&quotas);

    tokio::spawn(HealthCheckLoop::run_healthcheck_loop(rpc_client.clone()));
    tokio::spawn(persistence::start_disk_flusher(rpc_client.clone()));
    #[cfg(unix)]
    tokio::spawn(reload::watch_sighup(
        rpc_client.clone(),
        server_config.clone(),
    ));

    server::init_server(
        rpc_client.clone(),
        server_config.port,
        server_config.host,
        server_config.enable_metrics,
    )
    .await?;

    persistence::flush(&rpc_client).await;

    Ok(())
}
