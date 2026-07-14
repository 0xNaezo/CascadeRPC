use anyhow::Result;
use rpc_load_balancer::{client::rpc::RpcClient, server};

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    // let helius_key = std::env::var("HELIUS_API_KEY")?;
    // let alchemy_key = std::env::var("ALCHEMY_API_KEY")?;

    let nodes = vec![];

    let rpc_client = RpcClient::new(nodes);

    server::init_server(rpc_client).await?;

    Ok(())
}
