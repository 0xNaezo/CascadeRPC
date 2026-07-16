use anyhow::Result;
use rpc_load_balancer::{
    client::{node::RpcNode, router::LockFreeRouter, rpc::RpcClient},
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

    let helius_key = std::env::var("HELIUS_API_KEY")?;
    let alchemy_key = std::env::var("ALCHEMY_API_KEY")?;

    let helius = RpcNode::new(
        "helius".into(),
        &format!("https://mainnet.heliu----s-rpc.com/?api-key={helius_key}"),
        100,
        10,
        1,
    )?;
    let alchemy = RpcNode::new(
        "alchemy".into(),
        &format!("https://solana-mainnet.g.alchemy.com/v2/{alchemy_key}"),
        100,
        10,
        1,
    )?;

    let public_mainnet = RpcNode::new(
        "public-mainnet".into(),
        "https://api.mainnet-beta.solana.com",
        40,
        5,
        0,
    )?;

    let nodes = vec![helius, alchemy, public_mainnet];

    let rpc_client = RpcClient::new(nodes)?;

    tokio::spawn(LockFreeRouter::run_healthcheck_loop(rpc_client.clone()));

    server::init_server(rpc_client).await?;

    Ok(())
}
