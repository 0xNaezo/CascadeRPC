use anyhow::Result;
use rpc_load_balancer::{
    client::{NodeConfigs, RpcClient, RpcNode},
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

    let nodes = vec![
        RpcNode {
            name: "Helius".into(),
            url: format!("https://mainnet.helius-rpc.com/?api-key={helius_key}").parse()?,
        },
        RpcNode {
            name: "Alchemy".into(),
            url: format!("https://solana-mainnet.g.alchemy.com/v2/{alchemy_key}").parse()?,
        },
        RpcNode {
            name: "Public Mainnet".into(),
            url: "https://api.mainnet-beta.solana.com".parse()?,
        },
    ];

    let node_configs = NodeConfigs { nodes };
    let rpc_client = RpcClient::new(node_configs);

    server::init_server(rpc_client).await?;

    Ok(())
}
