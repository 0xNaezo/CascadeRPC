use crate::client::node::RpcNode;
use crate::client::{node::RoutingTable, rpc::RpcClient};
use arc_swap::ArcSwap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::task::JoinSet;
use tracing::{info, warn};

pub struct LockFreeRouter {
    pub table: ArcSwap<RoutingTable>,
}

impl LockFreeRouter {
    pub async fn run_healthcheck_loop(rpc_client: &RpcClient) {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));

        loop {
            interval.tick().await;

            let mut set = JoinSet::new();
            let client = rpc_client.client.clone();

            for node in &rpc_client.all_nodes {
                let node = node.clone();
                let client = client.clone();

                set.spawn(async move {
                    if RpcClient::get_health(client, &node).await {
                        return Some(node);
                    }

                    warn!("node {} unhealthy", node.name);
                    None
                });
            }

            let result = set.join_all().await;
            let mut active_nodes: Vec<Arc<RpcNode>> = result.into_iter().flatten().collect();

            active_nodes
                .sort_unstable_by_key(|node| (node.tier, node.latency.load(Ordering::Relaxed)));

            info!(
                "healthcheck: {}/{} nodes active",
                active_nodes.len(),
                rpc_client.all_nodes.len()
            );

            rpc_client
                .routing_table
                .store(Arc::new(RoutingTable { active_nodes }));
        }
    }
}
