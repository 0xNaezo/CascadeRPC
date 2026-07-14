use crate::client::{node::RoutingTable, rpc::RpcClient};
use arc_swap::ArcSwap;
use std::sync::Arc;
use tracing::{info, warn};

pub struct LockFreeRouter {
    pub table: ArcSwap<RoutingTable>,
}

impl LockFreeRouter {
    pub async fn run_healthcheck_loop(rpc_client: &RpcClient) {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;

            let mut active_nodes = Vec::new();
            // не будет ли замедлять выделение памяти

            for node in &rpc_client.all_nodes {
                if rpc_client.get_health(node).await {
                    active_nodes.push(Arc::clone(node));
                } else {
                    warn!("node {} unhealthy", node.name);
                }
            }

            active_nodes.sort_unstable_by_key(|node| node.tier);

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
