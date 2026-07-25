use crate::client::node::RpcNode;
use crate::client::{node::RoutingTable, rpc::RpcClient};
use metrics::gauge;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::task::JoinSet;
use tracing::{info, warn};

pub struct LockFreeRouter;

impl LockFreeRouter {
    pub async fn run_healthcheck_loop(rpc_client: RpcClient) {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;

            let mut set = JoinSet::new();
            let client = rpc_client.client.clone();

            for node in &rpc_client.all_nodes {
                let node = node.clone();
                let client = client.clone();

                set.spawn(async move {
                    let (health, latency) = RpcClient::get_health(client, &node).await;

                    node.latency.store(latency, Ordering::Relaxed);
                    node.healthy.store(health, Ordering::Relaxed);
                    gauge!(
                        description: "Whether an RPC node passed its latest healthcheck",
                        "rpc_node_healthy",
                        "node" => node.name.clone(),
                        "tier" => node.tier.to_string(),
                    )
                    .set(if health { 1.0 } else { 0.0 });

                    if health {
                        info!("node {} latency: {}ms", node.name, latency);
                        return Some(node);
                    }

                    warn!("node {} unhealthy", node.name);
                    None
                });
            }

            let result = set.join_all().await;
            let mut active_nodes: Vec<Arc<RpcNode>> = result.into_iter().flatten().collect();
            let healthy_nodes = active_nodes.len();

            gauge!(
                description: "Number of RPC nodes that passed the latest healthcheck",
                "rpc_healthy_nodes",
            )
            .set(u32::try_from(healthy_nodes).unwrap_or(u32::MAX));

            if active_nodes.is_empty() {
                tracing::error!(
                    "CRITICAL: All nodes failed healthcheck! Failing open (fallback to all nodes)."
                );

                active_nodes.clone_from(&rpc_client.all_nodes);
            }

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
