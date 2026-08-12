use crate::core::node::RpcNode;
use crate::core::{node::RoutingTable, rpc::RpcClient};
use metrics::{Unit, gauge, histogram};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::task::JoinSet;
use tracing::{debug, error, info, warn};

pub struct HealthCheckLoop;

impl HealthCheckLoop {
    pub async fn run_healthcheck_loop(rpc_client: RpcClient) {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut all_nodes_unhealthy = false;

        loop {
            interval.tick().await;

            let mut set = JoinSet::new();
            let client = rpc_client.client.clone();

            for node in &rpc_client.all_nodes {
                let node = node.clone();
                let client = client.clone();

                set.spawn(async move {
                    let started = tokio::time::Instant::now();
                    let (health, latency) = RpcClient::get_health(client, &node).await;
                    let outcome = if health { "healthy" } else { "unhealthy" };

                    histogram!(
                        description: "Time spent completing an RPC node healthcheck",
                        unit: Unit::Seconds,
                        "rpc_healthcheck_duration",
                        "node" => node.name.clone(),
                        "outcome" => outcome,
                    )
                    .record(started.elapsed().as_secs_f64());

                    node.latency.store(latency, Ordering::Relaxed);
                    let was_healthy = node.healthy.swap(health, Ordering::Relaxed);
                    gauge!(
                        description: "Whether an RPC node passed its latest healthcheck",
                        "rpc_node_healthy",
                        "node" => node.name.clone(),
                        "tier" => node.tier.to_string(),
                    )
                    .set(if health { 1.0 } else { 0.0 });

                    if health {
                        debug!(node = %node.name, latency_ms = latency, "node healthy");
                        if !was_healthy {
                            info!(node = %node.name, "node recovered");
                        }
                        return Some(node);
                    }

                    if was_healthy {
                        warn!(node = %node.name, "node became unhealthy");
                    }
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

            let no_healthy_nodes = active_nodes.is_empty();
            if no_healthy_nodes {
                if !all_nodes_unhealthy {
                    error!("all nodes failed healthcheck; failing open");
                }

                active_nodes.clone_from(&rpc_client.all_nodes);
            }
            all_nodes_unhealthy = no_healthy_nodes;

            active_nodes
                .sort_unstable_by_key(|node| (node.tier, node.latency.load(Ordering::Relaxed)));

            debug!(
                healthy_nodes,
                total_nodes = rpc_client.all_nodes.len(),
                "healthcheck completed"
            );

            rpc_client
                .routing_table
                .store(Arc::new(RoutingTable { active_nodes }));
        }
    }
}
