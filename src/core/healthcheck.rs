//! The loop that keeps the routing table honest: probe every node, then
//! republish the order the router walks them in.
//!
//! This is where node selection happens. The router itself does not choose — it
//! takes the first node in the table that will accept the request, so the sort
//! published here *is* the balancing policy.

use crate::core::rpc::{RpcClient, Topology};
use metrics::{Unit, gauge, histogram};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::task::JoinSet;
use tracing::{debug, error, info, warn};

/// Namespace for the probe loop; carries no state of its own.
pub struct HealthCheckLoop;

impl HealthCheckLoop {
    /// Probes every node every 10 seconds, for as long as the task lives.
    ///
    /// The "everything is down" error is logged on the edge, not on every tick:
    /// the balancer keeps serving in that state (see [`Topology::rank`]), and a
    /// line every 10 seconds would bury whatever is actually wrong.
    pub async fn run_healthcheck_loop(rpc_client: RpcClient) {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut all_nodes_unhealthy = false;

        loop {
            interval.tick().await;

            let no_healthy_nodes = Self::run_once(&rpc_client).await == 0;

            if no_healthy_nodes && !all_nodes_unhealthy {
                error!("all nodes failed healthcheck; failing open");
            }
            all_nodes_unhealthy = no_healthy_nodes;
        }
    }

    /// Probes every node once and republishes the routing order, returning how
    /// many nodes answered.
    ///
    /// Also called straight after a config reload: a brand-new node set carries
    /// nothing but default health, and waiting a whole tick to find out which
    /// of its nodes are actually up would route requests on a guess.
    ///
    /// Reading the node set, probing it — up to ~2.5s for a node that never
    /// answers, see [`RpcClient::get_health`] — and storing the result is one
    /// critical section. Without the lock a reload landing mid-round would be
    /// undone by an `active` list built from the node set it had just replaced.
    pub async fn run_once(rpc_client: &RpcClient) -> usize {
        let _guard = rpc_client.topology_lock.lock().await;

        let all = rpc_client.topology.load().all.clone();

        let mut set = JoinSet::new();
        let client = rpc_client.client.clone();

        for node in &all {
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
                    return;
                }

                if was_healthy {
                    warn!(node = %node.name, "node became unhealthy");
                }
            });
        }

        set.join_all().await;

        // Every probe has stored its verdict on the node itself, so the ranking
        // reads the same flags the router will.
        let healthy_nodes = all
            .iter()
            .filter(|node| node.healthy.load(Ordering::Relaxed))
            .count();

        gauge!(
            description: "Number of RPC nodes that passed the latest healthcheck",
            "rpc_healthy_nodes",
        )
        .set(u32::try_from(healthy_nodes).unwrap_or(u32::MAX));

        debug!(
            healthy_nodes,
            total_nodes = all.len(),
            "healthcheck completed"
        );

        rpc_client.topology.store(Arc::new(Topology {
            active: Topology::rank(&all),
            all,
        }));

        healthy_nodes
    }
}
