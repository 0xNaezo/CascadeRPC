//! The loop that keeps the routing table honest: probe every node, then
//! republish the order the router walks them in.
//!
//! This is where node selection happens. The router itself does not choose — it
//! takes the first node in the table that will accept the request, so the sort
//! published here *is* the balancing policy.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::task::JoinSet;
use tracing::{debug, error, info, warn};

use crate::core::health;
use crate::core::rpc::RpcClient;
use crate::core::topology::Topology;
use crate::metrics;

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
    /// answers, see [`crate::core::health::probe`] — and storing the result is one
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
                let (health, latency) = health::probe(&client, &node).await;

                metrics::record_probe(
                    &node.name,
                    node.tier,
                    health,
                    started.elapsed().as_secs_f64(),
                );

                node.status.latency.store(latency, Ordering::Relaxed);
                let was_healthy = node.status.healthy.swap(health, Ordering::Relaxed);

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
            .filter(|node| node.status.healthy.load(Ordering::Relaxed))
            .count();

        metrics::set_healthy_nodes(healthy_nodes);

        debug!(
            healthy_nodes,
            total_nodes = all.len(),
            "healthcheck completed"
        );

        rpc_client.topology.store(Arc::new(Topology::new(all)));

        healthy_nodes
    }
}
