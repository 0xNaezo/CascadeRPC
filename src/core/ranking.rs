//! The loop that keeps the routing table ordered: read what real traffic has
//! measured, then republish the order the router walks the nodes in.
//!
//! Nothing here dials an upstream. Latency and failures are collected by
//! [`crate::core::upstream`] from the requests clients are already paying for,
//! which is what makes the balancer's view of its nodes cost nothing — no probe
//! traffic, no quota spent on questions instead of answers.
//!
//! This is where node *ordering* happens. Which of the ordered nodes may take a
//! given request is [`crate::core::router`]'s call, made per request against
//! the penalty each node is serving.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::time::{Duration, Instant, MissedTickBehavior, interval};
use tracing::{debug, error};

use crate::core::node::seconds_since_start;
use crate::core::rpc::RpcClient;
use crate::core::topology::Topology;
use crate::metrics;

/// How often the routing order is rebuilt from the measurements traffic left
/// behind.
///
/// A second and not less: the ranking only reorders nodes that are all still
/// eligible, and a node that just broke is skipped by the request path the
/// moment it breaks rather than when this next runs. Nothing here is on the
/// critical path of a failure.
const RANK_PERIOD: Duration = Duration::from_secs(1);

/// Namespace for the ranking loop; carries no state of its own.
pub struct RankLoop;

impl RankLoop {
    /// Republishes the routing order once a second, for as long as the task
    /// lives.
    ///
    /// The "everything is down" error is logged on the edge, not on every tick:
    /// the balancer keeps serving in that state — the router fails open, see
    /// [`crate::core::router`] — and a line a second would bury whatever is
    /// actually wrong.
    pub async fn run_rank_loop(rpc_client: RpcClient) {
        let mut ticker = interval(RANK_PERIOD);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut all_nodes_penalized = false;

        loop {
            ticker.tick().await;

            let none_available = Self::rerank(&rpc_client).await == 0;

            if none_available && !all_nodes_penalized {
                error!("every node is serving a penalty; failing open");
            }
            all_nodes_penalized = none_available;
        }
    }

    /// Rebuilds the routing order once and publishes it, returning how many
    /// nodes are not currently serving a penalty.
    ///
    /// Reading the node set, ordering it and storing the result is one critical
    /// section. Without the lock a reload landing mid-round would be undone by a
    /// table built from the node set it had just replaced.
    pub async fn rerank(rpc_client: &RpcClient) -> usize {
        let _guard = rpc_client.topology_lock.lock().await;

        let all = rpc_client.topology.load().all.clone();
        let now_s = seconds_since_start(Instant::now());

        // Once a second per node, off the request path: the gauges are what a
        // dashboard reads, and they are the only reason this walks the set
        // beyond the count below.
        let mut available = 0;
        for node in &all {
            let penalized = node.is_penalized(now_s);
            available += usize::from(!penalized);

            metrics::set_node_state(
                &node.name,
                node.tier,
                penalized,
                node.latency.ema_us.load(Ordering::Relaxed),
            );
        }

        metrics::set_healthy_nodes(available);

        debug!(
            available,
            total_nodes = all.len(),
            "routing order republished"
        );

        rpc_client.topology.store(Arc::new(Topology::new(all)));

        available
    }
}
