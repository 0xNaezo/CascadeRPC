//! The node set the router walks, and the quota slot each node keeps across
//! reloads.
//!
//! Published whole and never mutated in place: a health check round or a reload
//! builds a new [`Topology`] and swaps it in, so a request that already started
//! finishes against the set it began with.

use anyhow::Result;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tracing::{info, warn};

use crate::core::node::RpcNode;
use crate::quotas::state::{GlobalQuotaState, MAX_NODES};

/// Every node the config describes, plus the order the router walks them in.
///
/// The two live in one struct so a single swap replaces both: published apart,
/// they would disagree for a moment and `active` could name a node `all` no
/// longer has.
#[derive(Clone)]
pub struct Topology {
    pub all: Vec<Arc<RpcNode>>,
    pub active: Vec<Arc<RpcNode>>,
}

/// One node's line in the `/health` response.
#[derive(Serialize)]
pub struct NodeHealth {
    pub name: String,
    pub tier: u8,
    pub status: &'static str,
    pub latency_ms: Option<u32>,
}

impl Topology {
    /// Builds a topology from a node set, ranking it in the same step.
    ///
    /// Every publisher goes through here rather than filling the two fields in
    /// by hand, which is what keeps `active` from being ranked out of a
    /// different set than `all`.
    #[must_use]
    pub fn new(all: Vec<Arc<RpcNode>>) -> Self {
        Self {
            active: Self::rank(&all),
            all,
        }
    }

    /// Picks and orders the nodes the router may use: healthy ones first by
    /// tier, then by measured latency.
    ///
    /// The sort is stable so nodes the balancer cannot yet tell apart keep the
    /// order the operator wrote them in — every unprobed node reports the same
    /// `u32::MAX` latency.
    ///
    /// With nothing healthy it fails open on the whole set: answering 502 to
    /// everything is worse than trying a node the last probe disliked.
    #[must_use]
    pub fn rank(all: &[Arc<RpcNode>]) -> Vec<Arc<RpcNode>> {
        let mut active: Vec<Arc<RpcNode>> = all
            .iter()
            .filter(|node| node.healthy.load(Ordering::Relaxed))
            .cloned()
            .collect();

        if active.is_empty() {
            active.extend_from_slice(all);
        }

        active.sort_by_key(|node| (node.tier, node.latency.load(Ordering::Relaxed)));

        active
    }

    /// What the last health check round measured, per node.
    ///
    /// Reads only the per-node atomics, so it costs nothing and never dials an
    /// upstream. It lives here rather than in the HTTP layer so that the memory
    /// model of a node — which fields are atomic, and under which ordering —
    /// stays inside the module that owns it.
    #[must_use]
    pub fn health(&self) -> Vec<NodeHealth> {
        self.all
            .iter()
            .map(|node| {
                let is_up = node.healthy.load(Ordering::Relaxed);
                let latency = node.latency.load(Ordering::Relaxed);

                NodeHealth {
                    name: node.name.clone(),
                    tier: node.tier,
                    status: if is_up { "up" } else { "down" },
                    latency_ms: is_up.then_some(latency),
                }
            })
            .collect()
    }
}

/// Hands every node the quota-counter slot it has to keep using.
///
/// `id` indexes [`GlobalQuotaState`], so it cannot stay the node's position in
/// the config: inserting one node at the top of the TOML would shift every
/// counter by one. A node keeps the slot it was given for as long as its name
/// stays in the config, and slots freed by removed nodes are recycled — zeroed
/// on the way out, or whoever takes one would inherit a stranger's spend.
///
/// The name is the identity, the same key the usage file on disk is written
/// with. A rename is indistinguishable from "one node left, another arrived",
/// so a renamed node starts from zero; both halves of that are logged rather
/// than hidden.
///
/// # Errors
///
/// Returns an error if the set is larger than [`MAX_NODES`] or two nodes share
/// a name. Both are checked before anything is written, so a rejected set
/// leaves the counters untouched.
pub(crate) fn assign_ids(
    prev: &[Arc<RpcNode>],
    new: &mut [RpcNode],
    usage: &GlobalQuotaState,
) -> Result<()> {
    if new.len() > MAX_NODES {
        anyhow::bail!(
            "Fatal error: at most {MAX_NODES} nodes are supported, config has {}",
            new.len()
        );
    }

    {
        // Two nodes under one name would share a counter here and collapse into
        // a single entry in the usage file.
        let mut seen = HashSet::with_capacity(new.len());
        for node in new.iter() {
            if !seen.insert(node.name.as_str()) {
                anyhow::bail!("Fatal error: two nodes share the name '{}'", node.name);
            }
        }
    }

    let prev_ids: HashMap<&str, usize> = prev
        .iter()
        .map(|node| (node.name.as_str(), node.id))
        .collect();

    let mut taken = [false; MAX_NODES];
    let mut arrived: Vec<usize> = Vec::new();

    for (position, node) in new.iter_mut().enumerate() {
        match prev_ids.get(node.name.as_str()).copied() {
            Some(id) => {
                node.id = id;
                taken[id] = true;
            }
            None => arrived.push(position),
        }
    }

    // Before the slots below are recycled: a departing node's counter is about
    // to be handed to a new node and zeroed, so this is the last chance to
    // report what it had spent.
    for old in prev {
        if !new.iter().any(|node| node.name == old.name) {
            warn!(
                node = %old.name,
                usage = usage.usage(old.id).get(),
                "node left the config; its usage is no longer tracked"
            );
        }
    }

    let mut free = (0..MAX_NODES).filter(|id| !taken[*id]);

    for position in arrived {
        // Cannot run dry: `taken` holds one slot per surviving node and the
        // whole set is no larger than the number of slots.
        let id = free
            .next()
            .ok_or_else(|| anyhow::anyhow!("Fatal error: out of quota slots"))?;

        new[position].id = id;
        usage.usage(id).set(0);

        if !prev.is_empty() {
            info!(node = %new[position].name, "node is new to the config, starting from zero usage");
        }
    }

    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    use crate::core::node::NewNode;
    use crate::protocol::cost_table::ProviderCostTable;

    /// Nodes under the given names, in the given order — a stand-in for the
    /// `[[nodes]]` list the operator edits between reloads.
    fn named(names: &[&str]) -> Vec<RpcNode> {
        names
            .iter()
            .map(|name| {
                RpcNode::new(NewNode {
                    name: (*name).to_owned(),
                    url: "http://localhost:9".into(),
                    rps_limit: 1,
                    max_concurrent: 1,
                    tier: 0,
                    method_costs: ProviderCostTable::default(),
                    monthly_limit: u64::MAX,
                    spillover_percent: 100,
                    reset_day: 1,
                })
                .unwrap()
            })
            .collect()
    }

    fn arced(nodes: Vec<RpcNode>) -> Vec<Arc<RpcNode>> {
        nodes.into_iter().map(Arc::new).collect()
    }

    fn names_of(nodes: &[Arc<RpcNode>]) -> Vec<&str> {
        nodes.iter().map(|node| node.name.as_str()).collect()
    }

    #[test]
    fn rank_keeps_config_order_between_nodes_it_cannot_tell_apart() {
        // Every unprobed node reports the same u32::MAX latency, so the sort
        // has nothing to go on and must not shuffle what the operator wrote.
        let all = arced(named(&["a", "b", "c"]));

        assert_eq!(names_of(&Topology::rank(&all)), ["a", "b", "c"]);
    }

    #[test]
    fn rank_orders_by_tier_then_latency() {
        let mut built = named(&["slow_t0", "fast_t0", "fast_t1"]);
        built[2].tier = 1;
        let all = arced(built);
        all[0].latency.store(200, Ordering::Relaxed);
        all[1].latency.store(1, Ordering::Relaxed);
        all[2].latency.store(1, Ordering::Relaxed);

        assert_eq!(
            names_of(&Topology::rank(&all)),
            ["fast_t0", "slow_t0", "fast_t1"]
        );
    }

    #[test]
    fn rank_drops_unhealthy_nodes() {
        let all = arced(named(&["up", "down"]));
        all[1].healthy.store(false, Ordering::Relaxed);

        assert_eq!(names_of(&Topology::rank(&all)), ["up"]);
    }

    #[test]
    fn rank_fails_open_when_nothing_is_healthy() {
        // Trying a node the last probe disliked beats answering 502 to
        // everything.
        let all = arced(named(&["a", "b"]));
        for node in &all {
            node.healthy.store(false, Ordering::Relaxed);
        }

        assert_eq!(names_of(&Topology::rank(&all)), ["a", "b"]);
    }

    #[test]
    fn health_reports_latency_only_for_nodes_that_are_up() {
        // A down node's last measured latency says nothing about the node now,
        // and reporting it as if it did is how a dashboard shows a dead node as
        // the fastest one.
        let all = arced(named(&["up", "down"]));
        all[0].latency.store(12, Ordering::Relaxed);
        all[1].latency.store(7, Ordering::Relaxed);
        all[1].healthy.store(false, Ordering::Relaxed);

        let health = Topology::new(all).health();

        assert_eq!((health[0].status, health[0].latency_ms), ("up", Some(12)));
        assert_eq!((health[1].status, health[1].latency_ms), ("down", None));
    }
}
