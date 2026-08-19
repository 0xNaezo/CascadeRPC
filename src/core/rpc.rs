//! The shared state every request is served from: the node set, the order the
//! router walks it in, and the quota counters it bills against.
//!
//! Cheap to clone and cloned into every task that needs it — the state lives
//! behind `Arc`s, so a handler, the health check loop and the flusher all see
//! the same topology and the same counters.
//!
//! What lives here is the state and the three operations that replace it whole:
//! building it, reloading it, and seeding it from disk. Routing is in
//! [`crate::core::router`], probing in [`crate::core::health`], and the node set
//! itself in [`crate::core::topology`]; this module owns what all of them share.

use anyhow::Result;
use arc_swap::ArcSwap;
use reqwest::Client;
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::Mutex;

use crate::core::{
    node::RpcNode,
    topology::{NodeHealth, Topology, assign_ids},
    upstream,
};
use crate::quotas::{
    period::{PeriodMap, lock_periods},
    persistence::NodeUsage,
    state::GlobalQuotaState,
};

/// Everything a request needs, shared by every task that serves one.
///
/// `Clone` is cheap and is how the state is handed out: each field is behind an
/// `Arc`, so clones share one topology and one set of counters.
#[derive(Clone)]
pub struct RpcClient {
    /// One pooled HTTP client for every upstream. Its timeout is only a
    /// backstop — see [`crate::core::upstream`], where it is built.
    pub client: Client,
    /// The live node set and routing order, republished whole by a health check
    /// round or a reload. Read with `load()` on the request path, never locked.
    pub topology: Arc<ArcSwap<Topology>>,
    /// Usage counters, indexed by `node.id`. Billed on admission, before the
    /// request is sent.
    pub nodes_usage: Arc<GlobalQuotaState>,
    /// Which billing period each node's counter in [`Self::nodes_usage`] is
    /// credited to — the other half of what the usage file on disk holds. See
    /// [`crate::quotas::period`].
    ///
    /// A plain `Mutex` and not an atomic per slot: it is touched once a minute
    /// by the flusher, never by the request path.
    pub periods: Arc<std::sync::Mutex<PeriodMap>>,
    /// Held by whoever rebuilds [`Self::topology`], so that reading the node
    /// set, working on it and publishing the result is one critical section.
    /// The request path never takes it — it only ever calls `topology.load()`.
    pub(crate) topology_lock: Arc<Mutex<()>>,
}

impl RpcClient {
    /// Builds the client the balancer runs on: one pooled HTTP client, the
    /// ranked node set, and counters at zero.
    ///
    /// Usage is seeded separately, by [`Self::load_quotas`], because it comes
    /// from disk and only makes sense together with the billing periods it was
    /// counted in.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP client cannot be initialized (e.g. a TLS
    /// backend failure), if the set is larger than
    /// [`MAX_NODES`](crate::quotas::state::MAX_NODES), or if two nodes share a
    /// name.
    pub fn new(mut nodes: Vec<RpcNode>) -> Result<Self> {
        let client = upstream::client()?;

        let nodes_usage = Arc::new(GlobalQuotaState::default());

        assign_ids(&[], &mut nodes, &nodes_usage)?;

        let all: Vec<Arc<RpcNode>> = nodes.into_iter().map(Arc::new).collect();

        Ok(Self {
            client,
            topology: Arc::new(ArcSwap::from_pointee(Topology::new(all))),
            nodes_usage,
            periods: Arc::new(std::sync::Mutex::new(PeriodMap::new())),
            topology_lock: Arc::new(Mutex::new(())),
        })
    }

    /// Publishes a freshly built node set, leaving every surviving node its
    /// accumulated usage.
    ///
    /// The counters are not re-read from disk here: the in-memory ones are
    /// newer than the file, which is only written once a minute.
    ///
    /// Nothing is published unless the whole set is accepted, so a rejected
    /// reload leaves the running topology exactly as it was.
    ///
    /// # Errors
    ///
    /// Returns an error if the new set is larger than
    /// [`MAX_NODES`](crate::quotas::state::MAX_NODES) or two of its nodes share
    /// a name.
    pub async fn reload(&self, mut nodes: Vec<RpcNode>) -> Result<()> {
        let _guard = self.topology_lock.lock().await;

        assign_ids(&self.topology.load().all, &mut nodes, &self.nodes_usage)?;

        let all: Vec<Arc<RpcNode>> = nodes.into_iter().map(Arc::new).collect();

        self.topology.store(Arc::new(Topology::new(all)));

        Ok(())
    }

    /// Seeds the usage counters, and the periods they were counted in, from a
    /// previous run's flush.
    ///
    /// Entries are matched by node name, the same key the flusher writes, so a
    /// reordered config still restores each node its own usage. A name with no
    /// node is dropped (it left the config) and a node with no entry keeps its
    /// zero (it is new to the config).
    ///
    /// Seeding usage without its period would be worse than not seeding at all:
    /// the counter would carry last month's spend with nothing to say the month
    /// has turned. The two go in together, and
    /// [`crate::quotas::period::rollover_if_new_period`] resolves them right
    /// after.
    ///
    /// Overwrites the counters, so it must run before the healthcheck loop, the
    /// flusher and the server are started.
    pub fn load_quotas(&self, quotas: &BTreeMap<String, NodeUsage>) {
        let mut periods = lock_periods(&self.periods);

        for node in &self.topology.load().all {
            if let Some(entry) = quotas.get(&node.name) {
                self.nodes_usage.usage(node.id).set(entry.used);
                periods.insert(node.name.clone(), entry.period_start);
            }
        }
    }

    /// What the last health check round measured, per node.
    ///
    /// Costs nothing and never dials an upstream, so the HTTP layer can answer
    /// `/health` from it directly.
    #[must_use]
    pub fn health_snapshot(&self) -> Vec<NodeHealth> {
        self.topology.load().health()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    use crate::core::node::NewNode;
    use crate::protocol::cost_table::ProviderCostTable;
    use crate::quotas::state::MAX_NODES;

    fn nodes(count: usize) -> Vec<RpcNode> {
        (0..count)
            .map(|i| {
                RpcNode::new(NewNode {
                    name: format!("node{i}"),
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

    /// Nodes under the given names, in the given order — a stand-in for the
    /// `[[nodes]]` list the operator edits between reloads.
    fn named(names: &[&str]) -> Vec<RpcNode> {
        names
            .iter()
            .map(|name| {
                let mut node = nodes(1).remove(0);
                node.name = (*name).to_owned();
                node
            })
            .collect()
    }

    fn slot_of(client: &RpcClient, name: &str) -> usize {
        client
            .topology
            .load()
            .all
            .iter()
            .find(|node| node.name == name)
            .map(|node| node.id)
            .unwrap()
    }

    fn usage_of(client: &RpcClient, name: &str) -> u64 {
        client.nodes_usage.usage(slot_of(client, name)).get()
    }

    #[test]
    fn new_assigns_dense_ids_in_config_order() {
        // The quota array is indexed by these ids; a gap or a duplicate would
        // make two nodes share one counter.
        let client = RpcClient::new(nodes(3)).unwrap();

        let ids: Vec<usize> = client
            .topology
            .load()
            .all
            .iter()
            .map(|node| node.id)
            .collect();

        assert_eq!(ids, vec![0, 1, 2]);
    }

    #[test]
    fn new_rejects_more_nodes_than_quota_slots() {
        // Without this guard the extra node carries an id past the end of the
        // array and `usage()` panics on the request hot path.
        assert!(RpcClient::new(nodes(MAX_NODES + 1)).is_err());
        assert!(RpcClient::new(nodes(MAX_NODES)).is_ok());
    }

    /// A restored file entry: usage, plus the period it was counted in.
    fn stored(used: u64) -> NodeUsage {
        NodeUsage {
            used,
            period_start: time::macros::date!(2026 - 08 - 01),
        }
    }

    #[test]
    fn load_quotas_seeds_each_node_from_its_own_entry() {
        let client = RpcClient::new(nodes(2)).unwrap();

        client.load_quotas(&BTreeMap::from([
            ("node0".to_owned(), stored(42)),
            ("node1".to_owned(), stored(7)),
        ]));

        assert_eq!(client.nodes_usage.usage(0).get(), 42);
        assert_eq!(client.nodes_usage.usage(1).get(), 7);
    }

    #[test]
    fn load_quotas_seeds_the_period_alongside_the_counter() {
        // Usage without its period would look like a month that never turns, so
        // the rollover would never fire for a restored node.
        let client = RpcClient::new(nodes(1)).unwrap();

        client.load_quotas(&BTreeMap::from([("node0".to_owned(), stored(42))]));

        assert_eq!(
            lock_periods(&client.periods).get("node0").copied(),
            Some(time::macros::date!(2026 - 08 - 01))
        );
    }

    #[test]
    fn load_quotas_matches_by_name_not_by_position() {
        // Why the file is keyed by name at all: reordering the config must not
        // hand a node the spend of whoever used to sit at that index.
        let mut reordered = nodes(2);
        reordered.reverse(); // node1 now takes id 0

        let client = RpcClient::new(reordered).unwrap();

        client.load_quotas(&BTreeMap::from([("node1".to_owned(), stored(42))]));

        assert_eq!(
            client.nodes_usage.usage(0).get(),
            42,
            "node1 kept its usage"
        );
        assert_eq!(client.nodes_usage.usage(1).get(), 0);
    }

    #[test]
    fn load_quotas_ignores_entries_and_nodes_that_do_not_pair_up() {
        // A node dropped from the config keeps its entry in the file until the
        // next flush; a node added to the config has no entry yet. Neither may
        // disturb the other's counter.
        let client = RpcClient::new(nodes(2)).unwrap();

        client.load_quotas(&BTreeMap::from([
            ("node1".to_owned(), stored(42)),
            ("retired-node".to_owned(), stored(9)),
        ]));

        assert_eq!(
            client.nodes_usage.usage(0).get(),
            0,
            "node0 is new to the config"
        );
        assert_eq!(client.nodes_usage.usage(1).get(), 42);
        assert!(
            lock_periods(&client.periods).get("retired-node").is_none(),
            "an entry with no node must not seed a period either"
        );
    }

    #[tokio::test]
    async fn reload_keeps_a_surviving_node_its_usage() {
        let client = RpcClient::new(named(&["a", "b"])).unwrap();
        client.nodes_usage.usage(slot_of(&client, "a")).add(42);

        // "a" moves to the back of the config: its slot has to follow the name,
        // not the position, or it would start spending "b"'s quota.
        client.reload(named(&["b", "a"])).await.unwrap();

        assert_eq!(usage_of(&client, "a"), 42);
        assert_eq!(usage_of(&client, "b"), 0);
    }

    #[tokio::test]
    async fn reload_zeroes_the_slot_a_new_node_inherits() {
        // "a" leaves and "c" arrives, so "c" is handed the slot "a" was
        // spending on. Without the reset it would start the month 42 in.
        let client = RpcClient::new(named(&["a", "b"])).unwrap();
        let freed = slot_of(&client, "a");
        client.nodes_usage.usage(freed).add(42);

        client.reload(named(&["b", "c"])).await.unwrap();

        assert_eq!(slot_of(&client, "c"), freed, "the freed slot is recycled");
        assert_eq!(usage_of(&client, "c"), 0, "and cleared before reuse");
    }

    #[tokio::test]
    async fn reload_with_duplicate_names_changes_nothing() {
        // Two nodes under one name would share a counter and collapse into a
        // single entry in the usage file. Rejecting the set has to leave the
        // running one exactly as it was.
        let client = RpcClient::new(named(&["a"])).unwrap();
        let before = client.topology.load_full();

        assert!(client.reload(named(&["dup", "dup"])).await.is_err());

        assert!(Arc::ptr_eq(&before, &client.topology.load_full()));
    }

    #[tokio::test]
    async fn reload_past_the_slot_limit_changes_nothing() {
        let client = RpcClient::new(named(&["a"])).unwrap();
        let before = client.topology.load_full();

        assert!(client.reload(nodes(MAX_NODES + 1)).await.is_err());

        assert!(Arc::ptr_eq(&before, &client.topology.load_full()));
    }
}
