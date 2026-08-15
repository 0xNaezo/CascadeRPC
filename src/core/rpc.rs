use anyhow::Result;
use arc_swap::ArcSwap;
use bytes::Bytes;
use metrics::{Unit, counter, histogram};
use reqwest::{Client, Response, StatusCode};
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};
use tokio::sync::Mutex;
use tracing::{info, warn};
use url::Url;

use crate::{
    core::node::RpcNode,
    quotas::state::{GlobalQuotaState, MAX_NODES},
};

pub struct GaugeGuard(metrics::Gauge);

impl GaugeGuard {
    #[must_use]
    pub fn new(gauge: metrics::Gauge) -> Self {
        gauge.increment(1);
        Self(gauge)
    }
}

impl Drop for GaugeGuard {
    fn drop(&mut self) {
        self.0.decrement(1);
    }
}

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

impl Topology {
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
}

#[derive(Clone)]
pub struct RpcClient {
    pub client: Client,
    pub topology: Arc<ArcSwap<Topology>>,
    pub nodes_usage: Arc<GlobalQuotaState>,
    /// Held by whoever rebuilds [`Self::topology`], so that reading the node
    /// set, working on it and publishing the result is one critical section.
    /// The request path never takes it — it only ever calls `topology.load()`.
    pub(crate) topology_lock: Arc<Mutex<()>>,
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
fn assign_ids(prev: &[Arc<RpcNode>], new: &mut [RpcNode], usage: &GlobalQuotaState) -> Result<()> {
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

impl RpcClient {
    /// Builds a new [`RpcClient`] with a 2-second HTTP timeout.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying HTTP client cannot be initialized
    /// (e.g. TLS backend failure).
    pub fn new(mut nodes: Vec<RpcNode>) -> Result<Self> {
        let client = Client::builder().timeout(Duration::new(2, 0)).build()?;

        let nodes_usage = Arc::new(GlobalQuotaState::default());

        assign_ids(&[], &mut nodes, &nodes_usage)?;

        let all: Vec<Arc<RpcNode>> = nodes.into_iter().map(Arc::new).collect();

        Ok(Self {
            client,
            topology: Arc::new(ArcSwap::from_pointee(Topology {
                active: Topology::rank(&all),
                all,
            })),
            nodes_usage,
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
    /// Returns an error if the new set is larger than [`MAX_NODES`] or two of
    /// its nodes share a name.
    pub async fn reload(&self, mut nodes: Vec<RpcNode>) -> Result<()> {
        let _guard = self.topology_lock.lock().await;

        assign_ids(&self.topology.load().all, &mut nodes, &self.nodes_usage)?;

        let all: Vec<Arc<RpcNode>> = nodes.into_iter().map(Arc::new).collect();

        self.topology.store(Arc::new(Topology {
            active: Topology::rank(&all),
            all,
        }));

        Ok(())
    }

    /// Seeds the usage counters from a previous run's flush.
    ///
    /// Entries are matched by node name, the same key the flusher writes, so a
    /// reordered config still restores each node its own usage. A name with no
    /// node is dropped (it left the config) and a node with no entry keeps its
    /// zero (it is new to the config).
    ///
    /// Overwrites the counters, so it must run before the healthcheck loop, the
    /// flusher and the server are started.
    pub fn load_quotas(&self, quotas: &BTreeMap<String, u64>) {
        for node in &self.topology.load().all {
            if let Some(&quota) = quotas.get(&node.name) {
                self.nodes_usage.usage(node.id).set(quota);
            }
        }
    }

    pub fn record_upstream(node: &RpcNode, outcome: &'static str, duration_seconds: f64) {
        counter!(
            description: "Attempts sent to upstream RPC nodes",
            "rpc_upstream_attempts",
            "node" => node.name.clone(),
            "outcome" => outcome,
        )
        .increment(1);
        histogram!(
            description: "Upstream RPC attempt duration",
            unit: Unit::Seconds,
            "rpc_upstream_duration",
            "node" => node.name.clone(),
            "outcome" => outcome,
        )
        .record(duration_seconds);
    }

    /// Sends a JSON-RPC request to the given URL.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP request fails.
    pub async fn send_request(client: Client, body: Bytes, url: Url) -> Result<Response> {
        let result = client
            .post(url)
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await?;

        Ok(result)
    }

    #[must_use]
    pub fn is_retryable_error(error_code: StatusCode) -> bool {
        let http_status = error_code.as_u16();
        let not_retryable = [
            400, 402, 404, 405, 406, 407, 408, 409, 410, 411, 412, 413, 414, 415, 416, 417, 418,
            421, 422, 423, 424, 425, 426, 428, 431, 451,
        ];

        if not_retryable.contains(&http_status) {
            return false;
        }

        true
    }

    #[must_use]
    pub fn is_retryable_json_rpc_error(error_code: i32) -> bool {
        let not_retryable = [-32700, -32601, -32602, -32600];

        if not_retryable.contains(&error_code) {
            return false;
        }

        true
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::core::node::NewNode;
    use crate::provider::cost_table::ProviderCostTable;

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
                    billing_type: "credits".into(),
                    spillover_percent: 100,
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

    fn arced(nodes: Vec<RpcNode>) -> Vec<Arc<RpcNode>> {
        nodes.into_iter().map(Arc::new).collect()
    }

    fn names_of(nodes: &[Arc<RpcNode>]) -> Vec<&str> {
        nodes.iter().map(|node| node.name.as_str()).collect()
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

    #[test]
    fn load_quotas_seeds_each_node_from_its_own_entry() {
        let client = RpcClient::new(nodes(2)).unwrap();

        client.load_quotas(&BTreeMap::from([
            ("node0".to_owned(), 42),
            ("node1".to_owned(), 7),
        ]));

        assert_eq!(client.nodes_usage.usage(0).get(), 42);
        assert_eq!(client.nodes_usage.usage(1).get(), 7);
    }

    #[test]
    fn load_quotas_matches_by_name_not_by_position() {
        // Why the file is keyed by name at all: reordering the config must not
        // hand a node the spend of whoever used to sit at that index.
        let mut reordered = nodes(2);
        reordered.reverse(); // node1 now takes id 0

        let client = RpcClient::new(reordered).unwrap();

        client.load_quotas(&BTreeMap::from([("node1".to_owned(), 42)]));

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
            ("node1".to_owned(), 42),
            ("retired-node".to_owned(), 9),
        ]));

        assert_eq!(
            client.nodes_usage.usage(0).get(),
            0,
            "node0 is new to the config"
        );
        assert_eq!(client.nodes_usage.usage(1).get(), 42);
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
    fn retryable_error_true_for_5xx_and_429() {
        assert!(RpcClient::is_retryable_error(StatusCode::TOO_MANY_REQUESTS));
        assert!(RpcClient::is_retryable_error(
            StatusCode::INTERNAL_SERVER_ERROR
        ));
        assert!(RpcClient::is_retryable_error(StatusCode::BAD_GATEWAY));
        assert!(RpcClient::is_retryable_error(
            StatusCode::SERVICE_UNAVAILABLE
        ));
        assert!(RpcClient::is_retryable_error(StatusCode::GATEWAY_TIMEOUT));
    }

    #[test]
    fn retryable_error_true_for_success() {
        assert!(RpcClient::is_retryable_error(StatusCode::OK));
    }

    #[test]
    fn retryable_error_false_for_not_retryable_list() {
        let not_retryable = [
            400, 402, 404, 405, 406, 407, 408, 409, 410, 411, 412, 413, 414, 415, 416, 417, 418,
            421, 422, 423, 424, 425, 426, 428, 431, 451,
        ];
        for code in not_retryable {
            assert!(
                !RpcClient::is_retryable_error(StatusCode::from_u16(code).unwrap()),
                "expected {code} to be not-retryable"
            );
        }
    }

    #[test]
    fn retryable_jsonrpc_false_for_not_retryable_codes() {
        for code in [-32700, -32601, -32602, -32600] {
            assert!(
                !RpcClient::is_retryable_json_rpc_error(code),
                "expected {code} to be not-retryable"
            );
        }
    }

    #[test]
    fn retryable_jsonrpc_true_for_server_internal_and_zero() {
        assert!(RpcClient::is_retryable_json_rpc_error(-32000)); // server error
        assert!(RpcClient::is_retryable_json_rpc_error(-32603)); // internal error
        assert!(RpcClient::is_retryable_json_rpc_error(0)); // no error
    }
}
