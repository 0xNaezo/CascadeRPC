use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use serde_json::to_vec_pretty;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tracing::warn;

use crate::core::node::RpcNode;
use crate::core::rpc::RpcClient;
use crate::quotas::state::GlobalQuotaState;

const FLUSH_PERIOD: Duration = Duration::from_mins(1);
const TEMP_PATH: &str = "quotas_temp.json";
const FINAL_PATH: &str = "quotas.json";

/// Periodically writes every node's usage counter to [`FINAL_PATH`].
///
/// Entries are keyed by node name, not by the counter's array index: the index
/// is the node's position in the config, so reordering the TOML would hand a
/// node someone else's usage on the next start.
///
/// Runs until the task is dropped. A failed flush is logged and retried on the
/// next tick.
pub async fn start_disk_flusher(rpc_client: RpcClient) {
    let mut interval =
        tokio::time::interval_at(tokio::time::Instant::now() + FLUSH_PERIOD, FLUSH_PERIOD);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        interval.tick().await;

        let usage = snapshot(&rpc_client.all_nodes, &rpc_client.nodes_usage);

        match to_vec_pretty(&usage) {
            Ok(bytes) => {
                if let Err(e) = write_atomic_async(&bytes).await {
                    warn!(error = %e, "quota flush failed");
                }
            }
            Err(e) => warn!(error = %e, "quota serialization failed"),
        }
    }
}

/// Pairs each node's name with its current usage.
///
/// `BTreeMap` keeps the output ordered by name, so the file stays diffable and
/// hand-editable across flushes.
fn snapshot<'a>(nodes: &'a [Arc<RpcNode>], usage: &GlobalQuotaState) -> BTreeMap<&'a str, u64> {
    nodes
        .iter()
        .map(|node| (node.name.as_str(), usage.usage(node.id).get()))
        .collect()
}

/// Writes to a temp file in the same directory, then renames it over the
/// target, so a reader never observes a half-written file.
async fn write_atomic_async(data: &[u8]) -> Result<()> {
    let mut file = fs::File::create(TEMP_PATH).await?;

    file.write_all(data).await?;
    file.sync_all().await?;

    fs::rename(TEMP_PATH, FINAL_PATH).await?;

    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    use crate::core::node::NewNode;
    use crate::provider::cost_table::ProviderCostTable;

    fn node(id: usize, name: &str) -> Arc<RpcNode> {
        let mut node = RpcNode::new(NewNode {
            name: name.into(),
            url: "http://localhost:9".into(),
            rps_limit: 1,
            max_concurrent: 1,
            tier: 0,
            method_costs: ProviderCostTable::default(),
            monthly_limit: 1000,
            billing_type: "credits".into(),
            spillover_percent: 95,
        })
        .unwrap();
        node.id = id;
        Arc::new(node)
    }

    #[test]
    fn snapshot_keys_usage_by_node_name() {
        let usage = GlobalQuotaState::default();
        usage.usage(0).add(42);
        usage.usage(1).add(7);

        let nodes = [node(0, "helius"), node(1, "quicknode")];

        assert_eq!(
            snapshot(&nodes, &usage),
            BTreeMap::from([("helius", 42), ("quicknode", 7)])
        );
    }

    #[test]
    fn snapshot_follows_the_id_not_the_position() {
        // The whole point of keying by name: a node keeps its own counter even
        // when the config order no longer matches the id it was handed.
        let usage = GlobalQuotaState::default();
        usage.usage(0).add(42);
        usage.usage(1).add(7);

        let reordered = [node(1, "quicknode"), node(0, "helius")];

        assert_eq!(
            snapshot(&reordered, &usage),
            BTreeMap::from([("helius", 42), ("quicknode", 7)])
        );
    }
}
