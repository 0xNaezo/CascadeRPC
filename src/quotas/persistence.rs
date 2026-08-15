use std::collections::BTreeMap;
use std::fs::read_to_string;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use serde_json::to_vec_pretty;
use std::io::ErrorKind;
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

        flush(&rpc_client).await;
    }
}

/// Writes the current usage of every node to [`FINAL_PATH`].
///
/// Failures are logged rather than returned: the periodic caller retries on the
/// next tick, and the shutdown caller has no one left to report to.
pub async fn flush(rpc_client: &RpcClient) {
    // `load_full` rather than `load`: the guard would otherwise be held across
    // the write below.
    let topology = rpc_client.topology.load_full();
    let usage = snapshot(&topology.all, &rpc_client.nodes_usage);

    match to_vec_pretty(&usage) {
        Ok(bytes) => {
            if let Err(e) = write_atomic_async(&bytes).await {
                warn!(error = %e, "quota flush failed");
            }
        }
        Err(e) => warn!(error = %e, "quota serialization failed"),
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

/// Reads back the usage counters written by the last flush.
///
/// [`FINAL_PATH`] is relative to the working directory, same as on the write
/// side: starting the balancer from a different directory starts it from zero.
///
/// A missing file is the normal first start and yields an empty map. A file
/// that exists but cannot be read or parsed is fatal instead: starting from
/// zero would let the balancer spend an already-spent monthly quota a second
/// time, so the operator has to delete the file to say that is what they want.
///
/// # Errors
///
/// Returns an error if [`FINAL_PATH`] exists but cannot be read or parsed.
pub fn restore() -> Result<BTreeMap<String, u64>> {
    let content = match read_to_string(FINAL_PATH) {
        Ok(data) => data,
        Err(err) if err.kind() == ErrorKind::NotFound => {
            return Ok(BTreeMap::new());
        }
        Err(err) => return Err(err).with_context(|| format!("cannot read {FINAL_PATH}")),
    };

    serde_json::from_str(&content)
        .with_context(|| format!("{FINAL_PATH} is corrupt; delete it to start from zero usage"))
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
    fn a_flushed_snapshot_parses_back_into_the_same_usage() {
        // The halves of the round trip are written apart: `snapshot` serializes
        // `&str` keys, `restore` deserializes `String` ones. This pins the file
        // format the two of them have to keep agreeing on.
        let usage = GlobalQuotaState::default();
        usage.usage(0).add(42);
        usage.usage(1).add(7);

        let nodes = [node(0, "helius"), node(1, "quicknode")];
        let flushed = to_vec_pretty(&snapshot(&nodes, &usage)).unwrap();

        let parsed: BTreeMap<String, u64> = serde_json::from_slice(&flushed).unwrap();

        assert_eq!(
            parsed,
            BTreeMap::from([("helius".to_owned(), 42), ("quicknode".to_owned(), 7)])
        );
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
