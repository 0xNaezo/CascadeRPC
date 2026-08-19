//! The usage file: what every node has spent, and which billing period that
//! spend belongs to.
//!
//! Written once a minute and on shutdown, read once at startup. It exists so a
//! restart does not re-open a monthly quota the provider still considers spent
//! — see [`crate::quotas::period`] for the half that decides when it should be.

use std::collections::BTreeMap;
use std::fs::read_to_string;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::to_vec_pretty;
use std::io::ErrorKind;
use time::Date;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tracing::warn;

use crate::core::node::RpcNode;
use crate::core::rpc::RpcClient;
use crate::quotas::period::{self, PeriodMap, lock_periods};
use crate::quotas::state::GlobalQuotaState;

const FLUSH_PERIOD: Duration = Duration::from_mins(1);
const TEMP_PATH: &str = "quotas_temp.json";
const FINAL_PATH: &str = "quotas.json";

/// One node's line in [`FINAL_PATH`]: what it has spent, and the billing period
/// that spend belongs to.
///
/// The period is stored beside the counter, not derived on read, because it is
/// what makes a monthly reset happen exactly once across a restart: see
/// [`period::rollover_if_new_period`].
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeUsage {
    pub used: u64,
    pub period_start: Date,
}

/// Rolls the billing period over and writes every node's usage counter to
/// [`FINAL_PATH`], once a minute.
///
/// The rollover shares this tick instead of running a loop of its own: it needs
/// no other timer, and pairing the two means a reset and the marker that records
/// it reach the disk together.
///
/// Entries are keyed by node name, not by the counter's array index: a reload
/// can move a node to a different slot, but never to a different name, so the
/// name is the only key that still means the same node on the next start.
///
/// Runs until the task is dropped. A failed flush is logged and retried on the
/// next tick.
pub async fn start_disk_flusher(rpc_client: RpcClient) {
    let mut interval =
        tokio::time::interval_at(tokio::time::Instant::now() + FLUSH_PERIOD, FLUSH_PERIOD);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        interval.tick().await;

        // Before the flush, so a counter this tick zeroes is written out zeroed
        // rather than a minute later.
        period::rollover_if_new_period(&rpc_client, SystemTime::now());

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
    let usage = {
        // Dropped before the write: the flusher is the only writer, but holding
        // it across an await would block a reload's rollover for a disk write.
        let periods = lock_periods(&rpc_client.periods);
        snapshot(&topology.all, &rpc_client.nodes_usage, &periods)
    };

    match to_vec_pretty(&usage) {
        Ok(bytes) => {
            if let Err(e) = write_atomic_async(&bytes).await {
                warn!(error = %e, "quota flush failed");
            }
        }
        Err(e) => warn!(error = %e, "quota serialization failed"),
    }
}

/// Pairs each node's name with its current usage and billing period.
///
/// `BTreeMap` keeps the output ordered by name, so the file stays diffable and
/// hand-editable across flushes.
///
/// A node with no period yet is skipped rather than guessed at: writing a period
/// the rollover has not agreed to would be the one way this file can cause a
/// wrong reset. Every caller rolls over first, so the gap closes on the next
/// tick.
fn snapshot<'a>(
    nodes: &'a [Arc<RpcNode>],
    usage: &GlobalQuotaState,
    periods: &PeriodMap,
) -> BTreeMap<&'a str, NodeUsage> {
    nodes
        .iter()
        .filter_map(|node| {
            let Some(&period_start) = periods.get(&node.name) else {
                warn!(node = %node.name, "no billing period yet; usage not written this tick");
                return None;
            };

            Some((
                node.name.as_str(),
                NodeUsage {
                    used: usage.usage(node.id).get(),
                    period_start,
                },
            ))
        })
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

/// Reads back the usage counters and billing periods written by the last flush.
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
pub fn restore() -> Result<BTreeMap<String, NodeUsage>> {
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

    use time::macros::date;

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
            spillover_percent: 95,
            reset_day: 1,
        })
        .unwrap();
        node.id = id;
        Arc::new(node)
    }

    /// Both nodes in the same period, which is the ordinary case.
    fn periods(names: &[&str], start: Date) -> PeriodMap {
        names
            .iter()
            .map(|name| ((*name).to_owned(), start))
            .collect()
    }

    fn entry(used: u64, period_start: Date) -> NodeUsage {
        NodeUsage { used, period_start }
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
        let august = date!(2026 - 08 - 01);
        let flushed = to_vec_pretty(&snapshot(
            &nodes,
            &usage,
            &periods(&["helius", "quicknode"], august),
        ))
        .unwrap();

        let parsed: BTreeMap<String, NodeUsage> = serde_json::from_slice(&flushed).unwrap();

        assert_eq!(
            parsed,
            BTreeMap::from([
                ("helius".to_owned(), entry(42, august)),
                ("quicknode".to_owned(), entry(7, august)),
            ])
        );
    }

    #[test]
    fn the_period_is_written_as_a_plain_iso_date() {
        // The file is meant to be readable and hand-editable, and an operator
        // recovering from a bad reset edits this field. Pinned because the date
        // format comes from a dependency's serde impl, not from this crate.
        let usage = GlobalQuotaState::default();
        let nodes = [node(0, "helius")];

        let flushed = to_vec_pretty(&snapshot(
            &nodes,
            &usage,
            &periods(&["helius"], date!(2026 - 08 - 15)),
        ))
        .unwrap();

        assert_eq!(
            String::from_utf8(flushed).unwrap(),
            "{\n  \"helius\": {\n    \"used\": 0,\n    \"period_start\": \"2026-08-15\"\n  }\n}"
        );
    }

    #[test]
    fn snapshot_keys_usage_by_node_name() {
        let usage = GlobalQuotaState::default();
        usage.usage(0).add(42);
        usage.usage(1).add(7);

        let nodes = [node(0, "helius"), node(1, "quicknode")];
        let august = date!(2026 - 08 - 01);

        assert_eq!(
            snapshot(&nodes, &usage, &periods(&["helius", "quicknode"], august)),
            BTreeMap::from([
                ("helius", entry(42, august)),
                ("quicknode", entry(7, august))
            ])
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
        let august = date!(2026 - 08 - 01);

        assert_eq!(
            snapshot(
                &reordered,
                &usage,
                &periods(&["helius", "quicknode"], august)
            ),
            BTreeMap::from([
                ("helius", entry(42, august)),
                ("quicknode", entry(7, august))
            ])
        );
    }

    #[test]
    fn snapshot_keeps_each_node_its_own_period() {
        // Two providers with different billing anchors are the reason the period
        // is per entry and not one field for the whole file.
        let usage = GlobalQuotaState::default();
        let nodes = [node(0, "helius"), node(1, "quicknode")];

        let mut mixed = PeriodMap::new();
        mixed.insert("helius".to_owned(), date!(2026 - 08 - 01));
        mixed.insert("quicknode".to_owned(), date!(2026 - 07 - 15));

        assert_eq!(
            snapshot(&nodes, &usage, &mixed),
            BTreeMap::from([
                ("helius", entry(0, date!(2026 - 08 - 01))),
                ("quicknode", entry(0, date!(2026 - 07 - 15))),
            ])
        );
    }

    #[test]
    fn snapshot_skips_a_node_with_no_period_rather_than_inventing_one() {
        let usage = GlobalQuotaState::default();
        usage.usage(0).add(42);

        let nodes = [node(0, "helius"), node(1, "just-arrived")];

        assert_eq!(
            snapshot(&nodes, &usage, &periods(&["helius"], date!(2026 - 08 - 01))),
            BTreeMap::from([("helius", entry(42, date!(2026 - 08 - 01)))])
        );
    }
}
