//! The usage file: what every node has spent, and which billing period that
//! spend belongs to.
//!
//! Written once a minute and on shutdown, read once at startup. It exists so a
//! restart does not re-open a monthly quota the provider still considers spent
//! — see [`crate::quotas::period`] for the half that decides when it should be.

use std::collections::BTreeMap;
use std::fs::read_to_string;
use std::sync::Arc;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::to_vec_pretty;
use std::io::ErrorKind;
use time::Date;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tracing::warn;

use crate::core::node::RpcNode;
use crate::quotas::period::PeriodMap;
use crate::quotas::state::GlobalQuotaState;

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

/// Writes one [`snapshot`] to [`FINAL_PATH`].
///
/// Takes the snapshot rather than the client it came from: this module writes a
/// file, and reaching into a live topology to work out what to put in it is the
/// caller's job — see `flush_usage` in `main`. It is also what keeps the whole
/// write path testable without an HTTP client.
///
/// Failures are logged rather than returned: the periodic caller retries on the
/// next tick, and the shutdown caller has no one left to report to.
pub async fn flush(usage: &BTreeMap<&str, NodeUsage>) {
    match to_vec_pretty(usage) {
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
/// Entries are keyed by node name, not by the counter's array index: a reload
/// can move a node to a different slot, but never to a different name, so the
/// name is the only key that still means the same node on the next start.
///
/// `BTreeMap` keeps the output ordered by name, so the file stays diffable and
/// hand-editable across flushes.
///
/// A node with no period yet is skipped rather than guessed at: writing a period
/// the rollover has not agreed to would be the one way this file can cause a
/// wrong reset. Every caller rolls over first, so the gap closes on the next
/// tick.
pub fn snapshot<'a>(
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
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    use time::macros::date;

    use crate::core::node::NewNode;
    use crate::protocol::cost_table::ProviderCostTable;

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

    // -----------------------------------------------------------------------
    // `flush` and `restore`
    //
    // Both resolve `quotas.json` against the working directory, which is
    // process-global — hence `#[serial]` and a guard that puts the old
    // directory back even when a test panics.
    // -----------------------------------------------------------------------

    use serial_test::serial;
    use std::path::PathBuf;

    /// Moves the process into a scratch directory for the duration of one test.
    struct Scratch {
        previous: PathBuf,
        dir: PathBuf,
    }

    impl Scratch {
        fn new(tag: &str) -> Self {
            let unique = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos());
            let dir =
                std::env::temp_dir().join(format!("rpc_lb_{tag}_{}_{unique}", std::process::id()));
            std::fs::create_dir_all(&dir).unwrap();

            let previous = std::env::current_dir().unwrap();
            std::env::set_current_dir(&dir).unwrap();

            Self { previous, dir }
        }

        fn write_usage_file(&self, contents: &str) {
            std::fs::write(self.dir.join(FINAL_PATH), contents).unwrap();
        }

        fn has(&self, name: &str) -> bool {
            self.dir.join(name).exists()
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            std::env::set_current_dir(&self.previous).ok();
            std::fs::remove_dir_all(&self.dir).ok();
        }
    }

    #[test]
    #[serial]
    fn a_missing_usage_file_restores_an_empty_map() {
        let _scratch = Scratch::new("quota_absent");

        // The ordinary first start: no file, no usage, no error.
        assert!(restore().unwrap().is_empty());
    }

    #[test]
    #[serial]
    fn a_corrupt_usage_file_is_fatal() {
        let scratch = Scratch::new("quota_corrupt");
        scratch.write_usage_file("{ this is not json");

        // Starting from zero here would re-open a monthly quota the provider
        // still bills as spent, so the operator has to delete the file on
        // purpose.
        let error = restore().expect_err("a corrupt usage file must not start from zero");
        assert!(
            format!("{error}").contains(FINAL_PATH),
            "the error must name the file to delete: {error}"
        );
    }

    #[test]
    #[serial]
    fn a_usage_file_with_the_wrong_shape_is_fatal() {
        let scratch = Scratch::new("quota_shape");
        // Valid JSON, wrong type: `used` must be a number.
        scratch.write_usage_file(r#"{"helius":{"used":"lots","period_start":"2026-08-01"}}"#);

        assert!(restore().is_err());
    }

    #[test]
    #[serial]
    fn an_unreadable_usage_file_is_fatal() {
        let scratch = Scratch::new("quota_unreadable");
        // A directory where the file should be: not `NotFound`, so it must take
        // the fatal branch rather than the empty-map one.
        std::fs::create_dir(scratch.dir.join(FINAL_PATH)).unwrap();

        assert!(restore().is_err());
    }

    #[tokio::test]
    #[serial]
    async fn flush_then_restore_returns_the_same_counters() {
        let _scratch = Scratch::new("quota_roundtrip");

        let usage = GlobalQuotaState::default();
        usage.usage(0).add(42);
        usage.usage(1).add(7);

        let nodes = [node(0, "helius"), node(1, "quicknode")];
        let august = date!(2026 - 08 - 01);

        flush(&snapshot(
            &nodes,
            &usage,
            &periods(&["helius", "quicknode"], august),
        ))
        .await;

        // The whole point of the file: a restart resumes the month where it
        // left off.
        assert_eq!(
            restore().unwrap(),
            BTreeMap::from([
                ("helius".to_owned(), entry(42, august)),
                ("quicknode".to_owned(), entry(7, august)),
            ])
        );
    }

    #[tokio::test]
    #[serial]
    async fn a_flush_leaves_no_temp_file_behind() {
        let scratch = Scratch::new("quota_temp");

        let usage = GlobalQuotaState::default();
        let nodes = [node(0, "helius")];

        flush(&snapshot(
            &nodes,
            &usage,
            &periods(&["helius"], date!(2026 - 08 - 01)),
        ))
        .await;

        assert!(scratch.has(FINAL_PATH), "the flush wrote nothing");
        assert!(
            !scratch.has(TEMP_PATH),
            "the rename left the temp file behind"
        );
    }

    #[tokio::test]
    #[serial]
    async fn a_second_flush_replaces_the_first() {
        let _scratch = Scratch::new("quota_overwrite");

        let usage = GlobalQuotaState::default();
        let nodes = [node(0, "helius")];
        let august = date!(2026 - 08 - 01);
        let periods = periods(&["helius"], august);

        usage.usage(0).add(5);
        flush(&snapshot(&nodes, &usage, &periods)).await;

        usage.usage(0).add(5);
        flush(&snapshot(&nodes, &usage, &periods)).await;

        // Truncated, not appended to: the rename replaces the file whole.
        assert_eq!(restore().unwrap()["helius"], entry(10, august));
    }

    #[tokio::test]
    #[serial]
    async fn a_node_with_no_period_is_left_out_of_the_file() {
        let _scratch = Scratch::new("quota_no_period");

        let usage = GlobalQuotaState::default();
        usage.usage(0).add(9);
        usage.usage(1).add(9);

        let nodes = [node(0, "helius"), node(1, "quicknode")];

        flush(&snapshot(
            &nodes,
            &usage,
            &periods(&["helius"], date!(2026 - 08 - 01)),
        ))
        .await;

        // Writing a guessed period is the one way this file can cause a wrong
        // reset, so the node is skipped until the rollover gives it one.
        let restored = restore().unwrap();
        assert!(restored.contains_key("helius"));
        assert!(!restored.contains_key("quicknode"));
    }
}
