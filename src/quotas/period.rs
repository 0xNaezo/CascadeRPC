//! Billing periods: when a node's usage counter goes back to zero.
//!
//! The balancer cannot ask a provider when its billing month rolls over, so it
//! computes the period from the wall clock and the node's `reset_day`, and
//! remembers the period start it last acted on. Comparing the two is the whole
//! mechanism: it makes a reset happen exactly once — not once per tick, and not
//! never after a restart that spanned the boundary.
//!
//! The elapsed-time clocks are all useless here. A monotonic `Instant` dies with
//! the process and does not advance while the balancer is down, which is exactly
//! the case that has to work: down on the 31st at 23:55, up on the 1st at 00:05,
//! quota open again.

use std::collections::BTreeMap;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::SystemTime;

use anyhow::{Context, Result};
use time::{Date, Month, OffsetDateTime, util::days_in_month};
use tracing::{info, warn};

use crate::core::rpc::RpcClient;

/// A period longer than this is a restart after a long outage, or a machine
/// whose clock is simply wrong. Either way the operator wants to hear about it.
const SUSPICIOUS_JUMP_DAYS: i64 = 62;

/// The billing period each node's usage counter belongs to, keyed by node name.
///
/// Keyed by name and not by quota slot for the same reason the usage file is: a
/// reload can hand a node a different slot, but never a different name.
pub type PeriodMap = BTreeMap<String, Date>;

/// Takes the period map, recovering from a poisoned lock instead of failing.
///
/// Nothing that holds this lock can panic — the map is only read, inserted into
/// and pruned — so a poisoned lock means a panic elsewhere in the process.
/// Refusing to track billing periods after that would be the more expensive
/// failure of the two: an unreset counter sends live traffic to a lower tier for
/// the rest of the month.
pub fn lock_periods(periods: &Mutex<PeriodMap>) -> MutexGuard<'_, PeriodMap> {
    periods.lock().unwrap_or_else(PoisonError::into_inner)
}

/// First day of the billing period `today` falls in, for a node that resets on
/// `reset_day`.
///
/// A `reset_day` past the end of the month lands on its last day, which is what
/// the provider bills on too: a node on the 31st resets on Feb 28, or Feb 29 in
/// a leap year.
///
/// UTC throughout. A provider cuts the month in its own timezone, so the two can
/// disagree for up to a day around the boundary; the balancer errs by resetting
/// late rather than early, and late only costs a detour to a lower tier.
///
/// # Errors
///
/// Returns an error if the resulting date does not exist, which no `reset_day`
/// the config accepts (1..=31) can produce.
pub fn period_start(today: Date, reset_day: u8) -> Result<Date> {
    let (year, month) = if today.day() >= clamp_to_month(today.year(), today.month(), reset_day) {
        (today.year(), today.month())
    } else if today.month() == Month::January {
        (today.year() - 1, Month::December)
    } else {
        (today.year(), today.month().previous())
    };

    Date::from_calendar_date(year, month, clamp_to_month(year, month, reset_day))
        .with_context(|| format!("no day {reset_day} in {month} {year}"))
}

/// `reset_day`, pulled back to the last day of the month when it overshoots.
fn clamp_to_month(year: i32, month: Month, reset_day: u8) -> u8 {
    reset_day.min(days_in_month(month, year))
}

/// Zeroes the counter of every node whose billing period has moved on since the
/// period that counter was last credited to.
///
/// Called at startup before the server accepts anything, and on every flush
/// tick. Both call sites matter: a restart that spanned the boundary has to
/// reset before the first request is routed, and a process that stays up has to
/// reset without one.
///
/// Idempotent, because the marker moves together with the counter: a reset lost
/// to a crash before the next flush leaves the old period on disk and simply
/// happens again on the next start.
pub fn rollover_if_new_period(rpc_client: &RpcClient, now: SystemTime) {
    let today = OffsetDateTime::from(now).date();
    let topology = rpc_client.topology.load();
    let mut periods = lock_periods(&rpc_client.periods);

    for node in &topology.all {
        let start = match period_start(today, node.reset_day) {
            Ok(start) => start,
            Err(e) => {
                warn!(node = %node.name, error = %e, "cannot tell which billing period this is; usage kept");
                continue;
            }
        };

        match periods.get(&node.name).copied() {
            // `<=` and not `!=`: a clock that went backwards must not reset a
            // month that was already spent. A container booting before NTP has
            // synced is the ordinary way that happens.
            Some(stored) if start <= stored => {}
            Some(stored) => {
                if (start - stored).whole_days() > SUSPICIOUS_JUMP_DAYS {
                    warn!(
                        node = %node.name, from = %stored, to = %start,
                        "billing period jumped by more than two months: long outage, or a wrong system clock"
                    );
                }

                let usage = rpc_client.nodes_usage.usage(node.id);
                let spent = usage.get();
                usage.set(0);
                periods.insert(node.name.clone(), start);

                info!(node = %node.name, spent, from = %stored, to = %start, "billing period rolled over");
            }
            // First sight of this node — first ever start, or a node a reload
            // added. It adopts the current period and keeps whatever usage it
            // was seeded with; there is no earlier period to compare against.
            None => {
                periods.insert(node.name.clone(), start);
            }
        }
    }

    // A node dropped from the config leaves its marker behind. The usage file is
    // written from the live node set, so this only keeps the map from growing
    // across reloads.
    periods.retain(|name, _| topology.all.iter().any(|node| node.name == *name));
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    use time::macros::{date, datetime};

    use crate::core::node::{NewNode, RpcNode};
    use crate::provider::cost_table::ProviderCostTable;

    fn client(names: &[&str], reset_day: u8) -> RpcClient {
        let nodes = names
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
                    reset_day,
                })
                .unwrap()
            })
            .collect();

        RpcClient::new(nodes).unwrap()
    }

    /// Wall-clock instant, the way the callers get it: `SystemTime`.
    fn at(when: OffsetDateTime) -> SystemTime {
        when.into()
    }

    fn marker(client: &RpcClient, name: &str) -> Option<Date> {
        lock_periods(&client.periods).get(name).copied()
    }

    #[test]
    fn period_starts_this_month_once_the_reset_day_has_passed() {
        assert_eq!(
            period_start(date!(2026 - 08 - 17), 1).unwrap(),
            date!(2026 - 08 - 01)
        );
        assert_eq!(
            period_start(date!(2026 - 08 - 15), 15).unwrap(),
            date!(2026 - 08 - 15)
        );
    }

    #[test]
    fn period_starts_last_month_before_the_reset_day() {
        assert_eq!(
            period_start(date!(2026 - 08 - 14), 15).unwrap(),
            date!(2026 - 07 - 15)
        );
    }

    #[test]
    fn period_crosses_the_year_backwards() {
        // The one case where "previous month" is not just month - 1.
        assert_eq!(
            period_start(date!(2026 - 01 - 05), 15).unwrap(),
            date!(2025 - 12 - 15)
        );
    }

    #[test]
    fn reset_day_past_the_end_of_the_month_lands_on_its_last_day() {
        // A node billed on the 31st still resets in February, and on the 29th in
        // a leap year — the day the provider bills on.
        assert_eq!(
            period_start(date!(2026 - 02 - 28), 31).unwrap(),
            date!(2026 - 02 - 28)
        );
        assert_eq!(
            period_start(date!(2024 - 02 - 29), 31).unwrap(),
            date!(2024 - 02 - 29)
        );
        assert_eq!(
            period_start(date!(2026 - 02 - 27), 31).unwrap(),
            date!(2026 - 01 - 31)
        );
        assert_eq!(
            period_start(date!(2026 - 04 - 29), 31).unwrap(),
            date!(2026 - 03 - 31)
        );
    }

    #[test]
    fn first_sight_of_a_node_adopts_the_period_without_resetting() {
        // A node added by a reload mid-month has no earlier period to compare
        // against; zeroing it here would throw away usage it really has.
        let client = client(&["helius"], 1);
        client.nodes_usage.usage(0).add(42);

        rollover_if_new_period(&client, at(datetime!(2026-08-17 12:00 UTC)));

        assert_eq!(client.nodes_usage.usage(0).get(), 42);
        assert_eq!(marker(&client, "helius"), Some(date!(2026 - 08 - 01)));
    }

    #[test]
    fn a_second_call_in_the_same_period_changes_nothing() {
        let client = client(&["helius"], 1);
        rollover_if_new_period(&client, at(datetime!(2026-08-17 12:00 UTC)));
        client.nodes_usage.usage(0).add(42);

        rollover_if_new_period(&client, at(datetime!(2026-08-31 23:55 UTC)));

        assert_eq!(
            client.nodes_usage.usage(0).get(),
            42,
            "still the same month"
        );
    }

    #[test]
    fn crossing_the_boundary_resets_once() {
        // The restart case, without the restart: down 31.08 23:55, up 01.09
        // 00:05 is the same comparison, and the marker on disk is what carries
        // it across the process boundary.
        let client = client(&["helius"], 1);
        rollover_if_new_period(&client, at(datetime!(2026-08-31 23:55 UTC)));
        client.nodes_usage.usage(0).add(49_900_000);

        rollover_if_new_period(&client, at(datetime!(2026-09-01 00:05 UTC)));

        assert_eq!(client.nodes_usage.usage(0).get(), 0);
        assert_eq!(marker(&client, "helius"), Some(date!(2026 - 09 - 01)));

        // A minute later, the same September tick must not zero what the new
        // month has already spent.
        client.nodes_usage.usage(0).add(7);
        rollover_if_new_period(&client, at(datetime!(2026-09-01 00:06 UTC)));

        assert_eq!(client.nodes_usage.usage(0).get(), 7);
    }

    #[test]
    fn an_outage_spanning_months_resets_once_not_once_per_month() {
        let client = client(&["helius"], 1);
        rollover_if_new_period(&client, at(datetime!(2026-08-05 10:00 UTC)));
        client.nodes_usage.usage(0).add(1_000);

        rollover_if_new_period(&client, at(datetime!(2026-10-02 10:00 UTC)));

        assert_eq!(client.nodes_usage.usage(0).get(), 0);
        assert_eq!(marker(&client, "helius"), Some(date!(2026 - 10 - 01)));
    }

    #[test]
    fn a_clock_that_went_backwards_resets_nothing() {
        // A container that boots before NTP has synced reads a date in the past.
        // Resetting on it would re-open a quota the provider considers spent.
        let client = client(&["helius"], 1);
        rollover_if_new_period(&client, at(datetime!(2026-08-17 12:00 UTC)));
        client.nodes_usage.usage(0).add(42);

        rollover_if_new_period(&client, at(datetime!(2026-07-04 12:00 UTC)));

        assert_eq!(client.nodes_usage.usage(0).get(), 42);
        assert_eq!(
            marker(&client, "helius"),
            Some(date!(2026 - 08 - 01)),
            "the marker must not move backwards either"
        );
    }

    #[test]
    fn nodes_roll_over_on_their_own_reset_days() {
        // Two providers, two billing anchors: resetting a node on a day its
        // provider does not is how the balancer overspends a real quota.
        let mut nodes = vec![];
        for (name, reset_day) in [("first-of-month", 1u8), ("fifteenth", 15u8)] {
            nodes.push(
                RpcNode::new(NewNode {
                    name: name.to_owned(),
                    url: "http://localhost:9".into(),
                    rps_limit: 1,
                    max_concurrent: 1,
                    tier: 0,
                    method_costs: ProviderCostTable::default(),
                    monthly_limit: u64::MAX,
                    spillover_percent: 100,
                    reset_day,
                })
                .unwrap(),
            );
        }
        let client = RpcClient::new(nodes).unwrap();

        rollover_if_new_period(&client, at(datetime!(2026-08-20 12:00 UTC)));
        client.nodes_usage.usage(0).add(10);
        client.nodes_usage.usage(1).add(20);

        rollover_if_new_period(&client, at(datetime!(2026-09-01 12:00 UTC)));

        assert_eq!(client.nodes_usage.usage(0).get(), 0, "resets on the 1st");
        assert_eq!(
            client.nodes_usage.usage(1).get(),
            20,
            "its period runs 15.08 to 15.09"
        );
    }

    #[tokio::test]
    async fn a_node_dropped_from_the_config_stops_being_tracked() {
        let client = client(&["a", "b"], 1);
        rollover_if_new_period(&client, at(datetime!(2026-08-17 12:00 UTC)));

        let mut kept = RpcNode::build_nodes(vec![]).unwrap();
        kept.push(
            RpcNode::new(NewNode {
                name: "a".into(),
                url: "http://localhost:9".into(),
                rps_limit: 1,
                max_concurrent: 1,
                tier: 0,
                method_costs: ProviderCostTable::default(),
                monthly_limit: u64::MAX,
                spillover_percent: 100,
                reset_day: 1,
            })
            .unwrap(),
        );
        client.reload(kept).await.unwrap();

        rollover_if_new_period(&client, at(datetime!(2026-08-18 12:00 UTC)));

        assert!(marker(&client, "a").is_some());
        assert!(marker(&client, "b").is_none(), "stale markers are pruned");
    }
}
