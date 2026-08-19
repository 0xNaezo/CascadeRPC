//! The usage counters the router bills against, one per node.
//!
//! A fixed array rather than a map: the request path reads a counter on every
//! admission decision, so the lookup is an index and the storage is laid out to
//! keep two nodes off one cache line.

use std::sync::atomic::{AtomicU64, Ordering};

/// Hard cap on how many nodes one config may describe.
///
/// 64 and not an arbitrary number: the router tracks which nodes a request has
/// already tried in a `u64` bitmask, one bit per slot. `assign_ids` rejects
/// larger configs, which is what keeps that mask and this array in agreement.
pub const MAX_NODES: usize = 64;

/// One node's usage counter, alone on its cache line.
///
/// The padding is the point: without it several nodes' counters share a line,
/// and every quota increment invalidates it for whichever cores are billing the
/// others.
#[repr(align(64))]
pub struct PaddedCounter(pub AtomicU64);

impl PaddedCounter {
    /// Books `val` against this node. The only way the request path may touch a
    /// counter.
    pub fn add(&self, val: u64) {
        self.0.fetch_add(val, Ordering::Relaxed);
    }
    /// Reads the counter. `Relaxed`, so a concurrent `add` may or may not be
    /// included — the quota is a budget, not a ledger, and a request either side
    /// of the threshold is not worth ordering for.
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }

    /// Overwrites the counter, discarding whatever it held.
    ///
    /// Only for seeding usage from disk before the server starts serving. The
    /// request path must use [`PaddedCounter::add`]: a `set` there would drop
    /// every increment that landed between the read and the store.
    pub fn set(&self, val: u64) {
        self.0.store(val, Ordering::Relaxed);
    }
}

/// Every node's counter, indexed by the slot `assign_ids` gave it.
pub struct GlobalQuotaState {
    nodes: [PaddedCounter; MAX_NODES],
}

impl Default for GlobalQuotaState {
    fn default() -> Self {
        Self {
            nodes: std::array::from_fn(|_| PaddedCounter(AtomicU64::new(0))),
        }
    }
}

impl GlobalQuotaState {
    /// Usage counter of the node holding this id.
    ///
    /// Ids come from `assign_ids`, which rejects configs larger than
    /// [`MAX_NODES`], so the index is in range by construction. It also keeps a
    /// slot with the node's *name* across reloads, so a counter follows the node
    /// that spent it rather than the position it happens to sit at.
    #[inline]
    #[must_use]
    pub const fn usage(&self, id: usize) -> &PaddedCounter {
        &self.nodes[id]
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn default_initializes_every_counter_to_zero() {
        let state = GlobalQuotaState::default();

        for id in 0..MAX_NODES {
            assert_eq!(state.usage(id).get(), 0, "slot {id} should start empty");
        }
    }

    #[test]
    fn add_accumulates_per_node() {
        let state = GlobalQuotaState::default();

        state.usage(0).add(10);
        state.usage(0).add(5);
        state.usage(1).add(7);

        assert_eq!(state.usage(0).get(), 15);
        assert_eq!(state.usage(1).get(), 7);
    }

    #[test]
    fn concurrent_adds_lose_nothing() {
        // Relaxed ordering still guarantees atomicity of each fetch_add, so the
        // total is exact even though the interleaving is not.
        let state = GlobalQuotaState::default();

        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    for _ in 0..10_000 {
                        state.usage(0).add(1);
                    }
                });
            }
        });

        assert_eq!(state.usage(0).get(), 80_000);
        assert_eq!(state.usage(1).get(), 0);
    }

    #[test]
    #[should_panic(expected = "index out of bounds")]
    fn usage_past_the_last_slot_panics() {
        // Reachable only if a caller hands out an id `RpcClient::new` never
        // issued; the config-size guard there is what keeps this unreachable.
        let _ = GlobalQuotaState::default().usage(MAX_NODES);
    }

    #[test]
    fn counter_occupies_a_whole_cache_line() {
        // The padding is the point of the type: two nodes' counters must not
        // share a cache line, or every quota increment ping-pongs between cores.
        assert_eq!(std::mem::align_of::<PaddedCounter>(), 64);
        assert_eq!(std::mem::size_of::<PaddedCounter>(), 64);
    }

    #[test]
    fn counters_stay_one_per_cache_line_in_the_array() {
        // Guards the whole point of the array: no packing, no shared lines
        // between two nodes' counters.
        assert_eq!(
            std::mem::size_of::<GlobalQuotaState>(),
            MAX_NODES * 64,
            "counters must not share cache lines"
        );
    }
}
