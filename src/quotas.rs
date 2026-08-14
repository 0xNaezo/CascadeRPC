use std::{
    collections::HashMap,
    sync::atomic::{AtomicU64, Ordering},
};

#[repr(align(64))] // align to one cache line to avoid False Sharing
pub struct PaddedCounter(pub AtomicU64);

impl PaddedCounter {
    pub fn add(&self, val: u64) {
        self.0.fetch_add(val, Ordering::Relaxed);
    }
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

pub struct GlobalQuotaState {
    pub nodes: HashMap<String, PaddedCounter>, // String - Node.name
}

impl GlobalQuotaState {
    #[must_use]
    pub fn new(node_names: Vec<String>) -> Self {
        let nodes: HashMap<String, PaddedCounter> = node_names
            .into_iter()
            .map(|name| (name, PaddedCounter(AtomicU64::new(0))))
            .collect();

        Self { nodes }
    }

    #[must_use]
    pub fn get_node_usage(&self, node_name: &str) -> &PaddedCounter {
        self.nodes.get(node_name).unwrap_or_else(|| {
            unreachable!("nodes_usage built from the same node list as routing table (rpc.rs:49)")
        })
    }

    pub fn add_node_usage(&self, node_name: &str, val: u64) {
        if let Some(counter) = self.nodes.get(node_name) {
            counter.add(val);
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    fn state() -> GlobalQuotaState {
        GlobalQuotaState::new(vec!["A".to_string(), "B".to_string()])
    }

    #[test]
    fn new_initializes_every_counter_to_zero() {
        let state = state();

        assert_eq!(state.nodes.len(), 2);
        assert_eq!(state.get_node_usage("A").get(), 0);
        assert_eq!(state.get_node_usage("B").get(), 0);
    }

    #[test]
    fn add_accumulates_per_node() {
        let state = state();

        state.get_node_usage("A").add(10);
        state.get_node_usage("A").add(5);
        state.add_node_usage("B", 7);

        assert_eq!(state.get_node_usage("A").get(), 15);
        assert_eq!(state.get_node_usage("B").get(), 7);
    }

    #[test]
    fn concurrent_adds_lose_nothing() {
        // Relaxed ordering still guarantees atomicity of each fetch_add, so the
        // total is exact even though the interleaving is not.
        let state = state();

        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    for _ in 0..10_000 {
                        state.get_node_usage("A").add(1);
                    }
                });
            }
        });

        assert_eq!(state.get_node_usage("A").get(), 80_000);
        assert_eq!(state.get_node_usage("B").get(), 0);
    }

    #[test]
    fn add_node_usage_silently_ignores_unknown_nodes() {
        let state = state();

        state.add_node_usage("nope", 100);

        assert_eq!(state.get_node_usage("A").get(), 0);
    }

    #[test]
    #[should_panic(expected = "nodes_usage built from")]
    fn get_node_usage_panics_on_unknown_node() {
        // Asymmetric with `add_node_usage`, which no-ops on the same input.
        // This path is reachable from the request hot path (router.rs admit).
        let _ = state().get_node_usage("nope");
    }

    #[test]
    fn counter_occupies_a_whole_cache_line() {
        // The padding is the point of the type: two nodes' counters must not
        // share a cache line, or every quota increment ping-pongs between cores.
        assert_eq!(std::mem::align_of::<PaddedCounter>(), 64);
        assert_eq!(std::mem::size_of::<PaddedCounter>(), 64);
    }
}
