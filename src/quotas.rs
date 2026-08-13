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
