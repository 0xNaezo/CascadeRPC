use std::collections::HashMap;

use crate::protocol::methods::{RpcMethod, get_standard_method_id};

#[derive(Clone)]
pub struct ProviderCostTable {
    costs: [u32; RpcMethod::Count as usize],
}

impl Default for ProviderCostTable {
    fn default() -> Self {
        Self {
            costs: [u32::MAX; RpcMethod::Count as usize],
        }
    }
}

impl ProviderCostTable {
    #[must_use]
    pub fn new(config_methods: HashMap<String, u32>) -> Self {
        let mut costs = [u32::MAX; RpcMethod::Count as usize];

        for (name, price) in config_methods {
            // guard against overwriting the Unknown slot
            let id = get_standard_method_id(name.as_bytes());
            if id != 0 {
                costs[id] = price;
            }
        }

        Self { costs }
    }

    #[inline]
    #[must_use]
    pub const fn cost(&self, id: usize) -> u32 {
        // stub for custom methods not in the enum — they resolve to the
        // Unknown slot (0), which is never priced and stays u32::MAX ("node can't
        // do it"). Add a HashMap<String, u32> branch here when custom methods land.
        if id < self.costs.len() {
            self.costs[id]
        } else {
            u32::MAX
        }
    }
}
