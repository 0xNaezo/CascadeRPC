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
            // temporarily skip methods not in the enum; they will be added later
            if let Some(id) = get_standard_method_id(name.as_bytes())
            // guard against unknown methods and out-of-bounds indices
                && id != 0
                && id < costs.len()
            {
                costs[id] = price;
            }

            // store unknown methods
        }

        Self { costs }
    }

    #[inline]
    #[must_use]
    pub const fn cost(&self, method: RpcMethod) -> u32 {
        self.costs[method as usize]
    }
}
