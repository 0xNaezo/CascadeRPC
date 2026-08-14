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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    fn table(entries: &[(&str, u32)]) -> ProviderCostTable {
        ProviderCostTable::new(
            entries
                .iter()
                .map(|(name, cost)| ((*name).to_string(), *cost))
                .collect(),
        )
    }

    fn id(name: &str) -> usize {
        get_standard_method_id(name.as_bytes())
    }

    #[test]
    fn default_prices_nothing() {
        // "This node can do nothing" is the safe default: an unpriced method
        // makes the router skip the node rather than send a request it cannot
        // account for.
        let costs = ProviderCostTable::default();

        for slot in 0..RpcMethod::Count as usize {
            assert_eq!(costs.cost(slot), u32::MAX, "slot {slot} should be unpriced");
        }
    }

    #[test]
    fn new_prices_only_the_listed_methods() {
        let costs = table(&[("getBalance", 1), ("getProgramAccounts", 3000)]);

        assert_eq!(costs.cost(id("getBalance")), 1);
        assert_eq!(costs.cost(id("getProgramAccounts")), 3000);
        assert_eq!(costs.cost(id("getTransaction")), u32::MAX);
    }

    #[test]
    fn unknown_method_names_cannot_price_the_unknown_slot() {
        // Every unrecognized name resolves to id 0. Without the guard in
        // `new`, a typo in a provider TOML would price slot 0 and make the
        // node accept *every* unknown method at that price.
        let costs = table(&[("getBalanace", 1), ("totallyMadeUp", 5)]);

        assert_eq!(costs.cost(0), u32::MAX);
        assert_eq!(costs.cost(id("getBalance")), u32::MAX);
    }

    #[test]
    fn explicit_unknown_entry_is_also_ignored() {
        // `Unknown = 1` appears verbatim in config/provider_config/helius.toml.
        let costs = table(&[("Unknown", 1), ("getBalance", 7)]);

        assert_eq!(costs.cost(0), u32::MAX);
        assert_eq!(costs.cost(id("getBalance")), 7);
    }

    #[test]
    fn out_of_range_id_is_unpriced() {
        let costs = table(&[("getBalance", 1)]);

        assert_eq!(costs.cost(RpcMethod::Count as usize), u32::MAX);
        assert_eq!(costs.cost(usize::MAX), u32::MAX);
    }

    #[test]
    fn zero_cost_is_distinct_from_unpriced() {
        // A free method must stay routable; only u32::MAX means "can't serve".
        let costs = table(&[("getHealth", 0)]);

        assert_eq!(costs.cost(id("getHealth")), 0);
    }
}
