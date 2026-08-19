//! What one provider charges for each method, as a flat array the router can
//! index on the request path.
//!
//! The TOML is read into a [`CostSpec`] and compiled once into a
//! [`ProviderCostTable`] per node; from then on pricing a request is a single
//! bounds-checked load.

use std::collections::HashMap;

use crate::protocol::methods::{RpcMethod, get_standard_method_id};
use crate::protocol::registry::CustomMethods;

/// Everything one provider's TOML says about what its methods cost.
#[derive(Debug)]
pub struct CostSpec {
    /// `[routing]`: names that have to resolve to an [`RpcMethod`] variant. One
    /// that does not is a typo and is dropped.
    pub routing: HashMap<String, u32>,
    /// `[custom]`: names the enum does not carry, interned on the way in.
    pub custom: HashMap<String, u32>,
    /// `[limits] unknown_method_cost`: what a method neither table prices costs.
    /// `u32::MAX` means the node cannot serve it at all.
    pub unknown_cost: u32,
}

impl Default for CostSpec {
    /// Prices nothing. Spelled out rather than derived: `u32::default()` is 0,
    /// which would read as "every method on this node is free".
    fn default() -> Self {
        Self {
            routing: HashMap::new(),
            custom: HashMap::new(),
            unknown_cost: u32::MAX,
        }
    }
}

#[derive(Clone)]
pub struct ProviderCostTable {
    /// One slot per method id: the built-in ids up to [`RpcMethod::Count`], then
    /// one for every name interned in [`CustomMethods`]. Slots no name priced
    /// hold the provider's fallback, which defaults to `u32::MAX` — "cannot
    /// serve this".
    costs: Box<[u32]>,
}

impl Default for ProviderCostTable {
    fn default() -> Self {
        Self {
            costs: vec![u32::MAX; RpcMethod::Count as usize].into_boxed_slice(),
        }
    }
}

impl ProviderCostTable {
    /// Builds the table one provider is served from, interning the custom names
    /// it prices so that every node's table agrees on their ids.
    #[must_use]
    pub fn new(spec: &CostSpec, methods: &CustomMethods) -> Self {
        let custom_names: Vec<&str> = spec.custom.keys().map(String::as_str).collect();
        methods.register(&custom_names);

        // Every slot starts at the fallback, so "the operator did not name this
        // method" and "the operator priced everything unnamed at N" end up the
        // same statement. Left unset that fallback is `u32::MAX` and the router
        // skips the node, exactly as it did before custom methods existed.
        let mut costs = vec![spec.unknown_cost; methods.table_size()];

        for (name, price) in &spec.routing {
            let id = get_standard_method_id(name.as_bytes());

            // Slot 0 is the fallback and no name may write to it: an
            // unrecognized name resolves there, so one provider's typo would
            // otherwise set what the node charges for *everything* unnamed.
            if id == RpcMethod::Unknown as usize {
                continue;
            }

            if let Some(slot) = costs.get_mut(id) {
                *slot = *price;
            }
        }

        for (name, price) in &spec.custom {
            // Registered a few lines up, so the id exists and sits inside the
            // table `table_size` was just read for.
            if let Some(slot) = methods.lookup(name).and_then(|id| costs.get_mut(id)) {
                *slot = *price;
            }
        }

        Self {
            costs: costs.into_boxed_slice(),
        }
    }

    /// What this provider charges for a method id.
    ///
    /// An id past the end belongs to a custom method some *other* provider
    /// declared after this table was built. Unpriced here, so the router skips
    /// the node — the same answer as for a slot inside the table that no name
    /// and no fallback priced.
    #[inline]
    #[must_use]
    pub fn cost(&self, id: usize) -> u32 {
        self.costs.get(id).copied().unwrap_or(u32::MAX)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn priced(entries: &[(&str, u32)]) -> HashMap<String, u32> {
        entries
            .iter()
            .map(|(name, cost)| ((*name).to_string(), *cost))
            .collect()
    }

    /// Table over a registry of its own, so one test's custom names cannot
    /// shift another's ids.
    fn table(spec: &CostSpec) -> (ProviderCostTable, CustomMethods) {
        let methods = CustomMethods::default();
        let table = ProviderCostTable::new(spec, &methods);

        (table, methods)
    }

    fn routing_only(entries: &[(&str, u32)]) -> ProviderCostTable {
        table(&CostSpec {
            routing: priced(entries),
            custom: HashMap::new(),
            unknown_cost: u32::MAX,
        })
        .0
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
        let costs = routing_only(&[("getBalance", 1), ("getProgramAccounts", 3000)]);

        assert_eq!(costs.cost(id("getBalance")), 1);
        assert_eq!(costs.cost(id("getProgramAccounts")), 3000);
        assert_eq!(costs.cost(id("getTransaction")), u32::MAX);
    }

    #[test]
    fn unknown_method_names_cannot_price_the_unknown_slot() {
        // Every unrecognized name resolves to id 0. Without the guard in
        // `new`, a typo in a provider TOML would price slot 0 and make the
        // node accept *every* unknown method at that price.
        let costs = routing_only(&[("getBalanace", 1), ("totallyMadeUp", 5)]);

        assert_eq!(costs.cost(0), u32::MAX);
        assert_eq!(costs.cost(id("getBalance")), u32::MAX);
    }

    #[test]
    fn explicit_unknown_entry_is_also_ignored() {
        // `Unknown = 1` appears verbatim in config/provider_config/helius.toml.
        let costs = routing_only(&[("Unknown", 1), ("getBalance", 7)]);

        assert_eq!(costs.cost(0), u32::MAX);
        assert_eq!(costs.cost(id("getBalance")), 7);
    }

    #[test]
    fn out_of_range_id_is_unpriced() {
        let costs = routing_only(&[("getBalance", 1)]);

        assert_eq!(costs.cost(RpcMethod::Count as usize), u32::MAX);
        assert_eq!(costs.cost(usize::MAX), u32::MAX);
    }

    #[test]
    fn zero_cost_is_distinct_from_unpriced() {
        // A free method must stay routable; only u32::MAX means "can't serve".
        let costs = routing_only(&[("getHealth", 0)]);

        assert_eq!(costs.cost(id("getHealth")), 0);
    }

    #[test]
    fn custom_methods_are_priced_at_their_interned_id() {
        let (costs, methods) = table(&CostSpec {
            routing: priced(&[("getBalance", 1)]),
            custom: priced(&[("helius_getFoo", 50)]),
            unknown_cost: u32::MAX,
        });

        let custom_id = methods.lookup("helius_getFoo").expect("interned by `new`");

        assert_eq!(costs.cost(custom_id), 50);
        assert_eq!(costs.cost(id("getBalance")), 1, "built-ins are unaffected");
    }

    #[test]
    fn a_custom_entry_cannot_reprice_a_builtin_name() {
        // `resolve` answers from the enum first, so `[custom]` is not a second
        // way to price a known method — it would intern an id nothing ever
        // resolves to. `[routing]` is where a built-in gets its price.
        let (costs, methods) = table(&CostSpec {
            routing: HashMap::new(),
            custom: priced(&[("getBalance", 9)]),
            unknown_cost: u32::MAX,
        });

        assert_eq!(methods.resolve("getBalance"), id("getBalance"));
        assert_eq!(costs.cost(id("getBalance")), u32::MAX);
    }

    #[test]
    fn the_fallback_backs_every_method_no_name_priced() {
        // What `unknown_method_cost` buys: the node stays routable for methods
        // the operator never enumerated, billed at whatever margin they chose.
        let (costs, _methods) = table(&CostSpec {
            routing: priced(&[("getBalance", 1)]),
            custom: HashMap::new(),
            unknown_cost: 500,
        });

        assert_eq!(costs.cost(id("getBalance")), 1, "named prices still win");
        assert_eq!(costs.cost(RpcMethod::Unknown as usize), 500);
        assert_eq!(
            costs.cost(id("getTransaction")),
            500,
            "a built-in the file never listed falls back too"
        );
    }

    #[test]
    fn a_typo_cannot_raise_the_fallback() {
        // The guard has to hold with a fallback configured, not only against
        // `u32::MAX`: a typo'd name resolves to slot 0 like any other.
        let (costs, _methods) = table(&CostSpec {
            routing: priced(&[("getBalanace", 1)]),
            custom: HashMap::new(),
            unknown_cost: 500,
        });

        assert_eq!(costs.cost(RpcMethod::Unknown as usize), 500);
    }

    #[test]
    fn an_id_from_a_later_registration_reads_as_unpriced() {
        // Tables are sized when they are built. A provider reloaded later can
        // intern a name this table has no slot for, and the router has to skip
        // the node rather than read past the end.
        let methods = CustomMethods::default();
        let costs = ProviderCostTable::new(&CostSpec::default(), &methods);

        methods.register(&["arrived_after_the_table"]);
        let late = methods
            .lookup("arrived_after_the_table")
            .expect("registered");

        assert_eq!(costs.cost(late), u32::MAX);
    }
}
