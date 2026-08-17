//! Ids for method names the [`RpcMethod`] enum does not carry.
//!
//! A provider is free to expose methods outside the Solana RPC spec, and the
//! balancer has to price them like any other — an unpriced method makes the
//! router skip the node. The enum cannot grow to cover them: it is compiled in,
//! and the point of the provider TOMLs is that editing one and sending SIGHUP is
//! enough.
//!
//! So a name that resolves to no variant is interned here at config load and
//! handed an id past [`RpcMethod::Count`]. From there on it indexes the same
//! per-node cost table as a built-in method, and the request path pays one
//! lookup per request for it — the built-in names never touch this module.
//!
//! Ids are append-only. Once handed out, an id means the same method for the
//! rest of the process: a reload only adds names to the end, it never renumbers
//! what is already there. That is what lets the router resolve a name once at
//! the top of a request and keep using the id across retry rounds, even when a
//! SIGHUP republishes every cost table underneath it mid-flight.

use std::sync::LazyLock;

use arc_swap::ArcSwap;

use crate::protocol::methods::{RpcMethod, get_standard_method_id};

/// Multiplier and rotation are `FxHash`'s, the hash rustc interns its own strings
/// with: a few instructions per byte, and nothing to add to `Cargo.toml`.
///
/// It is neither collision resistant nor keyed, and it is invertible by
/// multiplying through by the modular inverse — a matching hash is therefore
/// only ever a *candidate*, which [`CustomMethods::lookup`] confirms against the
/// stored name. Method names arrive in the request body, so without that
/// confirmation a client could mint a name colliding with a cheap method and
/// have an expensive one billed at its price.
const MULTIPLIER: u64 = 0x51_7c_c1_b7_27_22_0a_95;

/// The interning table the balancer runs on.
///
/// Process-global because the ids are: they outlive any one topology and stay
/// valid across every reload, so there is nothing for a per-client instance to
/// scope. Tests that want isolation build their own [`CustomMethods`].
///
/// `LazyLock` rather than a plain `static` because the table owns two `Vec`s
/// behind an `ArcSwap`, and neither can be built in a const initializer.
///
/// ponytail: append-only means a name dropped from every TOML keeps its slot
/// for the life of the process — a few dozen bytes per name ever configured,
/// and reclaiming one would mean renumbering ids that requests are holding. If
/// a deployment ever churns custom method names enough for that to show up,
/// reclaim on restart, not on reload.
pub static CUSTOM_METHODS: LazyLock<CustomMethods> = LazyLock::new(CustomMethods::default);

/// The two halves of one interned entry, kept apart on purpose.
#[derive(Default)]
struct Interned {
    /// Scanned on every lookup, and the only array the scan reads: eight
    /// candidates to a cache line, no pointers to chase.
    hashes: Vec<u64>,
    /// Read only once a hash matched, to tell a real hit from a collision.
    names: Vec<Box<str>>,
}

/// Method names outside [`RpcMethod`], and the ids they were given.
pub struct CustomMethods {
    interned: ArcSwap<Interned>,
}

impl Default for CustomMethods {
    fn default() -> Self {
        Self {
            interned: ArcSwap::from_pointee(Interned::default()),
        }
    }
}

impl CustomMethods {
    /// Interns every name it does not already carry, leaving the ids handed out
    /// earlier exactly where they are.
    ///
    /// Called once per provider file at load, never on the request path.
    pub fn register(&self, names: &[&str]) {
        // `rcu` re-runs the closure if another registration publishes first, so
        // it has to be repeatable: it only ever appends to a fresh copy.
        self.interned.rcu(|current| {
            let mut next = Interned {
                hashes: current.hashes.clone(),
                names: current.names.clone(),
            };

            for name in names {
                // By name, not by hash: two colliding names are two methods and
                // both have to get a slot.
                if !next.names.iter().any(|known| &**known == *name) {
                    next.hashes.push(hash_name(name));
                    next.names.push((*name).into());
                }
            }

            next
        });
    }

    /// Id of an interned name, or `None` if nothing registered it.
    #[must_use]
    pub fn lookup(&self, name: &str) -> Option<usize> {
        let interned = self.interned.load();
        let wanted = hash_name(name);

        // The name comparison is what makes a weak hash safe to key on. It runs
        // only after a hash matched, and a collision just lets the scan carry on
        // to the entry that really is this name.
        interned
            .hashes
            .iter()
            .zip(interned.names.iter())
            .position(|(&hash, known)| hash == wanted && **known == *name)
            .map(|slot| RpcMethod::Count as usize + slot)
    }

    /// Number of slots a cost table needs to hold every id in circulation.
    #[must_use]
    pub fn table_size(&self) -> usize {
        RpcMethod::Count as usize + self.interned.load().hashes.len()
    }

    /// Id of a method name, built-in or interned.
    ///
    /// A name nothing declared resolves to [`RpcMethod::Unknown`], the slot each
    /// node prices with its `unknown_method_cost`.
    #[must_use]
    pub fn resolve(&self, name: &str) -> usize {
        let id = get_standard_method_id(name.as_bytes());

        if id == RpcMethod::Unknown as usize {
            return self.lookup(name).unwrap_or(RpcMethod::Unknown as usize);
        }

        id
    }

    /// Plants an entry under a hash of the caller's choosing.
    ///
    /// Only way to build the collision the name check exists to survive: the
    /// hash is 64 bits wide, so no test is going to stumble on a pair.
    #[cfg(test)]
    fn plant(&self, name: &str, hash: u64) {
        self.interned.rcu(|current| {
            let mut next = Interned {
                hashes: current.hashes.clone(),
                names: current.names.clone(),
            };
            next.hashes.push(hash);
            next.names.push(name.into());

            next
        });
    }
}

fn hash_name(name: &str) -> u64 {
    let mut hash: u64 = 0;

    for &byte in name.as_bytes() {
        hash = (hash.rotate_left(5) ^ u64::from(byte)).wrapping_mul(MULTIPLIER);
    }

    hash
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    const COUNT: usize = RpcMethod::Count as usize;

    fn registry(names: &[&str]) -> CustomMethods {
        let methods = CustomMethods::default();
        methods.register(names);

        methods
    }

    #[test]
    fn a_registered_name_gets_an_id_past_the_builtin_table() {
        // Ids share one space with the enum, so a custom one must start where
        // the enum stops or it would collide with a built-in method's price.
        let methods = registry(&["helius_getFoo"]);

        assert_eq!(methods.lookup("helius_getFoo"), Some(COUNT));
    }

    #[test]
    fn distinct_names_get_distinct_ids() {
        let methods = registry(&["one", "two", "three"]);

        let ids: HashSet<usize> = ["one", "two", "three"]
            .iter()
            .map(|name| methods.lookup(name).expect("registered"))
            .collect();

        assert_eq!(ids.len(), 3);
    }

    #[test]
    fn an_unregistered_name_has_no_id() {
        let methods = registry(&["known"]);

        assert_eq!(methods.lookup("unknown"), None);
        assert_eq!(methods.lookup(""), None);
    }

    #[test]
    fn registering_the_same_name_twice_hands_out_one_id() {
        let methods = registry(&["dup", "dup"]);
        methods.register(&["dup"]);

        assert_eq!(methods.lookup("dup"), Some(COUNT));
        assert_eq!(methods.table_size(), COUNT + 1, "one slot, not three");
    }

    #[test]
    fn later_registrations_never_move_an_earlier_id() {
        // The whole reason the router may resolve an id once and keep using it
        // across retry rounds while a SIGHUP swaps the tables underneath.
        let methods = registry(&["first"]);
        let before = methods.lookup("first");

        methods.register(&["second", "third"]);

        assert_eq!(methods.lookup("first"), before);
    }

    #[test]
    fn a_colliding_name_does_not_inherit_another_methods_id() {
        // The whole reason lookups confirm the name. `FxHash` is invertible, so
        // a client can craft a method name that hashes like a priced one — and
        // method names come straight out of the request body. Priced as that
        // method, an expensive call would bill as a cheap one.
        let methods = CustomMethods::default();
        methods.plant("helius_getFoo", hash_name("attacker_crafted"));

        assert_eq!(methods.lookup("attacker_crafted"), None);
        assert_eq!(
            methods.resolve("attacker_crafted"),
            RpcMethod::Unknown as usize,
            "it falls through to the fallback slot like any unknown name"
        );
    }

    #[test]
    fn the_scan_walks_past_a_collision_to_the_real_entry() {
        // A collision must not shadow the method sitting behind it either: the
        // first hash match is a candidate, not an answer.
        let methods = CustomMethods::default();
        methods.plant("collides_with_it", hash_name("helius_getFoo"));
        methods.register(&["helius_getFoo"]);

        assert_eq!(methods.lookup("helius_getFoo"), Some(COUNT + 1));
    }

    #[test]
    fn table_size_covers_every_id_handed_out() {
        // `ProviderCostTable` allocates this many slots; an id at or past the
        // end would silently read as unpriced.
        let methods = registry(&["a", "b"]);

        for name in ["a", "b"] {
            assert!(methods.lookup(name).expect("registered") < methods.table_size());
        }
    }

    #[test]
    fn resolve_answers_from_the_builtin_table_first() {
        // A provider listing `getBalance` under `[custom]` must not shadow the
        // enum: the built-in id is what every other node's table is keyed by.
        let methods = registry(&["getBalance"]);

        assert_eq!(
            methods.resolve("getBalance"),
            RpcMethod::GetBalance as usize
        );
    }

    #[test]
    fn resolve_finds_interned_names() {
        let methods = registry(&["helius_getFoo"]);

        assert_eq!(methods.resolve("helius_getFoo"), COUNT);
    }

    #[test]
    fn resolve_falls_back_to_unknown() {
        // Not an error: slot 0 is where a node's `unknown_method_cost` lives, so
        // a node may still decide to serve the request.
        let methods = registry(&["helius_getFoo"]);

        assert_eq!(
            methods.resolve("nobody_declared_this"),
            RpcMethod::Unknown as usize
        );
    }

    #[test]
    fn similar_names_hash_apart() {
        // Real method names share long prefixes and lengths, which is the input
        // a weak hash is worst at. Not a correctness guarantee — the name check
        // is — but a scan where everything collides would be a linear memcmp.
        let names = [
            "getAssetsByOwner",
            "getAssetsByCreator",
            "getAssetsByAuthority",
            "getAssetsByGroup",
            "helius_getFoo",
            "helius_getFop",
        ];

        let hashes: HashSet<u64> = names.iter().map(|name| hash_name(name)).collect();

        assert_eq!(hashes.len(), names.len());
    }

    #[test]
    fn the_empty_name_hashes_to_zero_and_still_round_trips() {
        // Nothing stops a TOML from carrying `"" = 5`; it must not alias.
        let methods = registry(&[""]);

        assert_eq!(hash_name(""), 0);
        assert_eq!(methods.lookup(""), Some(COUNT));
        assert_eq!(methods.lookup("other"), None);
    }
}
