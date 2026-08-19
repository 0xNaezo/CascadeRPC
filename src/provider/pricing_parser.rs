//! Reading a provider's pricing file into the cost table its nodes are served
//! from.
//!
//! One file per provider, named by each node in the balancer config and re-read
//! on every reload, so editing a price list and sending SIGHUP is enough to
//! apply it.

use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result, anyhow};
use config::{Config, File};
use serde::Deserialize;

use crate::protocol::cost_table::{CostSpec, ProviderCostTable};
use crate::protocol::registry::CustomMethods;

const DEFAULT_SPILLOVER_PERCENT: u8 = 95;

/// The optional `[limits]` section of a provider file.
#[derive(Debug, Deserialize)]
struct ProviderLimits {
    /// Share of the node's `monthly_limit` it may spend before the router
    /// starts skipping it, in percent. Left out, [`DEFAULT_SPILLOVER_PERCENT`]
    /// keeps a small reserve.
    #[serde(default = "default_spillover_percent")]
    spillover_percent: u8,
    /// Price for a method neither `[routing]` nor `[custom]` names. Left out,
    /// the node is skipped for those methods instead of guessing what they cost.
    #[serde(default)]
    unknown_method_cost: Option<u32>,
}

const fn default_spillover_percent() -> u8 {
    DEFAULT_SPILLOVER_PERCENT
}

/// One provider pricing file, as parsed.
#[derive(Debug, Deserialize)]
struct ProviderRouting {
    /// `[routing]`: prices for methods the [`crate::protocol::methods`] table
    /// knows. A name it does not know is a typo and is dropped when the table is
    /// built.
    routing: HashMap<String, u32>,
    /// Methods outside the standard set, under whatever names the provider gave
    /// them. Separate from `[routing]` so that section keeps its property: a
    /// name it does not recognize is a typo, not a new method.
    #[serde(default)]
    custom: HashMap<String, u32>,
    #[serde(default)]
    limits: Option<ProviderLimits>,
}

/// Parses a single provider config file into what its methods cost and a
/// `spillover_percent` (1..=100). Missing `[limits]` → `DEFAULT_SPILLOVER_PERCENT`
/// and no fallback price.
///
/// # Errors
///
/// Returns an error if the file cannot be read, parsed, or `spillover_percent`
/// is outside `1..=100`.
fn parse_file(path: &str) -> Result<(CostSpec, u8)> {
    let config = Config::builder()
        .add_source(File::with_name(path).required(true))
        .build()?;

    let parsed: ProviderRouting = config.try_deserialize()?;

    let (spillover_percent, unknown_cost) =
        parsed
            .limits
            .map_or((DEFAULT_SPILLOVER_PERCENT, u32::MAX), |l| {
                (
                    l.spillover_percent,
                    l.unknown_method_cost.unwrap_or(u32::MAX),
                )
            });

    if !(1..=100).contains(&spillover_percent) {
        return Err(anyhow!(
            "spillover_percent must be in 1..=100, got {spillover_percent}"
        ));
    }

    Ok((
        CostSpec {
            routing: parsed.routing,
            custom: parsed.custom,
            unknown_cost,
        },
        spillover_percent,
    ))
}

/// Loads the routing cost table and `spillover_percent` from a single provider
/// config file.
///
/// Custom method names in the file are interned into `methods` on the way
/// through, which is what makes the ids in the returned table the same ids the
/// router resolves requests to — so the registry passed here has to be the one
/// the router reads, `CUSTOM_METHODS`, for every call that is not a test.
///
/// The registry is an argument and not the global because interning is a side
/// effect on the process: parsing a file to look at it would otherwise grow the
/// process-wide table, and every reload would grow it again.
///
/// # Errors
///
/// Returns an error if the file cannot be read or parsed.
pub fn load_from_path(path: &str, methods: &CustomMethods) -> Result<(ProviderCostTable, u8)> {
    let (spec, spillover_percent) = parse_file(path)?;

    Ok((ProviderCostTable::new(&spec, methods), spillover_percent))
}

/// Reads every `*.toml` in `dir` and returns each provider's `[routing]` table,
/// keyed by file stem.
///
/// Not on any runtime path — nodes name their pricing file individually, via
/// [`load_from_path`]. This exists for the crate's own consistency checks over
/// the shipped provider configs, which need every priced name in one place.
///
/// # Errors
///
/// Returns an error if the directory cannot be read or any TOML file fails to
/// parse.
pub fn load_from_dir(dir: &Path) -> Result<HashMap<String, HashMap<String, u32>>> {
    let mut out = HashMap::new();

    for entry in std::fs::read_dir(dir)
        .with_context(|| format!("cannot read provider config dir {}", dir.display()))?
    {
        let path = entry?.path();
        if path.extension().is_none_or(|ext| ext != "toml") {
            continue;
        }

        let provider = path
            .file_stem()
            .context("config file without a name")?
            .to_string_lossy();

        // spillover_percent and the custom table are discarded here: callers of
        // this one want the names that have to resolve to an `RpcMethod`.
        let (spec, _) = parse_file(&path.to_string_lossy())?;
        out.insert(provider.into_owned(), spec.routing);
    }

    Ok(out)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::{load_from_dir, load_from_path};
    use crate::protocol::methods::RpcMethod;
    use crate::protocol::registry::CUSTOM_METHODS;
    use std::path::{Path, PathBuf};

    /// Absolute, so the tests do not depend on the working directory cargo
    /// happens to run them from.
    const CONFIG_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/config/provider_config");

    fn config(file: &str) -> String {
        format!("{CONFIG_DIR}/{file}")
    }

    /// Scratch directory unique to one test run, removed even if the test
    /// panics part-way through.
    struct TempDir(PathBuf);

    impl TempDir {
        fn new(tag: &str) -> Self {
            let unique = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos());
            let dir =
                std::env::temp_dir().join(format!("rpc_lb_{tag}_{}_{unique}", std::process::id()));
            std::fs::create_dir_all(&dir).unwrap();

            Self(dir)
        }

        fn write(&self, name: &str, contents: &str) -> PathBuf {
            let path = self.0.join(name);
            std::fs::write(&path, contents).unwrap();

            path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).ok();
        }
    }

    #[test]
    fn parses_real_provider_configs() {
        let map = load_from_dir(Path::new(CONFIG_DIR)).unwrap();

        assert_eq!(map.len(), 3);
        assert!(map.contains_key("alchemy"));
        assert!(map.contains_key("helius"));
        assert!(map.contains_key("solana"));

        let alchemy = &map["alchemy"];
        assert_eq!(alchemy["getBalance"], 10);
        assert_eq!(alchemy["getLargestAccounts"], 3000);
        assert_eq!(alchemy["sendTransaction"], 20);
    }

    #[test]
    fn parses_single_provider_config() {
        let (table, spillover) = load_from_path(&config("helius.toml"), &CUSTOM_METHODS).unwrap();

        assert_eq!(table.cost(RpcMethod::GetBalance as usize), 1);
        assert_eq!(spillover, 95);
    }

    #[test]
    fn missing_limits_defaults_to_95() {
        // solana.toml has no [limits] section → default 95.
        let (_, spillover) = load_from_path(&config("solana.toml"), &CUSTOM_METHODS).unwrap();
        assert_eq!(spillover, 95);
    }

    #[test]
    fn invalid_spillover_percent_errors() {
        let dir = TempDir::new("spillover_zero");
        let path = dir.write(
            "bad.toml",
            "[limits]\nspillover_percent = 0\n[routing]\ngetBalance = 1\n",
        );

        assert!(load_from_path(path.to_string_lossy().as_ref(), &CUSTOM_METHODS).is_err());
    }

    #[test]
    fn spillover_percent_above_100_errors() {
        let dir = TempDir::new("spillover_over");
        let path = dir.write(
            "bad.toml",
            "[limits]\nspillover_percent = 101\n[routing]\ngetBalance = 1\n",
        );

        assert!(load_from_path(path.to_string_lossy().as_ref(), &CUSTOM_METHODS).is_err());
    }

    #[test]
    fn spillover_percent_bounds_are_inclusive() {
        let dir = TempDir::new("spillover_bounds");

        for percent in [1, 100] {
            let path = dir.write(
                &format!("ok_{percent}.toml"),
                &format!("[limits]\nspillover_percent = {percent}\n[routing]\ngetBalance = 1\n"),
            );
            let (_, parsed) = load_from_path(path.to_string_lossy().as_ref(), &CUSTOM_METHODS)
                .unwrap_or_else(|e| panic!("{percent} should be accepted: {e}"));

            assert_eq!(parsed, percent);
        }
    }

    #[test]
    fn parses_custom_methods_and_the_fallback_price() {
        let dir = TempDir::new("custom");
        let path = dir.write(
            "provider.toml",
            "[limits]\nspillover_percent = 50\nunknown_method_cost = 400\n\
             [routing]\ngetBalance = 1\n\
             [custom]\nparser_test_doSomething = 77\n",
        );

        let (table, spillover) = load_from_path(path.to_string_lossy().as_ref(), &CUSTOM_METHODS).unwrap();

        assert_eq!(spillover, 50);
        assert_eq!(table.cost(RpcMethod::GetBalance as usize), 1);
        assert_eq!(
            table.cost(CUSTOM_METHODS.resolve("parser_test_doSomething")),
            77,
            "the name has to be interned where the router will resolve it"
        );
        assert_eq!(table.cost(RpcMethod::Unknown as usize), 400);
    }

    #[test]
    fn a_file_without_limits_prices_nothing_it_does_not_name() {
        // solana.toml has no [limits], so no fallback: the node is skipped for
        // anything its [routing] table leaves out.
        let (table, _) = load_from_path(&config("solana.toml"), &CUSTOM_METHODS).unwrap();

        assert_eq!(table.cost(RpcMethod::Unknown as usize), u32::MAX);
    }

    #[test]
    fn a_custom_table_does_not_disturb_the_routing_table() {
        // load_from_dir feeds the "every priced name must resolve" check in
        // protocol::methods, which would start failing if custom names leaked
        // into what it returns.
        let dir = TempDir::new("custom_isolated");
        dir.write(
            "provider.toml",
            "[routing]\ngetBalance = 1\n[custom]\nnot_a_real_rpc_method = 2\n",
        );

        let loaded = load_from_dir(&dir.0).unwrap();

        assert_eq!(loaded["provider"].len(), 1);
        assert!(loaded["provider"].contains_key("getBalance"));
    }

    #[test]
    fn missing_file_errors() {
        let missing = config("does_not_exist.toml");

        assert!(load_from_path(&missing, &CUSTOM_METHODS).is_err());
    }
}
