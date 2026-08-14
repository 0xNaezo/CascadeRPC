use std::collections::HashMap;
use std::env;
use std::path::Path;

use anyhow::{Context, Result, anyhow};
use config::{Config, File};
use serde::Deserialize;

use crate::provider::cost_table::ProviderCostTable;

const DEFAULT_SPILLOVER_PERCENT: u8 = 95;

#[derive(Debug, Deserialize)]
struct ProviderLimits {
    #[serde(default = "default_spillover_percent")]
    spillover_percent: u8,
}

const fn default_spillover_percent() -> u8 {
    DEFAULT_SPILLOVER_PERCENT
}

#[derive(Debug, Deserialize)]
struct ProviderRouting {
    routing: HashMap<String, u32>,
    #[serde(default)]
    limits: Option<ProviderLimits>,
}

/// Parses a single provider config file into a `method -> cost` map and a
/// `spillover_percent` (1..=100). Missing `[limits]` → `DEFAULT_SPILLOVER_PERCENT`.
///
/// # Errors
///
/// Returns an error if the file cannot be read, parsed, or `spillover_percent`
/// is outside `1..=100`.
fn parse_file(path: &str) -> Result<(HashMap<String, u32>, u8)> {
    let config = Config::builder()
        .add_source(File::with_name(path).required(true))
        .build()?;

    let parsed: ProviderRouting = config.try_deserialize()?;

    let spillover_percent = parsed
        .limits
        .map_or(DEFAULT_SPILLOVER_PERCENT, |l| l.spillover_percent);

    if !(1..=100).contains(&spillover_percent) {
        return Err(anyhow!(
            "spillover_percent must be in 1..=100, got {spillover_percent}"
        ));
    }

    Ok((parsed.routing, spillover_percent))
}

/// Loads routing cost tables for all providers from `PROVIDER_CONFIG_DIR`
/// (required).
///
/// Keys are provider names (file names without `.toml`), values are
/// `method -> cost` maps from each file's `[routing]` table.
///
/// # Errors
///
/// Returns an error if the directory cannot be read or any TOML file fails to parse.
pub fn load_all() -> Result<HashMap<String, HashMap<String, u32>>> {
    let dir = env::var("PROVIDER_CONFIG_DIR")?;

    load_from_dir(Path::new(&dir))
}

/// Loads the routing cost table and `spillover_percent` from a single provider
/// config file.
///
/// # Errors
///
/// Returns an error if the file cannot be read or parsed.
pub fn load_from_path(path: &str) -> Result<(ProviderCostTable, u8)> {
    let (routing, spillover_percent) = parse_file(path)?;
    Ok((ProviderCostTable::new(routing), spillover_percent))
}

/// Loads routing cost tables from all `*.toml` files in `dir`.
///
/// # Errors
///
/// Returns an error if the directory cannot be read or any TOML file fails to parse.
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

        // spillover_percent discarded here.
        let (routing, _) = parse_file(&path.to_string_lossy())?;
        out.insert(provider.into_owned(), routing);
    }

    Ok(out)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::{load_from_dir, load_from_path};
    use crate::protocol::methods::RpcMethod;
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
        let (table, spillover) = load_from_path(&config("helius.toml")).unwrap();

        assert_eq!(table.cost(RpcMethod::GetBalance as usize), 1);
        assert_eq!(spillover, 95);
    }

    #[test]
    fn missing_limits_defaults_to_95() {
        // solana.toml has no [limits] section → default 95.
        let (_, spillover) = load_from_path(&config("solana.toml")).unwrap();
        assert_eq!(spillover, 95);
    }

    #[test]
    fn invalid_spillover_percent_errors() {
        let dir = TempDir::new("spillover_zero");
        let path = dir.write(
            "bad.toml",
            "[limits]\nspillover_percent = 0\n[routing]\ngetBalance = 1\n",
        );

        assert!(load_from_path(path.to_string_lossy().as_ref()).is_err());
    }

    #[test]
    fn spillover_percent_above_100_errors() {
        let dir = TempDir::new("spillover_over");
        let path = dir.write(
            "bad.toml",
            "[limits]\nspillover_percent = 101\n[routing]\ngetBalance = 1\n",
        );

        assert!(load_from_path(path.to_string_lossy().as_ref()).is_err());
    }

    #[test]
    fn spillover_percent_bounds_are_inclusive() {
        let dir = TempDir::new("spillover_bounds");

        for percent in [1, 100] {
            let path = dir.write(
                &format!("ok_{percent}.toml"),
                &format!("[limits]\nspillover_percent = {percent}\n[routing]\ngetBalance = 1\n"),
            );
            let (_, parsed) = load_from_path(path.to_string_lossy().as_ref())
                .unwrap_or_else(|e| panic!("{percent} should be accepted: {e}"));

            assert_eq!(parsed, percent);
        }
    }

    #[test]
    fn missing_file_errors() {
        let missing = config("does_not_exist.toml");

        assert!(load_from_path(&missing).is_err());
    }
}
