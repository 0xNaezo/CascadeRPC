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
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{load_from_dir, load_from_path};
    use crate::protocol::methods::RpcMethod;
    use std::path::Path;

    #[test]
    fn parses_real_provider_configs() {
        let map = load_from_dir(Path::new("config/provider_config")).unwrap();

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
        let (table, spillover) = load_from_path("config/provider_config/helius.toml").unwrap();

        assert_eq!(table.cost(RpcMethod::GetBalance as usize), 1);
        assert_eq!(spillover, 95);
    }

    #[test]
    fn missing_limits_defaults_to_95() {
        // solana.toml has no [limits] section → default 95.
        let (_, spillover) = load_from_path("config/provider_config/solana.toml").unwrap();
        assert_eq!(spillover, 95);
    }

    #[test]
    fn invalid_spillover_percent_errors() {
        // Write a temp config with spillover_percent out of range.
        let dir = std::env::temp_dir().join("rpc_lb_pricing_test_invalid");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("bad.toml");
        std::fs::write(
            &path,
            "[limits]\nspillover_percent = 0\n[routing]\ngetBalance = 1\n",
        )
        .unwrap();
        let res = load_from_path(path.to_string_lossy().as_ref());
        assert!(res.is_err());
        std::fs::remove_file(&path).ok();
        std::fs::remove_dir(&dir).ok();
    }
}
