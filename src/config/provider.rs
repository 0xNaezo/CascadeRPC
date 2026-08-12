use std::collections::HashMap;
use std::env;
use std::path::Path;

use anyhow::{Context, Result};
use config::{Config, File};
use serde::Deserialize;

use crate::provider::ProviderCostTable;

#[derive(Debug, Deserialize)]
struct ProviderRouting {
    routing: HashMap<String, u32>,
}

/// Parses a single provider config file into a `method -> cost` map.
///
/// # Errors
///
/// Returns an error if the file cannot be read or parsed.
fn parse_file(path: &str) -> Result<HashMap<String, u32>> {
    let config = Config::builder()
        .add_source(File::with_name(path).required(true))
        .build()?;

    let parsed: ProviderRouting = config.try_deserialize()?;

    Ok(parsed.routing)
}

/// Loads routing cost tables for all providers from `PROVIDER_CONFIG_DIR`
/// (default `config/provider_config`).
///
/// Keys are provider names (file names without `.toml`), values are
/// `method -> cost` maps from each file's `[routing]` table.
///
/// # Errors
///
/// Returns an error if the directory cannot be read or any TOML file fails to parse.
pub fn load_all() -> Result<HashMap<String, HashMap<String, u32>>> {
    let dir =
        env::var("PROVIDER_CONFIG_DIR").unwrap_or_else(|_| "config/provider_config".to_owned());

    load_from_dir(Path::new(&dir))
}

/// Loads the routing cost table from a single provider config file.
///
/// # Errors
///
/// Returns an error if the file cannot be read or parsed.
pub fn load_from_path(path: &str) -> Result<ProviderCostTable> {
    Ok(ProviderCostTable::init(parse_file(path)?))
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

        out.insert(provider.into_owned(), parse_file(&path.to_string_lossy())?);
    }

    Ok(out)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{load_from_dir, load_from_path};
    use crate::structs::provider::RpcMethod;
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
        let table = load_from_path("config/provider_config/helius.toml").unwrap();

        assert_eq!(table.cost(RpcMethod::GetBalance), 1);
    }
}
