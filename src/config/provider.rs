use std::collections::HashMap;
use std::env;
use std::path::Path;

use anyhow::{Context, Result};
use config::{Config, File};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct ProviderRouting {
    routing: HashMap<String, u64>,
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
pub fn load_all() -> Result<HashMap<String, HashMap<String, u64>>> {
    let dir =
        env::var("PROVIDER_CONFIG_DIR").unwrap_or_else(|_| "config/provider_config".to_owned());

    load_from_dir(Path::new(&dir))
}

/// Loads routing cost tables from all `*.toml` files in `dir`.
///
/// # Errors
///
/// Returns an error if the directory cannot be read or any TOML file fails to parse.
pub fn load_from_dir(dir: &Path) -> Result<HashMap<String, HashMap<String, u64>>> {
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

        let config = Config::builder()
            .add_source(File::with_name(&path.to_string_lossy()).required(true))
            .build()?;

        let parsed: ProviderRouting = config.try_deserialize()?;
        out.insert(provider.into_owned(), parsed.routing);
    }

    Ok(out)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::load_from_dir;
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
}
