use std::env;

use anyhow::{Result, anyhow};
use config::{Config, ConfigError, File};
use serde::Deserialize;
use tracing::info;

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    pub server: ServerSettings,
    pub nodes: Vec<ConfigNode>,
}

impl Settings {
    /// Load settings from `CONFIG_PATH` (default `config/config.toml`).
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read or parsed.
    pub fn load() -> Result<Self> {
        dotenvy::dotenv().ok();

        info!("Loading config from config/config.toml");

        let config_path = env::var("CONFIG_PATH")?;

        let config = Config::builder()
            .add_source(File::with_name(&config_path).required(true))
            .build()?;

        let mut settings: Self = config.try_deserialize()?;

        for node in &mut settings.nodes {
            node.url = resolve_env(&node.url)?;
        }

        info!(
            "Config loaded: server {}:{} with {} node(s)",
            settings.server.host,
            settings.server.port,
            settings.nodes.len(),
        );

        Ok(settings)
    }
}

const fn default_enable_metrics() -> bool {
    false
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerSettings {
    pub port: u16,
    pub host: String,
    #[serde(default = "default_enable_metrics")]
    pub enable_metrics: bool,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ConfigNode {
    pub name: String,
    pub url: String,
    pub tier: u8,
    pub rps_limit: u32,
    pub max_concurrent: usize,
}

fn resolve_env(s: &str) -> Result<String, ConfigError> {
    let mut out = String::new();
    let mut rest = s;
    while let Some(i) = rest.find('$') {
        out.push_str(&rest[..i]);
        rest = &rest[i + 1..];
        let end = rest
            .bytes()
            .position(|b| !b.is_ascii_alphanumeric() && b != b'_')
            .unwrap_or(rest.len());
        if end > 0 {
            let key = &rest[..end];
            out.push_str(&std::env::var(key).map_err(|_| {
                ConfigError::Foreign(anyhow!("env var {key} is required but not set").into())
            })?);
            rest = &rest[end..];
        } else {
            out.push('$');
        }
    }
    out.push_str(rest);
    Ok(out)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::resolve_env;

    #[test]
    fn plain_string_no_dollar_returns_as_is() {
        assert_eq!(
            resolve_env("https://example.com/v1").unwrap(),
            "https://example.com/v1"
        );
    }

    #[test]
    fn empty_string_returns_empty() {
        assert_eq!(resolve_env("").unwrap(), "");
    }

    #[test]
    fn env_var_substituted_from_environment() {
        // CARGO_PKG_NAME is always set by cargo during tests (= crate name).
        assert_eq!(resolve_env("$CARGO_PKG_NAME").unwrap(), "rpc-load-balancer");
    }

    #[test]
    fn env_var_substituted_with_surrounding_text() {
        // CARGO_PKG_VERSION (= "0.1.0") avoids unsafe std::env::set_var races.
        assert_eq!(
            resolve_env("https://api.$CARGO_PKG_VERSION/v1").unwrap(),
            "https://api.0.1.0/v1"
        );
    }

    #[test]
    fn double_dollar_preserved() {
        // $$: neither $ is followed by a valid var-name char, so both pass through.
        assert_eq!(resolve_env("$$").unwrap(), "$$");
    }

    #[test]
    fn trailing_dollar_preserved() {
        assert_eq!(resolve_env("prefix$").unwrap(), "prefix$");
    }

    #[test]
    fn brace_var_not_substituted() {
        // ${VAR}: '{' is not alphanumeric, so '$' has no valid name after it
        // and is pushed literally; '{VAR}' follows as plain text.
        assert_eq!(
            resolve_env("${CARGO_PKG_NAME}").unwrap(),
            "${CARGO_PKG_NAME}"
        );
    }

    #[test]
    fn missing_env_var_returns_error() {
        assert!(
            resolve_env("$RPC_LB_TEST_DEFINITELY_UNSET_VAR_XYZ123").is_err(),
            "missing env var should error"
        );
    }
}
