//! The balancer's own config file: the listener, and the node list every
//! reload rebuilds the topology from.
//!
//! Provider pricing lives in separate files, one per provider, read through
//! [`crate::provider::pricing_parser`] — a node only names the path to its own.

use std::env;

use anyhow::{Result, anyhow};
use config::{Config, ConfigError, File};
use serde::Deserialize;
use tracing::info;

use crate::core::node::{NewNode, RpcNode};
use crate::protocol::registry::CustomMethods;
use crate::provider::pricing_parser;

/// The whole config file, as parsed.
#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    pub server: ServerSettings,
    pub nodes: Vec<ConfigNode>,
}

impl Settings {
    /// Reads the config file named by the `CONFIG_PATH` environment variable
    /// and normalizes every node in it: `$VAR` references in the URL are
    /// resolved from the environment, a `monthly_limit` of 0 becomes "no
    /// limit", and `reset_day` is bounds-checked.
    ///
    /// Also the reload path, so anything checked here is checked again on every
    /// SIGHUP.
    ///
    /// # Errors
    ///
    /// Returns an error if `CONFIG_PATH` is unset, the file cannot be read or
    /// parsed, a URL uses an unsupported `${VAR}` reference or names an unset
    /// environment variable, or a node's `reset_day` is outside `1..=31`.
    pub fn load() -> Result<Self> {
        let config_path = env::var("CONFIG_PATH")?;

        info!("Loading config from {config_path}");

        let config = Config::builder()
            .add_source(File::with_name(&config_path).required(true))
            .build()?;

        let mut settings: Self = config.try_deserialize()?;

        for node in &mut settings.nodes {
            node.url = resolve_env(&node.url)?;

            // 0 reads as "unmetered" in the config; the quota accounting has no
            // separate "no limit" state, so it is spelled as a limit nothing can
            // reach.
            if node.monthly_limit == 0 {
                node.monthly_limit = u64::MAX;
            }

            // Checked here rather than at use: a day outside this range would
            // leave the node's counter unreset for the rest of its life, and a
            // reload runs through here too.
            if !(1..=31).contains(&node.reset_day) {
                anyhow::bail!(
                    "Fatal error: reset_day for node '{}' must be 1..=31, got {}",
                    node.name,
                    node.reset_day
                );
            }
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

/// Builds every node the config describes, pricing tables included.
///
/// Shared by startup and by hot reload, which is why the provider TOMLs are
/// re-read here rather than once at boot: editing a price list and sending
/// SIGHUP has to be enough to apply it.
///
/// Lives on this side of the config boundary, not on [`RpcNode`], so that the
/// node type stays a domain type that reads no files and knows no TOML schema:
/// everything that touches the disk is in `provider`, and `core` only ever sees
/// a finished [`NewNode`].
///
/// `methods` is the registry the custom method names in those files are
/// interned into; every caller outside a test passes
/// [`crate::protocol::registry::CUSTOM_METHODS`], the one the router resolves
/// requests against.
///
/// # Errors
///
/// Returns an error if a pricing file cannot be read or a node is invalid.
/// Nothing is published until the whole set builds, so a bad edit leaves the
/// running configuration alone.
pub fn build_nodes(configs: Vec<ConfigNode>, methods: &CustomMethods) -> Result<Vec<RpcNode>> {
    configs
        .into_iter()
        .map(|n| {
            let (costs, spillover_percent) =
                pricing_parser::load_from_path(&n.provider_pricing_path, methods)?;

            RpcNode::new(NewNode {
                name: n.name,
                url: n.url,
                rps_limit: n.rps_limit,
                max_concurrent: n.max_concurrent,
                tier: n.tier,
                method_costs: costs,
                monthly_limit: n.monthly_limit,
                spillover_percent,
                reset_day: n.reset_day,
            })
        })
        .collect()
}

const fn default_enable_metrics() -> bool {
    false
}

/// What most providers bill on, and the only sane guess for one that does not
/// say: resetting late costs a detour to a lower tier, resetting early
/// overspends a quota the provider still considers spent.
const fn default_reset_day() -> u8 {
    1
}

// `PartialEq` so a reload can tell the operator that the `[server]` section
// they edited is not one of the things a reload can apply.
#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
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
    /// Quota for one billing period, in the unit the provider bills in. 0 means
    /// unmetered and is rewritten to [`u64::MAX`] on load.
    pub monthly_limit: u64,
    /// Path to this node's provider pricing file, re-read on every reload.
    pub provider_pricing_path: String,
    /// Day of the month this provider's quota goes back to zero.
    ///
    /// Per node, because the anchor is the account's, not the protocol's: one
    /// provider bills on the 1st, the next on the day the subscription started.
    /// A day past the end of a short month lands on its last day.
    #[serde(default = "default_reset_day")]
    pub reset_day: u8,
}

/// Substitutes `$VAR` references in a config string from the environment,
/// which is how an API key reaches a node URL without being written to the
/// config file.
///
/// Only the bare `$VAR` form is recognized — a name runs to the first character
/// that is neither alphanumeric nor `_`. `${VAR}` is not a form of it and is
/// passed through unchanged, as is a `$` with no name after it.
///
/// # Errors
///
/// Returns an error if a referenced variable is not set. Substituting an empty
/// string would produce a URL that fails much further from the cause.
fn resolve_env(s: &str) -> Result<String, ConfigError> {
    let mut out = String::new();
    let mut rest = s;
    while let Some(i) = rest.find('$') {
        out.push_str(&rest[..i]);
        rest = &rest[i + 1..];

        // `${VAR}` is the shell syntax most operators reach for first, and it is
        // not the one implemented here. Passing it through as a literal puts the
        // eight characters `${TOKEN}` in the upstream URL and the node answers
        // 401 at request time; refusing the config is the cheaper failure.
        if rest.starts_with('{') {
            return Err(ConfigError::Foreign(
                anyhow!(
                    "unsupported `${{...}}` syntax in '{s}'; write the reference as $VAR instead"
                )
                .into(),
            ));
        }

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
    fn brace_var_is_rejected() {
        // ${VAR} is not supported, and passing it through as a literal would put
        // the braces straight into an upstream URL.
        assert!(
            resolve_env("${CARGO_PKG_NAME}").is_err(),
            "${{...}} should be refused, not passed through"
        );
    }

    #[test]
    fn brace_var_is_rejected_mid_string() {
        assert!(resolve_env("https://api.example.com/${TOKEN}").is_err());
    }

    #[test]
    fn missing_env_var_returns_error() {
        assert!(
            resolve_env("$RPC_LB_TEST_DEFINITELY_UNSET_VAR_XYZ123").is_err(),
            "missing env var should error"
        );
    }
}
