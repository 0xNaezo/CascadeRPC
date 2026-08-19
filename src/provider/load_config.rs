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
#[allow(clippy::unwrap_used, clippy::panic)]
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
    #[serial]
    fn env_var_substituted_from_environment() {
        // CARGO_PKG_NAME is always set by cargo during tests (= crate name).
        assert_eq!(resolve_env("$CARGO_PKG_NAME").unwrap(), "rpc-load-balancer");
    }

    #[test]
    #[serial]
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
    #[serial]
    fn missing_env_var_returns_error() {
        assert!(
            resolve_env("$RPC_LB_TEST_DEFINITELY_UNSET_VAR_XYZ123").is_err(),
            "missing env var should error"
        );
    }

    // -----------------------------------------------------------------------
    // `Settings::load` and `build_nodes`
    //
    // `load` reads `CONFIG_PATH`, so every test below touches process-global
    // state and runs under `#[serial]`. That lock is also what makes the
    // `set_var` calls sound: edition 2024 marks them unsafe precisely because
    // a concurrent `getenv` in another thread would be a data race.
    // -----------------------------------------------------------------------

    use serial_test::serial;
    use std::path::{Path, PathBuf};

    use super::{ConfigNode, Settings, build_nodes};
    use crate::protocol::methods::RpcMethod;
    use crate::protocol::registry::CUSTOM_METHODS;

    /// Absolute, so a test does not depend on the directory cargo runs it from.
    const PRICING_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/config/provider_config");

    /// Scratch directory unique to one test run, removed even on panic.
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

        /// Writes `body` as the config file and points `CONFIG_PATH` at it.
        fn config(&self, body: &str) -> PathBuf {
            let path = self.0.join("config.toml");
            std::fs::write(&path, body).unwrap();
            set_config_path(&path);

            path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).ok();
        }
    }

    fn set_config_path(path: &Path) {
        // SAFETY: every test that reads or writes the environment is `#[serial]`,
        // so no other thread is inside `getenv`/`setenv` while this runs.
        unsafe { std::env::set_var("CONFIG_PATH", path) };
    }

    /// A whole config file: fixed `[server]`, plus the node keys given.
    fn config_with(node_keys: &str) -> String {
        format!("[server]\nhost = \"127.0.0.1\"\nport = 3000\n\n{node_keys}")
    }

    /// Every node key except `monthly_limit`, which each test sets itself.
    fn node_base(url: &str) -> String {
        format!(
            "[[nodes]]\nname = \"n1\"\nurl = \"{url}\"\ntier = 0\nrps_limit = 1\n\
             max_concurrent = 1\nprovider_pricing_path = \"{PRICING_DIR}/helius.toml\"\n"
        )
    }

    /// Node keys with a `monthly_limit` and anything else the test needs.
    fn node(monthly_limit: u64, extra: &str) -> String {
        format!(
            "{}monthly_limit = {monthly_limit}\n{extra}",
            node_base("http://127.0.0.1:1")
        )
    }

    /// A `ConfigNode` pointing at a real provider file, for the `build_nodes`
    /// tests that skip the file-reading half.
    fn config_node(name: &str, pricing_file: &str) -> ConfigNode {
        ConfigNode {
            name: name.to_owned(),
            url: "http://127.0.0.1:1".to_owned(),
            tier: 0,
            rps_limit: 1,
            max_concurrent: 1,
            monthly_limit: 1000,
            provider_pricing_path: format!("{PRICING_DIR}/{pricing_file}"),
            reset_day: 1,
        }
    }

    #[test]
    #[serial]
    fn zero_monthly_limit_reads_as_unlimited() {
        let dir = TempDir::new("cfg_unlimited");
        dir.config(&config_with(&node(0, "")));

        let settings = Settings::load().unwrap();

        // Not 0: the quota accounting has no separate "no limit" state, so
        // unmetered is spelled as a limit nothing can reach.
        assert_eq!(settings.nodes[0].monthly_limit, u64::MAX);
    }

    #[test]
    #[serial]
    fn a_real_monthly_limit_is_left_alone() {
        let dir = TempDir::new("cfg_limit");
        dir.config(&config_with(&node(1000, "")));

        assert_eq!(Settings::load().unwrap().nodes[0].monthly_limit, 1000);
    }

    #[test]
    #[serial]
    fn reset_day_zero_is_rejected() {
        let dir = TempDir::new("cfg_day_zero");
        dir.config(&config_with(&node(1000, "reset_day = 0\n")));

        // Left through, the node's counter would never reset again.
        assert!(Settings::load().is_err(), "reset_day 0 must be refused");
    }

    #[test]
    #[serial]
    fn reset_day_above_31_is_rejected() {
        let dir = TempDir::new("cfg_day_32");
        dir.config(&config_with(&node(1000, "reset_day = 32\n")));

        assert!(Settings::load().is_err(), "reset_day 32 must be refused");
    }

    #[test]
    #[serial]
    fn reset_day_bounds_are_inclusive() {
        for day in [1, 31] {
            let dir = TempDir::new("cfg_day_bounds");
            dir.config(&config_with(&node(1000, &format!("reset_day = {day}\n"))));

            let settings = Settings::load().unwrap_or_else(|e| panic!("day {day}: {e}"));
            assert_eq!(settings.nodes[0].reset_day, day);
        }
    }

    #[test]
    #[serial]
    fn reset_day_defaults_to_the_first() {
        let dir = TempDir::new("cfg_day_default");
        dir.config(&config_with(&node(1000, "")));

        assert_eq!(Settings::load().unwrap().nodes[0].reset_day, 1);
    }

    #[test]
    #[serial]
    fn enable_metrics_defaults_to_false() {
        let dir = TempDir::new("cfg_metrics_default");
        dir.config(&config_with(&node(1000, "")));

        // The scrape endpoint exposes node names and spend, so its default is
        // the closed one.
        assert!(!Settings::load().unwrap().server.enable_metrics);
    }

    #[test]
    #[serial]
    fn enable_metrics_is_read_when_set() {
        let dir = TempDir::new("cfg_metrics_on");
        dir.config(&format!(
            "[server]\nhost = \"127.0.0.1\"\nport = 3000\nenable_metrics = true\n\n{}",
            node(1000, "")
        ));

        assert!(Settings::load().unwrap().server.enable_metrics);
    }

    #[test]
    #[serial]
    fn a_missing_config_path_env_var_errors() {
        // SAFETY: `#[serial]`, as above.
        unsafe { std::env::remove_var("CONFIG_PATH") };

        assert!(
            Settings::load().is_err(),
            "no CONFIG_PATH must fail, not fall back to a default file"
        );
    }

    #[test]
    #[serial]
    fn a_missing_config_file_errors() {
        let dir = TempDir::new("cfg_missing");
        set_config_path(&dir.0.join("does_not_exist.toml"));

        assert!(Settings::load().is_err());
    }

    #[test]
    #[serial]
    fn a_config_that_does_not_parse_errors() {
        let dir = TempDir::new("cfg_broken");
        dir.config("this is not toml {{{");

        assert!(Settings::load().is_err());
    }

    #[test]
    #[serial]
    fn env_substitution_reaches_the_node_url() {
        let dir = TempDir::new("cfg_env_url");
        // CARGO_PKG_NAME is always set by cargo during tests, so no `set_var`
        // for the substituted variable itself.
        dir.config(&config_with(&format!(
            "{}monthly_limit = 1000\n",
            node_base("http://$CARGO_PKG_NAME.test/rpc")
        )));

        assert_eq!(
            Settings::load().unwrap().nodes[0].url,
            "http://rpc-load-balancer.test/rpc"
        );
    }

    #[test]
    #[serial]
    fn an_unset_url_variable_stops_the_load() {
        let dir = TempDir::new("cfg_env_unset");
        dir.config(&config_with(&format!(
            "{}monthly_limit = 1000\n",
            node_base("http://$RPC_LB_TEST_DEFINITELY_UNSET_XYZ123.test")
        )));

        // Substituting an empty string would produce a URL that fails at
        // request time, far from the cause.
        assert!(Settings::load().is_err());
    }

    #[test]
    fn build_nodes_prices_each_node_from_its_own_file() {
        let nodes = build_nodes(
            vec![
                config_node("helius", "helius.toml"),
                config_node("alchemy", "alchemy.toml"),
            ],
            &CUSTOM_METHODS,
        )
        .unwrap();

        let get_balance = RpcMethod::GetBalance as usize;

        // Same method, two prices: the tables did not get crossed.
        assert_eq!(nodes[0].method_costs.cost(get_balance), 1);
        assert_eq!(nodes[1].method_costs.cost(get_balance), 10);
    }

    #[test]
    fn build_nodes_carries_the_spillover_percent_from_the_pricing_file() {
        let nodes =
            build_nodes(vec![config_node("helius", "helius.toml")], &CUSTOM_METHODS).unwrap();

        // helius.toml declares 95%, over a monthly_limit of 1000.
        assert_eq!(nodes[0].spillover_threshold, 950);
    }

    #[test]
    fn build_nodes_fails_the_whole_set_if_one_pricing_file_is_missing() {
        let result = build_nodes(
            vec![
                config_node("helius", "helius.toml"),
                config_node("ghost", "no_such_provider.toml"),
            ],
            &CUSTOM_METHODS,
        );

        // All or nothing: a half-built set published on reload would drop the
        // nodes after the bad one.
        assert!(result.is_err());
    }

    #[test]
    fn build_nodes_rejects_a_node_the_domain_layer_refuses() {
        let mut bad = config_node("zero-rps", "helius.toml");
        bad.rps_limit = 0;

        assert!(build_nodes(vec![bad], &CUSTOM_METHODS).is_err());
    }
}
