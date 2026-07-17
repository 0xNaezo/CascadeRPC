use anyhow::anyhow;
use config::{Config, ConfigError, File};
use serde::Deserialize;
use tracing::info;

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    pub server: ServerSettings,
    pub nodes: Vec<ConfigNode>,
}

impl Settings {
    /// Load settings from `config/local.toml`.
    ///
    /// # Errors
    ///
    /// Returns `ConfigError` if the file cannot be read or parsed.
    pub fn load() -> Result<Self, ConfigError> {
        dotenvy::dotenv().ok();

        info!("Loading config from config/config.toml");

        let config = Config::builder()
            .add_source(File::with_name("config/config.toml").required(true))
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

#[derive(Debug, Deserialize, Clone)]
pub struct ServerSettings {
    pub port: u16,
    pub host: String,
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
            out.push_str(
                &std::env::var(key)
                    .map_err(|_| ConfigError::Foreign(anyhow!("env var {key} is required but not set").into()))?,
            );
            rest = &rest[end..];
        } else {
            out.push('$');
        }
    }
    out.push_str(rest);
    Ok(out)
}
