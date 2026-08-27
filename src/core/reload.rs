//! Configuration hot reload, driven by SIGHUP.
//!
//! A signal is the whole trigger: it needs no dependency, no port and no
//! authentication, and `docker kill -s HUP` reaches a container just as well as
//! `kill -HUP` reaches a process.

use anyhow::Result;
use tracing::{error, info, warn};

use crate::core::rpc::RpcClient;
use crate::protocol::registry::CUSTOM_METHODS;
use crate::provider::load_config::{ServerSettings, Settings, build_nodes};

/// Rebuilds the node set from disk on every SIGHUP, forever.
///
/// A reload that cannot be applied is logged and dropped — the balancer keeps
/// serving the configuration it already has, rather than dying on a typo in a
/// file the operator can fix and re-signal.
///
/// `startup_server` is carried only to tell the operator that the `[server]`
/// section they edited had no effect; rebinding the listener under live traffic
/// is not what a reload is for.
#[cfg(unix)]
pub async fn watch_sighup(rpc_client: RpcClient, startup_server: ServerSettings) {
    use tokio::signal::unix::{SignalKind, signal};

    let mut sighup = match signal(SignalKind::hangup()) {
        Ok(sighup) => sighup,
        Err(e) => {
            error!(error = %e, "cannot listen for SIGHUP; config hot reload is off");
            return;
        }
    };

    while sighup.recv().await.is_some() {
        info!("SIGHUP received, reloading config");

        match reload_once(&rpc_client, &startup_server).await {
            Ok(nodes) => info!(nodes, "config reloaded"),
            Err(e) => error!(error = %e, "config reload failed; keeping the running config"),
        }
    }
}

/// Reads the config, rebuilds every node from it and republishes the topology.
///
/// # Errors
///
/// Returns an error if the config or a pricing file cannot be read or parsed,
/// or if the resulting node set is rejected. The running topology is untouched
/// in every one of those cases.
#[cfg(unix)]
async fn reload_once(rpc_client: &RpcClient, startup_server: &ServerSettings) -> Result<usize> {
    // Blocking file reads on the runtime: a handful of small TOMLs, once per
    // signal, on a task of its own.
    let settings = Settings::load()?;

    if settings.server != *startup_server {
        warn!("[server] section changed; the listener is only bound at startup, so it is ignored");
    }

    let nodes = build_nodes(settings.nodes, &CUSTOM_METHODS)?;
    let node_count = nodes.len();

    // Publishing is all a reload has to do. There is no health to measure up
    // front any more: a fresh node is ranked last until its first answer
    // measures it, and the request path reaches it as soon as the nodes ahead
    // are busy.
    rpc_client.reload(nodes).await?;

    Ok(node_count)
}

#[cfg(all(test, unix))]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    //! `reload_once` reads `CONFIG_PATH` and the files it names, so every test
    //! here is `#[serial]` — the environment is process-global, and edition
    //! 2024 marks `set_var` unsafe for that reason.

    use std::fmt::Write as _;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::Ordering;

    use axum::{Router, routing::post};
    use serial_test::serial;

    use super::{ServerSettings, reload_once};
    use crate::core::node::{NewNode, RpcNode};
    use crate::core::rpc::RpcClient;
    use crate::protocol::cost_table::ProviderCostTable;
    use crate::quotas::state::MAX_NODES;

    const PRICING_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/config/provider_config");

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

        /// Writes the config file and points `CONFIG_PATH` at it.
        fn config(&self, body: &str) {
            let path = self.0.join("config.toml");
            std::fs::write(&path, body).unwrap();

            set_config_path(&path);
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).ok();
        }
    }

    fn set_config_path(path: &Path) {
        // SAFETY: every test in this module is `#[serial]`, so no other thread
        // is inside `getenv`/`setenv` while this runs.
        unsafe { std::env::set_var("CONFIG_PATH", path) };
    }

    fn server() -> ServerSettings {
        ServerSettings {
            port: 3000,
            host: "127.0.0.1".into(),
            enable_metrics: false,
        }
    }

    /// A config file naming one node per entry in `nodes`, all priced from the
    /// shipped helius table.
    fn config_for(nodes: &[(&str, &str)]) -> String {
        let mut out = String::from("[server]\nhost = \"127.0.0.1\"\nport = 3000\n");

        for (name, url) in nodes {
            let _ = write!(
                out,
                "\n[[nodes]]\nname = \"{name}\"\nurl = \"{url}\"\ntier = 0\nrps_limit = 10\n\
                 max_concurrent = 4\nmonthly_limit = 1000\n\
                 provider_pricing_path = \"{PRICING_DIR}/helius.toml\"\n"
            );
        }

        out
    }

    /// The client a reload is applied to: one node, whatever the config says.
    fn client(name: &str, url: &str) -> RpcClient {
        let node = RpcNode::new(NewNode {
            name: name.into(),
            url: url.into(),
            rps_limit: 10,
            max_concurrent: 4,
            tier: 0,
            method_costs: ProviderCostTable::default(),
            monthly_limit: 1000,
            spillover_percent: 95,
            reset_day: 1,
        })
        .unwrap();

        RpcClient::new(vec![node]).unwrap()
    }

    fn node_names(rpc_client: &RpcClient) -> Vec<String> {
        rpc_client
            .topology
            .load()
            .all
            .iter()
            .map(|node| node.name.clone())
            .collect()
    }

    /// An upstream that answers `getHealth` with `ok`, so the probe round a
    /// reload ends with returns on its first attempt instead of timing out.
    async fn spawn_ok_node() -> String {
        let app = Router::new().route(
            "/",
            post(|| async { r#"{"jsonrpc":"2.0","id":1,"result":"ok"}"# }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(std::future::pending::<()>())
                .await
                .ok();
        });

        format!("http://{addr}")
    }

    #[tokio::test]
    #[serial]
    async fn a_reload_publishes_the_new_node_set() {
        let url = spawn_ok_node().await;
        let rpc_client = client("old", &url);

        let dir = TempDir::new("reload_ok");
        dir.config(&config_for(&[("new-a", &url), ("new-b", &url)]));

        let count = reload_once(&rpc_client, &server()).await.unwrap();

        assert_eq!(count, 2);
        assert_eq!(node_names(&rpc_client), ["new-a", "new-b"]);
    }

    #[tokio::test]
    #[serial]
    async fn a_reload_publishes_a_ranked_table_without_dialling_anything() {
        let url = spawn_ok_node().await;
        let rpc_client = client("old", &url);

        let dir = TempDir::new("reload_ranked");
        dir.config(&config_for(&[("fresh-b", &url), ("fresh-a", &url)]));

        reload_once(&rpc_client, &server()).await.unwrap();

        // The reload returns a table the router can walk immediately, and every
        // fresh node is unmeasured, so the ranking holds the config order
        // rather than inventing one.
        let topology = rpc_client.topology.load();
        assert_eq!(
            topology
                .ranked
                .iter()
                .map(|node| node.name.as_str())
                .collect::<Vec<_>>(),
            ["fresh-b", "fresh-a"]
        );
        assert!(
            topology.all[0]
                .latency
                .ema_us
                .load(Ordering::Relaxed)
                == u32::MAX,
            "a reload must not dial the new nodes"
        );
    }

    #[tokio::test]
    #[serial]
    async fn a_broken_config_leaves_the_running_topology_alone() {
        let rpc_client = client("old", "http://127.0.0.1:1");

        let dir = TempDir::new("reload_broken");
        dir.config("this is not toml {{{");

        assert!(reload_once(&rpc_client, &server()).await.is_err());

        // The balancer keeps serving what it has rather than dying on a typo.
        assert_eq!(node_names(&rpc_client), ["old"]);
    }

    #[tokio::test]
    #[serial]
    async fn a_missing_config_file_leaves_the_running_topology_alone() {
        let rpc_client = client("old", "http://127.0.0.1:1");

        let dir = TempDir::new("reload_absent");
        set_config_path(&dir.0.join("does_not_exist.toml"));

        assert!(reload_once(&rpc_client, &server()).await.is_err());
        assert_eq!(node_names(&rpc_client), ["old"]);
    }

    #[tokio::test]
    #[serial]
    async fn a_missing_pricing_file_leaves_the_running_topology_alone() {
        let rpc_client = client("old", "http://127.0.0.1:1");

        let dir = TempDir::new("reload_pricing");
        dir.config(
            "[server]\nhost = \"127.0.0.1\"\nport = 3000\n\n\
             [[nodes]]\nname = \"ghost\"\nurl = \"http://127.0.0.1:1\"\ntier = 0\n\
             rps_limit = 10\nmax_concurrent = 4\nmonthly_limit = 1000\n\
             provider_pricing_path = \"/nonexistent/provider.toml\"\n",
        );

        assert!(reload_once(&rpc_client, &server()).await.is_err());

        // `build_nodes` is all-or-nothing, so nothing reached the topology.
        assert_eq!(node_names(&rpc_client), ["old"]);
    }

    #[tokio::test]
    #[serial]
    async fn a_reload_past_the_slot_limit_leaves_the_topology_alone() {
        let rpc_client = client("old", "http://127.0.0.1:1");

        let names: Vec<String> = (0..=MAX_NODES).map(|i| format!("n{i}")).collect();
        let nodes: Vec<(&str, &str)> = names
            .iter()
            .map(|name| (name.as_str(), "http://127.0.0.1:1"))
            .collect();

        let dir = TempDir::new("reload_too_many");
        dir.config(&config_for(&nodes));

        // The config parses and every node builds; it is the quota slots that
        // run out, and that rejection has to be just as safe.
        assert!(reload_once(&rpc_client, &server()).await.is_err());
        assert_eq!(node_names(&rpc_client), ["old"]);
    }

    #[tokio::test]
    #[serial]
    async fn a_duplicate_node_name_leaves_the_topology_alone() {
        let rpc_client = client("old", "http://127.0.0.1:1");

        let dir = TempDir::new("reload_dupes");
        dir.config(&config_for(&[
            ("twin", "http://127.0.0.1:1"),
            ("twin", "http://127.0.0.1:2"),
        ]));

        // Names are how usage follows a node across a reload, so two of them
        // is not a set the balancer can bill.
        assert!(reload_once(&rpc_client, &server()).await.is_err());
        assert_eq!(node_names(&rpc_client), ["old"]);
    }

    #[tokio::test]
    #[serial]
    async fn a_surviving_node_keeps_its_usage_across_a_reload() {
        let url = spawn_ok_node().await;
        let rpc_client = client("keeper", &url);

        let slot = rpc_client.topology.load().all[0].id;
        rpc_client.nodes_usage.usage(slot).add(500);

        let dir = TempDir::new("reload_usage");
        dir.config(&config_for(&[("newcomer", &url), ("keeper", &url)]));

        reload_once(&rpc_client, &server()).await.unwrap();

        // Reordered in the config, but the counter follows the name: losing it
        // would re-open a quota the provider still bills as spent.
        let topology = rpc_client.topology.load();
        let keeper = topology
            .all
            .iter()
            .find(|node| node.name == "keeper")
            .expect("keeper survived the reload");

        assert_eq!(rpc_client.nodes_usage.usage(keeper.id).get(), 500);
    }

    #[tokio::test]
    #[serial]
    async fn a_changed_server_section_still_reloads_the_nodes() {
        let url = spawn_ok_node().await;
        let rpc_client = client("old", &url);

        let dir = TempDir::new("reload_server");
        dir.config(&config_for(&[("new", &url)]));

        // The startup listener was bound on a different port; that is warned
        // about, not treated as a reason to refuse the node changes.
        let startup = ServerSettings {
            port: 9999,
            host: "0.0.0.0".into(),
            enable_metrics: true,
        };

        assert_eq!(reload_once(&rpc_client, &startup).await.unwrap(), 1);
        assert_eq!(node_names(&rpc_client), ["new"]);
    }
}
