//! Configuration hot reload, driven by SIGHUP.
//!
//! A signal is the whole trigger: it needs no dependency, no port and no
//! authentication, and `docker kill -s HUP` reaches a container just as well as
//! `kill -HUP` reaches a process.

use anyhow::Result;
use tracing::{error, info, warn};

use crate::core::{healthcheck::HealthCheckLoop, node::RpcNode, rpc::RpcClient};
use crate::provider::load_config::{ServerSettings, Settings};

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

    let nodes = RpcNode::build_nodes(settings.nodes)?;
    let node_count = nodes.len();

    rpc_client.reload(nodes).await?;

    // The new nodes carry default health, so measure it now instead of routing
    // on the default until the periodic loop's next tick.
    HealthCheckLoop::run_once(rpc_client).await;

    Ok(node_count)
}
