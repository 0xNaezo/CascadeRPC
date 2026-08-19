use std::time::{Duration, SystemTime};

use anyhow::Result;
use rpc_load_balancer::{
    core::{healthcheck::HealthCheckLoop, node::RpcNode, reload, rpc::RpcClient},
    protocol::registry::CUSTOM_METHODS,
    provider::load_config::{Settings, build_nodes},
    quotas::{
        period::{self, lock_periods},
        persistence::{self, restore},
    },
    server,
};

const FLUSH_PERIOD: Duration = Duration::from_mins(1);

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let config = Settings::load()?;

    let server_config = config.server;

    let quotas = restore()?;

    let nodes: Vec<RpcNode> = build_nodes(config.nodes, &CUSTOM_METHODS)?;

    let rpc_client = RpcClient::new(nodes)?;

    rpc_client.load_quotas(&quotas);

    // Before anything is served: a restart that spanned a billing boundary must
    // route its first request against a reset counter, not wait for the flusher's
    // first tick a minute in.
    roll_over_periods(&rpc_client);

    tokio::spawn(HealthCheckLoop::run_healthcheck_loop(rpc_client.clone()));
    tokio::spawn(start_disk_flusher(rpc_client.clone()));
    #[cfg(unix)]
    tokio::spawn(reload::watch_sighup(
        rpc_client.clone(),
        server_config.clone(),
    ));

    server::init_server(
        rpc_client.clone(),
        server_config.port,
        server_config.host,
        server_config.enable_metrics,
    )
    .await?;

    flush_usage(&rpc_client).await;

    Ok(())
}

/// Rolls the billing period over and writes every node's usage counter to disk,
/// once a minute.
///
/// The rollover shares this tick instead of running a loop of its own: it needs
/// no other timer, and pairing the two means a reset and the marker that records
/// it reach the disk together.
///
/// Runs until the task is dropped. A failed flush is logged and retried on the
/// next tick.
async fn start_disk_flusher(rpc_client: RpcClient) {
    let mut interval =
        tokio::time::interval_at(tokio::time::Instant::now() + FLUSH_PERIOD, FLUSH_PERIOD);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        interval.tick().await;

        // Before the flush, so a counter this tick zeroes is written out zeroed
        // rather than a minute later.
        roll_over_periods(&rpc_client);

        flush_usage(&rpc_client).await;
    }
}

/// Reads the live node set and hands it to the rollover.
///
/// This is the half that knows where the counters live; `quotas::period` is the
/// half that knows when a period has turned.
fn roll_over_periods(rpc_client: &RpcClient) {
    let topology = rpc_client.topology.load();

    period::rollover_if_new_period(
        &topology.all,
        &rpc_client.nodes_usage,
        &rpc_client.periods,
        SystemTime::now(),
    );
}

/// Takes a snapshot of what every node has spent and writes it out.
///
/// The topology guard and the period lock are both released before the write:
/// the snapshot borrows the node names, so it and the guard have to share a
/// scope, but neither may be held across the disk I/O that follows.
async fn flush_usage(rpc_client: &RpcClient) {
    // `load_full` rather than `load`: the guard would otherwise be held across
    // the write below.
    let topology = rpc_client.topology.load_full();
    let usage = {
        // Dropped before the write: the flusher is the only writer, but holding
        // it across an await would block a reload's rollover for a disk write.
        let periods = lock_periods(&rpc_client.periods);
        persistence::snapshot(&topology.all, &rpc_client.nodes_usage, &periods)
    };

    persistence::flush(&usage).await;
}
