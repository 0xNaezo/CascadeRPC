use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::Result;
use cascaderpc::{
    core::{node::RpcNode, ranking::RankLoop, reload, rpc::RpcClient},
    metrics,
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

    // Before anything emits, and before `build_nodes` below: the recorder is
    // global, and what is measured before it exists is dropped, not held for
    // it. `RpcNode::new` resolves its metric handles once, at construction —
    // built against no recorder they are no-ops for the life of the process,
    // so swapping these two statements would silently take every per-node
    // metric off the scrape without failing anything.
    let metrics_handle = server_config
        .enable_metrics
        .then(server::install_metrics_recorder)
        .transpose()?;

    let quotas = restore()?;

    let nodes: Vec<RpcNode> = build_nodes(config.nodes, &CUSTOM_METHODS)?;

    let rpc_client = RpcClient::new(nodes)?;

    rpc_client.load_quotas(&quotas);

    // Before anything is served: a restart that spanned a billing boundary must
    // route its first request against a reset counter, not wait for the flusher's
    // first tick a minute in.
    roll_over_periods(&rpc_client);
    publish_quota_gauges(&rpc_client);

    tokio::spawn(RankLoop::run_rank_loop(rpc_client.clone()));
    tokio::spawn(start_disk_flusher(rpc_client.clone()));
    #[cfg(unix)]
    tokio::spawn(reload::watch_sighup(
        rpc_client.clone(),
        server_config.clone(),
    ));

    // The HTTP layer is the one place the client is cloned per request, so it
    // gets the `Arc`; the background tasks above clone it once at startup and
    // do not care.
    server::init_server(
        Arc::new(rpc_client.clone()),
        server_config.port,
        server_config.host,
        metrics_handle,
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
        publish_quota_gauges(&rpc_client);

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

/// Republishes every node's usage counter as a gauge.
///
/// Shares the flusher's tick rather than the request path: a quota is a monthly
/// budget, so a value refreshed once a minute says everything about it that an
/// atomic read per request would. It also runs once at startup, so a restart
/// does not leave the gauges missing until the first tick a minute later.
fn publish_quota_gauges(rpc_client: &RpcClient) {
    let topology = rpc_client.topology.load();

    for node in &topology.all {
        metrics::set_node_quota(
            &node.name,
            rpc_client.nodes_usage.usage(node.id).get(),
            node.spillover_threshold,
        );
    }
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
