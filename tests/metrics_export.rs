//! What the scrape endpoint actually publishes.
//!
//! Metric names and label values are a contract with whatever reads them — a
//! dashboard, an alert rule, a recording rule — and nothing else in this suite
//! looks at them. The claims pinned here are the ones the code makes in prose:
//! that `outcome` partitions the attempts, that `reason` partitions the skips,
//! and that a gauge held up by a guard comes back down.
//!
//! Its own test binary, and `#[serial]` within it: the recorder is installed
//! process-wide and every test shares one registry.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    // Counters and gauges render as exact small integers, so comparing them by
    // value is the assertion, not an approximation of one.
    clippy::float_cmp
)]

use std::sync::OnceLock;
use std::time::Duration;

use bytes::Bytes;
use metrics_exporter_prometheus::PrometheusHandle;
use rpc_load_balancer::{
    core::rpc::RpcClient,
    metrics::{self, NodeMetrics, Outcome, RequestOutcome, SkipReason},
    server,
};
use serial_test::serial;

mod common;

use common::{
    INVALID_PARAMS_BODY, NO_ERROR_BODY, NOT_JSON_BODY, OK_BODY, SERVER_ERROR_BODY, build_client,
    build_client_one, dead_url, node, spawn_mock, spawn_mock_latency, spawn_server,
};

/// The process-wide recorder, installed once however many tests run.
fn handle() -> &'static PrometheusHandle {
    static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

    HANDLE.get_or_init(|| {
        server::install_metrics_recorder().expect("no recorder is installed in this process yet")
    })
}

fn scrape() -> String {
    handle().render()
}

/// The value of one series, found by metric name and by every label fragment
/// given.
///
/// Matches on the name as a prefix because the exporter appends units and the
/// `_total` suffix that Prometheus naming asks for, and this should not break
/// when a metric grows one.
fn series(metric: &str, labels: &[&str]) -> Option<f64> {
    let render = scrape();

    render
        .lines()
        .filter(|line| line.starts_with(metric))
        .find(|line| labels.iter().all(|label| line.contains(label)))
        .and_then(|line| line.rsplit(' ').next()?.parse().ok())
}

fn expect_series(metric: &str, labels: &[&str]) -> f64 {
    series(metric, labels)
        .unwrap_or_else(|| panic!("no series {metric}{labels:?} in:\n{}", scrape()))
}

const fn body(text: &'static str) -> Bytes {
    Bytes::from_static(text.as_bytes())
}

/// Sends one request through the router, ignoring what it answers.
async fn send(client: &RpcClient, request: &'static str) {
    let _ = client.send(body(request)).await;
}

#[tokio::test]
#[serial]
async fn every_upstream_outcome_lands_in_rpc_upstream_attempts() {
    handle();

    // One node per outcome, each named after it, so the counters do not have
    // to be told apart by value.
    let success = spawn_mock(200, NO_ERROR_BODY).await;
    let http_error = spawn_mock(404, OK_BODY).await;
    let rpc_error = spawn_mock(200, INVALID_PARAMS_BODY).await;
    let retryable = spawn_mock(200, SERVER_ERROR_BODY).await;
    let garbage = spawn_mock(500, NOT_JSON_BODY).await;
    let gone = dead_url().await;

    for (name, url) in [
        ("m-success", success.url.as_str()),
        ("m-http", http_error.url.as_str()),
        ("m-rpc", rpc_error.url.as_str()),
        ("m-retryable", retryable.url.as_str()),
        ("m-garbage", garbage.url.as_str()),
        ("m-gone", gone.as_str()),
    ] {
        send(&build_client_one(node(name, url).build()), OK_BODY).await;
    }

    // Every path out of an attempt records exactly one, which is what makes
    // `outcome` a partition rather than a sample.
    for (name, outcome) in [
        ("m-success", "success"),
        ("m-http", "forwarded_http_error"),
        ("m-rpc", "forwarded_rpc_error"),
        ("m-retryable", "retryable_rpc_error"),
        ("m-garbage", "invalid_json"),
        ("m-gone", "transport_error"),
    ] {
        let labels = [
            format!(r#"node="{name}""#),
            format!(r#"outcome="{outcome}""#),
        ];
        let labels: Vec<&str> = labels.iter().map(String::as_str).collect();

        assert_eq!(
            expect_series("rpc_upstream_attempts", &labels),
            1.0,
            "{name}/{outcome}"
        );
        assert_eq!(
            expect_series("rpc_upstream_duration_seconds_count", &labels),
            1.0,
            "{name}/{outcome} recorded no duration beside its attempt"
        );
    }
}

#[tokio::test]
#[serial]
async fn a_skipped_node_is_counted_with_its_reason() {
    handle();

    // Quota spent: seeded past the node's own spillover threshold.
    let exhausted = build_client_one(
        node("s-quota", "http://127.0.0.1:1")
            .monthly_limit(10)
            .spillover_percent(100)
            .build(),
    );
    exhausted.nodes_usage.usage(0).add(11);
    send(&exhausted, OK_BODY).await;

    // Method the node does not price at all.
    send(
        &build_client_one(
            node("s-method", "http://127.0.0.1:1")
                .prices_nothing()
                .build(),
        ),
        OK_BODY,
    )
    .await;

    // Rate limited: one token per second, two requests.
    let upstream = spawn_mock(200, OK_BODY).await;
    let limited = build_client_one(node("s-rate", &upstream.url).rps(1).build());
    send(&limited, OK_BODY).await;
    send(&limited, OK_BODY).await;

    for (name, reason) in [
        ("s-quota", "quota_exhausted"),
        ("s-method", "method_unsupported"),
        ("s-rate", "rate_limit"),
    ] {
        let labels = [format!(r#"node="{name}""#), format!(r#"reason="{reason}""#)];
        let labels: Vec<&str> = labels.iter().map(String::as_str).collect();

        assert!(
            expect_series("rpc_upstream_skips", &labels) >= 1.0,
            "{name} was not counted as skipped for {reason}"
        );
    }

    // A skip sends nothing, so it must not land in the latency histogram as an
    // observation of zero. The node's histograms exist from the moment it is
    // built — one handle per outcome, resolved up front — so the claim is that
    // every one of them is still empty, not that the series is absent.
    let observed: Vec<f64> = scrape()
        .lines()
        .filter(|line| {
            line.starts_with("rpc_upstream_duration_seconds_count")
                && line.contains(r#"node="s-quota""#)
        })
        .filter_map(|line| line.rsplit(' ').next()?.parse().ok())
        .collect();

    assert!(
        !observed.is_empty(),
        "the node's histograms were never registered"
    );
    assert!(
        observed.iter().all(|count| *count == 0.0),
        "a skipped node recorded a duration: {observed:?}"
    );
}

#[tokio::test]
#[serial]
async fn a_rate_limited_node_is_skipped_not_attempted_twice() {
    handle();

    // Tier 0 runs out of tokens after one request; tier 1 picks the next one
    // up, so the router never sends a second request to the limited node.
    let first = spawn_mock(200, OK_BODY).await;
    let second = spawn_mock(200, OK_BODY).await;
    let client = build_client(vec![
        node("s-limited", &first.url).rps(1).tier(0).build(),
        node("s-spare", &second.url).tier(1).build(),
    ]);

    send(&client, OK_BODY).await;
    send(&client, OK_BODY).await;

    let limited = [r#"node="s-limited""#, r#"outcome="success""#];
    assert_eq!(
        expect_series("rpc_upstream_attempts", &limited),
        1.0,
        "the rate-limited node was sent a request it had no token for"
    );
    assert_eq!(
        expect_series(
            "rpc_upstream_skips",
            &[r#"node="s-limited""#, r#"reason="rate_limit""#]
        ),
        1.0
    );
    assert_eq!(
        expect_series(
            "rpc_upstream_attempts",
            &[r#"node="s-spare""#, r#"outcome="success""#]
        ),
        1.0,
        "the spare node did not take over"
    );
}

#[tokio::test]
#[serial]
async fn end_to_end_outcomes_partition_rpc_requests() {
    handle();

    let before = |outcome: &str| {
        series("rpc_requests", &[&format!(r#"outcome="{outcome}""#)]).unwrap_or(0.0)
    };
    let (forwarded, bad_request, bad_gateway, timeout) = (
        before("forwarded"),
        before("bad_request"),
        before("bad_gateway"),
        before("timeout"),
    );

    let upstream = spawn_mock(200, OK_BODY).await;
    let good = build_client_one(node("r-good", &upstream.url).build());
    send(&good, OK_BODY).await;
    send(&good, NOT_JSON_BODY).await;

    send(
        &build_client_one(node("r-gone", &dead_url().await).build()),
        OK_BODY,
    )
    .await;

    // Slower than the router's one-second budget for a whole request.
    let sluggish = spawn_mock_latency(200, OK_BODY, Duration::from_millis(1500)).await;
    send(
        &build_client_one(node("r-slow", &sluggish.url).build()),
        OK_BODY,
    )
    .await;

    for (outcome, was) in [
        ("forwarded", forwarded),
        ("bad_request", bad_request),
        ("bad_gateway", bad_gateway),
        ("timeout", timeout),
    ] {
        let label = format!(r#"outcome="{outcome}""#);

        assert_eq!(
            expect_series("rpc_requests", &[&label]),
            was + 1.0,
            "outcome {outcome} was not counted exactly once"
        );
        assert!(
            series("rpc_request_duration_seconds_count", &[&label]).is_some(),
            "outcome {outcome} recorded no end-to-end duration"
        );
    }
}

#[tokio::test]
#[serial]
async fn the_sleep_queue_gauge_returns_to_zero() {
    handle();

    // One token per second and two requests, so the second one parks.
    let upstream = spawn_mock(200, OK_BODY).await;
    let client = build_client_one(node("g-sleep", &upstream.url).rps(1).build());

    send(&client, OK_BODY).await;
    send(&client, OK_BODY).await;

    // The series exists, so the guard went up; it reads zero, so `Drop` brought
    // it back down. A leak here would read as a permanently congested balancer.
    assert_eq!(expect_series("rpc_sleep_queue_size", &[]), 0.0);
}

#[tokio::test]
#[serial]
async fn quota_gauges_report_used_and_threshold_separately() {
    handle();

    metrics::set_node_quota("q-node", 250, 1000);

    // Two gauges and not one ratio: the ratio is what alerts fire on, the
    // absolute spend is what a provider's bill is reconciled against.
    assert_eq!(
        expect_series("rpc_node_quota_used", &[r#"node="q-node""#]),
        250.0
    );
    assert_eq!(
        expect_series("rpc_node_quota_threshold", &[r#"node="q-node""#]),
        1000.0
    );
}

#[tokio::test]
#[serial]
async fn an_unlimited_node_reports_a_threshold_it_cannot_reach() {
    handle();

    metrics::set_node_quota("q-unlimited", 5, u64::MAX);

    // Past 2^53 an f64 stops resolving single units; what matters is that the
    // ratio stays pinned near zero rather than wrapping to something small.
    let threshold = expect_series("rpc_node_quota_threshold", &[r#"node="q-unlimited""#]);
    assert!(
        threshold > 1e18,
        "unlimited threshold collapsed to {threshold}"
    );
}

#[tokio::test]
#[serial]
async fn the_node_health_gauge_carries_the_tier_label() {
    handle();

    metrics::record_probe("p-up", 2, true, 0.01);
    metrics::record_probe("p-down", 0, false, 0.5);

    // The tier is on the gauge so an alert can say "every tier-0 node is down"
    // without joining against the config.
    assert_eq!(
        expect_series("rpc_node_healthy", &[r#"node="p-up""#, r#"tier="2""#]),
        1.0
    );
    assert_eq!(
        expect_series("rpc_node_healthy", &[r#"node="p-down""#, r#"tier="0""#]),
        0.0
    );

    // The histogram beside it is what says whether a node about to be marked
    // down is merely slow.
    assert_eq!(
        expect_series(
            "rpc_healthcheck_duration_seconds_count",
            &[r#"node="p-down""#, r#"outcome="unhealthy""#]
        ),
        1.0
    );
}

#[tokio::test]
#[serial]
async fn a_node_count_past_u32_saturates_rather_than_wrapping() {
    handle();

    metrics::set_healthy_nodes(usize::MAX);

    // Saturating, not wrapping: a wrap would read as "no healthy nodes" and
    // page someone at 3am for a balancer that is fine.
    assert_eq!(expect_series("rpc_healthy_nodes", &[]), f64::from(u32::MAX));

    metrics::set_healthy_nodes(3);
    assert_eq!(expect_series("rpc_healthy_nodes", &[]), 3.0);
}

#[tokio::test]
#[serial]
async fn every_metric_renders_help_and_type() {
    handle();

    // Touch every emission site once so nothing is missing merely because it
    // was never called.
    let node = NodeMetrics::new("h-node");
    node.record_attempt(Outcome::Success, 0.01);
    node.record_skip(SkipReason::RateLimit);
    metrics::record_request(RequestOutcome::Forwarded, 0.01);
    metrics::record_probe("h-node", 0, true, 0.01);
    metrics::set_healthy_nodes(1);
    metrics::set_node_quota("h-node", 1, 2);
    drop(metrics::sleeping_on_rate_limit());

    let render = scrape();

    for metric in [
        "rpc_upstream_attempts",
        "rpc_upstream_duration",
        "rpc_requests",
        "rpc_request_duration",
        "rpc_sleep_queue_size",
        "rpc_healthcheck_duration",
        "rpc_node_healthy",
        "rpc_healthy_nodes",
        "rpc_upstream_skips",
        "rpc_node_quota_used",
        "rpc_node_quota_threshold",
    ] {
        assert!(
            render.contains(&format!("# HELP {metric}")),
            "{metric} has no description"
        );
        assert!(
            render.contains(&format!("# TYPE {metric}")),
            "{metric} has no type"
        );
    }
}

#[tokio::test]
#[serial]
async fn the_metrics_endpoint_renders_over_http() {
    let base = spawn_server(
        build_client_one(node("e-node", "http://127.0.0.1:1").build()),
        Some(handle().clone()),
    )
    .await;

    let response = reqwest::get(format!("{base}/metrics")).await.unwrap();

    assert_eq!(response.status(), 200);
    assert!(
        response.text().await.unwrap().contains("rpc_"),
        "the scrape endpoint rendered nothing the balancer emits"
    );
}
