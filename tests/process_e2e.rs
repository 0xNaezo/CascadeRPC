//! The balancer as an operator runs it: a real process, a config file on disk,
//! signals, and the usage file it leaves behind.
//!
//! This is the only place `main` is exercised at all — the flusher, the startup
//! rollover, the SIGHUP task and the shutdown flush are wired together there
//! and nowhere else. Everything here drives the binary from the outside, so it
//! asserts on what an operator can actually observe: the HTTP surface, the
//! usage file, and the exit code.
#![cfg(unix)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::fmt::Write as _;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

mod common;

use common::{HEALTH_OK_BODY, OK_BODY, free_port, spawn_mock};

/// Absolute: the balancer runs with its working directory in a scratch folder,
/// so every path it is handed has to be too.
const PRICING: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/config/provider_config/helius.toml"
);

// ---------------------------------------------------------------------------
// Scratch directory: the config the balancer reads and the usage file it writes
// ---------------------------------------------------------------------------

struct Scratch(PathBuf);

impl Scratch {
    fn new(tag: &str) -> Self {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let dir = std::env::temp_dir().join(format!("rpc_lb_e2e_{tag}_{unique}"));
        std::fs::create_dir_all(&dir).unwrap();

        Self(dir)
    }

    fn write_config(&self, body: &str) {
        std::fs::write(self.0.join("config.toml"), body).unwrap();
    }

    fn write_quotas(&self, body: &str) {
        std::fs::write(self.0.join("quotas.json"), body).unwrap();
    }

    /// What the balancer flushed, or `None` if it never wrote the file.
    fn quotas(&self) -> Option<serde_json::Value> {
        let text = std::fs::read_to_string(self.0.join("quotas.json")).ok()?;

        Some(serde_json::from_str(&text).expect("the usage file must be valid JSON"))
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).ok();
    }
}

/// A config file: one `[server]` section and one node per entry.
fn config(port: u16, metrics: bool, nodes: &[(&str, &str)]) -> String {
    let mut out =
        format!("[server]\nhost = \"127.0.0.1\"\nport = {port}\nenable_metrics = {metrics}\n");

    for (name, url) in nodes {
        let _ = write!(
            out,
            "\n[[nodes]]\nname = \"{name}\"\nurl = \"{url}\"\ntier = 0\nrps_limit = 100\n\
             max_concurrent = 8\nmonthly_limit = 1000\nreset_day = 1\n\
             provider_pricing_path = \"{PRICING}\"\n"
        );
    }

    out
}

// ---------------------------------------------------------------------------
// The process under test
// ---------------------------------------------------------------------------

/// The balancer binary, killed when the handle goes out of scope.
struct Balancer {
    child: Child,
    base: String,
}

impl Balancer {
    /// Starts the binary against `scratch`, without waiting for it to be ready.
    fn start(scratch: &Scratch, port: u16) -> Self {
        // Cargo sets this for integration tests, so nothing has to guess where
        // the built binary landed.
        let child = Command::new(env!("CARGO_BIN_EXE_rpc-load-balancer"))
            .env("CONFIG_PATH", scratch.0.join("config.toml"))
            // The usage file is resolved against the working directory.
            .current_dir(&scratch.0)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("the balancer binary is built by `cargo test`");

        Self {
            child,
            base: format!("http://127.0.0.1:{port}"),
        }
    }

    /// Starts it and waits until `/health` answers.
    async fn serving(scratch: &Scratch, port: u16) -> Self {
        let balancer = Self::start(scratch, port);
        balancer.wait_until_ready().await;

        balancer
    }

    async fn wait_until_ready(&self) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);

        while reqwest::get(format!("{}/health", self.base)).await.is_err() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the balancer never started serving"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    fn pid(&self) -> u32 {
        self.child.id()
    }

    fn signal(&self, name: &str) {
        // `kill(1)` rather than a libc dependency for two calls.
        let status = Command::new("kill")
            .args([&format!("-{name}"), &self.pid().to_string()])
            .status()
            .expect("kill(1) is available");

        assert!(status.success(), "could not send {name}");
    }

    /// Waits for the process to exit and reports whether it exited cleanly.
    async fn wait_for_exit(&mut self) -> bool {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);

        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return status.success();
            }

            assert!(
                tokio::time::Instant::now() < deadline,
                "the balancer never exited"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Sends SIGTERM and waits for the shutdown to complete.
    async fn shutdown(&mut self) {
        self.signal("TERM");
        assert!(self.wait_for_exit().await, "shutdown was not clean");
    }

    async fn send(&self, body: &'static str) -> reqwest::Response {
        reqwest::Client::new()
            .post(format!("{}/send-request", self.base))
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
            .expect("the balancer answered")
    }

    async fn health(&self) -> serde_json::Value {
        reqwest::get(format!("{}/health", self.base))
            .await
            .expect("the balancer answered")
            .json()
            .await
            .expect("/health returns JSON")
    }
}

impl Drop for Balancer {
    fn drop(&mut self) {
        self.child.kill().ok();
        self.child.wait().ok();
    }
}

/// Polls `check` until it holds, so a test never sleeps on a fixed guess.
async fn eventually<F>(what: &str, mut check: F)
where
    F: AsyncFnMut() -> bool,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);

    while !check().await {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn the_balancer_serves_a_request_end_to_end() {
    let upstream = spawn_mock(200, HEALTH_OK_BODY).await;
    let scratch = Scratch::new("serve");
    let port = free_port().await;
    scratch.write_config(&config(port, false, &[("only", &upstream.url)]));

    let balancer = Balancer::serving(&scratch, port).await;

    let response = balancer.send(OK_BODY).await;

    // Config read from disk, listener bound, upstream reached, answer
    // forwarded — the whole path, with nothing stubbed.
    assert_eq!(response.status(), 200);
    assert_eq!(response.text().await.unwrap(), HEALTH_OK_BODY);
}

#[tokio::test(flavor = "multi_thread")]
async fn sigterm_flushes_usage_before_exiting() {
    let upstream = spawn_mock(200, HEALTH_OK_BODY).await;
    let scratch = Scratch::new("flush");
    let port = free_port().await;
    scratch.write_config(&config(port, false, &[("only", &upstream.url)]));

    let mut balancer = Balancer::serving(&scratch, port).await;
    for _ in 0..3 {
        assert_eq!(balancer.send(OK_BODY).await.status(), 200);
    }

    // The periodic flush is a minute away, so anything on disk now was written
    // by the shutdown path.
    assert!(
        scratch.quotas().is_none(),
        "usage was flushed before shutdown"
    );

    balancer.shutdown().await;

    // getBalance costs 1 on the helius table.
    assert_eq!(scratch.quotas().unwrap()["only"]["used"], 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn usage_survives_a_restart() {
    let upstream = spawn_mock(200, HEALTH_OK_BODY).await;
    let scratch = Scratch::new("restart");
    let port = free_port().await;
    scratch.write_config(&config(port, false, &[("only", &upstream.url)]));

    let mut first = Balancer::serving(&scratch, port).await;
    for _ in 0..2 {
        first.send(OK_BODY).await;
    }
    first.shutdown().await;

    let mut second = Balancer::serving(&scratch, port).await;
    second.send(OK_BODY).await;
    second.shutdown().await;

    // Cumulative, not restarted: a restart that re-opened the month would let
    // the balancer spend a quota the provider still bills as spent.
    assert_eq!(scratch.quotas().unwrap()["only"]["used"], 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_restart_across_a_billing_boundary_starts_from_zero() {
    let upstream = spawn_mock(200, HEALTH_OK_BODY).await;
    let scratch = Scratch::new("rollover");
    let port = free_port().await;
    scratch.write_config(&config(port, false, &[("only", &upstream.url)]));

    // Restored usage from a period years in the past.
    scratch.write_quotas(r#"{"only":{"used":900,"period_start":"2020-01-01"}}"#);

    let mut balancer = Balancer::serving(&scratch, port).await;
    balancer.send(OK_BODY).await;
    balancer.shutdown().await;

    let quotas = scratch.quotas().unwrap();

    // Rolled over at startup, before the first request was served — not a
    // minute later on the flusher's first tick.
    assert_eq!(quotas["only"]["used"], 1);
    assert_ne!(quotas["only"]["period_start"], "2020-01-01");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_corrupt_quotas_file_stops_the_process() {
    let upstream = spawn_mock(200, HEALTH_OK_BODY).await;
    let scratch = Scratch::new("corrupt");
    let port = free_port().await;
    scratch.write_config(&config(port, false, &[("only", &upstream.url)]));
    scratch.write_quotas("{ not json");

    let mut balancer = Balancer::start(&scratch, port);

    // Starting from zero here would silently re-open a spent quota, so the
    // operator has to delete the file on purpose.
    assert!(
        !balancer.wait_for_exit().await,
        "a corrupt usage file must stop the balancer"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_missing_config_path_exits_nonzero() {
    let scratch = Scratch::new("no_config_path");

    let mut child = Command::new(env!("CARGO_BIN_EXE_rpc-load-balancer"))
        .env_remove("CONFIG_PATH")
        .current_dir(&scratch.0)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the balancer kept running without a config"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };

    assert!(!status.success());
}

#[tokio::test(flavor = "multi_thread")]
async fn an_invalid_config_exits_nonzero() {
    let scratch = Scratch::new("bad_config");
    scratch.write_config("this is not toml {{{");

    let mut balancer = Balancer::start(&scratch, free_port().await);

    assert!(!balancer.wait_for_exit().await);
}

#[tokio::test(flavor = "multi_thread")]
async fn sighup_applies_a_new_node_set() {
    let first = spawn_mock(200, HEALTH_OK_BODY).await;
    let second = spawn_mock(200, HEALTH_OK_BODY).await;

    let scratch = Scratch::new("sighup_ok");
    let port = free_port().await;
    scratch.write_config(&config(port, false, &[("one", &first.url)]));

    let balancer = Balancer::serving(&scratch, port).await;
    assert_eq!(balancer.health().await["total_nodes"], 1);

    scratch.write_config(&config(
        port,
        false,
        &[("one", &first.url), ("two", &second.url)],
    ));
    balancer.signal("HUP");

    eventually("the reloaded node set reaches /health", async || {
        balancer.health().await["total_nodes"] == 2
    })
    .await;

    // The reload probes before it returns, so both nodes are usable straight
    // away rather than after the next tick.
    assert_eq!(balancer.health().await["active_nodes"], 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn sighup_with_a_broken_config_keeps_serving() {
    let upstream = spawn_mock(200, HEALTH_OK_BODY).await;
    let scratch = Scratch::new("sighup_bad");
    let port = free_port().await;
    scratch.write_config(&config(port, false, &[("only", &upstream.url)]));

    let balancer = Balancer::serving(&scratch, port).await;

    scratch.write_config("this is not toml {{{");
    balancer.signal("HUP");

    // Give the reload task a chance to fail before checking it did no harm.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // A typo in a file the operator can fix and re-signal must not be fatal.
    assert_eq!(balancer.health().await["total_nodes"], 1);
    assert_eq!(balancer.send(OK_BODY).await.status(), 200);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_metrics_endpoint_is_off_unless_the_config_enables_it() {
    let upstream = spawn_mock(200, HEALTH_OK_BODY).await;
    let scratch = Scratch::new("metrics_off");
    let port = free_port().await;
    scratch.write_config(&config(port, false, &[("only", &upstream.url)]));

    let balancer = Balancer::serving(&scratch, port).await;

    // The scrape endpoint exposes node names and spend, so it stays off until
    // asked for.
    assert_eq!(
        reqwest::get(format!("{}/metrics", balancer.base))
            .await
            .unwrap()
            .status(),
        404
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn quota_gauges_are_published_at_startup() {
    let upstream = spawn_mock(200, HEALTH_OK_BODY).await;
    let scratch = Scratch::new("metrics_on");
    let port = free_port().await;
    scratch.write_config(&config(port, true, &[("only", &upstream.url)]));

    let balancer = Balancer::serving(&scratch, port).await;

    let scrape = reqwest::get(format!("{}/metrics", balancer.base))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    // Published before the flusher's first tick a minute in, so a restart does
    // not leave the dashboards blank.
    assert!(
        scrape.contains(r#"rpc_node_quota_used{node="only"}"#),
        "quota gauges missing at startup:\n{scrape}"
    );
    // monthly_limit 1000 at helius' 95% spillover.
    assert!(
        scrape.contains(r#"rpc_node_quota_threshold{node="only"} 950"#),
        "quota threshold missing or wrong:\n{scrape}"
    );
}
