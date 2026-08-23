# CascadeRPC

High-performance JSON-RPC reverse proxy for Web3 & Web2, written in Rust: lock-free routing, tier-based smart spillover, per-method credit accounting against provider quotas, and **~1.4 ms proxy overhead at 113,000 RPS** (140,000 RPS after the optimization pass below). Agnostic enough to proxy any JSON-RPC standard (Ethereum, EVM, REST, etc.).

---

## Benchmarks

Test bench: single Linux host (loopback), 6 mock RPC nodes (`src/bin/mock_node.rs`) with hardcoded response latencies, config from `config/balancer_config/mock_nodes.toml`, `--release` build. Metrics scraped by Prometheus from `/metrics` and rendered by the bundled Grafana dashboard.

**Hardware:**

- **CPU:** Intel® Core™ Ultra 5 Processor 125H (14 cores, 18 threads)
- **RAM:** 16 GB LPDDR5X 7467 MHz (soldered)
- **OS:** Arch Linux
- **Network:** `localhost` loopback - client, proxy, and all 6 mock nodes share the same CPU
- **Load generator:** [`oha`](https://github.com/hatoo/oha) (500–800 concurrent connections)

### Run 1 - Raw throughput (no artificial node latency)

![Benchmark without node latency](docs/benchmark-no-latency.png)

| Metric                  | Value           |
| ----------------------- | --------------- |
| Peak throughput (burst) | **113,000 RPS** |
| Steady-state plateau    | ~101,000 RPS    |
| End-to-end latency p50  | 2.55 ms         |
| End-to-end latency p95  | 4.61 ms         |
| End-to-end latency p99  | 5.74 ms         |

99% of client requests complete in under 6 milliseconds - at over 100k RPS.

**Proxy overhead.** Measured with Helius-1's mock delay pinned at 3.00 ms (a separate calibration run - the throughput run above uses zero node latency), its upstream p95 was 4.42 ms. So JSON parsing, the lock-free routing-table read, rate-limit checks, node selection, and byte proxying add **~1.42 ms** at p95 - an upper bound on the balancer's own work, since the mock server's tail queueing is included.

**Traffic distribution at peak (spillover).** The algorithm spread 113k successful req/s across nodes strictly by tier priority:

| Tier | Node          | req/s |
| ---- | ------------- | ----- |
| 0    | Helius-2      | 31.7K |
| 0    | Helius-1      | 31.0K |
| 1    | Alchemy-1     | 25.7K |
| 1    | Alchemy-2     | 23.0K |
| 2    | public-mirror | 1.05K |
| 2    | Triton-1      | 0     |

**Rate-limit exhaustion (rejected locally, zero network cost).** Requests the balancer bounced off a node's exhausted token bucket without ever touching the network: Helius-2 - 75.2K/s, Helius-1 - 56.9K/s, Alchemy-1 - 24.5K/s, Alchemy-2 - 1.47K/s. Provider limits stay respected; excess traffic cascades down the tiers instead of collecting 429s.

### Run 2 - Real-world latency (nodes with simulated network delay)

![Benchmark with node latency](docs/benchmark-with-latency.png)

Mock node delays: Helius-1 - 3 ms, Helius-2 - 5 ms, Alchemy-1 - 7 ms.

| Metric                             | Value          |
| ---------------------------------- | -------------- |
| Sustained peak (forwarded success) | **71,900 RPS** |
| End-to-end latency p50             | 6.14 ms        |
| End-to-end latency p95             | 8.82 ms        |
| End-to-end latency p99             | 9.57 ms        |

The drop from 113k to 71k RPS is Little's Law at work: artificial delay keeps connections open longer, so the same concurrency yields fewer requests per second - expected, not a regression.

The gap between the median request and the worst 1% is only **~3.4 ms**. No garbage collector, no GC pauses: response time stays flat and predictable under pressure.

**Proxy overhead under load.** Measured upstream p95 from the mock servers themselves:

| Node      | Hardcoded delay | Upstream p95 |
| --------- | --------------- | ------------ |
| Helius-1  | 3 ms            | 5.87 ms      |
| Helius-2  | 5 ms            | 7.83 ms      |
| Alchemy-1 | 7 ms            | 9.60 ms      |

(The mock servers and the OS loopback add their own micro-latency at 70k RPS, so the ~2.6–2.9 ms gaps above are a p95 upper bound on the balancer's share, not a direct measurement.) Most traffic (60k of 71k) went through Helius-1/2, which returned in ~6–7 ms - while the client-facing end-to-end p50 was 6.14 ms, so at the median the balancer's own work - parsing, the lock-free `arc-swap` table read, limit checks, byte forwarding - stays **under one millisecond**.

**Cascading spillover.** Both Tier 0 nodes hit their 30k RPS caps exactly (Helius-2 - 30.2K, Helius-1 - 30.0K); Tier 1 absorbed the overflow (Alchemy-1 - 11.7K).

### Update 1 - Optimization pass: 140,000 RPS peak, ~118,000 RPS steady

Same bench, same hardware, same `mock_nodes.toml` as Run 1. After a round of hot-path work the numbers moved from 113k peak / ~101k plateau to **140,000 RPS peak** and **~118,000 RPS steady-state**.

These are raw proxy throughput on loopback with zero node latency - the ceiling of the balancer's own machinery, not a number any real deployment sees. Over a real network, upstream RTT and provider rate limits set the pace long before this ceiling does; Run 2 above is the closer analogue.

What changed, in descending order of measured impact:

1. **Metric handles resolved once, not per request.** Every `counter!`/`histogram!` macro call re-emitted its metric description, and the Prometheus recorder takes a single process-wide `Mutex` to check whether that description is already registered - ~4 lock acquisitions and ~14 allocations per request, i.e. a serialization point across all workers at six figures of RPS. Descriptions now register once at startup (`metrics::describe_all`), per-node handles live in a `NodeMetrics` struct built in `RpcNode::new`, and global request handles live in a `LazyLock` array indexed by outcome. Metric names and labels are unchanged.
2. **No `arc_swap::Guard` held across `.await`.** The router held a cheap `load()` proxy across a full upstream round-trip. `arc-swap` has only 8 fast slots per thread, so under hundreds of concurrent tasks per worker every load degraded to the slow path and slowed down the health checker's `store()` as well. The router now takes one `load_full()` per retry round.
3. **Response validation gated properly.** Any upstream response under 64 KB was fully deserialized just to look for an `error` key - and RPC response bodies are orders of magnitude larger than request bodies. The threshold dropped to 512 bytes (still covering every real error body and gateway junk response), and the `memchr` `Finder` for `"error"` is built once in a static instead of per response.
4. **Method extraction without a JSON parse.** The request's `method` field is now read by a bounded leading-byte scan, with a fallback to `serde_json` for anything unusual (escapes, batch bodies, missing key), so correctness comes free from the slow path.
5. **`RpcNode` layout: five fewer allocations, no false sharing.** The node's limiter, semaphore, health flag and latency were each behind their own `Arc` inside a node that is itself always `Arc<RpcNode>` - five allocations, five pointer hops, and no padding, so one node's health flag could share a cache line with another node's token bucket. The fields are inline now, split across two `#[repr(align(64))]` cells: `NodeLimits` (written on every attempt) and `NodeStatus` (written once per health round, read on every ranking).

Smaller wins in the same loop: `lto = "fat"` + `codegen-units = 1` in the release profile; `release_max_level_info` compiling `trace!`/`debug!` out of release builds; a `const` `HeaderValue` for `content-type` instead of re-validating the string per request; `is_final_status` as a `matches!` instead of a 26-element linear scan; `State<Arc<RpcClient>>` instead of cloning four `Arc`s per request; a single `DefaultClock` static shared by every limiter instead of one constructed per rate-limited attempt; and a `Bytes::from_static` health-probe body instead of `json!` + `to_string()` per probe.

> The Run 1 / Run 2 figures above were measured before this pass and are kept as the baseline they were; they are not a measurement of the current code.

---

## Why it's fast

- **Lock-free routing table (`arc-swap`).** The hot path never takes a lock: every request does an atomic pointer load of the current routing table. The health-check loop builds a new table off to the side and swaps it in atomically.
- **Zero-copy request forwarding.** The client's request body is captured as `Bytes` and proxied to upstream as-is - no deserialization, no re-serialization, cheap reference-counted clones between retries.
- **Minimal response parsing.** Upstream responses are only deserialized into a tiny `error`-field-only struct (`RpcErrorOnly`) - just enough to decide "forward or retry", and only when the body is small or actually contains `"error"`. The response body itself is passed back to the client untouched.
- **Pricing is an array index.** Each provider's TOML is compiled once into a flat `ProviderCostTable`; the method name is resolved to an id once per request, and pricing an attempt on any node is one bounds-checked load.
- **Metrics off the registry.** Every counter and histogram handle is resolved at construction time, so recording an attempt is an atomic increment, not a registry lookup behind a global mutex.
- **Concurrent burst health probing.** Every 10 s a `JoinSet` probes all nodes in parallel (3 attempts, 500 ms timeout each), then sorts survivors by `(tier, measured latency)` and publishes the new table.
- **Smart fallback state machine.** Per request:
  1. **Fail-fast** - walk the sorted node list; skip any node whose token bucket is empty (no network call, remember its earliest-refill time), whose concurrency permits are all in use, whose monthly quota is spent, or which does not price the requested method.
  2. **Earliest-available wait** - if _every_ node is rate-limited, sleep exactly until the soonest bucket refills (minimum `NotUntil` from `governor`), not a fixed backoff.
  3. **Retry** - reload the routing table and loop, bounded by a 1 s global deadline.

  Non-retryable upstream responses (e.g. HTTP 400, JSON-RPC `-32602 invalid params`) are forwarded to the client verbatim instead of being masked as a generic 502.

## Features

- **Tier-based smart spillover** - traffic fills premium (Tier 0) nodes to their caps first, then cascades to lower tiers automatically.
- **Per-method credit accounting** - every request is priced from the provider's own price list and billed against that node's monthly quota. See [Cost & quotas](#cost--quotas).
- **Quota-aware routing** - a node that has spent its configured share of its monthly limit drops out of rotation before the provider starts refusing it.
- **Usage survives restarts** - counters are flushed to `quotas.json` once a minute and once more on shutdown, and restored at startup along with the billing period they belong to.
- **Per-node billing periods** - each node resets on its own `reset_day`, checked in UTC, because the anchor belongs to the provider account, not to the balancer.
- **Token-bucket rate limiting per node** (`governor`) - provider RPS limits enforced locally, before any network I/O.
- **Per-node concurrency caps** (`tokio::sync::Semaphore`) - a node at its `max_concurrent` is skipped, not queued on: the request spills to the next node instead of waiting for a permit.
- **Config hot reload on SIGHUP** - node set and price lists are re-read without a restart; a reload that fails to apply is logged and dropped, and the balancer keeps serving what it already has.
- **Anti-flapping health checks** - 3 probes per cycle before declaring a node healthy; state transitions logged once, not spammed.
- **Fail-open** - if _all_ nodes fail their health checks (e.g. a monitoring artifact), the balancer keeps routing to the full node list rather than going dark.
- **Graceful shutdown** - SIGINT/SIGTERM drain via `axum`'s graceful shutdown, followed by a final quota flush.
- **Config via TOML + env** - API keys are injected with `$VAR` substitution in URLs, never stored in config files.

## Cost & quotas

Providers do not bill per request - they bill per credit, and a `getProgramAccounts` costs many times what a `getBalance` does. The balancer models that directly.

**Pricing.** Each node names a `provider_pricing_path`: a TOML with what that provider charges for each method. It is compiled at load into a flat array indexed by method id, so the price of an attempt is one load on the request path. Method names outside the standard set (`helius_getFoo` and friends) are interned into a process-wide registry at config load and get ids past the built-in table - append-only, so an id never changes meaning while a request is in flight across a reload.

**Billing.** Before a node is picked, the request's method is priced against that node's table. A node that does not price the method at all - no `[routing]` entry, no `[custom]` entry, no `unknown_method_cost` - is skipped rather than billed at a guess.

**Spillover threshold.** `spillover_threshold = monthly_limit * spillover_percent / 100` (`spillover_percent` defaults to 95). Once a node's usage crosses it, the router skips the node and traffic cascades to the next tier - a little before the provider's quota is actually gone, so the reserve absorbs the accounting error that request parameters introduce. `monthly_limit = 0` reads as unmetered.

**Periods.** Each node resets on its own `reset_day` (1–31, default 1), checked in UTC once a minute. A day past the end of a short month lands on that month's last day, so 31 resets on Feb 28 (29 in a leap year). The period a node last reset for is stored next to its usage, so a restart that spans a billing boundary resets before it serves its first request rather than a minute later.

**Persistence.** Usage is written to `quotas.json` in the working directory every minute and once more on shutdown, and restored at startup. Usage is keyed by node **name** - renaming a node in the config reads as "one node left, another arrived": the new name starts the period from zero and may overspend the provider's real quota. Rename between billing periods, or not at all.

## Configuration

Two kinds of file, both re-read on SIGHUP. For runnable, containerized versions of
both, see [`examples/`](examples).

**Balancer config** (`config/balancer_config/config.toml`, path from `CONFIG_PATH`) - the `[server]` section plus one `[[nodes]]` block per upstream. See [`config.example.toml`](config/balancer_config/config.example.toml) for the annotated version.

```toml
[server]
host = "0.0.0.0"
port = 3000
enable_metrics = false  # enable only behind an internal network boundary

[[nodes]]
name = "Helius"                                                    # identity, not a label: quotas and metrics are keyed by it
url = "https://mainnet.helius-rpc.com/?api-key=$HELIUS_API_KEY"    # $VARS resolved from env
tier = 0                                                           # 0 = highest priority
rps_limit = 50                                                     # provider rate limit (token bucket)
max_concurrent = 16                                                # in-flight request cap
monthly_limit = 50_000_000                                         # credits per billing period; 0 = unmetered
reset_day = 15                                                     # day of month the counter zeroes (default 1)
provider_pricing_path = "config/provider_config/helius.toml"
```

**Provider pricing** (`config/provider_config/*.toml`) - one file per provider, shared by every node pointing at it. Shipped examples: [`helius.toml`](config/provider_config/helius.toml), [`alchemy.toml`](config/provider_config/alchemy.toml), [`solana.toml`](config/provider_config/solana.toml).

```toml
[limits]
spillover_percent = 95     # share of monthly_limit spendable before the node drops out (1..=100, default 95)
unknown_method_cost = 20   # price for a method neither table names; omit to skip the node for those methods

[routing]                  # standard methods - a name the balancer does not know is a typo and is dropped
getBalance = 10
getProgramAccounts = 50

[custom]                   # provider-specific methods - any name accepted, interned at load
helius_getPriorityFeeEstimate = 50
```

Set `unknown_method_cost` high rather than low: over-charging makes the balancer believe the quota ran out early, under-charging overspends it.

**Hot reload.** Editing either file and sending `SIGHUP` applies it without a restart:

```bash
kill -HUP <pid>
```

The `[server]` section is the exception - the listener is bound only at startup, and a changed `host`/`port`/`enable_metrics` is reported and ignored. `docker kill -s HUP <container>` reaches a containerized process the same way.

## Observability

Prometheus metrics on `/metrics` (opt-in), pre-provisioned Grafana dashboard in `monitoring/`:

- `rpc_requests` / `rpc_request_duration` - client-facing outcomes and end-to-end latency histograms (p50/p95/p99).
- `rpc_upstream_attempts` / `rpc_upstream_duration` - per-node, per-outcome attempt counters and latency (success, rate_limit, transport_error, retryable/forwarded errors).
- `rpc_upstream_skips` - per-node counter for nodes passed over without a network call, labelled by reason: `rate_limit`, `quota_exhausted`, `method_unsupported`, `saturated`.
- `rpc_node_quota_used` / `rpc_node_quota_threshold` - credits spent this period per node, against the spillover threshold it is heading for.
- `rpc_node_healthy`, `rpc_healthy_nodes`, `rpc_healthcheck_duration` - health-check state and timing.
- `rpc_sleep_queue_size` - requests currently parked waiting for a rate-limit window.

`GET /health` returns structured JSON: overall status (`ok` / `degraded` / `critical`), per-node up/down state, tier, and last measured latency.

See [`monitoring/README.md`](monitoring/README.md) for the local Prometheus + Grafana docker-compose setup, or
[`examples/full_observability`](examples/full_observability) for the same stack with the balancer containerized alongside it.

## Quick start

Requires Rust (edition 2024). For a container instead, [`examples/basic_setup`](examples/basic_setup) runs on
`docker compose up` with no API keys.

```bash
git clone <repo-url> && cd cascaderpc

# 1. Configure nodes
cp config/balancer_config/config.example.toml config/balancer_config/config.toml
cp .env.example .env            # API keys, and CONFIG_PATH pointing at the config above

# 2. Run
cargo run --release
```

`CONFIG_PATH` is read from the environment (`.env` is loaded at startup), so pointing the balancer at a different config is `CONFIG_PATH=... cargo run --release` without touching the file.

Every node needs a `provider_pricing_path` - a node whose price list is missing or unreadable fails config load rather than routing unpriced traffic.

Send a request:

```bash
curl -s http://localhost:3000/send-request \
  -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"getHealth"}'

curl -s http://localhost:3000/health | jq
```

### Reproduce the benchmarks

```bash
# Terminal 1: 6 mock nodes with configurable latencies
cargo run --release --bin mock_node

# Terminal 2: balancer pointed at the mocks
CONFIG_PATH=config/balancer_config/mock_nodes.toml cargo run --release

# Terminal 3: monitoring stack (Grafana on :3001, Prometheus on :9090)
GRAFANA_ADMIN_PASSWORD=<password> docker compose -f monitoring/docker-compose.yml up -d
```

Then drive load at `POST /send-request` and watch the `CascadeRPC` dashboard. The numbers above were produced with `oha` at 500–800 concurrent connections.

> **Note:** Generating 100k+ RPS from a single machine will exhaust your OS file descriptors and TCP ephemeral ports. Before running the load generator, increase your limits:
>
> ```bash
> ulimit -n 65535
> ```

```bash
# Terminal 4: drive load with oha (install: cargo install oha)
oha -z 30s -c 400 --no-tui http://localhost:3000/send-request -m POST \
  -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"getHealth"}'
```

## Limitations / Non-goals

- **WebSockets:** Currently HTTP(S) only. WS/WSS proxying is not supported.
- **Batch requests:** JSON-RPC arrays (`[{"id": 1...}, {"id": 2...}]`) are parsed but not routed per element - a batch is forwarded as one opaque body, priced as a single method against the chosen node's quota, and its partial failures are not introspected.
- **Cost, not latency, decides:** routing is tier-first and quota-aware; there is no per-node or per-method "prefer speed over credits" strategy switch. A latency-sensitive workload gets the tier order it was given.
- **Prices are static:** a method whose real cost depends on its parameters is billed at its table price, so price it with headroom.

## Endpoints

| Method | Path            | Description                                       |
| ------ | --------------- | ------------------------------------------------- |
| `POST` | `/send-request` | Proxy a JSON-RPC request through the balancer     |
| `GET`  | `/health`       | Balancer + per-node health JSON                   |
| `GET`  | `/metrics`      | Prometheus metrics (when `enable_metrics = true`) |

## License

[MIT](LICENSE)
