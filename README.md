# RPC Load Balancer [🚧 Active Development]

## TODO

### Correctness / reliability

- [x] **Fix logging** - keep per-request, upstream-attempt, and periodic healthcheck details below `info`; log health state transitions once.
- [x] **Forward upstream errors instead of generic 502** - on a non-retryable HTTP status or JSON-RPC error (e.g. -32602 invalid params), `send_with_fallback` discards the upstream response and returns `anyhow` error text, which the server wraps in a 502. The caller should receive the actual upstream status/body (a JSON-RPC error is a valid 200 response) so clients can see what was wrong with their request.
- [x] **Wait instead of skipping when all nodes are rate-limited** — `acquire_and_check` fails fast on the governor check, so a burst that exhausts every node's RPS budget returns "All RPC nodes failed" even though capacity frees up milliseconds later. When all nodes fail `acquire_and_check`, collect the minimum `wait_time` from governor's `NotUntil`, sleep for that duration, then retry the full node list. Loop until success, non-retryable error, or 1s outer timeout.

### Configuration / operations

- [x] **Graceful shutdown** - handle SIGINT/SIGTERM via `axum::serve(...).with_graceful_shutdown(...)` and stop the health-check task.
- [x] **Real `/health` endpoint** - currently always returns `ok`; report the number of active nodes from the routing table.
- [x] **Metrics** - Prometheus request, upstream, latency, and node-health metrics exposed through `/metrics`; local Prometheus and Grafana configuration lives in `monitoring/`.
- [x] **Configurable config path** - `config/config.toml` is hardcoded in `Settings::load`; allow overriding via CLI arg or `CONFIG_PATH` env var.

### Quality

- [ ] **Tests** - none exist. Minimum: unit tests for `send_with_fallback` (fallback path, rate limiting, all nodes down, non-retryable errors), `get_health` against mocks (`wiremock`), and `resolve_env` in `config.rs`.
- [x] **Remove dead code** - `NodeConfigs` in `node.rs` is unused; `LockFreeRouter::table` field is never used (the struct only serves as a namespace for `run_healthcheck_loop`).
- [x] **Fix stale doc comment** - `init_server` docs still say "Starts the HTTP server on `0.0.0.0:3000`" though host/port are now configurable.
- [ ] **Write a proper README** - document setup, configuration (config.toml + env vars for API keys), and endpoints.
