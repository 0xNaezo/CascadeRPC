# RPC Load Balancer [🚧 Active Development]

## TODO

### Correctness / reliability

- [x] **Measure latency in health checks** - `node.latency` is never updated (always 0), so the `(tier, latency)` sort in the routing table does nothing. Time the `getHealth` request in `get_health` and store the result in the atomic.
- [ ] **Anti-flapping for health checks** - a single failed check currently drops a node for the whole interval. Evict after 2–3 consecutive failures, restore after 1 success (add a `fail_count: AtomicU32` to `RpcNode`).
- [ ] **Empty routing table policy** - if all nodes fail health checks, `send_with_fallback` errors immediately. Decide: fall back to the full node list, or return 503 with Retry-After.
- [ ] **Rotate the starting node** - every request starts with the first node in the list (best tier), so it hits its rate limit first. Add round-robin / weighted selection within a tier (`request_counter` exists but is unused for this).
- [x] **Remove or wire up `is_live`** - the `RpcNode::is_live` field is dead code; the routing table already serves that role.
- [ ] **Distinguish retryable JSON-RPC errors** - `send_with_fallback` retries on _any_ `error` in the response, but client errors (e.g. -32602 invalid params) will fail on every node and should be returned to the caller immediately. Only retry server-side errors (e.g. -32005 rate limited).

### Configuration / operations

- [x] **Load node config from a file** - nodes are hardcoded in `main.rs` (including a broken Helius URL). Move to TOML/YAML with API keys from env.
- [x] **Configurable bind address** - `0.0.0.0:3000` is hardcoded in `server.rs`.
- [ ] **Graceful shutdown** - handle SIGINT/SIGTERM via `axum::serve(...).with_graceful_shutdown(...)` and stop the health-check task.
- [ ] **Real `/health` endpoint** - currently always returns `ok`; report the number of active nodes from the routing table and return 503 when it is 0.
- [ ] **Metrics** - per-node request/error/latency counters (e.g. Prometheus via `axum-prometheus`), replacing the temporary `/test-speed` endpoint.

### Quality

- [ ] **Tests** - none exist. Minimum: unit tests for `send_with_fallback` (fallback path, rate limiting, all nodes down) and `get_health` against mocks (`wiremock`).
- [ ] **Separate health-check timeout** - checks share the main 2 s client timeout; use a shorter one (500 ms–1 s) so a slow node isn't considered healthy.
- [ ] **Write a proper README** - document setup, configuration, and endpoints.
