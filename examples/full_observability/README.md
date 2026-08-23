# full_observability

Five nodes across three tiers, two providers with two accounts each, a free
public tail, and the whole Prometheus + Grafana stack in the same compose
project.

This is the shape a real deployment has: paid capacity first, cheaper paid
capacity behind it, a free endpoint that keeps answering when the paid quotas
are gone — and enough instrumentation to see which of those is happening right
now. If you have not read
[`../basic_setup`](../basic_setup) yet, start there: it explains the config
fields and the routing rules that this example only builds on.

## Files

| File                                       | What it is                                           |
| ------------------------------------------ | ---------------------------------------------------- |
| [`config.toml`](config.toml)               | Five `[[nodes]]` blocks and `enable_metrics = true`. |
| [`docker-compose.yml`](docker-compose.yml) | `balancer` + `prometheus` + `grafana`, one network.  |
| [`prometheus.yml`](prometheus.yml)         | Scrape config targeting `balancer:3000`.             |
| [`.env.example`](.env.example)             | The API keys and the Grafana password to fill in.    |

Pricing tables come from the repository's shared
[`config/provider_config`](../../config/provider_config), mounted read-only.
Grafana's datasource, dashboard provider and the `CascadeRPC` dashboard come
from [`../../monitoring/grafana`](../../monitoring/grafana), also mounted rather
than copied — there is one dashboard in this repository, not three.

## Topology

| Node                | Tier | Pricing file   | Monthly limit | Role                                  |
| ------------------- | ---- | -------------- | ------------- | ------------------------------------- |
| `helius-primary`    | 0    | `helius.toml`  | 50,000,000    | Main paid capacity, bills on the 15th |
| `alchemy-primary`   | 0    | `alchemy.toml` | 20,000,000    | Second paid account in the same tier  |
| `helius-secondary`  | 1    | `helius.toml`  | 10,000,000    | Smaller plan, same provider           |
| `alchemy-secondary` | 1    | `alchemy.toml` | 5,000,000     | Smaller plan, same provider           |
| `public-mainnet`    | 2    | `solana.toml`  | 0 (unmetered) | Free tail, never drops out on quota   |

Tier 0 is tried first, and the two nodes in it are alternatives rather than a
queue: a request that finds `helius-primary` rate-limited or saturated goes to
`alchemy-primary` immediately. Tier 1 is only reached when nothing in tier 0 can
take the request, and tier 2 when nothing in tier 1 can either.

The two Helius nodes share one pricing file and the two Alchemy nodes share
another. That is the split the config is built around: what a method costs
belongs to the provider, while the plan size, the rate limit, the billing day
and the key belong to the account.

## Keys

Four keys — one per paid node:

```bash
cp .env.example .env
$EDITOR .env
```

Two nodes must not share one key. Usage is the account's, so a shared key means
one quota spent at twice the rate while each node counts half of it, and the
spillover reserve stops protecting anything. If you only have one key per
provider, **delete the `-secondary` node blocks** from `config.toml` rather than
pointing them at the same key. Three nodes is a perfectly good version of this
example.

`.env` is git-ignored. The compose file reads it automatically and refuses to
start with a named message if any key is missing, which is a clearer failure
than a config load that dies on an unresolved `$VAR`.

## Run

From this directory:

```bash
docker compose up --build -d
```

The first build takes a couple of minutes (fat LTO, one codegen unit). Then:

| What       | Where                                                                                                |
| ---------- | ---------------------------------------------------------------------------------------------------- |
| Balancer   | <http://localhost:3000> — `/send-request`, `/health`, `/metrics`                                     |
| Prometheus | <http://localhost:9090>                                                                              |
| Grafana    | <http://localhost:3001> — `admin` / `GRAFANA_ADMIN_PASSWORD`, `CascadeRPC` dashboard pre-provisioned |

Every port is published on `127.0.0.1` only. `/send-request` is an
unauthenticated proxy to your paid upstreams and `/metrics` reports what they
have cost so far; neither belongs on a public interface. `enable_metrics = true`
is safe here for exactly that reason — the endpoint is reachable from your
loopback and from the compose network, and nowhere else.

## Verify

```bash
# a request goes through
curl -s http://localhost:3000/send-request \
  -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"getHealth"}'

# every node's state, tier and last measured latency
curl -s http://localhost:3000/health | jq

# the balancer target is up in Prometheus
curl -s 'http://localhost:9090/api/v1/targets?state=active' | jq '.data.activeTargets[].health'

# Grafana is provisioned
curl -s http://localhost:3001/api/health
```

A key that is wrong rather than missing shows up here rather than at startup:

```json
{
  "status": "degraded",
  "active_nodes": 1,
  "total_nodes": 5,
  "nodes": [
    { "name": "helius-primary",  "tier": 0, "status": "down", "latency_ms": null },
    ...
    { "name": "public-mainnet",  "tier": 2, "status": "up",   "latency_ms": 43 }
  ]
}
```

`degraded` means some nodes are down but traffic is still being served — here,
entirely by the free tier-2 node. `critical` means nothing is left to route to.

## Which metric answers which question

| Question                                             | Metric                                                                                                                                                                            |
| ---------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| What are clients actually getting?                   | `rpc_requests` by `outcome`: `forwarded`, `bad_request`, `bad_gateway`, `timeout`                                                                                                 |
| How slow is the balancer end to end?                 | `rpc_request_duration` (histogram; p50/p95/p99 on the dashboard)                                                                                                                  |
| Which node served it, and how did the attempt end?   | `rpc_upstream_attempts` by `node` and `outcome`: `success`, `forwarded_http_error`, `forwarded_rpc_error`, `retryable_rpc_error`, `invalid_json`, `body_error`, `transport_error` |
| How slow is one upstream?                            | `rpc_upstream_duration` by `node`, `outcome`                                                                                                                                      |
| **Why did traffic skip a node?**                     | `rpc_upstream_skips` by `node` and `reason`: `rate_limit`, `quota_exhausted`, `method_unsupported`, `saturated`                                                                   |
| How close is a node to dropping out?                 | `rpc_node_quota_used` against `rpc_node_quota_threshold`, both by `node`                                                                                                          |
| Is a node up? How many are?                          | `rpc_node_healthy` by `node`, `rpc_healthy_nodes`                                                                                                                                 |
| Are health checks themselves slow?                   | `rpc_healthcheck_duration`                                                                                                                                                        |
| Are requests parked waiting for a rate-limit window? | `rpc_sleep_queue_size`                                                                                                                                                            |

Counters appear on the scrape with a `_total` suffix, as Prometheus convention
requires: `rpc_upstream_skips_total`, `rpc_requests_total`,
`rpc_upstream_attempts_total`.

### Reading the cascade

`rpc_upstream_skips_total` is the one to reach for first when traffic is not
where you expect it. Every skip is a decision made **without a network call**,
and the `reason` label says which:

```promql
sum by (node, reason) (rate(rpc_upstream_skips_total[5m]))
```

- `rate_limit` — the node's `rps_limit` is the binding constraint. Either the
  provider's real limit is higher than what you configured, or you need more
  capacity in that tier.
- `saturated` — `max_concurrent` is the binding constraint. The node is not
  slow, it is full; requests spill sideways instead of queueing.
- `quota_exhausted` — usage crossed `monthly_limit * spillover_percent / 100`.
  Expected near the end of a billing period, a warning at the start of one.
- `method_unsupported` — that node's pricing file does not price the method, so
  it is skipped rather than billed at a guess. A steady rate of this on one
  provider usually means a missing `[custom]` entry, not a missing feature.

Watching `rpc_node_quota_used` climb toward `rpc_node_quota_threshold` tells you
when the next `quota_exhausted` wave will start:

```promql
rpc_node_quota_used / rpc_node_quota_threshold
```

## Hot reload

`config.toml` is bind-mounted, so an edit on the host is an edit the container
sees. Applying it is a signal, not a restart:

```bash
docker kill -s HUP rpc-lb-observability-balancer-1
```

Both the balancer config and every pricing file are re-read. Nothing is
published unless the whole set builds, so a typo leaves the running
configuration serving and reports itself in the log. The `[server]` section is
the exception — the listener is bound once at startup, so changing `host`,
`port` or `enable_metrics` needs a restart and says so.

This is how you add a node, retire one, move a plan between tiers, or apply a
provider's new price list without dropping a request.

## Quotas across restarts

Usage is written to `/data/quotas.json` once a minute and once more on
shutdown, restored at startup, and kept in the named `quotas` volume:

```bash
docker compose exec balancer cat /data/quotas.json
```

Usage is keyed by node **name**. Renaming a node reads as "one node left,
another arrived": the new name starts the period from zero and may overspend
the provider's real quota — and it breaks the `node` label on every metric
above, which is the other half of why names here are identities rather than
labels.

`helius-primary` sets `reset_day = 15`, the rest default to the 1st. The period
each node last reset for is stored beside its counter, so a restart that spans
a billing boundary resets before the first request rather than a minute later.

## Stop

```bash
docker compose down      # keeps quota history and Grafana state
docker compose down -v   # deletes both, plus Prometheus data
```

## See also

- [`../basic_setup`](../basic_setup) — the same balancer with two public nodes,
  no keys and no metrics.
- [`../../monitoring/README.md`](../../monitoring/README.md) — the same
  Prometheus and Grafana against a balancer running on the host instead of in a
  container.
