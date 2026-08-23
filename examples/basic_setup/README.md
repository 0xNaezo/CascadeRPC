# basic_setup

The smallest configuration that is still a load balancer: one container, two
upstream nodes in two tiers, no metrics, no API keys.

Everything here runs against public Solana endpoints, so `docker compose up` is
the whole setup. Start with this example to see how a request is routed and how
the cascade behaves, then move to
[`../full_observability`](../full_observability) for the metrics side.

## Files

| File                                       | What it is                                                                          |
| ------------------------------------------ | ----------------------------------------------------------------------------------- |
| [`config.toml`](config.toml)               | The balancer config: the `[server]` section and one `[[nodes]]` block per upstream. |
| [`docker-compose.yml`](docker-compose.yml) | One service, built from the repository root [`Dockerfile`](../../Dockerfile).       |

The pricing tables are **not** in this directory. The compose file mounts the
repository's [`config/provider_config`](../../config/provider_config) read-only
at `/etc/rpc-lb/provider_config` — see [Where prices
live](#where-prices-live).

## Run

From this directory:

```bash
docker compose up --build
```

The first build compiles the balancer with fat LTO and a single codegen unit,
which takes a couple of minutes. Later starts reuse the image.

Then, from another terminal:

```bash
curl -s http://localhost:3000/send-request \
  -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"getHealth"}'
```

```json
{ "jsonrpc": "2.0", "result": "ok", "id": 1 }
```

```bash
curl -s http://localhost:3000/health | jq
```

```json
{
  "status": "ok",
  "active_nodes": 2,
  "total_nodes": 2,
  "nodes": [
    {
      "name": "solana-mainnet-beta",
      "tier": 0,
      "status": "up",
      "latency_ms": 30
    },
    { "name": "publicnode", "tier": 1, "status": "up", "latency_ms": 46 }
  ]
}
```

`status` is `ok` while every tier has a healthy node, `degraded` when some nodes
are down but traffic can still be served, and `critical` when nothing is left to
route to.

## The config, field by field

```toml
[server]
host = "0.0.0.0"
port = 3000
enable_metrics = false
```

`host` is the address **inside** the container, which is why it is `0.0.0.0` —
the port is published on `127.0.0.1` by the compose file, and that is what keeps
it off your network. `/send-request` is an unauthenticated proxy to whatever
upstreams you configure; on a public interface it is someone else's free RPC.

`enable_metrics` is off by default and left off here. The scrape endpoint
reports node names, per-node spend and health, so it is opt-in rather than
opt-out.

The `[server]` section is the one part a hot reload cannot apply: the listener
is bound once at startup. Changing `host`, `port` or `enable_metrics` needs a
restart, and the process says so in the log instead of pretending to apply it.

```toml
[[nodes]]
name = "solana-mainnet-beta"
url = "https://api.mainnet-beta.solana.com"
tier = 0
rps_limit = 5
max_concurrent = 2
monthly_limit = 1_000_000
provider_pricing_path = "/etc/rpc-lb/provider_config/solana.toml"
```

- **`name`** is an identity, not a display label. Usage is stored under it in
  `quotas.json` and every metric is labelled with it. Renaming a node reads as
  "one node left, another arrived": the new name starts the period from zero and
  can overspend the provider's real quota. Rename between billing periods, or
  not at all.
- **`url`** may contain `$VAR` references, resolved from the container's
  environment at load. Only the bare form works — `${VAR}` is refused rather
  than passed through, because passing it through would put the braces
  themselves into the upstream URL and turn a config mistake into a 401 much
  further from the cause. Neither node here needs a key, so neither uses one.
- **`tier`** is priority: 0 first. Nodes in the same tier are alternatives, not a
  queue.
- **`rps_limit`** is the provider's rate limit, enforced locally as a token
  bucket so the balancer stops before the provider does.
- **`max_concurrent`** caps in-flight requests to that node. A node already at
  its cap is skipped rather than queued behind — the next node is almost always
  faster than waiting.
- **`monthly_limit`** is the quota for one billing period, in whatever unit the
  provider bills (credits, CU, requests). `0` means unmetered. The `1_000_000`
  on the tier-0 node is a deliberate fiction — a public endpoint bills nobody —
  so the quota cascade can be watched without waiting on a real plan.
- **`reset_day`** (not set here, defaults to `1`) is the day of the month the
  counter zeroes, per node, because the anchor belongs to the account rather
  than the protocol.
- **`provider_pricing_path`** is that node's price list. A node whose price list
  is missing or unreadable fails config load rather than routing unpriced
  traffic.

## How a request picks a node

For each request the balancer reads the method name out of the JSON-RPC body,
then walks the tiers in order. Inside a tier a node is skipped, without any
network call, when:

- it is over its rate limit (`rate_limit`),
- it is already at `max_concurrent` (`saturated`),
- its usage has crossed the spillover threshold (`quota_exhausted`),
- its pricing file does not price this method (`method_unsupported`),
- the health loop has it marked down.

Only what survives that filter gets a request. If the attempt fails in a
retryable way, the next candidate takes over; an error the upstream means as an
answer (a real JSON-RPC error) is forwarded to the client as-is instead of being
retried against every node.

**Spillover threshold** is `monthly_limit * spillover_percent / 100`, with
`spillover_percent` coming from the pricing file (default 95). The tier-0 node
here drops out at 950,000 of its 1,000,000, not at 1,000,000: request cost
depends on parameters the balancer cannot see in advance, so the reserve absorbs
the accounting error rather than discovering it as an overage on the invoice.

## Where prices live

Each node names a `provider_pricing_path`, and the compose file mounts the
repository's shared pricing directory read-only:

```yaml
- ../../config/provider_config:/etc/rpc-lb/provider_config:ro
```

The two files have different lifetimes, which is why they are separate:

- **prices** — `[routing]`, `[custom]`, `unknown_method_cost` — change when the
  provider changes its price list. They are a fact about the provider, identical
  across every deployment that uses it.
- **plan size, rate limit, billing day, API key** change per account and per
  deployment. They live in this `config.toml`.

Copying the price tables into each example would create a second and third
source of truth for the same provider, and they would drift. Both nodes here
point at the same [`solana.toml`](../../config/provider_config/solana.toml), the
way two nodes on one provider should.

One wrinkle worth knowing: `spillover_percent` lives in the pricing file even
though it reads more like a deployment knob. Two nodes sharing a pricing file
share their reserve percentage; giving them different ones means giving them
different files.

## Quotas across restarts

Usage is written to `quotas.json` in the working directory (`/data` in the
image) once a minute and once more on shutdown, and read back at startup:

```bash
docker compose exec balancer cat /data/quotas.json
```

```json
{
  "publicnode": { "used": 0, "period_start": "2026-08-01" },
  "solana-mainnet-beta": { "used": 1, "period_start": "2026-08-01" }
}
```

The named `quotas` volume in the compose file is what makes this survive
`docker compose down`. Without it, every restart would re-open a quota the
provider still considers spent. `docker compose down -v` deletes it — which is
also how you reset this example.

`period_start` is stored next to the counter rather than recomputed on read, so
a restart that spans a billing boundary resets before it serves its first
request instead of a minute later.

## Hot reload

`config.toml` is bind-mounted, so editing it on the host changes the file the
container sees. Applying it takes a signal, not a restart:

```bash
docker kill -s HUP rpc-lb-basic-balancer-1
```

Both the balancer config and the pricing files are re-read. If the new
configuration does not build — a missing pricing file, an unset `$VAR`, a
`reset_day` outside `1..=31` — nothing is published and the running
configuration keeps serving; the error goes to the log.

Try it: change the tier-0 node's `monthly_limit` to `1`, send SIGHUP, and watch
subsequent requests go to `publicnode` instead. `quotas.json` is keyed by node
name, so the usage already recorded stays with the node.

## Stop

```bash
docker compose down      # keeps quotas.json in the named volume
docker compose down -v   # deletes it, resetting recorded usage
```

## Not in this example

Metrics, Prometheus and Grafana, multiple providers, API keys, and per-account
billing days — all of that is in
[`../full_observability`](../full_observability).
