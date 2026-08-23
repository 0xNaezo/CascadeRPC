# Examples

Two runnable deployments, both containerized, both built from the repository
root [`Dockerfile`](../Dockerfile).

| Example                                    | Nodes                         | Metrics              | API keys needed |
| ------------------------------------------ | ----------------------------- | -------------------- | --------------- |
| [`basic_setup`](basic_setup)               | 2 public endpoints, 2 tiers   | off                  | none            |
| [`full_observability`](full_observability) | 5 nodes, 3 tiers, 2 providers | Prometheus + Grafana | 4               |

Start with `basic_setup`: it runs on `docker compose up` with no secrets and
explains the config fields and the routing rules. `full_observability` builds on
it and adds the instrumentation.

## What both examples share

**One image.** Built from the sources in this checkout. The binary runs as an
unprivileged user with `/data` as its working directory, which is where
`quotas.json` is written and why each example mounts a named volume there.

**Container paths.** Each example mounts its own `config.toml` at
`/etc/rpc-lb/config.toml` and points `CONFIG_PATH` at it. The balancer has no
default config location and refuses to start without that variable.

**Shared pricing files.** Neither example carries a copy of a provider's price
list. Both mount [`../config/provider_config`](../config/provider_config)
read-only at `/etc/rpc-lb/provider_config`, and their configs reference files
inside it.

That split is deliberate. What a method costs is a fact about the provider —
the same for every deployment, changing only when the provider changes its
prices. The plan size, the rate limit, the billing day and the API key are facts
about your account, and those live in each example's `config.toml`. Copying the
tables per example would mean three sources of truth for one provider's prices,
and they would drift apart on the first price change.

The one thing that sits on the wrong side of that line is `spillover_percent`,
which lives in the pricing file although it reads more like a deployment knob:
two nodes sharing a pricing file share their reserve percentage. Giving them
different reserves means giving them different files.

**Loopback only.** Every published port is bound to `127.0.0.1`.
`/send-request` is an unauthenticated proxy to your paid upstreams, and
`/metrics` reports what they have cost.

## Related

- [`../monitoring`](../monitoring) — Prometheus and Grafana pointed at a
  balancer running on the host rather than in a container.
- [`../config/balancer_config/config.example.toml`](../config/balancer_config/config.example.toml)
  — the annotated reference config for a non-containerized run.
