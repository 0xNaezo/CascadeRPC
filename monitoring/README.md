# Local monitoring

The load balancer runs on the host at `0.0.0.0:3000`. Prometheus and Grafana
run in Docker and reach it through `host.docker.internal`.

## Start

From the repository root, prepare and run the load balancer:

```bash
cp config/config.example.toml config/config.toml
CONFIG_PATH=config/config.toml cargo run --release
```

Set any API-key environment variables referenced by `config/config.toml` before
starting the process. Keep `server.enable_metrics = true`.

In another terminal, start monitoring:

```bash
docker compose -f monitoring/docker-compose.yml up -d
```

- Prometheus: <http://localhost:9090>
- Grafana: <http://localhost:3001>
- Grafana login: `admin` / `admin`

Override the local Grafana password when needed:

```bash
GRAFANA_ADMIN_PASSWORD=change-me docker compose -f monitoring/docker-compose.yml up -d
```

The Prometheus datasource and `RPC Load Balancer` dashboard are provisioned
automatically.

## Verify

```bash
curl --fail http://localhost:3000/metrics
curl --fail http://localhost:9090/-/ready
curl --fail http://localhost:3001/api/health
docker compose -f monitoring/docker-compose.yml ps
```

Validate the Prometheus configuration without starting the stack:

```bash
docker run --rm --entrypoint promtool \
  -v "$PWD/monitoring/prometheus.yml:/etc/prometheus/prometheus.yml:ro" \
  prom/prometheus:v3.5.0 check config /etc/prometheus/prometheus.yml
```

## Stop

```bash
docker compose -f monitoring/docker-compose.yml down
```

Named volumes keep dashboards and metrics between restarts. To delete local
monitoring data, use `docker compose -f monitoring/docker-compose.yml down -v`.
