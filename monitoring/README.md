# Local monitoring

The load balancer runs on the host at `0.0.0.0:3000`. Prometheus and Grafana
run in Docker and reach it through `host.docker.internal`.

## Start

From the repository root, prepare and run the load balancer:

```bash
cp config/balancer_config/config.example.toml config/balancer_config/config.toml
CONFIG_PATH=config/balancer_config/config.toml cargo run --release
```

Set any API-key environment variables referenced by `config/balancer_config/config.toml` before
starting the process. Metrics are disabled by default for security; set `server.enable_metrics = true` only behind an internal network boundary.

In another terminal, start monitoring (a strong `GRAFANA_ADMIN_PASSWORD` is required):

```bash
GRAFANA_ADMIN_PASSWORD=<your-password> docker compose -f monitoring/docker-compose.yml up -d
```

- Prometheus: <http://localhost:9090>
- Grafana: <http://localhost:3001>

The Prometheus datasource and `CascadeRPC` dashboard are provisioned
automatically.

## Verify

```bash
# requires server.enable_metrics = true
# curl --fail http://localhost:3000/metrics
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
