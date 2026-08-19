# Execution Plan & Milestones

The core high-performance engine of CascadeRPC is already built and validated. This grant will fund the transition from a bare-metal routing proxy into a production-grade infrastructure layer. Resources will be explicitly allocated to implement Solana-native features (WebSockets, CU-budgeting), conduct rigorous cloud-based stress tests, and develop the TypeScript SDK to ensure frictionless ecosystem adoption

## Milestone 1: Stateful Architecture & Intelligent HTTP Routing

- **Deliverables:**
  - [x] **Persistent Quota Engine (Anti-Bill-Shock):** Integrating a local embedded database to persist monthly API usage (CUs/Credits) across restarts. Crucial for protecting developers from unexpected overage billing on premium tiers.
  - [ ] **Zero-Cost Passive Probing:** Overhauling the health check system. Instead of active pings that burn API credits, the proxy will passively monitor real user traffic, calculating Exponential Moving Average (EMA) for latency and instantly penalizing nodes on HTTP 429 / 5xx errors.
  - [ ] **Compute-Unit (CU) Aware Routing:** Implementing dynamic traffic distribution based on RPC method weights. E.g., routing expensive getProgramAccounts to unlimited public tiers while reserving premium Helius limits for latency-critical sendTransaction calls.

## Milestone 2: Complex Payloads & Protocol Expansion

- **Deliverables:**
  - [ ] **Intelligent JSON-RPC Batching Execution:** Building a parallel execution engine. When a batched array ([{}, {}]) is received, the proxy will deconstruct the payload, route individual queries to the most optimal nodes concurrently, and perfectly reconstruct the JSON response array for the client.
  - [ ] **Stateful WebSockets Proxying (WSS):** Expanding the architecture beyond stateless HTTP to support persistent WebSocket connections. Managing WSS handshakes, connection pooling, and silent failovers for real-time Solana dApps (e.g., accountSubscribe).

## Milestone 3: Production Hardening & Benchmarking

- **Deliverables:**
  - [ ] **Cloud Infrastructure & Tier Provisioning:** Deploying CascadeRPC on geographically distributed servers (AWS/Hetzner) and acquiring premium API tiers (Helius, Alchemy, QuickNode) to establish an authentic, cross-region testing environment
  - [ ] **TCP/TLS Tuning & Resilience Hardening:** Upgrading the Tokio network stack to handle production realities. This includes connection pool optimization, managing TLS handshake CPU spikes, and implementing aggressive circuit breakers for HTTP 429/500 provider errors to prevent memory leaks and thread exhaustion.
  - [ ] **Production Load Report Publication:** Releasing a comprehensive, reproducible benchmark article detailing throughput, end-to-end latency vs. localhost, and actual CU cost savings under production-grade stress tests.

## Milestone 4: Developer Ecosystem (SDK & Onboarding)

- **Deliverables:**
  - [ ] **TypeScript SDK Release (npm):** Developing and publishing cascade-rpc-client. 100% drop-in compatibility with the standard @solana/web3.js Connection object. Developers can migrate existing dApps and bots with zero business-logic rewrites.
  - [ ] **Ecosystem Onboarding (PoC Partnerships):** Actively onboarding 2-3 real Solana projects (e.g., trading bots, indexers, or DeFi frontends) to replace their current RPC setup with CascadeRPC. Gathering feedback and proving real-world adoption.
  - [ ] **Comprehensive Docs & Ops Guides:** Writing step-by-step production deployment guides (Docker-compose, Nginx/Traefik setups) and documenting best practices for leveraging the open-sourced Grafana observability dashboards.
