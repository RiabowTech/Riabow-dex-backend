<div align="center">

# Riabow Exchange

**A next-generation decentralized exchange engineered in Rust for institutional-grade performance.**

[![Rust](https://img.shields.io/badge/Rust-1.75%2B-orange?logo=rust)](https://www.rust-lang.org/)
[![Axum](https://img.shields.io/badge/Axum-0.7-blue)](https://github.com/tokio-rs/axum)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15%2B-336791?logo=postgresql)](https://www.postgresql.org/)
[![Redis](https://img.shields.io/badge/Redis-7%2B-DC382D?logo=redis)](https://redis.io/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](./CONTRIBUTING.md)

[Documentation](#documentation) · [Quick Start](#quick-start) · [Architecture](#architecture) · [API Reference](#api-reference) · [Contributing](#contributing)

</div>

---

## Overview

**Riabow Exchange** is a fully featured, non-custodial decentralized exchange backend built from the ground up in Rust. It delivers a CEX-grade trading experience — sub-millisecond order matching, deep liquidity, real-time market data, and advanced derivatives — while preserving the trust-minimization, on-chain settlement, and self-custody guarantees of DeFi.

Designed for both **spot** and **perpetual futures** markets, Riabow combines a lock-free in-memory matching engine with EIP-712 signed orders, on-chain settlement, and a unified margin system to power the next generation of decentralized trading venues.

> **Why Riabow?** Most DEXes force a tradeoff between performance and decentralization. Riabow refuses that compromise: a Rust core delivers institutional throughput, while every order is cryptographically signed and every settlement is verifiable on-chain.

## ✨ Key Features

### Trading Engine
- ⚡ **High-Performance Matching Engine** — Lock-free, in-memory order book powered by `DashMap` and `parking_lot`, with strict price-time priority and async persistence.
- 📈 **Spot & Perpetual Markets** — Full support for spot trading and perpetual futures with isolated and cross-margin modes.
- 🎯 **Advanced Order Types** — Limit, Market, Take-Profit (TP), Stop-Loss (SL), trigger orders, and conditional execution with client-side `client_order_id` tracking.
- 📊 **Unified Margin System** — Capital-efficient cross-collateralization across markets with real-time risk evaluation.
- 🔒 **Configurable Per-Market Leverage** — User-defined leverage with market-level risk caps and open-interest controls.

### Risk & Settlement
- 🛡️ **Liquidation Engine** — Automated liquidation with insurance-fund integration and configurable maintenance margins.
- 🔄 **Auto-Deleveraging (ADL)** — Fair, rank-based ADL queue to handle systemic risk events.
- 💰 **Funding Rate Mechanism** — Continuous funding-rate computation with index-price oracle integration.
- 📉 **Realized & Unrealized PnL Tracking** — Trade-level PnL attribution with accumulated trading-fee accounting.
- 🏦 **Protocol Fee Ledger** — On-chain-verifiable fee ledger with full auditability.

### On-Chain Integration
- 🔐 **EIP-712 Signed Orders** — Every order, deposit, and withdrawal is cryptographically signed by the user's wallet.
- ⛓️ **Non-Custodial Settlement** — Funds settle on-chain; the backend never holds private keys.
- 🌉 **Multi-Chain Ready** — Built on `ethers-rs` and `alloy` with primitives for any EVM-compatible network.
- ✍️ **Withdrawal Signer Service** — Dedicated, isolated signing service for withdrawal authorization.

### Real-Time Infrastructure
- 🔌 **WebSocket Streams** — Sub-second order-book deltas, trade ticks, kline updates, and account events.
- 📡 **External Market Data Proxy** — Built-in Binance kline sync and price-feed aggregation for index pricing.
- 🕯️ **K-Line Aggregator** — Real-time candlestick computation across all standard timeframes.
- 📬 **Listen Key Auth** — Developer-friendly signed listen keys for private WebSocket streams.

### Growth & Tokenomics
- 🏆 **VIP Tier System** — Volume-based tier progression with dynamic fee rebates.
- 🎁 **Referral & Commission Engine** — Multi-tier referral graph with daily incremental leaderboards.
- 🪙 **Points System** — On-chain-redeemable points with idempotent earn events.
- 💎 **NFT-Boosted Earn** — NFT-based reward multipliers for trading and staking.
- 🌐 **RWA Module** — First-class support for tokenized real-world-asset markets.

### Operations
- 🔭 **Prometheus Metrics** — End-to-end observability with custom counters, histograms, and panic instrumentation.
- 🧯 **Process-Wide Panic Hook** — Catches and labels every Tokio task panic for full visibility.
- 🚦 **Configuration Validator** — Fail-fast startup validation across all environments.
- 🧪 **Reaper & Reconciler** — Background workers that reconcile balances and clean stale state.
- 📨 **Alerting** — Built-in SMTP alerting via `lettre` for incident response.

## 🏗️ Architecture

Riabow is structured as a modular, service-oriented Rust monolith — a deliberate choice that combines microservice clarity with the performance and operational simplicity of a single binary.

```
                            ┌───────────────────────────────────┐
                            │        Client Applications        │
                            │  (Web · Mobile · Bots · Partners) │
                            └─────────────────┬─────────────────┘
                                              │
                          REST (HTTPS) │ WebSocket (WSS) │ EIP-712
                                              │
┌─────────────────────────────────────────────▼──────────────────────────────────────────────┐
│                                  Axum HTTP/WS Gateway                                       │
│           Routing · Auth (JWT + API Key) · Rate Limiting · CORS · Compression               │
└─────────────────┬──────────────────────────────────┬───────────────────────────────────────┘
                  │                                  │
   ┌──────────────▼──────────────┐    ┌──────────────▼──────────────┐
   │     Order Flow Orchestrator │    │   WebSocket Channel Hub     │
   │  ───────────────────────────│    │  ───────────────────────────│
   │  • EIP-712 verification     │    │  • Per-symbol fan-out       │
   │  • Risk pre-checks          │    │  • Account streams          │
   │  • Margin engine            │    │  • External data proxy      │
   └──────────────┬──────────────┘    └──────────────┬──────────────┘
                  │                                  │
   ┌──────────────▼─────────────────────────────────▼──────────────┐
   │                    In-Memory Matching Engine                   │
   │  Lock-free DashMap · Price-Time Priority · Async Persistence   │
   └──────────────┬─────────────────────────────────────────────────┘
                  │
   ┌──────────────▼─────────────────────────────────────────────────┐
   │                       Service Layer                            │
   │  Matching · Position · Liquidation · ADL · Funding · Risk      │
   │  Unified Margin · Trigger Orders · MM Pool · VIP · Referral    │
   │  Points · Earn · RWA · Spot · Withdraw Signer · Keeper         │
   └──┬───────────────────────────────────────────┬─────────────────┘
      │                                           │
   ┌──▼──────────────────┐                ┌───────▼──────────────┐
   │  PostgreSQL (sqlx)  │                │   Redis (cluster)    │
   │  ─────────────────  │                │  ──────────────────  │
   │  Trades · Orders    │                │  Cache · Pub/Sub     │
   │  Positions · Ledger │                │  Rate-limit buckets  │
   └─────────────────────┘                └──────────────────────┘
                  │
   ┌──────────────▼─────────────────────────────────────────────────┐
   │                      EVM Settlement Layer                      │
   │      Withdrawal Signing · Deposit Detection · Fee Ledger       │
   └────────────────────────────────────────────────────────────────┘
```

### Project Layout

```
backend/
├── src/
│   ├── api/              # HTTP handlers, routes, middleware
│   ├── app/              # Bootstrap, workers, router, application state
│   ├── auth/             # JWT · API key · listen-key auth
│   ├── cache/            # Redis adapters and caching primitives
│   ├── config/           # Environment-aware config + validator
│   ├── constants/        # EIP-712 domains and protocol constants
│   ├── db/               # Database pool and migration helpers
│   ├── i18n/             # Internationalization
│   ├── models/           # Domain models (Order, Position, Balance, …)
│   ├── services/         # Core business logic — see below
│   ├── utils/            # Shared utilities
│   ├── websocket/        # WS channels, handlers, external proxies
│   └── main.rs           # Tokio runtime entry point
├── migrations/           # Versioned SQL migrations
└── Cargo.toml
```

### Core Services

| Service              | Responsibility                                                       |
| -------------------- | -------------------------------------------------------------------- |
| `matching`           | In-memory matching engine, order book, orchestrator, retry logic     |
| `spot`               | Spot markets, wallet, blockchain settlement, reconciliation          |
| `position`           | Position lifecycle, margin updates, accumulated-fee accounting       |
| `liquidation`        | Maintenance-margin checks and forced-liquidation execution           |
| `adl`                | Auto-deleveraging queue and ranking                                  |
| `funding_rate`       | Continuous funding-rate calculation and settlement                   |
| `unified_margin`     | Cross-margin and isolated-margin accounting                          |
| `risk`               | Pre-trade and continuous risk evaluation                             |
| `trigger_orders`     | TP/SL and conditional-order execution                                |
| `mm_pool`            | Market-maker liquidity pool                                          |
| `vip_tier`           | Volume-based VIP tier progression and fee rebates                    |
| `referral`           | Referral graph, commission distribution, leaderboards                |
| `points` / `earn`    | Points, NFT-boosted earnings, idempotent reward events               |
| `rwa`                | Real-world-asset market support                                      |
| `withdraw`            | Withdrawal request lifecycle and signing                            |
| `keeper`             | Background keepers for liquidation, funding, and reconciliation      |
| `kline`              | Real-time candlestick aggregation                                    |
| `binance_kline_sync` | External-market kline synchronization                                |
| `price_feed`         | Index-price oracle aggregation                                       |
| `protocol_fee_ledger`| On-chain-verifiable fee accounting                                   |
| `alert`              | SMTP-based incident alerting                                         |
| `metrics`            | Prometheus instrumentation                                           |
| `sharding`           | Horizontal-sharding primitives                                       |

## 🚀 Quick Start

### Prerequisites

- **Rust** ≥ 1.75 (`rustup install stable`)
- **PostgreSQL** ≥ 15
- **Redis** ≥ 7 (cluster mode supported)
- **Node.js** ≥ 18 *(only if running the companion frontend)*

### 1. Clone the repository

```bash
git clone https://github.com/riabow/riabow-dex-backend.git
cd riabow-dex-backend/backend
```

### 2. Configure your environment

Copy the example file and fill in the required values:

```bash
cp .env.example .env
```

Minimum required variables:

```env
ENVIRONMENT=development
DATABASE_URL=postgres://user:password@localhost:5432/riabow
REDIS_URL=redis://localhost:6379
JWT_SECRET=<random-32-byte-secret>

# Blockchain
RPC_URL=https://mainnet.infura.io/v3/<key>
CHAIN_ID=1
VAULT_ADDRESS=0x...
REFERRAL_STORAGE_ADDRESS=0x...
REFERRAL_REBATE_ADDRESS=0x...

# Collateral (USDT by default)
COLLATERAL_TOKEN_ADDRESS=0x...
COLLATERAL_TOKEN_SYMBOL=USDT
COLLATERAL_TOKEN_DECIMALS=6

# EIP-712
EIP712_DOMAIN_NAME=Riabow Vault
EIP712_DOMAIN_VERSION=1

# Withdrawal signer
BACKEND_SIGNER_PRIVATE_KEY=<hex-encoded-private-key>
```

See [`.env.example`](./.env.example) for the full reference, including optional tuning knobs for the matching engine, cache TTLs, points system, and spot subsystem.

### 3. Run database migrations

```bash
cargo install sqlx-cli --no-default-features --features postgres
sqlx migrate run
```

### 4. Build and run

```bash
# Development
cargo run

# Production (LTO + opt-level 3)
cargo build --release
./target/release/ztdx-backend
```

The server will start on `http://0.0.0.0:8080` (configurable). Prometheus metrics are exposed at `/metrics`.

### 5. Verify the deployment

```bash
curl http://localhost:8080/health
```

## 📡 API Reference

Riabow exposes three primary surfaces:

### REST API

| Group              | Sample Endpoints                                                |
| ------------------ | --------------------------------------------------------------- |
| **Auth**           | `POST /api/auth/nonce`, `POST /api/auth/login`                  |
| **Account**        | `GET /api/account`, `GET /api/account/stats`                    |
| **Wallet**         | `GET /api/wallet/balance`, `POST /api/deposit`                  |
| **Markets**        | `GET /api/markets`, `GET /api/markets/{symbol}`                 |
| **Orders**         | `POST /api/orders`, `DELETE /api/orders/{id}`, `GET /api/orders`|
| **Positions**      | `GET /api/positions`, `POST /api/positions/leverage`            |
| **Trigger Orders** | `POST /api/trigger-orders`, `GET /api/trigger-orders`           |
| **K-Line**         | `GET /api/klines?symbol=...&interval=1m`                        |
| **Funding Rate**   | `GET /api/funding-rate/{symbol}`                                |
| **Withdraw**       | `POST /api/withdraw`, `GET /api/withdraw/history`               |
| **Referral**       | `GET /api/referral`, `GET /api/referral/leaderboard`            |
| **Earn / Points**  | `GET /api/earn`, `GET /api/points`                              |
| **Developer**      | `POST /api/developer/listen-key`, `POST /api/developer/orders`  |

### WebSocket Streams

```
wss://api.riabow.exchange/ws

# Public channels
SUB orderbook@BTCUSDT
SUB trades@BTCUSDT
SUB kline@BTCUSDT_1m
SUB ticker@BTCUSDT

# Private channels (require listen key)
SUB account@<listen_key>
SUB orders@<listen_key>
SUB positions@<listen_key>
```

### EIP-712 Order Signing

Every order is signed with EIP-712 typed data:

```json
{
  "domain": {
    "name": "Riabow Exchange",
    "version": "1",
    "chainId": 1
  },
  "types": {
    "Order": [
      { "name": "trader",     "type": "address" },
      { "name": "symbol",     "type": "string"  },
      { "name": "side",       "type": "uint8"   },
      { "name": "orderType",  "type": "uint8"   },
      { "name": "quantity",   "type": "uint256" },
      { "name": "price",      "type": "uint256" },
      { "name": "nonce",      "type": "uint256" },
      { "name": "expiry",     "type": "uint256" }
    ]
  }
}
```

## 📊 Observability

Riabow ships with first-class observability out of the box:

- **Prometheus** — `GET /metrics` exposes counters, histograms, and gauges across the matching engine, persistence layer, and HTTP surface.
- **Structured Logging** — JSON-formatted `tracing` output suitable for Loki, Datadog, or any modern log aggregator.
- **Panic Telemetry** — Process-wide panic hook tags every Tokio task panic with thread name and stack — no more silent worker deaths.
- **Health Probes** — `/health` and `/ready` endpoints for Kubernetes liveness and readiness.

## 🔐 Security

Security is a non-negotiable pillar of Riabow:

- **Non-Custodial by Design** — Backend services never hold user private keys.
- **EIP-712 Everywhere** — All trade-impacting actions require typed-data signatures.
- **Withdrawal Isolation** — Signing service runs as a separate, hardened process with its own key material.
- **Idempotent Earn Events** — Reward issuance protected against replay via on-chain-anchored idempotency keys.
- **Rate Limiting & DDoS Protection** — Tower middleware with per-route, per-IP, and per-account limits.
- **Audited Dependencies** — `cargo audit` enforced in CI.

> Found a security issue? Please **do not open a public issue.** Email `security@riabow.exchange` — see [SECURITY.md](./SECURITY.md) for our disclosure policy.

## 🧪 Testing

```bash
# Unit tests
cargo test

# Integration tests (requires Postgres + Redis)
cargo test --test integration -- --test-threads=1

# Lint and format
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

## 🛣️ Roadmap

- [x] Spot & Perpetual matching engine
- [x] Unified margin & cross-collateralization
- [x] EIP-712 signed orders & on-chain settlement
- [x] VIP tier, referral, and points systems
- [x] RWA market module
- [ ] Multi-chain settlement (Arbitrum · Base · Solana via bridge)
- [ ] On-chain order book with proof-of-matching
- [ ] Native options market
- [ ] zk-Proof of Solvency
- [ ] Decentralized matching via threshold-signed sequencer

## 🤝 Contributing

We love contributions of every shape and size. Whether you're fixing a typo, proposing a new market type, or building out a brand-new module — we'd love to have you.

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-thing`)
3. Commit your changes with conventional commits
4. Run `cargo fmt && cargo clippy && cargo test`
5. Open a Pull Request

Please read [CONTRIBUTING.md](./CONTRIBUTING.md) and our [Code of Conduct](./CODE_OF_CONDUCT.md) before getting started.

## 📚 Documentation

- 📖 [API Reference](./docs/api.md)
- 🏛️ [Architecture Deep Dive](./docs/architecture.md)
- 🔧 [Operations Runbook](./docs/operations.md)
- 🔐 [Security Model](./docs/security.md)
- 🧩 [Module Guides](./docs/modules/)

## 🛠️ Built With

- **[Rust](https://www.rust-lang.org/)** — Memory-safe, fearless concurrency
- **[Axum](https://github.com/tokio-rs/axum)** — Ergonomic, modular web framework
- **[Tokio](https://tokio.rs/)** — Asynchronous runtime
- **[SQLx](https://github.com/launchbadge/sqlx)** — Compile-time-checked SQL
- **[Redis](https://redis.io/)** — In-memory data and pub/sub
- **[Ethers-rs](https://github.com/gakonst/ethers-rs)** & **[Alloy](https://github.com/alloy-rs/alloy)** — EVM integration
- **[DashMap](https://github.com/xacrimon/dashmap)** — Lock-free concurrent maps
- **[Prometheus](https://prometheus.io/)** — Metrics and monitoring

## 📄 License

Riabow Exchange is licensed under the **MIT License**. See [LICENSE](./LICENSE) for the full text.

## 💬 Community

- 🌐 **Website** — [riabow.exchange](https://riabow.exchange)
- 🐦 **Twitter / X** — [@RiabowExchange](https://twitter.com/RiabowExchange)
- 💬 **Discord** — [discord.gg/riabow](https://discord.gg/riabow)
- 📨 **Email** — `hello@riabow.exchange`

---

<div align="center">

**Built with ❤️ and 🦀 by the Riabow team and the global open-source community.**

*If Riabow is useful to you, please consider giving us a ⭐ — it helps more than you might think.*

</div>
