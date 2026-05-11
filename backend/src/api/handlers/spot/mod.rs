//! Spot HTTP handlers.
//!
//! Mounted at `/api/v1/spot/*` only when `Config::spot` is `Some`
//! (i.e., SPOT_ENABLED=true at startup).
//! See spec: docs/superpowers/specs/2026-05-09-spot-df-wallet-design.md

pub mod balances;
pub mod deposits;
pub mod orders;
pub mod transfer;
pub mod withdraw;
pub mod market_data;
pub mod health;
