//! Unified Margin (Portfolio Margin) service.
//!
//! Modules:
//!   * `calculator`   — pure equity / IM / MM / uniMMR computation
//!                      (with per-symbol tier ladder support).
//!   * `tiers`        — in-memory MarginTier store hot-reloaded from DB.
//!   * `risk_worker`  — 2 s tick: persists snapshots, drives state
//!                      machine, fires `enforcement` and `liquidation`.
//!   * `enforcement`  — reduce_only auto-cancel of open orders.
//!   * `liquidation`  — forced liquidation waterfall via the shared
//!                      `LiquidationService` (insurance fund + ADL).
//!
//! See design_docs/08_统一保证金模式设计.md.

pub mod calculator;
pub mod enforcement;
pub mod liquidation;
pub mod risk_worker;
pub mod tiers;

pub use calculator::{
    compute_risk, compute_risk_with_tiers, simulate_open, simulate_open_with_tiers,
    SimulateResult, MMR_DEFAULT,
};
pub use tiers::{TierStore, TierStoreHandle};
