//! Market-Maker Points Pool (PRD §2.5 / §5.3).
//!
//! Independent of the user points pool. Every `INTERVAL_SECS` we
//! sample each whitelisted MM's quality across 4 dimensions:
//!
//!   maker_volume_usd  (40%) — Σ trades since last snapshot
//!   spread_bps         (25%) — best ask − best bid on that symbol
//!                              when the MM is quoting both sides
//!   depth_usd          (20%) — Σ open-order notional value
//!   uptime             (15%) — 1.0 if MM has any open order, else 0
//!
//! The composite `quality_score` is the weighted sum of normalized
//! values; the per-tick sum is appended to `mm_points_balance`.

pub mod scoring;
pub mod worker;

pub use worker::spawn;
