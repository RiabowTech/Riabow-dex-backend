//! Unified Margin Account model
//!
//! See design_docs/08_统一保证金模式设计.md §2.3 / §4.2

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use std::fmt;
use std::str::FromStr;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnifiedAccountStatus {
    Normal,
    Warning1,
    Warning2,
    ReduceOnly,
    Liquidating,
}

impl Default for UnifiedAccountStatus {
    fn default() -> Self {
        UnifiedAccountStatus::Normal
    }
}

impl fmt::Display for UnifiedAccountStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            UnifiedAccountStatus::Normal => "normal",
            UnifiedAccountStatus::Warning1 => "warning_1",
            UnifiedAccountStatus::Warning2 => "warning_2",
            UnifiedAccountStatus::ReduceOnly => "reduce_only",
            UnifiedAccountStatus::Liquidating => "liquidating",
        };
        write!(f, "{}", s)
    }
}

impl FromStr for UnifiedAccountStatus {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "normal" => Ok(Self::Normal),
            "warning_1" => Ok(Self::Warning1),
            "warning_2" => Ok(Self::Warning2),
            "reduce_only" => Ok(Self::ReduceOnly),
            "liquidating" => Ok(Self::Liquidating),
            other => Err(format!("invalid unified_account_status: {}", other)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct UnifiedMarginAccount {
    pub id: Uuid,
    pub user_address: String,
    pub total_equity: Decimal,
    pub available_balance: Decimal,
    pub total_initial_margin: Decimal,
    pub total_maint_margin: Decimal,
    pub total_unrealized_pnl: Decimal,
    pub uni_mmr: Option<Decimal>,
    /// Stored as VARCHAR in DB — caller parses to enum via FromStr.
    pub account_status: String,
    pub is_reduce_only: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Snapshot of live-computed unified-margin risk metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnifiedRiskSnapshot {
    /// Wallet balance (available + frozen of collateral token).
    pub wallet_balance: Decimal,
    pub total_unrealized_pnl: Decimal,
    pub total_accumulated_fees: Decimal,
    pub total_equity: Decimal,
    pub total_initial_margin: Decimal,
    pub total_maint_margin: Decimal,
    /// None when account has no open positions (division-by-zero guard).
    pub uni_mmr: Option<Decimal>,
    pub available_balance: Decimal,
    pub account_status: UnifiedAccountStatus,
    /// Symbols for which the mark-price lookup failed during this
    /// snapshot. The risk worker uses this to (a) emit a per-symbol
    /// metric/log, and (b) refuse to clear `reduce_only` until prices
    /// recover. PnL for missing-price positions falls back to
    /// `entry_price` (zero unrealized) — that's a numeric fallback,
    /// NOT a risk-blind one.
    #[serde(default)]
    pub missing_mark_symbols: Vec<String>,
}
