//! Protocol fee ledger helper.
//!
//! Single-entry helper for inserting rows into `protocol_fee_ledger`.
//! Callers (position close, funding settlement, liquidation) supply the
//! user / position / trade context plus a fee_type + signed amount;
//! this module owns the SQL and JSON metadata shape.
//!
//! Spec: docs/superpowers/specs/2026-04-29-fee-unification-and-protocol-revenue-ledger-design.md

use rust_decimal::Decimal;
use serde::Serialize;
use sqlx::{Executor, Postgres};
use uuid::Uuid;

/// Persisted strings for `protocol_fee_ledger.fee_type`. Locked by a
/// unit test below — these values are stored on disk and must not
/// silently drift without a data migration.
#[derive(Debug, Clone, Copy)]
pub enum FeeType {
    TradingFee,
    FundingFee,
    BorrowingFee,
    LiquidationFee,
    InsuranceContribution,
    LiquidatorReward,
    BootstrapPreMigration,
}

impl FeeType {
    pub fn as_str(&self) -> &'static str {
        match self {
            FeeType::TradingFee             => "trading_fee",
            FeeType::FundingFee             => "funding_fee",
            FeeType::BorrowingFee           => "borrowing_fee",
            FeeType::LiquidationFee         => "liquidation_fee",
            FeeType::InsuranceContribution  => "insurance_contribution",
            FeeType::LiquidatorReward       => "liquidator_reward",
            FeeType::BootstrapPreMigration  => "bootstrap_pre_migration",
        }
    }
}

/// Insert one row into `protocol_fee_ledger`. Caller passes the executor
/// (typically a `&mut Transaction`) so the write happens atomically with
/// the underlying balance/position change.
///
/// Returns `Ok(Some(id))` for a successful insert, or `Ok(None)` when
/// the amount is zero — zero-amount events are silently dropped to keep
/// the ledger free of no-op rows (e.g. borrowing fee with rate=0).
///
/// `user_address` is normalised to lowercase before insert so per-user
/// reconciliation queries don't have to worry about case.
pub async fn record_fee_event<'e, E, M>(
    executor: E,
    user_address: &str,
    position_id: Option<Uuid>,
    trade_id: Option<Uuid>,
    fee_type: FeeType,
    amount: Decimal,
    metadata: &M,
) -> Result<Option<Uuid>, sqlx::Error>
where
    E: Executor<'e, Database = Postgres>,
    M: Serialize,
{
    if amount.is_zero() {
        return Ok(None);
    }

    let metadata_json =
        serde_json::to_value(metadata).unwrap_or(serde_json::json!({}));

    let id: Uuid = sqlx::query_scalar(
        r#"
        INSERT INTO protocol_fee_ledger
            (user_address, position_id, trade_id, fee_type, amount, asset, metadata)
        VALUES ($1, $2, $3, $4, $5, 'USDT', $6)
        RETURNING id
        "#,
    )
    .bind(user_address.to_lowercase())
    .bind(position_id)
    .bind(trade_id)
    .bind(fee_type.as_str())
    .bind(amount)
    .bind(metadata_json)
    .fetch_one(executor)
    .await?;

    Ok(Some(id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn fee_type_strings_are_stable() {
        // Lock the on-disk values — these strings are persisted and
        // must not silently change without a data migration.
        assert_eq!(FeeType::TradingFee.as_str(),            "trading_fee");
        assert_eq!(FeeType::FundingFee.as_str(),            "funding_fee");
        assert_eq!(FeeType::BorrowingFee.as_str(),          "borrowing_fee");
        assert_eq!(FeeType::LiquidationFee.as_str(),        "liquidation_fee");
        assert_eq!(FeeType::InsuranceContribution.as_str(), "insurance_contribution");
        assert_eq!(FeeType::LiquidatorReward.as_str(),      "liquidator_reward");
        assert_eq!(FeeType::BootstrapPreMigration.as_str(), "bootstrap_pre_migration");
    }

    #[test]
    fn zero_amount_is_recognised() {
        // record_fee_event short-circuits on this check; this test
        // documents the contract for callers — borrowing_fee with rate=0
        // expects None so the caller doesn't have to filter.
        assert!(dec!(0).is_zero());
        assert!(!dec!(0.0001).is_zero());
        assert!(!dec!(-0.0001).is_zero());
    }
}
