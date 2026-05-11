#![allow(dead_code)]
//! Liquidation Engine Service
//!
//! Implements GMX V2-style liquidation logic with insurance fund mechanism.
//! The liquidation engine runs as a background task and periodically checks
//! all open positions for liquidation conditions.

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use rust_decimal::prelude::*;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool};
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::models::{Position, PositionSide, PositionStatus};
use crate::services::alert::AlertService;
use crate::services::position::PositionService;
use crate::services::price_feed::PriceFeedService;

/// Liquidation configuration for a market
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct LiquidationConfig {
    pub symbol: String,
    pub liquidation_fee_rate: Decimal,
    pub max_leverage: i32,
    pub maintenance_margin_rate: Decimal,
    pub min_collateral_usd: Decimal,
    pub insurance_fund_fee_rate: Decimal,
    pub max_insurance_payout_rate: Decimal,
    pub liquidator_reward_rate: Decimal,
}

impl Default for LiquidationConfig {
    fn default() -> Self {
        Self {
            symbol: "BTCUSDT".to_string(),
            liquidation_fee_rate: Decimal::from_str("0.005").unwrap(),      // 0.5%
            max_leverage: 50,
            maintenance_margin_rate: Decimal::from_str("0.005").unwrap(),   // 0.5%
            min_collateral_usd: Decimal::from_str("10").unwrap(),           // $10
            insurance_fund_fee_rate: Decimal::from_str("0.001").unwrap(),   // 0.1%
            max_insurance_payout_rate: Decimal::from_str("0.5").unwrap(),   // 50%
            liquidator_reward_rate: Decimal::from_str("0.001").unwrap(),    // 0.1%
        }
    }
}

/// Liquidation record
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct LiquidationRecord {
    pub id: Uuid,
    pub position_id: Uuid,
    pub user_address: String,
    pub symbol: String,
    pub side: String,
    pub position_size_usd: Decimal,
    pub position_size_tokens: Decimal,
    pub collateral_amount: Decimal,
    pub entry_price: Decimal,
    pub liquidation_price: Decimal,
    pub mark_price: Decimal,
    pub remaining_collateral: Decimal,
    pub liquidation_fee: Decimal,
    pub insurance_fund_contribution: Decimal,
    pub pnl: Decimal,
    pub liquidator_address: Option<String>,
    pub liquidator_reward: Decimal,
    pub liquidated_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

/// Insurance fund balance
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct InsuranceFund {
    pub id: Uuid,
    pub symbol: String,
    pub balance: Decimal,
    pub total_contributions: Decimal,
    pub total_payouts: Decimal,
    pub updated_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

/// Liquidation result for API response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiquidationResult {
    pub position_id: Uuid,
    pub user_address: String,
    pub symbol: String,
    pub side: PositionSide,
    pub position_size_usd: Decimal,
    pub collateral_returned: Decimal,
    pub pnl: Decimal,
    pub liquidation_fee: Decimal,
    pub insurance_fund_contribution: Decimal,
}

/// Statistics about liquidation run
#[derive(Debug, Clone, Default)]
pub struct LiquidationStats {
    pub positions_checked: usize,
    pub positions_liquidated: usize,
    pub total_volume_liquidated: Decimal,
    pub total_fees_collected: Decimal,
    pub insurance_fund_contributions: Decimal,
    pub insurance_fund_payouts: Decimal,
}

/// Liquidation Engine Service
pub struct LiquidationService {
    pool: PgPool,
    position_service: Arc<PositionService>,
    price_feed_service: Arc<PriceFeedService>,
    alert_service: Arc<AlertService>,
    /// Cached configs per market
    configs: Arc<RwLock<std::collections::HashMap<String, LiquidationConfig>>>,
    /// Check interval in seconds
    check_interval_secs: u64,
}

impl LiquidationService {
    /// Create a new LiquidationService
    pub fn new(
        pool: PgPool,
        position_service: Arc<PositionService>,
        price_feed_service: Arc<PriceFeedService>,
        alert_service: Arc<AlertService>,
    ) -> Self {
        Self {
            pool,
            position_service,
            price_feed_service,
            alert_service,
            configs: Arc::new(RwLock::new(std::collections::HashMap::new())),
            check_interval_secs: 5, // Check every 5 seconds
        }
    }

    /// Get liquidation config for a symbol
    pub async fn get_config(&self, symbol: &str) -> Result<LiquidationConfig> {
        // Check cache first
        {
            let configs = self.configs.read().await;
            if let Some(config) = configs.get(symbol) {
                return Ok(config.clone());
            }
        }

        // Load from database
        let config: Option<LiquidationConfig> = sqlx::query_as::<_, LiquidationConfig>(
            r#"
            SELECT symbol, liquidation_fee_rate, max_leverage, maintenance_margin_rate,
                   min_collateral_usd, insurance_fund_fee_rate, max_insurance_payout_rate,
                   liquidator_reward_rate
            FROM liquidation_config
            WHERE symbol = $1
            "#
        )
        .bind(symbol)
        .fetch_optional(&self.pool)
        .await?;

        match config {
            Some(c) => {
                // Cache it
                let mut configs = self.configs.write().await;
                configs.insert(symbol.to_string(), c.clone());
                Ok(c)
            }
            None => {
                // Create default config
                let default = LiquidationConfig {
                    symbol: symbol.to_string(),
                    ..Default::default()
                };

                sqlx::query(
                    r#"
                    INSERT INTO liquidation_config (symbol, liquidation_fee_rate, max_leverage,
                        maintenance_margin_rate, min_collateral_usd, insurance_fund_fee_rate,
                        max_insurance_payout_rate, liquidator_reward_rate)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (symbol) DO NOTHING
                    "#
                )
                .bind(symbol)
                .bind(default.liquidation_fee_rate)
                .bind(default.max_leverage)
                .bind(default.maintenance_margin_rate)
                .bind(default.min_collateral_usd)
                .bind(default.insurance_fund_fee_rate)
                .bind(default.max_insurance_payout_rate)
                .bind(default.liquidator_reward_rate)
                .execute(&self.pool)
                .await?;

                Ok(default)
            }
        }
    }

    /// Get insurance fund balance for a symbol
    pub async fn get_insurance_fund(&self, symbol: &str) -> Result<InsuranceFund> {
        let fund: Option<InsuranceFund> = sqlx::query_as::<_, InsuranceFund>(
            r#"
            SELECT id, symbol, balance, total_contributions, total_payouts, updated_at, created_at
            FROM insurance_fund
            WHERE symbol = $1
            "#
        )
        .bind(symbol)
        .fetch_optional(&self.pool)
        .await?;

        match fund {
            Some(f) => Ok(f),
            None => {
                // Create new fund entry
                let id = Uuid::new_v4();
                sqlx::query(
                    r#"
                    INSERT INTO insurance_fund (id, symbol, balance, total_contributions, total_payouts)
                    VALUES ($1, $2, 0, 0, 0)
                    ON CONFLICT (symbol) DO NOTHING
                    "#
                )
                .bind(id)
                .bind(symbol)
                .execute(&self.pool)
                .await?;

                Ok(InsuranceFund {
                    id,
                    symbol: symbol.to_string(),
                    balance: Decimal::ZERO,
                    total_contributions: Decimal::ZERO,
                    total_payouts: Decimal::ZERO,
                    updated_at: Utc::now(),
                    created_at: Utc::now(),
                })
            }
        }
    }

    /// Check if a position should be liquidated
    pub fn should_liquidate(
        &self,
        position: &Position,
        mark_price: Decimal,
        config: &LiquidationConfig,
    ) -> bool {
        let remaining_collateral = self.calculate_remaining_collateral(position, mark_price);
        let min_collateral = position.size_in_usd * config.maintenance_margin_rate;

        remaining_collateral < min_collateral || remaining_collateral <= Decimal::ZERO
    }

    /// Calculate remaining collateral for a position
    fn calculate_remaining_collateral(&self, position: &Position, mark_price: Decimal) -> Decimal {
        // Calculate PnL
        let position_value = position.size_in_tokens * mark_price;
        let pnl = match position.side {
            PositionSide::Long => position_value - position.size_in_usd,
            PositionSide::Short => position.size_in_usd - position_value,
        };

        // Remaining = collateral + PnL - fees
        let fees = position.accumulated_funding_fee + position.accumulated_borrowing_fee;
        position.collateral_amount + pnl - fees
    }

    /// Execute liquidation for a single position
    /// Uses FOR UPDATE lock to prevent race conditions with close_position
    pub async fn execute_liquidation(
        &self,
        position: &Position,
        mark_price: Decimal,
        liquidator_address: Option<&str>,
    ) -> Result<LiquidationResult> {
        let config = self.get_config(&position.symbol).await?;

        // Begin transaction FIRST, then lock the position
        let mut tx = self.pool.begin().await?;

        // Lock and re-fetch the position to prevent race conditions
        let locked_position: Option<Position> = sqlx::query_as::<_, Position>(
            "SELECT * FROM positions WHERE id = $1 FOR UPDATE"
        )
        .bind(position.id)
        .fetch_optional(&mut *tx)
        .await?;

        let locked_position = match locked_position {
            Some(p) => p,
            None => {
                tracing::warn!("Position {} not found during liquidation (may have been closed)", position.id);
                return Err(anyhow!("Position not found"));
            }
        };

        // Check if position is still open - prevent double liquidation
        if locked_position.status != PositionStatus::Open {
            tracing::warn!(
                "Position {} already closed/liquidated during race condition check: status = {:?}",
                position.id, locked_position.status
            );
            return Err(anyhow!("Position already closed or liquidated"));
        }

        // Use locked position data for calculations
        let position = &locked_position;

        // Calculate values
        let position_value = position.size_in_tokens * mark_price;
        let pnl = match position.side {
            PositionSide::Long => position_value - position.size_in_usd,
            PositionSide::Short => position.size_in_usd - position_value,
        };

        let remaining_collateral = self.calculate_remaining_collateral(position, mark_price);

        // Calculate fees
        let liquidation_fee = position.size_in_usd * config.liquidation_fee_rate;
        let insurance_contribution = position.size_in_usd * config.insurance_fund_fee_rate;
        let liquidator_reward = if liquidator_address.is_some() {
            position.size_in_usd * config.liquidator_reward_rate
        } else {
            Decimal::ZERO
        };

        // Calculate amount returned to user (if any)
        let total_fees = liquidation_fee + insurance_contribution + liquidator_reward;
        let collateral_returned = (remaining_collateral - total_fees).max(Decimal::ZERO);

        // Check if insurance fund needs to cover bad debt
        let mut insurance_payout = Decimal::ZERO;
        if remaining_collateral < Decimal::ZERO {
            // Bad debt - insurance fund covers it
            let fund = self.get_insurance_fund(&position.symbol).await?;
            let max_payout = fund.balance * config.max_insurance_payout_rate;
            insurance_payout = remaining_collateral.abs().min(max_payout);
        }

        // Record liquidation
        let liquidation_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO liquidations (
                id, position_id, user_address, symbol, side,
                position_size_usd, position_size_tokens, collateral_amount,
                entry_price, liquidation_price, mark_price,
                remaining_collateral, liquidation_fee, insurance_fund_contribution,
                pnl, liquidator_address, liquidator_reward, liquidated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18
            )
            "#
        )
        .bind(liquidation_id)
        .bind(position.id)
        .bind(&position.user_address)
        .bind(&position.symbol)
        .bind(match position.side {
            PositionSide::Long => "long",
            PositionSide::Short => "short",
        })
        .bind(position.size_in_usd)
        .bind(position.size_in_tokens)
        .bind(position.collateral_amount)
        .bind(position.entry_price)
        .bind(position.liquidation_price)
        .bind(mark_price)
        .bind(remaining_collateral)
        .bind(liquidation_fee)
        .bind(insurance_contribution)
        .bind(pnl)
        .bind(liquidator_address)
        .bind(liquidator_reward)
        .bind(Utc::now())
        .execute(&mut *tx)
        .await?;

        // Update position status
        sqlx::query(
            r#"
            UPDATE positions SET
                status = 'liquidated',
                size_in_usd = 0,
                size_in_tokens = 0,
                collateral_amount = 0,
                realized_pnl = realized_pnl + $2,
                updated_at = NOW(),
                decreased_at = NOW()
            WHERE id = $1
            "#
        )
        .bind(position.id)
        .bind(pnl)
        .execute(&mut *tx)
        .await?;

        // Record realized PnL event so liquidations show up in the user's
        // Trade History / PnL aggregations. trade_id is NULL because
        // liquidations don't emit a `trades` row — /account/trades will
        // fall back to time-proximity JOIN (unchanged behavior).
        if let Err(e) = sqlx::query(
            r#"
            INSERT INTO realized_pnl_events (
                user_address, symbol, position_id, realized_pnl,
                execution_price, size_delta_usd, is_full_close, trade_id
            ) VALUES ($1, $2, $3, $4, $5, $6, TRUE, NULL)
            "#
        )
        .bind(&position.user_address)
        .bind(&position.symbol)
        .bind(position.id)
        .bind(pnl)
        .bind(mark_price)
        .bind(position.size_in_usd)
        .execute(&mut *tx)
        .await
        {
            tracing::warn!(
                "Failed to record liquidation realized_pnl_event for position {}: {}",
                position.id, e
            );
        }

        // Update insurance fund + record audit row in one atomic statement.
        // ON CONFLICT path is bit-equivalent to the prior standalone UPDATE; the
        // INSERT path covers symbols whose insurance_fund row hasn't been seeded
        // yet (previously failed with `null value in "balance_after"` and rolled
        // back the whole liquidation tx).
        if insurance_contribution > Decimal::ZERO || insurance_payout > Decimal::ZERO {
            let net_change = insurance_contribution - insurance_payout;
            sqlx::query(
                r#"
                WITH fund AS (
                    INSERT INTO insurance_fund
                        (symbol, balance, total_contributions, total_payouts)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (symbol) DO UPDATE SET
                        balance              = insurance_fund.balance              + EXCLUDED.balance,
                        total_contributions  = insurance_fund.total_contributions  + EXCLUDED.total_contributions,
                        total_payouts        = insurance_fund.total_payouts        + EXCLUDED.total_payouts,
                        updated_at           = NOW()
                    RETURNING balance
                )
                INSERT INTO insurance_fund_transactions
                    (id, symbol, transaction_type, amount, balance_after, liquidation_id, position_id)
                SELECT $5, $1, $6, $7, balance, $8, $9 FROM fund
                "#
            )
            .bind(&position.symbol)
            .bind(net_change)
            .bind(insurance_contribution)
            .bind(insurance_payout)
            .bind(Uuid::new_v4())
            .bind(if insurance_contribution > Decimal::ZERO { "contribution" } else { "payout" })
            .bind(if insurance_contribution > Decimal::ZERO { insurance_contribution } else { insurance_payout })
            .bind(liquidation_id)
            .bind(position.id)
            .execute(&mut *tx)
            .await?;
        }

        // Update user balance: unfreeze collateral and return remaining amount.
        // PR 2 (2026-04-29): the legacy `liq_opening_fee = size × 0.1%` term
        // was based on the assumption that opening_fee was held in frozen
        // throughout a position's lifetime. Post-fix, opening_fee is not
        // charged at all (spec §2.5), so there is nothing to release here.
        // Kept as Decimal::ZERO for one cycle so the GREATEST clamp on the
        // UPDATE keeps the transaction balanced. Cleanup block below sweeps
        // residual frozen for legacy liquidations.
        let liq_margin = position.size_in_usd / Decimal::from(position.leverage);
        let liq_buffer = liq_margin * Decimal::new(5, 3); // 0.5% buffer
        let liq_opening_fee = Decimal::ZERO;
        let total_frozen_release = position.collateral_amount + liq_buffer + liq_opening_fee;
        let total_available_add = collateral_returned + liq_buffer;

        sqlx::query(
            r#"
            UPDATE balances
            SET frozen = GREATEST(frozen - $1, 0),
                available = available + $2,
                updated_at = NOW()
            WHERE user_address = $3 AND token = 'USDT'
            "#
        )
        .bind(total_frozen_release)
        .bind(total_available_add)
        .bind(&position.user_address.to_lowercase())
        .execute(&mut *tx)
        .await?;

        tracing::info!(
            "Updated balance for user {} after liquidation: frozen -{} (collateral={}, buffer={}, open_fee={}), available +{}",
            position.user_address,
            total_frozen_release,
            position.collateral_amount,
            liq_buffer,
            liq_opening_fee,
            total_available_add
        );

        // Clean up residual frozen from price slippage (liquidation always fully closes)
        let cleanup = sqlx::query(
            r#"
            UPDATE balances
            SET available = available + frozen,
                frozen = 0,
                updated_at = NOW()
            WHERE user_address = $1 AND token = 'USDT' AND frozen > 0
              AND NOT EXISTS (
                SELECT 1 FROM positions
                WHERE user_address = $1 AND status = 'open' AND size_in_usd > 0
              )
              AND NOT EXISTS (
                SELECT 1 FROM orders
                WHERE user_address = $1 AND status = 'pending'
              )
            "#
        )
        .bind(&position.user_address.to_lowercase())
        .execute(&mut *tx)
        .await;

        if let Ok(r) = &cleanup {
            if r.rows_affected() > 0 {
                tracing::info!("Cleaned up residual frozen balance for user {} after liquidation", position.user_address);
            }
        }

        // PR 2 (2026-04-29): protocol_fee_ledger rows for liquidation.
        // - liquidation_fee:        positive → protocol revenue
        // - insurance_contribution: positive → goes to insurance fund (also
        //   recorded in insurance_fund_transactions; ledger row mirrors the
        //   user-side debit)
        // - liquidator_reward:      NEGATIVE → USDT physically leaves VAULT
        //   to the keeper wallet, so reconciliation must subtract it from
        //   protocol holdings.
        // Spec §2.3.
        {
            use crate::services::protocol_fee_ledger::{record_fee_event, FeeType};
            let metadata = serde_json::json!({
                "size_in_usd":           position.size_in_usd.to_string(),
                "liquidation_fee_rate":  config.liquidation_fee_rate.to_string(),
                "mark_price":            mark_price.to_string(),
                "liquidation_id":        liquidation_id.to_string(),
            });
            for (fee_type, amount) in [
                (FeeType::LiquidationFee,        liquidation_fee),
                (FeeType::InsuranceContribution, insurance_contribution),
                // Reward is paid OUT of VAULT to the keeper wallet → record signed negative.
                (FeeType::LiquidatorReward,      -liquidator_reward),
            ] {
                if let Err(e) = record_fee_event(
                    &mut *tx,
                    &position.user_address,
                    Some(position.id),
                    None, // liquidations have no trade_id
                    fee_type,
                    amount,
                    &metadata,
                ).await {
                    tracing::error!(
                        target: "audit.protocol_fee_ledger",
                        user = %position.user_address,
                        position_id = %position.id,
                        liquidation_id = %liquidation_id,
                        fee_type = fee_type.as_str(),
                        amount = %amount,
                        "Failed to write protocol_fee_ledger row on liquidation: {}", e
                    );
                    return Err(e.into());
                }
            }
        }

        tx.commit().await?;

        tracing::info!(
            "Liquidated position {} for user {}: size={}, pnl={}, remaining={}",
            position.id,
            position.user_address,
            position.size_in_usd,
            pnl,
            remaining_collateral
        );

        // Send liquidation email notification to user (async, non-blocking)
        let pool_clone = self.pool.clone();
        let alert_service = self.alert_service.clone();
        let user_address = position.user_address.clone();
        let symbol = position.symbol.clone();
        let side_str = match position.side {
            PositionSide::Long => "Long",
            PositionSide::Short => "Short",
        };
        let liq_time = Utc::now().format("%Y-%m-%d %H:%M:%S UTC").to_string();
        let mark_price_str = format!("{:.6}", mark_price);
        let entry_price_str = format!("{:.6}", position.entry_price);
        let size_str = format!("{:.2}", position.size_in_usd);
        let collateral_str = format!("{:.2}", position.collateral_amount);
        // Actual loss = collateral - max(returned, 0)
        let actual_loss = position.collateral_amount - collateral_returned;
        let actual_loss_str = format!("{:.2}", actual_loss);
        let returned_str = if collateral_returned > Decimal::ZERO {
            format!("{:.2}", collateral_returned)
        } else {
            "0.00".to_string()
        };

        tokio::spawn(async move {
            // Look up user's verified email
            let user_email: Option<(String,)> = sqlx::query_as(
                "SELECT email FROM users WHERE LOWER(address) = LOWER($1) AND email IS NOT NULL AND email_verified = true"
            )
            .bind(&user_address)
            .fetch_optional(&pool_clone)
            .await
            .ok()
            .flatten();

            if let Some((email,)) = user_email {
                let subject = format!("AXBlade Liquidation Alert — {} {}", symbol, side_str);
                let body = format!(
                    r#"<html><body style="font-family: Arial, sans-serif; color: #333;">
<h2 style="color: #e53e3e;">Position Liquidated</h2>
<p>Your <strong>{side}</strong> position on <strong>{symbol}</strong> has been liquidated.</p>
<table style="border-collapse: collapse; margin: 16px 0;">
  <tr><td style="padding: 6px 16px 6px 0; color: #666;">Symbol</td><td style="padding: 6px 0;"><strong>{symbol}</strong></td></tr>
  <tr><td style="padding: 6px 16px 6px 0; color: #666;">Side</td><td style="padding: 6px 0;">{side}</td></tr>
  <tr><td style="padding: 6px 16px 6px 0; color: #666;">Position Size</td><td style="padding: 6px 0;">{size} USDT</td></tr>
  <tr><td style="padding: 6px 16px 6px 0; color: #666;">Margin</td><td style="padding: 6px 0;">{collateral} USDT</td></tr>
  <tr><td style="padding: 6px 16px 6px 0; color: #666;">Entry Price</td><td style="padding: 6px 0;">{entry}</td></tr>
  <tr><td style="padding: 6px 16px 6px 0; color: #666;">Liquidation Price</td><td style="padding: 6px 0;">{mark}</td></tr>
  <tr><td style="padding: 6px 16px 6px 0; color: #e53e3e; font-weight: bold;">Actual Loss</td><td style="padding: 6px 0; color: #e53e3e; font-weight: bold;">-{loss} USDT</td></tr>
  <tr><td style="padding: 6px 16px 6px 0; color: #666;">Returned</td><td style="padding: 6px 0;">{returned} USDT</td></tr>
  <tr><td style="padding: 6px 16px 6px 0; color: #666;">Time</td><td style="padding: 6px 0;">{time}</td></tr>
</table>
<p style="color: #666; font-size: 13px;">Please review your account and manage your risk accordingly.</p>
<p style="color: #999; font-size: 12px;">— AXBlade Team</p>
</body></html>"#,
                    side = side_str,
                    symbol = symbol,
                    size = size_str,
                    collateral = collateral_str,
                    entry = entry_price_str,
                    mark = mark_price_str,
                    loss = actual_loss_str,
                    returned = returned_str,
                    time = liq_time,
                );

                match alert_service.send_email_to(&email, &subject, &body).await {
                    Ok(_) => tracing::info!("Liquidation email sent to {} for position {}", email, user_address),
                    Err(e) => tracing::error!("Failed to send liquidation email to {}: {}", email, e),
                }
            }
        });

        Ok(LiquidationResult {
            position_id: position.id,
            user_address: position.user_address.clone(),
            symbol: position.symbol.clone(),
            side: position.side,
            position_size_usd: position.size_in_usd,
            collateral_returned,
            pnl,
            liquidation_fee,
            insurance_fund_contribution: insurance_contribution,
        })
    }

    /// Check and liquidate all positions for a market
    pub async fn check_and_liquidate_market(&self, symbol: &str) -> Result<LiquidationStats> {
        let mut stats = LiquidationStats::default();

        // Get current mark price
        let mark_price = self.price_feed_service.get_mark_price(symbol).await
            .ok_or_else(|| anyhow!("No price available for {}", symbol))?;

        let config = self.get_config(symbol).await?;

        // Get all open positions for this market, excluding unified-margin users.
        // Unified-margin users are handled by the separate risk_worker which
        // evaluates the whole account (uniMMR) rather than individual positions.
        let positions: Vec<Position> = sqlx::query_as::<_, Position>(
            r#"
            SELECT p.* FROM positions p
            JOIN users u ON LOWER(p.user_address) = LOWER(u.address)
            WHERE p.symbol = $1 AND p.status = 'open'
              AND COALESCE(u.margin_mode, 'isolated') = 'isolated'
            "#
        )
        .bind(symbol)
        .fetch_all(&self.pool)
        .await?;

        stats.positions_checked = positions.len();

        for position in positions {
            if self.should_liquidate(&position, mark_price, &config) {
                match self.execute_liquidation(&position, mark_price, None).await {
                    Ok(result) => {
                        stats.positions_liquidated += 1;
                        stats.total_volume_liquidated += result.position_size_usd;
                        stats.total_fees_collected += result.liquidation_fee;
                        stats.insurance_fund_contributions += result.insurance_fund_contribution;
                    }
                    Err(e) => {
                        tracing::error!("Failed to liquidate position {}: {:?}", position.id, e);
                    }
                }
            }
        }

        if stats.positions_liquidated > 0 {
            tracing::info!(
                "Market {} liquidation run: {} liquidated out of {} checked, volume: {}",
                symbol,
                stats.positions_liquidated,
                stats.positions_checked,
                stats.total_volume_liquidated
            );
        }

        Ok(stats)
    }

    /// Get liquidation history for a user
    pub async fn get_user_liquidations(
        &self,
        user_address: &str,
        limit: i64,
    ) -> Result<Vec<LiquidationRecord>> {
        let records: Vec<LiquidationRecord> = sqlx::query_as::<_, LiquidationRecord>(
            r#"
            SELECT id, position_id, user_address, symbol, side,
                   position_size_usd, position_size_tokens, collateral_amount,
                   entry_price, liquidation_price, mark_price,
                   remaining_collateral, liquidation_fee, insurance_fund_contribution,
                   pnl, liquidator_address, liquidator_reward, liquidated_at, created_at
            FROM liquidations
            WHERE user_address = $1
            ORDER BY liquidated_at DESC
            LIMIT $2
            "#
        )
        .bind(user_address)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(records)
    }

    /// Get recent liquidations for a market
    pub async fn get_market_liquidations(
        &self,
        symbol: &str,
        limit: i64,
    ) -> Result<Vec<LiquidationRecord>> {
        let records: Vec<LiquidationRecord> = sqlx::query_as::<_, LiquidationRecord>(
            r#"
            SELECT id, position_id, user_address, symbol, side,
                   position_size_usd, position_size_tokens, collateral_amount,
                   entry_price, liquidation_price, mark_price,
                   remaining_collateral, liquidation_fee, insurance_fund_contribution,
                   pnl, liquidator_address, liquidator_reward, liquidated_at, created_at
            FROM liquidations
            WHERE symbol = $1
            ORDER BY liquidated_at DESC
            LIMIT $2
            "#
        )
        .bind(symbol)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(records)
    }

    /// Start the liquidation check loop
    /// Runs in background and checks all markets periodically
    pub async fn start_liquidation_loop(self: Arc<Self>, symbols: Vec<String>) {
        let service = self.clone();
        let interval_secs = self.check_interval_secs;
        let symbols_for_log = symbols.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(
                tokio::time::Duration::from_secs(interval_secs)
            );

            loop {
                interval.tick().await;

                for symbol in &symbols {
                    if let Err(e) = service.check_and_liquidate_market(symbol).await {
                        tracing::error!("Liquidation check failed for {}: {:?}", symbol, e);
                    }
                }
            }
        });

        tracing::info!(
            "Liquidation engine started, checking every {} seconds for markets: {:?}",
            interval_secs,
            symbols_for_log
        );
    }
}
