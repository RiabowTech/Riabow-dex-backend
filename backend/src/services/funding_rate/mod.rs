#![allow(dead_code)]
//! Funding Rate Service
//!
//! Implements GMX V2-style funding rate calculation and settlement.
//! Funding rates are used to keep perpetual contract prices aligned with spot prices.

use anyhow::{anyhow, Result};
use chrono::{DateTime, Duration, Timelike, Utc};
use rust_decimal::prelude::*;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

use tracing::Instrument;
use crate::models::PositionSide;
use crate::services::price_feed::PriceFeedService;

/// Funding rate configuration for a market
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct FundingConfig {
    pub symbol: String,
    pub funding_interval_hours: i32,
    pub max_funding_rate: Decimal,
    pub min_funding_rate: Decimal,
    pub impact_pool_size: Decimal,
}

impl Default for FundingConfig {
    fn default() -> Self {
        Self {
            symbol: "BTCUSDT".to_string(),
            funding_interval_hours: 8,
            max_funding_rate: Decimal::from_str("0.01").unwrap(),      // 1% max
            min_funding_rate: Decimal::from_str("-0.01").unwrap(),   // -1% min
            impact_pool_size: Decimal::ZERO,
        }
    }
}

/// Current funding rate info for a market
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FundingRateInfo {
    pub symbol: String,
    pub funding_rate: Decimal,
    pub funding_rate_per_hour: Decimal,
    pub mark_price: Decimal,
    pub index_price: Decimal,
    pub next_funding_time: DateTime<Utc>,
    pub funding_time: i64,
    pub long_open_interest: Decimal,
    pub short_open_interest: Decimal,
}

/// Database row for funding rate history
#[derive(Debug, Clone, FromRow)]
struct FundingRateRow {
    pub symbol: String,
    pub funding_rate: Decimal,
    pub funding_rate_per_hour: Decimal,
    pub mark_price: Decimal,
    pub index_price: Decimal,
    pub next_funding_time: DateTime<Utc>,
    pub long_open_interest: Decimal,
    pub short_open_interest: Decimal,
    pub created_at: DateTime<Utc>,
}

/// Funding settlement record
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct FundingSettlement {
    pub id: Uuid,
    pub position_id: Uuid,
    pub user_address: String,
    pub symbol: String,
    pub funding_rate: Decimal,
    pub position_size: Decimal,
    pub funding_fee: Decimal,
    pub is_long: bool,
    pub settled_at: DateTime<Utc>,
}

/// Position row for funding settlement queries
#[derive(Debug, Clone, FromRow)]
struct PositionRow {
    pub id: Uuid,
    pub user_address: String,
    pub symbol: String,
    pub side: String,
    pub size_in_usd: Decimal,
    pub collateral_amount: Decimal,
}

/// Open interest tracking for a market
#[derive(Debug, Clone, Default)]
pub struct OpenInterest {
    pub long: Decimal,
    pub short: Decimal,
}

/// Returned when an order's net-side OI delta would push the per-market
/// per-side aggregate past the configured cap. Hard-rejected at order
/// placement; existing positions can still close. Spec §1.3.
#[derive(Debug, Clone)]
pub struct OiCapExceeded {
    pub symbol: String,
    pub side: PositionSide,
    pub current_oi_usd: Decimal,
    pub cap_usd: Decimal,
    pub requested_delta_usd: Decimal,
}

impl std::fmt::Display for OiCapExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {:?}-side OI cap exceeded: current={}, cap={}, requested_delta={}",
            self.symbol, self.side, self.current_oi_usd, self.cap_usd, self.requested_delta_usd
        )
    }
}

impl std::error::Error for OiCapExceeded {}

/// Funding rate service for calculating and settling funding rates
pub struct FundingRateService {
    pool: PgPool,
    /// Cached funding rates per market
    funding_rates: Arc<RwLock<HashMap<String, FundingRateInfo>>>,
    /// Cached open interest per market
    open_interest: Arc<RwLock<HashMap<String, OpenInterest>>>,
    /// Funding factor (controls rate sensitivity)
    funding_factor: Decimal,
}

impl FundingRateService {
    /// Create a new FundingRateService
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            funding_rates: Arc::new(RwLock::new(HashMap::new())),
            open_interest: Arc::new(RwLock::new(HashMap::new())),
            funding_factor: Decimal::from_str("0.0001").unwrap(), // Base funding factor
        }
    }

    /// Ensure funding_rates table has the required columns
    pub async fn ensure_schema(&self) -> Result<()> {
        // Add long_open_interest and short_open_interest columns if they don't exist
        sqlx::query(
            r#"
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'funding_rates' AND column_name = 'long_open_interest'
                ) THEN
                    ALTER TABLE funding_rates ADD COLUMN long_open_interest numeric(36,18) NOT NULL DEFAULT 0;
                END IF;
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'funding_rates' AND column_name = 'short_open_interest'
                ) THEN
                    ALTER TABLE funding_rates ADD COLUMN short_open_interest numeric(36,18) NOT NULL DEFAULT 0;
                END IF;
            END
            $$;
            "#
        )
        .execute(&self.pool)
        .await?;
        tracing::info!("funding_rates schema ensured (long_open_interest, short_open_interest)");
        Ok(())
    }

    /// Get current funding rate for a symbol
    pub async fn get_funding_rate(&self, symbol: &str) -> Option<FundingRateInfo> {
        let rates = self.funding_rates.read().await;
        rates.get(symbol).cloned()
    }

    /// Get all current funding rates
    pub async fn get_all_funding_rates(&self) -> Vec<FundingRateInfo> {
        let rates = self.funding_rates.read().await;
        rates.values().cloned().collect()
    }

    /// Calculate funding rate based on open interest imbalance
    ///
    /// Formula: fundingRate = clamp((longOI - shortOI) / totalOI * fundingFactor, min, max)
    pub fn calculate_funding_rate(
        &self,
        long_open_interest: Decimal,
        short_open_interest: Decimal,
        config: &FundingConfig,
    ) -> Decimal {
        let total_oi = long_open_interest + short_open_interest;

        if total_oi.is_zero() {
            return Decimal::ZERO;
        }

        let imbalance = long_open_interest - short_open_interest;
        let raw_rate = crate::safe_div!(
            imbalance,
            total_oi,
            "funding_rate: calculate_funding_rate"
        ) * self.funding_factor;

        // Clamp to configured min/max
        raw_rate.max(config.min_funding_rate).min(config.max_funding_rate)
    }

    /// Update open interest for a market
    pub async fn update_open_interest(&self, symbol: &str, side: PositionSide, delta: Decimal) {
        let mut oi_map = self.open_interest.write().await;
        let oi = oi_map.entry(symbol.to_string()).or_default();

        match side {
            PositionSide::Long => oi.long += delta,
            PositionSide::Short => oi.short += delta,
        }

        // Ensure non-negative
        if oi.long < Decimal::ZERO {
            oi.long = Decimal::ZERO;
        }
        if oi.short < Decimal::ZERO {
            oi.short = Decimal::ZERO;
        }
    }

    /// Get open interest for a market
    pub async fn get_open_interest(&self, symbol: &str) -> OpenInterest {
        let oi_map = self.open_interest.read().await;
        oi_map.get(symbol).cloned().unwrap_or_default()
    }

    /// Hard-reject helper for order placement. Returns Err(OiCapExceeded)
    /// if `current_oi[side] + delta_usd > cap_for_side`. Caller (order.rs)
    /// reads the cap from market_configs and passes it in — keeps this
    /// method DB-free and avoids a circular dep on MarketConfigService.
    /// Spec §1.3 + §1.4.
    ///
    /// `delta_usd` is the NET OI delta on `side` after the caller nets
    /// against any opposing position the user already holds. Pure closes
    /// pass `delta_usd <= 0` and short-circuit.
    pub async fn check_oi_cap_with_cap(
        &self,
        symbol: &str,
        side: PositionSide,
        delta_usd: Decimal,
        cap_usd: Decimal,
    ) -> Result<(), OiCapExceeded> {
        if delta_usd <= Decimal::ZERO {
            return Ok(());
        }
        let oi = self.get_open_interest(symbol).await;
        let current = match side {
            PositionSide::Long => oi.long,
            PositionSide::Short => oi.short,
        };
        if current + delta_usd > cap_usd {
            return Err(OiCapExceeded {
                symbol: symbol.to_string(),
                side,
                current_oi_usd: current,
                cap_usd,
                requested_delta_usd: delta_usd,
            });
        }
        Ok(())
    }

    /// Query open interest from database
    async fn query_open_interest_from_db(&self, symbol: &str) -> Result<OpenInterest> {
        let result: Option<(Decimal, Decimal)> = sqlx::query_as(
            r#"
            SELECT
                COALESCE(SUM(CASE WHEN side = 'long' THEN size_in_usd ELSE 0 END), 0) as long_oi,
                COALESCE(SUM(CASE WHEN side = 'short' THEN size_in_usd ELSE 0 END), 0) as short_oi
            FROM positions
            WHERE symbol = $1 AND status = 'open'
            "#
        )
        .bind(symbol)
        .fetch_optional(&self.pool)
        .await?;

        let (long, short) = result.unwrap_or((Decimal::ZERO, Decimal::ZERO));
        Ok(OpenInterest { long, short })
    }

    /// Update funding rate for a market
    pub async fn update_funding_rate(
        &self,
        symbol: &str,
        mark_price: Decimal,
        index_price: Decimal,
    ) -> Result<FundingRateInfo> {
        // Get open interest from database (authoritative source)
        let oi = self.query_open_interest_from_db(symbol).await?;

        // Get config from database or use default
        let config = self.get_or_create_config(symbol).await?;

        // Calculate funding rate
        let funding_rate = self.calculate_funding_rate(oi.long, oi.short, &config);
        
        // Safety check: prevent division by zero if funding_interval_hours is somehow 0
        let funding_rate_per_hour = crate::safe_div_or!(
            funding_rate,
            Decimal::from(config.funding_interval_hours),
            funding_rate / Decimal::from(8),  // Default to 8 hours
            "funding_rate: update_funding_rate per_hour calculation"
        );

        // Calculate next funding time (round to next interval)
        let now = Utc::now();
        let hours_until_next = config.funding_interval_hours -
            (now.hour() as i32 % config.funding_interval_hours);
        let next_funding_time = now + Duration::hours(hours_until_next as i64);
        let next_funding_time = next_funding_time
            .with_nanosecond(0)
            .unwrap()
            .with_second(0)
            .unwrap()
            .with_minute(0)
            .unwrap();

        let info = FundingRateInfo {
            symbol: symbol.to_string(),
            funding_rate,
            funding_rate_per_hour,
            mark_price,
            index_price,
            next_funding_time,
            funding_time: Utc::now().timestamp_millis(),
            long_open_interest: oi.long,
            short_open_interest: oi.short,
        };

        // Update cache
        {
            let mut rates = self.funding_rates.write().await;
            
            if let Some(old) = rates.get(symbol) {
                if old.funding_rate != funding_rate || old.funding_rate_per_hour != funding_rate_per_hour {
                    tracing::info!(
                        "FUNDING_RATE_UPDATE symbol={} old_rate={} new_rate={} rate_per_hour={} mark_price={} long_oi={} short_oi={}",
                        symbol, old.funding_rate, funding_rate, funding_rate_per_hour, mark_price, oi.long, oi.short
                    );
                }
            } else {
                tracing::info!(
                    "FUNDING_RATE_INITIAL symbol={} rate={} rate_per_hour={} mark_price={} long_oi={} short_oi={}",
                    symbol, funding_rate, funding_rate_per_hour, mark_price, oi.long, oi.short
                );
            }

            rates.insert(symbol.to_string(), info.clone());
        }

        // Store in database
        self.store_funding_rate(&info).await?;

        Ok(info)
    }

    /// Read the funding config row for `symbol` without side effects.
    ///
    /// Distinct from `get_or_create_config`, which auto-inserts a default row
    /// when none exists. The admin cap endpoint needs to *know* whether the
    /// row exists so it can reject partial requests on new symbols rather than
    /// silently apply defaults — auto-creating here would defeat that.
    pub async fn get_funding_config(&self, symbol: &str) -> Result<Option<FundingConfig>> {
        let row: Option<FundingConfig> = sqlx::query_as::<_, FundingConfig>(
            r#"
            SELECT symbol, funding_interval_hours, max_funding_rate, min_funding_rate, impact_pool_size
            FROM market_funding_config
            WHERE symbol = $1
            "#,
        )
        .bind(symbol)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    /// UPSERT the per-symbol funding caps. Insert-side defaults
    /// `funding_interval_hours = 8` and `impact_pool_size = 0` match
    /// `FundingConfig::default()`. `RETURNING` saves a follow-up SELECT.
    ///
    /// Validation (sign / range / ceiling / new-symbol completeness) is the
    /// caller's responsibility — see admin handler.
    pub async fn set_funding_caps(
        &self,
        symbol: &str,
        max_funding_rate: Decimal,
        min_funding_rate: Decimal,
    ) -> Result<FundingConfig> {
        let row: FundingConfig = sqlx::query_as::<_, FundingConfig>(
            r#"
            INSERT INTO market_funding_config
                (symbol, funding_interval_hours, max_funding_rate, min_funding_rate, impact_pool_size)
            VALUES
                ($1, 8, $2, $3, 0)
            ON CONFLICT (symbol) DO UPDATE
            SET max_funding_rate = EXCLUDED.max_funding_rate,
                min_funding_rate = EXCLUDED.min_funding_rate
            RETURNING symbol, funding_interval_hours, max_funding_rate, min_funding_rate, impact_pool_size
            "#,
        )
        .bind(symbol)
        .bind(max_funding_rate)
        .bind(min_funding_rate)
        .fetch_one(&self.pool)
        .await?;
        Ok(row)
    }

    /// Get or create funding config for a symbol
    async fn get_or_create_config(&self, symbol: &str) -> Result<FundingConfig> {
        let config: Option<FundingConfig> = sqlx::query_as::<_, FundingConfig>(
            r#"
            SELECT symbol, funding_interval_hours, max_funding_rate, min_funding_rate, impact_pool_size
            FROM market_funding_config
            WHERE symbol = $1
            "#
        )
        .bind(symbol)
        .fetch_optional(&self.pool)
        .await?;

        match config {
            Some(c) => Ok(c),
            None => {
                // Create default config
                let default = FundingConfig {
                    symbol: symbol.to_string(),
                    ..Default::default()
                };

                sqlx::query(
                    r#"
                    INSERT INTO market_funding_config (symbol, funding_interval_hours, max_funding_rate, min_funding_rate)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (symbol) DO NOTHING
                    "#
                )
                .bind(symbol)
                .bind(default.funding_interval_hours)
                .bind(default.max_funding_rate)
                .bind(default.min_funding_rate)
                .execute(&self.pool)
                .await?;

                Ok(default)
            }
        }
    }

    /// Store funding rate in database
    async fn store_funding_rate(&self, info: &FundingRateInfo) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO funding_rates (symbol, funding_rate, funding_rate_per_hour, mark_price, index_price, next_funding_time, long_open_interest, short_open_interest)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#
        )
        .bind(&info.symbol)
        .bind(info.funding_rate)
        .bind(info.funding_rate_per_hour)
        .bind(info.mark_price)
        .bind(info.index_price)
        .bind(info.next_funding_time)
        .bind(info.long_open_interest)
        .bind(info.short_open_interest)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Settle funding for all open positions in a market
    ///
    /// Returns the number of positions settled
    pub async fn settle_funding(&self, symbol: &str) -> Result<usize> {
        // Get current funding rate
        let funding_info = self.get_funding_rate(symbol).await
            .ok_or_else(|| anyhow!("No funding rate for symbol: {}", symbol))?;

        // Get all open positions for this market.
        // Cast `side` ENUM (position_side) to TEXT so sqlx can decode into
        // PositionRow.side: String — otherwise the settlement loop fails with
        // "mismatched types: Rust `String` (TEXT) vs SQL `position_side`"
        // and no positions get settled.
        let positions: Vec<PositionRow> = sqlx::query_as::<_, PositionRow>(
            r#"
            SELECT id, user_address, symbol, side::TEXT AS side, size_in_usd, collateral_amount
            FROM positions
            WHERE symbol = $1 AND status = 'open'
            "#
        )
        .bind(symbol)
        .fetch_all(&self.pool)
        .await?;

        let mut settled_count = 0;

        for position in positions {
            let is_long = position.side == "long";

            // Calculate funding fee
            // Long pays when rate is positive, short pays when rate is negative
            let funding_fee = if is_long {
                position.size_in_usd * funding_info.funding_rate
            } else {
                position.size_in_usd * (-funding_info.funding_rate)
            };

            // Record settlement
            let settlement = FundingSettlement {
                id: Uuid::new_v4(),
                position_id: position.id,
                user_address: position.user_address.clone(),
                symbol: symbol.to_string(),
                funding_rate: funding_info.funding_rate,
                position_size: position.size_in_usd,
                funding_fee,
                is_long,
                settled_at: Utc::now(),
            };

            // Store settlement and update position
            self.apply_funding_settlement(&settlement).await?;
            settled_count += 1;
        }

        // Mark funding rate as settled
        sqlx::query(
            r#"
            UPDATE funding_rates
            SET settled_at = NOW()
            WHERE symbol = $1 AND settled_at IS NULL
            "#
        )
        .bind(symbol)
        .execute(&self.pool)
        .await?;

        tracing::info!(
            "Settled funding for {} positions in {}, rate: {}",
            settled_count,
            symbol,
            funding_info.funding_rate
        );

        Ok(settled_count)
    }

    /// Apply a funding settlement to a position
    async fn apply_funding_settlement(&self, settlement: &FundingSettlement) -> Result<()> {
        // Begin transaction
        let mut tx = self.pool.begin().await?;

        // Record settlement
        sqlx::query(
            r#"
            INSERT INTO funding_settlements (id, position_id, user_address, symbol, funding_rate, position_size, funding_fee, is_long, settled_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            "#
        )
        .bind(settlement.id)
        .bind(settlement.position_id)
        .bind(&settlement.user_address)
        .bind(&settlement.symbol)
        .bind(settlement.funding_rate)
        .bind(settlement.position_size)
        .bind(settlement.funding_fee)
        .bind(settlement.is_long)
        .bind(settlement.settled_at)
        .execute(&mut *tx)
        .await?;

        // Update accumulated funding fee on position AND recalculate liquidation_price.
        // The liquidation_price must stay in sync with the actual remaining-collateral
        // formula used by the liquidation engine:
        //   remaining = collateral + PnL - fees  vs  size_usd * mmr
        // For Long:  liq_mark = (size_usd * mmr - collateral + fees + size_usd) / tokens
        // For Short: liq_mark = (collateral - fees + size_usd - size_usd * mmr) / tokens
        //
        // We compute this in a single UPDATE to avoid an extra round-trip.
        let mmr = Decimal::from_str("0.005").unwrap_or(Decimal::new(5, 3)); // 0.5% default
        sqlx::query(
            r#"
            UPDATE positions
            SET accumulated_funding_fee = accumulated_funding_fee + $1,
                liquidation_price = CASE
                    WHEN side = 'long' AND size_in_tokens > 0 THEN
                        GREATEST(
                            (size_in_usd * $3 - collateral_amount
                             + (accumulated_funding_fee + $1) + accumulated_borrowing_fee
                             + size_in_usd)
                            / size_in_tokens,
                            0
                        )
                    WHEN side = 'short' AND size_in_tokens > 0 THEN
                        GREATEST(
                            (collateral_amount
                             - (accumulated_funding_fee + $1) - accumulated_borrowing_fee
                             + size_in_usd - size_in_usd * $3)
                            / size_in_tokens,
                            0
                        )
                    ELSE liquidation_price
                END,
                updated_at = NOW()
            WHERE id = $2
            "#
        )
        .bind(settlement.funding_fee)
        .bind(settlement.position_id)
        .bind(mmr)
        .execute(&mut *tx)
        .await?;

        // PR 2 (2026-04-29): protocol_fee_ledger row for this settlement.
        // Signed amount: positive = user pays funding (protocol receives);
        // negative = user earns funding (protocol pays out).
        // Spec §2.3.
        {
            use crate::services::protocol_fee_ledger::{record_fee_event, FeeType};
            let metadata = serde_json::json!({
                "funding_rate":  settlement.funding_rate.to_string(),
                "position_size": settlement.position_size.to_string(),
                "is_long":       settlement.is_long,
            });
            if let Err(e) = record_fee_event(
                &mut *tx,
                &settlement.user_address,
                Some(settlement.position_id),
                None, // funding settlements have no trade_id
                FeeType::FundingFee,
                settlement.funding_fee,
                &metadata,
            ).await {
                tracing::error!(
                    target: "audit.protocol_fee_ledger",
                    user = %settlement.user_address,
                    position_id = %settlement.position_id,
                    funding_fee = %settlement.funding_fee,
                    "Failed to write protocol_fee_ledger row on funding settlement: {}", e
                );
                // Surface as a tx error so the whole settlement rolls back —
                // funding fee already committed to position.accumulated_funding_fee
                // without a ledger row would create reconciliation drift.
                return Err(e.into());
            }
        }

        tx.commit().await?;

        Ok(())
    }

    /// Get funding history for a symbol (optionally filtered by period lookback)
    pub async fn get_funding_history(
        &self,
        symbol: &str,
        limit: i64,
        since: Option<DateTime<Utc>>,
    ) -> Result<Vec<FundingRateInfo>> {
        let records: Vec<FundingRateRow> = sqlx::query_as::<_, FundingRateRow>(
            r#"
            SELECT symbol, funding_rate, funding_rate_per_hour, mark_price, index_price, next_funding_time,
                   COALESCE(long_open_interest, 0) as long_open_interest,
                   COALESCE(short_open_interest, 0) as short_open_interest,
                   created_at
            FROM funding_rates
            WHERE symbol = $1
              AND ($3::timestamptz IS NULL OR created_at >= $3)
            ORDER BY created_at DESC
            LIMIT $2
            "#
        )
        .bind(symbol)
        .bind(limit)
        .bind(since)
        .fetch_all(&self.pool)
        .await?;

        let history: Vec<FundingRateInfo> = records
            .into_iter()
            .map(|r| FundingRateInfo {
                symbol: r.symbol,
                funding_rate: r.funding_rate,
                funding_rate_per_hour: r.funding_rate_per_hour,
                mark_price: r.mark_price,
                index_price: r.index_price,
                next_funding_time: r.next_funding_time,
                funding_time: r.created_at.timestamp_millis(),
                long_open_interest: r.long_open_interest,
                short_open_interest: r.short_open_interest,
            })
            .collect();

        Ok(history)
    }

    /// Get user's funding settlement history
    pub async fn get_user_settlements(
        &self,
        user_address: &str,
        limit: i64,
        since: Option<DateTime<Utc>>,
    ) -> Result<Vec<FundingSettlement>> {
        let settlements: Vec<FundingSettlement> = sqlx::query_as::<_, FundingSettlement>(
            r#"
            SELECT id, position_id, user_address, symbol, funding_rate, position_size, funding_fee, is_long, settled_at
            FROM funding_settlements
            WHERE user_address = $1
              AND ($3::timestamptz IS NULL OR settled_at >= $3)
            ORDER BY settled_at DESC
            LIMIT $2
            "#
        )
        .bind(user_address)
        .bind(limit)
        .bind(since)
        .fetch_all(&self.pool)
        .await?;

        Ok(settlements)
    }

    /// Start the funding rate update loop
    /// Updates funding rates every minute and settles at intervals
    pub async fn start_update_loop(self: Arc<Self>, symbols: Vec<String>, price_feed: Arc<PriceFeedService>) {
        let service = self.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60));

            loop {
                interval.tick().await;

                for symbol in &symbols {
                    // Get real mark_price and index_price from price feed
                    let (mark_price, index_price) = match price_feed.get_price_data(symbol).await {
                        Some(data) => (data.mark_price, data.last_price),
                        None => {
                            tracing::info!("No price data for {}, skipping funding rate update", symbol);
                            continue;
                        }
                    };

                    match service.update_funding_rate(
                        symbol,
                        mark_price,
                        index_price,
                    ).await {
                        Ok(info) => {
                            tracing::info!(
                                "Funding rate updated: {} mark={} index={} rate={} oi_long={} oi_short={}",
                                symbol, mark_price, index_price, info.funding_rate, info.long_open_interest, info.short_open_interest
                            );
                        }
                        Err(e) => {
                            tracing::error!("Failed to update funding rate for {}: {:?}", symbol, e);
                        }
                    }
                }
            }
        }.instrument(tracing::info_span!("funding-rate-update")));
    }

    /// Start the funding settlement scheduler
    /// Settles funding at configured intervals (default: every 8 hours, aligned to
    /// 00:00 / 08:00 / 16:00 UTC).
    ///
    /// 历史 bug：原实现用 `tokio::time::interval(60s)` 自由漂移，tick 和墙钟
    /// 不对齐，任何容器重启（本项目近 24h 有大量重启）都会让 tick 打不到
    /// `minute == 0 && hour % 8 == 0` 的窗口，造成 settlement 从未运行
    /// （funding_settlements 表整历史 0 条、funding_rates 6M 条未结算）。
    /// 改为每轮显式计算下一次 00/08/16:00 UTC 的墙钟时刻并 `sleep_until`
    /// 过去，必定命中。
    pub async fn start_settlement_scheduler(self: Arc<Self>, symbols: Vec<String>) {
        let service = self.clone();

        tokio::spawn(async move {
            loop {
                let now = Utc::now();
                let next = next_settlement_at(now);
                let wait = (next - now).to_std().unwrap_or(std::time::Duration::from_secs(1));
                tracing::info!(
                    "Next funding settlement scheduled at {} (in {}s)",
                    next, wait.as_secs()
                );
                tokio::time::sleep(wait).await;

                // Settle all symbols sequentially. errors are per-symbol non-fatal.
                for symbol in &symbols {
                    match service.settle_funding(symbol).await {
                        Ok(count) => {
                            tracing::info!("Settled {} positions for {}", count, symbol);
                        }
                        Err(e) => {
                            tracing::error!("Failed to settle funding for {}: {:?}", symbol, e);
                        }
                    }
                }
            }
        }.instrument(tracing::info_span!("funding-rate-settlement")));
    }
}

/// Returns the next settlement timestamp: the nearest future occurrence of
/// :00:00 at an hour that is a multiple of 8 (UTC). Rolls to next day at 24:00.
fn next_settlement_at(now: DateTime<Utc>) -> DateTime<Utc> {
    let hour = now.hour();
    let next_hour = ((hour / 8) + 1) * 8;
    if next_hour >= 24 {
        // tomorrow 00:00:00 UTC
        let tomorrow = now.date_naive() + Duration::days(1);
        tomorrow.and_hms_opt(0, 0, 0).unwrap().and_utc()
    } else {
        now.date_naive().and_hms_opt(next_hour, 0, 0).unwrap().and_utc()
    }
}

#[cfg(test)]
mod settlement_schedule_tests {
    use super::next_settlement_at;
    use chrono::{TimeZone, Utc};

    #[test]
    fn aligns_to_next_8h_boundary() {
        // Arbitrary moment: 2026-04-20 03:47:21 UTC → next = 08:00:00 same day
        let now = Utc.with_ymd_and_hms(2026, 4, 20, 3, 47, 21).unwrap();
        let expected = Utc.with_ymd_and_hms(2026, 4, 20, 8, 0, 0).unwrap();
        assert_eq!(next_settlement_at(now), expected);
    }

    #[test]
    fn rolls_over_midnight() {
        // 2026-04-20 23:59:59 → next = 2026-04-21 00:00:00
        let now = Utc.with_ymd_and_hms(2026, 4, 20, 23, 59, 59).unwrap();
        let expected = Utc.with_ymd_and_hms(2026, 4, 21, 0, 0, 0).unwrap();
        assert_eq!(next_settlement_at(now), expected);
    }

    #[test]
    fn exact_boundary_skips_to_next() {
        // At exactly 16:00:00, next is 00:00 next day (don't fire same-minute twice)
        let now = Utc.with_ymd_and_hms(2026, 4, 20, 16, 0, 0).unwrap();
        let expected = Utc.with_ymd_and_hms(2026, 4, 21, 0, 0, 0).unwrap();
        assert_eq!(next_settlement_at(now), expected);
    }
}

// Note: Tests require a database connection and are temporarily disabled
// TODO: Add integration tests with test database

#[cfg(test)]
mod oi_cap_tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn make_service() -> FundingRateService {
        let pool = sqlx::PgPool::connect_lazy("postgres://x")
            .expect("lazy pool ok without connecting");
        FundingRateService::new(pool)
    }

    #[tokio::test]
    async fn cap_check_passes_when_well_below_cap() {
        let svc = make_service();
        svc.update_open_interest("BTCUSDT", PositionSide::Long, dec!(1000)).await;
        let result = svc
            .check_oi_cap_with_cap("BTCUSDT", PositionSide::Long, dec!(500), dec!(50_000))
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn cap_check_rejects_when_delta_pushes_past_cap() {
        let svc = make_service();
        svc.update_open_interest("BTCUSDT", PositionSide::Long, dec!(40_000)).await;
        let result = svc
            .check_oi_cap_with_cap("BTCUSDT", PositionSide::Long, dec!(20_000), dec!(50_000))
            .await;
        let err = result.expect_err("should reject");
        assert_eq!(err.symbol, "BTCUSDT");
        assert_eq!(err.cap_usd, dec!(50_000));
        assert_eq!(err.current_oi_usd, dec!(40_000));
        assert_eq!(err.requested_delta_usd, dec!(20_000));
    }

    #[tokio::test]
    async fn cap_check_passes_when_delta_zero() {
        let svc = make_service();
        svc.update_open_interest("BTCUSDT", PositionSide::Long, dec!(40_000)).await;
        let result = svc
            .check_oi_cap_with_cap("BTCUSDT", PositionSide::Long, dec!(0), dec!(50_000))
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn cap_check_isolates_long_and_short_sides() {
        let svc = make_service();
        svc.update_open_interest("BTCUSDT", PositionSide::Long, dec!(45_000)).await;
        svc.update_open_interest("BTCUSDT", PositionSide::Short, dec!(1_000)).await;
        // Long is near cap, short has plenty of room
        assert!(svc
            .check_oi_cap_with_cap("BTCUSDT", PositionSide::Long, dec!(10_000), dec!(50_000))
            .await
            .is_err());
        assert!(svc
            .check_oi_cap_with_cap("BTCUSDT", PositionSide::Short, dec!(10_000), dec!(50_000))
            .await
            .is_ok());
    }
}
