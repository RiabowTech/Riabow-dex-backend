#![allow(dead_code)]
//! ADL (Auto-Deleveraging) Service
//!
//! GMX V2-style auto-deleveraging system for handling extreme market conditions
//! when the insurance fund cannot cover liquidation losses.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::sync::Arc;
use uuid::Uuid;

use tracing::Instrument;
use crate::services::position::PositionService;
use crate::services::price_feed::PriceFeedService;

/// ADL Event - represents a single auto-deleveraging execution
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct AdlEvent {
    pub id: Uuid,
    pub market_symbol: String,
    pub liquidation_id: Uuid,
    pub insurance_fund_shortfall: Decimal,
    pub total_reduced_size: Decimal,
    pub total_pnl_realized: Decimal,
    pub positions_affected: i32,
    pub status: String,
    pub error_message: Option<String>,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

/// ADL Position Reduction - individual position impact from ADL
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct AdlReduction {
    pub id: Uuid,
    pub adl_event_id: Uuid,
    pub position_id: Uuid,
    pub user_address: String,
    pub market_symbol: String,
    pub original_size: Decimal,
    pub original_collateral: Decimal,
    pub original_pnl: Decimal,
    pub size_reduced: Decimal,
    pub pnl_realized: Decimal,
    pub adl_rank: i32,
    pub adl_score: Decimal,
    pub compensation_amount: Decimal,
    pub created_at: DateTime<Utc>,
}

/// ADL Ranking Entry - position ranking for ADL queue
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct AdlRanking {
    pub id: Uuid,
    pub market_symbol: String,
    pub side: String,
    pub position_id: Uuid,
    pub user_address: String,
    pub position_size: Decimal,
    pub unrealized_pnl: Decimal,
    pub pnl_percentage: Decimal,
    pub leverage: Decimal,
    pub adl_score: Decimal,
    pub rank: i32,
    pub computed_at: DateTime<Utc>,
}

/// ADL Configuration per market
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct AdlConfig {
    pub id: Uuid,
    pub market_symbol: String,
    pub insurance_fund_threshold: Decimal,
    pub max_positions_per_adl: i32,
    pub min_reduction_percentage: Decimal,
    pub max_reduction_percentage: Decimal,
    pub pnl_weight: Decimal,
    pub leverage_weight: Decimal,
    pub size_weight: Decimal,
    pub min_interval_seconds: i32,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// User ADL Statistics
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct UserAdlStats {
    pub id: Uuid,
    pub user_address: String,
    pub market_symbol: String,
    pub total_adl_events: i32,
    pub total_size_reduced: Decimal,
    pub total_pnl_realized: Decimal,
    pub total_compensation: Decimal,
    pub last_adl_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Position data for ADL calculation
#[derive(Debug, Clone)]
pub struct PositionForAdl {
    pub id: Uuid,
    pub user_address: String,
    pub market_symbol: String,
    pub side: String,
    pub size: Decimal,
    pub collateral: Decimal,
    pub entry_price: Decimal,
    pub leverage: Decimal,
    pub unrealized_pnl: Decimal,
    pub pnl_percentage: Decimal,
}

/// ADL Service
pub struct AdlService {
    pool: PgPool,
    position_service: Arc<PositionService>,
    price_feed_service: Arc<PriceFeedService>,
}

impl AdlService {
    pub fn new(
        pool: PgPool,
        position_service: Arc<PositionService>,
        price_feed_service: Arc<PriceFeedService>,
    ) -> Self {
        Self {
            pool,
            position_service,
            price_feed_service,
        }
    }

    /// Get ADL configuration for a market
    pub async fn get_config(&self, market_symbol: &str) -> Result<AdlConfig, sqlx::Error> {
        sqlx::query_as::<_, AdlConfig>(
            r#"
            SELECT * FROM adl_config WHERE market_symbol = $1
            "#,
        )
        .bind(market_symbol)
        .fetch_one(&self.pool)
        .await
    }

    /// Get ADL rankings for a market and side
    pub async fn get_rankings(
        &self,
        market_symbol: &str,
        side: &str,
        limit: i64,
    ) -> Result<Vec<AdlRanking>, sqlx::Error> {
        sqlx::query_as::<_, AdlRanking>(
            r#"
            SELECT * FROM adl_rankings
            WHERE market_symbol = $1 AND side = $2
            ORDER BY rank ASC
            LIMIT $3
            "#,
        )
        .bind(market_symbol)
        .bind(side)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
    }

    /// Get ADL history for a market
    pub async fn get_market_adl_history(
        &self,
        market_symbol: &str,
        limit: i64,
    ) -> Result<Vec<AdlEvent>, sqlx::Error> {
        sqlx::query_as::<_, AdlEvent>(
            r#"
            SELECT * FROM adl_events
            WHERE market_symbol = $1
            ORDER BY created_at DESC
            LIMIT $2
            "#,
        )
        .bind(market_symbol)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
    }

    /// Get ADL history for a user
    pub async fn get_user_adl_history(
        &self,
        user_address: &str,
        limit: i64,
    ) -> Result<Vec<AdlReduction>, sqlx::Error> {
        sqlx::query_as::<_, AdlReduction>(
            r#"
            SELECT * FROM adl_reductions
            WHERE user_address = $1
            ORDER BY created_at DESC
            LIMIT $2
            "#,
        )
        .bind(user_address)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
    }

    /// Get user ADL statistics
    pub async fn get_user_stats(
        &self,
        user_address: &str,
        market_symbol: &str,
    ) -> Result<Option<UserAdlStats>, sqlx::Error> {
        sqlx::query_as::<_, UserAdlStats>(
            r#"
            SELECT * FROM user_adl_stats
            WHERE user_address = $1 AND market_symbol = $2
            "#,
        )
        .bind(user_address)
        .bind(market_symbol)
        .fetch_optional(&self.pool)
        .await
    }

    /// Calculate ADL score for a position
    /// Score = (pnl_weight * pnl_percentage) + (leverage_weight * leverage) + (size_weight * normalized_size)
    pub fn calculate_adl_score(
        &self,
        pnl_percentage: Decimal,
        leverage: Decimal,
        size: Decimal,
        max_size: Decimal,
        config: &AdlConfig,
    ) -> Decimal {
        let normalized_size = crate::safe_div!(
            size,
            max_size,
            "ADL: calculate_adl_score normalized_size"
        );

        (config.pnl_weight * pnl_percentage)
            + (config.leverage_weight * leverage)
            + (config.size_weight * normalized_size)
    }

    /// Update ADL rankings for a market
    pub async fn update_rankings(&self, market_symbol: &str) -> Result<(), sqlx::Error> {
        let config = self.get_config(market_symbol).await?;
        if !config.enabled {
            return Ok(());
        }

        // Get current price
        let current_price = match self.price_feed_service.get_mark_price(market_symbol).await {
            Some(price) => price,
            None => {
                tracing::warn!("No price available for {}, skipping ADL ranking update", market_symbol);
                return Ok(());
            }
        };

        // Get all open positions for this market
        let positions = self.get_profitable_positions(market_symbol, current_price).await?;

        // Report position counts per side for metrics
        let long_count = positions.iter().filter(|p| p.side == "long").count();
        let short_count = positions.iter().filter(|p| p.side == "short").count();
        crate::services::metrics::ADL_RANKING_POSITION_COUNT
            .with_label_values(&[market_symbol, "long"])
            .set(long_count as f64);
        crate::services::metrics::ADL_RANKING_POSITION_COUNT
            .with_label_values(&[market_symbol, "short"])
            .set(short_count as f64);

        if positions.is_empty() {
            // No profitable positions — clear stale rankings and return
            sqlx::query("DELETE FROM adl_rankings WHERE market_symbol = $1")
                .bind(market_symbol)
                .execute(&self.pool)
                .await?;
            return Ok(());
        }

        // Find max size for normalization
        let max_size = positions.iter().map(|p| p.size).max().unwrap_or(Decimal::ONE);

        // Wrap DELETE + INSERT in a transaction to prevent race conditions
        let mut tx = self.pool.begin().await?;

        // Clear old rankings within transaction
        sqlx::query("DELETE FROM adl_rankings WHERE market_symbol = $1")
            .bind(market_symbol)
            .execute(&mut *tx)
            .await?;

        // Calculate scores and insert rankings for both sides
        for side in ["long", "short"] {
            let positions_by_side: Vec<&PositionForAdl> = positions
                .iter()
                .filter(|p| p.side == side)
                .collect();

            let mut scored_positions: Vec<_> = positions_by_side
                .into_iter()
                .map(|p| {
                    let score = self.calculate_adl_score(
                        p.pnl_percentage,
                        p.leverage,
                        p.size,
                        max_size,
                        &config,
                    );
                    (p, score)
                })
                .collect();

            // Sort by score descending (highest score = first to be reduced)
            scored_positions.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

            // Insert rankings (ON CONFLICT UPDATE to handle any duplicate position_id)
            for (rank, (position, score)) in scored_positions.into_iter().enumerate() {
                sqlx::query(
                    r#"
                    INSERT INTO adl_rankings (
                        market_symbol, side, position_id, user_address,
                        position_size, unrealized_pnl, pnl_percentage, leverage,
                        adl_score, rank
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    ON CONFLICT (market_symbol, side, position_id) DO UPDATE SET
                        position_size = EXCLUDED.position_size,
                        unrealized_pnl = EXCLUDED.unrealized_pnl,
                        pnl_percentage = EXCLUDED.pnl_percentage,
                        leverage = EXCLUDED.leverage,
                        adl_score = EXCLUDED.adl_score,
                        rank = EXCLUDED.rank
                    "#,
                )
                .bind(&position.market_symbol)
                .bind(side)
                .bind(position.id)
                .bind(&position.user_address)
                .bind(position.size)
                .bind(position.unrealized_pnl)
                .bind(position.pnl_percentage)
                .bind(position.leverage)
                .bind(score)
                .bind((rank + 1) as i32)
                .execute(&mut *tx)
                .await?;
            }
        }

        tx.commit().await?;

        tracing::debug!("Updated ADL rankings for {}", market_symbol);
        Ok(())
    }

    /// Get profitable positions for a market
    async fn get_profitable_positions(
        &self,
        market_symbol: &str,
        current_price: Decimal,
    ) -> Result<Vec<PositionForAdl>, sqlx::Error> {
        // Query positions and calculate PnL
        // Note: positions table uses 'symbol' column, not 'market_symbol'
        let rows = sqlx::query_as::<_, (Uuid, String, String, String, Decimal, Decimal, Decimal, i32)>(
            r#"
            SELECT id, user_address, symbol, side::text, size_in_usd, collateral_amount, entry_price, leverage
            FROM positions
            WHERE symbol = $1 AND status = 'open' AND size_in_usd > 0
            "#,
        )
        .bind(market_symbol)
        .fetch_all(&self.pool)
        .await?;

        let positions: Vec<PositionForAdl> = rows
            .into_iter()
            .filter_map(|(id, user_address, symbol, side, size, collateral, entry_price, leverage)| {
                // Calculate unrealized PnL
                let pnl = if side == "long" {
                    (current_price - entry_price) * size
                } else {
                    (entry_price - current_price) * size
                };

                // Only include profitable positions
                if pnl <= Decimal::ZERO {
                    return None;
                }

                let pnl_percentage = crate::safe_div!(
                    pnl,
                    collateral,
                    "ADL: get_profitable_positions pnl_percentage"
                ) * Decimal::from(100);

                Some(PositionForAdl {
                    id,
                    user_address,
                    market_symbol: symbol,
                    side,
                    size,
                    collateral,
                    entry_price,
                    leverage: Decimal::from(leverage),
                    unrealized_pnl: pnl,
                    pnl_percentage,
                })
            })
            .collect();

        Ok(positions)
    }

    /// Execute ADL to cover insurance fund shortfall
    pub async fn execute_adl(
        &self,
        market_symbol: &str,
        liquidation_id: Uuid,
        shortfall: Decimal,
        opposite_side: &str,  // Side of positions to reduce (opposite of liquidated position)
    ) -> Result<AdlEvent, sqlx::Error> {
        let config = self.get_config(market_symbol).await?;

        if !config.enabled {
            return Err(sqlx::Error::Protocol("ADL is disabled for this market".into()));
        }

        // Create ADL event
        let event_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO adl_events (
                id, market_symbol, liquidation_id, insurance_fund_shortfall,
                total_reduced_size, total_pnl_realized, positions_affected, status
            ) VALUES ($1, $2, $3, $4, 0, 0, 0, 'pending')
            "#,
        )
        .bind(event_id)
        .bind(market_symbol)
        .bind(liquidation_id)
        .bind(shortfall)
        .execute(&self.pool)
        .await?;

        // Get ranked positions to reduce
        let rankings = self.get_rankings(market_symbol, opposite_side, config.max_positions_per_adl as i64).await?;

        if rankings.is_empty() {
            // No positions to reduce - mark event as failed
            sqlx::query(
                "UPDATE adl_events SET status = 'failed', error_message = 'No profitable positions to reduce', completed_at = NOW() WHERE id = $1"
            )
            .bind(event_id)
            .execute(&self.pool)
            .await?;

            return self.get_adl_event(event_id).await;
        }

        let mut total_reduced_size = Decimal::ZERO;
        let mut total_pnl_realized = Decimal::ZERO;
        let mut positions_affected = 0;
        let mut remaining_shortfall = shortfall;

        // Process positions in order until shortfall is covered
        for ranking in rankings {
            if remaining_shortfall <= Decimal::ZERO {
                break;
            }

            // Calculate how much to reduce this position
            let pnl_available = ranking.unrealized_pnl;
            let pnl_to_take = remaining_shortfall.min(pnl_available);

            // Calculate reduction ratio
            let reduction_ratio = crate::safe_div!(
                pnl_to_take,
                pnl_available,
                "ADL: execute_adl reduction_ratio"
            ).min(config.max_reduction_percentage);

            // Ensure minimum reduction
            let reduction_ratio = reduction_ratio.max(config.min_reduction_percentage);
            let size_to_reduce = ranking.position_size * reduction_ratio;
            let pnl_realized = pnl_available * reduction_ratio;

            // Get original position data for reduction record
            let original_collateral = crate::safe_div_or!(
                ranking.position_size,
                ranking.leverage,
                ranking.position_size,  // If leverage is 0, use position_size
                "ADL: execute_adl original_collateral calculation"
            );

            // Record the reduction
            sqlx::query(
                r#"
                INSERT INTO adl_reductions (
                    adl_event_id, position_id, user_address, market_symbol,
                    original_size, original_collateral, original_pnl,
                    size_reduced, pnl_realized, adl_rank, adl_score
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                "#,
            )
            .bind(event_id)
            .bind(ranking.position_id)
            .bind(&ranking.user_address)
            .bind(market_symbol)
            .bind(ranking.position_size)
            .bind(original_collateral)
            .bind(ranking.unrealized_pnl)
            .bind(size_to_reduce)
            .bind(pnl_realized)
            .bind(ranking.rank)
            .bind(ranking.adl_score)
            .execute(&self.pool)
            .await?;

            // Update the position (reduce size) with atomic check
            // Only update if position is still open to prevent race conditions
            let new_size = ranking.position_size - size_to_reduce;
            let collateral_to_return = original_collateral * reduction_ratio;

            let is_full_close = new_size <= Decimal::ZERO;
            let update_result = if is_full_close {
                // Close position completely - only if still open
                sqlx::query(
                    "UPDATE positions SET size_in_usd = 0, size_in_tokens = 0, collateral_amount = 0, status = 'closed', decreased_at = NOW(), updated_at = NOW() WHERE id = $1 AND status = 'open'"
                )
                .bind(ranking.position_id)
                .execute(&self.pool)
                .await?
            } else {
                // Reduce position - only if still open
                let new_collateral = original_collateral * (Decimal::ONE - reduction_ratio);
                sqlx::query(
                    "UPDATE positions SET size_in_usd = $1, collateral_amount = $2, updated_at = NOW() WHERE id = $3 AND status = 'open'"
                )
                .bind(new_size)
                .bind(new_collateral)
                .bind(ranking.position_id)
                .execute(&self.pool)
                .await?
            };

            // Skip balance update if position was already closed (race condition)
            if update_result.rows_affected() == 0 {
                tracing::warn!(
                    "ADL: Position {} was already closed, skipping balance update",
                    ranking.position_id
                );
                continue;
            }

            // Record realized PnL event so ADL reductions show up in the user's
            // PnL aggregations. trade_id is NULL (no matching `trades` row);
            // /account/trades falls back to time-proximity matching for these.
            let adl_exec_price = self
                .price_feed_service
                .get_mark_price(market_symbol)
                .await
                .unwrap_or(Decimal::ZERO);
            if let Err(e) = sqlx::query(
                r#"
                INSERT INTO realized_pnl_events (
                    user_address, symbol, position_id, realized_pnl,
                    execution_price, size_delta_usd, is_full_close, trade_id
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, NULL)
                "#,
            )
            .bind(&ranking.user_address)
            .bind(market_symbol)
            .bind(ranking.position_id)
            .bind(pnl_realized)
            .bind(adl_exec_price)
            .bind(size_to_reduce)
            .bind(is_full_close)
            .execute(&self.pool)
            .await
            {
                tracing::warn!(
                    "Failed to record ADL realized_pnl_event for position {}: {}",
                    ranking.position_id, e
                );
            }

            // Update user balance: return collateral + realized PnL (minus the PnL taken for shortfall coverage)
            // In ADL, the profitable position is reduced, so user gets back: collateral + (pnl_realized - pnl_to_take)
            // pnl_to_take goes to cover the insurance fund shortfall
            // Use GREATEST to prevent frozen from going negative
            let amount_returned = collateral_to_return + (pnl_realized - pnl_to_take);
            sqlx::query(
                r#"
                UPDATE balances
                SET frozen = GREATEST(frozen - $1, 0),
                    available = available + $2,
                    updated_at = NOW()
                WHERE user_address = $3 AND token = 'USDT'
                "#
            )
            .bind(collateral_to_return)
            .bind(amount_returned.max(Decimal::ZERO))
            .bind(&ranking.user_address.to_lowercase())
            .execute(&self.pool)
            .await?;

            tracing::info!(
                "ADL: Updated balance for user {}: frozen -{}, available +{}",
                ranking.user_address,
                collateral_to_return,
                amount_returned.max(Decimal::ZERO)
            );

            // Update user stats
            self.update_user_stats(&ranking.user_address, market_symbol, size_to_reduce, pnl_realized).await?;

            total_reduced_size += size_to_reduce;
            total_pnl_realized += pnl_realized;
            positions_affected += 1;
            remaining_shortfall -= pnl_realized;

            tracing::info!(
                "ADL: Reduced position {} by {} ({}%), realized PnL: {}",
                ranking.position_id,
                size_to_reduce,
                reduction_ratio * Decimal::from(100),
                pnl_realized
            );
        }

        // Update ADL event
        let status = if remaining_shortfall <= Decimal::ZERO {
            "completed"
        } else {
            "partial" // Couldn't fully cover shortfall
        };

        sqlx::query(
            r#"
            UPDATE adl_events
            SET total_reduced_size = $1, total_pnl_realized = $2, positions_affected = $3,
                status = $4, completed_at = NOW()
            WHERE id = $5
            "#,
        )
        .bind(total_reduced_size)
        .bind(total_pnl_realized)
        .bind(positions_affected)
        .bind(status)
        .bind(event_id)
        .execute(&self.pool)
        .await?;

        tracing::info!(
            "ADL event {} completed: reduced {} positions, total size {}, PnL realized {}",
            event_id,
            positions_affected,
            total_reduced_size,
            total_pnl_realized
        );

        self.get_adl_event(event_id).await
    }

    /// Get ADL event by ID
    async fn get_adl_event(&self, event_id: Uuid) -> Result<AdlEvent, sqlx::Error> {
        sqlx::query_as::<_, AdlEvent>("SELECT * FROM adl_events WHERE id = $1")
            .bind(event_id)
            .fetch_one(&self.pool)
            .await
    }

    /// Update user ADL statistics
    async fn update_user_stats(
        &self,
        user_address: &str,
        market_symbol: &str,
        size_reduced: Decimal,
        pnl_realized: Decimal,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            INSERT INTO user_adl_stats (user_address, market_symbol, total_adl_events, total_size_reduced, total_pnl_realized, last_adl_at)
            VALUES ($1, $2, 1, $3, $4, NOW())
            ON CONFLICT (user_address, market_symbol)
            DO UPDATE SET
                total_adl_events = user_adl_stats.total_adl_events + 1,
                total_size_reduced = user_adl_stats.total_size_reduced + $3,
                total_pnl_realized = user_adl_stats.total_pnl_realized + $4,
                last_adl_at = NOW(),
                updated_at = NOW()
            "#,
        )
        .bind(user_address)
        .bind(market_symbol)
        .bind(size_reduced)
        .bind(pnl_realized)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Start ADL ranking update loop
    pub async fn start_ranking_update_loop(self: Arc<Self>, markets: Vec<String>) {
        let service = self.clone();

        tokio::spawn(async move {
            tracing::info!("Starting ADL ranking update loop for markets: {:?}", markets);

            loop {
                use crate::services::metrics::{TaskTimer, TASK_LAST_RUN_TIMESTAMP};

                TASK_LAST_RUN_TIMESTAMP
                    .with_label_values(&["adl-ranking-update"])
                    .set(chrono::Utc::now().timestamp() as f64);

                for market in &markets {
                    let timer = TaskTimer::start("adl-ranking-update", market);
                    match service.update_rankings(market).await {
                        Ok(()) => timer.success(),
                        Err(e) => {
                            tracing::error!("Error updating ADL rankings for {}: {:?}", market, e);
                            timer.failure(&format!("{:?}", e));
                        }
                    }
                }

                // Update rankings every 30 seconds
                tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
            }
        }.instrument(tracing::info_span!("adl-ranking-update")));
    }
}
