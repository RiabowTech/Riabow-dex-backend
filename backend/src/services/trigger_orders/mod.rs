//! Trigger Orders Service - Stop-Loss, Take-Profit, Trailing Stop
//!
//! This service manages advanced order types that trigger based on price conditions.
//! Similar to GMX V2's trigger order system.

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool};
use std::sync::Arc;
use uuid::Uuid;

use crate::models::PositionSide;
use crate::services::price_feed::PriceFeedService;

/// Trigger order type
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, sqlx::Type)]
#[sqlx(type_name = "trigger_order_type", rename_all = "snake_case")]
pub enum TriggerOrderType {
    StopLoss,
    TakeProfit,
    TrailingStop,
    StopLimit,
    TakeProfitLimit,
}

/// Trigger condition
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, sqlx::Type)]
#[sqlx(type_name = "trigger_condition", rename_all = "snake_case")]
pub enum TriggerCondition {
    PriceAbove,
    PriceBelow,
}

/// Trigger order status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, sqlx::Type)]
#[sqlx(type_name = "trigger_order_status", rename_all = "snake_case")]
pub enum TriggerOrderStatus {
    Active,
    Triggered,
    Executed,
    Cancelled,
    Expired,
    Failed,
}

/// Order side (buy/sell)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, sqlx::Type)]
#[sqlx(type_name = "order_side", rename_all = "snake_case")]
pub enum OrderSide {
    Buy,
    Sell,
}

/// Trigger order from database
#[derive(Debug, Clone, FromRow, Serialize)]
pub struct TriggerOrder {
    pub id: Uuid,
    pub user_address: String,
    pub position_id: Option<Uuid>,
    pub market_symbol: String,
    pub trigger_type: TriggerOrderType,
    pub side: OrderSide,
    /// Size to close in USD. Will be converted to tokens when executed.
    /// Use 0 or set close_position=true for full position close.
    pub size: Decimal,
    pub trigger_price: Decimal,
    pub trigger_condition: TriggerCondition,
    pub limit_price: Option<Decimal>,
    pub trailing_delta: Option<Decimal>,
    pub trailing_delta_type: Option<String>,
    pub peak_price: Option<Decimal>,
    pub status: TriggerOrderStatus,
    pub triggered_at: Option<DateTime<Utc>>,
    pub triggered_price: Option<Decimal>,
    pub executed_order_id: Option<Uuid>,
    pub executed_price: Option<Decimal>,
    pub executed_at: Option<DateTime<Utc>>,
    pub reduce_only: bool,
    pub close_position: bool,
    pub expires_at: Option<DateTime<Utc>>,
    pub client_order_id: Option<String>,
    pub error_message: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Trigger order execution record
#[derive(Debug, Clone, FromRow, Serialize)]
pub struct TriggerOrderExecution {
    pub id: Uuid,
    pub trigger_order_id: Uuid,
    pub user_address: String,
    pub market_symbol: String,
    pub trigger_type: TriggerOrderType,
    pub trigger_price: Decimal,
    pub mark_price: Decimal,
    pub execution_price: Option<Decimal>,
    /// Executed size in tokens (converted from USD at execution time)
    pub size: Decimal,
    pub side: OrderSide,
    pub success: bool,
    pub error_message: Option<String>,
    pub resulting_order_id: Option<Uuid>,
    pub realized_pnl: Option<Decimal>,
    pub created_at: DateTime<Utc>,
}

/// Position TP/SL settings
#[derive(Debug, Clone, FromRow, Serialize)]
pub struct PositionTpSl {
    pub id: Uuid,
    pub position_id: Uuid,
    pub user_address: String,
    pub market_symbol: String,
    pub take_profit_price: Option<Decimal>,
    pub take_profit_size: Option<Decimal>,
    pub take_profit_trigger_order_id: Option<Uuid>,
    pub stop_loss_price: Option<Decimal>,
    pub stop_loss_size: Option<Decimal>,
    pub stop_loss_trigger_order_id: Option<Uuid>,
    pub trailing_stop_delta: Option<Decimal>,
    pub trailing_stop_delta_type: Option<String>,
    pub trailing_stop_size: Option<Decimal>,
    pub trailing_stop_trigger_order_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Trigger order configuration
#[derive(Debug, Clone, FromRow, Serialize)]
pub struct TriggerOrderConfig {
    pub id: Uuid,
    pub market_symbol: String,
    pub max_trigger_orders_per_user: i32,
    pub max_trigger_orders_per_position: i32,
    pub min_trigger_distance_pct: Decimal,
    pub max_trigger_distance_pct: Decimal,
    pub min_trailing_delta_pct: Decimal,
    pub max_trailing_delta_pct: Decimal,
    pub trigger_check_interval_ms: i32,
    pub slippage_tolerance_pct: Decimal,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// User trigger order statistics
#[derive(Debug, Clone, FromRow, Serialize)]
pub struct UserTriggerOrderStats {
    pub user_address: String,
    pub market_symbol: String,
    pub total_stop_loss_orders: i64,
    pub triggered_stop_loss_orders: i64,
    pub stop_loss_pnl: Decimal,
    pub total_take_profit_orders: i64,
    pub triggered_take_profit_orders: i64,
    pub take_profit_pnl: Decimal,
    pub total_trailing_stop_orders: i64,
    pub triggered_trailing_stop_orders: i64,
    pub trailing_stop_pnl: Decimal,
    pub total_saved_by_sl: Decimal,
    pub total_captured_by_tp: Decimal,
}

/// Request to create a trigger order
#[derive(Debug, Deserialize)]
pub struct CreateTriggerOrderRequest {
    pub position_id: Option<Uuid>,
    pub market_symbol: String,
    pub trigger_type: TriggerOrderType,
    pub side: OrderSide,
    /// Size to close in USD. Use 0 for full position close.
    pub size: Decimal,
    pub trigger_price: Decimal,
    pub limit_price: Option<Decimal>,
    pub trailing_delta: Option<Decimal>,
    pub trailing_delta_type: Option<String>,
    pub reduce_only: Option<bool>,
    pub close_position: Option<bool>,
    pub expires_at: Option<DateTime<Utc>>,
    pub client_order_id: Option<String>,
}

/// Request to set position TP/SL
#[derive(Debug, Deserialize)]
pub struct SetPositionTpSlRequest {
    pub take_profit_price: Option<Decimal>,
    /// Size to close in USD when take profit triggers. None or 0 = full position.
    pub take_profit_size: Option<Decimal>,
    /// Optional limit price for take profit. When set, the triggered order is
    /// placed as a LIMIT order (trigger_type = take_profit_limit). When None,
    /// falls back to a MARKET order on trigger.
    pub take_profit_limit_price: Option<Decimal>,
    pub stop_loss_price: Option<Decimal>,
    /// Size to close in USD when stop loss triggers. None or 0 = full position.
    pub stop_loss_size: Option<Decimal>,
    /// Optional limit price for stop loss. When set, the triggered order is
    /// placed as a LIMIT order (trigger_type = stop_limit). When None, falls
    /// back to a MARKET order on trigger.
    pub stop_loss_limit_price: Option<Decimal>,
    pub trailing_stop_delta: Option<Decimal>,
    pub trailing_stop_delta_type: Option<String>,
    pub trailing_stop_size: Option<Decimal>,
}

/// Trigger Orders Service
pub struct TriggerOrdersService {
    pool: PgPool,
    price_feed_service: Arc<PriceFeedService>,
}

impl TriggerOrdersService {
    pub fn new(pool: PgPool, price_feed_service: Arc<PriceFeedService>) -> Self {
        Self {
            pool,
            price_feed_service,
        }
    }

    /// Get a reference to the database pool (for use in spawned tasks)
    pub fn get_pool(&self) -> &PgPool {
        &self.pool
    }

    /// Create a new trigger order
    /// Uses atomic INSERT...SELECT to prevent race conditions on limit checks
    pub async fn create_trigger_order(
        &self,
        user_address: &str,
        request: CreateTriggerOrderRequest,
    ) -> Result<TriggerOrder> {
        // Validate the request (price distance, trailing delta, etc.)
        // Note: count-based limits are checked atomically in the INSERT below
        self.validate_trigger_order_request(user_address, &request).await?;

        let config = self.get_config(&request.market_symbol).await?;

        // Determine trigger condition based on order type and side
        let trigger_condition = self.determine_trigger_condition(&request.trigger_type, &request.side);

        // Get current price for trailing stop initialization
        let peak_price = if request.trigger_type == TriggerOrderType::TrailingStop {
            self.price_feed_service
                .get_mark_price(&request.market_symbol)
                .await
        } else {
            None
        };

        // Atomic INSERT with count check to prevent race conditions
        // This ensures that limits are checked and enforced atomically
        let order = sqlx::query_as::<_, TriggerOrder>(
            r#"
            INSERT INTO trigger_orders (
                user_address, position_id, market_symbol, trigger_type, side, size,
                trigger_price, trigger_condition, limit_price, trailing_delta,
                trailing_delta_type, peak_price, reduce_only, close_position,
                expires_at, client_order_id
            )
            SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
            WHERE
                -- Check user's total active orders limit
                (SELECT COUNT(*) FROM trigger_orders WHERE user_address = $1 AND status = 'active') < $17
                -- Check position-specific limit (only if position_id is provided)
                AND (
                    $2 IS NULL
                    OR (SELECT COUNT(*) FROM trigger_orders WHERE position_id = $2 AND status = 'active') < $18
                )
            RETURNING *
            "#,
        )
        .bind(user_address)                                    // $1
        .bind(request.position_id)                             // $2
        .bind(&request.market_symbol)                          // $3
        .bind(&request.trigger_type)                           // $4
        .bind(&request.side)                                   // $5
        .bind(request.size)                                    // $6
        .bind(request.trigger_price)                           // $7
        .bind(&trigger_condition)                              // $8
        .bind(request.limit_price)                             // $9
        .bind(request.trailing_delta)                          // $10
        .bind(&request.trailing_delta_type)                    // $11
        .bind(peak_price)                                      // $12
        .bind(request.reduce_only.unwrap_or(true))            // $13
        .bind(request.close_position.unwrap_or(false))        // $14
        .bind(request.expires_at)                              // $15
        .bind(&request.client_order_id)                        // $16
        .bind(config.max_trigger_orders_per_user)             // $17
        .bind(config.max_trigger_orders_per_position)         // $18
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| anyhow!(
            "Maximum trigger orders reached (user limit: {}, position limit: {})",
            config.max_trigger_orders_per_user,
            config.max_trigger_orders_per_position
        ))?;

        tracing::info!(
            "Created trigger order {} for user {} on {}",
            order.id,
            user_address,
            request.market_symbol
        );

        Ok(order)
    }

    /// Cancel a trigger order
    ///
    /// 原来的实现把 not_found / ownership / 状态非 active 揉成一条 "Trigger order not
    /// found or not cancellable"，出现 keeper 已触发或 UI stale 的场景时用户拿不到有
    /// 意义的错误。现在先 SELECT 再 conditional UPDATE，把四种失败分开报：
    ///   - 订单不存在
    ///   - 不属于当前用户
    ///   - 已处于 triggered / executed / cancelled / expired / failed
    ///   - SELECT-UPDATE 之间 keeper 刚触发（原子 guard 失败）
    pub async fn cancel_trigger_order(
        &self,
        user_address: &str,
        order_id: Uuid,
    ) -> Result<TriggerOrder> {
        let existing = sqlx::query_as::<_, TriggerOrder>(
            "SELECT * FROM trigger_orders WHERE id = $1",
        )
        .bind(order_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| anyhow!("Trigger order not found"))?;

        // JWT 走 `claims.sub` 已是 lowercase、API key 从 users 表直接读，历史上两边
        // 大小写不一定一致，用 eq_ignore_ascii_case 与 position handler 保持一致。
        if !existing.user_address.eq_ignore_ascii_case(user_address) {
            return Err(anyhow!("Trigger order does not belong to you"));
        }

        match existing.status {
            TriggerOrderStatus::Active => {}
            TriggerOrderStatus::Triggered => {
                return Err(anyhow!(
                    "Trigger order already fired; cannot cancel once triggered"
                ));
            }
            TriggerOrderStatus::Executed => {
                return Err(anyhow!("Trigger order already executed; cannot cancel"));
            }
            TriggerOrderStatus::Cancelled => {
                return Err(anyhow!("Trigger order already cancelled"));
            }
            TriggerOrderStatus::Expired => {
                return Err(anyhow!("Trigger order already expired"));
            }
            TriggerOrderStatus::Failed => {
                return Err(anyhow!("Trigger order already failed; cannot cancel"));
            }
        }

        // 原子 guard：SELECT 到 UPDATE 之间 keeper 可能刚好把它推进 triggered；
        // 这里还保留 status='active' 条件，避免覆盖已触发的订单。
        let order = sqlx::query_as::<_, TriggerOrder>(
            r#"
            UPDATE trigger_orders
            SET status = 'cancelled', updated_at = NOW()
            WHERE id = $1 AND status = 'active'
            RETURNING *
            "#,
        )
        .bind(order_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| anyhow!("Trigger order just fired; cannot cancel"))?;

        tracing::info!("Cancelled trigger order {} for user {}", order_id, user_address);

        Ok(order)
    }

    /// Get user's trigger orders
    pub async fn get_user_trigger_orders(
        &self,
        user_address: &str,
        market_symbol: Option<&str>,
        status: Option<TriggerOrderStatus>,
        limit: i64,
    ) -> Result<Vec<TriggerOrder>> {
        let orders = if let Some(symbol) = market_symbol {
            if let Some(st) = status {
                sqlx::query_as::<_, TriggerOrder>(
                    r#"
                    SELECT * FROM trigger_orders
                    WHERE user_address = $1 AND market_symbol = $2 AND status = $3
                    ORDER BY created_at DESC
                    LIMIT $4
                    "#,
                )
                .bind(user_address)
                .bind(symbol)
                .bind(st)
                .bind(limit)
                .fetch_all(&self.pool)
                .await?
            } else {
                sqlx::query_as::<_, TriggerOrder>(
                    r#"
                    SELECT * FROM trigger_orders
                    WHERE user_address = $1 AND market_symbol = $2
                    ORDER BY created_at DESC
                    LIMIT $3
                    "#,
                )
                .bind(user_address)
                .bind(symbol)
                .bind(limit)
                .fetch_all(&self.pool)
                .await?
            }
        } else if let Some(st) = status {
            sqlx::query_as::<_, TriggerOrder>(
                r#"
                SELECT * FROM trigger_orders
                WHERE user_address = $1 AND status = $2
                ORDER BY created_at DESC
                LIMIT $3
                "#,
            )
            .bind(user_address)
            .bind(st)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query_as::<_, TriggerOrder>(
                r#"
                SELECT * FROM trigger_orders
                WHERE user_address = $1
                ORDER BY created_at DESC
                LIMIT $2
                "#,
            )
            .bind(user_address)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        };

        Ok(orders)
    }

    /// Get a specific trigger order
    pub async fn get_trigger_order(
        &self,
        user_address: &str,
        order_id: Uuid,
    ) -> Result<Option<TriggerOrder>> {
        let order = sqlx::query_as::<_, TriggerOrder>(
            "SELECT * FROM trigger_orders WHERE id = $1 AND user_address = $2",
        )
        .bind(order_id)
        .bind(user_address)
        .fetch_optional(&self.pool)
        .await?;

        Ok(order)
    }

    /// Get active trigger orders for a market (for monitoring)
    pub async fn get_active_orders_for_market(
        &self,
        market_symbol: &str,
    ) -> Result<Vec<TriggerOrder>> {
        let orders = sqlx::query_as::<_, TriggerOrder>(
            r#"
            SELECT * FROM trigger_orders
            WHERE market_symbol = $1 AND status = 'active'
            AND (expires_at IS NULL OR expires_at > NOW())
            ORDER BY trigger_price ASC
            "#,
        )
        .bind(market_symbol)
        .fetch_all(&self.pool)
        .await?;

        Ok(orders)
    }

    /// Check and trigger orders based on current price
    pub async fn check_and_trigger_orders(&self, market_symbol: &str) -> Result<Vec<TriggerOrder>> {
        let current_price = self
            .price_feed_service
            .get_mark_price(market_symbol)
            .await
            .ok_or_else(|| anyhow!("No price available for {}", market_symbol))?;

        let mark_price = current_price;
        let active_orders = self.get_active_orders_for_market(market_symbol).await?;
        let mut triggered_orders = Vec::new();

        for order in active_orders {
            let should_trigger = match order.trigger_type {
                TriggerOrderType::TrailingStop => {
                    self.check_trailing_stop_trigger(&order, mark_price).await?
                }
                _ => self.check_price_trigger(&order, mark_price),
            };

            if should_trigger {
                match self.execute_trigger_order(&order, mark_price).await {
                    Ok(executed_order) => {
                        triggered_orders.push(executed_order);
                    }
                    Err(e) => {
                        tracing::error!("Failed to execute trigger order {}: {:?}", order.id, e);
                        self.mark_order_failed(order.id, &e.to_string()).await?;
                    }
                }
            }
        }

        // Also expire any orders that have passed their expiry
        self.expire_old_orders(market_symbol).await?;

        Ok(triggered_orders)
    }

    /// Check if a regular price trigger should fire
    fn check_price_trigger(&self, order: &TriggerOrder, current_price: Decimal) -> bool {
        match order.trigger_condition {
            TriggerCondition::PriceAbove => current_price >= order.trigger_price,
            TriggerCondition::PriceBelow => current_price <= order.trigger_price,
        }
    }

    /// Check and update trailing stop trigger
    async fn check_trailing_stop_trigger(
        &self,
        order: &TriggerOrder,
        current_price: Decimal,
    ) -> Result<bool> {
        let delta = order.trailing_delta.unwrap_or(Decimal::ZERO);
        let delta_type = order.trailing_delta_type.as_deref().unwrap_or("absolute");
        let peak = order.peak_price.unwrap_or(current_price);

        // Update peak price if we have a new high/low
        let new_peak = match order.side {
            OrderSide::Sell => {
                // For sell (closing long), track highest price
                if current_price > peak {
                    Some(current_price)
                } else {
                    None
                }
            }
            OrderSide::Buy => {
                // For buy (closing short), track lowest price
                if current_price < peak {
                    Some(current_price)
                } else {
                    None
                }
            }
        };

        // Update peak if changed
        if let Some(new_peak_price) = new_peak {
            sqlx::query(
                "UPDATE trigger_orders SET peak_price = $1, updated_at = NOW() WHERE id = $2",
            )
            .bind(new_peak_price)
            .bind(order.id)
            .execute(&self.pool)
            .await?;
        }

        let effective_peak = new_peak.unwrap_or(peak);

        // Calculate trigger threshold
        let trigger_threshold = if delta_type == "percentage" {
            effective_peak * (Decimal::ONE - delta / Decimal::from(100))
        } else {
            effective_peak - delta
        };

        // Check if we should trigger
        let should_trigger = match order.side {
            OrderSide::Sell => current_price <= trigger_threshold,
            OrderSide::Buy => current_price >= trigger_threshold,
        };

        Ok(should_trigger)
    }

    /// Execute a triggered order
    /// All operations are wrapped in a transaction to ensure atomicity
    async fn execute_trigger_order(
        &self,
        order: &TriggerOrder,
        mark_price: Decimal,
    ) -> Result<TriggerOrder> {
        // Start transaction for atomic execution
        let mut tx = self.pool.begin().await?;

        // Step 1: Mark order as triggered (only if still active to prevent race conditions)
        let result = sqlx::query(
            r#"
            UPDATE trigger_orders
            SET status = 'triggered', triggered_at = NOW(), triggered_price = $1, updated_at = NOW()
            WHERE id = $2 AND status = 'active'
            "#,
        )
        .bind(mark_price)
        .bind(order.id)
        .execute(&mut *tx)
        .await?;

        // If no rows were affected, the order was already processed by another service
        if result.rows_affected() == 0 {
            tx.rollback().await?;
            return Err(anyhow!("Order {} already processed or not active", order.id));
        }

        // Step 2: Update user stats (within transaction)
        // Note: Execution record is created by the Keeper service which handles
        // actual order execution via matching engine
        self.update_user_stats_tx(&mut tx, &order.user_address, &order.market_symbol, &order.trigger_type)
            .await?;

        // Step 4: Mark as executed
        let executed_order = sqlx::query_as::<_, TriggerOrder>(
            r#"
            UPDATE trigger_orders
            SET status = 'executed', executed_price = $1, executed_at = NOW(), updated_at = NOW()
            WHERE id = $2
            RETURNING *
            "#,
        )
        .bind(mark_price)
        .bind(order.id)
        .fetch_one(&mut *tx)
        .await?;

        // Commit transaction atomically
        tx.commit().await?;

        tracing::info!(
            "Trigger order {} executed at price {} (trigger: {})",
            order.id,
            mark_price,
            order.trigger_price
        );

        Ok(executed_order)
    }

    /// Mark an order as failed
    async fn mark_order_failed(&self, order_id: Uuid, error: &str) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE trigger_orders
            SET status = 'failed', error_message = $1, updated_at = NOW()
            WHERE id = $2
            "#,
        )
        .bind(error)
        .bind(order_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Expire old orders
    async fn expire_old_orders(&self, market_symbol: &str) -> Result<i64> {
        let result = sqlx::query(
            r#"
            UPDATE trigger_orders
            SET status = 'expired', updated_at = NOW()
            WHERE market_symbol = $1 AND status = 'active' AND expires_at <= NOW()
            "#,
        )
        .bind(market_symbol)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() as i64)
    }

    /// Update user statistics
    #[allow(dead_code)]
    async fn update_user_stats(
        &self,
        user_address: &str,
        market_symbol: &str,
        trigger_type: &TriggerOrderType,
    ) -> Result<()> {
        let col = Self::get_stats_column(trigger_type);

        // Upsert stats
        let query = format!(
            r#"
            INSERT INTO user_trigger_order_stats (user_address, market_symbol, {col} )
            VALUES ($1, $2, 1)
            ON CONFLICT (user_address, market_symbol) DO UPDATE
            SET {col} = user_trigger_order_stats.{col} + 1, updated_at = NOW()
            "#,
            col = col
        );

        sqlx::query(&query)
            .bind(user_address)
            .bind(market_symbol)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    /// Update user statistics (transaction-aware version)
    async fn update_user_stats_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        user_address: &str,
        market_symbol: &str,
        trigger_type: &TriggerOrderType,
    ) -> Result<()> {
        let col = Self::get_stats_column(trigger_type);

        // Upsert stats within transaction
        let query = format!(
            r#"
            INSERT INTO user_trigger_order_stats (user_address, market_symbol, {col} )
            VALUES ($1, $2, 1)
            ON CONFLICT (user_address, market_symbol) DO UPDATE
            SET {col} = user_trigger_order_stats.{col} + 1, updated_at = NOW()
            "#,
            col = col
        );

        sqlx::query(&query)
            .bind(user_address)
            .bind(market_symbol)
            .execute(&mut **tx)
            .await?;

        Ok(())
    }

    /// Get the stats column name based on trigger type
    fn get_stats_column(trigger_type: &TriggerOrderType) -> &'static str {
        match trigger_type {
            TriggerOrderType::StopLoss | TriggerOrderType::StopLimit => "triggered_stop_loss_orders",
            TriggerOrderType::TakeProfit | TriggerOrderType::TakeProfitLimit => "triggered_take_profit_orders",
            TriggerOrderType::TrailingStop => "triggered_trailing_stop_orders",
        }
    }

    /// Set position TP/SL
    pub async fn set_position_tp_sl(
        &self,
        user_address: &str,
        position_id: Uuid,
        market_symbol: &str,
        position_side: PositionSide,
        request: SetPositionTpSlRequest,
    ) -> Result<PositionTpSl> {
        // Determine the correct order side for closing based on position side
        // Long position closes with SELL, Short position closes with BUY
        let closing_order_side = match position_side {
            PositionSide::Long => OrderSide::Sell,
            PositionSide::Short => OrderSide::Buy,
        };
        // Get existing TP/SL settings to preserve values not being updated
        let existing = self.get_position_tp_sl(position_id).await?;

        // Only cancel orders of types that are being replaced
        // Cancel TP orders only if new TP price is specified
        if request.take_profit_price.is_some() {
            sqlx::query(
                r#"
                UPDATE trigger_orders
                SET status = 'cancelled', updated_at = NOW()
                WHERE position_id = $1 AND status = 'active'
                AND trigger_type IN ('take_profit', 'take_profit_limit')
                "#,
            )
            .bind(position_id)
            .execute(&self.pool)
            .await?;
        }

        // Cancel SL orders only if new SL price is specified
        if request.stop_loss_price.is_some() {
            sqlx::query(
                r#"
                UPDATE trigger_orders
                SET status = 'cancelled', updated_at = NOW()
                WHERE position_id = $1 AND status = 'active'
                AND trigger_type IN ('stop_loss', 'stop_limit')
                "#,
            )
            .bind(position_id)
            .execute(&self.pool)
            .await?;
        }

        // Cancel trailing stop orders only if new trailing stop is specified
        if request.trailing_stop_delta.is_some() {
            sqlx::query(
                r#"
                UPDATE trigger_orders
                SET status = 'cancelled', updated_at = NOW()
                WHERE position_id = $1 AND status = 'active'
                AND trigger_type = 'trailing_stop'
                "#,
            )
            .bind(position_id)
            .execute(&self.pool)
            .await?;
        }

        // Create new TP order if specified, otherwise preserve existing
        let (final_tp_price, final_tp_size, tp_order_id) = if let Some(tp_price) = request.take_profit_price {
            let tp_size = request.take_profit_size.unwrap_or(Decimal::ZERO);
            let close_full_position = tp_size == Decimal::ZERO;

            // When the caller supplies a take_profit_limit_price, the triggered
            // order is a LIMIT order (take_profit_limit); otherwise it falls
            // back to a MARKET take_profit — preserving the previous behaviour.
            let (tp_trigger_type, tp_limit_price) = match request.take_profit_limit_price {
                Some(lp) => (TriggerOrderType::TakeProfitLimit, Some(lp)),
                None => (TriggerOrderType::TakeProfit, None),
            };

            let order = self
                .create_trigger_order(
                    user_address,
                    CreateTriggerOrderRequest {
                        position_id: Some(position_id),
                        market_symbol: market_symbol.to_string(),
                        trigger_type: tp_trigger_type,
                        side: closing_order_side.clone(), // Use correct side based on position
                        size: tp_size,
                        trigger_price: tp_price,
                        limit_price: tp_limit_price,
                        trailing_delta: None,
                        trailing_delta_type: None,
                        reduce_only: Some(true),
                        close_position: Some(close_full_position),
                        expires_at: None,
                        client_order_id: None,
                    },
                )
                .await?;
            (Some(tp_price), request.take_profit_size, Some(order.id))
        } else if let Some(ref ex) = existing {
            // Preserve existing TP values
            (ex.take_profit_price, ex.take_profit_size, ex.take_profit_trigger_order_id)
        } else {
            (None, None, None)
        };

        // Create new SL order if specified, otherwise preserve existing
        let (final_sl_price, final_sl_size, sl_order_id) = if let Some(sl_price) = request.stop_loss_price {
            let sl_size = request.stop_loss_size.unwrap_or(Decimal::ZERO);
            let close_full_position = sl_size == Decimal::ZERO;

            // Same pattern as TP above: an explicit stop_loss_limit_price opts
            // into a LIMIT stop (stop_limit); otherwise a MARKET stop_loss.
            let (sl_trigger_type, sl_limit_price) = match request.stop_loss_limit_price {
                Some(lp) => (TriggerOrderType::StopLimit, Some(lp)),
                None => (TriggerOrderType::StopLoss, None),
            };

            let order = self
                .create_trigger_order(
                    user_address,
                    CreateTriggerOrderRequest {
                        position_id: Some(position_id),
                        market_symbol: market_symbol.to_string(),
                        trigger_type: sl_trigger_type,
                        side: closing_order_side.clone(), // Use correct side based on position
                        size: sl_size,
                        trigger_price: sl_price,
                        limit_price: sl_limit_price,
                        trailing_delta: None,
                        trailing_delta_type: None,
                        reduce_only: Some(true),
                        close_position: Some(close_full_position),
                        expires_at: None,
                        client_order_id: None,
                    },
                )
                .await?;
            (Some(sl_price), request.stop_loss_size, Some(order.id))
        } else if let Some(ref ex) = existing {
            // Preserve existing SL values
            (ex.stop_loss_price, ex.stop_loss_size, ex.stop_loss_trigger_order_id)
        } else {
            (None, None, None)
        };

        // Create trailing stop if specified, otherwise preserve existing
        let (final_ts_delta, final_ts_delta_type, final_ts_size, ts_order_id) = if let Some(delta) = request.trailing_stop_delta {
            let ts_size = request.trailing_stop_size.unwrap_or(Decimal::ZERO);
            let close_full_position = ts_size == Decimal::ZERO;

            let order = self
                .create_trigger_order(
                    user_address,
                    CreateTriggerOrderRequest {
                        position_id: Some(position_id),
                        market_symbol: market_symbol.to_string(),
                        trigger_type: TriggerOrderType::TrailingStop,
                        side: closing_order_side.clone(), // Use correct side based on position
                        size: ts_size,
                        trigger_price: Decimal::ZERO, // Will be calculated dynamically
                        limit_price: None,
                        trailing_delta: Some(delta),
                        trailing_delta_type: request.trailing_stop_delta_type.clone(),
                        reduce_only: Some(true),
                        close_position: Some(close_full_position),
                        expires_at: None,
                        client_order_id: None,
                    },
                )
                .await?;
            (Some(delta), request.trailing_stop_delta_type.clone(), request.trailing_stop_size, Some(order.id))
        } else if let Some(ref ex) = existing {
            // Preserve existing trailing stop values
            (ex.trailing_stop_delta, ex.trailing_stop_delta_type.clone(), ex.trailing_stop_size, ex.trailing_stop_trigger_order_id)
        } else {
            (None, None, None, None)
        };

        // Upsert position TP/SL record with merged values
        let tp_sl = sqlx::query_as::<_, PositionTpSl>(
            r#"
            INSERT INTO position_tp_sl (
                position_id, user_address, market_symbol,
                take_profit_price, take_profit_size, take_profit_trigger_order_id,
                stop_loss_price, stop_loss_size, stop_loss_trigger_order_id,
                trailing_stop_delta, trailing_stop_delta_type, trailing_stop_size, trailing_stop_trigger_order_id
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            ON CONFLICT (position_id) DO UPDATE SET
                take_profit_price = $4,
                take_profit_size = $5,
                take_profit_trigger_order_id = $6,
                stop_loss_price = $7,
                stop_loss_size = $8,
                stop_loss_trigger_order_id = $9,
                trailing_stop_delta = $10,
                trailing_stop_delta_type = $11,
                trailing_stop_size = $12,
                trailing_stop_trigger_order_id = $13,
                updated_at = NOW()
            RETURNING *
            "#,
        )
        .bind(position_id)
        .bind(user_address)
        .bind(market_symbol)
        .bind(final_tp_price)
        .bind(final_tp_size)
        .bind(tp_order_id)
        .bind(final_sl_price)
        .bind(final_sl_size)
        .bind(sl_order_id)
        .bind(final_ts_delta)
        .bind(&final_ts_delta_type)
        .bind(final_ts_size)
        .bind(ts_order_id)
        .fetch_one(&self.pool)
        .await?;

        Ok(tp_sl)
    }

    /// Get position TP/SL settings
    pub async fn get_position_tp_sl(&self, position_id: Uuid) -> Result<Option<PositionTpSl>> {
        let tp_sl = sqlx::query_as::<_, PositionTpSl>(
            "SELECT * FROM position_tp_sl WHERE position_id = $1",
        )
        .bind(position_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(tp_sl)
    }

    /// Delete position TP/SL settings
    pub async fn delete_position_tp_sl(&self, position_id: Uuid) -> Result<()> {
        // First, cancel all active trigger orders for this position
        sqlx::query(
            r#"
            UPDATE trigger_orders
            SET status = 'cancelled', updated_at = NOW()
            WHERE position_id = $1 AND status = 'active'
            "#,
        )
        .bind(position_id)
        .execute(&self.pool)
        .await?;

        // Then delete the position_tp_sl record
        sqlx::query("DELETE FROM position_tp_sl WHERE position_id = $1")
            .bind(position_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    /// Get trigger order config
    pub async fn get_config(&self, market_symbol: &str) -> Result<TriggerOrderConfig> {
        let config = sqlx::query_as::<_, TriggerOrderConfig>(
            "SELECT * FROM trigger_order_config WHERE market_symbol = $1",
        )
        .bind(market_symbol)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| anyhow!("No trigger order config for {}", market_symbol))?;

        Ok(config)
    }

    /// Get user trigger order stats
    pub async fn get_user_stats(
        &self,
        user_address: &str,
        market_symbol: &str,
    ) -> Result<Option<UserTriggerOrderStats>> {
        let stats = sqlx::query_as::<_, UserTriggerOrderStats>(
            "SELECT * FROM user_trigger_order_stats WHERE user_address = $1 AND market_symbol = $2",
        )
        .bind(user_address)
        .bind(market_symbol)
        .fetch_optional(&self.pool)
        .await?;

        Ok(stats)
    }

    /// Get user's trigger order execution history
    pub async fn get_user_executions(
        &self,
        user_address: &str,
        limit: i64,
    ) -> Result<Vec<TriggerOrderExecution>> {
        let executions = sqlx::query_as::<_, TriggerOrderExecution>(
            r#"
            SELECT * FROM trigger_order_executions
            WHERE user_address = $1
            ORDER BY created_at DESC
            LIMIT $2
            "#,
        )
        .bind(user_address)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(executions)
    }

    /// Validate trigger order request
    /// Note: Count-based limits (max orders per user/position) are checked atomically
    /// in create_trigger_order() to prevent race conditions
    async fn validate_trigger_order_request(
        &self,
        _user_address: &str,
        request: &CreateTriggerOrderRequest,
    ) -> Result<()> {
        let config = self.get_config(&request.market_symbol).await?;

        if !config.enabled {
            return Err(anyhow!("Trigger orders are disabled for this market"));
        }

        // Note: Count-based limit checks have been moved to create_trigger_order()
        // where they are enforced atomically with the INSERT to prevent race conditions

        // Validate trigger price distance from current price
        if let Some(current_price) = self
            .price_feed_service
            .get_mark_price(&request.market_symbol)
            .await
        {
            let price_diff = crate::safe_div!(
                (request.trigger_price - current_price).abs(),
                current_price,
                "TriggerOrders: validate price_diff"
            ) * Decimal::from(100);

            if price_diff < config.min_trigger_distance_pct {
                return Err(anyhow!(
                    "Trigger price too close to current price (min {}%)",
                    config.min_trigger_distance_pct
                ));
            }

            if price_diff > config.max_trigger_distance_pct {
                return Err(anyhow!(
                    "Trigger price too far from current price (max {}%)",
                    config.max_trigger_distance_pct
                ));
            }
        }

        // Validate trailing stop delta
        if request.trigger_type == TriggerOrderType::TrailingStop {
            if let Some(delta) = request.trailing_delta {
                let delta_type = request.trailing_delta_type.as_deref().unwrap_or("absolute");
                if delta_type == "percentage" {
                    if delta < config.min_trailing_delta_pct {
                        return Err(anyhow!(
                            "Trailing delta too small (min {}%)",
                            config.min_trailing_delta_pct
                        ));
                    }
                    if delta > config.max_trailing_delta_pct {
                        return Err(anyhow!(
                            "Trailing delta too large (max {}%)",
                            config.max_trailing_delta_pct
                        ));
                    }
                }
            } else {
                return Err(anyhow!("Trailing stop requires trailing_delta"));
            }
        }

        Ok(())
    }

    /// Determine trigger condition based on order type and side
    fn determine_trigger_condition(
        &self,
        trigger_type: &TriggerOrderType,
        side: &OrderSide,
    ) -> TriggerCondition {
        match trigger_type {
            TriggerOrderType::StopLoss | TriggerOrderType::StopLimit => {
                match side {
                    OrderSide::Sell => TriggerCondition::PriceBelow, // Long position SL
                    OrderSide::Buy => TriggerCondition::PriceAbove,  // Short position SL
                }
            }
            TriggerOrderType::TakeProfit | TriggerOrderType::TakeProfitLimit => {
                match side {
                    OrderSide::Sell => TriggerCondition::PriceAbove, // Long position TP
                    OrderSide::Buy => TriggerCondition::PriceBelow,  // Short position TP
                }
            }
            TriggerOrderType::TrailingStop => {
                // Trailing stop always monitors in the direction opposite to exit
                match side {
                    OrderSide::Sell => TriggerCondition::PriceBelow, // Triggered when price drops from peak
                    OrderSide::Buy => TriggerCondition::PriceAbove,  // Triggered when price rises from trough
                }
            }
        }
    }

    /// Start the trigger order monitoring loop
    pub async fn start_monitoring_loop(self: Arc<Self>, markets: Vec<String>) {
        tracing::info!("Starting trigger order monitoring for {:?}", markets);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(500));

            loop {
                interval.tick().await;

                for market in &markets {
                    if let Err(e) = self.check_and_trigger_orders(market).await {
                        tracing::error!("Error checking triggers for {}: {:?}", market, e);
                    }
                }
            }
        });
    }
}
