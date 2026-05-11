//! Order Matching Engine Module
//!
//! High-performance order matching with price-time priority.
//!
//! # Architecture
//!
//! ```text
//! API Handler
//!   ↓
//! OrderFlowOrchestrator
//!   ├→ MatchingEngine (in-memory matching)
//!   │    └→ Orderbook (per symbol)
//!   └→ Database (async persistence)
//! ```
//!
//! # Features
//!
//! - **Concurrent Access**: Uses DashMap for lock-free orderbook access
//! - **Price-Time Priority**: Orders are matched by best price, then oldest first
//! - **Async Persistence**: Database operations are non-blocking
//! - **WebSocket Integration**: Broadcasts trade events in real-time
//!
//! # Usage
//!
//! ```rust,no_run
//! use crate::services::matching::{MatchingEngine, OrderFlowOrchestrator};
//!
//! // Create matching engine
//! let engine = Arc::new(MatchingEngine::new(vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]));
//!
//! // Create orchestrator for database integration
//! let orchestrator = OrderFlowOrchestrator::new(engine.clone(), pool);
//!
//! // Start persistence worker
//! let engine = orchestrator.start_persistence_worker();
//!
//! // Submit an order
//! let result = engine.submit_order(
//!     "BTCUSDT",
//!     "0x1234...",
//!     Side::Buy,
//!     OrderType::Limit,
//!     dec!(1.0),
//!     Some(dec!(100.0)),
//!     1,
//! )?;
//! ```

pub mod engine;
mod orderbook;
mod orchestrator;
pub mod retry;
mod types;

// Re-export main types
#[allow(unused_imports)]
pub use engine::{EngineStats, MatchingEngine, SymbolStats};
#[allow(unused_imports)]
pub use orderbook::Orderbook;
pub use orchestrator::OrderFlowOrchestrator;
pub use types::*;

// ============================================================================
// Legacy Compatibility Layer
// ============================================================================
// The following re-exports maintain backwards compatibility with existing code
// that uses the old MatchingEngine interface.

use crate::models::{Order, OrderSide as ModelOrderSide, OrderStatus as ModelOrderStatus, OrderType as ModelOrderType};
use rust_decimal::Decimal;
use sqlx::PgPool;
use std::sync::Arc;
use tokio::sync::broadcast;
use uuid::Uuid;

/// Legacy MatchingEngine wrapper for backwards compatibility
///
/// This wraps the new MatchingEngine and provides the same interface
/// as the old implementation for gradual migration.
#[allow(dead_code)]
pub struct LegacyMatchingEngine {
    inner: Arc<MatchingEngine>,
    pool: PgPool,
}

#[allow(dead_code)]
impl LegacyMatchingEngine {
    /// Create a new legacy matching engine
    pub fn new(pool: PgPool) -> Self {
        Self {
            inner: Arc::new(MatchingEngine::new(vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()])),
            pool,
        }
    }

    /// Get trade event receiver
    pub fn subscribe_trades(&self) -> broadcast::Receiver<TradeEvent> {
        self.inner.subscribe_trades()
    }

    /// Get orderbook update receiver
    pub fn subscribe_orderbook(&self) -> broadcast::Receiver<OrderbookUpdate> {
        self.inner.subscribe_orderbook()
    }

    /// Broadcast a trade event
    pub fn broadcast_trade(&self, trade: TradeEvent) -> Result<usize, broadcast::error::SendError<TradeEvent>> {
        self.inner.broadcast_trade(trade)
    }

    /// Initialize orderbook for a symbol
    pub async fn init_orderbook(&self, symbol: &str) {
        // New engine auto-initializes, but we can add symbol if needed
        if !self.inner.is_valid_symbol(symbol) {
            // Note: This requires mutable access, which is not ideal
            // In production, symbols should be configured at startup
            tracing::info!("Symbol {} not in engine, using existing symbols", symbol);
        }
    }

    /// Submit an order (legacy interface)
    pub async fn submit_order(&self, order: &Order) -> anyhow::Result<LegacyMatchResult> {
        let side = match order.side {
            ModelOrderSide::Buy => Side::Buy,
            ModelOrderSide::Sell => Side::Sell,
        };

        let order_type = match order.order_type {
            ModelOrderType::Limit => OrderType::Limit,
            ModelOrderType::Market => OrderType::Market,
            ModelOrderType::TakeProfitLimit => OrderType::TakeProfitLimit,
            ModelOrderType::StopLossLimit => OrderType::StopLossLimit,
            ModelOrderType::TakeProfitMarket => OrderType::TakeProfitMarket,
            ModelOrderType::StopLossMarket => OrderType::StopLossMarket,
        };

        // LegacyMatchingEngine is dead code (#[allow(dead_code)] above) kept
        // only for the gradual-migration shim. Resolve rates against the
        // user's current tier so that if anyone *does* re-enable this path
        // they don't silently fall back to VIP0.
        let (taker_fee_rate, maker_fee_rate) =
            crate::services::vip_tier::current_fee_rates(&self.pool, &order.user_address).await;
        let result = self.inner.submit_order(
            order.id,
            &order.symbol,
            &order.user_address,
            side,
            order_type,
            order.amount,
            order.price,
            TimeInForce::GTC,
            order.leverage as u32,
            taker_fee_rate,
            maker_fee_rate,
        ).map_err(|e| anyhow::anyhow!("{}", e))?;

        // Convert to legacy format
        let status = match result.status {
            OrderStatus::Open => ModelOrderStatus::Open,
            OrderStatus::PartiallyFilled => ModelOrderStatus::PartiallyFilled,
            OrderStatus::Filled => ModelOrderStatus::Filled,
            OrderStatus::Cancelled => ModelOrderStatus::Cancelled,
            OrderStatus::Rejected => ModelOrderStatus::Cancelled,
        };

        let trades: Vec<LegacyTradeExecution> = result.trades.iter().map(|t| LegacyTradeExecution {
            trade_id: t.trade_id,
            maker_order_id: t.maker_order_id,
            price: t.price,
            amount: t.amount,
            maker_fee: t.maker_fee,
            taker_fee: t.taker_fee,
        }).collect();

        // Persist trades to database (legacy behavior)
        self.persist_trades(order, &trades).await?;

        Ok(LegacyMatchResult {
            order_id: result.order_id,
            status,
            filled_amount: result.filled_amount,
            remaining_amount: result.remaining_amount,
            average_price: result.average_price,
            trades,
        })
    }

    /// Cancel an order
    pub async fn cancel_order(&self, symbol: &str, order_id: Uuid) -> anyhow::Result<bool> {
        let cancelled = self.inner.cancel_order(symbol, order_id, "")
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        if cancelled {
            sqlx::query("UPDATE orders SET status = 'cancelled' WHERE id = $1")
                .bind(order_id)
                .execute(&self.pool)
                .await?;
        }

        Ok(cancelled)
    }

    /// Get orderbook snapshot
    pub async fn get_orderbook(&self, symbol: &str, depth: usize) -> anyhow::Result<OrderbookSnapshot> {
        self.inner.get_orderbook(symbol, depth)
            .map_err(|e| anyhow::anyhow!("{}", e))
    }

    /// Persist trades to database (legacy)
    async fn persist_trades(&self, taker_order: &Order, trades: &[LegacyTradeExecution]) -> anyhow::Result<()> {
        if trades.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;

        for trade in trades {
            let maker_addr: Option<String> = sqlx::query_scalar(
                "SELECT user_address FROM orders WHERE id = $1"
            )
            .bind(trade.maker_order_id)
            .fetch_optional(&mut *tx)
            .await?;

            // Use WHERE NOT EXISTS instead of ON CONFLICT for TimescaleDB compatibility
            // TimescaleDB requires time column in all unique constraints
            sqlx::query(
                r#"
                INSERT INTO trades (id, symbol, maker_order_id, taker_order_id, maker_address, taker_address, side, price, amount, maker_fee, taker_fee, created_at)
                SELECT $1, $2, $3, $4, $5, $6, $7::order_side, $8, $9, $10, $11, NOW()
                WHERE NOT EXISTS (SELECT 1 FROM trades WHERE id = $1)
                "#
            )
            .bind(trade.trade_id)
            .bind(&taker_order.symbol)
            .bind(trade.maker_order_id)
            .bind(taker_order.id)
            .bind(&maker_addr.unwrap_or_default())
            .bind(&taker_order.user_address)
            .bind(taker_order.side.to_string().to_lowercase())
            .bind(trade.price)
            .bind(trade.amount)
            .bind(trade.maker_fee)
            .bind(trade.taker_fee)
            .execute(&mut *tx)
            .await?;

            sqlx::query(
                "UPDATE orders SET filled_amount = filled_amount + $1, status = CASE WHEN filled_amount + $1 >= amount THEN 'filled'::order_status ELSE 'partially_filled'::order_status END WHERE id = $2"
            )
            .bind(trade.amount)
            .bind(trade.maker_order_id)
            .execute(&mut *tx)
            .await?;
        }

        let total_filled: Decimal = trades.iter().map(|t| t.amount).sum();
        sqlx::query(
            "UPDATE orders SET filled_amount = filled_amount + $1, status = CASE WHEN filled_amount + $1 >= amount THEN 'filled'::order_status ELSE 'partially_filled'::order_status END WHERE id = $2"
        )
        .bind(total_filled)
        .bind(taker_order.id)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }
}

/// Legacy match result
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct LegacyMatchResult {
    pub order_id: Uuid,
    pub status: ModelOrderStatus,
    pub filled_amount: Decimal,
    pub remaining_amount: Decimal,
    pub average_price: Option<Decimal>,
    pub trades: Vec<LegacyTradeExecution>,
}

/// Legacy trade execution
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct LegacyTradeExecution {
    pub trade_id: Uuid,
    pub maker_order_id: Uuid,
    pub price: Decimal,
    pub amount: Decimal,
    pub maker_fee: Decimal,
    pub taker_fee: Decimal,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_price_level() {
        use rust_decimal_macros::dec;

        let price = dec!(97500.12345678);
        let level = PriceLevel::from_decimal(price);
        let back = level.to_decimal();

        // Should preserve 8 decimal places
        assert_eq!(price, back);
    }

    #[test]
    fn test_engine_basic() {
        use rust_decimal_macros::dec;
        use uuid::Uuid;

        let engine = MatchingEngine::new(vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]);

        // Submit a buy order
        // Arguments: order_id, symbol, user_address, side, order_type,
        //            amount, price, time_in_force, leverage, taker_fee_rate, maker_fee_rate
        let result = engine.submit_order(
            Uuid::new_v4(),
            "BTCUSDT",
            "0x1234",
            Side::Buy,
            OrderType::Limit,
            dec!(1.0),
            Some(dec!(100.0)),
            TimeInForce::GTC,
            1,
            Decimal::ZERO,
            Decimal::ZERO,
        );

        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.status, OrderStatus::Open);
    }
}
