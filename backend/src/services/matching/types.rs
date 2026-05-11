//! Matching Engine Types
//!
//! Shared types and DTOs for the matching engine.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use uuid::Uuid;

// ============================================================================
// Price Level
// ============================================================================

/// Price level with 8 decimal precision for exact comparison
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PriceLevel(i64);

impl PriceLevel {
    /// Create a PriceLevel from a Decimal price
    pub fn from_decimal(price: Decimal) -> Self {
        let scaled = price * Decimal::from(100_000_000);
        let truncated = scaled.trunc();
        let value = truncated.mantissa() / 10i128.pow(truncated.scale() as u32);
        PriceLevel(value as i64)
    }

    /// Convert back to Decimal
    pub fn to_decimal(&self) -> Decimal {
        Decimal::from(self.0) / Decimal::from(100_000_000)
    }

    /// Get raw value
    #[allow(dead_code)]
    pub fn raw(&self) -> i64 {
        self.0
    }
}

impl Ord for PriceLevel {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl PartialOrd for PriceLevel {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// ============================================================================
// Order Types
// ============================================================================

/// Order side
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Buy,
    Sell,
}

impl std::fmt::Display for Side {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Side::Buy => write!(f, "buy"),
            Side::Sell => write!(f, "sell"),
        }
    }
}

/// Order type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderType {
    Limit,
    Market,
    TakeProfitLimit,
    StopLossLimit,
    TakeProfitMarket,
    StopLossMarket,
}

/// Time in force
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum TimeInForce {
    /// Good Till Cancel
    GTC,
    /// Immediate or Cancel
    IOC,
    /// Fill or Kill
    FOK,
}

impl Default for TimeInForce {
    fn default() -> Self {
        TimeInForce::GTC
    }
}

/// Order status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OrderStatus {
    /// Order is active in the orderbook
    Open,
    /// Order is partially filled
    PartiallyFilled,
    /// Order is completely filled
    Filled,
    /// Order was cancelled
    Cancelled,
    /// Order was rejected
    Rejected,
}

impl std::fmt::Display for OrderStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OrderStatus::Open => write!(f, "open"),
            OrderStatus::PartiallyFilled => write!(f, "partially_filled"),
            OrderStatus::Filled => write!(f, "filled"),
            OrderStatus::Cancelled => write!(f, "cancelled"),
            OrderStatus::Rejected => write!(f, "rejected"),
        }
    }
}

// ============================================================================
// Order Entry (in orderbook)
// ============================================================================

/// An order entry in the orderbook
#[derive(Debug, Clone)]
pub struct OrderEntry {
    pub id: Uuid,
    pub user_address: String,
    pub price: Decimal,
    pub original_amount: Decimal,
    pub remaining_amount: Decimal,
    pub side: Side,
    pub time_in_force: TimeInForce,
    pub timestamp: i64,
    pub leverage: u32, // Added leverage
    /// Maker fee rate for this user, locked at the moment the order was
    /// placed (already includes referral / staking discount via
    /// `fee_tiers::discount_multiplier`). When this entry rests in the book
    /// and a taker hits it, `match_order` charges
    /// `trade_value * maker_fee_rate` as the maker_fee column.
    ///
    /// Locking at placement (rather than resolving the maker's tier per
    /// fill) keeps `match_order` synchronous and avoids a per-fill DB hit
    /// on a hot path. Recovery from DB on restart re-resolves this field
    /// from each maker's *current* tier; rates do not survive a restart.
    pub maker_fee_rate: Decimal,
}

// ============================================================================
// Trade Execution
// ============================================================================

/// A trade execution result
#[derive(Debug, Clone, Serialize)]
pub struct TradeExecution {
    pub trade_id: Uuid,
    pub maker_order_id: Uuid,
    pub taker_order_id: Uuid,
    pub maker_address: String,
    pub price: Decimal,
    pub amount: Decimal,
    pub maker_fee: Decimal,
    pub taker_fee: Decimal,
    pub timestamp: i64,
    pub maker_leverage: u32, // Added maker leverage
    pub taker_leverage: u32, // Added taker leverage
}

/// Trade event for broadcasting
#[derive(Debug, Clone, Serialize)]
pub struct TradeEvent {
    pub symbol: String,
    pub trade_id: Uuid,
    pub maker_order_id: Uuid,
    pub taker_order_id: Uuid,
    pub maker_address: String,
    pub taker_address: String,
    pub side: String,
    pub price: Decimal,
    pub amount: Decimal,
    pub maker_fee: Decimal,
    pub taker_fee: Decimal,
    pub timestamp: i64,
    pub maker_leverage: u32, // Added maker leverage
    pub taker_leverage: u32, // Added taker leverage
    /// Self-trade detection (PRD §3.2): true when maker_address ==
    /// taker_address. Downstream points calculation skips TP/PP/RP for
    /// self-trades to prevent points farming.
    #[serde(default)]
    pub is_self_trade: bool,
}

// ============================================================================
// Match Result
// ============================================================================

/// Result of order matching
#[derive(Debug, Clone)]
pub struct MatchResult {
    pub order_id: Uuid,
    pub status: OrderStatus,
    pub filled_amount: Decimal,
    pub remaining_amount: Decimal,
    pub average_price: Option<Decimal>,
    pub trades: Vec<TradeExecution>,
}

// ============================================================================
// Orderbook Snapshot
// ============================================================================

/// Orderbook snapshot for API response
#[derive(Debug, Clone, Serialize)]
pub struct OrderbookSnapshot {
    pub symbol: String,
    pub bids: Vec<[String; 2]>,
    pub asks: Vec<[String; 2]>,
    pub last_price: Option<Decimal>,
    pub timestamp: i64,
}

/// Orderbook update event for broadcasting
#[derive(Debug, Clone, Serialize)]
pub struct OrderbookUpdate {
    pub symbol: String,
    pub bids: Vec<[String; 2]>,
    pub asks: Vec<[String; 2]>,
    pub timestamp: i64,
}

// ============================================================================
// Error Types
// ============================================================================

/// Matching engine errors
#[derive(Debug, thiserror::Error)]
pub enum MatchingError {
    #[error("Symbol not found: {0}")]
    SymbolNotFound(String),

    #[error("Order not found: {0}")]
    OrderNotFound(String),

    #[error("Invalid price: {0}")]
    InvalidPrice(String),

    #[error("Invalid amount: {0}")]
    InvalidAmount(String),

    #[error("Invalid side: {0}")]
    InvalidSide(String),

    #[error("Insufficient liquidity")]
    InsufficientLiquidity,

    #[error("Database error: {0}")]
    DatabaseError(String),

    #[error("Internal error: {0}")]
    InternalError(String),
}

// ============================================================================
// Fee Configuration
// ============================================================================
//
// `FeeConfig` (a single global maker/taker rate held by the engine) was
// removed when per-user VIP-tier rates became authoritative — see
// `submit_order`'s `taker_fee_rate` / `maker_fee_rate` parameters and
// `OrderEntry::maker_fee_rate`. The previous default (0.02% maker /
// 0.05% taker) didn't even match VIP0 and overcharged every user
// regardless of tier. Callers must resolve VIP via `vip_tier::resolve`
// (interactive paths) or `vip_tier::current_fee_rates` (background paths)
// and pass the resulting rates into `submit_order`.

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_price_level_conversion() {
        let price = dec!(97500.50);
        let level = PriceLevel::from_decimal(price);
        let back = level.to_decimal();
        assert_eq!(price, back);
    }

    #[test]
    fn test_price_level_ordering() {
        let p1 = PriceLevel::from_decimal(dec!(100.0));
        let p2 = PriceLevel::from_decimal(dec!(200.0));
        assert!(p1 < p2);
    }

}
