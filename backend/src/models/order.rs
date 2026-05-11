use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use std::fmt;
use uuid::Uuid;

// Helper module to serialize DateTime as milliseconds timestamp
mod datetime_as_millis {
    use chrono::{DateTime, Utc};
    use serde::{Serializer};

    pub fn serialize<S>(dt: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_i64(dt.timestamp_millis())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "order_side", rename_all = "lowercase")]
#[serde(rename_all = "lowercase")]
pub enum OrderSide {
    Buy,
    Sell,
}

impl fmt::Display for OrderSide {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OrderSide::Buy => write!(f, "buy"),
            OrderSide::Sell => write!(f, "sell"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "order_type", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum OrderType {
    Limit,
    Market,
    TakeProfitLimit,
    StopLossLimit,
    TakeProfitMarket,
    StopLossMarket,
}

impl fmt::Display for OrderType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OrderType::Limit => write!(f, "limit"),
            OrderType::Market => write!(f, "market"),
            OrderType::TakeProfitLimit => write!(f, "take_profit_limit"),
            OrderType::StopLossLimit => write!(f, "stop_loss_limit"),
            OrderType::TakeProfitMarket => write!(f, "take_profit_market"),
            OrderType::StopLossMarket => write!(f, "stop_loss_market"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "order_status", rename_all = "lowercase")]
#[serde(rename_all = "lowercase")]
pub enum OrderStatus {
    Pending,
    Open,
    PartiallyFilled,
    Filled,
    Cancelled,
    Rejected,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Order {
    pub id: Uuid,
    pub user_address: String,
    pub symbol: String,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub price: Option<Decimal>,
    pub amount: Decimal,
    pub filled_amount: Decimal,
    pub leverage: i32,
    pub status: OrderStatus,
    pub signature: String,
    #[serde(serialize_with = "datetime_as_millis::serialize")]
    pub created_at: DateTime<Utc>,
    #[serde(serialize_with = "datetime_as_millis::serialize")]
    pub updated_at: DateTime<Utc>,
    /// Frozen margin amount for this order (used to track margin to release on cancel)
    pub frozen_margin: Option<Decimal>,
    pub reduce_only: bool,
    pub trigger_price: Option<Decimal>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateOrderRequest {
    pub symbol: String,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub price: Option<Decimal>,
    pub amount: Decimal,
    pub leverage: i32,
    /// EIP-712 signature — required for JWT (wallet) auth, optional for API Key auth
    #[serde(default)]
    pub signature: Option<String>,
    /// Unix timestamp — required for JWT (wallet) auth, optional for API Key auth
    #[serde(default)]
    pub timestamp: Option<u64>,
    #[serde(default)]
    pub reduce_only: bool,
    /// Take-profit price — if set, auto-creates a TP trigger order after fill
    #[serde(default)]
    pub tp_price: Option<Decimal>,
    /// Stop-loss price — if set, auto-creates a SL trigger order after fill
    #[serde(default)]
    pub sl_price: Option<Decimal>,
    /// Max slippage tolerance (e.g., 0.01 = 1%). Market orders only.
    #[serde(default)]
    pub max_slippage: Option<Decimal>,
    /// Trigger price for TP/SL order types
    #[serde(default)]
    pub trigger_price: Option<Decimal>,
}

/// Request to modify an existing order's price and/or amount
#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateOrderRequest {
    pub price: Option<String>,
    pub amount: Option<String>,
    pub size: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderResponse {
    pub order_id: Uuid,
    pub symbol: String,
    pub side: OrderSide,
    pub order_type: OrderType,
    /// Order price (for limit orders) or execution price (for market orders)
    pub price: Decimal,
    /// Order size in USDT (= amount * price)
    pub size: Decimal,
    /// Order amount in tokens (e.g., BTC quantity)
    pub amount: Decimal,
    pub filled_amount: Decimal,
    pub remaining_amount: Decimal,
    pub leverage: i32,
    pub status: OrderStatus,
    #[serde(serialize_with = "datetime_as_millis::serialize")]
    pub created_at: DateTime<Utc>,
    pub reduce_only: bool,
    pub trigger_price: Option<Decimal>,
}

impl From<Order> for OrderResponse {
    fn from(order: Order) -> Self {
        let price = order.price.unwrap_or(Decimal::ZERO);
        let size = order.amount * price;
        Self {
            order_id: order.id,
            symbol: order.symbol.clone(),
            side: order.side,
            order_type: order.order_type,
            price,
            size,
            amount: order.amount,
            filled_amount: order.filled_amount,
            remaining_amount: order.amount - order.filled_amount,
            leverage: order.leverage,
            status: order.status,
            created_at: order.created_at,
            reduce_only: order.reduce_only,
            trigger_price: order.trigger_price,
        }
    }
}
