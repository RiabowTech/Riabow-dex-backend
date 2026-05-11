#![allow(dead_code)]
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

// Helper module to serialize DateTime as milliseconds timestamp
mod datetime_as_millis {
    use chrono::{DateTime, Utc};
    use serde::Serializer;

    pub fn serialize<S>(dt: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_i64(dt.timestamp_millis())
    }
}

// Helper module for Option<DateTime>
mod option_datetime_as_millis {
    use chrono::{DateTime, Utc};
    use serde::Serializer;

    pub fn serialize<S>(dt: &Option<DateTime<Utc>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match dt {
            Some(dt) => serializer.serialize_i64(dt.timestamp_millis()),
            None => serializer.serialize_none(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "position_side", rename_all = "lowercase")]
#[serde(rename_all = "lowercase")]
pub enum PositionSide {
    Long,
    Short,
}

impl std::fmt::Display for PositionSide {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PositionSide::Long => write!(f, "long"),
            PositionSide::Short => write!(f, "short"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "position_status", rename_all = "lowercase")]
#[serde(rename_all = "lowercase")]
pub enum PositionStatus {
    Open,
    Closed,
    Liquidated,
}

impl Default for PositionStatus {
    fn default() -> Self {
        Self::Open
    }
}

/// Enhanced Position model based on GMX V2 design
/// Key insight: Track both sizeInUsd AND sizeInTokens for accurate PnL
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Position {
    pub id: Uuid,
    pub user_address: String,
    pub symbol: String,
    pub side: PositionSide,

    // === Core Position Fields (GMX-style) ===
    /// Position size in USD (notional value at entry)
    pub size_in_usd: Decimal,
    /// Position size in tokens (locked at entry for accurate PnL)
    pub size_in_tokens: Decimal,
    /// Collateral amount deposited
    pub collateral_amount: Decimal,

    // === Entry/Exit Prices ===
    /// Average entry price
    pub entry_price: Decimal,
    /// Leverage used (1-100x)
    pub leverage: i32,
    /// Calculated liquidation price
    pub liquidation_price: Decimal,

    // === Fee Tracking (GMX-style) ===
    /// Borrowing factor for tracking accumulated borrowing fees
    pub borrowing_factor: Decimal,
    /// Funding fee amount per size for tracking funding payments
    pub funding_fee_amount_per_size: Decimal,
    /// Accumulated funding fee (positive = paid, negative = received)
    pub accumulated_funding_fee: Decimal,
    /// Accumulated borrowing fee
    pub accumulated_borrowing_fee: Decimal,
    /// Accumulated trading fee (sum of maker/taker fees from each fill;
    /// charged proportionally on close — see services/position close path).
    pub accumulated_trading_fee: Decimal,

    // === PnL Tracking ===
    /// Unrealized PnL (calculated dynamically)
    pub unrealized_pnl: Decimal,
    /// Realized PnL (from partial closes)
    pub realized_pnl: Decimal,

    // === Status ===
    pub status: PositionStatus,

    // === Timestamps ===
    #[serde(serialize_with = "datetime_as_millis::serialize")]
    pub created_at: DateTime<Utc>,
    #[serde(serialize_with = "datetime_as_millis::serialize")]
    pub updated_at: DateTime<Utc>,
    /// Last time position was increased
    #[serde(serialize_with = "option_datetime_as_millis::serialize")]
    pub increased_at: Option<DateTime<Utc>>,
    /// Last time position was decreased
    #[serde(serialize_with = "option_datetime_as_millis::serialize")]
    pub decreased_at: Option<DateTime<Utc>>,
}

/// Legacy Position struct for backward compatibility
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct PositionLegacy {
    pub id: Uuid,
    pub user_address: String,
    pub symbol: String,
    pub side: PositionSide,
    pub size: Decimal,
    pub entry_price: Decimal,
    pub leverage: i32,
    pub liquidation_price: Decimal,
    pub margin: Decimal,
    pub unrealized_pnl: Decimal,
    pub realized_pnl: Decimal,
    #[serde(serialize_with = "datetime_as_millis::serialize")]
    pub created_at: DateTime<Utc>,
    #[serde(serialize_with = "datetime_as_millis::serialize")]
    pub updated_at: DateTime<Utc>,
}

/// Enhanced PositionResponse with GMX-style fields
#[derive(Debug, Serialize, Deserialize)]
pub struct PositionResponse {
    pub position_id: Uuid,
    pub symbol: String,
    pub side: PositionSide,
    pub status: PositionStatus,

    // Size info - both naming conventions for compatibility
    /// Position size in USDT (legacy name)
    pub size_in_usd: Decimal,
    /// Position size in USDT (unified name)
    pub size: Decimal,
    /// Position size in tokens (legacy name)
    pub size_in_tokens: Decimal,
    /// Position size in tokens (unified name, e.g., BTC quantity)
    pub amount: Decimal,
    pub collateral_amount: Decimal,

    // Prices
    pub entry_price: Decimal,
    pub mark_price: Decimal,
    pub liquidation_price: Decimal,

    // Leverage & margin
    pub leverage: i32,
    pub margin_ratio: Decimal,

    // PnL
    pub unrealized_pnl: Decimal,
    pub unrealized_pnl_percent: Decimal,
    pub realized_pnl: Decimal,

    // Fees
    pub accumulated_funding_fee: Decimal,
    pub accumulated_borrowing_fee: Decimal,
    /// Accumulated trading fee (sum of maker/taker fees on each fill).
    /// Charged proportionally on close — see services/position close path.
    #[serde(default)]
    pub accumulated_trading_fee: Decimal,

    // Net value after fees
    pub net_value: Decimal,

    // Timestamps
    #[serde(serialize_with = "datetime_as_millis::serialize")]
    pub created_at: DateTime<Utc>,
    #[serde(serialize_with = "datetime_as_millis::serialize")]
    pub updated_at: DateTime<Utc>,
}

/// Request to open a new position or increase existing
#[derive(Debug, Serialize, Deserialize)]
pub struct OpenPositionRequest {
    pub symbol: String,
    pub side: PositionSide,
    pub collateral_amount: Decimal,
    pub leverage: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<u64>,
}

/// Request to close or decrease a position
#[derive(Debug, Serialize, Deserialize)]
pub struct ClosePositionRequest {
    /// Size to close in USD (required). Use position's size_in_usd for full close.
    pub size: Decimal,
    /// Limit price (None = market order)
    pub price: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<u64>,
}

/// Request to add collateral to position
#[derive(Debug, Serialize, Deserialize)]
pub struct AddCollateralRequest {
    pub position_id: Uuid,
    pub amount: Decimal,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<u64>,
}

/// Request to remove collateral from position
#[derive(Debug, Serialize, Deserialize)]
pub struct RemoveCollateralRequest {
    pub position_id: Uuid,
    pub amount: Decimal,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<u64>,
}

/// Result of a liquidation check
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiquidationInfo {
    pub is_liquidatable: bool,
    pub reason: Option<String>,
    pub remaining_collateral_usd: Decimal,
    pub min_collateral_usd: Decimal,
    pub min_collateral_for_leverage: Decimal,
}

/// Position increase result
#[derive(Debug, Serialize, Deserialize)]
pub struct IncreasePositionResult {
    pub position: PositionResponse,
    pub execution_price: Decimal,
    pub size_delta_usd: Decimal,
    pub size_delta_tokens: Decimal,
    pub collateral_delta: Decimal,
    pub fees_paid: Decimal,
}

/// Position decrease result
#[derive(Debug, Serialize, Deserialize)]
pub struct DecreasePositionResult {
    pub position: Option<PositionResponse>,
    pub execution_price: Decimal,
    pub size_delta_usd: Decimal,
    pub pnl_realized: Decimal,
    pub collateral_returned: Decimal,
    pub fees_paid: Decimal,
    pub is_fully_closed: bool,
}

/// Configuration for position management
#[derive(Debug, Clone)]
pub struct PositionConfig {
    /// Minimum collateral in USD
    pub min_collateral_usd: Decimal,
    /// Deprecated: replaced by per-market `market_configs.min_order_size_usd`.
    /// User-close handler (api/handlers/position.rs) reads from MarketConfigService
    /// directly. PositionService internal call sites still read this as a fallback
    /// because PositionService doesn't hold a MarketConfigService reference; remove
    /// once that dep is plumbed through. Spec §2.1.
    #[deprecated(note = "use market_configs.min_order_size_usd via MarketConfigService")]
    pub min_position_size_usd: Decimal,
    /// Maximum leverage allowed
    pub max_leverage: i32,
    /// Maintenance margin rate (e.g., 0.005 = 0.5%)
    pub maintenance_margin_rate: Decimal,
    /// Position fee rate (e.g., 0.001 = 0.1%)
    pub position_fee_rate: Decimal,
    /// Borrowing fee rate per hour
    pub borrowing_fee_rate_per_hour: Decimal,
}

impl Default for PositionConfig {
    #[allow(deprecated)] // PositionConfig.min_position_size_usd kept for one cycle; spec §2.1
    fn default() -> Self {
        Self {
            min_collateral_usd: Decimal::new(10, 0),       // $10
            min_position_size_usd: Decimal::new(100, 0),   // $100
            max_leverage: 100,
            maintenance_margin_rate: Decimal::new(5, 3),   // 0.5%
            position_fee_rate: Decimal::new(1, 3),         // 0.1%
            borrowing_fee_rate_per_hour: Decimal::new(1, 5), // 0.001% per hour
        }
    }
}
