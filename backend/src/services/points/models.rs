use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PointType {
    Trading,
    Pnl,
    Holding,
    Referral,
    Staking,
}

impl ToString for PointType {
    fn to_string(&self) -> String {
        match self {
            PointType::Trading => "trading".to_string(),
            PointType::Pnl => "pnl".to_string(),
            PointType::Holding => "holding".to_string(),
            PointType::Referral => "referral".to_string(),
            PointType::Staking => "staking".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct PointsEpoch {
    pub id: Uuid,
    pub epoch_number: i32,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub duration_days: i32,
    pub status: String,
    pub config: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct UserPointsSummary {
    pub id: Uuid,
    pub user_address: String,
    pub epoch_number: i32,
    pub trading_points: BigDecimal,
    pub pnl_points: BigDecimal,
    pub holding_points: BigDecimal,
    pub referral_points: BigDecimal,
    pub staking_points: BigDecimal,
    pub total_points: BigDecimal,
    pub trading_volume: BigDecimal,
    pub trade_count: i32,
    pub realized_pnl: BigDecimal,
    pub tier: Option<String>,
    pub tier_multiplier: Option<BigDecimal>,
    pub referral_count: i32,
    pub referral_volume: BigDecimal,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct PointsEvent {
    pub id: Uuid,
    pub user_address: String,
    pub epoch_number: i32,
    pub point_type: String,
    pub points: BigDecimal,
    pub related_trade_id: Option<Uuid>,
    pub related_order_id: Option<Uuid>,
    pub related_position_id: Option<Uuid>,
    pub referrer_address: Option<String>,
    pub metadata: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct TradingTierConfig {
    pub id: Uuid,
    pub tier_name: String,
    pub min_volume: BigDecimal,
    pub max_volume: Option<BigDecimal>,
    pub multiplier: BigDecimal,
    pub epoch_number: Option<i32>,
    pub is_active: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct PointsStaking {
    pub id: Uuid,
    pub user_address: String,
    pub amount: BigDecimal,
    pub token_address: String,
    pub start_time: DateTime<Utc>,
    pub end_time: Option<DateTime<Utc>>,
    pub status: String,
    pub tx_hash: Option<String>,
    pub withdraw_tx_hash: Option<String>,
    pub last_calculated_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct PointsLeaderboardEntry {
    pub id: Uuid,
    pub epoch_number: i32,
    pub rank_type: String,
    pub user_address: String,
    pub rank: i32,
    pub points: BigDecimal,
    pub username: Option<String>,
    pub tier: Option<String>,
    pub updated_at: DateTime<Utc>,
}
