#![allow(dead_code)]
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct ReferralCode {
    pub id: Uuid,
    pub owner_address: String,
    pub code: String,
    pub total_referrals: i64,
    pub total_earnings: Decimal,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct ReferralRelation {
    pub id: Uuid,
    pub referee_address: String,
    pub referrer_address: String,
    pub code: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct ReferralEarning {
    pub id: Uuid,
    pub referrer_address: String,
    pub referee_address: String,
    pub token: String,
    pub amount: Decimal,
    pub trade_id: Uuid,
    pub claimed: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReferralDashboard {
    pub code: Option<String>,
    pub total_referrals: i64,
    pub total_earnings: Decimal,
    pub pending_earnings: Decimal,
    pub recent_activity: Vec<ReferralActivity>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReferralActivity {
    pub referee_address: String,
    pub amount: Decimal,
    pub token: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateReferralCodeRequest {
    pub signature: String,
    pub timestamp: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BindReferralRequest {
    pub code: String,
    pub signature: String,
    pub timestamp: u64,
}
