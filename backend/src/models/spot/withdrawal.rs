use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Serialize;
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Debug, Clone, FromRow, Serialize)]
pub struct SpotWithdrawal {
    pub id: Uuid,
    pub user_address: String,
    pub token: String,
    pub amount: Decimal,
    pub fee: Decimal,
    pub chain_id: i64,
    pub nonce: i64,
    pub signature: String,
    pub deadline: DateTime<Utc>,
    pub status: String,
    pub tx_hash: Option<String>,
    pub block_number: Option<i64>,
    pub requested_at: DateTime<Utc>,
    pub confirmed_at: Option<DateTime<Utc>>,
}
