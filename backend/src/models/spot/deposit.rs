use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Serialize;
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Debug, Clone, FromRow, Serialize)]
pub struct SpotDeposit {
    pub id: Uuid,
    pub user_address: String,
    pub token: String,
    pub amount: Decimal,
    pub chain_id: i64,
    pub tx_hash: String,
    pub block_number: i64,
    pub log_index: i32,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub confirmed_at: Option<DateTime<Utc>>,
}
