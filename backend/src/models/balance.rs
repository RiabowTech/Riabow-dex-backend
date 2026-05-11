#![allow(dead_code)]
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Balance {
    pub id: Uuid,
    pub user_address: String,
    pub token: String,
    pub available: Decimal,
    pub frozen: Decimal,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BalanceResponse {
    pub token: String,
    pub available: Decimal,
    pub frozen: Decimal,
    pub total: Decimal,
}

impl From<Balance> for BalanceResponse {
    fn from(balance: Balance) -> Self {
        Self {
            token: balance.token,
            available: balance.available.clone(),
            frozen: balance.frozen.clone(),
            total: balance.available + balance.frozen,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Deposit {
    pub id: Uuid,
    pub user_address: String,
    pub token: String,
    pub amount: Decimal,
    pub tx_hash: String,
    pub block_number: i64,
    pub status: String,
    pub created_at: DateTime<Utc>,
}
