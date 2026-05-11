#![allow(dead_code)]
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "withdrawal_status", rename_all = "lowercase")]
#[serde(rename_all = "lowercase")]
pub enum WithdrawalStatus {
    Pending,
    Signed,
    Submitted,
    Confirmed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Withdrawal {
    pub id: Uuid,
    pub user_address: String,
    pub token: String,
    pub amount: Decimal,
    pub to_address: String,
    pub nonce: i64,
    pub expiry: i64,
    pub backend_signature: Option<String>,
    pub tx_hash: Option<String>,
    pub status: WithdrawalStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WithdrawRequest {
    pub token: String,
    pub amount: Decimal,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConfirmWithdrawRequest {
    pub tx_hash: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WithdrawResponse {
    pub withdraw_id: Uuid,
    pub backend_signature: String,
    pub nonce: i64,
    pub expiry: i64,
    pub contract_address: String,
}
