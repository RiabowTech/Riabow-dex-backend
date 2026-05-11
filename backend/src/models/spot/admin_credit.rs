use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use chrono::{DateTime, Utc};
use uuid::Uuid;

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct SpotAdminCredit {
    pub id: Uuid,
    pub user_address: String,
    pub token: String,
    pub amount: Decimal,
    pub admin_actor: String,
    pub reason: Option<String>,
    pub created_at: DateTime<Utc>,
}
