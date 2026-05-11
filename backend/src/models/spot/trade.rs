use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use chrono::{DateTime, Utc};
use uuid::Uuid;

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct SpotTrade {
    pub id: Uuid,
    pub market_id: String,
    pub maker_order_id: Uuid,
    pub taker_order_id: Uuid,
    pub maker_user: String,
    pub taker_user: String,
    pub side: String,
    pub price: Decimal,
    pub quantity: Decimal,
    pub maker_fee: Decimal,
    pub taker_fee: Decimal,
    pub created_at: DateTime<Utc>,
}
