use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use chrono::{DateTime, Utc};

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct SpotTicker24h {
    pub market_id: String,
    pub last_price: Decimal,
    pub open_price_24h: Decimal,
    pub high_24h: Decimal,
    pub low_24h: Decimal,
    pub volume_24h: Decimal,
    pub quote_volume_24h: Decimal,
    pub trade_count_24h: i64,
    pub updated_at: DateTime<Utc>,
}
