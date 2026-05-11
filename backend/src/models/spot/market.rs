use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use chrono::{DateTime, Utc};

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct SpotMarket {
    pub id: String,
    pub base_token: String,
    pub quote_token: String,
    pub tick_size: Decimal,
    pub lot_size: Decimal,
    pub min_notional: Decimal,
    pub maker_fee_bps: i32,
    pub taker_fee_bps: i32,
    pub status: String,
    /// Curated human-friendly name. NULL falls back to `BASE / QUOTE`.
    pub display_name: Option<String>,
    /// Free-form market description (NULL until the team fills it in).
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketStatus { Listed, Halted, Delisted }

impl SpotMarket {
    pub fn parsed_status(&self) -> Option<MarketStatus> {
        match self.status.as_str() {
            "listed" => Some(MarketStatus::Listed),
            "halted" => Some(MarketStatus::Halted),
            "delisted" => Some(MarketStatus::Delisted),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parsed_status_round_trips() {
        let m = SpotMarket {
            id: "DFUSDT".into(), base_token: "DF".into(), quote_token: "USDT".into(),
            tick_size: Decimal::new(1, 4), lot_size: Decimal::new(1, 2),
            min_notional: Decimal::new(1, 0), maker_fee_bps: 0, taker_fee_bps: 0,
            status: "listed".into(), display_name: None, description: None,
            created_at: Utc::now(), updated_at: Utc::now(),
        };
        assert_eq!(m.parsed_status(), Some(MarketStatus::Listed));
    }
}
