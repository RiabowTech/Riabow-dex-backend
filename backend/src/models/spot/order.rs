use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use chrono::{DateTime, Utc};
use uuid::Uuid;

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct SpotOrder {
    pub id: Uuid,
    pub user_address: String,
    pub market_id: String,
    pub side: String,
    pub r#type: String,
    pub tif: String,
    pub price: Option<Decimal>,
    pub quantity: Option<Decimal>,
    pub quote_quantity: Option<Decimal>,
    pub filled_qty: Decimal,
    pub avg_fill_price: Decimal,
    pub status: String,
    pub reject_reason: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side { Buy, Sell }
impl Side {
    pub fn as_str(self) -> &'static str { match self { Self::Buy => "buy", Self::Sell => "sell" } }
    pub fn parse(s: &str) -> Option<Self> { match s { "buy" => Some(Self::Buy), "sell" => Some(Self::Sell), _ => None } }
    pub fn opposite(self) -> Self { match self { Self::Buy => Self::Sell, Self::Sell => Self::Buy } }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderType { Limit, Market }
impl OrderType {
    pub fn as_str(self) -> &'static str { match self { Self::Limit => "limit", Self::Market => "market" } }
    pub fn parse(s: &str) -> Option<Self> { match s { "limit" => Some(Self::Limit), "market" => Some(Self::Market), _ => None } }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tif { Gtc, Ioc, PostOnly }
impl Tif {
    pub fn as_str(self) -> &'static str {
        match self { Self::Gtc => "gtc", Self::Ioc => "ioc", Self::PostOnly => "post_only" }
    }
    pub fn parse(s: &str) -> Option<Self> {
        match s { "gtc" => Some(Self::Gtc), "ioc" => Some(Self::Ioc), "post_only" => Some(Self::PostOnly), _ => None }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderStatus { Open, PartiallyFilled, Filled, Canceled, Rejected, Expired }
impl OrderStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open", Self::PartiallyFilled => "partially_filled",
            Self::Filled => "filled", Self::Canceled => "canceled",
            Self::Rejected => "rejected", Self::Expired => "expired",
        }
    }
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Filled | Self::Canceled | Self::Rejected | Self::Expired)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test] fn side_round_trip() {
        for s in ["buy","sell"] { assert_eq!(Side::parse(s).unwrap().as_str(), s); }
        assert_eq!(Side::Buy.opposite(), Side::Sell);
    }
    #[test] fn tif_round_trip() {
        for s in ["gtc","ioc","post_only"] { assert_eq!(Tif::parse(s).unwrap().as_str(), s); }
    }
    #[test] fn status_terminal() {
        assert!(OrderStatus::Filled.is_terminal());
        assert!(!OrderStatus::Open.is_terminal());
    }
}
