//! WebSocket Channel Definitions

#![allow(dead_code)]
use serde::{Deserialize, Serialize};
use rust_decimal::Decimal;

/// Orderbook update message
#[derive(Debug, Serialize, Deserialize)]
pub struct OrderbookUpdate {
    pub symbol: String,
    pub bids: Vec<[Decimal; 2]>,
    pub asks: Vec<[Decimal; 2]>,
    pub timestamp: i64,
}

/// Trade update message
#[derive(Debug, Serialize, Deserialize)]
pub struct TradeUpdate {
    pub id: String,
    pub symbol: String,
    pub price: Decimal,
    pub amount: Decimal,
    pub side: String,
    pub timestamp: i64,
}

/// Position update message (private)
#[derive(Debug, Serialize, Deserialize)]
pub struct PositionUpdate {
    pub position_id: String,
    pub symbol: String,
    pub side: String,
    pub size: Decimal,
    pub entry_price: Decimal,
    pub mark_price: Decimal,
    pub unrealized_pnl: Decimal,
    pub unrealized_pnl_percent: Decimal,
    pub liquidation_price: Decimal,
}

/// Order update message (private)
#[derive(Debug, Serialize, Deserialize)]
pub struct OrderUpdate {
    pub order_id: String,
    pub symbol: String,
    pub side: String,
    pub order_type: String,
    pub price: Option<Decimal>,
    pub amount: Decimal,
    pub filled_amount: Decimal,
    pub status: String,
    pub timestamp: i64,
}

/// Balance update message (private)
#[derive(Debug, Serialize, Deserialize)]
pub struct BalanceUpdate {
    pub token: String,
    pub available: Decimal,
    pub frozen: Decimal,
}

/// Ticker update message
#[derive(Debug, Serialize, Deserialize)]
pub struct TickerUpdate {
    pub symbol: String,
    pub last_price: Decimal,
    pub price_change_24h: Decimal,
    pub price_change_percent_24h: Decimal,
    pub high_24h: Decimal,
    pub low_24h: Decimal,
    pub volume_24h: Decimal,
}

/// Channel types
pub enum Channel {
    Orderbook(String),    // orderbook.{symbol}
    Trades(String),       // trades.{symbol}
    Ticker(String),       // ticker.{symbol}
    Kline(String, String), // kline:{symbol}:{period}
    Positions,            // positions (private)
    Orders,               // orders (private)
    Balances,             // balances (private)
}

impl Channel {
    pub fn parse(channel_str: &str) -> Option<Self> {
        // Handle colon-separated format (kline:BTCUSDT:5m)
        if channel_str.starts_with("kline:") {
            let parts: Vec<&str> = channel_str.strip_prefix("kline:").unwrap().split(':').collect();
            if parts.len() == 2 {
                return Some(Channel::Kline(parts[0].to_string(), parts[1].to_string()));
            }
            return None;
        }

        // Handle colon-separated format for ticker (ticker:BTCUSDT)
        if channel_str.starts_with("ticker:") {
            if let Some(symbol) = channel_str.strip_prefix("ticker:") {
                return Some(Channel::Ticker(symbol.to_string()));
            }
            return None;
        }

        // Handle dot-separated format (orderbook.BTCUSDT, trades.BTCUSDT, ticker.BTCUSDT)
        let parts: Vec<&str> = channel_str.split('.').collect();

        match parts.as_slice() {
            ["orderbook", symbol] => Some(Channel::Orderbook(symbol.to_string())),
            ["trades", symbol] => Some(Channel::Trades(symbol.to_string())),
            ["ticker", symbol] => Some(Channel::Ticker(symbol.to_string())),
            ["positions"] => Some(Channel::Positions),
            ["orders"] => Some(Channel::Orders),
            ["balances"] => Some(Channel::Balances),
            _ => None,
        }
    }

    pub fn is_private(&self) -> bool {
        matches!(
            self,
            Channel::Positions | Channel::Orders | Channel::Balances
        )
    }
}

/// K-line update message
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct KlineUpdate {
    pub symbol: String,
    pub period: String,
    pub time: i64,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
    pub is_final: bool,
}
