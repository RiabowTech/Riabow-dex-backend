//! Wire-format structs for the spot WebSocket channels. Serializing these
//! produces the JSON described in the spot-trading-websocket-design spec §4.

use serde::Serialize;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize)]
pub struct SpotDepthSnapshot {
    pub symbol: String,
    pub last_update_id: u64,
    pub bids: Vec<[String; 2]>,
    pub asks: Vec<[String; 2]>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SpotDepthDiff {
    pub symbol: String,
    pub update_id_first: u64,
    pub update_id_last: u64,
    pub bids: Vec<[String; 2]>,
    pub asks: Vec<[String; 2]>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SpotTradePush {
    pub trade_id: Uuid,
    pub symbol: String,
    pub side: String,         // taker side
    pub price: String,
    pub quantity: String,
    pub ts: i64,              // unix millis
}

#[derive(Debug, Clone, Serialize)]
pub struct SpotTickerPush {
    pub symbol: String,
    pub last_price: String,
    pub open_price: String,
    pub high: String,
    pub low: String,
    pub volume: String,
    pub quote_volume: String,
    pub trade_count: i64,
    pub open_time: i64,
    pub close_time: i64,
    pub ts: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct SpotKlinePush {
    pub symbol: String,
    pub interval: String,
    pub open_time: i64,
    pub close_time: i64,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub volume: String,
    pub quote_volume: String,
    pub trade_count: i64,
    pub is_closed: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct SpotUserOrderPush {
    /// User address — the WS handler filters per-connection by this field.
    /// Lowercased; use eq_ignore_ascii_case at the handler.
    #[serde(skip)]
    pub user_address: String,
    pub id: Uuid,
    pub symbol: String,
    pub side: String,
    pub r#type: String,
    pub tif: String,
    pub price: Option<String>,
    pub quantity: Option<String>,
    pub quote_quantity: Option<String>,
    pub filled_qty: String,
    pub avg_fill_price: String,
    pub status: String,
    pub reject_reason: Option<String>,
    pub updated_at: i64,
    /// Present when the change was caused by a fill; omitted on plain place / cancel / reject.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_fill: Option<SpotUserOrderFill>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SpotUserOrderFill {
    pub trade_id: Uuid,
    pub price: String,
    pub quantity: String,
    pub fee: String,
    pub fee_token: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SpotUserBalancePush {
    /// Same skip-and-filter pattern as SpotUserOrderPush.
    #[serde(skip)]
    pub user_address: String,
    pub token: String,
    pub available: String,
    pub frozen: String,
    pub ts: i64,
}

