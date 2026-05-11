//! WebSocket Handler
//!
//! Phase 11: Complete WebSocket with proper authentication and real-time updates

use axum::extract::ws::{Message, WebSocket};
use futures::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, Semaphore};
use uuid::Uuid;

use crate::auth::eip712::{verify_ws_auth_signature, WebSocketAuthMessage};
use crate::auth::jwt::validate_token;
use crate::services::kline::KlinePeriod;
use crate::AppState;

/// Normalize symbol format to backend format (BTCUSDT)
/// Supports multiple input formats:
/// - "BTCUSDT" -> "BTCUSDT" (already correct)
/// - "BTC-USD" -> "BTCUSDT" (frontend TradingView format)
/// - "BTC-USDT" -> "BTCUSDT"
/// - "btcusdt" -> "BTCUSDT" (lowercase)
fn normalize_symbol(symbol: &str) -> String {
    let upper = symbol.to_uppercase();
    
    // If already in BTCUSDT format (no separators), return as is
    if !upper.contains('-') && !upper.contains('/') && !upper.contains('_') {
        return upper;
    }
    
    // Handle BTC-USD format (convert to BTCUSDT)
    if upper.ends_with("-USD") {
        let base = upper.strip_suffix("-USD").unwrap_or(&upper);
        return format!("{}USDT", base);
    }
    
    // Handle BTC-USDT format (convert to BTCUSDT)
    if upper.contains("-USDT") {
        return upper.replace("-", "");
    }
    
    // Handle BTC/USD or BTC_USD formats
    if upper.contains("/") || upper.contains("_") {
        let cleaned = upper.replace("/", "").replace("_", "");
        if !cleaned.ends_with("USDT") && cleaned.ends_with("USD") {
            let base = cleaned.strip_suffix("USD").unwrap_or(&cleaned);
            return format!("{}USDT", base);
        }
        return cleaned;
    }
    
    // Default: return uppercase version
    upper
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMessage {
    /// Authenticate with wallet signature, JWT token, or fapi listenKey
    Auth {
        #[serde(default)]
        address: Option<String>,
        #[serde(default)]
        signature: Option<String>,
        #[serde(default)]
        timestamp: Option<u64>,
        #[serde(default)]
        token: Option<String>,
        /// Binance-style listenKey, issued by `POST /fapi/v1/listenKey`
        /// (HMAC-authenticated). Mutually exclusive with `token` /
        /// signature fields. Looked up against Redis to resolve the
        /// owning user_address; lookup also implicitly extends the TTL
        /// so a long-running WS connection acts as keepalive.
        #[serde(default, rename = "listenKey")]
        listen_key: Option<String>,
    },
    /// Authenticate with JWT token (alternative to signature auth)
    AuthToken {
        token: String,
    },
    Subscribe {
        channel: String,
        #[serde(default)]
        token: Option<String>,
    },
    Unsubscribe {
        channel: String,
    },
    Ping,
    /// Internal trade (development only)
    InternalTrade {
        symbol: String,
        #[serde(deserialize_with = "deserialize_string_or_number")]
        price: String,
        #[serde(deserialize_with = "deserialize_string_or_number")]
        amount: String,
        side: String,
        #[serde(default)]
        timestamp: Option<i64>,
    },
    /// Batch internal trades (development only)
    InternalTradeBatch {
        trades: Vec<InternalTradeData>,
    },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct InternalTradeData {
    pub symbol: String,
    #[serde(deserialize_with = "deserialize_string_or_number")]
    pub price: String,
    #[serde(deserialize_with = "deserialize_string_or_number")]
    pub amount: String,
    pub side: String,
    #[serde(default)]
    pub timestamp: Option<i64>,
}

// Helper function to deserialize string or number
fn deserialize_string_or_number<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;
    use serde_json::Value;

    let value = Value::deserialize(deserializer)?;
    match value {
        Value::String(s) => Ok(s),
        Value::Number(n) => Ok(n.to_string()),
        _ => Err(Error::custom("expected string or number")),
    }
}

#[derive(Debug, Serialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMessage {
    AuthResult {
        success: bool,
        message: Option<String>,
    },
    Subscribed {
        channel: String,
    },
    Unsubscribed {
        channel: String,
    },
    Trade {
        id: String,
        symbol: String,
        price: String,
        amount: String,
        side: String,
        timestamp: i64,
    },
    Orderbook {
        symbol: String,
        bids: Vec<OrderbookLevel>,
        asks: Vec<OrderbookLevel>,
        timestamp: i64,
    },
    Ticker {
        symbol: String,
        last_price: String,
        mark_price: String,
        index_price: String,
        price_change_24h: String,
        price_change_percent_24h: String,
        high_24h: String,
        low_24h: String,
        volume_24h: String,
        volume_24h_usd: String,
        /// Open Interest - Long position value in USD
        open_interest_long: String,
        /// Open Interest - Short position value in USD
        open_interest_short: String,
        /// Open Interest - Long percentage (e.g., "58")
        open_interest_long_percent: String,
        /// Open Interest - Short percentage (e.g., "42")
        open_interest_short_percent: String,
        /// Available liquidity for long positions
        available_liquidity_long: String,
        /// Available liquidity for short positions
        available_liquidity_short: String,
        /// Funding rate for long positions per hour (negative = pay)
        funding_rate_long_1h: String,
        /// Funding rate for short positions per hour (negative = pay)
        funding_rate_short_1h: String,
    },
    Position {
        id: String,
        symbol: String,
        side: String,
        size: String,
        entry_price: String,
        mark_price: String,
        liquidation_price: String,
        unrealized_pnl: String,
        leverage: i32,
        margin: String,
        updated_at: i64,
        #[serde(skip_serializing_if = "Option::is_none")]
        event: Option<String>,
    },
    Order {
        id: String,
        symbol: String,
        side: String,
        order_type: String,
        price: Option<String>,
        amount: String,
        filled_amount: String,
        status: String,
        updated_at: i64,
        #[serde(skip_serializing_if = "Option::is_none")]
        event: Option<String>,
    },
    Balance {
        token: String,
        symbol: String,
        available: String,
        frozen: String,
        total: String,
    },
    Error {
        code: String,
        message: String,
    },
    Pong,
    /// K-line update
    Kline {
        channel: String,
        data: KlineData,
    },
    /// Internal trade result (development only)
    InternalTradeResult {
        success: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        trade_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        symbol: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        price: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        amount: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        side: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        timestamp: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    /// Batch internal trade result (development only)
    InternalTradeBatchResult {
        success: bool,
        created: usize,
        failed: usize,
    },
    /// K-line snapshot (initial data on subscribe)
    KlineSnapshot {
        channel: String,
        data: KlineData,
    },
}

/// Orderbook level for WebSocket (frontend compatible format)
#[derive(Debug, Serialize, Clone)]
pub struct OrderbookLevel {
    pub price: String,
    pub size: String,
}

/// K-line data for WebSocket
#[derive(Debug, Serialize, Clone)]
pub struct KlineData {
    pub time: i64,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub volume: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quote_volume: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trade_count: Option<u32>,
    pub is_final: bool,
}

/// Format funding rate as percentage string with sign
fn format_funding_rate(rate: Decimal) -> String {
    let pct = rate * Decimal::from(100);
    if pct >= Decimal::ZERO {
        format!("+{}%", pct.round_dp(4))
    } else {
        format!("{}%", pct.round_dp(4))
    }
}


/// Validate timestamp (within 5 minutes)
#[allow(dead_code)]
// 全局快照并发上限。Redis 是 Arc<Mutex<ClusterConnection>>(单连接、串行)。
// 53 条 subscribe 每条 spawn 一个抓 Redis/DB 的任务,N 个客户端同时 reconnect
// 会把 Redis 当成串行队列,顺带卡住 matching engine 的写路径。
// 32 是经验值:同时跑的快照足够喂满 ~5 个客户端的初始 subscribe burst,
// 又给 REST 与撮合留余量。
static SNAPSHOT_SEM: OnceLock<Arc<Semaphore>> = OnceLock::new();
fn snapshot_sem() -> Arc<Semaphore> {
    SNAPSHOT_SEM
        .get_or_init(|| Arc::new(Semaphore::new(32)))
        .clone()
}

fn validate_timestamp(timestamp: u64) -> bool {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    now.abs_diff(timestamp) <= 300
}

pub async fn handle_socket(socket: WebSocket, state: Arc<AppState>) {
    let (mut sender, mut receiver) = socket.split();

    // Writer task pattern: 主循环只把 Message 推进 mpsc,真正的 sender.send().await
    // 在独立任务里跑。这样 select! 的任何一个分支(client message / broadcast / interval)
    // 都不会因为 socket 写慢被阻塞,subscribe 处理也不会拖死 ping/pong。
    //
    // 256 帧的 bounded 缓冲是 OOM 防御:slow client 会把 writer 卡住,unbounded 会把
    // 每个滞留连接的内存吃到无上限。256 帧 × ~5KB ≈ 1.25MB 上限;burst 期间(53 ack +
    // 53 snapshot = ~106 条)也能轻松吃下,稳态下也远低于上限。塞满时 try_send 静默丢
    // 帧 — 真要排到 256 条就说明 socket 已经废了,丢一条 ticker/orderbook 不致命。
    let (tx, mut rx) = mpsc::channel::<Message>(256);
    let mut writer_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if sender.send(msg).await.is_err() {
                break;
            }
        }
        let _ = sender.close().await;
    });

    let mut authenticated = false;
    let mut user_address: Option<String> = None;
    let mut subscriptions: HashSet<String> = HashSet::new();

    // Subscribe to trade events from matching engine
    let mut trade_receiver = state.matching_engine.subscribe_trades();
    tracing::info!("📡 WebSocket subscribed to trade events from matching engine");

    // Subscribe to orderbook updates from matching engine
    let mut orderbook_receiver = state.matching_engine.subscribe_orderbook();
    tracing::info!("📡 WebSocket subscribed to orderbook events from matching engine");

    // Subscribe to K-line updates
    let mut kline_receiver = state.kline_service.subscribe();

    // Subscribe to order updates for real-time push
    let mut order_update_receiver = state.order_update_sender.subscribe();
    tracing::info!("📡 WebSocket subscribed to order update events");

    // Subscribe to balance updates (event-driven via PG LISTEN/NOTIFY, see
    // migration 20260423100000_balance_change_notify.sql + bootstrap.rs).
    let mut balance_update_receiver = state.balance_update_sender.subscribe();

    // Subscribe to unified-margin account events (Phase 2 risk worker).
    let mut unified_account_receiver = state.unified_account_sender.subscribe();

    // Subscribe to Points events (PRD §9.4).
    let mut points_event_receiver = state.points_event_sender.subscribe();

    // Subscribe to VIP tier change events.
    let mut vip_tier_receiver = state.vip_tier_event_sender.subscribe();

    // Subscribe to spot public-channel broadcasts (Task 3 of spot WS plan).
    // Publisher lives at services/spot/ws_publisher.rs and only fans out per
    // channel; per-connection subscription filtering happens in the select!
    // arms below against `subscriptions`.
    let mut spot_depth_receiver  = state.spot_depth_sender.subscribe();
    let mut spot_trade_receiver  = state.spot_trade_sender.subscribe();
    let mut spot_ticker_receiver = state.spot_ticker_sender.subscribe();
    let mut spot_kline_receiver  = state.spot_kline_sender.subscribe();
    // Spot private channels (Task 5). Filtered per-connection by user_address
    // and `authenticated` in the select! arms below.
    let mut spot_user_order_receiver   = state.spot_user_order_sender.subscribe();
    let mut spot_user_balance_receiver = state.spot_user_balance_sender.subscribe();

    // Ticker update interval (every 2 seconds)
    let mut ticker_interval = tokio::time::interval(tokio::time::Duration::from_secs(2));

    // Orderbook update interval (every 500ms for real-time feel)
    let mut orderbook_interval = tokio::time::interval(tokio::time::Duration::from_millis(500));

    // Position/balance update interval for authenticated users (every 5 seconds)
    let mut private_interval = tokio::time::interval(tokio::time::Duration::from_secs(5));

    loop {
        tokio::select! {
            // 客户端消息(含 ping、subscribe)优先于所有 broadcast / interval 分支。
            // 没 biased 时 select 在多个 ready 分支中随机选,client_message 会被
            // 高频的 orderbook/ticker 事件饿死,导致 53 条 subscribe 慢慢挤,
            // 排在末尾的 kline 等 ack 撑爆客户端 pong 超时。
            biased;

            // Handle incoming client messages
            msg = receiver.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        if let Err(response) = handle_client_message(
                            &text,
                            &mut authenticated,
                            &mut user_address,
                            &mut subscriptions,
                            &state,
                            &tx,
                        ).await {
                            let _ = tx.try_send(Message::Text(serde_json::to_string(&response).unwrap()));
                        }
                    }
                    Some(Ok(Message::Ping(data))) => {
                        let _ = tx.try_send(Message::Pong(data));
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        break;
                    }
                    Some(Err(e)) => {
                        // Connection reset without closing handshake is normal
                        // (user closes browser, network switch, etc.)
                        tracing::warn!("WebSocket disconnected: {}", e);
                        break;
                    }
                    _ => {}
                }
            }

            // Handle trade events from matching engine
            trade = trade_receiver.recv() => {
                match trade {
                    Ok(trade_event) => {
                        tracing::debug!(
                            "📊 WebSocket received trade event: symbol={}, price={}, amount={}, side={}",
                            trade_event.symbol, trade_event.price, trade_event.amount, trade_event.side
                        );
                        
                        let trade_channel = format!("trades:{}", trade_event.symbol);
                        tracing::debug!(
                            "📡 Checking subscriptions for channel '{}': {:?}",
                            trade_channel, subscriptions
                        );
                        
                        if subscriptions.contains(&trade_channel) || subscriptions.contains("trades:*") {
                            tracing::info!("✅ Sending trade to WebSocket client: {}", trade_channel);
                            // Generate unique trade ID from timestamp and random suffix
                            let trade_id = format!("{}-{}", trade_event.timestamp, Uuid::new_v4().to_string().split('-').next().unwrap_or("0"));
                            let msg = ServerMessage::Trade {
                                id: trade_id,
                                symbol: trade_event.symbol.clone(),
                                price: format!("{:.5}", trade_event.price),
                                amount: trade_event.amount.to_string(), // Keep amount as is for now unless requested
                                side: trade_event.side.clone(),
                                timestamp: trade_event.timestamp,
                            };
                            let _ = tx.try_send(Message::Text(serde_json::to_string(&msg).unwrap()));
                        } else {
                            tracing::warn!(
                                "⚠️  Trade NOT sent - no matching subscription. Channel: '{}', Have: {:?}",
                                trade_channel, subscriptions
                            );
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("⚠️  Trade receiver lagged by {} messages - some trades may have been missed!", n);
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        tracing::error!("❌ Trade receiver closed - no more trade events will be received");
                        break;
                    }
                }
            }

            // Handle orderbook updates from matching engine
            orderbook = orderbook_receiver.recv() => {
                match orderbook {
                    Ok(orderbook_update) => {
                        let orderbook_channel = format!("orderbook:{}", orderbook_update.symbol);
                        // Drop frames where both sides are empty. The engine briefly
                        // emits these during some state transitions; if forwarded,
                        // the frontend overwrites the last good book with an empty
                        // one and the depth bars flash to "--" until the next real
                        // frame. Frontend has a defensive skip too (see
                        // interface PR #14), but stopping at the source is cheaper
                        // and protects any other client (gateway, mobile, etc.).
                        let is_empty = orderbook_update.bids.is_empty() && orderbook_update.asks.is_empty();
                        if !is_empty && (subscriptions.contains(&orderbook_channel) || subscriptions.contains("orderbook:*")) {
                            // Convert to frontend-compatible format
                            let bids: Vec<OrderbookLevel> = orderbook_update.bids
                                .into_iter()
                                .map(|[price, size]| OrderbookLevel { price, size })
                                .collect();
                            let asks: Vec<OrderbookLevel> = orderbook_update.asks
                                .into_iter()
                                .map(|[price, size]| OrderbookLevel { price, size })
                                .collect();
                            let msg = ServerMessage::Orderbook {
                                symbol: orderbook_update.symbol.clone(),
                                bids,
                                asks,
                                timestamp: orderbook_update.timestamp,
                            };
                            let _ = tx.try_send(Message::Text(serde_json::to_string(&msg).unwrap()));
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("Orderbook receiver lagged by {} messages", n);
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        // Continue without orderbook updates
                    }
                }
            }

            // Handle K-line updates
            kline = kline_receiver.recv() => {
                match kline {
                    Ok(kline_update) => {
                        // Check if client is subscribed to this kline channel
                        let channel = format!("kline:{}:{}", kline_update.symbol, kline_update.period);
                        if subscriptions.contains(&channel) {
                            let msg = ServerMessage::Kline {
                                channel: channel.clone(),
                                data: KlineData {
                                    time: kline_update.candle.time,
                                    open: kline_update.candle.open.to_string(),
                                    high: kline_update.candle.high.to_string(),
                                    low: kline_update.candle.low.to_string(),
                                    close: kline_update.candle.close.to_string(),
                                    volume: kline_update.candle.volume.to_string(),
                                    quote_volume: kline_update.candle.quote_volume.map(|v| v.to_string()),
                                    trade_count: kline_update.candle.trade_count,
                                    is_final: kline_update.is_final,
                                },
                            };
                            let _ = tx.try_send(Message::Text(serde_json::to_string(&msg).unwrap()));
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("Kline receiver lagged by {} messages", n);
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        // Continue without kline updates
                    }
                }
            }

            // Points push (PRD §9.4)
            points_event = points_event_receiver.recv() => {
                match points_event {
                    Ok(event) => {
                        if authenticated && user_address.is_some() {
                            let addr = user_address.as_ref().unwrap().to_lowercase();
                            if addr == event.user_address && subscriptions.contains("points") {
                                let msg = serde_json::json!({
                                    "channel": "points",
                                    "event": event.event,
                                    "data": event,
                                });
                                let _ = tx.try_send(Message::Text(serde_json::to_string(&msg).unwrap()));
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("Points event receiver lagged by {} messages", n);
                    }
                    Err(broadcast::error::RecvError::Closed) => {}
                }
            }

            // VIP tier change push
            vip_tier_event = vip_tier_receiver.recv() => {
                match vip_tier_event {
                    Ok(event) => {
                        if authenticated && user_address.is_some() {
                            let addr = user_address.as_ref().unwrap().to_lowercase();
                            if addr == event.user_address && subscriptions.contains("vip_tier") {
                                let msg = serde_json::json!({
                                    "channel": "vip_tier",
                                    "type": "vip_tier_changed",
                                    "data": event,
                                });
                                let _ = tx.try_send(Message::Text(serde_json::to_string(&msg).unwrap()));
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("VIP tier receiver lagged by {} messages", n);
                    }
                    Err(broadcast::error::RecvError::Closed) => {}
                }
            }

            // Unified-margin account updates (Phase 2)
            unified_account_event = unified_account_receiver.recv() => {
                match unified_account_event {
                    Ok(event) => {
                        if authenticated && user_address.is_some() {
                            let addr = user_address.as_ref().unwrap().to_lowercase();
                            if addr == event.user_address
                                && subscriptions.contains("unified_account")
                            {
                                let msg = serde_json::json!({
                                    "channel": "unified_account",
                                    "event": event.event,
                                    "data": event,
                                });
                                let _ = tx.try_send(Message::Text(serde_json::to_string(&msg).unwrap()));
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("Unified-account receiver lagged by {} messages", n);
                    }
                    Err(broadcast::error::RecvError::Closed) => {}
                }
            }

            // Handle balance updates (event-driven via PG LISTEN/NOTIFY).
            // Replaces the old 5s `private_interval` poll for the `balances`
            // channel — that poll still runs for positions (not yet migrated).
            balance_update = balance_update_receiver.recv() => {
                match balance_update {
                    Ok(event) => {
                        if authenticated
                            && user_address.is_some()
                            && subscriptions.contains("balances")
                        {
                            let addr = user_address.as_ref().unwrap().to_lowercase();
                            if addr == event.user_address.to_lowercase() {
                                let msg = serde_json::json!({
                                    "channel": "balances",
                                    "type": "balance_update",
                                    "data": event,
                                });
                                let _ = tx.try_send(Message::Text(serde_json::to_string(&msg).unwrap()));
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("Balance update receiver lagged by {} messages", n);
                    }
                    Err(broadcast::error::RecvError::Closed) => {}
                }
            }

            // Handle order updates (real-time push when orders are created/updated)
            order_update = order_update_receiver.recv() => {
                match order_update {
                    Ok(event) => {
                        // Only send to the user who owns this order
                        if authenticated && user_address.is_some() {
                            let addr = user_address.as_ref().unwrap().to_lowercase();
                            if addr == event.user_address && subscriptions.contains("orders") {
                                tracing::info!(
                                    "📤 Sending real-time order update to {}: order_id={}, status={:?}",
                                    addr, event.order.order_id, event.order.status
                                );
                                let msg = serde_json::json!({
                                    "channel": "orders",
                                    "type": "order_update",
                                    "data": event.order
                                });
                                let _ = tx.try_send(Message::Text(serde_json::to_string(&msg).unwrap()));
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("Order update receiver lagged by {} messages", n);
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        // Continue without order updates
                    }
                }
            }

            // Spot public channel: depth diff (Task 3 of spot WS plan).
            spot_depth = spot_depth_receiver.recv() => {
                match spot_depth {
                    Ok(push) => {
                        let chan = format!("spot:depth:{}", push.symbol);
                        if subscriptions.contains(&chan) {
                            if let Ok(json) = serde_json::to_string(&serde_json::json!({
                                "type":    "spot_depth_diff",
                                "channel": chan,
                                "data":    push,
                            })) {
                                let _ = tx.try_send(Message::Text(json));
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("Spot depth receiver lagged by {} messages", n);
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        // Continue without spot depth updates
                    }
                }
            }

            // Spot public channel: trade (Task 3 of spot WS plan).
            spot_trade = spot_trade_receiver.recv() => {
                match spot_trade {
                    Ok(push) => {
                        let chan = format!("spot:trade:{}", push.symbol);
                        if subscriptions.contains(&chan) {
                            if let Ok(json) = serde_json::to_string(&serde_json::json!({
                                "type":    "spot_trade",
                                "channel": chan,
                                "data":    push,
                            })) {
                                let _ = tx.try_send(Message::Text(json));
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("Spot trade receiver lagged by {} messages", n);
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        // Continue without spot trade updates
                    }
                }
            }

            // Spot public channel: ticker (Task 3 of spot WS plan).
            spot_ticker = spot_ticker_receiver.recv() => {
                match spot_ticker {
                    Ok(push) => {
                        let chan = format!("spot:ticker:{}", push.symbol);
                        if subscriptions.contains(&chan) {
                            if let Ok(json) = serde_json::to_string(&serde_json::json!({
                                "type":    "spot_ticker",
                                "channel": chan,
                                "data":    push,
                            })) {
                                let _ = tx.try_send(Message::Text(json));
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("Spot ticker receiver lagged by {} messages", n);
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        // Continue without spot ticker updates
                    }
                }
            }

            // Spot public channel: kline incremental update (Task 4 of spot WS plan).
            spot_kline = spot_kline_receiver.recv() => {
                match spot_kline {
                    Ok(push) => {
                        let chan = format!("spot:kline:{}:{}", push.symbol, push.interval);
                        if subscriptions.contains(&chan) {
                            if let Ok(json) = serde_json::to_string(&serde_json::json!({
                                "type":    "spot_kline_update",
                                "channel": chan,
                                "data":    push,
                            })) {
                                let _ = tx.try_send(Message::Text(json));
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("Spot kline receiver lagged by {} messages", n);
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        // Continue without spot kline updates
                    }
                }
            }

            spot_user_order = spot_user_order_receiver.recv() => {
                match spot_user_order {
                    Ok(push) => {
                        if authenticated
                            && user_address.as_deref()
                                .map(|a| a.eq_ignore_ascii_case(&push.user_address))
                                .unwrap_or(false)
                            && subscriptions.contains("spot:user:orders")
                        {
                            if let Ok(json) = serde_json::to_string(&serde_json::json!({
                                "type":    "spot_user_order",
                                "channel": "spot:user:orders",
                                "data":    push,
                            })) {
                                let _ = tx.try_send(Message::Text(json));
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("spot user order lagged: {n}");
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        tracing::warn!("spot user order channel closed");
                    }
                }
            }

            spot_user_balance = spot_user_balance_receiver.recv() => {
                match spot_user_balance {
                    Ok(push) => {
                        if authenticated
                            && user_address.as_deref()
                                .map(|a| a.eq_ignore_ascii_case(&push.user_address))
                                .unwrap_or(false)
                            && subscriptions.contains("spot:user:balances")
                        {
                            if let Ok(json) = serde_json::to_string(&serde_json::json!({
                                "type":    "spot_user_balance",
                                "channel": "spot:user:balances",
                                "data":    push,
                            })) {
                                let _ = tx.try_send(Message::Text(json));
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("spot user balance lagged: {n}");
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        tracing::warn!("spot user balance channel closed");
                    }
                }
            }

            // Ticker updates
            _ = ticker_interval.tick() => {
                for channel in &subscriptions {
                    if channel.starts_with("ticker:") {
                        let raw_symbol = channel.strip_prefix("ticker:").unwrap_or("");
                        let symbol = normalize_symbol(raw_symbol);
                        
                        tracing::trace!("Ticker update check for {} (from channel: {})", symbol, channel);
                        
                        if let Some(price_data) = state.price_feed_service.get_price_data(&symbol).await {
                            tracing::trace!("Sending ticker data for {}: price={}", symbol, price_data.last_price);
                            // Get funding rate info for open interest and rates
                            let funding_info = state.funding_rate_service.get_funding_rate(&symbol).await;

                            // Get open interest values
                            let (oi_long, oi_short) = if let Some(ref info) = funding_info {
                                (info.long_open_interest, info.short_open_interest)
                            } else {
                                (Decimal::ZERO, Decimal::ZERO)
                            };

                            // Calculate OI percentages
                            let total_oi = oi_long + oi_short;
                            let (oi_long_pct, oi_short_pct) = if total_oi > Decimal::ZERO {
                                let long_pct = (oi_long / total_oi * Decimal::from(100)).round_dp(0);
                                let short_pct = (oi_short / total_oi * Decimal::from(100)).round_dp(0);
                                (long_pct, short_pct)
                            } else {
                                (Decimal::from(50), Decimal::from(50))
                            };

                            // Get funding rate per hour
                            let funding_rate_1h = if let Some(ref info) = funding_info {
                                info.funding_rate_per_hour
                            } else {
                                Decimal::ZERO
                            };

                            // Funding: Long pays when rate is positive, Short pays when rate is negative
                            let funding_long = -funding_rate_1h;  // Long pays positive rate
                            let funding_short = funding_rate_1h;   // Short receives positive rate

                            // Calculate available liquidity from orderbook
                            let (liq_long, liq_short) = if let Some(orderbook_cache) = state.cache.orderbook_opt() {
                                let ob = orderbook_cache.get_orderbook(&symbol, Some(50)).await;
                                let ask_liquidity: Decimal = ob.asks.iter()
                                    .map(|level| level.price * level.amount)
                                    .sum();
                                let bid_liquidity: Decimal = ob.bids.iter()
                                    .map(|level| level.price * level.amount)
                                    .sum();
                                (ask_liquidity, bid_liquidity)  // Long buys from asks, Short buys from bids
                            } else {
                                (Decimal::ZERO, Decimal::ZERO)
                            };

                            let msg = ServerMessage::Ticker {
                                symbol: symbol.to_string(),
                                last_price: price_data.last_price.to_string(),
                                mark_price: price_data.mark_price.to_string(),
                                index_price: price_data.index_price.to_string(),
                                price_change_24h: price_data.price_change_24h.to_string(),
                                price_change_percent_24h: price_data.price_change_percent_24h.to_string(),
                                high_24h: price_data.high_24h.to_string(),
                                low_24h: price_data.low_24h.to_string(),
                                volume_24h: price_data.volume_24h.to_string(),
                                volume_24h_usd: price_data.volume_ccy_24h.to_string(),
                                open_interest_long: oi_long.to_string(),
                                open_interest_short: oi_short.to_string(),
                                open_interest_long_percent: oi_long_pct.to_string(),
                                open_interest_short_percent: oi_short_pct.to_string(),
                                available_liquidity_long: liq_long.to_string(),
                                available_liquidity_short: liq_short.to_string(),
                                funding_rate_long_1h: format_funding_rate(funding_long),
                                funding_rate_short_1h: format_funding_rate(funding_short),
                            };
                            let _ = tx.try_send(Message::Text(serde_json::to_string(&msg).unwrap()));
                        }
                    }
                }
            }

            // Orderbook updates from Redis cache
            _ = orderbook_interval.tick() => {
                if let Some(orderbook_cache) = state.cache.orderbook_opt() {
                    for channel in &subscriptions {
                        if channel.starts_with("orderbook:") {
                            let raw_symbol = channel.strip_prefix("orderbook:").unwrap_or("");
                            let symbol = normalize_symbol(raw_symbol);
                            let cached = orderbook_cache.get_orderbook(&symbol, Some(30)).await;
                            if !cached.bids.is_empty() || !cached.asks.is_empty() {
                                let bids: Vec<OrderbookLevel> = cached.bids
                                    .iter()
                                    .map(|level| OrderbookLevel {
                                        price: level.price.to_string(),
                                        size: level.amount.to_string(),
                                    })
                                    .collect();
                                let asks: Vec<OrderbookLevel> = cached.asks
                                    .iter()
                                    .map(|level| OrderbookLevel {
                                        price: level.price.to_string(),
                                        size: level.amount.to_string(),
                                    })
                                    .collect();
                                let msg = ServerMessage::Orderbook {
                                    symbol: cached.symbol,
                                    bids,
                                    asks,
                                    timestamp: cached.timestamp,
                                };
                                let _ = tx.try_send(Message::Text(serde_json::to_string(&msg).unwrap()));
                            }
                        }
                    }
                }
            }

            // Private data updates (positions, orders, balances)
            _ = private_interval.tick() => {
                if authenticated && user_address.is_some() {
                    let address = user_address.as_ref().unwrap().to_lowercase();

                    // Send position updates
                    if subscriptions.contains("positions") {
                        if let Ok(positions) = fetch_user_positions(&state, &address).await {
                            for position in positions {
                                let _ = tx.try_send(Message::Text(serde_json::to_string(&position).unwrap()));
                            }
                        }
                    }

                    // Legacy balance poll (kept for `balance` subscribers during a
                    // transition window so no client regresses). New clients should
                    // subscribe to `balances` and rely on the event-driven push in
                    // the `balance_update_receiver` arm above — this poll is the
                    // fallback path only.
                    if subscriptions.contains("balance") {
                        if let Ok(balances) = fetch_user_balances(&state, &address).await {
                            for balance in balances {
                                let _ = tx.try_send(Message::Text(serde_json::to_string(&balance).unwrap()));
                            }
                        }
                    }

                    // Send open order updates
                    if subscriptions.contains("orders") {
                        if let Ok(orders) = fetch_user_orders(&state, &address).await {
                            for order in orders {
                                let _ = tx.try_send(Message::Text(serde_json::to_string(&order).unwrap()));
                            }
                        }
                    }
                }
            }
        }
    }

    // 关掉 mpsc → writer task 收完剩余消息后 sender.close() 并退出。
    // 注意:spawned snapshot 任务里持有 tx_c 的克隆,所以 channel 在所有克隆 drop 之前
    // 不会真正关闭。如果 snapshot 卡在慢 Redis/DB 上,writer_task 会一直挂。给个 5s
    // 超时,超了就 abort,避免 socket 已断的连接长期占着 task。
    drop(tx);
    if tokio::time::timeout(Duration::from_secs(5), &mut writer_task).await.is_err() {
        writer_task.abort();
    }

    tracing::info!("WebSocket connection closed for {:?}", user_address);
}

async fn handle_client_message(
    text: &str,
    authenticated: &mut bool,
    user_address: &mut Option<String>,
    subscriptions: &mut HashSet<String>,
    state: &Arc<AppState>,
    tx: &mpsc::Sender<Message>,
) -> Result<(), ServerMessage> {
    let client_msg: ClientMessage = serde_json::from_str(text).map_err(|e| ServerMessage::Error {
        code: "INVALID_MESSAGE".to_string(),
        message: format!("Failed to parse message: {}", e),
    })?;

    match client_msg {
        ClientMessage::Auth {
            address,
            signature,
            timestamp,
            token,
            listen_key,
        } => {
            // listenKey-based auth (Binance fapi compatibility). The key is
            // resolved against Redis where `POST /fapi/v1/listenKey` stored
            // the (key → user_address) mapping; the lookup also refreshes
            // the TTL on both directions, so an open WS effectively keeps
            // the key alive without the bot having to call `PUT /listenKey`.
            if let Some(key) = listen_key {
                match crate::api::handlers::developer_listen_key::resolve_listen_key(state, &key).await {
                    Some(addr) => {
                        *authenticated = true;
                        *user_address = Some(addr.clone());
                        tracing::info!("WebSocket authenticated via listenKey: {}", addr);
                        let response = ServerMessage::AuthResult {
                            success: true,
                            message: None,
                        };
                        let _ = tx.try_send(Message::Text(serde_json::to_string(&response).unwrap()));
                    }
                    None => {
                        tracing::warn!("WebSocket listenKey not found / expired");
                        let response = ServerMessage::AuthResult {
                            success: false,
                            message: Some("Invalid or expired listenKey".to_string()),
                        };
                        let _ = tx.try_send(Message::Text(serde_json::to_string(&response).unwrap()));
                    }
                }
                return Ok(());
            }

            // Check if token-based auth (JWT)
            if let Some(jwt_token) = token {
                match validate_token(&jwt_token, &state.config.jwt_secret) {
                    Ok(claims) => {
                        *authenticated = true;
                        *user_address = Some(claims.sub.to_lowercase());

                        tracing::info!("WebSocket authenticated via JWT: {}", claims.sub);

                        let response = ServerMessage::AuthResult {
                            success: true,
                            message: None,
                        };
                        let _ = tx.try_send(Message::Text(serde_json::to_string(&response).unwrap()));
                    }
                    Err(e) => {
                        tracing::warn!("WebSocket JWT validation failed: {}", e);
                        let response = ServerMessage::AuthResult {
                            success: false,
                            message: Some("Invalid or expired token".to_string()),
                        };
                        let _ = tx.try_send(Message::Text(serde_json::to_string(&response).unwrap()));
                    }
                }
                return Ok(());
            }

            // Signature-based auth requires all fields
            let (address, signature, timestamp) = match (address, signature, timestamp) {
                (Some(a), Some(s), Some(t)) => (a, s, t),
                _ => {
                    let response = ServerMessage::AuthResult {
                        success: false,
                        message: Some("Missing required fields for signature auth".to_string()),
                    };
                    let _ = tx.try_send(Message::Text(serde_json::to_string(&response).unwrap()));
                    return Ok(());
                }
            };

            // 验证时间戳（5分钟内有效）
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();

            if now.abs_diff(timestamp) > 300 {
                tracing::warn!("WebSocket auth timestamp expired for address: {}", address);
                let response = ServerMessage::AuthResult {
                    success: false,
                    message: Some("Timestamp expired".to_string()),
                };
                let _ = tx.try_send(Message::Text(serde_json::to_string(&response).unwrap()));
                return Ok(());
            }

            // EIP-712 签名验证
            let ws_auth_msg = WebSocketAuthMessage {
                wallet: address.to_lowercase(),
                timestamp,
            };

            let valid = match verify_ws_auth_signature(&ws_auth_msg, &signature, &address) {
                Ok(v) => v,
                Err(e) => {
                    tracing::error!("WebSocket auth signature verification error for {}: {}", address, e);
                    let response = ServerMessage::AuthResult {
                        success: false,
                        message: Some("Invalid signature format".to_string()),
                    };
                    let _ = tx.try_send(Message::Text(serde_json::to_string(&response).unwrap()));
                    return Ok(());
                }
            };

            if !valid {
                tracing::warn!("WebSocket auth signature verification failed for address: {}", address);
                let response = ServerMessage::AuthResult {
                    success: false,
                    message: Some("Signature verification failed".to_string()),
                };
                let _ = tx.try_send(Message::Text(serde_json::to_string(&response).unwrap()));
                return Ok(());
            }

            tracing::info!("EIP-712 WebSocket auth signature verified for address: {}", address);

            *authenticated = true;
            *user_address = Some(address.to_lowercase());

            tracing::info!("WebSocket authenticated: {}", address);

            let response = ServerMessage::AuthResult {
                success: true,
                message: None,
            };
            let _ = tx.try_send(Message::Text(serde_json::to_string(&response).unwrap()));
        }

        ClientMessage::AuthToken { token } => {
            // Validate JWT token
            match validate_token(&token, &state.config.jwt_secret) {
                Ok(claims) => {
                    *authenticated = true;
                    *user_address = Some(claims.sub.to_lowercase());

                    tracing::info!("WebSocket authenticated via JWT: {}", claims.sub);

                    let response = ServerMessage::AuthResult {
                        success: true,
                        message: None,
                    };
                    let _ = tx.try_send(Message::Text(serde_json::to_string(&response).unwrap()));
                }
                Err(e) => {
                    tracing::warn!("WebSocket JWT validation failed: {}", e);
                    let response = ServerMessage::AuthResult {
                        success: false,
                        message: Some("Invalid or expired token".to_string()),
                    };
                    let _ = tx.try_send(Message::Text(serde_json::to_string(&response).unwrap()));
                }
            }
        }

        ClientMessage::Subscribe { channel, token } => {
            // If token is provided with subscribe, try to authenticate first
            if let Some(jwt_token) = token {
                if !*authenticated {
                    if let Ok(claims) = validate_token(&jwt_token, &state.config.jwt_secret) {
                        *authenticated = true;
                        *user_address = Some(claims.sub.to_lowercase());
                        tracing::info!("WebSocket auto-authenticated via subscribe token: {}", claims.sub);
                    }
                }
            }

            // ── Channel validation ────────────────────────────────────────
            // 已知公开频道前缀（无需认证）
            const PUBLIC_PREFIXES: &[&str] = &[
                "trade:", "trades:",          // 成交广播（双命名历史遗留）
                "orderbook:",                 // 订单簿快照
                "ticker:", "kline:", "candle:", // 市场数据
                "funding_rate:", "liquidation:", "adl:",
                "mark_price:", "index_price:",
                // Spot public channels (Tasks 3-4 of spot WS plan).
                "spot:depth:", "spot:trade:", "spot:ticker:", "spot:kline:",
            ];
            // 已知私有频道（只能看自己的，静态名 + 需 auth）
            const PRIVATE_STATIC: &[&str] = &[
                "positions", "orders", "balance",
                "unified_account", "points", "vip_tier",
                // Spot private channels (Task 5). Auth-gated by the
                // is_private_static branch below; pushes are filtered
                // per-connection by user_address in the spot_user_*
                // select! arms. No snapshot — clients call REST
                // /spot/orders or /spot/balances for initial state.
                "spot:user:orders", "spot:user:balances",
            ];
            // 私有频道前缀：user_*:<addr> 或 private:<addr>
            const PRIVATE_PREFIXES: &[&str] = &[
                "user_orders:", "user_trades:", "user_positions:",
                "user_balance:", "user_balances:", "user_account:",
                "private:",
            ];

            let is_public = PUBLIC_PREFIXES.iter().any(|p| channel.starts_with(p));
            let is_private_static = PRIVATE_STATIC.iter().any(|p| channel == *p);
            let is_private_prefix = PRIVATE_PREFIXES.iter().any(|p| channel.starts_with(p));
            let is_known = is_public || is_private_static || is_private_prefix;

            if !is_known {
                // 之前未知频道会被静默接受（"subscribed" 响应），误导客户端。
                // QA 发现 subscribe 到 `user_orders:<他人地址>` 返 subscribed，
                // 虽然实际无数据推送，但客户端无法感知自己订阅错。
                return Err(ServerMessage::Error {
                    code: "INVALID_CHANNEL".to_string(),
                    message: format!("Unknown channel: {}", channel),
                });
            }

            let is_private = is_private_static || is_private_prefix;
            if is_private && !*authenticated {
                return Err(ServerMessage::Error {
                    code: "AUTH_REQUIRED".to_string(),
                    message: "Authentication required for private channels".to_string(),
                });
            }

            // 私有前缀频道里的 address 必须和已认证的 user 匹配，
            // 否则可以"订阅"他人通道（当前实际无数据流，但策略上不该允许）。
            if is_private_prefix {
                if let Some(suffix) = channel.splitn(2, ':').nth(1) {
                    let auth_addr = user_address.as_ref().map(|a| a.to_lowercase()).unwrap_or_default();
                    if suffix.to_lowercase() != auth_addr {
                        return Err(ServerMessage::Error {
                            code: "FORBIDDEN".to_string(),
                            message: "Cannot subscribe to another user's private channel".to_string(),
                        });
                    }
                }
            }

            subscriptions.insert(channel.clone());
            
            tracing::info!(
                "✅ Client subscribed to '{}' (total subscriptions: {})",
                channel, subscriptions.len()
            );
            tracing::debug!("Current subscriptions: {:?}", subscriptions);

            let response = ServerMessage::Subscribed { channel: channel.clone() };
            let _ = tx.try_send(Message::Text(serde_json::to_string(&response).unwrap()));

            // Send initial data for certain channels.
            // 所有 snapshot 抓取都丢到 tokio::spawn 里跑,handle_client_message 立刻
            // 返回。否则一次 subscribe 串行 await Redis/DB,53 条 subscribe 把主循环
            // 卡 7~15 秒,客户端 pong 超时直接断线。
            if channel.starts_with("orderbook:") {
                let raw_symbol = channel.strip_prefix("orderbook:").unwrap_or("").to_string();
                let state_c = state.clone();
                let tx_c = tx.clone();
                let sem = snapshot_sem();
                tokio::spawn(async move {
                    let _permit = sem.acquire_owned().await.ok();
                    let symbol = normalize_symbol(&raw_symbol);
                    let orderbook_msg = if let Some(orderbook_cache) = state_c.cache.orderbook_opt() {
                        let cached = orderbook_cache.get_orderbook(&symbol, Some(30)).await;
                        if !cached.bids.is_empty() || !cached.asks.is_empty() {
                            let bids: Vec<OrderbookLevel> = cached.bids
                                .iter()
                                .map(|level| OrderbookLevel {
                                    price: level.price.to_string(),
                                    size: level.amount.to_string(),
                                })
                                .collect();
                            let asks: Vec<OrderbookLevel> = cached.asks
                                .iter()
                                .map(|level| OrderbookLevel {
                                    price: level.price.to_string(),
                                    size: level.amount.to_string(),
                                })
                                .collect();
                            Some(ServerMessage::Orderbook {
                                symbol: cached.symbol,
                                bids,
                                asks,
                                timestamp: cached.timestamp,
                            })
                        } else {
                            None
                        }
                    } else {
                        None
                    };

                    // Symmetric to the cached path above (line ~1069: only emit
                    // when at least one side is non-empty). Previously this fell
                    // back to `ServerMessage::Orderbook { bids: vec![], asks: vec![] }`
                    // when `get_orderbook` returned Err, which the frontend would
                    // accept as "real data" and use to wipe the displayed book.
                    // Now we just don't send anything — the next non-empty frame
                    // from the broadcast channel will populate the client.
                    let msg = orderbook_msg.or_else(|| {
                        let snapshot = state_c.matching_engine.get_orderbook(&symbol, 30).ok()?;
                        if snapshot.bids.is_empty() && snapshot.asks.is_empty() {
                            return None;
                        }
                        let bids: Vec<OrderbookLevel> = snapshot.bids
                            .into_iter()
                            .map(|[price, size]| OrderbookLevel { price, size })
                            .collect();
                        let asks: Vec<OrderbookLevel> = snapshot.asks
                            .into_iter()
                            .map(|[price, size]| OrderbookLevel { price, size })
                            .collect();
                        Some(ServerMessage::Orderbook {
                            symbol: snapshot.symbol,
                            bids,
                            asks,
                            timestamp: snapshot.timestamp,
                        })
                    });
                    if let Some(msg) = msg {
                        let _ = tx_c.try_send(Message::Text(serde_json::to_string(&msg).unwrap()));
                    }
                });
            } else if channel.starts_with("ticker:") {
                let raw_symbol = channel.strip_prefix("ticker:").unwrap_or("").to_string();
                let state_c = state.clone();
                let tx_c = tx.clone();
                let sem = snapshot_sem();
                tokio::spawn(async move {
                    let _permit = sem.acquire_owned().await.ok();
                    let symbol = normalize_symbol(&raw_symbol);
                    let Some(price_data) = state_c.price_feed_service.get_price_data(&symbol).await else { return; };
                    let funding_info = state_c.funding_rate_service.get_funding_rate(&symbol).await;

                    let (oi_long, oi_short) = if let Some(ref info) = funding_info {
                        (info.long_open_interest, info.short_open_interest)
                    } else {
                        (Decimal::ZERO, Decimal::ZERO)
                    };

                    let total_oi = oi_long + oi_short;
                    let (oi_long_pct, oi_short_pct) = if total_oi > Decimal::ZERO {
                        let long_pct = (oi_long / total_oi * Decimal::from(100)).round_dp(0);
                        let short_pct = (oi_short / total_oi * Decimal::from(100)).round_dp(0);
                        (long_pct, short_pct)
                    } else {
                        (Decimal::from(50), Decimal::from(50))
                    };

                    let funding_rate_1h = if let Some(ref info) = funding_info {
                        info.funding_rate_per_hour
                    } else {
                        Decimal::ZERO
                    };

                    let funding_long = -funding_rate_1h;
                    let funding_short = funding_rate_1h;

                    let (liq_long, liq_short) = if let Some(orderbook_cache) = state_c.cache.orderbook_opt() {
                        let ob = orderbook_cache.get_orderbook(&symbol, Some(50)).await;
                        let ask_liquidity: Decimal = ob.asks.iter()
                            .map(|level| level.price * level.amount)
                            .sum();
                        let bid_liquidity: Decimal = ob.bids.iter()
                            .map(|level| level.price * level.amount)
                            .sum();
                        (ask_liquidity, bid_liquidity)
                    } else {
                        (Decimal::ZERO, Decimal::ZERO)
                    };

                    let msg = ServerMessage::Ticker {
                        symbol: symbol.to_string(),
                        last_price: price_data.last_price.to_string(),
                        mark_price: price_data.mark_price.to_string(),
                        index_price: price_data.index_price.to_string(),
                        price_change_24h: price_data.price_change_24h.to_string(),
                        price_change_percent_24h: price_data.price_change_percent_24h.to_string(),
                        high_24h: price_data.high_24h.to_string(),
                        low_24h: price_data.low_24h.to_string(),
                        volume_24h: price_data.volume_24h.to_string(),
                        volume_24h_usd: price_data.volume_ccy_24h.to_string(),
                        open_interest_long: oi_long.to_string(),
                        open_interest_short: oi_short.to_string(),
                        open_interest_long_percent: oi_long_pct.to_string(),
                        open_interest_short_percent: oi_short_pct.to_string(),
                        available_liquidity_long: liq_long.to_string(),
                        available_liquidity_short: liq_short.to_string(),
                        funding_rate_long_1h: format_funding_rate(funding_long),
                        funding_rate_short_1h: format_funding_rate(funding_short),
                    };
                    let _ = tx_c.try_send(Message::Text(serde_json::to_string(&msg).unwrap()));
                });
            } else if channel == "positions" && *authenticated && user_address.is_some() {
                let address = user_address.as_ref().unwrap().to_lowercase();
                let state_c = state.clone();
                let tx_c = tx.clone();
                let sem = snapshot_sem();
                tokio::spawn(async move {
                    let _permit = sem.acquire_owned().await.ok();
                    if let Ok(positions) = fetch_user_positions(&state_c, &address).await {
                        for position in positions {
                            let _ = tx_c.try_send(Message::Text(serde_json::to_string(&position).unwrap()));
                        }
                    }
                });
            } else if (channel == "balances" || channel == "balance")
                && *authenticated
                && user_address.is_some()
            {
                // Initial snapshot on subscribe. LISTEN/NOTIFY only emits
                // future deltas; without this, a client that connects and
                // subscribes would have no balance state until the first
                // INSERT/UPDATE lands.
                let address = user_address.as_ref().unwrap().to_lowercase();
                let state_c = state.clone();
                let tx_c = tx.clone();
                let sem = snapshot_sem();
                tokio::spawn(async move {
                    let _permit = sem.acquire_owned().await.ok();
                    if let Ok(balances) = fetch_user_balances(&state_c, &address).await {
                        for balance in balances {
                            let _ = tx_c.try_send(Message::Text(serde_json::to_string(&balance).unwrap()));
                        }
                    }
                });
            } else if channel == "orders" && *authenticated && user_address.is_some() {
                let address = user_address.as_ref().unwrap().to_lowercase();
                let state_c = state.clone();
                let tx_c = tx.clone();
                let sem = snapshot_sem();
                tokio::spawn(async move {
                    let _permit = sem.acquire_owned().await.ok();
                    if let Ok(orders) = fetch_user_orders(&state_c, &address).await {
                        for order in orders {
                            let _ = tx_c.try_send(Message::Text(serde_json::to_string(&order).unwrap()));
                        }
                    }
                });
            } else if channel.starts_with("kline:") {
                // Parse kline channel: kline:{symbol}:{period}
                let parts: Vec<&str> = channel.strip_prefix("kline:").unwrap_or("").split(':').collect();
                if parts.len() == 2 {
                    let raw_symbol = parts[0].to_string();
                    let period_str = parts[1].to_string();
                    let chan = channel.clone();
                    let state_c = state.clone();
                    let tx_c = tx.clone();
                    let sem = snapshot_sem();
                    tokio::spawn(async move {
                        let _permit = sem.acquire_owned().await.ok();
                        let symbol = normalize_symbol(&raw_symbol);
                        let Some(period) = KlinePeriod::from_str(&period_str) else {
                            tracing::warn!("kline subscribe with unknown period: {}", period_str);
                            return;
                        };
                        let candles = state_c.kline_service.get_candles(&symbol, period, 1, None, None).await;
                        if let Some(latest_candle) = candles.into_iter().last() {
                            let snapshot_data = KlineData {
                                time: latest_candle.time,
                                open: latest_candle.open.to_string(),
                                high: latest_candle.high.to_string(),
                                low: latest_candle.low.to_string(),
                                close: latest_candle.close.to_string(),
                                volume: latest_candle.volume.to_string(),
                                quote_volume: latest_candle.quote_volume.map(|v| v.to_string()),
                                trade_count: latest_candle.trade_count,
                                is_final: true,
                            };
                            let msg = ServerMessage::KlineSnapshot {
                                channel: chan,
                                data: snapshot_data,
                            };
                            let _ = tx_c.try_send(Message::Text(serde_json::to_string(&msg).unwrap()));
                        }
                    });
                }
            } else if channel.starts_with("spot:depth:") {
                // Spot depth snapshot via the spot matching engine. Quietly
                // skips when the engine isn't running (e.g. spot trading
                // disabled in this env).
                let symbol = channel.strip_prefix("spot:depth:").unwrap_or("").to_string();
                if !symbol.is_empty() {
                    let state_c = state.clone();
                    let tx_c = tx.clone();
                    let chan_c = channel.clone();
                    let sem = snapshot_sem();
                    tokio::spawn(async move {
                        let _permit = sem.acquire_owned().await.ok();
                        let Some(eng) = state_c.spot_engine.as_ref() else { return };
                        if let Ok((bids, asks, last_id)) = eng.depth(1000).await {
                            let bids_s: Vec<[String; 2]> = bids.into_iter()
                                .map(|(p, q)| [p.normalize().to_string(), q.normalize().to_string()])
                                .collect();
                            let asks_s: Vec<[String; 2]> = asks.into_iter()
                                .map(|(p, q)| [p.normalize().to_string(), q.normalize().to_string()])
                                .collect();
                            let msg = serde_json::json!({
                                "type":    "spot_depth_snapshot",
                                "channel": chan_c,
                                "data": {
                                    "symbol":         symbol,
                                    "last_update_id": last_id,
                                    "bids":           bids_s,
                                    "asks":           asks_s,
                                }
                            });
                            if let Ok(json) = serde_json::to_string(&msg) {
                                let _ = tx_c.try_send(Message::Text(json));
                            }
                        }
                    });
                }
            } else if channel.starts_with("spot:trade:") {
                // No initial snapshot for trades — live tape only.
                // Validity (non-empty symbol) is the only check; subscription
                // already inserted above.
            } else if channel.starts_with("spot:ticker:") {
                let symbol = channel.strip_prefix("spot:ticker:").unwrap_or("").to_string();
                if !symbol.is_empty() {
                    let state_c = state.clone();
                    let tx_c = tx.clone();
                    let chan_c = channel.clone();
                    let sem = snapshot_sem();
                    tokio::spawn(async move {
                        let _permit = sem.acquire_owned().await.ok();
                        let row: Result<crate::models::spot::SpotTicker24h, _> = sqlx::query_as(
                            "SELECT * FROM spot_ticker_24h WHERE market_id=$1"
                        ).bind(&symbol).fetch_one(&state_c.db.pool).await;
                        if let Ok(t) = row {
                            let now = chrono::Utc::now().timestamp();
                            let payload = serde_json::json!({
                                "type":    "spot_ticker",
                                "channel": chan_c,
                                "data": {
                                    "symbol":       t.market_id,
                                    "last_price":   t.last_price.normalize().to_string(),
                                    "open_price":   t.open_price_24h.normalize().to_string(),
                                    "high":         t.high_24h.normalize().to_string(),
                                    "low":          t.low_24h.normalize().to_string(),
                                    "volume":       t.volume_24h.normalize().to_string(),
                                    "quote_volume": t.quote_volume_24h.normalize().to_string(),
                                    "trade_count":  t.trade_count_24h,
                                    "open_time":    now - 24 * 3600,
                                    "close_time":   now,
                                    "ts":           now,
                                }
                            });
                            if let Ok(json) = serde_json::to_string(&payload) {
                                let _ = tx_c.try_send(Message::Text(json));
                            }
                        }
                    });
                }
            } else if channel.starts_with("spot:kline:") {
                // Spot kline channel: 4-segment form `spot:kline:{symbol}:{interval}`.
                // Snapshot the latest candle from spot_klines so a fresh subscriber
                // has state before the next live update arrives.
                let suffix = channel.strip_prefix("spot:kline:").unwrap_or("");
                let parts: Vec<&str> = suffix.split(':').collect();
                if parts.len() == 2 && !parts[0].is_empty() && !parts[1].is_empty() {
                    let symbol = parts[0].to_string();
                    let interval = parts[1].to_string();
                    if ["1m","5m","15m","1h","4h","1d"].contains(&interval.as_str()) {
                        let state_c = state.clone();
                        let tx_c = tx.clone();
                        let chan_c = channel.clone();
                        let sem = snapshot_sem();
                        tokio::spawn(async move {
                            let _permit = sem.acquire_owned().await.ok();
                            let row: Result<crate::models::spot::SpotKline, _> = sqlx::query_as(
                                "SELECT * FROM spot_klines WHERE market_id=$1 AND interval=$2
                                 ORDER BY open_time DESC LIMIT 1"
                            )
                            .bind(&symbol).bind(&interval)
                            .fetch_one(&state_c.db.pool).await;
                            if let Ok(k) = row {
                                let iv_secs: i64 = match interval.as_str() {
                                    "1m"=>60,"5m"=>300,"15m"=>900,"1h"=>3600,"4h"=>14400,"1d"=>86400,_=>0
                                };
                                let open_ts = k.open_time.timestamp();
                                let close_ts = open_ts + iv_secs - 1;
                                let payload = serde_json::json!({
                                    "type":    "spot_kline_snapshot",
                                    "channel": chan_c,
                                    "data": {
                                        "symbol":       k.market_id,
                                        "interval":     k.interval,
                                        "open_time":    open_ts,
                                        "close_time":   close_ts,
                                        "open":         k.open_price.normalize().to_string(),
                                        "high":         k.high_price.normalize().to_string(),
                                        "low":          k.low_price.normalize().to_string(),
                                        "close":        k.close_price.normalize().to_string(),
                                        "volume":       k.volume.normalize().to_string(),
                                        "quote_volume": k.quote_volume.normalize().to_string(),
                                        "trade_count":  k.trade_count,
                                        "is_closed":    chrono::Utc::now().timestamp() > close_ts,
                                    }
                                });
                                if let Ok(json) = serde_json::to_string(&payload) {
                                    let _ = tx_c.try_send(Message::Text(json));
                                }
                            }
                        });
                    }
                }
            }
        }

        ClientMessage::Unsubscribe { channel } => {
            subscriptions.remove(&channel);

            let response = ServerMessage::Unsubscribed { channel };
            let _ = tx.try_send(Message::Text(serde_json::to_string(&response).unwrap()));
        }

        ClientMessage::Ping => {
            let response = ServerMessage::Pong;
            let _ = tx.try_send(Message::Text(serde_json::to_string(&response).unwrap()));
        }

        ClientMessage::InternalTrade {
            symbol,
            price,
            amount,
            side,
            timestamp,
        } => {
            // Check if in development environment
            let is_dev = state.config.environment == "development" 
                || std::env::var("ENVIRONMENT").unwrap_or_default() == "development"
                || std::env::var("ENV").unwrap_or_default() == "development";

            if !is_dev {
                let response = ServerMessage::InternalTradeResult {
                    success: false,
                    trade_id: None,
                    symbol: None,
                    price: None,
                    amount: None,
                    side: None,
                    timestamp: None,
                    error: Some("Internal API only available in development environment".to_string()),
                };
                let _ = tx.try_send(Message::Text(serde_json::to_string(&response).unwrap()));
                return Ok(());
            }

            // Process internal trade
            match process_internal_trade(state, symbol.clone(), price.clone(), amount.clone(), side.clone(), timestamp).await {
                Ok(trade_id) => {
                    let ts = timestamp.unwrap_or_else(|| chrono::Utc::now().timestamp());
                    let response = ServerMessage::InternalTradeResult {
                        success: true,
                        trade_id: Some(trade_id),
                        symbol: Some(symbol),
                        price: Some(price),
                        amount: Some(amount),
                        side: Some(side),
                        timestamp: Some(ts),
                        error: None,
                    };
                    let _ = tx.try_send(Message::Text(serde_json::to_string(&response).unwrap()));
                }
                Err(e) => {
                    let response = ServerMessage::InternalTradeResult {
                        success: false,
                        trade_id: None,
                        symbol: None,
                        price: None,
                        amount: None,
                        side: None,
                        timestamp: None,
                        error: Some(e),
                    };
                    let _ = tx.try_send(Message::Text(serde_json::to_string(&response).unwrap()));
                }
            }
        }

        ClientMessage::InternalTradeBatch { trades } => {
            // Check if in development environment
            let is_dev = state.config.environment == "development" 
                || std::env::var("ENVIRONMENT").unwrap_or_default() == "development"
                || std::env::var("ENV").unwrap_or_default() == "development";

            if !is_dev {
                let response = ServerMessage::InternalTradeBatchResult {
                    success: false,
                    created: 0,
                    failed: trades.len(),
                };
                let _ = tx.try_send(Message::Text(serde_json::to_string(&response).unwrap()));
                return Ok(());
            }

            // Process batch trades
            let mut created = 0;
            let mut failed = 0;

            for trade in trades {
                match process_internal_trade(
                    state,
                    trade.symbol,
                    trade.price,
                    trade.amount,
                    trade.side,
                    trade.timestamp,
                )
                .await
                {
                    Ok(_) => created += 1,
                    Err(_) => failed += 1,
                }
            }

            let response = ServerMessage::InternalTradeBatchResult {
                success: created > 0,
                created,
                failed,
            };
            let _ = tx.try_send(Message::Text(serde_json::to_string(&response).unwrap()));
        }
    }

    Ok(())
}

/// Process internal trade (helper function for WebSocket)
async fn process_internal_trade(
    state: &Arc<AppState>,
    symbol: String,
    price: String,
    amount: String,
    side: String,
    timestamp: Option<i64>,
) -> Result<String, String> {
    use uuid::Uuid;

    // Validate side
    if side != "buy" && side != "sell" {
        return Err("Side must be 'buy' or 'sell'".to_string());
    }

    // Parse price and amount
    let price_f64: f64 = price.parse().map_err(|_| "Invalid price format".to_string())?;
    let amount_f64: f64 = amount.parse().map_err(|_| "Invalid amount format".to_string())?;

    if price_f64 <= 0.0 {
        return Err("Price must be positive".to_string());
    }
    if amount_f64 <= 0.0 {
        return Err("Amount must be positive".to_string());
    }

    let trade_id = Uuid::new_v4().to_string();
    let ts = timestamp.unwrap_or_else(|| chrono::Utc::now().timestamp());
    let normalized_symbol = normalize_symbol(&symbol);

    // Store trade in database
    let query = r#"
        INSERT INTO trades (id, symbol, price, amount, side, trade_time, created_at)
        VALUES ($1, $2, $3, $4, $5, to_timestamp($6), NOW())
    "#;

    sqlx::query(query)
        .bind(&trade_id)
        .bind(&normalized_symbol)
        .bind(price_f64)
        .bind(amount_f64)
        .bind(&side)
        .bind(ts)
        .execute(&state.db.pool)
        .await
        .map_err(|e| format!("Database error: {}", e))?;

    // Update price cache
    if let Some(price_cache) = state.cache.price_opt() {
        use rust_decimal::Decimal;
        use std::str::FromStr;
        if let Ok(price_decimal) = Decimal::from_str(&price_f64.to_string()) {
            let _ = price_cache.set_last_price(&normalized_symbol, price_decimal).await;
        }
    }

    // Publish trade to subscribers
    let trade_data = serde_json::json!({
        "type": "trade",
        "symbol": normalized_symbol,
        "price": price,
        "amount": amount,
        "side": side,
        "timestamp": ts * 1000,
    });

    let channel = format!("trades:{}", normalized_symbol);
    if let Some(pubsub) = state.cache.pubsub_opt() {
        let _ = pubsub.publisher().publish(&channel, &trade_data.to_string()).await;
    }

    // Update kline
    let kline_time = (ts / 60) * 60;
    let kline_query = r#"
        INSERT INTO historical_klines (symbol, period, time, open, high, low, close, volume, is_final)
        VALUES ($1, '1m', to_timestamp($2), $3, $3, $3, $3, $4, false)
        ON CONFLICT (symbol, period, time) 
        DO UPDATE SET
            high = GREATEST(historical_klines.high, EXCLUDED.high),
            low = LEAST(historical_klines.low, EXCLUDED.low),
            close = EXCLUDED.close,
            volume = historical_klines.volume + EXCLUDED.volume,
            updated_at = NOW()
    "#;

    let _ = sqlx::query(kline_query)
        .bind(&normalized_symbol)
        .bind(kline_time)
        .bind(price_f64)
        .bind(amount_f64)
        .execute(&state.db.pool)
        .await;

    tracing::info!("WebSocket internal trade: {} {} {} @ {}", side, amount_f64, normalized_symbol, price_f64);

    Ok(trade_id)
}

/// Fetch user positions from database
async fn fetch_user_positions(state: &Arc<AppState>, address: &str) -> Result<Vec<ServerMessage>, sqlx::Error> {
    let rows: Vec<(String, String, String, Decimal, Decimal, Decimal, i32, chrono::DateTime<chrono::Utc>)> = sqlx::query_as(
        r#"
        SELECT id::text, symbol, side::text, size_in_usd, entry_price, collateral_amount, leverage, updated_at
        FROM positions
        WHERE user_address = $1 AND status = 'open'
        "#
    )
    .bind(address)
    .fetch_all(&state.db.pool)
    .await?;

    let mut messages = Vec::new();
    for (id, symbol, side, size, entry_price, collateral, leverage, updated_at) in rows {
        // Get mark price
        let mark_price = state.price_feed_service
            .get_mark_price(&symbol)
            .await
            .unwrap_or(entry_price);

        // Calculate unrealized PnL
        let is_long = side.to_lowercase() == "long";
        let unrealized_pnl = if is_long {
            (mark_price - entry_price) * size
        } else {
            (entry_price - mark_price) * size
        };

        // Calculate liquidation price
        let position_value = size * entry_price;
        let maintenance_margin = position_value * Decimal::new(5, 3);
        let liq_distance = (collateral - maintenance_margin) / size;
        let liquidation_price = if is_long {
            entry_price - liq_distance
        } else {
            entry_price + liq_distance
        };

        messages.push(ServerMessage::Position {
            id,
            symbol,
            side,
            size: size.to_string(),
            entry_price: entry_price.to_string(),
            mark_price: mark_price.to_string(),
            liquidation_price: liquidation_price.max(Decimal::ZERO).to_string(),
            unrealized_pnl: unrealized_pnl.to_string(),
            leverage,
            margin: collateral.to_string(),
            updated_at: updated_at.timestamp_millis(),
            event: None, // Event is set when position state changes
        });
    }

    Ok(messages)
}

/// Fetch user balances from database
async fn fetch_user_balances(state: &Arc<AppState>, address: &str) -> Result<Vec<ServerMessage>, sqlx::Error> {
    let rows: Vec<(String, Decimal, Decimal)> = sqlx::query_as(
        "SELECT token, available, frozen FROM balances WHERE user_address = $1"
    )
    .bind(address)
    .fetch_all(&state.db.pool)
    .await?;

    let messages: Vec<ServerMessage> = rows
        .into_iter()
        .map(|(token, available, frozen)| {
            // Get symbol from config if possible, otherwise use token address
            let symbol = state.config.get_token_symbol(&token)
                .map(|s| s.to_string())
                .unwrap_or_else(|| token.clone());

            ServerMessage::Balance {
                token,
                symbol,
                available: available.to_string(),
                frozen: frozen.to_string(),
                total: (available + frozen).to_string(),
            }
        })
        .collect();

    Ok(messages)
}

/// Fetch user open orders from database
async fn fetch_user_orders(state: &Arc<AppState>, address: &str) -> Result<Vec<ServerMessage>, sqlx::Error> {
    let rows: Vec<(String, String, String, String, Option<Decimal>, Decimal, Decimal, String, chrono::DateTime<chrono::Utc>)> = sqlx::query_as(
        r#"
        SELECT id::text, symbol, side, order_type, price, amount, filled_amount, status, updated_at
        FROM orders
        WHERE user_address = $1 AND status IN ('open', 'pending', 'partially_filled')
        ORDER BY created_at DESC
        LIMIT 50
        "#
    )
    .bind(address)
    .fetch_all(&state.db.pool)
    .await?;

    let messages: Vec<ServerMessage> = rows
        .into_iter()
        .map(|(id, symbol, side, order_type, price, amount, filled_amount, status, updated_at)| {
            ServerMessage::Order {
                id,
                symbol,
                side,
                order_type,
                price: price.map(|p| p.to_string()),
                amount: amount.to_string(),
                filled_amount: filled_amount.to_string(),
                status,
                updated_at: updated_at.timestamp_millis(),
                event: None, // Event is set when order state changes
            }
        })
        .collect();

    Ok(messages)
}
