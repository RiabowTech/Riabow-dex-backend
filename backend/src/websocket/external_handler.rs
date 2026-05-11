use axum::extract::ws::{Message, WebSocket};
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message as TungsteniteMessage, MaybeTlsStream, WebSocketStream};
use reqwest::Url;
use std::sync::OnceLock;
use dashmap::DashMap;

use tracing::Instrument;
use crate::AppState;
use crate::websocket::handler::{ServerMessage, OrderbookLevel, KlineData}; // Reuse existing types

const HYPERLIQUID_WS_URL: &str = "wss://api.hyperliquid.xyz/ws";
const HYPERLIQUID_API_URL: &str = "https://api.hyperliquid.xyz/info";

// Global cache for market statistics
static MARKET_STATS_CACHE: OnceLock<DashMap<String, MarketStats>> = OnceLock::new();
// Global cache for spot market statistics (key: "SPOT:{token_name}", e.g. "SPOT:TSLA")
static SPOT_STATS_CACHE: OnceLock<DashMap<String, MarketStats>> = OnceLock::new();
static FETCHER_STARTED: OnceLock<bool> = OnceLock::new();

#[derive(Debug, Clone, Default)]
pub struct MarketStats {
    pub prev_day_px: f64,
    pub day_ntl_vlm: f64, // Volume USD
    pub day_base_vlm: f64, // Volume Base
    pub open_interest: f64,
    pub funding_rate: f64,
    pub mark_px: f64,
}

pub fn get_stats_cache() -> &'static DashMap<String, MarketStats> {
    MARKET_STATS_CACHE.get_or_init(|| DashMap::new())
}

pub fn get_spot_stats_cache() -> &'static DashMap<String, MarketStats> {
    SPOT_STATS_CACHE.get_or_init(|| DashMap::new())
}

// get_cached_ticker / get_hyperliquid_mark_prices / is_plausible_rwa_spot_price
// were removed: they let HL stats cache feed user-facing tickers and
// PriceCache/KLine. Tickers must come solely from the matching engine's own
// trades (PriceCache via `update_price_from_trade`); HL data lives only in
// the explicit /ws/external channel which clients opt into.

/// Start the Hyperliquid market data fetcher proactively
// start_hyperliquid_fetcher() removed alongside the price-sync worker. The
// fetcher now lazy-starts the first time a client opens /ws/external (see
// FETCHER_STARTED in handle_external_socket below). Cold-start clients pay
// the ~5s populate latency themselves; nothing else depends on this cache.

async fn fetch_market_data_loop() {
    let client = reqwest::Client::new();
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5));
    
    tracing::info!("🚀 Starting background market data fetcher");

    loop {
        interval.tick().await;

        match client.post(HYPERLIQUID_API_URL)
            .json(&json!({"type": "metaAndAssetCtxs"}))
            .send()
            .await 
        {
            Ok(resp) => {
                if let Ok(data) = resp.json::<serde_json::Value>().await {
                   if let (Some(universe), Some(ctxs)) = (data[0].get("universe").and_then(|v| v.as_array()), data[1].as_array()) {
                        if universe.len() == ctxs.len() {
                            let cache = get_stats_cache();
                            let mut updated_count = 0;
                            for (i, asset_info) in universe.iter().enumerate() {
                                if let Some(name) = asset_info["name"].as_str() {
                                    let ctx = &ctxs[i];
                                    let stats = MarketStats {
                                        prev_day_px: ctx["prevDayPx"].as_str().unwrap_or("0").parse().unwrap_or(0.0),
                                        day_ntl_vlm: ctx["dayNtlVlm"].as_str().unwrap_or("0").parse().unwrap_or(0.0),
                                        day_base_vlm: ctx["dayBaseVlm"].as_str().unwrap_or("0").parse().unwrap_or(0.0),
                                        open_interest: ctx["openInterest"].as_str().unwrap_or("0").parse().unwrap_or(0.0),
                                        funding_rate: ctx["funding"].as_str().unwrap_or("0").parse().unwrap_or(0.0),
                                        mark_px: ctx["markPx"].as_str().unwrap_or("0").parse().unwrap_or(0.0),
                                    };
                                    cache.insert(name.to_string(), stats);
                                    updated_count += 1;
                                }
                            }
                            tracing::debug!("✅ Updated market stats for {} assets", updated_count);
                        }
                   }
                }
            }
            Err(e) => {
                tracing::warn!("❌ Failed to fetch market data from Hyperliquid: {}", e);
            }
        }

        // Fetch spot market data (for RWA assets like TSLA, AMZN, SLV, etc.)
        match client.post(HYPERLIQUID_API_URL)
            .json(&json!({"type": "spotMetaAndAssetCtxs"}))
            .send()
            .await
        {
            Ok(resp) => {
                if let Ok(data) = resp.json::<serde_json::Value>().await {
                    if let (Some(meta), Some(ctxs)) = (data[0].as_object(), data[1].as_array()) {
                        let tokens = meta.get("tokens").and_then(|v| v.as_array());
                        let universe = meta.get("universe").and_then(|v| v.as_array());
                        if let (Some(tokens), Some(universe)) = (tokens, universe) {
                            // Build token index: token_index -> token_name
                            let token_map: std::collections::HashMap<u64, String> = tokens.iter().filter_map(|t| {
                                let idx = t["index"].as_u64()?;
                                let name = t["name"].as_str()?.to_string();
                                Some((idx, name))
                            }).collect();

                            // Find USDC token index (to only cache USDC-quoted pairs)
                            let usdc_idx = tokens.iter().find_map(|t| {
                                if t["name"].as_str() == Some("USDC") { t["index"].as_u64() } else { None }
                            });

                            let spot_cache = get_spot_stats_cache();
                            let mut spot_count = 0;
                            for (i, pair) in universe.iter().enumerate() {
                                if i >= ctxs.len() { break; }
                                let toks = pair["tokens"].as_array();
                                if let Some(toks) = toks {
                                    if toks.len() >= 2 {
                                        let base_idx = toks[0].as_u64().unwrap_or(0);
                                        let quote_idx = toks[1].as_u64().unwrap_or(0);
                                        // Only cache USDC-quoted pairs for USD-denominated prices
                                        if usdc_idx.is_some() && Some(quote_idx) != usdc_idx {
                                            continue;
                                        }
                                        if let Some(base_name) = token_map.get(&base_idx) {
                                            let ctx = &ctxs[i];
                                            let mark_px: f64 = ctx["markPx"].as_str().unwrap_or("0").parse().unwrap_or(0.0);
                                            if mark_px > 0.0 {
                                                let stats = MarketStats {
                                                    prev_day_px: ctx["prevDayPx"].as_str().unwrap_or("0").parse().unwrap_or(0.0),
                                                    day_ntl_vlm: ctx["dayNtlVlm"].as_str().unwrap_or("0").parse().unwrap_or(0.0),
                                                    day_base_vlm: 0.0,
                                                    open_interest: 0.0,
                                                    funding_rate: 0.0,
                                                    mark_px,
                                                };
                                                spot_cache.insert(base_name.clone(), stats);
                                                spot_count += 1;
                                            }
                                        }
                                    }
                                }
                            }
                            tracing::debug!("✅ Updated spot stats for {} assets", spot_count);
                        }
                    }
                }
            }
            Err(e) => {
                tracing::warn!("❌ Failed to fetch spot data from Hyperliquid: {}", e);
            }
        }
    }
}

/// Normalize symbol format to Hyperliquid format (BTCUSDT -> BTC)
fn to_hl_symbol(symbol: &str) -> String {
    symbol.to_uppercase().replace("USDT", "").replace("USD", "")
}

/// Helper to convert Hyperliquid symbol back to internal format
fn from_hl_symbol(symbol: &str) -> String {
    format!("{}USDT", symbol)
}

#[derive(Debug, Deserialize)]
struct HlMessage {
    channel: String,
    // `data` 对多数业务 channel 必带,但 HL 的 pong 帧是 `{"channel":"pong"}` 没 data,
    // 标 default 让它解析成 Null,避免每 30s 刷一条 WARN。真正依赖 data 的分支(l2Book/trades/...)
    // 自己会再往里面 index,缺字段就是 Value::Null,分支内的字段解析会自然失败,不会往下走出事。
    #[serde(default)]
    data: serde_json::Value,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum ClientMessage {
    Subscribe { channel: String },
    Unsubscribe { channel: String },
    Ping,
}

pub async fn handle_external_socket(socket: WebSocket, _state: Arc<AppState>) {
    tracing::info!("🌐 External WebSocket connection established");
    
    // Ensure background fetcher is running
    FETCHER_STARTED.get_or_init(|| {
        tokio::spawn(fetch_market_data_loop().instrument(tracing::info_span!("hyperliquid-fetcher")));
        true
    });
    
    let (mut client_sender, mut client_receiver) = socket.split();

    // Send initial connection success message
    let welcome_msg = json!({
        "type": "connected",
        "message": "Connected to external data feed (Hyperliquid)",
        "timestamp": chrono::Utc::now().timestamp_millis()
    });
    if let Err(e) = client_sender.send(Message::Text(serde_json::to_string(&welcome_msg).unwrap())).await {
        tracing::error!("Failed to send welcome message: {}", e);
        return;
    }

    // Connect to Hyperliquid WS
    tracing::info!("🔌 Connecting to Hyperliquid WebSocket: {}", HYPERLIQUID_WS_URL);
    let (hl_stream, _) : (WebSocketStream<MaybeTlsStream<TcpStream>>, _) = match connect_async(Url::parse(HYPERLIQUID_WS_URL).unwrap()).await {
        Ok(s) => {
            tracing::info!("✅ Successfully connected to Hyperliquid WebSocket");
            s
        },
        Err(e) => {
            tracing::error!("❌ Failed to connect to Hyperliquid WS: {}", e);
            let error_msg = ServerMessage::Error {
                code: "HL_CONNECTION_FAILED".to_string(),
                message: format!("Failed to connect to Hyperliquid: {}", e),
            };
            let _ = client_sender.send(Message::Text(serde_json::to_string(&error_msg).unwrap())).await;
            let _ = client_sender.close().await;
            return;
        }
    };
    
    let (mut hl_sender, mut hl_receiver) = hl_stream.split();
    let (tx, mut rx) = mpsc::channel::<TungsteniteMessage>(100);

    // Forwarder: External -> Client
    let mut subscriptions = HashSet::new();
    
    // Spawn a task to write to HL WS to avoid borrowing issues
    let hl_write_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            tracing::info!("📤 Sending to Hyperliquid: {}", match &msg {
                TungsteniteMessage::Text(t) => t.clone(),
                _ => format!("{:?}", msg),
            });
            if let Err(e) = hl_sender.send(msg).await {
                tracing::error!("Error sending to HL WS: {}", e);
                break;
            }
            tracing::info!("📤 Message sent to Hyperliquid successfully");
        }
        tracing::warn!("⚠️ HL write task ending - no more messages to send");
    }.instrument(tracing::info_span!("hyperliquid-ws-writer")));

    // Heartbeat interval (ping every 30 seconds)
    let mut heartbeat_interval = tokio::time::interval(tokio::time::Duration::from_secs(30));

    loop {
        tokio::select! {
            // Client (Frontend) -> Backend
            msg = client_receiver.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        tracing::debug!("📨 Received from client: {}", text);
                        if let Ok(client_msg) = serde_json::from_str::<ClientMessage>(&text) {
                            match client_msg {
                                ClientMessage::Subscribe { channel } => {
                                    tracing::info!("📡 External WS Subscribe request: {}", channel);
                                    subscriptions.insert(channel.clone());
                                    
                                    // Parse channel and forward subscription to HL
                                    if channel.starts_with("orderbook:") {
                                        let symbol = channel.strip_prefix("orderbook:").unwrap_or("");
                                        let hl_symbol = to_hl_symbol(symbol);
                                        tracing::info!("🔄 Subscribing to Hyperliquid l2Book for {}", hl_symbol);
                                        let sub_msg = json!({
                                            "method": "subscribe",
                                            "subscription": {
                                                "type": "l2Book",
                                                "coin": hl_symbol
                                            }
                                        });
                                        let _ = tx.send(TungsteniteMessage::Text(sub_msg.to_string())).await;
                                    } else if channel.starts_with("trades:") {
                                        let symbol = channel.strip_prefix("trades:").unwrap_or("");
                                        let hl_symbol = to_hl_symbol(symbol);
                                        tracing::info!("🔄 Subscribing to Hyperliquid trades for {}", hl_symbol);
                                        let sub_msg = json!({
                                            "method": "subscribe",
                                            "subscription": {
                                                "type": "trades",
                                                "coin": hl_symbol
                                            }
                                        });
                                        let _ = tx.send(TungsteniteMessage::Text(sub_msg.to_string())).await;
                                    } else if channel.starts_with("ticker:") {
                                        // HL doesn't have a direct ticker channel per coin, usually we derive it from l2Book
                                        // For simplicity, we subscribe to l2Book and will synth ticker events
                                        let symbol = channel.strip_prefix("ticker:").unwrap_or("");
                                        let hl_symbol = to_hl_symbol(symbol);
                                        tracing::info!("🔄 Subscribing to Hyperliquid l2Book (for ticker) for {}", hl_symbol);
                                        let sub_msg = json!({
                                            "method": "subscribe",
                                            "subscription": {
                                                "type": "l2Book",
                                                "coin": hl_symbol
                                            }
                                        });
                                        let sub_msg_str = sub_msg.to_string();
                                        tracing::info!("📨 Queuing subscription message: {}", sub_msg_str);
                                        match tx.send(TungsteniteMessage::Text(sub_msg_str)).await {
                                            Ok(_) => tracing::info!("✅ Subscription message queued successfully for {}", hl_symbol),
                                            Err(e) => tracing::error!("❌ Failed to queue subscription message: {}", e),
                                        }
                                    } else if channel.starts_with("kline:") {
                                        // Parse kline channel: kline:BTCUSDT:1m
                                        let parts: Vec<&str> = channel.strip_prefix("kline:").unwrap_or("").split(':').collect();
                                        if parts.len() == 2 {
                                            let symbol = parts[0];
                                            let interval = parts[1];
                                            let hl_symbol = to_hl_symbol(symbol);
                                            tracing::info!("🔄 Subscribing to Hyperliquid candles for {} ({})", hl_symbol, interval);
                                            let sub_msg = json!({
                                                "method": "subscribe",
                                                "subscription": {
                                                    "type": "candle",
                                                    "coin": hl_symbol,
                                                    "interval": interval
                                                }
                                            });
                                            let _ = tx.send(TungsteniteMessage::Text(sub_msg.to_string())).await;
                                        }
                                    }
                                    
                                    // Ack subscription
                                    let resp = ServerMessage::Subscribed { channel };
                                    tracing::info!("✅ Subscription confirmed: {:?}", resp);
                                    let _ = client_sender.send(Message::Text(serde_json::to_string(&resp).unwrap())).await;
                                }
                                ClientMessage::Unsubscribe { channel } => {
                                    tracing::info!("📡 External WS Unsubscribe request: {}", channel);
                                    subscriptions.remove(&channel);
                                    let resp = ServerMessage::Unsubscribed { channel };
                                    let _ = client_sender.send(Message::Text(serde_json::to_string(&resp).unwrap())).await;
                                }
                                ClientMessage::Ping => {
                                    tracing::debug!("🏓 Ping received, sending Pong");
                                    let _ = client_sender.send(Message::Pong(vec![])).await;
                                }
                            }
                        } else {
                            tracing::warn!("⚠️ Failed to parse client message: {}", text);
                        }
                    }
                    Some(Ok(Message::Ping(data))) => {
                        tracing::debug!("🏓 Ping frame received");
                        let _ = client_sender.send(Message::Pong(data)).await;
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        tracing::info!("👋 Client disconnected");
                        break;
                    }
                    Some(Err(e)) => {
                        tracing::warn!("⚠️ WebSocket error: {}", e);
                        break;
                    }
                    _ => {}
                }
            }
            
            // Backend (HL) -> Client (Frontend)
            msg = hl_receiver.next() => {
                match msg {
                    Some(Ok(TungsteniteMessage::Text(text))) => {
                        tracing::info!("📥 Received from Hyperliquid: {} bytes", text.len());

                        // Parse HL message and convert to internal format
                        if let Ok(hl_msg) = serde_json::from_str::<HlMessage>(&text) {
                            tracing::info!("📊 Hyperliquid message channel: {}", hl_msg.channel);
                            
                            if hl_msg.channel == "l2Book" {
                                // Handle L2 Book
                                if let Some(coin) = hl_msg.data["coin"].as_str() {
                                    let symbol = from_hl_symbol(coin);
                                    let orderbook_channel = format!("orderbook:{}", symbol);
                                    let ticker_channel = format!("ticker:{}", symbol);

                                    let has_orderbook_sub = subscriptions.contains(&orderbook_channel);
                                    let has_ticker_sub = subscriptions.contains(&ticker_channel);

                                    tracing::info!("📖 L2 Book update for {} (orderbook: {}, ticker: {})", symbol, has_orderbook_sub, has_ticker_sub);

                                    // Process if either orderbook or ticker is subscribed
                                    if has_orderbook_sub || has_ticker_sub {
                                        // Convert L2
                                        if let Some(levels) = hl_msg.data["levels"].as_array() {
                                            if levels.len() >= 2 {
                                                let bids: Vec<OrderbookLevel> = levels[0].as_array().unwrap_or(&vec![]).iter().take(20).filter_map(|l| {
                                                    Some(OrderbookLevel {
                                                        price: l["px"].as_str()?.to_string(),
                                                        size: l["sz"].as_str()?.to_string(),
                                                    })
                                                }).collect();

                                                let asks: Vec<OrderbookLevel> = levels[1].as_array().unwrap_or(&vec![]).iter().take(20).filter_map(|l| {
                                                    Some(OrderbookLevel {
                                                        price: l["px"].as_str()?.to_string(),
                                                        size: l["sz"].as_str()?.to_string(),
                                                    })
                                                }).collect();

                                                // Send orderbook if subscribed.
                                                // Skip frames where both sides are empty: when filter_map
                                                // drops all levels (malformed `px`/`sz` from upstream),
                                                // forwarding a `{bids:[], asks:[]}` causes the frontend
                                                // depth bars to flash to "--". Symmetric with the
                                                // matching-engine forwarder in handler.rs.
                                                if has_orderbook_sub && !(bids.is_empty() && asks.is_empty()) {
                                                    tracing::info!("📤 Sending orderbook to client: {} (bids: {}, asks: {})", symbol, bids.len(), asks.len());

                                                    let msg = ServerMessage::Orderbook {
                                                        symbol: symbol.clone(),
                                                        bids: bids.clone(),
                                                        asks: asks.clone(),
                                                        timestamp: chrono::Utc::now().timestamp_millis(),
                                                    };
                                                    let _ = client_sender.send(Message::Text(serde_json::to_string(&msg).unwrap())).await;
                                                }

                                                // Synthesize Ticker if subscribed
                                                if has_ticker_sub {
                                                    // Get 24h stats from global cache
                                                    let cache = get_stats_cache();
                                                    let hl_symbol = to_hl_symbol(&symbol); // e.g. BTC
                                                    let stats = cache.get(&hl_symbol);

                                                    let best_bid = bids.first().map(|l| l.price.clone()).unwrap_or("0".to_string());
                                                    let best_ask = asks.first().map(|l| l.price.clone()).unwrap_or("0".to_string());
                                                    let last_price_str = best_bid.clone();
                                                    let last_price = last_price_str.parse::<f64>().unwrap_or(0.0);

                                                    let (prev_day, vol_usd_raw, vol_base_raw, oi, funding, high, low) = if let Some(s) = stats {
                                                        (s.prev_day_px, s.day_ntl_vlm, s.day_base_vlm, s.open_interest, s.funding_rate, s.mark_px * 1.05, s.mark_px * 0.95) // Mock high/low for now
                                                    } else {
                                                        (last_price, 0.0, 0.0, 0.0, 0.0, last_price, last_price)
                                                    };

                                                    // Optimize volume display to around 15m (scaled down from external source ~3.2b)
                                                    // User request: "daily turnover around 15 million"
                                                    let scale_factor = 0.01;
                                                    let vol_usd = vol_usd_raw * scale_factor;
                                                    let vol_base = vol_base_raw * scale_factor;

                                                    let change_24h = last_price - prev_day;
                                                    let change_pct = if prev_day != 0.0 { change_24h / prev_day * 100.0 } else { 0.0 };

                                                    let ticker_msg = ServerMessage::Ticker {
                                                        symbol: symbol.clone(),
                                                        last_price: last_price_str,
                                                        mark_price: best_ask, // Approx
                                                        index_price: best_bid, // Approx
                                                        price_change_24h: format!("{:.2}", change_24h),
                                                        price_change_percent_24h: format!("{:.2}%", change_pct),
                                                        high_24h: format!("{:.2}", high), // Approximation
                                                        low_24h: format!("{:.2}", low), // Approximation
                                                        volume_24h: format!("{:.4}", vol_base),
                                                        volume_24h_usd: format!("{:.2}", vol_usd),
                                                        open_interest_long: format!("{:.4}", oi), // Use total OI for long
                                                        open_interest_short: format!("{:.4}", oi), // Use total OI for short
                                                        open_interest_long_percent: "50".to_string(), // Balanced
                                                        open_interest_short_percent: "50".to_string(),
                                                        available_liquidity_long: format!("{:.2}", vol_usd * 0.1), // Mock connection to liquidity
                                                        available_liquidity_short: format!("{:.2}", vol_usd * 0.1),
                                                        funding_rate_long_1h: format!("{:.6}%", funding * 100.0), // HL funding is 1h usually
                                                        funding_rate_short_1h: format!("{:.6}%", -funding * 100.0),
                                                    };
                                                    tracing::info!("📤 Sending ticker to client: {} @ {}", symbol, last_price);
                                                    let _ = client_sender.send(Message::Text(serde_json::to_string(&ticker_msg).unwrap())).await;
                                                }
                                            }
                                        }
                                    } else {
                                        tracing::debug!("⏭️ Skipping L2 update - not subscribed to {} or {}", orderbook_channel, ticker_channel);
                                    }
                                }
                            } else if hl_msg.channel == "trades" {
                                // Handle Trades
                                if let Some(trades) = hl_msg.data.as_array() {
                                    tracing::debug!("💱 Received {} trades from Hyperliquid", trades.len());
                                    
                                    // Trades array: [{"coin": "BTC", "side": "B", "px": "...", "sz": "...", "time": ...}]
                                    for trade in trades {
                                        if let Some(coin) = trade["coin"].as_str() {
                                            let symbol = from_hl_symbol(coin);
                                            let internal_channel = format!("trades:{}", symbol);
                                            
                                            if subscriptions.contains(&internal_channel) {
                                                let price = trade["px"].as_str().unwrap_or("0").to_string();
                                                let msg = ServerMessage::Trade {
                                                    id: format!("{}", chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)),
                                                    symbol: symbol.clone(),
                                                    price: price.clone(),
                                                    amount: trade["sz"].as_str().unwrap_or("0").to_string(),
                                                    side: if trade["side"].as_str() == Some("B") { "buy".to_string() } else { "sell".to_string() },
                                                    timestamp: trade["time"].as_i64().unwrap_or(0),
                                                };
                                                tracing::info!("📤 Sending trade to client: {} @ {}", symbol, price);
                                                let _ = client_sender.send(Message::Text(serde_json::to_string(&msg).unwrap())).await;
                                            }
                                        }
                                    }
                                }
                            } else if hl_msg.channel == "candle" {
                                // Handle K-line/Candle data
                                tracing::debug!("🕯️ Received candle from Hyperliquid");
                                
                                // hl_msg.data is the candle object
                                let c = &hl_msg.data;
                                if let (Some(t), Some(o), Some(h), Some(l), Some(close), Some(v), Some(s), Some(i)) = (
                                    c["t"].as_i64(),
                                    c["o"].as_str(),
                                    c["h"].as_str(),
                                    c["l"].as_str(),
                                    c["c"].as_str(),
                                    c["v"].as_str(),
                                    c["s"].as_str(),
                                    c["i"].as_str(),
                                ) {
                                    let symbol = from_hl_symbol(s);
                                    let channel_name = format!("kline:{}:{}", symbol, i);
                                    
                                    // Check subscription
                                    if subscriptions.contains(&channel_name) {
                                        let msg = ServerMessage::Kline {
                                            channel: channel_name,
                                            data: KlineData {
                                                time: t,
                                                open: o.to_string(),
                                                high: h.to_string(),
                                                low: l.to_string(),
                                                close: close.to_string(),
                                                volume: v.to_string(),
                                                quote_volume: None, // HL doesn't provide quote volume directly in this update
                                                trade_count: c["n"].as_u64().map(|n| n as u32),
                                                is_final: false, // Streaming updates are usually not final until new interval starts
                                            },
                                        };
                                        tracing::info!("📤 Sending kline to client: {} Open={} Close={}", symbol, o, close);
                                        let _ = client_sender.send(Message::Text(serde_json::to_string(&msg).unwrap())).await;
                                    }
                                }
                            } else {
                                tracing::debug!("📦 Unknown Hyperliquid channel: {}", hl_msg.channel);
                            }
                        } else {
                            // Not a structured message, might be subscription confirmation
                            tracing::warn!("📨 Failed to parse Hyperliquid message: {}", &text[..text.len().min(200)]);
                        }
                    }
                    Some(Ok(TungsteniteMessage::Ping(d))) => {
                        tracing::debug!("🏓 Ping from Hyperliquid");
                        let _ = tx.send(TungsteniteMessage::Pong(d)).await;
                    }
                    Some(Err(e)) => {
                        tracing::error!("❌ Hyperliquid WS Error: {}", e);
                        let error_msg = ServerMessage::Error {
                            code: "HL_ERROR".to_string(),
                            message: format!("Hyperliquid connection error: {}", e),
                        };
                        let _ = client_sender.send(Message::Text(serde_json::to_string(&error_msg).unwrap())).await;
                        break;
                    }
                    None => {
                        tracing::warn!("⚠️ Hyperliquid connection closed");
                        let error_msg = ServerMessage::Error {
                            code: "HL_DISCONNECTED".to_string(),
                            message: "Hyperliquid connection closed".to_string(),
                        };
                        let _ = client_sender.send(Message::Text(serde_json::to_string(&error_msg).unwrap())).await;
                        break;
                    }
                    _ => {}
                }
            }
            
            // Heartbeat
            _ = heartbeat_interval.tick() => {
                tracing::debug!("💓 Sending heartbeat ping");
                if let Err(e) = client_sender.send(Message::Ping(vec![])).await {
                    tracing::warn!("Failed to send heartbeat: {}", e);
                    break;
                }
                // 同时给 HL 上游发保活 ping。之前只 ping 客户端,上行链路无人主动打,
                // HL / 中间 NAT 在 ~60s 静默后会 RST,表现为 "Connection reset without closing handshake"
                // (tokio-tungstenite 原生错误),被这里的 Some(Err(_)) 分支捕获后前端就收到 HL_ERROR。
                // HL 文档推荐的 ping 形式是 text `{"method":"ping"}`,server 回 `{"channel":"pong"}`。
                if let Err(e) = tx.send(TungsteniteMessage::Text("{\"method\":\"ping\"}".to_string())).await {
                    tracing::warn!("Failed to enqueue HL heartbeat ping: {}", e);
                    break;
                }
            }
        }
    }
    
    // Cleanup
    tracing::info!("🧹 Cleaning up external WebSocket connection");
    hl_write_task.abort();
    let _ = client_sender.close().await;
}

