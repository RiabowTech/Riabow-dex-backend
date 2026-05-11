//! K-Line (Candlestick) Aggregation Service
//!
//! High-performance K-line service that aggregates candlesticks from trade events.
//!
//! Architecture:
//! - Uses lock-free data structures for high throughput
//! - Real-time aggregation from matching engine trades
//! - Multi-period support (1m, 5m, 15m, 1h, 4h, 1d, 1w, 1M)
//! - WebSocket broadcast for real-time updates
//! - In-memory cache with optional persistence

use chrono::{Duration, Utc};
use dashmap::DashMap;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::Arc;
use futures::StreamExt;
use tokio::sync::broadcast;
use tokio::sync::RwLock;
use sqlx::PgPool;
use std::str::FromStr;

use crate::constants::channels;
use crate::services::matching::TradeEvent;

/// Database row for K-line query
#[derive(Debug, sqlx::FromRow)]
struct DbKlineRow {
    #[allow(dead_code)]
    symbol: String,
    open_time: chrono::DateTime<Utc>,
    open: Decimal,
    high: Decimal,
    low: Decimal,
    close: Decimal,
    volume: Decimal,
    quote_volume: Option<Decimal>,
    trade_count: Option<i32>,
}

/// K-line period enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum KlinePeriod {
    #[serde(rename = "1m")]
    Min1,
    #[serde(rename = "5m")]
    Min5,
    #[serde(rename = "15m")]
    Min15,
    #[serde(rename = "1h")]
    Hour1,
    #[serde(rename = "4h")]
    Hour4,
    #[serde(rename = "1d")]
    Day1,
    #[serde(rename = "1w")]
    Week1,
    #[serde(rename = "1M")]
    Month1,
}

impl KlinePeriod {
    /// Get period duration in seconds
    pub fn seconds(&self) -> i64 {
        match self {
            KlinePeriod::Min1 => 60,
            KlinePeriod::Min5 => 300,
            KlinePeriod::Min15 => 900,
            KlinePeriod::Hour1 => 3600,
            KlinePeriod::Hour4 => 14400,
            KlinePeriod::Day1 => 86400,
            KlinePeriod::Week1 => 604800,
            KlinePeriod::Month1 => 2592000, // 30 days
        }
    }

    /// Parse period from string
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "1m" => Some(KlinePeriod::Min1),
            "5m" => Some(KlinePeriod::Min5),
            "15m" => Some(KlinePeriod::Min15),
            "1h" => Some(KlinePeriod::Hour1),
            "4h" => Some(KlinePeriod::Hour4),
            "1d" => Some(KlinePeriod::Day1),
            "1w" => Some(KlinePeriod::Week1),
            "1M" => Some(KlinePeriod::Month1),
            _ => None,
        }
    }

    /// Convert to string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            KlinePeriod::Min1 => "1m",
            KlinePeriod::Min5 => "5m",
            KlinePeriod::Min15 => "15m",
            KlinePeriod::Hour1 => "1h",
            KlinePeriod::Hour4 => "4h",
            KlinePeriod::Day1 => "1d",
            KlinePeriod::Week1 => "1w",
            KlinePeriod::Month1 => "1M",
        }
    }

    /// Get all periods
    pub fn all() -> Vec<KlinePeriod> {
        vec![
            KlinePeriod::Min1,
            KlinePeriod::Min5,
            KlinePeriod::Min15,
            KlinePeriod::Hour1,
            KlinePeriod::Hour4,
            KlinePeriod::Day1,
            KlinePeriod::Week1,
            KlinePeriod::Month1,
        ]
    }
}

/// Single candlestick/K-line data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Candle {
    /// Period start timestamp (Unix seconds)
    pub time: i64,
    /// Opening price
    pub open: Decimal,
    /// Highest price
    pub high: Decimal,
    /// Lowest price
    pub low: Decimal,
    /// Closing price
    pub close: Decimal,
    /// Trading volume (base asset)
    pub volume: Decimal,
    /// Quote volume (quote asset, USD)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quote_volume: Option<Decimal>,
    /// Number of trades
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trade_count: Option<u32>,
}

impl Candle {
    /// Create new candle from trade
    pub fn from_trade(time: i64, price: Decimal, amount: Decimal) -> Self {
        let quote_vol = price * amount;
        Self {
            time,
            open: price,
            high: price,
            low: price,
            close: price,
            volume: amount,
            quote_volume: Some(quote_vol),
            trade_count: Some(1),
        }
    }

    /// Seed a new candle from an external reference-price tick (e.g. Hyperliquid mark price)
    /// when there is no real trade yet. Volume / trade_count start at 0; they'll be populated
    /// by subsequent real trades via `update`. This lets the frontend TV chart tick OHLC
    /// between real trades without forging volume.
    pub fn from_tick(time: i64, price: Decimal) -> Self {
        Self {
            time,
            open: price,
            high: price,
            low: price,
            close: price,
            volume: Decimal::ZERO,
            quote_volume: Some(Decimal::ZERO),
            trade_count: Some(0),
        }
    }

    /// Update candle with new trade
    pub fn update(&mut self, price: Decimal, amount: Decimal) {
        self.high = self.high.max(price);
        self.low = self.low.min(price);
        self.close = price;
        self.volume += amount;
        if let Some(ref mut qv) = self.quote_volume {
            *qv += price * amount;
        }
        if let Some(ref mut tc) = self.trade_count {
            *tc += 1;
        }
    }

    /// Apply an external reference-price tick: only reshape OHLC envelope (high/low/close),
    /// **do not** touch volume / quote_volume / trade_count. Those stay for real trades to fill.
    pub fn apply_tick(&mut self, price: Decimal) {
        self.high = self.high.max(price);
        self.low = self.low.min(price);
        self.close = price;
    }

    /// Check if candle is final (period ended)
    pub fn is_final(&self, period: KlinePeriod) -> bool {
        let now = Utc::now().timestamp();
        now >= self.time + period.seconds()
    }
}

/// K-line update event for WebSocket broadcast
#[derive(Debug, Clone, Serialize)]
pub struct KlineUpdate {
    pub symbol: String,
    pub period: String,
    pub candle: Candle,
    pub is_final: bool,
}

/// Per-symbol, per-period candle storage
struct CandleStore {
    candles: VecDeque<Candle>,
    current: Option<Candle>,
    max_size: usize,
}

impl CandleStore {
    fn new(max_size: usize) -> Self {
        Self {
            candles: VecDeque::with_capacity(max_size),
            current: None,
            max_size,
        }
    }

    fn push(&mut self, candle: Candle) {
        if self.candles.len() >= self.max_size {
            self.candles.pop_front();
        }
        self.candles.push_back(candle);
    }

    fn get_history(&self, limit: usize) -> Vec<Candle> {
        let len = self.candles.len();
        let start = if len > limit { len - limit } else { 0 };
        self.candles.iter().skip(start).cloned().collect()
    }
}

/// High-performance K-line aggregation service
pub struct KlineService {
    /// Storage: symbol -> period -> candle store
    stores: DashMap<String, DashMap<KlinePeriod, RwLock<CandleStore>>>,
    /// Broadcast channel for K-line updates
    tx: broadcast::Sender<KlineUpdate>,
    /// Max candles to keep per period
    max_candles: usize,
    /// Database pool for persistence (optional)
    db_pool: Option<PgPool>,
}

impl KlineService {
    /// Create new K-line service
    pub fn new(db_pool: Option<PgPool>) -> Arc<Self> {
        let (tx, _) = broadcast::channel(channels::KLINE_CHANNEL_CAPACITY);
        Arc::new(Self {
            stores: DashMap::new(),
            tx,
            max_candles: 10000,
            db_pool,
        })
    }

    /// Subscribe to K-line updates
    pub fn subscribe(&self) -> broadcast::Receiver<KlineUpdate> {
        self.tx.subscribe()
    }

    /// Get period start timestamp aligned to period boundary
    fn align_timestamp(&self, ts: i64, period: KlinePeriod) -> i64 {
        let period_secs = period.seconds();
        (ts / period_secs) * period_secs
    }

    /// Ensure symbol store exists
    fn ensure_symbol(&self, symbol: &str) {
        if !self.stores.contains_key(symbol) {
            let period_map = DashMap::new();
            for period in KlinePeriod::all() {
                period_map.insert(period, RwLock::new(CandleStore::new(self.max_candles)));
            }
            self.stores.insert(symbol.to_string(), period_map);
        }
    }

    /// Apply an external reference-price tick to all period K-lines for a symbol.
    ///
    /// Unlike [`process_trade`](Self::process_trade), this does **not** increment
    /// volume / quote_volume / trade_count. It only reshapes the current candle's
    /// high/low/close so the candle visually tracks the upstream market even
    /// between real trades.
    ///
    /// Intended caller: Hyperliquid price-sync worker (every ~500ms). Without this,
    /// when real user taker flow is sparse, candles look nearly static between
    /// wash-trade prints. With this, frontend TV chart ticks OHLC smoothly while
    /// volume bars still reflect only real matching-engine trades.
    pub async fn apply_price_tick(&self, symbol: &str, price: Decimal, timestamp_ms: i64) {
        self.ensure_symbol(symbol);

        let tick_time = timestamp_ms / 1000;

        if let Some(period_map) = self.stores.get(symbol) {
            for period in KlinePeriod::all() {
                let period_start = self.align_timestamp(tick_time, period);

                if let Some(store_lock) = period_map.get(&period) {
                    let mut store = store_lock.write().await;

                    let candle = match &mut store.current {
                        Some(current) if current.time == period_start => {
                            // 同 period 内:只改 OHLC 的 high/low/close,volume/count 留给 process_trade
                            current.apply_tick(price);
                            current.clone()
                        }
                        Some(current) => {
                            // 跨 period:结束旧 candle,再以 tick 价作为新 candle 的 open
                            let finalized = current.clone();
                            store.push(finalized.clone());

                            self.persist_candle(symbol, period, &finalized).await;

                            let _ = self.tx.send(KlineUpdate {
                                symbol: symbol.to_string(),
                                period: period.as_str().to_string(),
                                candle: finalized,
                                is_final: true,
                            });

                            let new_candle = Candle::from_tick(period_start, price);
                            store.current = Some(new_candle.clone());
                            new_candle
                        }
                        None => {
                            // 冷启动第一根:以 tick 价种下 0-volume candle,等 process_trade 填量
                            let new_candle = Candle::from_tick(period_start, price);
                            store.current = Some(new_candle.clone());
                            new_candle
                        }
                    };

                    let _ = self.tx.send(KlineUpdate {
                        symbol: symbol.to_string(),
                        period: period.as_str().to_string(),
                        candle,
                        is_final: false,
                    });
                }
            }
        }
    }

    /// Process trade event and update all period K-lines
    pub async fn process_trade(&self, trade: &TradeEvent) {
        let symbol = &trade.symbol;
        self.ensure_symbol(symbol);

        // Convert milliseconds to seconds for proper time alignment
        let trade_time = trade.timestamp / 1000;
        let price = trade.price;
        let amount = trade.amount;

        if let Some(period_map) = self.stores.get(symbol) {
            for period in KlinePeriod::all() {
                let period_start = self.align_timestamp(trade_time, period);

                if let Some(store_lock) = period_map.get(&period) {
                    let mut store = store_lock.write().await;

                    let candle = match &mut store.current {
                        Some(current) if current.time == period_start => {
                            // Update existing candle
                            current.update(price, amount);
                            current.clone()
                        }
                        Some(current) => {
                            // Period changed, finalize current and start new
                            let finalized = current.clone();
                            store.push(finalized.clone());

                            // Persist finalized candle to database
                            self.persist_candle(symbol, period, &finalized).await;

                            // Broadcast finalized candle
                            let _ = self.tx.send(KlineUpdate {
                                symbol: symbol.clone(),
                                period: period.as_str().to_string(),
                                candle: finalized,
                                is_final: true,
                            });

                            let new_candle = Candle::from_trade(period_start, price, amount);
                            store.current = Some(new_candle.clone());
                            new_candle
                        }
                        None => {
                            // First candle
                            let new_candle = Candle::from_trade(period_start, price, amount);
                            store.current = Some(new_candle.clone());
                            new_candle
                        }
                    };

                    // Broadcast update
                    let _ = self.tx.send(KlineUpdate {
                        symbol: symbol.clone(),
                        period: period.as_str().to_string(),
                        candle,
                        is_final: false,
                    });
                }
            }
        }
    }

    /// Get historical candles - reads from memory first, falls back to database
    pub async fn get_candles(
        &self,
        symbol: &str,
        period: KlinePeriod,
        limit: usize,
        from: Option<i64>,
        to: Option<i64>,
    ) -> Vec<Candle> {
        self.ensure_symbol(symbol);

        let mut result = Vec::new();

        // First, try to get from memory cache
        if let Some(period_map) = self.stores.get(symbol) {
            if let Some(store_lock) = period_map.get(&period) {
                let store = store_lock.read().await;

                // Get historical candles from memory
                let history = store.get_history(limit);

                // Filter by time range if specified
                for candle in history {
                    let include = match (from, to) {
                        (Some(f), Some(t)) => candle.time >= f && candle.time <= t,
                        (Some(f), None) => candle.time >= f,
                        (None, Some(t)) => candle.time <= t,
                        (None, None) => true,
                    };
                    if include {
                        result.push(candle);
                    }
                }

                // Include current candle if in range
                if let Some(ref current) = store.current {
                    let include = match (from, to) {
                        (Some(f), Some(t)) => current.time >= f && current.time <= t,
                        (Some(f), None) => current.time >= f,
                        (None, Some(t)) => current.time <= t,
                        (None, None) => true,
                    };
                    if include {
                        result.push(current.clone());
                    }
                }
            }
        }

        // If memory cache is empty or has fewer candles than requested, query database
        if result.len() < limit {
            if let Some(db_candles) = self.get_candles_from_db(symbol, period, limit, from, to).await {
                // Merge database results with memory results
                // Use a set to avoid duplicates based on timestamp
                let existing_times: std::collections::HashSet<i64> = result.iter().map(|c| c.time).collect();
                for candle in db_candles {
                    if !existing_times.contains(&candle.time) {
                        result.push(candle);
                    }
                }
            }
        }

        // Sort by time ascending
        result.sort_by_key(|c| c.time);

        // Apply limit (take most recent)
        if result.len() > limit {
            result = result.into_iter().rev().take(limit).collect();
            result.reverse();
        }

        result
    }

    /// Query candles from TimescaleDB (klines_historical table)
    async fn get_candles_from_db(
        &self,
        symbol: &str,
        period: KlinePeriod,
        limit: usize,
        from: Option<i64>,
        to: Option<i64>,
    ) -> Option<Vec<Candle>> {
        let pool = self.db_pool.as_ref()?;

        let period_str = period.as_str();

        // Build query based on time filters
        let (start_time, end_time) = match (from, to) {
            (Some(f), Some(t)) => {
                (chrono::DateTime::from_timestamp(f, 0)?, chrono::DateTime::from_timestamp(t, 0)?)
            }
            (Some(f), None) => {
                (chrono::DateTime::from_timestamp(f, 0)?, Utc::now())
            }
            (None, Some(t)) => {
                // Default to 30 days ago
                let end = chrono::DateTime::from_timestamp(t, 0)?;
                let start = end - Duration::days(30);
                (start, end)
            }
            (None, None) => {
                // Default: last 30 days
                let end = Utc::now();
                let start = end - Duration::days(30);
                (start, end)
            }
        };

        let rows: Result<Vec<DbKlineRow>, _> = sqlx::query_as(
            r#"
            SELECT
                symbol,
                open_time,
                open,
                high,
                low,
                close,
                volume,
                quote_volume,
                trade_count
            FROM klines_historical
            WHERE symbol = $1
              AND period = $2
              AND open_time >= $3
              AND open_time <= $4
            ORDER BY open_time DESC
            LIMIT $5
            "#
        )
        .bind(symbol)
        .bind(period_str)
        .bind(start_time)
        .bind(end_time)
        .bind(limit as i64)
        .fetch_all(pool)
        .await;

        match rows {
            Ok(rows) => {
                let candles: Vec<Candle> = rows.into_iter().map(|row| {
                    Candle {
                        time: row.open_time.timestamp(),
                        open: row.open,
                        high: row.high,
                        low: row.low,
                        close: row.close,
                        volume: row.volume,
                        quote_volume: row.quote_volume,
                        trade_count: row.trade_count.map(|c| c as u32),
                    }
                }).collect();
                Some(candles)
            }
            Err(e) => {
                tracing::warn!("Failed to query klines from database: {}", e);
                None
            }
        }
    }

    /// Persist a finalized candle to TimescaleDB
    async fn persist_candle(&self, symbol: &str, period: KlinePeriod, candle: &Candle) {
        let Some(pool) = self.db_pool.as_ref() else { return };

        let open_time = match chrono::DateTime::from_timestamp(candle.time, 0) {
            Some(t) => t,
            None => return,
        };

        let result = sqlx::query(
            r#"
            INSERT INTO klines_historical (
                symbol, period, open_time, open, high, low, close, volume, quote_volume, trade_count
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (symbol, period, open_time)
            DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                quote_volume = EXCLUDED.quote_volume,
                trade_count = EXCLUDED.trade_count
            "#
        )
        .bind(symbol)
        .bind(period.as_str())
        .bind(open_time)
        .bind(candle.open)
        .bind(candle.high)
        .bind(candle.low)
        .bind(candle.close)
        .bind(candle.volume)
        .bind(candle.quote_volume)
        .bind(candle.trade_count.map(|c| c as i32))
        .execute(pool)
        .await;

        if let Err(e) = result {
            tracing::warn!("Failed to persist candle to database: {}", e);
        }
    }

    /// Get latest (current) candle
    pub async fn get_latest_candle(&self, symbol: &str, period: KlinePeriod) -> Option<(Candle, bool)> {
        self.ensure_symbol(symbol);

        if let Some(period_map) = self.stores.get(symbol) {
            if let Some(store_lock) = period_map.get(&period) {
                let store = store_lock.read().await;
                if let Some(ref current) = store.current {
                    let is_final = current.is_final(period);
                    return Some((current.clone(), is_final));
                }
            }
        }
        None
    }

    /// Start Binance WebSocket listener
    pub async fn start_binance_listener(self: Arc<Self>, symbols: Vec<String>) {
        tracing::info!("K-line service Binance listener started for {:?}", symbols);
        
        // Construct streams string
        // format: <symbol>@kline_<period>
        let mut streams = Vec::new();
        for symbol in &symbols {
            for period in KlinePeriod::all() {
                let p_str = period.as_str();
                streams.push(format!("{}@kline_{}", symbol.to_lowercase(), p_str));
            }
        }
        
        let url_str = format!("wss://fstream.binance.com/stream?streams={}", streams.join("/"));
        tracing::info!("Connecting to Binance WS: {}", url_str);
        
        loop {
            let url = match reqwest::Url::parse(&url_str) {
                Ok(u) => u,
                Err(e) => {
                    tracing::error!("Invalid Binance URL: {}", e);
                    return;
                }
            };

            match tokio_tungstenite::connect_async(url).await {
                Ok((ws_stream, _)) => {
                    tracing::info!("Connected to Binance Combined Stream");
                    let (_, mut read) = ws_stream.split();

                    while let Some(msg) = read.next().await {
                        match msg {
                            Ok(tokio_tungstenite::tungstenite::Message::Text(text)) => {
                                if let Ok(event) = serde_json::from_str::<BinanceCombinedEvent>(&text) {
                                    if event.data.e == "kline" {
                                        let k = event.data.k;
                                        let symbol = k.s.clone();
                                        
                                        // Parse period
                                        if let Some(period) = KlinePeriod::from_str(&k.i) {
                                            // Create Candle
                                            let candle = Candle {
                                                time: k.t / 1000, // ms to s
                                                open: Decimal::from_str(&k.o).unwrap_or_default(),
                                                high: Decimal::from_str(&k.h).unwrap_or_default(),
                                                low: Decimal::from_str(&k.l).unwrap_or_default(),
                                                close: Decimal::from_str(&k.c).unwrap_or_default(),
                                                volume: Decimal::from_str(&k.v).unwrap_or_default(),
                                                quote_volume: Some(Decimal::from_str(&k.q).unwrap_or_default()),
                                                trade_count: Some(k.n),
                                            };

                                            // Update internal store
                                            if let Some(period_map) = self.stores.get(&symbol) {
                                                if let Some(store_lock) = period_map.get(&period) {
                                                    let mut store = store_lock.write().await;
                                                    
                                                    // Handle history/current logic
                                                    // If it's a new candle (time changed), push old to history
                                                    let current_opt = store.current.clone();
                                                    if let Some(current) = current_opt {
                                                        if candle.time > current.time {
                                                            store.push(current);
                                                        }
                                                    }
                                                    store.current = Some(candle.clone());
                                                }
                                            }

                                            // Broadcast update
                                            let update = KlineUpdate {
                                                symbol,
                                                period: period.as_str().to_string(),
                                                candle,
                                                is_final: k.x,
                                            };
                                            
                                            let _ = self.tx.send(update);
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::error!("Binance WS read error: {}", e);
                                break;
                            }
                            _ => {}
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to connect to Binance WS: {}. Retrying in 5s...", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
            tracing::warn!("Binance WS connection lost, reconnecting...");
        }
    }
}

// Binance Helper Structs
#[allow(dead_code, non_snake_case)]
#[derive(Debug, Deserialize)]
struct BinanceCombinedEvent {
    stream: String,
    data: BinanceKlineEvent,
}

#[allow(dead_code, non_snake_case)]
#[derive(Debug, Deserialize)]
struct BinanceKlineEvent {
    e: String, // Event type
    E: i64,    // Event time
    s: String, // Symbol
    k: BinanceKlineRaw,
}

#[allow(dead_code, non_snake_case)]
#[derive(Debug, Deserialize)]
struct BinanceKlineRaw {
    t: i64,    // Kline start time
    T: i64,    // Kline close time
    s: String, // Symbol
    i: String, // Interval
    f: u64,    // First trade ID
    L: u64,    // Last trade ID
    o: String, // Open price
    c: String, // Close price
    h: String, // High price
    l: String, // Low price
    v: String, // Base asset volume
    n: u32,    // Number of trades
    x: bool,   // Is this kline closed?
    q: String, // Quote asset volume
    V: String, // Taker buy base asset volume
    Q: String, // Taker buy quote asset volume
    B: String, // Ignore
}

impl KlineService {
    /// Generate initial candles from recent trades in database
    pub async fn load_historical_from_db(&self, symbol: &str, hours: i64) -> Result<usize, sqlx::Error> {
        let pool = match &self.db_pool {
            Some(p) => p,
            None => return Ok(0),
        };

        let cutoff = Utc::now() - Duration::hours(hours);
        let _cutoff_ts = cutoff.timestamp();

        let trades: Vec<(Decimal, Decimal, i64)> = sqlx::query_as(
            r#"
            SELECT price, amount, EXTRACT(EPOCH FROM created_at)::bigint as ts
            FROM trades
            WHERE symbol = $1 AND created_at > $2
            ORDER BY created_at ASC
            "#
        )
        .bind(symbol)
        .bind(cutoff.naive_utc())
        .fetch_all(pool)
        .await?;

        let count = trades.len();
        for (price, amount, ts) in trades {
            let trade = TradeEvent {
                symbol: symbol.to_string(),
                trade_id: uuid::Uuid::new_v4(),
                maker_order_id: uuid::Uuid::nil(),
                taker_order_id: uuid::Uuid::nil(),
                maker_address: String::new(),
                taker_address: String::new(),
                side: String::new(),
                price,
                amount,
                maker_fee: rust_decimal::Decimal::ZERO,
                taker_fee: rust_decimal::Decimal::ZERO,
                timestamp: ts,
                maker_leverage: 1,
                taker_leverage: 1,
                is_self_trade: false,
            };
            self.process_trade(&trade).await;
        }

        tracing::info!("Loaded {} historical trades for {} K-line generation", count, symbol);
        Ok(count)
    }

    /// Clear all K-line data for a symbol
    /// Used to reset data before generating fresh trades
    pub async fn clear_data(&self, symbol: &str) {
        tracing::info!("Clearing K-line data for {}", symbol);

        if let Some(period_map) = self.stores.get(symbol) {
            for period in KlinePeriod::all() {
                if let Some(store_lock) = period_map.get(&period) {
                    let mut store = store_lock.write().await;
                    store.candles.clear();
                    store.current = None;
                }
            }
        }

        tracing::info!("Cleared K-line data for {}", symbol);
    }

    /// Clear all K-line data for all symbols
    pub async fn clear_all_data(&self) {
        tracing::info!("Clearing all K-line data");

        for entry in self.stores.iter() {
            let symbol = entry.key();
            self.clear_data(symbol).await;
        }

        tracing::info!("Cleared all K-line data");
    }

    /// Generate mock K-line data for development/testing
    /// This creates realistic-looking candles for the past N periods
    pub async fn generate_mock_data(&self, symbol: &str, num_candles: usize) {
        use rand::Rng;

        tracing::info!("Generating {} mock candles for {}", num_candles, symbol);
        self.ensure_symbol(symbol);

        let now = Utc::now().timestamp();
        let mut rng = rand::thread_rng();

        // Generate data for each period
        for period in KlinePeriod::all() {
            let period_secs = period.seconds();

            // Start from num_candles periods ago
            let mut base_price = match symbol {
                s if s.starts_with("BTC") => 97000.0,
                s if s.starts_with("ETH") => 3800.0,
                s if s.starts_with("SOL") => 220.0,
                _ => 100.0,
            };

            if let Some(period_map) = self.stores.get(symbol) {
                if let Some(store_lock) = period_map.get(&period) {
                    let mut store = store_lock.write().await;

                    for i in 0..num_candles {
                        let candle_time = self.align_timestamp(now, period) - (period_secs * (num_candles - i - 1) as i64);

                        // Random price movement (-2% to +2%)
                        let change_pct: f64 = rng.gen_range(-0.02..0.02);
                        base_price *= 1.0 + change_pct;

                        let open: f64 = base_price;
                        let close: f64 = base_price * (1.0 + rng.gen_range(-0.01_f64..0.01_f64));
                        let high: f64 = open.max(close) * (1.0 + rng.gen_range(0.0_f64..0.005_f64));
                        let low: f64 = open.min(close) * (1.0 - rng.gen_range(0.0_f64..0.005_f64));
                        let volume: f64 = rng.gen_range(10.0..1000.0);
                        let quote_volume: f64 = volume * (open + close) / 2.0;
                        let trade_count: u32 = rng.gen_range(50..500);

                        let candle = Candle {
                            time: candle_time,
                            open: Decimal::from_f64_retain(open).unwrap_or_default(),
                            high: Decimal::from_f64_retain(high).unwrap_or_default(),
                            low: Decimal::from_f64_retain(low).unwrap_or_default(),
                            close: Decimal::from_f64_retain(close).unwrap_or_default(),
                            volume: Decimal::from_f64_retain(volume).unwrap_or_default(),
                            quote_volume: Some(Decimal::from_f64_retain(quote_volume).unwrap_or_default()),
                            trade_count: Some(trade_count),
                        };

                        // Push to history (except last one which becomes current)
                        if i < num_candles - 1 {
                            store.push(candle);
                        } else {
                            store.current = Some(candle);
                        }

                        // Update base_price for next candle's continuity
                        base_price = close;
                    }
                }
            }
        }

        tracing::info!("Generated mock K-line data for {} ({} candles per period)", symbol, num_candles);
    }
}

/// Historical K-line data for import
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoricalKline {
    pub symbol: String,
    pub period: String,
    pub open_time: i64, // Unix timestamp in milliseconds
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
    #[serde(default)]
    pub quote_volume: Option<Decimal>,
    #[serde(default)]
    pub trade_count: Option<i32>,
}

impl KlineService {
    /// Save a single K-line to the database
    pub async fn save_kline_to_db(&self, kline: &HistoricalKline) -> Result<(), sqlx::Error> {
        let pool = match &self.db_pool {
            Some(p) => p,
            None => {
                tracing::warn!("No database pool configured for K-line persistence");
                return Ok(());
            }
        };

        let open_time = chrono::DateTime::from_timestamp(kline.open_time, 0)
            .unwrap_or_else(|| Utc::now().into());

        sqlx::query(
            r#"
            INSERT INTO klines_historical (
                symbol, period, open_time, open, high, low, close, volume, quote_volume, trade_count
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (symbol, period, open_time)
            DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                quote_volume = EXCLUDED.quote_volume,
                trade_count = EXCLUDED.trade_count
            "#
        )
        .bind(&kline.symbol)
        .bind(&kline.period)
        .bind(open_time)
        .bind(kline.open)
        .bind(kline.high)
        .bind(kline.low)
        .bind(kline.close)
        .bind(kline.volume)
        .bind(kline.quote_volume.unwrap_or(Decimal::ZERO))
        .bind(kline.trade_count.unwrap_or(0))
        .execute(pool)
        .await?;

        Ok(())
    }

    /// Save multiple K-lines to the database in batch
    pub async fn save_klines_batch(&self, klines: &[HistoricalKline]) -> Result<usize, sqlx::Error> {
        let pool = match &self.db_pool {
            Some(p) => p,
            None => {
                tracing::warn!("No database pool configured for K-line persistence");
                return Ok(0);
            }
        };

        let mut saved = 0;

        // Process in batches of 100 for better performance
        for chunk in klines.chunks(100) {
            let mut tx = pool.begin().await?;

            for kline in chunk {
                let open_time = chrono::DateTime::from_timestamp_millis(kline.open_time)
                    .unwrap_or_else(|| Utc::now().into());

                let result = sqlx::query(
                    r#"
                    INSERT INTO klines_historical (
                        symbol, period, open_time, open, high, low, close, volume, quote_volume, trade_count
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    ON CONFLICT (symbol, period, open_time)
                    DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume,
                        quote_volume = EXCLUDED.quote_volume,
                        trade_count = EXCLUDED.trade_count
                    "#
                )
                .bind(&kline.symbol)
                .bind(&kline.period)
                .bind(open_time)
                .bind(kline.open)
                .bind(kline.high)
                .bind(kline.low)
                .bind(kline.close)
                .bind(kline.volume)
                .bind(kline.quote_volume.unwrap_or(Decimal::ZERO))
                .bind(kline.trade_count.unwrap_or(0))
                .execute(&mut *tx)
                .await;

                if result.is_ok() {
                    saved += 1;
                }
            }

            tx.commit().await?;
        }

        tracing::info!("Saved {} K-lines to database", saved);
        Ok(saved)
    }

    /// Load historical K-lines from database
    pub async fn load_klines_from_db(
        &self,
        symbol: &str,
        period: &str,
        start_time: i64,
        end_time: i64,
        limit: i32,
    ) -> Result<Vec<Candle>, sqlx::Error> {
        let pool = match &self.db_pool {
            Some(p) => p,
            None => return Ok(Vec::new()),
        };

        let start_dt = chrono::DateTime::from_timestamp(start_time, 0)
            .unwrap_or_else(|| Utc::now().into());
        let end_dt = chrono::DateTime::from_timestamp(end_time, 0)
            .unwrap_or_else(|| Utc::now().into());

        let rows: Vec<(Decimal, Decimal, Decimal, Decimal, Decimal, Decimal, i32, chrono::DateTime<Utc>)> = sqlx::query_as(
            r#"
            SELECT open, high, low, close, volume, quote_volume, trade_count, open_time
            FROM klines_historical
            WHERE symbol = $1
              AND period = $2
              AND open_time >= $3
              AND open_time < $4
            ORDER BY open_time DESC
            LIMIT $5
            "#
        )
        .bind(symbol)
        .bind(period)
        .bind(start_dt)
        .bind(end_dt)
        .bind(limit)
        .fetch_all(pool)
        .await?;

        let candles = rows
            .into_iter()
            .map(|(open, high, low, close, volume, quote_volume, trade_count, open_time)| {
                Candle {
                    time: open_time.timestamp(),
                    open,
                    high,
                    low,
                    close,
                    volume,
                    quote_volume: Some(quote_volume),
                    trade_count: Some(trade_count as u32),
                }
            })
            .collect();

        Ok(candles)
    }

    /// Import K-lines into both memory cache and database
    pub async fn import_historical_klines(&self, klines: &[HistoricalKline]) -> Result<usize, sqlx::Error> {
        // First save to database
        let saved = self.save_klines_batch(klines).await?;

        // Then update in-memory cache for real-time access
        for kline in klines {
            if let Some(period) = KlinePeriod::from_str(&kline.period) {
                self.ensure_symbol(&kline.symbol);

                if let Some(period_map) = self.stores.get(&kline.symbol) {
                    if let Some(store_lock) = period_map.get(&period) {
                        let mut store = store_lock.write().await;

                        let candle = Candle {
                            time: kline.open_time / 1000, // Convert milliseconds to seconds
                            open: kline.open,
                            high: kline.high,
                            low: kline.low,
                            close: kline.close,
                            volume: kline.volume,
                            quote_volume: kline.quote_volume,
                            trade_count: kline.trade_count.map(|c| c as u32),
                        };

                        // Add to history
                        store.push(candle);
                    }
                }
            }
        }

        Ok(saved)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_period_seconds() {
        assert_eq!(KlinePeriod::Min1.seconds(), 60);
        assert_eq!(KlinePeriod::Min5.seconds(), 300);
        assert_eq!(KlinePeriod::Hour1.seconds(), 3600);
        assert_eq!(KlinePeriod::Day1.seconds(), 86400);
    }

    #[test]
    fn test_period_parse() {
        assert_eq!(KlinePeriod::from_str("1m"), Some(KlinePeriod::Min1));
        assert_eq!(KlinePeriod::from_str("5m"), Some(KlinePeriod::Min5));
        assert_eq!(KlinePeriod::from_str("1h"), Some(KlinePeriod::Hour1));
        assert_eq!(KlinePeriod::from_str("invalid"), None);
    }

    #[tokio::test]
    async fn test_candle_update() {
        let mut candle = Candle::from_trade(1000, Decimal::from(100), Decimal::from(1));
        assert_eq!(candle.open, Decimal::from(100));
        assert_eq!(candle.close, Decimal::from(100));

        candle.update(Decimal::from(110), Decimal::from(2));
        assert_eq!(candle.high, Decimal::from(110));
        assert_eq!(candle.close, Decimal::from(110));
        assert_eq!(candle.volume, Decimal::from(3));

        candle.update(Decimal::from(90), Decimal::from(1));
        assert_eq!(candle.low, Decimal::from(90));
        assert_eq!(candle.close, Decimal::from(90));
    }
}

impl KlineService {
    /// Delete all K-lines for a specific symbol and period
    pub async fn delete_klines(&self, symbol: Option<&str>, period: Option<&str>) -> Result<u64, sqlx::Error> {
        let pool = match &self.db_pool {
            Some(p) => p,
            None => {
                tracing::warn!("No database pool configured for K-line deletion");
                return Ok(0);
            }
        };

        let deleted = match (symbol, period) {
            (Some(sym), Some(per)) => {
                // Delete specific symbol + period
                let result = sqlx::query("DELETE FROM klines_historical WHERE symbol = $1 AND period = $2")
                    .bind(sym)
                    .bind(per)
                    .execute(pool)
                    .await?;
                result.rows_affected()
            }
            (Some(sym), None) => {
                // Delete all periods for a symbol
                let result = sqlx::query("DELETE FROM klines_historical WHERE symbol = $1")
                    .bind(sym)
                    .execute(pool)
                    .await?;
                result.rows_affected()
            }
            (None, Some(per)) => {
                // Delete specific period for all symbols
                let result = sqlx::query("DELETE FROM klines_historical WHERE period = $1")
                    .bind(per)
                    .execute(pool)
                    .await?;
                result.rows_affected()
            }
            (None, None) => {
                // Delete all K-lines
                let result = sqlx::query("DELETE FROM klines_historical")
                    .execute(pool)
                    .await?;
                result.rows_affected()
            }
        };

        tracing::info!("Deleted {} K-line records (symbol: {:?}, period: {:?})", deleted, symbol, period);
        Ok(deleted)
    }
}
