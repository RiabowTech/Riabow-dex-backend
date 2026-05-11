#![allow(dead_code)]
//! Price Feed Service
//!
//! Provides market data including:
//! - Mark prices
//! - Funding rates
//! - Ticker data (last price, 24h volume, etc.)
//!
//! [CURRENT MODE] Internal Market Maker:
//! All market data comes from internal market maker service via API.
//! Data is updated through `/internal/trade` and `/internal/orderbook` endpoints.
//!
//! [DISABLED] Binance External Feed:
//! Binance REST API fetching is disabled. The `start_update_loop()` method
//! is not called in main.rs.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{interval, Duration};
use tracing::Instrument;

const BINANCE_BASE_URL: &str = "https://fapi.binance.com";

/// Price feed configuration
#[derive(Debug, Clone)]
pub struct PriceFeedConfig {
    /// Number of top markets to track (by 24h USD volume)
    pub top_markets: usize,
    /// Price update interval in seconds
    pub update_interval_secs: u64,
    /// Market list refresh interval in seconds
    pub market_refresh_secs: u64,
}

impl Default for PriceFeedConfig {
    fn default() -> Self {
        Self {
            top_markets: 50,
            update_interval_secs: 5,
            market_refresh_secs: 300,
        }
    }
}

/// Binance 24hr Ticker Response
#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct BinanceTicker {
    symbol: String,
    last_price: String,
    open_price: String,
    high_price: String,
    low_price: String,
    volume: String,           // Base asset volume
    quote_volume: String,     // Quote asset volume (USDT)
    price_change: String,
    price_change_percent: String,
    weighted_avg_price: String,
    #[allow(dead_code)]
    open_time: i64,
    close_time: i64,
}

/// Binance Premium Index Response (for mark price and funding rate)
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BinancePremiumIndex {
    symbol: String,
    mark_price: String,
    index_price: String,
    #[allow(dead_code)]
    estimated_settle_price: String,
    last_funding_rate: String,
    #[allow(dead_code)]
    interest_rate: String,
    next_funding_time: i64,
    time: i64,
}

/// Market information
#[derive(Debug, Clone, Serialize)]
pub struct MarketInfo {
    pub symbol: String,           // Symbol in BTCUSDT format
    pub base_asset: String,       // BTC
    pub quote_asset: String,      // USDT
    pub volume_24h_usd: Decimal,  // 24h volume in USD
    pub rank: usize,              // Rank by volume
}

/// Cached price data for a symbol
#[derive(Debug, Clone, Serialize)]
pub struct PriceData {
    pub symbol: String,
    pub mark_price: Decimal,
    pub last_price: Decimal,
    pub index_price: Decimal,
    pub bid_price: Decimal,
    pub ask_price: Decimal,
    pub funding_rate: Decimal,
    pub next_funding_rate: Decimal,
    pub next_funding_time: i64,
    pub open_24h: Decimal,
    pub high_24h: Decimal,
    pub low_24h: Decimal,
    pub volume_24h: Decimal,
    pub volume_ccy_24h: Decimal,
    pub price_change_24h: Decimal,
    pub price_change_percent_24h: Decimal,
    pub updated_at: i64,
}

impl Default for PriceData {
    fn default() -> Self {
        Self {
            symbol: String::new(),
            mark_price: Decimal::ZERO,
            last_price: Decimal::ZERO,
            index_price: Decimal::ZERO,
            bid_price: Decimal::ZERO,
            ask_price: Decimal::ZERO,
            funding_rate: Decimal::ZERO,
            next_funding_rate: Decimal::ZERO,
            next_funding_time: 0,
            open_24h: Decimal::ZERO,
            high_24h: Decimal::ZERO,
            low_24h: Decimal::ZERO,
            volume_24h: Decimal::ZERO,
            volume_ccy_24h: Decimal::ZERO,
            price_change_24h: Decimal::ZERO,
            price_change_percent_24h: Decimal::ZERO,
            updated_at: 0,
        }
    }
}

pub struct PriceFeedService {
    client: reqwest::Client,
    /// Configuration for the price feed
    config: PriceFeedConfig,
    /// Price data cache: symbol -> PriceData
    price_cache: Arc<RwLock<HashMap<String, PriceData>>>,
    /// Market info cache: symbol -> MarketInfo
    markets_cache: Arc<RwLock<Vec<MarketInfo>>>,
    /// Set of active symbols being tracked
    active_symbols: Arc<RwLock<Vec<String>>>,
}

impl PriceFeedService {
    pub fn new() -> Self {
        Self::with_config(PriceFeedConfig::default())
    }

    pub fn with_config(config: PriceFeedConfig) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .unwrap_or_else(|e| {
                tracing::error!("Failed to create HTTP client with custom config: {:?}, using default client", e);
                reqwest::Client::new()
            });

        Self {
            client,
            config,
            price_cache: Arc::new(RwLock::new(HashMap::new())),
            markets_cache: Arc::new(RwLock::new(Vec::new())),
            active_symbols: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Initialize active symbols from a list (for use when Binance feed is disabled)
    pub async fn init_symbols(&self, symbols: Vec<String>) {
        let mut active = self.active_symbols.write().await;
        let mut markets = self.markets_cache.write().await;

        active.clear();
        markets.clear();

        for (rank, symbol) in symbols.iter().enumerate() {
            active.push(symbol.clone());

            // Extract base asset from symbol (e.g., BTCUSDT -> BTC)
            let base_asset = symbol.replace("USDT", "");

            markets.push(MarketInfo {
                symbol: symbol.clone(),
                base_asset,
                quote_asset: "USDT".to_string(),
                volume_24h_usd: Decimal::ZERO,
                rank: rank + 1,
            });
        }

        tracing::info!("Price feed initialized with {} symbols: {:?}", symbols.len(), symbols);
    }

    /// Start the background price update loop
    pub async fn start_update_loop(self: Arc<Self>) {
        let service = self.clone();
        let update_interval = self.config.update_interval_secs;
        let market_refresh_secs = self.config.market_refresh_secs;
        let top_markets = self.config.top_markets;

        // Initial fetch of markets
        if let Err(e) = service.refresh_markets().await {
            tracing::error!("Failed initial market refresh: {}", e);
        }

        // Calculate refresh iterations: market_refresh_secs / update_interval
        let refresh_iterations = (market_refresh_secs / update_interval) as u32;

        // Background loop for updating prices
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(update_interval));
            let mut market_refresh_counter = 0u32;

            loop {
                ticker.tick().await;

                // Refresh market list based on configured interval
                market_refresh_counter += 1;
                if market_refresh_counter >= refresh_iterations {
                    market_refresh_counter = 0;
                    if let Err(e) = service.refresh_markets().await {
                        tracing::warn!("Failed to refresh markets: {}", e);
                    }
                }

                // Update prices for all active symbols
                if let Err(e) = service.update_all_prices().await {
                    tracing::warn!("Failed to update prices: {}", e);
                }
            }
        }.instrument(tracing::info_span!("price-feed-binance")));

        tracing::info!(
            "Price feed service started: top {} markets, update every {}s, refresh every {}s",
            top_markets, update_interval, market_refresh_secs
        );
    }

    /// Refresh the list of top markets from Binance
    pub async fn refresh_markets(&self) -> anyhow::Result<()> {
        let url = format!("{}/fapi/v1/ticker/24hr", BINANCE_BASE_URL);

        let resp: Vec<BinanceTicker> = self.client
            .get(&url)
            .send()
            .await?
            .json()
            .await?;

        // Filter for USDT perpetual futures and calculate USD volume
        let mut markets: Vec<(BinanceTicker, Decimal)> = resp
            .into_iter()
            .filter(|t| t.symbol.ends_with("USDT"))
            .filter_map(|t| {
                // quoteVolume is already in USDT (USD equivalent)
                let usd_volume = Decimal::from_str(&t.quote_volume).unwrap_or(Decimal::ZERO);
                if usd_volume > Decimal::ZERO {
                    Some((t, usd_volume))
                } else {
                    None
                }
            })
            .collect();

        // Sort by USD volume descending
        markets.sort_by(|a, b| b.1.cmp(&a.1));

        // Take top N markets
        let top_markets: Vec<MarketInfo> = markets
            .into_iter()
            .take(self.config.top_markets)
            .enumerate()
            .map(|(idx, (ticker, usd_volume))| {
                let symbol = ticker.symbol.clone();
                // Extract base asset: BTCUSDT -> BTC
                let base = symbol.strip_suffix("USDT").unwrap_or(&symbol).to_string();

                MarketInfo {
                    symbol,
                    base_asset: base,
                    quote_asset: "USDT".to_string(),
                    volume_24h_usd: usd_volume,
                    rank: idx + 1,
                }
            })
            .collect();

        // Update active symbols
        let symbols: Vec<String> = top_markets.iter().map(|m| m.symbol.clone()).collect();

        {
            let mut active = self.active_symbols.write().await;
            *active = symbols;
        }

        {
            let mut cache = self.markets_cache.write().await;
            *cache = top_markets;
        }

        tracing::info!("Refreshed market list with {} markets", self.config.top_markets);
        Ok(())
    }

    /// Update prices for all active symbols
    async fn update_all_prices(&self) -> anyhow::Result<()> {
        // Fetch all tickers in one request
        let url = format!("{}/fapi/v1/ticker/24hr", BINANCE_BASE_URL);
        let ticker_resp: Vec<BinanceTicker> = self.client
            .get(&url)
            .send()
            .await?
            .json()
            .await?;

        // Create a map of Binance symbol -> ticker
        let ticker_map: HashMap<String, BinanceTicker> = ticker_resp
            .into_iter()
            .map(|t| (t.symbol.clone(), t))
            .collect();

        // Fetch all premium index data (mark price + funding rate)
        let premium_url = format!("{}/fapi/v1/premiumIndex", BINANCE_BASE_URL);
        let premium_resp: Vec<BinancePremiumIndex> = self.client
            .get(&premium_url)
            .send()
            .await?
            .json()
            .await?;

        let premium_map: HashMap<String, BinancePremiumIndex> = premium_resp
            .into_iter()
            .map(|p| (p.symbol.clone(), p))
            .collect();

        // Update price cache
        let mut cache = self.price_cache.write().await;
        let now = chrono::Utc::now().timestamp_millis();

        let active_symbols = self.active_symbols.read().await.clone();
        for symbol in &active_symbols {
            // Symbol is already in Binance format (BTCUSDT)
            if let Some(ticker) = ticker_map.get(symbol) {
                let last_price = Decimal::from_str(&ticker.last_price).unwrap_or(Decimal::ZERO);
                let open_24h = Decimal::from_str(&ticker.open_price).unwrap_or(Decimal::ZERO);
                let _price_change_24h = Decimal::from_str(&ticker.price_change).unwrap_or(Decimal::ZERO);
                let _price_change_percent_24h = Decimal::from_str(&ticker.price_change_percent).unwrap_or(Decimal::ZERO);

                // Get mark price and funding rate from premium index
                let (mark_price, index_price, funding_rate, next_funding_time) = 
                    if let Some(premium) = premium_map.get(symbol) {
                        (
                            Decimal::from_str(&premium.mark_price).unwrap_or(last_price),
                            Decimal::from_str(&premium.index_price).unwrap_or(last_price),
                            Decimal::from_str(&premium.last_funding_rate).unwrap_or(Decimal::ZERO),
                            premium.next_funding_time,
                        )
                    } else {
                        (last_price, last_price, Decimal::ZERO, 0)
                    };

                // Check if internal last_price diverges from external — if so, keep internal prices
                let existing = cache.get(symbol);
                let internal_last = existing.map(|e| e.last_price).unwrap_or(Decimal::ZERO);
                let internal_volume_24h = existing.map(|e| e.volume_24h).unwrap_or(Decimal::ZERO);
                let internal_volume_ccy = existing.map(|e| e.volume_ccy_24h).unwrap_or(Decimal::ZERO);
                let internal_open_24h = existing.map(|e| e.open_24h).unwrap_or(Decimal::ZERO);
                let internal_high = existing.map(|e| e.high_24h).unwrap_or(Decimal::ZERO);
                let internal_low = existing.map(|e| e.low_24h).unwrap_or(Decimal::ZERO);

                let use_internal_price = if internal_last > Decimal::ZERO && mark_price > Decimal::ZERO {
                    let divergence = ((internal_last - mark_price) / mark_price).abs();
                    divergence > Decimal::from_str("0.01").unwrap_or(Decimal::ZERO)
                } else {
                    false
                };

                let effective_mark = if use_internal_price { internal_last } else { mark_price };
                let effective_last = if use_internal_price { internal_last } else { last_price };
                let effective_high = if use_internal_price { internal_high.max(internal_last) } else { Decimal::from_str(&ticker.high_price).unwrap_or(Decimal::ZERO) };
                let effective_low = if use_internal_price { if internal_low > Decimal::ZERO { internal_low.min(internal_last) } else { internal_last } } else { Decimal::from_str(&ticker.low_price).unwrap_or(Decimal::ZERO) };
                let effective_vol = if use_internal_price { internal_volume_24h } else { Decimal::from_str(&ticker.volume).unwrap_or(Decimal::ZERO) };
                let effective_vol_ccy = if use_internal_price { internal_volume_ccy } else { Decimal::from_str(&ticker.quote_volume).unwrap_or(Decimal::ZERO) };
                let effective_open = if use_internal_price { if internal_open_24h > Decimal::ZERO { internal_open_24h } else { internal_last } } else { open_24h };
                let effective_change = effective_last - effective_open;
                let effective_change_pct = if effective_open > Decimal::ZERO { effective_change / effective_open * Decimal::from(100) } else { Decimal::ZERO };

                let data = PriceData {
                    symbol: symbol.clone(),
                    mark_price: effective_mark,
                    last_price: effective_last,
                    index_price,
                    bid_price: effective_last,
                    ask_price: effective_last,
                    funding_rate,
                    next_funding_rate: funding_rate,
                    next_funding_time,
                    open_24h: effective_open,
                    high_24h: effective_high,
                    low_24h: effective_low,
                    volume_24h: effective_vol,
                    volume_ccy_24h: effective_vol_ccy,
                    price_change_24h: effective_change,
                    price_change_percent_24h: effective_change_pct,
                    updated_at: now,
                };

                cache.insert(symbol.clone(), data);
            }
        }

        let count = cache.len();
        tracing::debug!("Updated prices for {} markets", count);
        Ok(())
    }

    /// Get list of all tracked markets
    pub async fn get_markets(&self) -> Vec<MarketInfo> {
        self.markets_cache.read().await.clone()
    }

    /// Get the number of active trading pairs on the exchange
    pub async fn active_symbols_count(&self) -> usize {
        self.active_symbols.read().await.len()
    }

    /// Check if a symbol is valid/tracked
    pub async fn is_valid_symbol(&self, symbol: &str) -> bool {
        let active = self.active_symbols.read().await;
        active.contains(&symbol.to_string())
    }

    /// Get cached price data for a symbol
    pub async fn get_price_data(&self, symbol: &str) -> Option<PriceData> {
        let cache = self.price_cache.read().await;
        let result = cache.get(symbol).cloned();
        
        // Add warning if cache is empty (helps debug ticker subscription issues)
        if result.is_none() && cache.is_empty() {
            tracing::warn!(
                "⚠️  Price cache is EMPTY! WebSocket ticker subscriptions will not receive data. \
                Make sure Binance feed or Auto Market Maker is enabled. Requested symbol: {}", 
                symbol
            );
        } else if result.is_none() {
            tracing::debug!(
                "Symbol '{}' not found in price cache. Available: {:?}", 
                symbol, 
                cache.keys().take(10).collect::<Vec<_>>()
            );
        }
        
        result
    }

    /// Get mark price for a symbol
    pub async fn get_mark_price(&self, symbol: &str) -> Option<Decimal> {
        self.get_price_data(symbol).await.map(|d| d.mark_price)
    }

    /// Batch get mark prices for multiple symbols (avoids N+1 queries)
    ///
    /// This method reads the cache once and returns prices for all requested symbols,
    /// which is much more efficient than calling `get_mark_price` in a loop.
    pub async fn batch_get_mark_prices(&self, symbols: &[String]) -> HashMap<String, Decimal> {
        let cache = self.price_cache.read().await;
        let mut result = HashMap::with_capacity(symbols.len());

        for symbol in symbols {
            if let Some(data) = cache.get(symbol) {
                result.insert(symbol.clone(), data.mark_price);
            }
        }

        result
    }

    /// Get last price for a symbol
    pub async fn get_last_price(&self, symbol: &str) -> Option<Decimal> {
        self.get_price_data(symbol).await.map(|d| d.last_price)
    }

    /// Get funding rate for a symbol
    pub async fn get_funding_rate(&self, symbol: &str) -> Option<Decimal> {
        self.get_price_data(symbol).await.map(|d| d.funding_rate)
    }

    /// Get all cached prices
    pub async fn get_all_prices(&self) -> HashMap<String, PriceData> {
        let cache = self.price_cache.read().await;
        cache.clone()
    }

    /// Get prices sorted by volume (top N)
    pub async fn get_top_prices(&self, limit: usize) -> Vec<PriceData> {
        let markets = self.markets_cache.read().await;
        let cache = self.price_cache.read().await;

        markets.iter()
            .take(limit)
            .filter_map(|m| cache.get(&m.symbol).cloned())
            .collect()
    }

    /// Force refresh prices for all symbols
    pub async fn refresh_all(&self) -> anyhow::Result<()> {
        self.refresh_markets().await?;
        self.update_all_prices().await
    }

    /// Hydrate price cache from database
    /// Queries both `virtual_trades` (market maker) and `trades` (real matching engine)
    /// to build accurate 24h volume and price statistics.
    pub async fn hydrate_from_db(&self, pool: &sqlx::PgPool) -> anyhow::Result<()> {
        let active_symbols = self.active_symbols.read().await.clone();
        tracing::info!("Hydrating price cache for {} symbols from virtual_trades + trades...", active_symbols.len());

        for symbol in active_symbols {
            // Query combined stats from both virtual_trades and trades tables
            // virtual_trades: uses millisecond timestamp column
            // trades: uses created_at (timestamptz) column
            let combined: (Option<String>, Option<String>, Option<String>, String, String) = sqlx::query_as(
                r#"
                WITH all_trades AS (
                    SELECT price::numeric as price, amount::numeric as amount, timestamp as ts_ms
                    FROM virtual_trades
                    WHERE symbol = $1
                      AND timestamp > (extract(epoch from NOW()) * 1000 - 86400000)::bigint
                    UNION ALL
                    SELECT price::numeric as price, amount::numeric as amount,
                           (extract(epoch from created_at) * 1000)::bigint as ts_ms
                    FROM trades
                    WHERE symbol = $1
                      AND created_at > NOW() - interval '24 hours'
                )
                SELECT
                    MAX(price)::text as "high",
                    MIN(price)::text as "low",
                    (SELECT price::text FROM all_trades ORDER BY ts_ms DESC LIMIT 1) as "last_price",
                    COALESCE(SUM(amount), 0)::text as "vol",
                    COALESCE(SUM(amount * price), 0)::text as "vol_usd"
                FROM all_trades
                "#
            )
            .bind(&symbol)
            .fetch_one(pool)
            .await?;

            // If no trades at all, also try to get the most recent trade (even outside 24h) for last_price
            let last_price_str = if let Some(ref lp) = combined.2 {
                lp.clone()
            } else {
                // Fallback: find most recent trade from either table
                let fallback: Option<(String,)> = sqlx::query_as(
                    r#"
                    SELECT price::text FROM (
                        SELECT price, timestamp as ts FROM virtual_trades WHERE symbol = $1
                        UNION ALL
                        SELECT price, (extract(epoch from created_at) * 1000)::bigint as ts FROM trades WHERE symbol = $1
                    ) combined ORDER BY ts DESC LIMIT 1
                    "#
                )
                .bind(&symbol)
                .fetch_optional(pool)
                .await?;
                match fallback {
                    Some(row) => row.0,
                    None => {
                        tracing::warn!("⚠️  No trades found for {} in DB, price cache pending...", symbol);
                        continue;
                    }
                }
            };

            // Get open price (earliest trade in 24h window)
            let open_trade: Option<(String,)> = sqlx::query_as(
                r#"
                SELECT price::text FROM (
                    SELECT price, timestamp as ts
                    FROM virtual_trades
                    WHERE symbol = $1
                      AND timestamp > (extract(epoch from NOW()) * 1000 - 86400000)::bigint
                    UNION ALL
                    SELECT price, (extract(epoch from created_at) * 1000)::bigint as ts
                    FROM trades
                    WHERE symbol = $1
                      AND created_at > NOW() - interval '24 hours'
                ) combined ORDER BY ts ASC LIMIT 1
                "#
            )
            .bind(&symbol)
            .fetch_optional(pool)
            .await?;

            let open_price = open_trade.map(|t| t.0).unwrap_or(last_price_str.clone());

            // Convert String to Decimal
            let last_price_dec = Decimal::from_str(&last_price_str).unwrap_or_default();
            let open_price_dec = Decimal::from_str(&open_price).unwrap_or_default();
            let high_dec = combined.0.as_ref().and_then(|v| Decimal::from_str(v).ok()).unwrap_or(last_price_dec);
            let low_dec = combined.1.as_ref().and_then(|v| Decimal::from_str(v).ok()).unwrap_or(last_price_dec);
            let vol_dec = Decimal::from_str(&combined.3).unwrap_or_default();
            let vol_usd_dec = Decimal::from_str(&combined.4).unwrap_or_default();

            let mut cache = self.price_cache.write().await;
            let entry = cache.entry(symbol.clone()).or_insert_with(|| PriceData {
                symbol: symbol.clone(),
                ..Default::default()
            });

            entry.last_price = last_price_dec;
            entry.mark_price = last_price_dec;
            entry.index_price = last_price_dec;
            entry.bid_price = last_price_dec;
            entry.ask_price = last_price_dec;
            entry.high_24h = high_dec;
            entry.low_24h = low_dec;
            entry.volume_24h = vol_dec;
            entry.volume_ccy_24h = vol_usd_dec;
            entry.open_24h = open_price_dec;
            entry.price_change_24h = last_price_dec - open_price_dec;
            entry.price_change_percent_24h = crate::safe_div!(
                entry.price_change_24h,
                open_price_dec,
                "price_feed: hydrate_from_db price change percent"
            ) * Decimal::from(100);
            entry.updated_at = chrono::Utc::now().timestamp_millis();

            tracing::info!("✅ Hydrated {} from DB: price={}, vol_24h_usd={}", symbol, last_price_str, vol_usd_dec);
        }
        Ok(())
    }

    /// Update price from internal trade (for use when Binance feed is disabled)
    pub async fn update_price_from_trade(&self, symbol: &str, price: Decimal, amount: Decimal) {
        let mut cache = self.price_cache.write().await;
        let now = chrono::Utc::now().timestamp_millis();

        let entry = cache.entry(symbol.to_string()).or_insert_with(|| PriceData {
            symbol: symbol.to_string(),
            ..Default::default()
        });

        // Update prices
        entry.last_price = price;
        entry.mark_price = price;
        entry.index_price = price;
        entry.bid_price = price;
        entry.ask_price = price;
        entry.updated_at = now;

        // Update 24h stats (simple accumulation for now)
        if entry.high_24h < price || entry.high_24h == Decimal::ZERO {
            entry.high_24h = price;
        }
        if entry.low_24h > price || entry.low_24h == Decimal::ZERO {
            entry.low_24h = price;
        }
        if entry.open_24h == Decimal::ZERO {
            entry.open_24h = price;
        }
        entry.volume_24h += amount;
        entry.volume_ccy_24h += amount * price;
        entry.price_change_24h = price - entry.open_24h;
        // Use safe division to prevent panic
        entry.price_change_percent_24h = crate::safe_div!(
            entry.price_change_24h,
            entry.open_24h,
            "price_feed: update_price_from_trade price change percent"
        ) * Decimal::from(100);

        tracing::debug!("Updated price for {} from internal trade: {}", symbol, price);
    }

    // update_mark_price_from_external / batch_update_mark_prices_from_external
    // were removed: PriceCache must be driven solely by `update_price_from_trade`
    // (matching engine output) and `hydrate_from_db`. Allowing external sources
    // (Hyperliquid spot @182 quoted XAUUSDT at ~0.857 instead of $/oz gold)
    // to write here surfaced as the "$4645 ↔ $0.85" flicker on the frontend.

    /// Refresh 24h volume stats from database (call periodically to keep volumes accurate)
    /// This recomputes volume_24h from both trades and virtual_trades tables,
    /// replacing the in-memory accumulation which can drift over time.
    pub async fn refresh_volumes_from_db(&self, pool: &sqlx::PgPool) -> anyhow::Result<()> {
        let active_symbols = self.active_symbols.read().await.clone();

        for symbol in &active_symbols {
            let stats: (String, String) = sqlx::query_as(
                r#"
                WITH all_trades AS (
                    SELECT price::numeric as price, amount::numeric as amount
                    FROM virtual_trades
                    WHERE symbol = $1
                      AND timestamp > (extract(epoch from NOW()) * 1000 - 86400000)::bigint
                    UNION ALL
                    SELECT price::numeric as price, amount::numeric as amount
                    FROM trades
                    WHERE symbol = $1
                      AND created_at > NOW() - interval '24 hours'
                )
                SELECT
                    COALESCE(SUM(amount), 0)::text as "vol",
                    COALESCE(SUM(amount * price), 0)::text as "vol_usd"
                FROM all_trades
                "#
            )
            .bind(symbol)
            .fetch_one(pool)
            .await?;

            let vol_dec = Decimal::from_str(&stats.0).unwrap_or_default();
            let vol_usd_dec = Decimal::from_str(&stats.1).unwrap_or_default();

            let mut cache = self.price_cache.write().await;
            if let Some(entry) = cache.get_mut(symbol) {
                entry.volume_24h = vol_dec;
                entry.volume_ccy_24h = vol_usd_dec;
            }
        }

        tracing::debug!("Refreshed 24h volumes from DB for {} symbols", active_symbols.len());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    // Internal trades are the *only* sanctioned writer of PriceCache.
    // After a trade, last_price/mark_price/index_price must all reflect the
    // matching-engine-produced price — there's no longer any external feed
    // mechanism that could race against this and rewrite index_price to a
    // bogus value (the XAUUSDT $0.85 incident).
    #[tokio::test]
    async fn internal_trade_sets_all_price_fields_consistently() {
        let svc = PriceFeedService::new();
        svc.update_price_from_trade("XAUUSDT", dec("4645"), dec("1")).await;

        let cache = svc.price_cache.read().await;
        let entry = cache.get("XAUUSDT").expect("XAUUSDT in cache");
        assert_eq!(entry.last_price, dec("4645"));
        assert_eq!(entry.mark_price, dec("4645"));
        assert_eq!(entry.index_price, dec("4645"));
    }

    #[tokio::test]
    async fn subsequent_trade_overwrites_all_price_fields() {
        let svc = PriceFeedService::new();
        svc.update_price_from_trade("BTCUSDT", dec("80000"), dec("1")).await;
        svc.update_price_from_trade("BTCUSDT", dec("80500"), dec("0.1")).await;

        let cache = svc.price_cache.read().await;
        let entry = cache.get("BTCUSDT").unwrap();
        assert_eq!(entry.last_price, dec("80500"));
        assert_eq!(entry.mark_price, dec("80500"));
        assert_eq!(entry.index_price, dec("80500"));
    }
}
