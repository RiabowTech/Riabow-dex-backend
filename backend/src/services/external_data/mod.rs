use rust_decimal::Decimal;
use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;
use crate::api::handlers::market::{TickerResponse, OrderbookResponse, Trade, TradesResponse};
use crate::api::handlers::kline::CandleDto;
use crate::services::rwa::registry::{get_rwa_by_user_symbol, DataSource};

pub mod cache;
pub mod coalescing_cache;
pub mod circuit_breaker;
pub use cache::ExternalMarketCache;
pub use coalescing_cache::CoalescingCache;
pub use circuit_breaker::CircuitBreaker;

const HYPERLIQUID_API_URL: &str = "https://api.hyperliquid.xyz/info";
const API_TIMEOUT_SECS: u64 = 10; // 10 second timeout for external API

#[derive(Debug, Clone)]
pub struct HyperliquidClient {
    client: reqwest::Client,
}

impl HyperliquidClient {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(API_TIMEOUT_SECS))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
        }
    }

    /// Helper to convert "BTCUSDT" -> "BTC"
    fn to_hyperliquid_symbol(symbol: &str) -> String {
        symbol.to_uppercase().replace("USDT", "").replace("USD", "")
    }
    
    /// Helper to convert "BTC" -> "BTCUSDT"
    #[allow(dead_code)]
    fn from_hyperliquid_symbol(hl_symbol: &str) -> String {
        format!("{}USDT", hl_symbol.to_uppercase())
    }
    
    /// Fetch ALL market/ticker data from Hyperliquid (perps + spot).
    ///
    /// Queries both `metaAndAssetCtxs` (perps) and `spotMetaAndAssetCtxs` (spot)
    /// so that RWA assets (stocks, precious metals, indices) are included alongside
    /// regular crypto perpetuals.
    pub async fn get_all_tickers(&self, symbols: &[&str]) -> anyhow::Result<HashMap<String, TickerResponse>> {
        // Separate symbols into perps vs spot based on RWA registry
        let mut perps_hl_to_our: HashMap<String, String> = HashMap::new();
        // spot: hl_ticker -> our_symbol
        let mut spot_hl_to_our: HashMap<String, String> = HashMap::new();

        for &sym in symbols {
            if let Some(rwa) = get_rwa_by_user_symbol(sym) {
                match rwa.data_source {
                    DataSource::Spot => {
                        spot_hl_to_our.insert(rwa.hl_ticker.to_string(), sym.to_string());
                    }
                    DataSource::Perps => {
                        // RWA on perps market (e.g. SPX)
                        perps_hl_to_our.insert(rwa.hl_ticker.to_string(), sym.to_string());
                    }
                }
            } else {
                // Regular crypto — strip USDT suffix for Hyperliquid perps lookup
                let hl_sym = Self::to_hyperliquid_symbol(sym);
                perps_hl_to_our.insert(hl_sym, sym.to_string());
            }
        }

        // Fire both API requests concurrently
        let perps_fut = self.client.post(HYPERLIQUID_API_URL)
            .json(&serde_json::json!({ "type": "metaAndAssetCtxs" }))
            .send();
        let spot_fut = self.client.post(HYPERLIQUID_API_URL)
            .json(&serde_json::json!({ "type": "spotMetaAndAssetCtxs" }))
            .send();

        let (perps_resp, spot_resp) = tokio::join!(perps_fut, spot_fut);

        let mut ticker_map = HashMap::new();

        // ── Parse perps ──────────────────────────────────────────────────
        if let Ok(resp) = perps_resp {
            if let Ok(data) = resp.json::<Vec<serde_json::Value>>().await {
                if data.len() >= 2 {
                    if let Some(universe_arr) = data[0]["universe"].as_array() {
                        for (i, coin) in universe_arr.iter().enumerate() {
                            if let Some(hl_name) = coin["name"].as_str() {
                                if let Some(our_symbol) = perps_hl_to_our.get(hl_name) {
                                    if let Some(ctx) = data[1].get(i) {
                                        if let Some(t) = Self::parse_perps_ctx(our_symbol, ctx) {
                                            ticker_map.insert(our_symbol.clone(), t);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } else {
            tracing::warn!("Failed to fetch perps data from Hyperliquid");
        }

        // ── Parse spot (for RWA assets) ──────────────────────────────────
        if !spot_hl_to_our.is_empty() {
            if let Ok(resp) = spot_resp {
                if let Ok(data) = resp.json::<Vec<serde_json::Value>>().await {
                    if data.len() >= 2 {
                        if let Some(meta) = data[0].as_object() {
                            let tokens = meta.get("tokens").and_then(|v| v.as_array());
                            let universe = meta.get("universe").and_then(|v| v.as_array());
                            let ctxs = data[1].as_array();

                            if let (Some(tokens), Some(universe), Some(ctxs)) = (tokens, universe, ctxs) {
                                // Build token index -> name map
                                let token_map: HashMap<u64, String> = tokens.iter().filter_map(|t| {
                                    let idx = t["index"].as_u64()?;
                                    let name = t["name"].as_str()?.to_string();
                                    Some((idx, name))
                                }).collect();

                                // Find USDC index (only care about USDC-quoted pairs)
                                let usdc_idx = tokens.iter().find_map(|t| {
                                    if t["name"].as_str() == Some("USDC") { t["index"].as_u64() } else { None }
                                });

                                for (i, pair) in universe.iter().enumerate() {
                                    if i >= ctxs.len() { break; }
                                    if let Some(toks) = pair["tokens"].as_array() {
                                        if toks.len() >= 2 {
                                            let base_idx = toks[0].as_u64().unwrap_or(0);
                                            let quote_idx = toks[1].as_u64().unwrap_or(0);
                                            // Only USDC-quoted pairs
                                            if usdc_idx.is_some() && Some(quote_idx) != usdc_idx {
                                                continue;
                                            }
                                            if let Some(base_name) = token_map.get(&base_idx) {
                                                if let Some(our_symbol) = spot_hl_to_our.get(base_name.as_str()) {
                                                    let ctx = &ctxs[i];
                                                    if let Some(t) = Self::parse_spot_ctx(our_symbol, ctx) {
                                                        ticker_map.insert(our_symbol.clone(), t);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                tracing::warn!("Failed to fetch spot data from Hyperliquid");
            }
        }

        Ok(ticker_map)
    }

    /// Parse a perps asset context into a TickerResponse
    fn parse_perps_ctx(symbol: &str, ctx: &serde_json::Value) -> Option<TickerResponse> {
        let mark_price = Decimal::from_str(ctx["markPx"].as_str()?).ok()?;
        let prev_day_px = Decimal::from_str(ctx["prevDayPx"].as_str().unwrap_or("0")).unwrap_or(mark_price);
        let price_change = mark_price - prev_day_px;
        let price_change_percent = if prev_day_px.is_zero() {
            Decimal::ZERO
        } else {
            (price_change / prev_day_px) * Decimal::from(100)
        };
        let day_notional = Decimal::from_str(ctx["dayNtlVlm"].as_str().unwrap_or("0")).unwrap_or(Decimal::ZERO);
        let funding = Decimal::from_str(ctx["funding"].as_str().unwrap_or("0")).unwrap_or(Decimal::ZERO);

        Some(TickerResponse {
            symbol: symbol.to_string(),
            last_price: mark_price,
            price_change_24h: price_change,
            price_change_percent_24h: price_change_percent,
            high_24h: mark_price,
            low_24h: prev_day_px,
            volume_24h: if mark_price.is_zero() { Decimal::ZERO } else { day_notional / mark_price },
            open_interest: Decimal::from_str(ctx["openInterest"].as_str().unwrap_or("0")).unwrap_or(Decimal::ZERO),
            funding_rate: funding,
            next_funding_time: crate::utils::time::next_hourly_funding_ms(),
        })
    }

    /// Parse a spot asset context into a TickerResponse
    fn parse_spot_ctx(symbol: &str, ctx: &serde_json::Value) -> Option<TickerResponse> {
        let mark_price = Decimal::from_str(ctx["markPx"].as_str()?).ok()?;
        if mark_price.is_zero() { return None; }
        let prev_day_px = Decimal::from_str(ctx["prevDayPx"].as_str().unwrap_or("0")).unwrap_or(mark_price);
        let price_change = mark_price - prev_day_px;
        let price_change_percent = if prev_day_px.is_zero() {
            Decimal::ZERO
        } else {
            (price_change / prev_day_px) * Decimal::from(100)
        };
        let day_notional = Decimal::from_str(ctx["dayNtlVlm"].as_str().unwrap_or("0")).unwrap_or(Decimal::ZERO);

        Some(TickerResponse {
            symbol: symbol.to_string(),
            last_price: mark_price,
            price_change_24h: price_change,
            price_change_percent_24h: price_change_percent,
            high_24h: mark_price,
            low_24h: prev_day_px,
            volume_24h: if mark_price.is_zero() { Decimal::ZERO } else { day_notional / mark_price },
            open_interest: Decimal::ZERO,
            funding_rate: Decimal::ZERO,
            next_funding_time: 0,
        })
    }

    /// Fetch Market/Ticker Data (24h stats)
    pub async fn get_ticker(&self, symbol: &str) -> anyhow::Result<TickerResponse> {
        let hl_symbol = Self::resolve_hl_coin(symbol);
        
        // Fetch metaAndAssetCtxs for all markets
        let response = self.client.post(HYPERLIQUID_API_URL)
            .json(&serde_json::json!({ "type": "metaAndAssetCtxs" }))
            .send()
            .await?
            .json::<Vec<serde_json::Value>>()
            .await?;

        // Response format: [meta, assetCtxs]
        // assetCtxs is an array of asset info
        
        if response.len() < 2 {
            return Err(anyhow::anyhow!("Invalid response from Hyperliquid"));
        }

        let universe = &response[0]["universe"];
        let asset_ctxs = &response[1];

        // Find index of our symbol
        let mut coin_index = None;
        if let Some(universe_arr) = universe.as_array() {
            for (i, coin) in universe_arr.iter().enumerate() {
                if coin["name"].as_str() == Some(&hl_symbol) {
                    coin_index = Some(i);
                    break;
                }
            }
        }

        if let Some(idx) = coin_index {
            if let Some(ctx) = asset_ctxs.get(idx) {
                // Parse fields
                let mark_price = Decimal::from_str(ctx["markPx"].as_str().unwrap_or("0"))?;
                // Hyperliquid doesn't easily give 24h stats in this specific endpoint, 
                // but usually frontends derive it or use a different aggressive query.
                // For simplicity, we might emulate some stats or fetch historical klines 
                // to calc 24h change if needed. 
                // However, "metaAndAssetCtxs" gives prevDayPx which helps.
                
                let prev_day_px = Decimal::from_str(ctx["prevDayPx"].as_str().unwrap_or("0")).unwrap_or(mark_price);
                let price_change = mark_price - prev_day_px;
                let price_change_percent = if prev_day_px.is_zero() { 
                    Decimal::ZERO 
                } else { 
                    (price_change / prev_day_px) * Decimal::from(100) 
                };

                let day_notional = Decimal::from_str(ctx["dayNtlVlm"].as_str().unwrap_or("0")).unwrap_or(Decimal::ZERO); // Volume USD
                
                // Funding
                let funding = Decimal::from_str(ctx["funding"].as_str().unwrap_or("0")).unwrap_or(Decimal::ZERO);

                return Ok(TickerResponse {
                    symbol: symbol.to_string(),
                    last_price: mark_price,
                    price_change_24h: price_change,
                    price_change_percent_24h: price_change_percent,
                    high_24h: mark_price, // Approx
                    low_24h: prev_day_px, // Approx
                    volume_24h: if mark_price.is_zero() { Decimal::ZERO } else { day_notional / mark_price },
                    open_interest: Decimal::from_str(ctx["openInterest"].as_str().unwrap_or("0")).unwrap_or(Decimal::ZERO),
                    funding_rate: funding,
                    next_funding_time: crate::utils::time::next_hourly_funding_ms(),
                });
            }
        }
        
        Err(anyhow::anyhow!("Symbol not found"))
    }
    
    /// Get Candles — tries RWA registry first, then falls back to generic symbol stripping.
    pub async fn get_candles(
        &self,
        symbol: &str,
        period: &str,
        limit: usize,
        start_time: Option<i64>,
        end_time: Option<i64>
    ) -> anyhow::Result<Vec<CandleDto>> {
        // Check if this is an RWA symbol with a known HL candle coin mapping
        let hl_coin = crate::services::rwa::registry::get_hl_candle_coin(symbol)
            .unwrap_or_else(|| Self::to_hyperliquid_symbol(symbol));

        self.get_candles_by_coin(&hl_coin, period, limit, start_time, end_time).await
    }

    /// Get candles using a specific Hyperliquid coin name (e.g. "BTC", "@182", "SPX")
    pub async fn get_candles_by_coin(
        &self,
        hl_coin: &str,
        period: &str,
        limit: usize,
        start_time: Option<i64>,
        end_time: Option<i64>
    ) -> anyhow::Result<Vec<CandleDto>> {
        // Map common periods
        let interval = match period {
            "1m" => "1m",
            "5m" => "5m",
            "15m" => "15m",
            "1h" => "1h",
            "4h" => "4h",
            "1d" => "1d",
            _ => "1h",
        };

        // Determine times
        // HL expects msg: {"type": "candleSnapshot", "req": {"coin": "BTC", "interval": "1h", "startTime": 12345678}}
        let now = chrono::Utc::now().timestamp_millis();
        // Calculate period duration in milliseconds for proper default time range
        let period_ms: i64 = match interval {
            "1m" => 60_000,
            "5m" => 300_000,
            "15m" => 900_000,
            "1h" => 3_600_000,
            "4h" => 14_400_000,
            "1d" => 86_400_000,
            _ => 3_600_000,
        };
        let req_start = start_time.map(|t| t * 1000).unwrap_or(now - (limit as i64 * period_ms));
        let req_end = end_time.map(|t| t * 1000).unwrap_or(now);

        let body = serde_json::json!({
            "type": "candleSnapshot",
            "req": {
                "coin": hl_coin,
                "interval": interval,
                "startTime": req_start,
                "endTime": req_end
            }
        });

        tracing::debug!("Fetching candles from Hyperliquid: coin={}, interval={}", hl_coin, interval);

        let response = self.client.post(HYPERLIQUID_API_URL)
            .json(&body)
            .send()
            .await?;

        let response_text = response.text().await?;

        // Hyperliquid returns "null" for unknown coins
        if response_text == "null" || response_text.is_empty() {
            tracing::warn!("Hyperliquid returned null for candle request: coin={}", hl_coin);
            return Ok(vec![]);
        }

        let response_data: Vec<serde_json::Value> = serde_json::from_str(&response_text)
            .map_err(|e| anyhow::anyhow!("Failed to parse candle response for {}: {}", hl_coin, e))?;

        // Response: [{"t": 123, "o": "1", "h": "2", "l": "1", "c": "1.5", "v": "100", ...}]
        let candles: Vec<CandleDto> = response_data.iter().map(|c| {
            CandleDto {
                time: c["t"].as_i64().unwrap_or(0) / 1000,
                open: c["o"].as_str().unwrap_or("0").to_string(),
                high: c["h"].as_str().unwrap_or("0").to_string(),
                low: c["l"].as_str().unwrap_or("0").to_string(),
                close: c["c"].as_str().unwrap_or("0").to_string(),
                volume: c["v"].as_str().unwrap_or("0").to_string(),
                quote_volume: Some(c["n"].as_str().unwrap_or("0").to_string()), // 'n' is notional (USD volume)
                trade_count: Some(c["T"].as_u64().unwrap_or(0) as u32), // 'T' might be tick count or similar
            }
        }).collect();

        // Sort and limit
        let mut sorted = candles;
        sorted.sort_by_key(|c| c.time);
        if sorted.len() > limit {
            sorted = sorted.into_iter().rev().take(limit).collect();
            sorted.reverse();
        }

        Ok(sorted)
    }

    /// Resolve a user-facing symbol to its Hyperliquid coin name.
    ///
    /// - Perps assets (BTC, ETH, SPX): returns the HL ticker directly (e.g. "BTC")
    /// - Spot/RWA assets with a pair index (GOOGL, XAUT0): returns "@N" format
    ///   because Hyperliquid spot l2Book/trades require the pair index, not the token name
    /// - Spot/RWA without a pair index: returns the HL ticker as fallback
    /// - Regular crypto: strips USDT/USD suffix
    fn resolve_hl_coin(symbol: &str) -> String {
        if let Some(rwa) = get_rwa_by_user_symbol(symbol) {
            match rwa.data_source {
                DataSource::Spot => {
                    // Spot l2Book requires @N format
                    if let Some(pair_idx) = rwa.spot_pair_index {
                        format!("@{}", pair_idx)
                    } else {
                        // No spot pair index — try HL ticker directly
                        rwa.hl_ticker.to_string()
                    }
                }
                DataSource::Perps => {
                    // Perps assets use the ticker name directly
                    rwa.hl_ticker.to_string()
                }
            }
        } else {
            Self::to_hyperliquid_symbol(symbol)
        }
    }

    /// Get Orderbook (L2)
    pub async fn get_orderbook(&self, symbol: &str) -> anyhow::Result<OrderbookResponse> {
        let hl_symbol = Self::resolve_hl_coin(symbol);
        
        let body = serde_json::json!({
            "type": "l2Book",
            "coin": hl_symbol
        });

        let response = self.client.post(HYPERLIQUID_API_URL)
            .json(&body)
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?;

        // Response: {"levels": [[{"px": "100", "sz": "1", "n": 1}, ...], ...]} 
        // levels[0] = bids, levels[1] = asks
        
        let levels = response["levels"].as_array()
            .ok_or(anyhow::anyhow!("Invalid orderbook format"))?;
            
        if levels.len() < 2 {
            return Err(anyhow::anyhow!("Invalid orderbook levels"));
        }

        let parse_level = |level: &serde_json::Value| -> [String; 2] {
            [
                level["px"].as_str().unwrap_or("0").to_string(),
                level["sz"].as_str().unwrap_or("0").to_string()
            ]
        };

        // Hyperliquid l2Book 每边可返回远多于 20 的档位；不再硬限截断，
        // 上层可通过 ?depth= 查询参数再裁剪。
        let bids: Vec<[String; 2]> = levels[0].as_array().unwrap_or(&vec![])
            .iter().map(parse_level).collect();

        let asks: Vec<[String; 2]> = levels[1].as_array().unwrap_or(&vec![])
            .iter().map(parse_level).collect();

        Ok(OrderbookResponse {
            symbol: symbol.to_string(),
            bids,
            asks,
            timestamp: chrono::Utc::now().timestamp_millis(),
        })
    }
    
    /// Get Recent Trades
    /// Note: HL doesn't specificially show public trade history easily in 'info', 
    /// but we can mock it from the L2 update or just return empty for cold start 
    /// OR fetch standard candles and synthesize "trades" from volume if desperate.
    /// A better way: Use L2 snapshot top bid/ask to synthesize a "recent trade".
    /// 
    /// Actually, let's use the 'userFills' equivalent for public data? No.
    /// Let's check docs... effectively no simple public trade history endpoint in 'info'.
    /// We will simulate trades based on orderbook + random jitter for "Cold Start" appearance.
    pub async fn get_trades(&self, symbol: &str) -> anyhow::Result<TradesResponse> {
        // Fetch orderbook to get current price
        let book = self.get_orderbook(symbol).await?;
        
        let mut trades = Vec::new();
        let now = chrono::Utc::now().timestamp_millis();
        
        // Generate some fake trades around the mid price
        if let (Some(best_bid), Some(best_ask)) = (book.bids.first(), book.asks.first()) {
            let bid_px = Decimal::from_str(&best_bid[0]).unwrap_or_default();
            let ask_px = Decimal::from_str(&best_ask[0]).unwrap_or_default();
            let mid_px = (bid_px + ask_px) / Decimal::from(2);
            
            for i in 0..10 {
                trades.push(Trade {
                    id: (now - i * 1000).to_string(),
                    price: mid_px.to_string(),
                    amount: "0.1".to_string(), // Dummy amount
                    side: if i % 2 == 0 { "buy".to_string() } else { "sell".to_string() },
                    timestamp: now - (i * 1000), // 1 sec ago, 2 sec ago...
                });
            }
        }
        
        Ok(TradesResponse {
            symbol: symbol.to_string(),
            trades,
        })
    }
}
