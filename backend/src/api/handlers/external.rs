use axum::{
    extract::{Path, Query, State},
    http::{StatusCode, HeaderMap, HeaderValue},
    Json,
};
use tokio::sync::OnceCell;
use std::sync::{Arc, OnceLock};
use std::collections::HashMap;
use std::time::Duration;
use rust_decimal::Decimal;
use crate::AppState;
use crate::services::external_data::{HyperliquidClient, ExternalMarketCache, CoalescingCache, CircuitBreaker};
use crate::api::handlers::market::{
    ErrorResponse, OrderbookResponse, TradesResponse,
    MarketsQuery, MarketsResponse, Market
};
use crate::api::handlers::kline::{CandlesQuery, CandlesResponse};

// ── Shared infrastructure (initialized once, reused across all requests) ─────

// Global cache for external market data (60 second TTL)
static MARKET_CACHE: OnceCell<ExternalMarketCache> = OnceCell::const_new();

// Shared HyperliquidClient — reuses the connection pool instead of creating a new
// client per request.
static SHARED_HL_CLIENT: OnceLock<HyperliquidClient> = OnceLock::new();

fn get_shared_client() -> &'static HyperliquidClient {
    SHARED_HL_CLIENT.get_or_init(HyperliquidClient::new)
}

// Coalescing caches: concurrent requests for the same key coalesce into a single
// upstream API call.
static ORDERBOOK_CACHE: OnceLock<CoalescingCache<OrderbookResponse>> = OnceLock::new();
static CANDLES_CACHE: OnceLock<CoalescingCache<Vec<crate::api::handlers::kline::CandleDto>>> = OnceLock::new();

fn orderbook_cache() -> &'static CoalescingCache<OrderbookResponse> {
    ORDERBOOK_CACHE.get_or_init(|| CoalescingCache::new(Duration::from_secs(3)))
}

fn candles_cache() -> &'static CoalescingCache<Vec<crate::api::handlers::kline::CandleDto>> {
    CANDLES_CACHE.get_or_init(|| CoalescingCache::new(Duration::from_secs(30)))
}

// Per-endpoint circuit breakers: 5 consecutive failures → open for 30 s.
static ORDERBOOK_BREAKER: OnceLock<CircuitBreaker> = OnceLock::new();
static CANDLES_BREAKER: OnceLock<CircuitBreaker> = OnceLock::new();

fn orderbook_breaker() -> &'static CircuitBreaker {
    ORDERBOOK_BREAKER.get_or_init(|| CircuitBreaker::new(5, 30_000))
}

fn candles_breaker() -> &'static CircuitBreaker {
    CANDLES_BREAKER.get_or_init(|| CircuitBreaker::new(5, 30_000))
}

async fn get_market_cache() -> &'static ExternalMarketCache {
    MARKET_CACHE.get_or_init(|| async {
        ExternalMarketCache::new(60) // 60 second cache
    }).await
}

// Removed old unused MarketConfig

// Re-implement normalize_symbol locally to be safe
fn normalize_symbol(symbol: &str) -> String {
    let upper = symbol.to_uppercase();
    if !upper.contains('-') && !upper.contains('/') && !upper.contains('_') {
        return upper;
    }
    if upper.ends_with("-USD") {
        let base = upper.strip_suffix("-USD").unwrap_or(&upper);
        return format!("{}USDT", base);
    }
    if upper.contains("-USDT") {
        return upper.replace("-", "");
    }
    if upper.contains("/") || upper.contains("_") {
        let cleaned = upper.replace("/", "").replace("_", "");
        if !cleaned.ends_with("USDT") && cleaned.ends_with("USD") {
            let base = cleaned.strip_suffix("USD").unwrap_or(&cleaned);
            return format!("{}USDT", base);
        }
        return cleaned;
    }
    upper
}

/// External Market List (Optimized with caching and batch fetching)
/// 
/// Performance improvements:
/// - Single API call instead of 50 sequential calls
/// - 60-second cache to prevent repeated API hits
/// - 10-second timeout on external API
/// - Consistent response (all or none from cache)
pub async fn list_markets_external(
    State(state): State<Arc<AppState>>,
    Query(_query): Query<MarketsQuery>,
) -> Result<Json<MarketsResponse>, StatusCode> {
    let cache = get_market_cache().await;
    
    // Fetch active market configurations from database
    let active_configs = state.market_config_service.get_all_active().await;

    // Use string symbols and create a fast lookup map for configuration
    let mut configs = HashMap::new();
    let mut symbols = Vec::new();
    
    for config in active_configs {
        symbols.push(config.symbol.clone());
        configs.insert(config.symbol.clone(), config);
    }

    if symbols.is_empty() {
        return Ok(Json(MarketsResponse {
            markets: vec![],
            total: 0,
        }));
    }

    // Convert Vec<String> to Vec<&str> for HyperliquidClient API
    let symbol_strs: Vec<&str> = symbols.iter().map(|s| s.as_str()).collect();

    // Try to get from cache first
    if let Some(cached_tickers) = cache.get().await {
        
        let mut markets = Vec::new();
        for (i, sym) in symbols.iter().enumerate() {
            if let Some(ticker) = cached_tickers.get(sym) {
                let (market_type, leverage) = if let Some(config) = configs.get(sym) {
                    (config.category.clone(), config.max_leverage)
                } else {
                    ("crypto".to_string(), 50)
                };

                markets.push(Market {
                    symbol: sym.to_string(),
                    base_asset: sym.replace("USDT", ""),
                    quote_asset: "USDT".to_string(),
                    last_price: ticker.last_price,
                    price_change_24h: ticker.price_change_24h,
                    price_change_percent_24h: ticker.price_change_percent_24h,
                    high_24h: ticker.high_24h,
                    low_24h: ticker.low_24h,
                    volume_24h: ticker.volume_24h,
                    volume_24h_usd: ticker.volume_24h * ticker.last_price,
                    rank: i + 1,
                    market_type,
                    leverage,
                    base_maker_fee_rate: configs.get(sym).map(|c| c.base_maker_fee_rate).unwrap_or(Decimal::new(2, 4)),
                    base_taker_fee_rate: configs.get(sym).map(|c| c.base_taker_fee_rate).unwrap_or(Decimal::new(5, 4)),
                    maintenance_margin_rate: configs.get(sym).map(|c| c.maintenance_margin_rate).unwrap_or(Decimal::new(5, 3)),
                    max_position_size_usd: configs.get(sym).map(|c| c.max_position_size_usd).unwrap_or(Decimal::new(10_000_000, 0)),
                    min_order_size_usd: configs.get(sym).map(|c| c.min_order_size_usd).unwrap_or(Decimal::new(10, 0)),
                    lot_size: configs.get(sym).map(|c| c.lot_size).unwrap_or(Decimal::new(1, 3)),
                    tick_size: configs.get(sym).map(|c| c.tick_size).unwrap_or(Decimal::new(1, 1)),
                });
            }
        }

        tracing::debug!("Served {} markets from cache (out of {} active configs)", markets.len(), symbols.len());

        let total = markets.len();
        return Ok(Json(MarketsResponse {
            markets,
            total,
        }));
    }
    
    // Cache miss or expired - fetch fresh data
    let client = get_shared_client();
    // Fetch all tickers in ONE API call
    match client.get_all_tickers(&symbol_strs).await {
        Ok(ticker_map) => {
            // Cache the results
            cache.set(ticker_map.clone()).await;
            
            let mut markets = Vec::new();
            for (i, sym) in symbols.iter().enumerate() {
                if let Some(ticker) = ticker_map.get(sym) {
                    let (market_type, leverage) = if let Some(config) = configs.get(sym) {
                        (config.category.clone(), config.max_leverage)
                    } else {
                        ("crypto".to_string(), 50)
                    };

                    markets.push(Market {
                        symbol: sym.to_string(),
                        base_asset: sym.replace("USDT", ""),
                        quote_asset: "USDT".to_string(),
                        last_price: ticker.last_price,
                        price_change_24h: ticker.price_change_24h,
                        price_change_percent_24h: ticker.price_change_percent_24h,
                        high_24h: ticker.high_24h,
                        low_24h: ticker.low_24h,
                        volume_24h: ticker.volume_24h,
                        volume_24h_usd: ticker.volume_24h * ticker.last_price,
                        rank: i + 1,
                        market_type,
                        leverage,
                        base_maker_fee_rate: configs.get(sym).map(|c| c.base_maker_fee_rate).unwrap_or(Decimal::new(2, 4)),
                        base_taker_fee_rate: configs.get(sym).map(|c| c.base_taker_fee_rate).unwrap_or(Decimal::new(5, 4)),
                        maintenance_margin_rate: configs.get(sym).map(|c| c.maintenance_margin_rate).unwrap_or(Decimal::new(5, 3)),
                        max_position_size_usd: configs.get(sym).map(|c| c.max_position_size_usd).unwrap_or(Decimal::new(10_000_000, 0)),
                        min_order_size_usd: configs.get(sym).map(|c| c.min_order_size_usd).unwrap_or(Decimal::new(10, 0)),
                        lot_size: configs.get(sym).map(|c| c.lot_size).unwrap_or(Decimal::new(1, 3)),
                        tick_size: configs.get(sym).map(|c| c.tick_size).unwrap_or(Decimal::new(1, 1)),
                    });
                }
            }

            tracing::info!("Fetched and cached {} markets from Hyperliquid (out of {} active configs)", markets.len(), symbols.len());

            if markets.is_empty() {
                return Err(StatusCode::SERVICE_UNAVAILABLE);
            }

            let total = markets.len();
            Ok(Json(MarketsResponse {
                markets,
                total,
            }))
        }
        Err(e) => {
            tracing::error!("Failed to fetch markets from Hyperliquid: {}", e);
            Err(StatusCode::SERVICE_UNAVAILABLE)
        }
    }
}

// get_ticker_external removed — the route now aliases to handlers::market::get_ticker
// (which reads PriceCache, populated only by MM-bot trades). External price feeds
// must never reach user-facing ticker fields. See routes/mod.rs.

/// External Orderbook — cached (3 s TTL) + request coalescing + circuit breaker.
#[derive(Debug, serde::Deserialize)]
pub struct OrderbookQuery {
    /// Optional per-side depth cap (e.g. 20/50/100). Default: full upstream depth.
    pub depth: Option<usize>,
}

pub async fn get_orderbook_external(
    Path(symbol): Path<String>,
    Query(query): Query<OrderbookQuery>,
) -> Result<(StatusCode, HeaderMap, Json<OrderbookResponse>), (StatusCode, Json<ErrorResponse>)> {
    let normalized = normalize_symbol(&symbol);
    let trim = |mut ob: OrderbookResponse| -> OrderbookResponse {
        if let Some(d) = query.depth {
            ob.bids.truncate(d);
            ob.asks.truncate(d);
        }
        ob
    };
    let cache = orderbook_cache();
    let breaker = orderbook_breaker();
    let client = get_shared_client();

    // If circuit is open, try to return stale data immediately.
    if !breaker.allow_request() {
        if let Some(stale) = cache.get_stale(&normalized) {
            let mut headers = HeaderMap::new();
            headers.insert("X-Data-Stale", HeaderValue::from_static("true"));
            return Ok((StatusCode::OK, headers, Json(trim((*stale).clone()))));
        }
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "Upstream circuit open and no cached data available".to_string(),
                code: "CIRCUIT_OPEN".to_string(),
            }),
        ));
    }

    let norm_clone = normalized.clone();
    let result = cache.get_or_fetch(&normalized, || async {
        client.get_orderbook(&norm_clone).await
    }).await;

    match result {
        Ok(book) => {
            breaker.record_success();
            Ok((StatusCode::OK, HeaderMap::new(), Json(trim((*book).clone()))))
        }
        Err(e) => {
            breaker.record_failure();
            // Stale-while-revalidate: return expired data on upstream failure.
            if let Some(stale) = cache.get_stale(&normalized) {
                let mut headers = HeaderMap::new();
                headers.insert("X-Data-Stale", HeaderValue::from_static("true"));
                return Ok((StatusCode::OK, headers, Json(trim((*stale).clone()))));
            }
            Err((
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: format!("External data unavailable: {}", e),
                    code: "EXTERNAL_ERROR".to_string(),
                }),
            ))
        }
    }
}

/// External Trades — uses the shared HyperliquidClient (connection pool reuse).
pub async fn get_trades_external(
    Path(symbol): Path<String>,
) -> Result<Json<TradesResponse>, (StatusCode, Json<ErrorResponse>)> {
    let normalized = normalize_symbol(&symbol);
    let client = get_shared_client();

    match client.get_trades(&normalized).await {
        Ok(trades) => Ok(Json(trades)),
        Err(e) => Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("External data unavailable: {}", e),
                code: "EXTERNAL_ERROR".to_string(),
            }),
        )),
    }
}

/// External Candles — cached (30 s TTL) + request coalescing + circuit breaker.
pub async fn get_candles_external(
    Path(symbol): Path<String>,
    Query(query): Query<CandlesQuery>,
) -> Result<(StatusCode, HeaderMap, Json<CandlesResponse>), (StatusCode, Json<ErrorResponse>)> {
    let normalized = normalize_symbol(&symbol);
    let cache = candles_cache();
    let breaker = candles_breaker();
    let client = get_shared_client();

    // Cache key encodes symbol + period + limit + time range for uniqueness.
    let cache_key = format!(
        "{}:{}:{}:{}:{}",
        normalized,
        query.period,
        query.limit,
        query.from.unwrap_or(0),
        query.to.unwrap_or(0),
    );

    // Circuit open → serve stale or 503.
    if !breaker.allow_request() {
        if let Some(stale) = cache.get_stale(&cache_key) {
            let mut headers = HeaderMap::new();
            headers.insert("X-Data-Stale", HeaderValue::from_static("true"));
            return Ok((StatusCode::OK, headers, Json(CandlesResponse {
                symbol: normalized,
                period: query.period,
                candles: (*stale).clone(),
            })));
        }
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "Upstream circuit open and no cached data available".to_string(),
                code: "CIRCUIT_OPEN".to_string(),
            }),
        ));
    }

    let norm_clone = normalized.clone();
    let period_clone = query.period.clone();
    let limit = query.limit;
    let from = query.from;
    let to = query.to;

    let result = cache.get_or_fetch(&cache_key, || async {
        client.get_candles(&norm_clone, &period_clone, limit, from, to).await
    }).await;

    match result {
        Ok(candles) => {
            breaker.record_success();
            Ok((StatusCode::OK, HeaderMap::new(), Json(CandlesResponse {
                symbol: normalized,
                period: query.period,
                candles: (*candles).clone(),
            })))
        }
        Err(e) => {
            breaker.record_failure();
            if let Some(stale) = cache.get_stale(&cache_key) {
                let mut headers = HeaderMap::new();
                headers.insert("X-Data-Stale", HeaderValue::from_static("true"));
                return Ok((StatusCode::OK, headers, Json(CandlesResponse {
                    symbol: normalized,
                    period: query.period,
                    candles: (*stale).clone(),
                })));
            }
            Err((
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: format!("External data unavailable: {}", e),
                    code: "EXTERNAL_ERROR".to_string(),
                }),
            ))
        }
    }
}
