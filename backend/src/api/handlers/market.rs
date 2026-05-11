use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;

use crate::AppState;
// use crate::services::price_feed::MarketInfo;

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

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub code: String,
}

#[derive(Debug, Serialize)]
pub struct Market {
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub last_price: Decimal,
    pub price_change_24h: Decimal,
    pub price_change_percent_24h: Decimal,
    pub high_24h: Decimal,
    pub low_24h: Decimal,
    pub volume_24h: Decimal,
    pub volume_24h_usd: Decimal,
    pub rank: usize,
    #[serde(rename = "type")]
    pub market_type: String,
    pub leverage: i32,
    pub base_maker_fee_rate: Decimal,
    pub base_taker_fee_rate: Decimal,
    pub maintenance_margin_rate: Decimal,
    pub max_position_size_usd: Decimal,
    pub min_order_size_usd: Decimal,
    pub lot_size: Decimal,
    pub tick_size: Decimal,
}

#[derive(Debug, Serialize)]
pub struct MarketsResponse {
    pub markets: Vec<Market>,
    pub total: usize,
}

#[derive(Debug, Deserialize)]
pub struct MarketsQuery {
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OrderbookResponse {
    pub symbol: String,
    pub bids: Vec<[String; 2]>, // [price, amount]
    pub asks: Vec<[String; 2]>,
    pub timestamp: i64,
}

#[derive(Debug, Serialize)]
pub struct Trade {
    pub id: String,
    pub price: String,
    pub amount: String,
    pub side: String,
    pub timestamp: i64,
}

#[derive(Debug, Serialize)]
pub struct TradesResponse {
    pub symbol: String,
    pub trades: Vec<Trade>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TickerResponse {
    pub symbol: String,
    pub last_price: Decimal,
    pub price_change_24h: Decimal,
    pub price_change_percent_24h: Decimal,
    pub high_24h: Decimal,
    pub low_24h: Decimal,
    pub volume_24h: Decimal,
    pub open_interest: Decimal,
    pub funding_rate: Decimal,
    pub next_funding_time: i64,
}

#[derive(Debug, Serialize)]
pub struct PriceResponse {
    pub symbol: String,
    pub mark_price: Decimal,
    pub index_price: Decimal,
    pub last_price: Decimal,
    pub bid_price: Decimal,
    pub ask_price: Decimal,
    pub funding_rate: Decimal,
    pub next_funding_rate: Decimal,
    pub next_funding_time: i64,
    pub updated_at: i64,
}

/// List all available markets (top 50 by volume from OKX)
pub async fn list_markets(
    State(state): State<Arc<AppState>>,
    Query(query): Query<MarketsQuery>,
) -> Result<Json<MarketsResponse>, StatusCode> {
    let limit = query.limit.unwrap_or(200).min(500);

    // Get markets from price feed service
    let market_infos = state.price_feed_service.get_markets().await;
    let prices = state.price_feed_service.get_all_prices().await;

    // Get market configs for real leverage/fee data
    let configs = state.market_config_service.get_all_active().await;
    let config_map: std::collections::HashMap<String, _> = configs
        .into_iter()
        .map(|c| (c.symbol.clone(), c))
        .collect();

    let markets: Vec<Market> = market_infos
        .into_iter()
        .take(limit)
        .map(|info| {
            let price_data = prices.get(&info.symbol);
            let config = config_map.get(&info.symbol);

            // Use last_price from price cache; fallback to mark_price (from external feed)
            let last_price = price_data
                .map(|p| if p.last_price > Decimal::ZERO { p.last_price } else { p.mark_price })
                .unwrap_or(Decimal::ZERO);
            // Use real trade volume from price cache (volume_ccy_24h = sum of amount*price)
            let volume_24h_usd = price_data
                .map(|p| p.volume_ccy_24h)
                .unwrap_or(Decimal::ZERO);

            Market {
                symbol: info.symbol.clone(),
                base_asset: info.base_asset,
                quote_asset: info.quote_asset,
                last_price,
                price_change_24h: price_data.map(|p| p.price_change_24h).unwrap_or(Decimal::ZERO),
                price_change_percent_24h: price_data.map(|p| p.price_change_percent_24h).unwrap_or(Decimal::ZERO),
                high_24h: price_data.map(|p| p.high_24h).unwrap_or(Decimal::ZERO),
                low_24h: price_data.map(|p| p.low_24h).unwrap_or(Decimal::ZERO),
                volume_24h: price_data.map(|p| p.volume_24h).unwrap_or(Decimal::ZERO),
                volume_24h_usd,
                rank: info.rank,
                market_type: "perpetual".to_string(),
                leverage: config.map(|c| c.max_leverage).unwrap_or(50),
                base_maker_fee_rate: config.map(|c| c.base_maker_fee_rate).unwrap_or(Decimal::new(2, 4)),
                base_taker_fee_rate: config.map(|c| c.base_taker_fee_rate).unwrap_or(Decimal::new(5, 4)),
                maintenance_margin_rate: config.map(|c| c.maintenance_margin_rate).unwrap_or(Decimal::new(5, 3)),
                max_position_size_usd: config.map(|c| c.max_position_size_usd).unwrap_or(Decimal::new(10_000_000, 0)),
                min_order_size_usd: config.map(|c| c.min_order_size_usd).unwrap_or(Decimal::new(10, 0)),
                lot_size: config.map(|c| c.lot_size).unwrap_or(Decimal::new(1, 3)),
                tick_size: config.map(|c| c.tick_size).unwrap_or(Decimal::new(1, 1)),
            }
        })
        .collect();

    let total = markets.len();

    Ok(Json(MarketsResponse { markets, total }))
}

/// Get orderbook for a symbol
pub async fn get_orderbook(
    State(state): State<Arc<AppState>>,
    Path(symbol): Path<String>,
) -> Result<Json<OrderbookResponse>, (StatusCode, Json<ErrorResponse>)> {
    // Normalize symbol format (supports BTC-USD, BTCUSDT, etc.)
    let normalized_symbol = normalize_symbol(&symbol);
    
    // Validate market using dynamic symbol list
    if !state.market_config_service.is_visible(&normalized_symbol).await {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Unknown trading pair: {}", normalized_symbol),
                code: "INVALID_MARKET".to_string(),
            }),
        ));
    }

    // Try to get orderbook from Redis cache first
    if let Some(orderbook_cache) = state.cache.orderbook_opt() {
        let cached = orderbook_cache.get_orderbook(&normalized_symbol, Some(20)).await;
        if !cached.bids.is_empty() || !cached.asks.is_empty() {
            // Convert PriceLevel to [String; 2] format
            let bids: Vec<[String; 2]> = cached.bids
                .iter()
                .map(|level| [level.price.to_string(), level.amount.to_string()])
                .collect();
            let asks: Vec<[String; 2]> = cached.asks
                .iter()
                .map(|level| [level.price.to_string(), level.amount.to_string()])
                .collect();

            return Ok(Json(OrderbookResponse {
                symbol: cached.symbol,
                bids,
                asks,
                timestamp: cached.timestamp,
            }));
        }
    }

    // Fallback to matching engine if Redis cache is empty
    match state.matching_engine.get_orderbook(&normalized_symbol, 20) {
        Ok(snapshot) => Ok(Json(OrderbookResponse {
            symbol: snapshot.symbol,
            bids: snapshot.bids,
            asks: snapshot.asks,
            timestamp: snapshot.timestamp,
        })),
        Err(_) => Ok(Json(OrderbookResponse {
            symbol: normalized_symbol,
            bids: vec![],
            asks: vec![],
            timestamp: chrono::Utc::now().timestamp_millis(),
        })),
    }
}

/// Get recent trades for a symbol
pub async fn get_trades(
    State(state): State<Arc<AppState>>,
    Path(symbol): Path<String>,
) -> Result<Json<TradesResponse>, (StatusCode, Json<ErrorResponse>)> {
    // Normalize symbol format (supports BTC-USD, BTCUSDT, etc.)
    let normalized_symbol = normalize_symbol(&symbol);
    
    // Validate market using dynamic symbol list
    if !state.market_config_service.is_visible(&normalized_symbol).await {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Unknown trading pair: {}", normalized_symbol),
                code: "INVALID_MARKET".to_string(),
            }),
        ));
    }

    // Get trades from database
    let rows: Vec<(String, Decimal, Decimal, String, i64)> = sqlx::query_as(
        r#"
        SELECT id::text, price, amount, side::text,
               EXTRACT(EPOCH FROM created_at)::bigint * 1000 as timestamp
        FROM trades
        WHERE symbol = $1
        ORDER BY created_at DESC
        LIMIT 50
        "#
    )
    .bind(&normalized_symbol)
    .fetch_all(&state.db.pool)
    .await
    .unwrap_or_default();

    let trades: Vec<Trade> = rows
        .into_iter()
        .map(|(id, price, amount, side, timestamp)| Trade {
            id,
            price: price.to_string(),
            amount: amount.to_string(),
            side,
            timestamp,
        })
        .collect();

    Ok(Json(TradesResponse { symbol: normalized_symbol, trades }))
}

/// Get ticker for a symbol
pub async fn get_ticker(
    State(state): State<Arc<AppState>>,
    Path(symbol): Path<String>,
) -> Result<Json<TickerResponse>, (StatusCode, Json<ErrorResponse>)> {
    // Normalize symbol format (supports BTC-USD, BTCUSDT, etc.)
    let normalized_symbol = normalize_symbol(&symbol);
    
    // Validate market using dynamic symbol list
    if !state.market_config_service.is_visible(&normalized_symbol).await {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Unknown trading pair: {}", normalized_symbol),
                code: "INVALID_MARKET".to_string(),
            }),
        ));
    }

    // Get real-time price data from price feed service.
    // OI + next_funding_time come from funding_rate_service (price_feed.next_funding_time
    // stays 0 because the Binance external feed is disabled — see price_feed/mod.rs header).
    if let Some(price_data) = state.price_feed_service.get_price_data(&normalized_symbol).await {
        let (open_interest, next_funding_time) = ticker_oi_and_next_funding(&state, &normalized_symbol).await;
        return Ok(Json(TickerResponse {
            symbol: normalized_symbol.clone(),
            last_price: price_data.last_price,
            price_change_24h: price_data.price_change_24h,
            price_change_percent_24h: price_data.price_change_percent_24h,
            high_24h: price_data.high_24h,
            low_24h: price_data.low_24h,
            volume_24h: price_data.volume_ccy_24h,
            open_interest,
            funding_rate: price_data.funding_rate,
            next_funding_time,
        }));
    }

    // Fallback to database if price feed not available
    // Query virtual_trades table for internal trades  
    let last_price_result: Option<(sqlx::types::BigDecimal,)> = sqlx::query_as(
        "SELECT price FROM virtual_trades WHERE symbol = $1 ORDER BY timestamp DESC LIMIT 1"
    )
    .bind(&normalized_symbol)
    .fetch_optional(&state.db.pool)
    .await
    .ok()
    .flatten();

    let last_price = last_price_result.map(|(price,)| {
        Decimal::from_str(&price.to_string()).unwrap_or(Decimal::ZERO)
    });

    // Get 24h stats from virtual_trades
    let stats_result: Option<(Option<sqlx::types::BigDecimal>, Option<sqlx::types::BigDecimal>, sqlx::types::BigDecimal)> = sqlx::query_as(
        r#"
        SELECT
            MAX(price) as high,
            MIN(price) as low,
            COALESCE(SUM(amount * price), 0) as volume
        FROM virtual_trades
        WHERE symbol = $1
        AND timestamp > (extract(epoch from NOW()) * 1000 - 86400000)::bigint
        "#
    )
    .bind(&normalized_symbol)
    .fetch_optional(&state.db.pool)
    .await
    .ok()
    .flatten();

    let (high_24h, low_24h, volume_24h) = if let Some((high_opt, low_opt, vol)) = stats_result {
        let high = high_opt.map(|v| Decimal::from_str(&v.to_string()).unwrap_or(Decimal::ZERO)).unwrap_or(Decimal::ZERO);
        let low = low_opt.map(|v| Decimal::from_str(&v.to_string()).unwrap_or(Decimal::ZERO)).unwrap_or(Decimal::ZERO);
        let volume = Decimal::from_str(&vol.to_string()).unwrap_or(Decimal::ZERO);
        (high, low, volume)
    } else {
        (Decimal::ZERO, Decimal::ZERO, Decimal::ZERO)
    };

    let (open_interest, next_funding_time) = ticker_oi_and_next_funding(&state, &normalized_symbol).await;

    Ok(Json(TickerResponse {
        symbol: normalized_symbol,
        last_price: last_price.unwrap_or(Decimal::ZERO),
        price_change_24h: Decimal::ZERO,
        price_change_percent_24h: Decimal::ZERO,
        high_24h,
        low_24h,
        volume_24h,
        open_interest,
        funding_rate: Decimal::new(1, 4), // 0.01%
        next_funding_time,
    }))
}

/// Pull total open interest and next-funding timestamp (unix seconds) for a
/// symbol from `funding_rate_service`. Falls back to (0, 0) if the cache has
/// no entry yet — both values are display-only on the FE so a transient zero
/// is preferable to bubbling an error.
async fn ticker_oi_and_next_funding(state: &Arc<AppState>, symbol: &str) -> (Decimal, i64) {
    let info = state.funding_rate_service.get_funding_rate(symbol).await;
    match info {
        Some(f) => (
            f.long_open_interest + f.short_open_interest,
            f.next_funding_time.timestamp(),
        ),
        None => (Decimal::ZERO, 0),
    }
}

/// Get real-time price data for a symbol (from OKX)
pub async fn get_price(
    State(state): State<Arc<AppState>>,
    Path(symbol): Path<String>,
) -> Result<Json<PriceResponse>, (StatusCode, Json<ErrorResponse>)> {
    // Normalize symbol format (supports BTC-USD, BTCUSDT, etc.)
    let normalized_symbol = normalize_symbol(&symbol);
    
    // Validate market using dynamic symbol list
    if !state.market_config_service.is_visible(&normalized_symbol).await {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Unknown trading pair: {}", normalized_symbol),
                code: "INVALID_MARKET".to_string(),
            }),
        ));
    }

    // Get price data from price feed service
    match state.price_feed_service.get_price_data(&normalized_symbol).await {
        Some(data) => Ok(Json(PriceResponse {
            symbol: normalized_symbol,
            mark_price: data.mark_price,
            index_price: data.index_price,
            last_price: data.last_price,
            bid_price: data.bid_price,
            ask_price: data.ask_price,
            funding_rate: data.funding_rate,
            next_funding_rate: data.next_funding_rate,
            next_funding_time: data.next_funding_time,
            updated_at: data.updated_at,
        })),
        None => Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "Price data temporarily unavailable".to_string(),
                code: "PRICE_DATA_UNAVAILABLE".to_string(),
            }),
        )),
    }
}

/// Response for risk parameters
#[derive(Debug, Serialize)]
pub struct MarketRiskParams {
    pub symbol: String,
    pub maintenance_margin_rate: Decimal,
    pub liquidation_fee_rate: Decimal,
    pub max_leverage: i32,
    pub max_position_size_usd: Decimal,
}

#[derive(Debug, Serialize)]
pub struct MarketRiskParamsResponse {
    pub markets: Vec<MarketRiskParams>,
}

/// Get risk parameters for all markets
/// GET /markets/risk-params
pub async fn get_markets_risk_params(
    State(state): State<Arc<AppState>>,
) -> Result<Json<MarketRiskParamsResponse>, StatusCode> {
    let configs = state.market_config_service.get_all_active().await;

    let markets: Vec<MarketRiskParams> = configs
        .into_iter()
        .map(|c| MarketRiskParams {
            symbol: c.symbol,
            maintenance_margin_rate: c.maintenance_margin_rate,
            liquidation_fee_rate: Decimal::new(5, 3), // Default 0.5%, from liquidation config
            max_leverage: c.max_leverage,
            max_position_size_usd: c.max_position_size_usd,
        })
        .collect();

    Ok(Json(MarketRiskParamsResponse { markets }))
}

// =============================================================================
// Lighter-style per-market details
// =============================================================================

/// Per-market detail view modelled on Lighter's market-details card.
///
/// Rendering map (Lighter label → response field):
///   "Market Name"                  → market_name
///   "<Description>"                → description
///   "Min <BASE> Amount"            → min_base_amount
///   "Min USD Amount"               → min_usd_amount
///   "Price Steps"                  → price_step
///   "Max Leverage"                 → max_leverage
///   "Initial Margin Fraction"      → initial_margin_fraction
///   "Maintenance Margin Fraction"  → maintenance_margin_fraction
///   "Close Out Margin Fraction"    → close_out_margin_fraction
///   "Market Cap"                   → market_cap
///   "FDV"                          → fully_diluted_valuation
#[derive(Debug, Serialize)]
pub struct MarketDetailsResponse {
    pub symbol: String,
    pub market_name: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub description: Option<String>,

    // Order-entry constraints
    pub min_base_amount: Decimal,
    pub min_usd_amount: Decimal,
    pub price_step: Decimal,
    pub lot_size: Decimal,

    // Leverage / margin structure (as fractions, 0..1)
    pub max_leverage: i32,
    pub initial_margin_fraction: Decimal,
    pub maintenance_margin_fraction: Decimal,
    pub close_out_margin_fraction: Option<Decimal>,

    // Valuation (nullable while the CoinGecko cache is empty)
    pub market_cap: Option<Decimal>,
    pub fully_diluted_valuation: Option<Decimal>,
    pub market_cap_updated_at: Option<i64>,

    // Live market state (included so a single request fully paints the
    // card without a second round-trip to /price or /ticker)
    pub mark_price: Decimal,
    pub last_price: Decimal,
    pub funding_rate: Decimal,
    pub next_funding_time: i64,

    // Listing lifecycle
    pub listing_phase: String,
    pub status: String,
}

pub async fn get_market_details(
    State(state): State<Arc<AppState>>,
    Path(symbol): Path<String>,
) -> Result<Json<MarketDetailsResponse>, (StatusCode, Json<ErrorResponse>)> {
    let sym = normalize_symbol(&symbol);
    let config = state
        .market_config_service
        .get_config(&sym)
        .await
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: format!("market {} not found", sym),
                    code: "MARKET_NOT_FOUND".to_string(),
                }),
            )
        })?;

    let price_data = state.price_feed_service.get_price_data(&sym).await;
    let mark_price = price_data.as_ref().map(|p| p.mark_price).unwrap_or(Decimal::ZERO);
    let last_price = price_data
        .as_ref()
        .map(|p| if p.last_price > Decimal::ZERO { p.last_price } else { p.mark_price })
        .unwrap_or(Decimal::ZERO);

    let funding = state.funding_rate_service.get_funding_rate(&sym).await;
    let funding_rate = funding.as_ref().map(|f| f.funding_rate).unwrap_or(Decimal::ZERO);
    let next_funding_time = funding
        .as_ref()
        .map(|f| f.next_funding_time.timestamp_millis())
        .unwrap_or(0);

    // Lighter: min_base_amount = min_usd / current price (floor to 2 dp for UI).
    let min_base_amount = if last_price > Decimal::ZERO {
        config.min_order_size_usd / last_price
    } else {
        config.lot_size
    };

    // Derived risk fractions (0..1) that the UI displays as percentages.
    let initial_margin_fraction = if config.max_leverage > 0 {
        Decimal::ONE / Decimal::from(config.max_leverage)
    } else {
        Decimal::ZERO
    };

    // Human display name: "AXBlade BTC" / "Bitcoin" etc. Fallback to
    // `{BASE} ({SYMBOL})` if the config doesn't provide one.
    let market_name = config
        .display_name
        .clone()
        .unwrap_or_else(|| format!("{} ({})", config.base_asset, config.symbol));

    Ok(Json(MarketDetailsResponse {
        symbol: config.symbol.clone(),
        market_name,
        base_asset: config.base_asset.clone(),
        quote_asset: config.quote_asset.clone(),
        description: config.description.clone(),

        min_base_amount,
        min_usd_amount: config.min_order_size_usd,
        price_step: config.tick_size,
        lot_size: config.lot_size,

        max_leverage: config.max_leverage,
        initial_margin_fraction,
        maintenance_margin_fraction: config.maintenance_margin_rate,
        close_out_margin_fraction: config.close_out_margin_rate,

        market_cap: config.market_cap,
        fully_diluted_valuation: config.fully_diluted_valuation,
        market_cap_updated_at: config
            .market_cap_updated_at
            .map(|t| t.timestamp_millis()),

        mark_price,
        last_price,
        funding_rate,
        next_funding_time,

        listing_phase: config.listing_phase.clone(),
        status: config.status.clone(),
    }))
}
