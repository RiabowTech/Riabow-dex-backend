//! Developer API — Market Data (Binance-compatible)
//!
//! Public endpoints under `/fapi/v1/` and `/futures/data/`.
//! These mirror the Binance Futures USDT-M API response format
//! so professional traders can integrate using familiar SDKs.

use axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use crate::AppState;

// ─── Helpers ──────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct BinanceError {
    code: i32,
    msg: String,
}

type ApiResult<T> = Result<Json<T>, (StatusCode, Json<BinanceError>)>;

fn bad_request(code: i32, msg: &str) -> (StatusCode, Json<BinanceError>) {
    (
        StatusCode::BAD_REQUEST,
        Json(BinanceError {
            code,
            msg: msg.to_string(),
        }),
    )
}

fn internal_error(msg: &str) -> (StatusCode, Json<BinanceError>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(BinanceError {
            code: -1001,
            msg: msg.to_string(),
        }),
    )
}

fn normalize_symbol(s: &str) -> String {
    let upper = s.to_uppercase();
    if !upper.contains('-') && !upper.contains('/') && !upper.contains('_') {
        return upper;
    }
    if upper.ends_with("-USD") {
        return format!("{}USDT", upper.strip_suffix("-USD").unwrap());
    }
    if upper.contains("-USDT") {
        return upper.replace("-", "");
    }
    upper.replace("/", "").replace("_", "")
}

/// Derive precision (number of decimal places) from a tick/lot size.
/// e.g. 0.01 → 2, 0.001 → 3, 1 → 0, 0.00001 → 5
fn decimal_places(d: &Decimal) -> i32 {
    if d.is_zero() {
        return 8;
    }
    let s = d.normalize().to_string();
    match s.find('.') {
        Some(pos) => (s.len() - pos - 1) as i32,
        None => 0,
    }
}

// ─── 1. Ping ──────────────────────────────────────────────────────
// GET /fapi/v1/ping

pub async fn ping() -> Json<serde_json::Value> {
    Json(serde_json::json!({}))
}

// ─── 2. Server Time ───────────────────────────────────────────────
// GET /fapi/v1/time

#[derive(Serialize)]
pub struct ServerTimeResponse {
    #[serde(rename = "serverTime")]
    pub server_time: i64,
}

pub async fn server_time() -> Json<ServerTimeResponse> {
    Json(ServerTimeResponse {
        server_time: Utc::now().timestamp_millis(),
    })
}

// ─── 3. Exchange Information ──────────────────────────────────────
// GET /fapi/v1/exchangeInfo

#[derive(Serialize)]
pub struct ExchangeInfoResponse {
    pub timezone: String,
    #[serde(rename = "serverTime")]
    pub server_time: i64,
    #[serde(rename = "rateLimits")]
    pub rate_limits: Vec<RateLimit>,
    pub assets: Vec<AssetInfo>,
    pub symbols: Vec<SymbolInfo>,
}

#[derive(Serialize)]
pub struct RateLimit {
    #[serde(rename = "rateLimitType")]
    pub rate_limit_type: String,
    pub interval: String,
    #[serde(rename = "intervalNum")]
    pub interval_num: i32,
    pub limit: i32,
}

#[derive(Serialize)]
pub struct AssetInfo {
    pub asset: String,
    #[serde(rename = "marginAvailable")]
    pub margin_available: bool,
    #[serde(rename = "autoAssetExchange")]
    pub auto_asset_exchange: String,
}

#[derive(Serialize)]
pub struct SymbolInfo {
    pub symbol: String,
    pub pair: String,
    #[serde(rename = "contractType")]
    pub contract_type: String,
    #[serde(rename = "deliveryDate")]
    pub delivery_date: i64,
    #[serde(rename = "onboardDate")]
    pub onboard_date: i64,
    pub status: String,
    #[serde(rename = "baseAsset")]
    pub base_asset: String,
    #[serde(rename = "quoteAsset")]
    pub quote_asset: String,
    #[serde(rename = "marginAsset")]
    pub margin_asset: String,
    #[serde(rename = "pricePrecision")]
    pub price_precision: i32,
    #[serde(rename = "quantityPrecision")]
    pub quantity_precision: i32,
    #[serde(rename = "baseAssetPrecision")]
    pub base_asset_precision: i32,
    #[serde(rename = "quotePrecision")]
    pub quote_precision: i32,
    #[serde(rename = "underlyingType")]
    pub underlying_type: String,
    #[serde(rename = "settlePlan")]
    pub settle_plan: i64,
    #[serde(rename = "triggerProtect")]
    pub trigger_protect: String,
    /// Binance-shape filter array. Bots feeding into Binance SDKs read
    /// LOT_SIZE.minQty/stepSize, PRICE_FILTER.tickSize, and MIN_NOTIONAL.notional
    /// to size orders client-side. Until 2026-05 we omitted this entirely,
    /// so SDKs defaulted to no-rounding and clients would hit -1013 at
    /// placement (e.g. 0.0001 SEI dust orders).
    pub filters: Vec<serde_json::Value>,
    #[serde(rename = "orderTypes")]
    pub order_types: Vec<String>,
    #[serde(rename = "timeInForce")]
    pub time_in_force: Vec<String>,
}

pub async fn exchange_info(
    State(state): State<Arc<AppState>>,
) -> Json<ExchangeInfoResponse> {
    let configs = state.market_config_service.get_all_active().await;

    let collateral = state.config.collateral_symbol().to_string();

    let symbols: Vec<SymbolInfo> = configs
        .iter()
        .map(|c| {
            // Derive precision from tick_size and lot_size
            // e.g. tick_size=0.01 → price_precision=2, lot_size=0.001 → quantity_precision=3
            let price_precision = decimal_places(&c.tick_size);
            let quantity_precision = decimal_places(&c.lot_size);

            // Map MarketConfig.status to Binance status
            let status = match c.status.as_str() {
                "active" => "TRADING",
                "suspended" => "HALT",
                "delisted" | "close_only" => "CLOSE",
                _ => "TRADING",
            };

            // Derive underlying type from category
            let underlying_type = match c.category.as_str() {
                "rwa" | "stock" | "equity" => "INDEX",
                "defi" => "DEFI",
                _ => "COIN",
            };

            // Use actual timestamps from config
            let onboard_date = c.created_at.timestamp_millis();
            let delivery_date = c.scheduled_delist_at
                .map(|d| d.timestamp_millis())
                .unwrap_or(4133404800000); // perpetual = far future

            // trigger_protect derived from maintenance_margin_rate
            let trigger_protect = c.maintenance_margin_rate.to_string();

            // Binance-shape filters. We populate the three filter types that
            // bots actually read (LOT_SIZE, PRICE_FILTER, MIN_NOTIONAL) plus
            // MARKET_LOT_SIZE which Binance always echoes equal to LOT_SIZE
            // for futures. max{Qty,Price} are soft caps — clients use them
            // for sanity bounds, not enforcement (the real cap is
            // max_order_size_usd, checked server-side at placement). We
            // expose a large finite number rather than 0, since some SDKs
            // treat 0 as "no orders allowed".
            let filters = vec![
                serde_json::json!({
                    "filterType": "PRICE_FILTER",
                    "minPrice": c.tick_size.to_string(),
                    "maxPrice": "1000000000",
                    "tickSize": c.tick_size.to_string(),
                }),
                serde_json::json!({
                    "filterType": "LOT_SIZE",
                    "minQty": c.lot_size.to_string(),
                    "maxQty": "1000000000",
                    "stepSize": c.lot_size.to_string(),
                }),
                serde_json::json!({
                    "filterType": "MARKET_LOT_SIZE",
                    "minQty": c.lot_size.to_string(),
                    "maxQty": "1000000000",
                    "stepSize": c.lot_size.to_string(),
                }),
                serde_json::json!({
                    "filterType": "MIN_NOTIONAL",
                    "notional": c.min_order_size_usd.to_string(),
                }),
            ];

            SymbolInfo {
                symbol: c.symbol.clone(),
                pair: c.symbol.clone(),
                contract_type: "PERPETUAL".to_string(),
                delivery_date,
                onboard_date,
                status: status.to_string(),
                base_asset: c.base_asset.clone(),
                quote_asset: c.quote_asset.clone(),
                margin_asset: collateral.clone(),
                price_precision,
                quantity_precision,
                base_asset_precision: price_precision.max(8),
                quote_precision: 8,
                underlying_type: underlying_type.to_string(),
                settle_plan: 0,
                trigger_protect,
                filters,
                order_types: vec![
                    "LIMIT".into(),
                    "MARKET".into(),
                    "STOP".into(),
                    "STOP_MARKET".into(),
                    "TAKE_PROFIT".into(),
                    "TAKE_PROFIT_MARKET".into(),
                ],
                time_in_force: vec!["GTC".into(), "IOC".into(), "FOK".into(), "GTX".into()],
            }
        })
        .collect();

    let assets = vec![AssetInfo {
        asset: collateral.clone(),
        margin_available: true,
        auto_asset_exchange: "0".to_string(),
    }];

    let rate_limits = vec![
        RateLimit {
            rate_limit_type: "REQUEST_WEIGHT".to_string(),
            interval: "MINUTE".to_string(),
            interval_num: 1,
            limit: 2400,
        },
        RateLimit {
            rate_limit_type: "ORDERS".to_string(),
            interval: "MINUTE".to_string(),
            interval_num: 1,
            limit: 1200,
        },
    ];

    Json(ExchangeInfoResponse {
        timezone: "UTC".to_string(),
        server_time: Utc::now().timestamp_millis(),
        rate_limits,
        assets,
        symbols,
    })
}

// ─── 4. Klines ────────────────────────────────────────────────────
// GET /fapi/v1/klines

#[derive(Debug, Deserialize)]
pub struct KlinesQuery {
    pub symbol: String,
    pub interval: String,
    #[serde(rename = "startTime")]
    pub start_time: Option<i64>,
    #[serde(rename = "endTime")]
    pub end_time: Option<i64>,
    pub limit: Option<usize>,
}

pub async fn klines(
    State(state): State<Arc<AppState>>,
    Query(q): Query<KlinesQuery>,
) -> ApiResult<Vec<serde_json::Value>> {
    let symbol = normalize_symbol(&q.symbol);
    let limit = q.limit.unwrap_or(500).min(1500);

    // Map Binance intervals to supported internal periods
    // Supported: 1m, 5m, 15m, 1h, 4h, 1d, 1w, 1M
    // Unsupported Binance intervals are mapped to the closest supported one
    let internal_interval = match q.interval.as_str() {
        "1m" => "1m",
        "3m" | "5m" => "5m",
        "15m" | "30m" => "15m",
        "1h" | "2h" => "1h",
        "4h" | "6h" | "8h" | "12h" => "4h",
        "1d" | "3d" => "1d",
        "1w" => "1w",
        "1M" => "1M",
        _ => return Err(bad_request(-1120, "Invalid interval.")),
    };

    let kline_period = crate::services::kline::KlinePeriod::from_str(internal_interval)
        .ok_or_else(|| bad_request(-1120, "Invalid interval."))?;

    // Convert ms to seconds for internal service
    let from = q.start_time.map(|t| if t > 10_000_000_000 { t / 1000 } else { t });
    let to = q.end_time.map(|t| if t > 10_000_000_000 { t / 1000 } else { t });

    let candles = state
        .kline_service
        .get_candles(&symbol, kline_period, limit, from, to)
        .await;

    // Return Binance format: array of arrays
    let result: Vec<serde_json::Value> = candles
        .into_iter()
        .map(|c| {
            let open_time_ms = c.time * 1000;
            let close_time_ms = open_time_ms + kline_period_ms(&q.interval) - 1;
            serde_json::json!([
                open_time_ms,
                c.open.to_string(),
                c.high.to_string(),
                c.low.to_string(),
                c.close.to_string(),
                c.volume.to_string(),
                close_time_ms,
                c.quote_volume.unwrap_or(Decimal::ZERO).to_string(),
                c.trade_count.unwrap_or(0),
                "0",  // taker buy base volume
                "0",  // taker buy quote volume
                "0"   // ignore
            ])
        })
        .collect();

    Ok(Json(result))
}

fn kline_period_ms(interval: &str) -> i64 {
    match interval {
        "1m" => 60_000,
        "3m" => 180_000,
        "5m" => 300_000,
        "15m" => 900_000,
        "30m" => 1_800_000,
        "1h" => 3_600_000,
        "2h" => 7_200_000,
        "4h" => 14_400_000,
        "6h" => 21_600_000,
        "8h" => 28_800_000,
        "12h" => 43_200_000,
        "1d" => 86_400_000,
        "3d" => 259_200_000,
        "1w" => 604_800_000,
        "1M" => 2_592_000_000, // ~30 days
        _ => 60_000,
    }
}

// ─── 5. Symbol Price Ticker ───────────────────────────────────────
// GET /fapi/v1/ticker/price

#[derive(Debug, Deserialize)]
pub struct SymbolQuery {
    pub symbol: Option<String>,
}

pub async fn ticker_price(
    State(state): State<Arc<AppState>>,
    Query(q): Query<SymbolQuery>,
) -> Json<serde_json::Value> {
    let prices = state.price_feed_service.get_all_prices().await;

    if let Some(ref sym) = q.symbol {
        let symbol = normalize_symbol(sym);
        let price = prices
            .get(&symbol)
            .map(|p| {
                if p.last_price > Decimal::ZERO {
                    p.last_price
                } else {
                    p.mark_price
                }
            })
            .unwrap_or(Decimal::ZERO);
        return Json(serde_json::json!({
            "symbol": symbol,
            "price": price.to_string(),
            "time": Utc::now().timestamp_millis()
        }));
    }

    // Return all symbols
    let configs = state.market_config_service.get_all_active().await;
    let items: Vec<serde_json::Value> = configs
        .iter()
        .map(|c| {
            let price = prices
                .get(&c.symbol)
                .map(|p| {
                    if p.last_price > Decimal::ZERO {
                        p.last_price
                    } else {
                        p.mark_price
                    }
                })
                .unwrap_or(Decimal::ZERO);
            serde_json::json!({
                "symbol": c.symbol,
                "price": price.to_string(),
                "time": Utc::now().timestamp_millis()
            })
        })
        .collect();
    Json(serde_json::Value::Array(items))
}

// ─── 6. 24hr Ticker ──────────────────────────────────────────────
// GET /fapi/v1/ticker/24hr

#[derive(Serialize)]
pub struct Ticker24hrItem {
    pub symbol: String,
    #[serde(rename = "priceChange")]
    pub price_change: String,
    #[serde(rename = "priceChangePercent")]
    pub price_change_percent: String,
    #[serde(rename = "weightedAvgPrice")]
    pub weighted_avg_price: String,
    #[serde(rename = "lastPrice")]
    pub last_price: String,
    #[serde(rename = "lastQty")]
    pub last_qty: String,
    #[serde(rename = "openPrice")]
    pub open_price: String,
    #[serde(rename = "highPrice")]
    pub high_price: String,
    #[serde(rename = "lowPrice")]
    pub low_price: String,
    pub volume: String,
    #[serde(rename = "quoteVolume")]
    pub quote_volume: String,
    #[serde(rename = "openTime")]
    pub open_time: i64,
    #[serde(rename = "closeTime")]
    pub close_time: i64,
    #[serde(rename = "firstId")]
    pub first_id: i64,
    #[serde(rename = "lastId")]
    pub last_id: i64,
    pub count: i64,
}

pub async fn ticker_24hr(
    State(state): State<Arc<AppState>>,
    Query(q): Query<SymbolQuery>,
) -> Json<serde_json::Value> {
    let prices = state.price_feed_service.get_all_prices().await;
    let now = Utc::now().timestamp_millis();

    // Determine which symbols to query
    let symbols: Vec<String> = if let Some(ref sym) = q.symbol {
        vec![normalize_symbol(sym)]
    } else {
        state.market_config_service.get_all_active().await.iter().map(|c| c.symbol.clone()).collect()
    };

    // Batch query 24h trade stats from DB for the requested symbols.
    //
    // The previous query had two compounding perf bugs that pushed
    // single-symbol calls past 8s on prod:
    //   1. A correlated subquery `(SELECT amount FROM trades t2
    //       WHERE t2.symbol = t.symbol ORDER BY created_at DESC LIMIT 1)`
    //      ran once per group, doing a fresh index seek for every symbol.
    //   2. Even when the caller supplied `?symbol=BTCUSDT` the SQL had no
    //      `symbol = $1` predicate — every call scanned all 24h of trades
    //      across every symbol on the hypertable.
    //
    // Rewritten as two queries that can each ride the
    // `(symbol, created_at)` index:
    //   Q1: aggregates (count, first/last time) grouped by symbol.
    //   Q2: last trade qty per symbol via `DISTINCT ON (symbol)`.
    // Both queries take the symbol filter when one was supplied.
    let trade_stats: HashMap<String, (i64, Decimal, i64, i64)> = {
        let symbol_filter: Option<String> = q.symbol.as_ref().map(|s| normalize_symbol(s));

        let agg_rows: Vec<(String, i64, i64, i64)> = if let Some(ref sym) = symbol_filter {
            sqlx::query_as(
                r#"
                SELECT
                    symbol,
                    COUNT(*)::bigint,
                    COALESCE(MIN(EXTRACT(EPOCH FROM created_at)::bigint * 1000), 0),
                    COALESCE(MAX(EXTRACT(EPOCH FROM created_at)::bigint * 1000), 0)
                FROM trades
                WHERE symbol = $1
                  AND created_at > NOW() - INTERVAL '24 hours'
                GROUP BY symbol
                "#,
            )
            .bind(sym)
            .fetch_all(&state.db.pool)
            .await
            .unwrap_or_default()
        } else {
            sqlx::query_as(
                r#"
                SELECT
                    symbol,
                    COUNT(*)::bigint,
                    COALESCE(MIN(EXTRACT(EPOCH FROM created_at)::bigint * 1000), 0),
                    COALESCE(MAX(EXTRACT(EPOCH FROM created_at)::bigint * 1000), 0)
                FROM trades
                WHERE created_at > NOW() - INTERVAL '24 hours'
                GROUP BY symbol
                "#,
            )
            .fetch_all(&state.db.pool)
            .await
            .unwrap_or_default()
        };

        let last_qty_rows: Vec<(String, Decimal)> = if let Some(ref sym) = symbol_filter {
            sqlx::query_as(
                r#"
                SELECT DISTINCT ON (symbol) symbol, amount
                FROM trades
                WHERE symbol = $1
                  AND created_at > NOW() - INTERVAL '24 hours'
                ORDER BY symbol, created_at DESC
                "#,
            )
            .bind(sym)
            .fetch_all(&state.db.pool)
            .await
            .unwrap_or_default()
        } else {
            sqlx::query_as(
                r#"
                SELECT DISTINCT ON (symbol) symbol, amount
                FROM trades
                WHERE created_at > NOW() - INTERVAL '24 hours'
                ORDER BY symbol, created_at DESC
                "#,
            )
            .fetch_all(&state.db.pool)
            .await
            .unwrap_or_default()
        };

        let last_qty_by_sym: HashMap<String, Decimal> =
            last_qty_rows.into_iter().collect();

        agg_rows
            .into_iter()
            .map(|(sym, count, first_time, last_time)| {
                let last_qty = last_qty_by_sym
                    .get(&sym)
                    .copied()
                    .unwrap_or(Decimal::ZERO);
                (sym, (count, last_qty, first_time, last_time))
            })
            .collect()
    };

    let build_ticker = |sym: &str| -> Ticker24hrItem {
        let pd = prices.get(sym);
        let last = pd
            .map(|p| {
                if p.last_price > Decimal::ZERO {
                    p.last_price
                } else {
                    p.mark_price
                }
            })
            .unwrap_or(Decimal::ZERO);
        let change = pd.map(|p| p.price_change_24h).unwrap_or(Decimal::ZERO);
        let pct = pd
            .map(|p| p.price_change_percent_24h)
            .unwrap_or(Decimal::ZERO);
        let high = pd.map(|p| p.high_24h).unwrap_or(Decimal::ZERO);
        let low = pd.map(|p| p.low_24h).unwrap_or(Decimal::ZERO);
        let vol = pd.map(|p| p.volume_24h).unwrap_or(Decimal::ZERO);
        let qvol = pd.map(|p| p.volume_ccy_24h).unwrap_or(Decimal::ZERO);
        let open = last - change;

        let (count, last_qty, first_id, last_id) = trade_stats
            .get(sym)
            .copied()
            .unwrap_or((0, Decimal::ZERO, 0, 0));

        Ticker24hrItem {
            symbol: sym.to_string(),
            price_change: change.to_string(),
            price_change_percent: pct.to_string(),
            weighted_avg_price: if vol > Decimal::ZERO {
                (qvol / vol).round_dp(8).to_string()
            } else {
                "0".to_string()
            },
            last_price: last.to_string(),
            last_qty: last_qty.to_string(),
            open_price: open.to_string(),
            high_price: high.to_string(),
            low_price: low.to_string(),
            volume: vol.to_string(),
            quote_volume: qvol.to_string(),
            open_time: now - 86_400_000,
            close_time: now,
            first_id,
            last_id,
            count,
        }
    };

    if q.symbol.is_some() {
        let ticker = build_ticker(&symbols[0]);
        return Json(serde_json::to_value(ticker).unwrap_or_default());
    }

    let items: Vec<Ticker24hrItem> = symbols.iter().map(|s| build_ticker(s)).collect();
    Json(serde_json::to_value(items).unwrap_or_default())
}

// ─── 7. Best Book Ticker ─────────────────────────────────────────
// GET /fapi/v1/ticker/bookTicker

#[derive(Serialize)]
pub struct BookTickerItem {
    pub symbol: String,
    #[serde(rename = "bidPrice")]
    pub bid_price: String,
    #[serde(rename = "bidQty")]
    pub bid_qty: String,
    #[serde(rename = "askPrice")]
    pub ask_price: String,
    #[serde(rename = "askQty")]
    pub ask_qty: String,
    pub time: i64,
}

pub async fn ticker_book(
    State(state): State<Arc<AppState>>,
    Query(q): Query<SymbolQuery>,
) -> Json<serde_json::Value> {
    let now = Utc::now().timestamp_millis();

    // Collect best bid/ask for requested symbols from orderbook cache
    let build_book = async |sym: &str| -> BookTickerItem {
        // Try orderbook cache for real bid/ask quantities
        if let Some(ob_cache) = state.cache.orderbook_opt() {
            let cached = ob_cache.get_orderbook(sym, Some(1)).await;
            if !cached.bids.is_empty() || !cached.asks.is_empty() {
                let (bp, bq) = cached.bids.first()
                    .map(|l| (l.price.to_string(), l.amount.to_string()))
                    .unwrap_or_else(|| ("0".into(), "0".into()));
                let (ap, aq) = cached.asks.first()
                    .map(|l| (l.price.to_string(), l.amount.to_string()))
                    .unwrap_or_else(|| ("0".into(), "0".into()));
                return BookTickerItem {
                    symbol: sym.to_string(),
                    bid_price: bp, bid_qty: bq,
                    ask_price: ap, ask_qty: aq,
                    time: now,
                };
            }
        }
        // Fallback to price feed (no qty available from price feed)
        let prices = state.price_feed_service.get_all_prices().await;
        let pd = prices.get(sym);
        BookTickerItem {
            symbol: sym.to_string(),
            bid_price: pd.map(|p| p.bid_price.to_string()).unwrap_or_else(|| "0".into()),
            bid_qty: "0".to_string(),
            ask_price: pd.map(|p| p.ask_price.to_string()).unwrap_or_else(|| "0".into()),
            ask_qty: "0".to_string(),
            time: now,
        }
    };

    if let Some(ref sym) = q.symbol {
        let symbol = normalize_symbol(sym);
        let item = build_book(&symbol).await;
        return Json(serde_json::to_value(item).unwrap_or_default());
    }

    let configs = state.market_config_service.get_all_active().await;
    let mut items = Vec::with_capacity(configs.len());
    for c in &configs {
        items.push(build_book(&c.symbol).await);
    }
    Json(serde_json::to_value(items).unwrap_or_default())
}

// ─── 8. Depth (Order Book) ───────────────────────────────────────
// GET /fapi/v1/depth

#[derive(Debug, Deserialize)]
pub struct DepthQuery {
    pub symbol: String,
    pub limit: Option<usize>,
}

#[derive(Serialize)]
pub struct DepthResponse {
    #[serde(rename = "lastUpdateId")]
    pub last_update_id: i64,
    #[serde(rename = "E")]
    pub message_time: i64,
    #[serde(rename = "T")]
    pub transaction_time: i64,
    pub bids: Vec<[String; 2]>,
    pub asks: Vec<[String; 2]>,
}

pub async fn depth(
    State(state): State<Arc<AppState>>,
    Query(q): Query<DepthQuery>,
) -> ApiResult<DepthResponse> {
    let symbol = normalize_symbol(&q.symbol);
    let limit = match q.limit.unwrap_or(500) {
        l if l <= 5 => 5,
        l if l <= 10 => 10,
        l if l <= 20 => 20,
        l if l <= 50 => 50,
        l if l <= 100 => 100,
        l if l <= 500 => 500,
        _ => 1000,
    };

    let now = Utc::now().timestamp_millis();

    // Try Redis orderbook cache first
    if let Some(ob_cache) = state.cache.orderbook_opt() {
        let cached = ob_cache.get_orderbook(&symbol, Some(limit)).await;
        if !cached.bids.is_empty() || !cached.asks.is_empty() {
            let bids: Vec<[String; 2]> = cached
                .bids
                .iter()
                .map(|l| [l.price.to_string(), l.amount.to_string()])
                .collect();
            let asks: Vec<[String; 2]> = cached
                .asks
                .iter()
                .map(|l| [l.price.to_string(), l.amount.to_string()])
                .collect();
            return Ok(Json(DepthResponse {
                last_update_id: now,
                message_time: now,
                transaction_time: now,
                bids,
                asks,
            }));
        }
    }

    // Fallback to matching engine
    match state.matching_engine.get_orderbook(&symbol, limit) {
        Ok(snapshot) => Ok(Json(DepthResponse {
            last_update_id: now,
            message_time: now,
            transaction_time: now,
            bids: snapshot.bids,
            asks: snapshot.asks,
        })),
        Err(_) => Ok(Json(DepthResponse {
            last_update_id: now,
            message_time: now,
            transaction_time: now,
            bids: vec![],
            asks: vec![],
        })),
    }
}

// ─── 9. Premium Index (Mark Price & Funding Rate) ─────────────────
// GET /fapi/v1/premiumIndex

#[derive(Serialize)]
pub struct PremiumIndexItem {
    pub symbol: String,
    #[serde(rename = "markPrice")]
    pub mark_price: String,
    #[serde(rename = "indexPrice")]
    pub index_price: String,
    #[serde(rename = "estimatedSettlePrice")]
    pub estimated_settle_price: String,
    #[serde(rename = "lastFundingRate")]
    pub last_funding_rate: String,
    #[serde(rename = "nextFundingTime")]
    pub next_funding_time: i64,
    #[serde(rename = "interestRate")]
    pub interest_rate: String,
    pub time: i64,
}

pub async fn premium_index(
    State(state): State<Arc<AppState>>,
    Query(q): Query<SymbolQuery>,
) -> Json<serde_json::Value> {
    let prices = state.price_feed_service.get_all_prices().await;
    let configs = state.market_config_service.get_all_active().await;
    let config_map: HashMap<String, _> = configs.iter().map(|c| (c.symbol.clone(), c)).collect();
    let now = Utc::now().timestamp_millis();

    let build_item = |sym: &str| -> PremiumIndexItem {
        let pd = prices.get(sym);
        let cfg = config_map.get(sym);
        // interest_rate from borrowing_fee_rate_per_hour in config
        let interest_rate = cfg
            .map(|c| c.borrowing_fee_rate_per_hour.to_string())
            .unwrap_or_else(|| "0.0001".to_string());
        PremiumIndexItem {
            symbol: sym.to_string(),
            mark_price: pd
                .map(|p| p.mark_price.to_string())
                .unwrap_or_else(|| "0".into()),
            index_price: pd
                .map(|p| p.index_price.to_string())
                .unwrap_or_else(|| "0".into()),
            estimated_settle_price: pd
                .map(|p| p.mark_price.to_string())
                .unwrap_or_else(|| "0".into()),
            last_funding_rate: pd
                .map(|p| p.funding_rate.to_string())
                .unwrap_or_else(|| "0".into()),
            next_funding_time: pd.map(|p| p.next_funding_time).unwrap_or(0),
            interest_rate,
            time: now,
        }
    };

    if let Some(ref sym) = q.symbol {
        let symbol = normalize_symbol(sym);
        let item = build_item(&symbol);
        return Json(serde_json::to_value(item).unwrap_or_default());
    }

    let items: Vec<PremiumIndexItem> = configs.iter().map(|c| build_item(&c.symbol)).collect();
    Json(serde_json::to_value(items).unwrap_or_default())
}

// ─── 10. Funding Info ─────────────────────────────────────────────
// GET /fapi/v1/fundingInfo

#[derive(Serialize)]
pub struct FundingInfoItem {
    pub symbol: String,
    #[serde(rename = "adjustedFundingRateCap")]
    pub adjusted_funding_rate_cap: String,
    #[serde(rename = "adjustedFundingRateFloor")]
    pub adjusted_funding_rate_floor: String,
    #[serde(rename = "fundingIntervalHours")]
    pub funding_interval_hours: i32,
}

pub async fn funding_info(
    State(state): State<Arc<AppState>>,
    Query(q): Query<SymbolQuery>,
) -> Json<Vec<FundingInfoItem>> {
    let configs = state.market_config_service.get_all_active().await;

    let items: Vec<FundingInfoItem> = configs
        .iter()
        .filter(|c| {
            if let Some(ref sym) = q.symbol {
                c.symbol == normalize_symbol(sym)
            } else {
                true
            }
        })
        .map(|c| {
            // fee_ceiling/fee_floor from market config represent the funding rate bounds
            let cap = c.fee_ceiling;
            let floor = c.fee_floor;
            // settlement_cycle is already in hours
            let interval_hours = if c.settlement_cycle > 0 {
                c.settlement_cycle
            } else {
                8
            };
            FundingInfoItem {
                symbol: c.symbol.clone(),
                adjusted_funding_rate_cap: cap.to_string(),
                adjusted_funding_rate_floor: format!("-{}", floor),
                funding_interval_hours: interval_hours.max(1),
            }
        })
        .collect();

    Json(items)
}

// ─── 11. Funding Rate History ─────────────────────────────────────
// GET /fapi/v1/fundingRate

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct FundingRateQuery {
    pub symbol: Option<String>,
    #[serde(rename = "startTime")]
    pub start_time: Option<i64>,
    #[serde(rename = "endTime")]
    pub end_time: Option<i64>,
    pub limit: Option<i64>,
}

#[derive(Serialize)]
pub struct FundingRateItem {
    pub symbol: String,
    #[serde(rename = "fundingRate")]
    pub funding_rate: String,
    #[serde(rename = "fundingTime")]
    pub funding_time: i64,
    #[serde(rename = "markPrice")]
    pub mark_price: String,
}

pub async fn funding_rate(
    State(state): State<Arc<AppState>>,
    Query(q): Query<FundingRateQuery>,
) -> ApiResult<Vec<FundingRateItem>> {
    let limit = q.limit.unwrap_or(100).min(1000);

    if let Some(ref sym) = q.symbol {
        let symbol = normalize_symbol(sym);
        let rates = state
            .funding_rate_service
            .get_funding_history(&symbol, limit, None)
            .await
            .map_err(|e| internal_error(&e.to_string()))?;

        let items: Vec<FundingRateItem> = rates
            .into_iter()
            .map(|r| FundingRateItem {
                symbol: r.symbol,
                funding_rate: r.funding_rate.to_string(),
                funding_time: r.next_funding_time.timestamp_millis(),
                mark_price: r.mark_price.to_string(),
            })
            .collect();
        return Ok(Json(items));
    }

    // No symbol specified: return current rates for all symbols
    let all_rates = state.funding_rate_service.get_all_funding_rates().await;
    let items: Vec<FundingRateItem> = all_rates
        .into_iter()
        .map(|r| FundingRateItem {
            symbol: r.symbol,
            funding_rate: r.funding_rate.to_string(),
            funding_time: r.next_funding_time.timestamp_millis(),
            mark_price: r.mark_price.to_string(),
        })
        .collect();
    Ok(Json(items))
}

// ─── 12. Open Interest ────────────────────────────────────────────
// GET /fapi/v1/openInterest

#[derive(Debug, Deserialize)]
pub struct OiQuery {
    pub symbol: String,
}

#[derive(Serialize)]
pub struct OpenInterestResponse {
    pub symbol: String,
    #[serde(rename = "openInterest")]
    pub open_interest: String,
    pub time: i64,
}

pub async fn open_interest(
    State(state): State<Arc<AppState>>,
    Query(q): Query<OiQuery>,
) -> ApiResult<OpenInterestResponse> {
    let symbol = normalize_symbol(&q.symbol);
    let (long_oi, short_oi) = state
        .market_config_service
        .get_open_interest(&symbol)
        .await
        .map_err(|e| internal_error(&e.to_string()))?;

    Ok(Json(OpenInterestResponse {
        symbol,
        open_interest: (long_oi + short_oi).to_string(),
        time: Utc::now().timestamp_millis(),
    }))
}

// ─── 13–16. /futures/data/* endpoints ─────────────────────────────
// Shared query for all /futures/data/ endpoints

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct FuturesDataQuery {
    pub symbol: String,
    pub period: Option<String>,
    pub limit: Option<i64>,
    #[serde(rename = "startTime")]
    pub start_time: Option<i64>,
    #[serde(rename = "endTime")]
    pub end_time: Option<i64>,
}

fn period_to_seconds(period: &str) -> Result<i64, (StatusCode, Json<BinanceError>)> {
    match period {
        "5m" => Ok(300),
        "15m" => Ok(900),
        "30m" => Ok(1800),
        "1h" => Ok(3600),
        "2h" => Ok(7200),
        "4h" => Ok(14400),
        "6h" => Ok(21600),
        "12h" => Ok(43200),
        "1d" => Ok(86400),
        _ => Err(bad_request(
            -1120,
            "Invalid period. Use: 5m,15m,30m,1h,2h,4h,6h,12h,1d",
        )),
    }
}

// ─── 13. Open Interest Statistics ─────────────────────────────────
// GET /futures/data/openInterestHist

#[derive(Serialize)]
pub struct OiHistItem {
    pub symbol: String,
    #[serde(rename = "sumOpenInterest")]
    pub sum_open_interest: String,
    #[serde(rename = "sumOpenInterestValue")]
    pub sum_open_interest_value: String,
    pub timestamp: String,
}

pub async fn open_interest_hist(
    State(state): State<Arc<AppState>>,
    Query(q): Query<FuturesDataQuery>,
) -> ApiResult<Vec<OiHistItem>> {
    let symbol = normalize_symbol(&q.symbol);
    let period = q.period.as_deref().unwrap_or("5m");
    let interval_secs = period_to_seconds(period)?;
    let limit = q.limit.unwrap_or(30).min(500);

    let rows: Vec<(DateTime<Utc>, Decimal, Decimal, Decimal)> = sqlx::query_as(
        r#"
        SELECT
            date_trunc('hour', created_at) +
                (EXTRACT(EPOCH FROM created_at - date_trunc('hour', created_at))::int / $3 * $3) * interval '1 second'
                AS bucket,
            AVG(long_oi_usd)::numeric(30,4),
            AVG(short_oi_usd)::numeric(30,4),
            AVG(total_oi_usd)::numeric(30,4)
        FROM market_fee_snapshots
        WHERE symbol = $1
        GROUP BY bucket
        ORDER BY bucket DESC
        LIMIT $2
        "#,
    )
    .bind(&symbol)
    .bind(limit)
    .bind(interval_secs as i32)
    .fetch_all(&state.db.pool)
    .await
    .map_err(|e| internal_error(&e.to_string()))?;

    let data: Vec<OiHistItem> = rows
        .into_iter()
        .rev()
        .map(|(ts, _long, _short, total)| OiHistItem {
            symbol: symbol.clone(),
            sum_open_interest: total.to_string(),
            sum_open_interest_value: total.to_string(),
            timestamp: ts.timestamp_millis().to_string(),
        })
        .collect();

    Ok(Json(data))
}

// ─── 14. Taker Buy/Sell Volume ────────────────────────────────────
// GET /futures/data/takerlongshortRatio

#[derive(Serialize)]
pub struct TakerVolItem {
    #[serde(rename = "buySellRatio")]
    pub buy_sell_ratio: String,
    #[serde(rename = "buyVol")]
    pub buy_vol: String,
    #[serde(rename = "sellVol")]
    pub sell_vol: String,
    pub timestamp: String,
}

pub async fn taker_buy_sell_vol(
    State(state): State<Arc<AppState>>,
    Query(q): Query<FuturesDataQuery>,
) -> ApiResult<Vec<TakerVolItem>> {
    let symbol = normalize_symbol(&q.symbol);
    let period = q.period.as_deref().unwrap_or("5m");
    let interval_secs = period_to_seconds(period)?;
    let limit = q.limit.unwrap_or(30).min(500);

    let rows: Vec<(DateTime<Utc>, Decimal, Decimal)> = sqlx::query_as(
        r#"
        SELECT
            date_trunc('hour', created_at) +
                (EXTRACT(EPOCH FROM created_at - date_trunc('hour', created_at))::int / $3 * $3) * interval '1 second'
                AS bucket,
            COALESCE(SUM(CASE WHEN side::text = 'buy' THEN price * amount ELSE 0 END), 0)::numeric(30,4),
            COALESCE(SUM(CASE WHEN side::text = 'sell' THEN price * amount ELSE 0 END), 0)::numeric(30,4)
        FROM trades
        WHERE symbol = $1
        GROUP BY bucket
        ORDER BY bucket DESC
        LIMIT $2
        "#,
    )
    .bind(&symbol)
    .bind(limit)
    .bind(interval_secs as i32)
    .fetch_all(&state.db.pool)
    .await
    .map_err(|e| internal_error(&e.to_string()))?;

    let data: Vec<TakerVolItem> = rows
        .into_iter()
        .rev()
        .map(|(ts, buy, sell)| {
            let ratio = if sell > Decimal::ZERO {
                buy / sell
            } else {
                Decimal::ZERO
            };
            TakerVolItem {
                buy_sell_ratio: ratio.round_dp(4).to_string(),
                buy_vol: buy.to_string(),
                sell_vol: sell.to_string(),
                timestamp: ts.timestamp_millis().to_string(),
            }
        })
        .collect();

    Ok(Json(data))
}

// ─── 15. Top Long/Short Account Ratio ────────────────────────────
// GET /futures/data/topLongShortAccountRatio

#[derive(Serialize)]
pub struct LsRatioItem {
    pub symbol: String,
    #[serde(rename = "longShortRatio")]
    pub long_short_ratio: String,
    #[serde(rename = "longAccount")]
    pub long_account: String,
    #[serde(rename = "shortAccount")]
    pub short_account: String,
    pub timestamp: String,
}

pub async fn top_long_short_account_ratio(
    State(state): State<Arc<AppState>>,
    Query(q): Query<FuturesDataQuery>,
) -> ApiResult<Vec<LsRatioItem>> {
    let symbol = normalize_symbol(&q.symbol);

    // Top 20% accounts by position size
    let result: Option<(i64, i64)> = sqlx::query_as(
        r#"
        WITH ranked AS (
            SELECT side::text, size_in_usd,
                   NTILE(5) OVER (ORDER BY size_in_usd DESC) AS tier
            FROM positions
            WHERE symbol = $1 AND status = 'open' AND size_in_usd > 0
        )
        SELECT
            COUNT(*) FILTER (WHERE side = 'long'),
            COUNT(*) FILTER (WHERE side = 'short')
        FROM ranked
        WHERE tier = 1
        "#,
    )
    .bind(&symbol)
    .fetch_optional(&state.db.pool)
    .await
    .map_err(|e| internal_error(&e.to_string()))?;

    let (lc, sc) = result.unwrap_or((0, 0));
    let total = lc + sc;
    let (la, sa) = if total > 0 {
        (
            Decimal::from(lc) / Decimal::from(total),
            Decimal::from(sc) / Decimal::from(total),
        )
    } else {
        (Decimal::new(5, 1), Decimal::new(5, 1))
    };
    let ratio = if sc > 0 {
        Decimal::from(lc) / Decimal::from(sc)
    } else {
        Decimal::ZERO
    };

    Ok(Json(vec![LsRatioItem {
        symbol,
        long_short_ratio: ratio.round_dp(4).to_string(),
        long_account: la.round_dp(4).to_string(),
        short_account: sa.round_dp(4).to_string(),
        timestamp: Utc::now().timestamp_millis().to_string(),
    }]))
}

// ─── 16. Top Long/Short Position Ratio ───────────────────────────
// GET /futures/data/topLongShortPositionRatio

pub async fn top_long_short_position_ratio(
    State(state): State<Arc<AppState>>,
    Query(q): Query<FuturesDataQuery>,
) -> ApiResult<Vec<LsRatioItem>> {
    let symbol = normalize_symbol(&q.symbol);

    // Top 20% traders by position size (by notional value)
    let result: Option<(Decimal, Decimal)> = sqlx::query_as(
        r#"
        WITH ranked AS (
            SELECT side::text, size_in_usd,
                   NTILE(5) OVER (ORDER BY size_in_usd DESC) AS tier
            FROM positions
            WHERE symbol = $1 AND status = 'open' AND size_in_usd > 0
        )
        SELECT
            COALESCE(SUM(CASE WHEN side = 'long' THEN size_in_usd ELSE 0 END), 0),
            COALESCE(SUM(CASE WHEN side = 'short' THEN size_in_usd ELSE 0 END), 0)
        FROM ranked
        WHERE tier = 1
        "#,
    )
    .bind(&symbol)
    .fetch_optional(&state.db.pool)
    .await
    .map_err(|e| internal_error(&e.to_string()))?;

    let (long_oi, short_oi) = result.unwrap_or((Decimal::ZERO, Decimal::ZERO));
    let total = long_oi + short_oi;
    let (la, sa) = if total > Decimal::ZERO {
        (long_oi / total, short_oi / total)
    } else {
        (Decimal::new(5, 1), Decimal::new(5, 1))
    };
    let ratio = if short_oi > Decimal::ZERO {
        long_oi / short_oi
    } else {
        Decimal::ZERO
    };

    Ok(Json(vec![LsRatioItem {
        symbol,
        long_short_ratio: ratio.round_dp(4).to_string(),
        long_account: la.round_dp(4).to_string(),
        short_account: sa.round_dp(4).to_string(),
        timestamp: Utc::now().timestamp_millis().to_string(),
    }]))
}
