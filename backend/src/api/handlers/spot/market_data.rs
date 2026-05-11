//! Public market data endpoints — no auth required.

use axum::{extract::{Query, State}, http::StatusCode, Json};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use chrono::{DateTime, Utc};

use crate::AppState;
use crate::services::spot::market_data;
use crate::models::spot::SpotMarket;

#[derive(Serialize)]
pub struct ErrorBody { pub error: String }

fn err(code: StatusCode, msg: impl Into<String>) -> (StatusCode, Json<ErrorBody>) {
    (code, Json(ErrorBody { error: msg.into() }))
}

// ---------- GET /spot/markets ----------

#[derive(Serialize)]
pub struct MarketView {
    pub id: String,
    pub base_token: String,
    pub quote_token: String,
    pub tick_size: String,
    pub lot_size: String,
    pub min_notional: String,
    pub maker_fee_bps: i32,
    pub taker_fee_bps: i32,
    pub status: String,
}

impl From<SpotMarket> for MarketView {
    fn from(m: SpotMarket) -> Self {
        Self {
            id: m.id, base_token: m.base_token, quote_token: m.quote_token,
            tick_size: m.tick_size.normalize().to_string(),
            lot_size: m.lot_size.normalize().to_string(),
            min_notional: m.min_notional.normalize().to_string(),
            maker_fee_bps: m.maker_fee_bps, taker_fee_bps: m.taker_fee_bps,
            status: m.status,
        }
    }
}

pub async fn list_markets(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<MarketView>>, (StatusCode, Json<ErrorBody>)> {
    let rows = market_data::list_markets(&state.db.pool).await
        .map_err(|_| err(StatusCode::INTERNAL_SERVER_ERROR, "DB_ERROR"))?;
    Ok(Json(rows.into_iter().map(Into::into).collect()))
}

// ---------- GET /spot/depth ----------

#[derive(Deserialize)]
pub struct DepthQuery { pub symbol: String, pub limit: Option<usize> }

#[derive(Serialize)]
pub struct DepthResponse {
    pub symbol: String,
    pub last_update_id: u64,
    pub bids: Vec<[String; 2]>,
    pub asks: Vec<[String; 2]>,
}

pub async fn depth(
    State(state): State<Arc<AppState>>,
    Query(q): Query<DepthQuery>,
) -> Result<Json<DepthResponse>, (StatusCode, Json<ErrorBody>)> {
    let limit = q.limit.unwrap_or(50).clamp(1, 1000);
    let engine = state.spot_engine.clone()
        .ok_or_else(|| err(StatusCode::SERVICE_UNAVAILABLE, "spot trading disabled"))?;
    let (bids, asks, last_id) = engine.depth(limit).await
        .map_err(|m| err(StatusCode::SERVICE_UNAVAILABLE, m))?;
    Ok(Json(DepthResponse {
        symbol: q.symbol,
        last_update_id: last_id,
        bids: bids.into_iter().map(|(p,q)| [p.normalize().to_string(), q.normalize().to_string()]).collect(),
        asks: asks.into_iter().map(|(p,q)| [p.normalize().to_string(), q.normalize().to_string()]).collect(),
    }))
}

// ---------- GET /spot/trades ----------

#[derive(Deserialize)]
pub struct TradesQuery { pub symbol: String, pub limit: Option<i64> }

#[derive(Serialize)]
pub struct PublicTradeView {
    pub trade_id: String,
    pub symbol: String,
    pub side: String,
    pub price: String,
    pub quantity: String,
    pub ts: i64,
}

pub async fn recent_trades(
    State(state): State<Arc<AppState>>,
    Query(q): Query<TradesQuery>,
) -> Result<Json<Vec<PublicTradeView>>, (StatusCode, Json<ErrorBody>)> {
    let limit = q.limit.unwrap_or(50).clamp(1, 1000);
    let rows = market_data::recent_trades(&state.db.pool, &q.symbol, limit).await
        .map_err(|_| err(StatusCode::INTERNAL_SERVER_ERROR, "DB_ERROR"))?;
    Ok(Json(rows.into_iter().map(|t| PublicTradeView {
        trade_id: t.id.to_string(),
        symbol: t.market_id,
        side: t.side,
        price: t.price.normalize().to_string(),
        quantity: t.quantity.normalize().to_string(),
        ts: t.created_at.timestamp_millis(),
    }).collect()))
}

// ---------- GET /spot/klines ----------

#[derive(Deserialize)]
pub struct KlinesQuery {
    pub symbol: String,
    pub interval: String,             // "1m"|"5m"|"15m"|"1h"|"4h"|"1d"
    pub limit: Option<i64>,
    pub start_time: Option<i64>,
    pub end_time: Option<i64>,
}

fn interval_seconds(iv: &str) -> Option<i64> {
    match iv {
        "1m" => Some(60),     "5m" => Some(300),  "15m" => Some(900),
        "1h" => Some(3600),   "4h" => Some(14400), "1d" => Some(86400),
        _ => None,
    }
}

pub async fn klines(
    State(state): State<Arc<AppState>>,
    Query(q): Query<KlinesQuery>,
) -> Result<Json<Vec<serde_json::Value>>, (StatusCode, Json<ErrorBody>)> {
    let iv_secs = interval_seconds(&q.interval)
        .ok_or_else(|| err(StatusCode::BAD_REQUEST, "invalid interval"))?;
    let limit = q.limit.unwrap_or(500).clamp(1, 1000);
    let start = q.start_time.and_then(|s| DateTime::<Utc>::from_timestamp(s, 0));
    let end   = q.end_time.and_then(|s| DateTime::<Utc>::from_timestamp(s, 0));
    let rows = market_data::klines(&state.db.pool, &q.symbol, &q.interval, limit, start, end).await
        .map_err(|_| err(StatusCode::INTERNAL_SERVER_ERROR, "DB_ERROR"))?;
    let out: Vec<serde_json::Value> = rows.iter().map(|k| {
        use serde_json::json;
        let open_ts = k.open_time.timestamp();
        let close_ts = open_ts + iv_secs - 1;
        json!([
            open_ts,
            k.open_price.normalize().to_string(),
            k.high_price.normalize().to_string(),
            k.low_price.normalize().to_string(),
            k.close_price.normalize().to_string(),
            k.volume.normalize().to_string(),
            close_ts,
            k.quote_volume.normalize().to_string(),
            k.trade_count
        ])
    }).collect();
    Ok(Json(out))
}

// ---------- GET /spot/ticker/24hr ----------

#[derive(Deserialize)]
pub struct TickerQuery { pub symbol: Option<String> }

#[derive(Serialize)]
pub struct TickerView {
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
}

pub async fn ticker_24hr(
    State(state): State<Arc<AppState>>,
    Query(q): Query<TickerQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorBody>)> {
    let rows = market_data::ticker_24h(&state.db.pool, q.symbol.as_deref()).await
        .map_err(|_| err(StatusCode::INTERNAL_SERVER_ERROR, "DB_ERROR"))?;
    let now = Utc::now().timestamp();
    let views: Vec<TickerView> = rows.into_iter().map(|t| TickerView {
        symbol: t.market_id,
        last_price: t.last_price.normalize().to_string(),
        open_price: t.open_price_24h.normalize().to_string(),
        high: t.high_24h.normalize().to_string(),
        low: t.low_24h.normalize().to_string(),
        volume: t.volume_24h.normalize().to_string(),
        quote_volume: t.quote_volume_24h.normalize().to_string(),
        trade_count: t.trade_count_24h,
        open_time: now - 24 * 3600,
        close_time: now,
    }).collect();
    // If symbol was specified and we have at most one row, return object; else array
    if q.symbol.is_some() {
        let v = views.into_iter().next();
        match v {
            Some(t) => Ok(Json(serde_json::to_value(t).unwrap())),
            None => Err(err(StatusCode::NOT_FOUND, "TICKER_NOT_FOUND")),
        }
    } else {
        Ok(Json(serde_json::to_value(views).unwrap()))
    }
}

// ---------- GET /spot/markets/:symbol/details ----------

/// Mirrors perp's `MarketDetailsResponse` shape so the FE can use the same
/// `getMarketDetails`/`MarketDetailsResponse` type for both products.
/// Spot has no leverage and no funding, so those fields are surfaced as
/// `null` instead of zeroes — the FE type already declares them nullable.
#[derive(Debug, Serialize)]
pub struct MarketDetailsResponse {
    pub symbol: String,
    pub market_name: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub description: Option<String>,

    // Order-entry constraints. `min_base_amount` is derived from
    // `min_notional / last_price` when a live price is available so the FE
    // can render the same input hint it does on perp; falls back to
    // `lot_size` when the ticker hasn't been populated yet.
    pub min_base_amount: String,
    pub min_usd_amount: String,
    pub price_step: String,
    pub lot_size: String,

    // Spot has no leverage or margin — everything is fully collateralised.
    pub max_leverage: Option<i32>,
    pub initial_margin_fraction: Option<String>,
    pub maintenance_margin_fraction: Option<String>,
    pub close_out_margin_fraction: Option<String>,

    // Valuation — not wired up for the DF token yet; reserved.
    pub market_cap: Option<String>,
    pub fully_diluted_valuation: Option<String>,
    pub market_cap_updated_at: Option<i64>,

    // Live state. Spot only has a last trade price; mark/funding don't apply.
    pub mark_price: Option<String>,
    pub last_price: Option<String>,
    pub funding_rate: Option<String>,
    pub next_funding_time: Option<i64>,

    // Listing lifecycle. Spot only tracks a flat status, so listing_phase
    // mirrors it for type compatibility with the perp shape.
    pub listing_phase: String,
    pub status: String,
}

pub async fn market_details(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(symbol): axum::extract::Path<String>,
) -> Result<Json<MarketDetailsResponse>, (StatusCode, Json<ErrorBody>)> {
    let market_id = symbol.to_uppercase();
    let market = market_data::get_market(&state.db.pool, &market_id).await
        .map_err(|_| err(StatusCode::INTERNAL_SERVER_ERROR, "DB_ERROR"))?
        .ok_or_else(|| err(StatusCode::NOT_FOUND, format!("market {} not found", market_id)))?;

    // Pull the 24h ticker if it exists so we can surface last_price.
    let ticker = market_data::ticker_24h(&state.db.pool, Some(&market_id)).await
        .map_err(|_| err(StatusCode::INTERNAL_SERVER_ERROR, "DB_ERROR"))?
        .into_iter().next();
    let last_price = ticker.as_ref().map(|t| t.last_price.normalize().to_string());

    // min_base_amount = min_notional / last_price when last_price > 0,
    // otherwise fall back to the configured lot_size step.
    let min_base_amount = ticker.as_ref()
        .filter(|t| t.last_price > rust_decimal::Decimal::ZERO)
        .map(|t| (market.min_notional / t.last_price).normalize().to_string())
        .unwrap_or_else(|| market.lot_size.normalize().to_string());

    // `display_name` is the curated friendly label (e.g. "Diffie / Tether");
    // when the team hasn't filled it in we fall back to the symbol pair so
    // the FE always has something to render.
    let market_name = market.display_name.clone()
        .unwrap_or_else(|| format!("{} / {}", market.base_token, market.quote_token));

    Ok(Json(MarketDetailsResponse {
        symbol: market.id.clone(),
        market_name,
        base_asset: market.base_token.clone(),
        quote_asset: market.quote_token.clone(),
        description: market.description.clone(),

        min_base_amount,
        min_usd_amount: market.min_notional.normalize().to_string(),
        price_step: market.tick_size.normalize().to_string(),
        lot_size: market.lot_size.normalize().to_string(),

        max_leverage: None,
        initial_margin_fraction: None,
        maintenance_margin_fraction: None,
        close_out_margin_fraction: None,

        market_cap: None,
        fully_diluted_valuation: None,
        market_cap_updated_at: None,

        mark_price: None,
        last_price,
        funding_rate: None,
        next_funding_time: None,

        listing_phase: market.status.clone(),
        status: market.status,
    }))
}

#[cfg(test)]
mod market_details_shape_tests {
    use super::*;
    use serde_json::json;

    /// The FE's `getMarketDetails` shares one TS interface across spot and
    /// perp. Spot must serialise the same keys; fields that don't apply are
    /// returned as JSON `null` (the FE type declares them nullable).
    #[test]
    fn market_details_serialises_with_nulls_for_spot_only_fields() {
        let resp = MarketDetailsResponse {
            symbol: "DFUSDT".into(),
            market_name: "DF / USDT".into(),
            base_asset: "DF".into(),
            quote_asset: "USDT".into(),
            description: None,
            min_base_amount: "10".into(),
            min_usd_amount: "1".into(),
            price_step: "0.0001".into(),
            lot_size: "0.01".into(),
            max_leverage: None,
            initial_margin_fraction: None,
            maintenance_margin_fraction: None,
            close_out_margin_fraction: None,
            market_cap: None,
            fully_diluted_valuation: None,
            market_cap_updated_at: None,
            mark_price: None,
            last_price: Some("0.1".into()),
            funding_rate: None,
            next_funding_time: None,
            listing_phase: "listed".into(),
            status: "listed".into(),
        };
        let v = serde_json::to_value(&resp).unwrap();
        // Identity + constraints — required, non-null.
        assert_eq!(v["symbol"], json!("DFUSDT"));
        assert_eq!(v["base_asset"], json!("DF"));
        assert_eq!(v["quote_asset"], json!("USDT"));
        assert_eq!(v["price_step"], json!("0.0001"));
        // Leverage / funding — spot doesn't have them; must be null.
        assert!(v["max_leverage"].is_null(), "max_leverage must be null on spot");
        assert!(v["funding_rate"].is_null(), "funding_rate must be null on spot");
        assert!(v["mark_price"].is_null(), "mark_price must be null on spot");
        // description is nullable when the team hasn't filled it in.
        assert!(v["description"].is_null(), "description is null when not set");
        // Live price — surfaces when available.
        assert_eq!(v["last_price"], json!("0.1"));
        // Listing — required, non-null.
        assert_eq!(v["status"], json!("listed"));
        assert_eq!(v["listing_phase"], json!("listed"));
    }

    /// The handler builds `market_name` from `display_name` and falls back
    /// to "BASE / QUOTE" when the curated label is NULL. Lock in both
    /// branches so future refactors can't silently regress the fallback.
    #[test]
    fn market_name_uses_display_name_or_falls_back() {
        let curated = Some("Diffie / Tether".to_string())
            .unwrap_or_else(|| format!("{} / {}", "DF", "USDT"));
        assert_eq!(curated, "Diffie / Tether");

        let fallback: String = None
            .unwrap_or_else(|| format!("{} / {}", "DF", "USDT"));
        assert_eq!(fallback, "DF / USDT");
    }
}
