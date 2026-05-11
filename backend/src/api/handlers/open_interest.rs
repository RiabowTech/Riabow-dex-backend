//! Open Interest & Market Data Public API (Binance-compatible)
//!
//! Endpoints:
//!   GET /open-interest/:symbol                — Current open interest
//!   GET /open-interest/:symbol/history        — Historical OI statistics
//!   GET /open-interest/:symbol/ratio          — Global long/short position ratio
//!   GET /open-interest/:symbol/accounts       — Global long/short account ratio
//!   GET /open-interest/:symbol/top-positions  — Top trader long/short position ratio
//!   GET /open-interest/:symbol/top-accounts   — Top trader long/short account ratio
//!   GET /open-interest/:symbol/taker-volume   — Taker buy/sell volume
//!   GET /open-interest/:symbol/leverage-brackets — Notional & leverage brackets

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::AppState;

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub code: String,
}

type ApiError = (StatusCode, Json<ErrorResponse>);

fn api_err(msg: &str, code: &str) -> ApiError {
    (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: msg.to_string(), code: code.to_string() }))
}

fn bad_req(msg: &str, code: &str) -> ApiError {
    (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: msg.to_string(), code: code.to_string() }))
}

// ─── Query params ───────────────────────────────────────────────

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct HistoryQuery {
    pub period: Option<String>,   // 5m, 15m, 30m, 1h, 2h, 4h, 6h, 12h, 1d
    pub limit: Option<i64>,       // default 30, max 500
    #[serde(rename = "startTime")]
    pub start_time: Option<i64>,
    #[serde(rename = "endTime")]
    pub end_time: Option<i64>,
}

fn period_to_seconds(period: &str) -> Result<i64, ApiError> {
    match period {
        "5m"  => Ok(300),
        "15m" => Ok(900),
        "30m" => Ok(1800),
        "1h"  => Ok(3600),
        "2h"  => Ok(7200),
        "4h"  => Ok(14400),
        "6h"  => Ok(21600),
        "12h" => Ok(43200),
        "1d"  => Ok(86400),
        _     => Err(bad_req("Invalid period. Use: 5m,15m,30m,1h,2h,4h,6h,12h,1d", "INVALID_PERIOD")),
    }
}

// ─── 1. Current Open Interest ───────────────────────────────────
// Binance: GET /fapi/v1/openInterest

#[derive(Debug, Serialize)]
pub struct OpenInterestResponse {
    pub symbol: String,
    #[serde(rename = "openInterest")]
    pub open_interest: String,
    #[serde(rename = "longOi")]
    pub long_oi: String,
    #[serde(rename = "shortOi")]
    pub short_oi: String,
    pub time: i64,
}

pub async fn get_open_interest(
    State(state): State<Arc<AppState>>,
    Path(symbol): Path<String>,
) -> Result<Json<OpenInterestResponse>, ApiError> {
    let symbol = symbol.to_uppercase();
    let (long_oi, short_oi) = state.market_config_service.get_open_interest(&symbol).await
        .map_err(|e| api_err(&e.to_string(), "QUERY_FAILED"))?;

    Ok(Json(OpenInterestResponse {
        symbol,
        open_interest: (long_oi + short_oi).to_string(),
        long_oi: long_oi.to_string(),
        short_oi: short_oi.to_string(),
        time: Utc::now().timestamp_millis(),
    }))
}

// ─── 2. Open Interest Statistics (History) ──────────────────────
// Binance: GET /futures/data/openInterestHist

#[derive(Debug, Serialize)]
pub struct OiHistRecord {
    pub symbol: String,
    #[serde(rename = "sumOpenInterest")]
    pub sum_open_interest: String,
    #[serde(rename = "sumOpenInterestValue")]
    pub sum_open_interest_value: String,
    #[serde(rename = "longOi")]
    pub long_oi: String,
    #[serde(rename = "shortOi")]
    pub short_oi: String,
    pub timestamp: String,
}

pub async fn get_oi_history(
    State(state): State<Arc<AppState>>,
    Path(symbol): Path<String>,
    Query(query): Query<HistoryQuery>,
) -> Result<Json<Vec<OiHistRecord>>, ApiError> {
    let symbol = symbol.to_uppercase();
    let period = query.period.as_deref().unwrap_or("5m");
    let interval_secs = period_to_seconds(period)?;
    let limit = query.limit.unwrap_or(30).min(500);

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
    .map_err(|e| api_err(&e.to_string(), "QUERY_FAILED"))?;

    let data: Vec<OiHistRecord> = rows.into_iter().rev().map(|(ts, long, short, total)| {
        OiHistRecord {
            symbol: symbol.clone(),
            sum_open_interest: total.to_string(),
            sum_open_interest_value: total.to_string(),
            long_oi: long.to_string(),
            short_oi: short.to_string(),
            timestamp: ts.timestamp_millis().to_string(),
        }
    }).collect();

    Ok(Json(data))
}

// ─── 3. Global Long/Short Account Ratio ─────────────────────────
// Binance: GET /futures/data/globalLongShortAccountRatio

#[derive(Debug, Serialize)]
pub struct LsRatioRecord {
    pub symbol: String,
    #[serde(rename = "longShortRatio")]
    pub long_short_ratio: String,
    #[serde(rename = "longAccount")]
    pub long_account: String,
    #[serde(rename = "shortAccount")]
    pub short_account: String,
    pub timestamp: String,
}

pub async fn get_account_ratio(
    State(state): State<Arc<AppState>>,
    Path(symbol): Path<String>,
    Query(_query): Query<HistoryQuery>,
) -> Result<Json<Vec<LsRatioRecord>>, ApiError> {
    let symbol = symbol.to_uppercase();

    let current: Option<(i64, i64)> = sqlx::query_as(
        "SELECT COUNT(*) FILTER (WHERE side = 'long'), COUNT(*) FILTER (WHERE side = 'short') FROM positions WHERE symbol = $1 AND status = 'open'"
    )
    .bind(&symbol)
    .fetch_optional(&state.db.pool)
    .await
    .map_err(|e| api_err(&e.to_string(), "QUERY_FAILED"))?;

    let (lc, sc) = current.unwrap_or((0, 0));
    let total = lc + sc;
    let (la, sa) = if total > 0 {
        (Decimal::from(lc) / Decimal::from(total), Decimal::from(sc) / Decimal::from(total))
    } else {
        (Decimal::new(5, 1), Decimal::new(5, 1))
    };
    let ratio = if sc > 0 { Decimal::from(lc) / Decimal::from(sc) } else { Decimal::ZERO };

    Ok(Json(vec![LsRatioRecord {
        symbol,
        long_short_ratio: ratio.round_dp(4).to_string(),
        long_account: la.round_dp(4).to_string(),
        short_account: sa.round_dp(4).to_string(),
        timestamp: Utc::now().timestamp_millis().to_string(),
    }]))
}

// ─── 4. Global Long/Short Position Ratio ────────────────────────
// Binance: (derived from OI snapshots)

pub async fn get_ls_ratio(
    State(state): State<Arc<AppState>>,
    Path(symbol): Path<String>,
    Query(query): Query<HistoryQuery>,
) -> Result<Json<Vec<LsRatioRecord>>, ApiError> {
    let symbol = symbol.to_uppercase();
    let period = query.period.as_deref().unwrap_or("5m");
    let interval_secs = period_to_seconds(period)?;
    let limit = query.limit.unwrap_or(30).min(500);

    let rows: Vec<(DateTime<Utc>, Decimal, Decimal)> = sqlx::query_as(
        r#"
        SELECT
            date_trunc('hour', created_at) +
                (EXTRACT(EPOCH FROM created_at - date_trunc('hour', created_at))::int / $3 * $3) * interval '1 second'
                AS bucket,
            AVG(long_oi_usd)::numeric(30,4),
            AVG(short_oi_usd)::numeric(30,4)
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
    .map_err(|e| api_err(&e.to_string(), "QUERY_FAILED"))?;

    let data: Vec<LsRatioRecord> = rows.into_iter().rev().map(|(ts, long_oi, short_oi)| {
        let total = long_oi + short_oi;
        let (la, sa) = if total > Decimal::ZERO {
            (long_oi / total, short_oi / total)
        } else {
            (Decimal::new(5, 1), Decimal::new(5, 1))
        };
        let ratio = if short_oi > Decimal::ZERO { long_oi / short_oi } else { Decimal::ZERO };
        LsRatioRecord {
            symbol: symbol.clone(),
            long_short_ratio: ratio.round_dp(4).to_string(),
            long_account: la.round_dp(4).to_string(),
            short_account: sa.round_dp(4).to_string(),
            timestamp: ts.timestamp_millis().to_string(),
        }
    }).collect();

    Ok(Json(data))
}

// ─── 5. Top Trader Long/Short Position Ratio ────────────────────
// Binance: GET /futures/data/topLongShortPositionRatio

pub async fn get_top_position_ratio(
    State(state): State<Arc<AppState>>,
    Path(symbol): Path<String>,
    Query(_query): Query<HistoryQuery>,
) -> Result<Json<Vec<LsRatioRecord>>, ApiError> {
    let symbol = symbol.to_uppercase();

    // Top 20% traders by position size
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
    .map_err(|e| api_err(&e.to_string(), "QUERY_FAILED"))?;

    let (long_oi, short_oi) = result.unwrap_or((Decimal::ZERO, Decimal::ZERO));
    let total = long_oi + short_oi;
    let (la, sa) = if total > Decimal::ZERO {
        (long_oi / total, short_oi / total)
    } else {
        (Decimal::new(5, 1), Decimal::new(5, 1))
    };
    let ratio = if short_oi > Decimal::ZERO { long_oi / short_oi } else { Decimal::ZERO };

    Ok(Json(vec![LsRatioRecord {
        symbol,
        long_short_ratio: ratio.round_dp(4).to_string(),
        long_account: la.round_dp(4).to_string(),
        short_account: sa.round_dp(4).to_string(),
        timestamp: Utc::now().timestamp_millis().to_string(),
    }]))
}

// ─── 6. Top Trader Long/Short Account Ratio ─────────────────────
// Binance: GET /futures/data/topLongShortAccountRatio

pub async fn get_top_account_ratio(
    State(state): State<Arc<AppState>>,
    Path(symbol): Path<String>,
    Query(_query): Query<HistoryQuery>,
) -> Result<Json<Vec<LsRatioRecord>>, ApiError> {
    let symbol = symbol.to_uppercase();

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
    .map_err(|e| api_err(&e.to_string(), "QUERY_FAILED"))?;

    let (lc, sc) = result.unwrap_or((0, 0));
    let total = lc + sc;
    let (la, sa) = if total > 0 {
        (Decimal::from(lc) / Decimal::from(total), Decimal::from(sc) / Decimal::from(total))
    } else {
        (Decimal::new(5, 1), Decimal::new(5, 1))
    };
    let ratio = if sc > 0 { Decimal::from(lc) / Decimal::from(sc) } else { Decimal::ZERO };

    Ok(Json(vec![LsRatioRecord {
        symbol,
        long_short_ratio: ratio.round_dp(4).to_string(),
        long_account: la.round_dp(4).to_string(),
        short_account: sa.round_dp(4).to_string(),
        timestamp: Utc::now().timestamp_millis().to_string(),
    }]))
}

// ─── 7. Taker Buy/Sell Volume ───────────────────────────────────
// Binance: GET /futures/data/takerlongshortRatio

#[derive(Debug, Serialize)]
pub struct TakerVolumeRecord {
    #[serde(rename = "buySellRatio")]
    pub buy_sell_ratio: String,
    #[serde(rename = "buyVol")]
    pub buy_vol: String,
    #[serde(rename = "sellVol")]
    pub sell_vol: String,
    pub timestamp: String,
}

pub async fn get_taker_volume(
    State(state): State<Arc<AppState>>,
    Path(symbol): Path<String>,
    Query(query): Query<HistoryQuery>,
) -> Result<Json<Vec<TakerVolumeRecord>>, ApiError> {
    let symbol = symbol.to_uppercase();
    let period = query.period.as_deref().unwrap_or("5m");
    let interval_secs = period_to_seconds(period)?;
    let limit = query.limit.unwrap_or(30).min(500);

    // Taker determines direction: taker buying = side 'buy', taker selling = side 'sell'
    // In our matching engine, `side` is the taker's side
    let rows: Vec<(DateTime<Utc>, Decimal, Decimal)> = sqlx::query_as(
        r#"
        SELECT
            date_trunc('hour', created_at) +
                (EXTRACT(EPOCH FROM created_at - date_trunc('hour', created_at))::int / $3 * $3) * interval '1 second'
                AS bucket,
            COALESCE(SUM(CASE WHEN side::text = 'buy' THEN price * amount ELSE 0 END), 0)::numeric(30,4) AS buy_vol,
            COALESCE(SUM(CASE WHEN side::text = 'sell' THEN price * amount ELSE 0 END), 0)::numeric(30,4) AS sell_vol
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
    .map_err(|e| api_err(&e.to_string(), "QUERY_FAILED"))?;

    let data: Vec<TakerVolumeRecord> = rows.into_iter().rev().map(|(ts, buy, sell)| {
        let ratio = if sell > Decimal::ZERO { buy / sell } else { Decimal::ZERO };
        TakerVolumeRecord {
            buy_sell_ratio: ratio.round_dp(4).to_string(),
            buy_vol: buy.to_string(),
            sell_vol: sell.to_string(),
            timestamp: ts.timestamp_millis().to_string(),
        }
    }).collect();

    Ok(Json(data))
}

// ─── 8. Leverage Brackets ───────────────────────────────────────
// Binance: GET /fapi/v1/leverageBracket

#[derive(Debug, Serialize)]
pub struct BracketEntry {
    pub bracket: i32,
    #[serde(rename = "initialLeverage")]
    pub initial_leverage: i32,
    #[serde(rename = "notionalCap")]
    pub notional_cap: i64,
    #[serde(rename = "notionalFloor")]
    pub notional_floor: i64,
    #[serde(rename = "maintMarginRatio")]
    pub maint_margin_ratio: String,
    pub cum: String,
}

#[derive(Debug, Serialize)]
pub struct LeverageBracketResponse {
    pub symbol: String,
    pub brackets: Vec<BracketEntry>,
}

pub async fn get_leverage_brackets(
    State(state): State<Arc<AppState>>,
    Path(symbol): Path<String>,
) -> Result<Json<Vec<LeverageBracketResponse>>, ApiError> {
    let symbol = symbol.to_uppercase();

    let config = state.market_config_service.get_config(&symbol).await;
    let max_lev = config.map(|c| c.max_leverage).unwrap_or(50);

    // Generate standard brackets based on max leverage
    let tiers: Vec<(i32, i64, i64, &str, &str)> = vec![
        (max_lev, 50_000, 0, "0.004", "0"),
        (max_lev.min(50), 250_000, 50_000, "0.005", "50"),
        (max_lev.min(25), 1_000_000, 250_000, "0.01", "1300"),
        (max_lev.min(10), 5_000_000, 1_000_000, "0.025", "16300"),
        (max_lev.min(5), 10_000_000, 5_000_000, "0.05", "141300"),
        (max_lev.min(2), 50_000_000, 10_000_000, "0.1", "641300"),
        (1, 100_000_000, 50_000_000, "0.125", "1891300"),
    ];

    let brackets: Vec<BracketEntry> = tiers.into_iter().enumerate().map(|(i, (lev, cap, floor, mmr, cum))| {
        BracketEntry {
            bracket: (i + 1) as i32,
            initial_leverage: lev,
            notional_cap: cap,
            notional_floor: floor,
            maint_margin_ratio: mmr.to_string(),
            cum: cum.to_string(),
        }
    }).collect();

    Ok(Json(vec![LeverageBracketResponse { symbol, brackets }]))
}
