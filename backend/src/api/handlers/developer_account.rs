//! Developer API — Account Endpoints (Binance-compatible)
//!
//! Authenticated endpoints for account data:
//!   GET  /fapi/v2/balance         — Futures account balance
//!   GET  /fapi/v1/positionSide/dual — Query position mode
//!   POST /fapi/v1/positionSide/dual — Change position mode
//!   GET  /fapi/v1/marginType      — Query margin type (always CROSSED)
//!   POST /fapi/v1/marginType      — Change margin type (CROSSED only)
//!   GET  /fapi/v1/commissionRate  — User commission rate
//!   GET  /fapi/v1/income          — Income history

use axum::{
    extract::{Query, State},
    http::StatusCode,
    Extension, Json,
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::auth::middleware::AuthUser;
use crate::AppState;

// ─── Helpers ──────────────────────────────────────────────────────

/// Accept JSON booleans (`true`/`false`) or Binance-style strings
/// (`"true"`/`"false"`). Missing/null defaults to `false`.
fn deserialize_bool_or_string<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::{self, Unexpected};
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum BoolOrString {
        Bool(bool),
        Str(String),
    }
    match BoolOrString::deserialize(deserializer)? {
        BoolOrString::Bool(b) => Ok(b),
        BoolOrString::Str(s) => match s.to_ascii_lowercase().as_str() {
            "true" | "1" => Ok(true),
            "false" | "0" | "" => Ok(false),
            other => Err(de::Error::invalid_value(
                Unexpected::Str(other),
                &"\"true\" or \"false\"",
            )),
        },
    }
}

#[derive(Debug, Serialize)]
pub struct BinanceError {
    pub code: i32,
    pub msg: String,
}

type ApiResult<T> = Result<Json<T>, (StatusCode, Json<BinanceError>)>;

fn internal_error(msg: &str) -> (StatusCode, Json<BinanceError>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(BinanceError {
            code: -1001,
            msg: msg.to_string(),
        }),
    )
}

fn bad_request(code: i32, msg: &str) -> (StatusCode, Json<BinanceError>) {
    (
        StatusCode::BAD_REQUEST,
        Json(BinanceError {
            code,
            msg: msg.to_string(),
        }),
    )
}

fn normalize_symbol(s: &str) -> String {
    s.to_uppercase()
        .replace("-USD", "USDT")
        .replace("-", "")
        .replace("/", "")
        .replace("_", "")
}

// ─── 1. Futures Account Balance ───────────────────────────────────
// GET /fapi/v2/balance

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct TimestampQuery {
    pub timestamp: Option<i64>,
}

#[derive(Serialize)]
pub struct BalanceItem {
    #[serde(rename = "accountAlias")]
    pub account_alias: String,
    pub asset: String,
    pub balance: String,
    #[serde(rename = "crossWalletBalance")]
    pub cross_wallet_balance: String,
    #[serde(rename = "crossUnPnl")]
    pub cross_un_pnl: String,
    #[serde(rename = "availableBalance")]
    pub available_balance: String,
    #[serde(rename = "maxWithdrawAmount")]
    pub max_withdraw_amount: String,
    #[serde(rename = "marginAvailable")]
    pub margin_available: bool,
    #[serde(rename = "updateTime")]
    pub update_time: i64,
}

pub async fn balance(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Query(_q): Query<TimestampQuery>,
) -> ApiResult<Vec<BalanceItem>> {
    let user_addr = auth_user.address.to_lowercase();

    let rows: Vec<(String, Decimal, Decimal)> =
        sqlx::query_as("SELECT token, available, frozen FROM balances WHERE user_address = $1")
            .bind(&user_addr)
            .fetch_all(&state.db.pool)
            .await
            .map_err(|e| internal_error(&e.to_string()))?;

    // Calculate unrealized PnL across all positions
    let pnl_rows: Vec<(String, String, Decimal, Decimal)> = sqlx::query_as(
        r#"
        SELECT symbol, side::text, size_in_usd, entry_price
        FROM positions WHERE user_address = $1 AND status = 'open'
        "#,
    )
    .bind(&user_addr)
    .fetch_all(&state.db.pool)
    .await
    .unwrap_or_default();

    let symbols: Vec<String> = pnl_rows.iter().map(|(s, ..)| s.clone()).collect();
    let prices = state.price_feed_service.batch_get_mark_prices(&symbols).await;

    let mut total_unrealized_pnl = Decimal::ZERO;
    for (symbol, side, size_usd, entry_price) in &pnl_rows {
        let mark = prices.get(symbol).copied().unwrap_or(*entry_price);
        let size_tokens = if *entry_price > Decimal::ZERO {
            *size_usd / *entry_price
        } else {
            Decimal::ZERO
        };
        let pnl = if side == "long" {
            (mark - *entry_price) * size_tokens
        } else {
            (*entry_price - mark) * size_tokens
        };
        total_unrealized_pnl += pnl;
    }

    let now = Utc::now().timestamp_millis();

    let collateral = state.config.collateral_symbol().to_string();

    // If no balance rows, still return collateral token with zero balance
    if rows.is_empty() {
        return Ok(Json(vec![BalanceItem {
            account_alias: "default".to_string(),
            asset: collateral,
            balance: "0".to_string(),
            cross_wallet_balance: "0".to_string(),
            cross_un_pnl: total_unrealized_pnl.round_dp(4).to_string(),
            available_balance: "0".to_string(),
            max_withdraw_amount: "0".to_string(),
            margin_available: true,
            update_time: now,
        }]));
    }

    let items: Vec<BalanceItem> = rows
        .into_iter()
        .map(|(token, available, frozen)| {
            let total = available + frozen;
            BalanceItem {
                account_alias: "default".to_string(),
                asset: token,
                balance: total.to_string(),
                cross_wallet_balance: total.to_string(),
                cross_un_pnl: total_unrealized_pnl.round_dp(4).to_string(),
                available_balance: available.to_string(),
                max_withdraw_amount: available.to_string(),
                margin_available: true,
                update_time: now,
            }
        })
        .collect();

    Ok(Json(items))
}

// ─── 2. Position Mode ─────────────────────────────────────────────
// GET /fapi/v1/positionSide/dual

#[derive(Serialize)]
pub struct PositionModeResponse {
    #[serde(rename = "dualSidePosition")]
    pub dual_side_position: bool,
}

pub async fn get_position_mode(
    Extension(_auth_user): Extension<AuthUser>,
    Query(_q): Query<TimestampQuery>,
) -> Json<PositionModeResponse> {
    // uses one-way mode (BOTH) by default
    Json(PositionModeResponse {
        dual_side_position: false,
    })
}

// POST /fapi/v1/positionSide/dual

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct ChangePositionModeRequest {
    #[serde(
        rename = "dualSidePosition",
        default,
        deserialize_with = "deserialize_bool_or_string"
    )]
    pub dual_side_position: bool,
    pub timestamp: Option<i64>,
}

pub async fn change_position_mode(
    Extension(_auth_user): Extension<AuthUser>,
    Json(req): Json<ChangePositionModeRequest>,
) -> ApiResult<serde_json::Value> {
    // Hedge mode (`dualSidePosition=true`) is not implemented in the matching
    // engine: order entries only carry a single Side, position lookups assume
    // one open position per (user, symbol), and the unified-margin / TP-SL
    // paths all expect single-side. The handler used to accept any toggle and
    // return 200 success regardless, then `GET /fapi/v1/positionSide/dual`
    // would still report `false` — a Binance MM client setting hedge mode and
    // expecting separate LONG/SHORT bookkeeping would silently get one-way
    // mode.
    //
    // Until hedge mode is actually wired up, fail loud: accept `false` (the
    // single-side mode the system already runs in — idempotent), reject
    // `true` with a Binance-shaped error so the client surface the gap.
    if req.dual_side_position {
        return Err(bad_request(
            -4059,
            "Hedge mode (dualSidePosition=true) is not supported. \
             This exchange runs in one-way mode only.",
        ));
    }

    Ok(Json(serde_json::json!({
        "code": 200,
        "msg": "success"
    })))
}

// ─── 2b. Margin Type ──────────────────────────────────────────────
// GET /fapi/v1/marginType
//
// Bots integrating via Binance fapi SDKs read the user's current margin type
// (cross / isolated) before placing orders so they can compute IM correctly.
// Until 2026-05 the endpoint did not exist (404), and the Binance SDKs that
// poll it on startup would surface this as "auth/connectivity broken" instead
// of a missing capability. The matching engine and unified-margin paths only
// support cross-margin in the developer/HMAC namespace, so we expose a stub:
// GET always returns CROSSED, POST accepts CROSSED idempotent and rejects
// ISOLATED with a Binance-shaped error so the client surfaces the gap.
//
// Users that genuinely need isolated mode go through `POST /api/v1/account/
// margin-mode` (JWT, see `unified_margin` handler). That route is not exposed
// over fapi yet — wiring it through HMAC auth is tracked separately.

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct MarginTypeQuery {
    pub symbol: Option<String>,
    pub timestamp: Option<i64>,
}

#[derive(Serialize)]
pub struct MarginTypeResponse {
    pub symbol: Option<String>,
    #[serde(rename = "marginType")]
    pub margin_type: String,
}

pub async fn get_margin_type(
    Extension(_auth_user): Extension<AuthUser>,
    Query(q): Query<MarginTypeQuery>,
) -> Json<MarginTypeResponse> {
    Json(MarginTypeResponse {
        symbol: q.symbol.map(|s| normalize_symbol(&s)),
        margin_type: "CROSSED".to_string(),
    })
}

// POST /fapi/v1/marginType

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct ChangeMarginTypeRequest {
    pub symbol: String,
    #[serde(rename = "marginType")]
    pub margin_type: String,
    pub timestamp: Option<i64>,
}

pub async fn change_margin_type(
    Extension(_auth_user): Extension<AuthUser>,
    Json(req): Json<ChangeMarginTypeRequest>,
) -> ApiResult<serde_json::Value> {
    // Accept CROSSED case-insensitively (Binance SDKs send "CROSSED";
    // some send "CROSS"). Reject everything else loud — silent no-op
    // would let an isolated-margin strategy run thinking it has
    // isolated bookkeeping when the engine is cross-only.
    let normalized = req.margin_type.trim().to_ascii_uppercase();
    match normalized.as_str() {
        "CROSSED" | "CROSS" => Ok(Json(serde_json::json!({
            "code": 200,
            "msg": "success"
        }))),
        "ISOLATED" => Err(bad_request(
            -4046,
            "Isolated margin is not supported on the developer fapi. \
             This exchange runs in cross-margin mode only.",
        )),
        _ => Err(bad_request(
            -1128,
            &format!(
                "Invalid marginType '{}'. Expected CROSSED or ISOLATED.",
                req.margin_type
            ),
        )),
    }
}

// ─── 3. Commission Rate ──────────────────────────────────────────
// GET /fapi/v1/commissionRate

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct CommissionRateQuery {
    pub symbol: String,
    pub timestamp: Option<i64>,
}

#[derive(Serialize)]
pub struct CommissionRateResponse {
    pub symbol: String,
    #[serde(rename = "makerCommissionRate")]
    pub maker_commission_rate: String,
    #[serde(rename = "takerCommissionRate")]
    pub taker_commission_rate: String,
}

pub async fn commission_rate(
    State(state): State<Arc<AppState>>,
    Extension(_auth_user): Extension<AuthUser>,
    Query(q): Query<CommissionRateQuery>,
) -> ApiResult<CommissionRateResponse> {
    let symbol = normalize_symbol(&q.symbol);

    let config = state.market_config_service.get_config(&symbol).await;
    let (maker, taker) = match config {
        Some(c) => (c.base_maker_fee_rate, c.base_taker_fee_rate),
        None => return Err((
            StatusCode::BAD_REQUEST,
            Json(BinanceError {
                code: -1121,
                msg: format!("Invalid symbol: {}", symbol),
            }),
        )),
    };

    Ok(Json(CommissionRateResponse {
        symbol,
        maker_commission_rate: maker.to_string(),
        taker_commission_rate: taker.to_string(),
    }))
}

// ─── 4. Income History ────────────────────────────────────────────
// GET /fapi/v1/income

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct IncomeQuery {
    pub symbol: Option<String>,
    #[serde(rename = "incomeType")]
    pub income_type: Option<String>,
    #[serde(rename = "startTime")]
    pub start_time: Option<i64>,
    #[serde(rename = "endTime")]
    pub end_time: Option<i64>,
    pub limit: Option<i64>,
    pub timestamp: Option<i64>,
}

#[derive(Serialize)]
pub struct IncomeItem {
    pub symbol: String,
    #[serde(rename = "incomeType")]
    pub income_type: String,
    pub income: String,
    pub asset: String,
    pub info: String,
    pub time: i64,
    #[serde(rename = "tranId")]
    pub tran_id: String,
    #[serde(rename = "tradeId")]
    pub trade_id: String,
}

pub async fn income(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Query(q): Query<IncomeQuery>,
) -> ApiResult<Vec<IncomeItem>> {
    let user_addr = auth_user.address.to_lowercase();
    let limit = q.limit.unwrap_or(100).min(1000);
    let mut items: Vec<IncomeItem> = Vec::new();

    // 1. Realized PnL events
    if q.income_type.is_none() || q.income_type.as_deref() == Some("REALIZED_PNL") {
        let pnl_rows: Vec<(String, Decimal, DateTime<Utc>, String)> = if let Some(ref sym) = q.symbol {
            let symbol = normalize_symbol(sym);
            sqlx::query_as(
                r#"
                SELECT symbol, realized_pnl, created_at, id::text
                FROM realized_pnl_events
                WHERE user_address = $1 AND symbol = $2
                ORDER BY created_at DESC LIMIT $3
                "#,
            )
            .bind(&user_addr)
            .bind(&symbol)
            .bind(limit)
            .fetch_all(&state.db.pool)
            .await
        } else {
            sqlx::query_as(
                r#"
                SELECT symbol, realized_pnl, created_at, id::text
                FROM realized_pnl_events
                WHERE user_address = $1
                ORDER BY created_at DESC LIMIT $2
                "#,
            )
            .bind(&user_addr)
            .bind(limit)
            .fetch_all(&state.db.pool)
            .await
        }
        .unwrap_or_default();

        for (sym, pnl, time, id) in pnl_rows {
            items.push(IncomeItem {
                symbol: sym,
                income_type: "REALIZED_PNL".to_string(),
                income: pnl.to_string(),
                asset: state.config.collateral_symbol().to_string(),
                info: "".to_string(),
                time: time.timestamp_millis(),
                tran_id: id.clone(),
                trade_id: id,
            });
        }
    }

    // 2. Funding fee settlements
    if q.income_type.is_none() || q.income_type.as_deref() == Some("FUNDING_FEE") {
        let fund_rows: Vec<(String, Decimal, DateTime<Utc>, String)> = if let Some(ref sym) = q.symbol {
            let symbol = normalize_symbol(sym);
            sqlx::query_as(
                r#"
                SELECT symbol, funding_fee, settled_at, id::text
                FROM funding_settlements
                WHERE user_address = $1 AND symbol = $2
                ORDER BY settled_at DESC LIMIT $3
                "#,
            )
            .bind(&user_addr)
            .bind(&symbol)
            .bind(limit)
            .fetch_all(&state.db.pool)
            .await
        } else {
            sqlx::query_as(
                r#"
                SELECT symbol, funding_fee, settled_at, id::text
                FROM funding_settlements
                WHERE user_address = $1
                ORDER BY settled_at DESC LIMIT $2
                "#,
            )
            .bind(&user_addr)
            .bind(limit)
            .fetch_all(&state.db.pool)
            .await
        }
        .unwrap_or_default();

        for (sym, amount, time, id) in fund_rows {
            items.push(IncomeItem {
                symbol: sym,
                income_type: "FUNDING_FEE".to_string(),
                income: amount.to_string(),
                asset: state.config.collateral_symbol().to_string(),
                info: "".to_string(),
                time: time.timestamp_millis(),
                tran_id: id.clone(),
                trade_id: id,
            });
        }
    }

    // 3. Commission (trade fees).
    //
    // The previous query was
    //   SELECT … FROM trades WHERE maker_address = $1 OR taker_address = $1
    //   ORDER BY created_at DESC LIMIT $2
    // which the planner cannot satisfy without scanning every trade where
    // either party matches — `OR` across two columns defeats single-column
    // indexes on `(maker_address, created_at)` and `(taker_address,
    // created_at)`. On the prod TimescaleDB hypertable this took >45s
    // (R4 P0 #7).
    //
    // Rewrite as a UNION ALL of the two sides; each leg can use its own
    // index and the outer LIMIT keeps the merged set small. The window
    // is also bounded by `start_time` / `end_time` if the caller passed
    // them, which Binance clients usually do.
    if q.income_type.is_none() || q.income_type.as_deref() == Some("COMMISSION") {
        let start_ms = q.start_time.unwrap_or(0);
        let end_ms = q.end_time.unwrap_or(i64::MAX);
        let start_dt = DateTime::<Utc>::from_timestamp_millis(start_ms)
            .unwrap_or_else(|| DateTime::<Utc>::from_timestamp(0, 0).unwrap());
        let end_dt = DateTime::<Utc>::from_timestamp_millis(end_ms.min(253_402_300_799_000))
            .unwrap_or_else(Utc::now);
        let fee_rows: Vec<(String, Decimal, DateTime<Utc>, String)> = sqlx::query_as(
            r#"
            (SELECT symbol, -maker_fee AS fee, created_at, id::text
             FROM trades
             WHERE maker_address = $1
               AND created_at BETWEEN $3 AND $4
             ORDER BY created_at DESC LIMIT $2)
            UNION ALL
            (SELECT symbol, -taker_fee AS fee, created_at, id::text
             FROM trades
             WHERE taker_address = $1
               AND created_at BETWEEN $3 AND $4
             ORDER BY created_at DESC LIMIT $2)
            ORDER BY created_at DESC LIMIT $2
            "#,
        )
        .bind(&user_addr)
        .bind(limit)
        .bind(start_dt)
        .bind(end_dt)
        .fetch_all(&state.db.pool)
        .await
        .unwrap_or_default();

        for (sym, fee, time, id) in fee_rows {
            items.push(IncomeItem {
                symbol: sym,
                income_type: "COMMISSION".to_string(),
                income: fee.to_string(),
                asset: state.config.collateral_symbol().to_string(),
                info: "".to_string(),
                time: time.timestamp_millis(),
                tran_id: id.clone(),
                trade_id: id,
            });
        }
    }

    // Sort by time descending, then limit
    items.sort_by(|a, b| b.time.cmp(&a.time));
    items.truncate(limit as usize);

    Ok(Json(items))
}

// ─── 5. Funding Fee History ───────────────────────────────────────
// GET /fapi/v1/fundingFeeHistory

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct FundingFeeHistoryQuery {
    pub symbol: Option<String>,
    #[serde(rename = "startTime")]
    pub start_time: Option<i64>,
    #[serde(rename = "endTime")]
    pub end_time: Option<i64>,
    pub limit: Option<i64>,
    pub timestamp: Option<i64>,
}

#[derive(Serialize)]
pub struct FundingFeeHistoryItem {
    pub symbol: String,
    #[serde(rename = "fundingRate")]
    pub funding_rate: String,
    #[serde(rename = "positionSize")]
    pub position_size: String,
    #[serde(rename = "fundingFee")]
    pub funding_fee: String,
    #[serde(rename = "positionSide")]
    pub position_side: String,
    pub asset: String,
    pub time: i64,
    #[serde(rename = "tranId")]
    pub tran_id: String,
}

pub async fn funding_fee_history(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Query(q): Query<FundingFeeHistoryQuery>,
) -> ApiResult<Vec<FundingFeeHistoryItem>> {
    let user_addr = auth_user.address.to_lowercase();
    let limit = q.limit.unwrap_or(100).clamp(1, 1000);
    let start = q.start_time.map(|ms| DateTime::<Utc>::from_timestamp_millis(ms).unwrap_or_default());
    let end = q.end_time.map(|ms| DateTime::<Utc>::from_timestamp_millis(ms).unwrap_or_default());
    let symbol = q.symbol.as_deref().map(normalize_symbol);

    let rows: Vec<(String, Decimal, Decimal, Decimal, bool, DateTime<Utc>, String)> = sqlx::query_as(
        r#"
        SELECT symbol, funding_rate, position_size, funding_fee, is_long, settled_at, id::text
        FROM funding_settlements
        WHERE user_address = $1
          AND ($2::text IS NULL OR symbol = $2)
          AND ($3::timestamptz IS NULL OR settled_at >= $3)
          AND ($4::timestamptz IS NULL OR settled_at <= $4)
        ORDER BY settled_at DESC
        LIMIT $5
        "#,
    )
    .bind(&user_addr)
    .bind(symbol.as_deref())
    .bind(start)
    .bind(end)
    .bind(limit)
    .fetch_all(&state.db.pool)
    .await
    .map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(BinanceError { code: -1000, msg: format!("db error: {}", e) }),
        )
    })?;

    let items: Vec<FundingFeeHistoryItem> = rows
        .into_iter()
        .map(|(sym, rate, size, fee, is_long, time, id)| FundingFeeHistoryItem {
            symbol: sym,
            funding_rate: rate.to_string(),
            position_size: size.to_string(),
            funding_fee: fee.to_string(),
            position_side: if is_long { "LONG".to_string() } else { "SHORT".to_string() },
            asset: state.config.collateral_symbol().to_string(),
            time: time.timestamp_millis(),
            tran_id: id,
        })
        .collect();

    Ok(Json(items))
}
