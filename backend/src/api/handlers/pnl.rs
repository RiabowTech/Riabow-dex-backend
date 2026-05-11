//! PnL (Profit and Loss) API Handlers
//!
//! Provides endpoints for retrieving daily and cumulative PnL statistics for users.
//! Optimized: all independent DB queries run concurrently via tokio::try_join!.

use axum::{
    extract::{Query, State},
    http::StatusCode,
    Extension, Json,
};
use chrono::{Utc, NaiveDate, Duration};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use crate::auth::middleware::AuthUser;
use crate::AppState;

/// Query parameters for PnL endpoint
#[derive(Debug, Deserialize)]
pub struct PnlQuery {
    pub symbol: Option<String>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub days: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct DailyPnl {
    pub date: String,
    pub realized_pnl: Decimal,
    pub volume: Decimal,
    pub trade_count: i64,
    pub fees: Decimal,
}

#[derive(Debug, Serialize)]
pub struct CumulativePnl {
    pub total_realized_pnl: Decimal,
    pub total_unrealized_pnl: Decimal,
    pub total_pnl: Decimal,
    pub total_volume: Decimal,
    pub total_trades: i64,
    pub total_fees: Decimal,
    pub win_rate: Decimal,
    pub avg_profit_per_trade: Decimal,
}

#[derive(Debug, Serialize)]
pub struct PnlResponse {
    pub daily: Vec<DailyPnl>,
    pub cumulative: CumulativePnl,
    pub symbol: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub code: String,
}

type PnlError = (StatusCode, Json<ErrorResponse>);

fn pnl_err(msg: &str, code: &str) -> PnlError {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorResponse {
            error: msg.to_string(),
            code: code.to_string(),
        }),
    )
}

/// GET /account/pnl
///
/// Runs all DB queries concurrently for maximum performance.
pub async fn get_pnl(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Query(query): Query<PnlQuery>,
) -> Result<Json<PnlResponse>, PnlError> {
    let user_address = auth_user.address.to_lowercase();

    // Determine date range
    let (start_date, end_date) = match (query.start_date.as_ref(), query.end_date.as_ref()) {
        (Some(start), Some(end)) => {
            let start = NaiveDate::parse_from_str(start, "%Y-%m-%d").map_err(|_| {
                (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: "无效的开始日期格式，请使用 YYYY-MM-DD".to_string(),
                        code: "INVALID_START_DATE".to_string(),
                    }),
                )
            })?;
            let end = NaiveDate::parse_from_str(end, "%Y-%m-%d").map_err(|_| {
                (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: "无效的结束日期格式，请使用 YYYY-MM-DD".to_string(),
                        code: "INVALID_END_DATE".to_string(),
                    }),
                )
            })?;
            (start, end)
        }
        _ => {
            let days_back = query.days.unwrap_or(30);
            let end = Utc::now().date_naive();
            let start = end - Duration::days(days_back);
            (start, end)
        }
    };

    let start_ts = start_date.and_hms_opt(0, 0, 0).unwrap().and_utc();
    let end_ts = end_date.and_hms_opt(23, 59, 59).unwrap().and_utc();
    let symbol = query.symbol.as_deref();

    // ── Run ALL independent queries concurrently ──────────────────────────
    let (
        daily_trade_stats,
        daily_pnl_rows,
        open_positions,
        cumulative_trade_stats,
        win_rate_data,
    ) = tokio::try_join!(
        // Q1: Daily trade stats (trades table)
        fetch_daily_trade_stats(&state, &user_address, symbol, start_ts, end_ts),
        // Q2: Daily realized PnL (realized_pnl_events table)
        fetch_daily_realized_pnl(&state, &user_address, symbol, start_ts, end_ts),
        // Q3: Open positions for unrealized PnL
        fetch_open_positions(&state, &user_address, symbol),
        // Q4: Cumulative trade stats (trades table)
        fetch_cumulative_trade_stats(&state, &user_address, symbol, start_ts, end_ts),
        // Q5: Win rate + total realized PnL (single query, realized_pnl_events)
        fetch_win_rate_and_realized(&state, &user_address, symbol, start_ts, end_ts),
    )?;

    // ── Build daily records ───────────────────────────────────────────────
    let trade_stats_map: HashMap<NaiveDate, (Decimal, Decimal, i64)> = daily_trade_stats
        .into_iter()
        .map(|(date, fees, volume, count)| (date, (fees, volume, count)))
        .collect();

    let pnl_map: HashMap<NaiveDate, Decimal> = daily_pnl_rows.into_iter().collect();

    let mut daily = Vec::new();
    let mut current_date = start_date;
    while current_date <= end_date {
        let date_str = current_date.format("%Y-%m-%d").to_string();
        let realized_pnl = pnl_map.get(&current_date).copied().unwrap_or(Decimal::ZERO);
        let (fees, volume, trade_count) = trade_stats_map
            .get(&current_date)
            .map(|(f, v, c)| (*f, *v, *c))
            .unwrap_or((Decimal::ZERO, Decimal::ZERO, 0));
        daily.push(DailyPnl {
            date: date_str,
            realized_pnl,
            volume,
            trade_count,
            fees,
        });
        current_date += chrono::Duration::days(1);
    }

    // ── Build cumulative stats ────────────────────────────────────────────
    let (total_realized_pnl, winning_count, total_closed_count) = win_rate_data;
    let (total_volume, total_trades, total_fees) = cumulative_trade_stats;

    // Calculate unrealized PnL from open positions
    let mut total_unrealized_pnl = Decimal::ZERO;
    for (pos_symbol, side_str, size_in_usd, size_in_tokens) in open_positions {
        if let Some(mark_price) = state.price_feed_service.get_mark_price(&pos_symbol).await {
            let position_value = size_in_tokens * mark_price;
            let pnl = if side_str.eq_ignore_ascii_case("long") {
                position_value - size_in_usd
            } else {
                size_in_usd - position_value
            };
            total_unrealized_pnl += pnl;
        }
    }

    let total_pnl = total_realized_pnl + total_unrealized_pnl;
    let win_rate = if total_closed_count > 0 {
        Decimal::from(winning_count) / Decimal::from(total_closed_count) * Decimal::from(100)
    } else {
        Decimal::ZERO
    };
    let avg_profit_per_trade = if total_trades > 0 {
        total_realized_pnl / Decimal::from(total_trades)
    } else {
        Decimal::ZERO
    };

    Ok(Json(PnlResponse {
        daily,
        cumulative: CumulativePnl {
            total_realized_pnl,
            total_unrealized_pnl,
            total_pnl,
            total_volume,
            total_trades,
            total_fees,
            win_rate,
            avg_profit_per_trade,
        },
        symbol: query.symbol,
    }))
}

// ── Individual query functions ─────────────────────────────────────────────

/// Q1: Daily trade statistics from `trades` table
async fn fetch_daily_trade_stats(
    state: &Arc<AppState>,
    user_address: &str,
    symbol: Option<&str>,
    start_ts: chrono::DateTime<Utc>,
    end_ts: chrono::DateTime<Utc>,
) -> Result<Vec<(NaiveDate, Decimal, Decimal, i64)>, PnlError> {
    let sql = if symbol.is_some() {
        r#"SELECT DATE(t.created_at) as trade_date,
                  SUM(CASE WHEN t.maker_address = $1 THEN t.maker_fee ELSE t.taker_fee END),
                  SUM(t.price * t.amount),
                  COUNT(*)
           FROM trades t
           WHERE (t.maker_address = $1 OR t.taker_address = $1)
             AND t.created_at >= $2 AND t.created_at <= $3 AND t.symbol = $4
           GROUP BY DATE(t.created_at) ORDER BY trade_date ASC"#
    } else {
        r#"SELECT DATE(t.created_at) as trade_date,
                  SUM(CASE WHEN t.maker_address = $1 THEN t.maker_fee ELSE t.taker_fee END),
                  SUM(t.price * t.amount),
                  COUNT(*)
           FROM trades t
           WHERE (t.maker_address = $1 OR t.taker_address = $1)
             AND t.created_at >= $2 AND t.created_at <= $3
           GROUP BY DATE(t.created_at) ORDER BY trade_date ASC"#
    };

    let rows = if let Some(sym) = symbol {
        sqlx::query_as(sql)
            .bind(user_address)
            .bind(start_ts)
            .bind(end_ts)
            .bind(sym)
            .fetch_all(&state.db.pool)
            .await
    } else {
        sqlx::query_as(sql)
            .bind(user_address)
            .bind(start_ts)
            .bind(end_ts)
            .fetch_all(&state.db.pool)
            .await
    }
    .map_err(|e| {
        tracing::error!("Failed to fetch daily trade stats: {}", e);
        pnl_err("获取每日交易统计失败", "DAILY_TRADE_STATS_FAILED")
    })?;

    Ok(rows)
}

/// Q2: Daily realized PnL from `realized_pnl_events` table
async fn fetch_daily_realized_pnl(
    state: &Arc<AppState>,
    user_address: &str,
    symbol: Option<&str>,
    start_ts: chrono::DateTime<Utc>,
    end_ts: chrono::DateTime<Utc>,
) -> Result<Vec<(NaiveDate, Decimal)>, PnlError> {
    let sql = if symbol.is_some() {
        r#"SELECT DATE(created_at), SUM(realized_pnl)
           FROM realized_pnl_events
           WHERE user_address = $1 AND symbol = $2
             AND created_at >= $3 AND created_at <= $4
           GROUP BY DATE(created_at)"#
    } else {
        r#"SELECT DATE(created_at), SUM(realized_pnl)
           FROM realized_pnl_events
           WHERE user_address = $1
             AND created_at >= $2 AND created_at <= $3
           GROUP BY DATE(created_at)"#
    };

    let rows = if let Some(sym) = symbol {
        sqlx::query_as(sql)
            .bind(user_address)
            .bind(sym)
            .bind(start_ts)
            .bind(end_ts)
            .fetch_all(&state.db.pool)
            .await
    } else {
        sqlx::query_as(sql)
            .bind(user_address)
            .bind(start_ts)
            .bind(end_ts)
            .fetch_all(&state.db.pool)
            .await
    }
    .map_err(|e| {
        tracing::error!("Failed to fetch daily realized PnL: {}", e);
        pnl_err("获取每日盈亏失败", "DAILY_PNL_FETCH_FAILED")
    })?;

    Ok(rows)
}

/// Q3: Open positions for unrealized PnL calculation
async fn fetch_open_positions(
    state: &Arc<AppState>,
    user_address: &str,
    symbol: Option<&str>,
) -> Result<Vec<(String, String, Decimal, Decimal)>, PnlError> {
    let sql = if symbol.is_some() {
        r#"SELECT symbol, side::text, size_in_usd, size_in_tokens
           FROM positions
           WHERE user_address = $1 AND status = 'open' AND symbol = $2"#
    } else {
        r#"SELECT symbol, side::text, size_in_usd, size_in_tokens
           FROM positions
           WHERE user_address = $1 AND status = 'open'"#
    };

    let rows = if let Some(sym) = symbol {
        sqlx::query_as(sql)
            .bind(user_address)
            .bind(sym)
            .fetch_all(&state.db.pool)
            .await
    } else {
        sqlx::query_as(sql)
            .bind(user_address)
            .fetch_all(&state.db.pool)
            .await
    }
    .unwrap_or_default();

    Ok(rows)
}

/// Q4: Cumulative trade volume/count/fees from `trades` table
async fn fetch_cumulative_trade_stats(
    state: &Arc<AppState>,
    user_address: &str,
    symbol: Option<&str>,
    start_ts: chrono::DateTime<Utc>,
    end_ts: chrono::DateTime<Utc>,
) -> Result<(Decimal, i64, Decimal), PnlError> {
    let sql = if symbol.is_some() {
        r#"SELECT COALESCE(SUM(t.price * t.amount), 0),
                  COUNT(*),
                  COALESCE(SUM(CASE WHEN t.maker_address = $1 THEN t.maker_fee ELSE t.taker_fee END), 0)
           FROM trades t
           WHERE (t.maker_address = $1 OR t.taker_address = $1)
             AND t.symbol = $2
             AND t.created_at >= $3 AND t.created_at <= $4"#
    } else {
        r#"SELECT COALESCE(SUM(t.price * t.amount), 0),
                  COUNT(*),
                  COALESCE(SUM(CASE WHEN t.maker_address = $1 THEN t.maker_fee ELSE t.taker_fee END), 0)
           FROM trades t
           WHERE (t.maker_address = $1 OR t.taker_address = $1)
             AND t.created_at >= $2 AND t.created_at <= $3"#
    };

    let row = if let Some(sym) = symbol {
        sqlx::query_as(sql)
            .bind(user_address)
            .bind(sym)
            .bind(start_ts)
            .bind(end_ts)
            .fetch_one(&state.db.pool)
            .await
    } else {
        sqlx::query_as(sql)
            .bind(user_address)
            .bind(start_ts)
            .bind(end_ts)
            .fetch_one(&state.db.pool)
            .await
    }
    .map_err(|e| {
        tracing::error!("Failed to fetch cumulative trade stats: {}", e);
        pnl_err("获取交易统计失败", "STATS_FETCH_FAILED")
    })?;

    Ok(row)
}

/// Q5: Win rate + total realized PnL in a single query (avoids double-scan)
async fn fetch_win_rate_and_realized(
    state: &Arc<AppState>,
    user_address: &str,
    symbol: Option<&str>,
    start_ts: chrono::DateTime<Utc>,
    end_ts: chrono::DateTime<Utc>,
) -> Result<(Decimal, i64, i64), PnlError> {
    let sql = if symbol.is_some() {
        r#"SELECT COALESCE(SUM(realized_pnl), 0),
                  COUNT(*) FILTER (WHERE realized_pnl > 0),
                  COUNT(*)
           FROM realized_pnl_events
           WHERE user_address = $1 AND symbol = $2
             AND created_at >= $3 AND created_at <= $4"#
    } else {
        r#"SELECT COALESCE(SUM(realized_pnl), 0),
                  COUNT(*) FILTER (WHERE realized_pnl > 0),
                  COUNT(*)
           FROM realized_pnl_events
           WHERE user_address = $1
             AND created_at >= $2 AND created_at <= $3"#
    };

    let row: (Decimal, i64, i64) = if let Some(sym) = symbol {
        sqlx::query_as(sql)
            .bind(user_address)
            .bind(sym)
            .bind(start_ts)
            .bind(end_ts)
            .fetch_one(&state.db.pool)
            .await
    } else {
        sqlx::query_as(sql)
            .bind(user_address)
            .bind(start_ts)
            .bind(end_ts)
            .fetch_one(&state.db.pool)
            .await
    }
    .map_err(|e| {
        tracing::error!("Failed to fetch win rate: {}", e);
        pnl_err("获取胜率失败", "WIN_RATE_FETCH_FAILED")
    })?;

    Ok(row)
}
