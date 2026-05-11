//! Admin Statistics API
//!
//! Internal API for querying trade volume and platform statistics.

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use std::sync::Arc;
use tracing::{error, info};

use crate::AppState;

// ==================== Request/Response Types ====================

#[derive(Debug, Deserialize)]
pub struct TradeVolumeQuery {
    /// Start date (YYYY-MM-DD format)
    pub start_date: Option<String>,
    /// End date (YYYY-MM-DD format)
    pub end_date: Option<String>,
    /// Filter by symbol (optional)
    pub symbol: Option<String>,
    /// Group by: "day", "hour", or "total" (default: "total")
    pub group_by: Option<String>,
}

#[derive(Debug, Serialize, FromRow)]
pub struct TradeVolumeSummary {
    pub trade_count: i64,
    pub total_volume_tokens: Decimal,
    pub total_volume_usd: Decimal,
    pub total_maker_fee: Decimal,
    pub total_taker_fee: Decimal,
    pub total_fees: Decimal,
}

#[derive(Debug, Serialize)]
pub struct TradeVolumeBySymbol {
    pub symbol: String,
    pub trade_count: i64,
    pub volume_tokens: Decimal,
    pub volume_usd: Decimal,
    pub fees: Decimal,
}

#[derive(Debug, Serialize)]
pub struct TradeVolumeByTime {
    pub time_bucket: String,
    pub trade_count: i64,
    pub volume_usd: Decimal,
    pub fees: Decimal,
}

#[derive(Debug, Serialize)]
pub struct TradeVolumeResponse {
    pub success: bool,
    pub start_date: String,
    pub end_date: String,
    pub summary: TradeVolumeSummary,
    pub by_symbol: Vec<TradeVolumeBySymbol>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub by_time: Option<Vec<TradeVolumeByTime>>,
    /// PR 2 (2026-04-29): actual protocol revenue from `protocol_fee_ledger`,
    /// broken down by fee_type. `summary.total_fees` is preserved as the
    /// nominal sum of `trades.maker_fee + trades.taker_fee`, which historically
    /// drove referral commission base; that figure is detached from money
    /// actually moved out of user collateral and should not be used for
    /// revenue / treasury reporting going forward.
    pub actual_protocol_revenue: ProtocolRevenueBreakdown,
}

/// Per-fee-type protocol revenue, sourced from `protocol_fee_ledger`.
///
/// All amounts are in USDT and signed: positive values represent funds
/// debited from user collateral (protocol receives); negative values
/// represent funds the protocol pays out (e.g. negative funding settlement
/// where the user earns funding, or `liquidator_reward` leaving VAULT to
/// the keeper wallet).
#[derive(Debug, Serialize, Default)]
pub struct ProtocolRevenueBreakdown {
    pub trading_fee:            Decimal,
    pub funding_fee:            Decimal,
    pub borrowing_fee:          Decimal,
    pub liquidation_fee:        Decimal,
    pub insurance_contribution: Decimal,
    pub liquidator_reward:      Decimal,
    pub total:                  Decimal,
}

async fn fetch_protocol_revenue(
    pool: &sqlx::PgPool,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<ProtocolRevenueBreakdown, sqlx::Error> {
    let rows: Vec<(String, Decimal)> = sqlx::query_as(
        r#"
        SELECT fee_type, COALESCE(SUM(amount), 0)::numeric AS amount
        FROM   protocol_fee_ledger
        WHERE  created_at >= $1 AND created_at < $2
        GROUP BY fee_type
        "#,
    )
    .bind(start)
    .bind(end)
    .fetch_all(pool)
    .await?;

    let mut out = ProtocolRevenueBreakdown::default();
    for (fee_type, amt) in rows {
        match fee_type.as_str() {
            "trading_fee"            => out.trading_fee            = amt,
            "funding_fee"            => out.funding_fee            = amt,
            "borrowing_fee"          => out.borrowing_fee          = amt,
            "liquidation_fee"        => out.liquidation_fee        = amt,
            "insurance_contribution" => out.insurance_contribution = amt,
            "liquidator_reward"      => out.liquidator_reward      = amt,
            // bootstrap_pre_migration intentionally NOT included in the
            // window total — it represents pre-cutover accumulated revenue
            // and is retrieved via a separate snapshot query when needed.
            _ => continue,
        }
        out.total += amt;
    }
    Ok(out)
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub success: bool,
    pub error: String,
}

// ==================== Handlers ====================

/// GET /api/v1/internal/stats/trade-volume
///
/// Query trade volume statistics within a time range.
///
/// Query parameters:
/// - start_date: Start date in YYYY-MM-DD format (default: today)
/// - end_date: End date in YYYY-MM-DD format (default: today)
/// - symbol: Filter by trading pair (optional)
/// - group_by: "day", "hour", or "total" (default: "total")
pub async fn get_trade_volume(
    State(state): State<Arc<AppState>>,
    Query(query): Query<TradeVolumeQuery>,
) -> impl IntoResponse {
    // Parse dates
    let today = Utc::now().format("%Y-%m-%d").to_string();
    let start_date = query.start_date.unwrap_or_else(|| today.clone());
    let end_date = query.end_date.unwrap_or_else(|| today.clone());

    // Validate date format
    let _start = match NaiveDate::parse_from_str(&start_date, "%Y-%m-%d") {
        Ok(d) => d,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    success: false,
                    error: "Invalid start_date format. Use YYYY-MM-DD".to_string(),
                }),
            ).into_response();
        }
    };

    let _end = match NaiveDate::parse_from_str(&end_date, "%Y-%m-%d") {
        Ok(d) => d,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    success: false,
                    error: "Invalid end_date format. Use YYYY-MM-DD".to_string(),
                }),
            ).into_response();
        }
    };

    info!("Querying trade volume from {} to {}", start_date, end_date);

    // Build the base query with optional symbol filter
    let symbol_filter = query.symbol.as_ref();

    // Query summary from trades + virtual_trades tables combined
    let summary_result = if let Some(sym) = symbol_filter {
        sqlx::query_as::<_, TradeVolumeSummary>(
            r#"
            WITH all_trades AS (
                SELECT amount, price, maker_fee, taker_fee
                FROM trades
                WHERE created_at >= $1::date
                  AND created_at < ($2::date + interval '1 day')
                  AND symbol = $3
                UNION ALL
                SELECT amount::numeric, price::numeric, 0::numeric as maker_fee, 0::numeric as taker_fee
                FROM virtual_trades
                WHERE timestamp >= (extract(epoch from $1::date) * 1000)::bigint
                  AND timestamp < (extract(epoch from ($2::date + interval '1 day')) * 1000)::bigint
                  AND symbol = $3
            )
            SELECT
                COUNT(*)::bigint as trade_count,
                COALESCE(SUM(amount), 0) as total_volume_tokens,
                COALESCE(SUM(amount * price), 0) as total_volume_usd,
                COALESCE(SUM(maker_fee), 0) as total_maker_fee,
                COALESCE(SUM(taker_fee), 0) as total_taker_fee,
                COALESCE(SUM(maker_fee + taker_fee), 0) as total_fees
            FROM all_trades
            "#
        )
        .bind(&start_date)
        .bind(&end_date)
        .bind(sym)
        .fetch_one(&state.db.pool)
        .await
    } else {
        sqlx::query_as::<_, TradeVolumeSummary>(
            r#"
            WITH all_trades AS (
                SELECT amount, price, maker_fee, taker_fee
                FROM trades
                WHERE created_at >= $1::date
                  AND created_at < ($2::date + interval '1 day')
                UNION ALL
                SELECT amount::numeric, price::numeric, 0::numeric as maker_fee, 0::numeric as taker_fee
                FROM virtual_trades
                WHERE timestamp >= (extract(epoch from $1::date) * 1000)::bigint
                  AND timestamp < (extract(epoch from ($2::date + interval '1 day')) * 1000)::bigint
            )
            SELECT
                COUNT(*)::bigint as trade_count,
                COALESCE(SUM(amount), 0) as total_volume_tokens,
                COALESCE(SUM(amount * price), 0) as total_volume_usd,
                COALESCE(SUM(maker_fee), 0) as total_maker_fee,
                COALESCE(SUM(taker_fee), 0) as total_taker_fee,
                COALESCE(SUM(maker_fee + taker_fee), 0) as total_fees
            FROM all_trades
            "#
        )
        .bind(&start_date)
        .bind(&end_date)
        .fetch_one(&state.db.pool)
        .await
    };

    let summary = match summary_result {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to query trade volume summary: {:?}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    success: false,
                    error: format!("Database error: {}", e),
                }),
            ).into_response();
        }
    };

    // Query by symbol from trades + virtual_trades tables
    let by_symbol_result: Result<Vec<(String, i64, Decimal, Decimal, Decimal)>, _> = if let Some(sym) = symbol_filter {
        sqlx::query_as(
            r#"
            WITH all_trades AS (
                SELECT symbol, amount, price, maker_fee + taker_fee as fees
                FROM trades
                WHERE created_at >= $1::date
                  AND created_at < ($2::date + interval '1 day')
                  AND symbol = $3
                UNION ALL
                SELECT symbol, amount::numeric, price::numeric, 0::numeric as fees
                FROM virtual_trades
                WHERE timestamp >= (extract(epoch from $1::date) * 1000)::bigint
                  AND timestamp < (extract(epoch from ($2::date + interval '1 day')) * 1000)::bigint
                  AND symbol = $3
            )
            SELECT
                symbol,
                COUNT(*)::bigint as trade_count,
                COALESCE(SUM(amount), 0) as volume_tokens,
                COALESCE(SUM(amount * price), 0) as volume_usd,
                COALESCE(SUM(fees), 0) as fees
            FROM all_trades
            GROUP BY symbol
            ORDER BY volume_usd DESC
            "#
        )
        .bind(&start_date)
        .bind(&end_date)
        .bind(sym)
        .fetch_all(&state.db.pool)
        .await
    } else {
        sqlx::query_as(
            r#"
            WITH all_trades AS (
                SELECT symbol, amount, price, maker_fee + taker_fee as fees
                FROM trades
                WHERE created_at >= $1::date
                  AND created_at < ($2::date + interval '1 day')
                UNION ALL
                SELECT symbol, amount::numeric, price::numeric, 0::numeric as fees
                FROM virtual_trades
                WHERE timestamp >= (extract(epoch from $1::date) * 1000)::bigint
                  AND timestamp < (extract(epoch from ($2::date + interval '1 day')) * 1000)::bigint
            )
            SELECT
                symbol,
                COUNT(*)::bigint as trade_count,
                COALESCE(SUM(amount), 0) as volume_tokens,
                COALESCE(SUM(amount * price), 0) as volume_usd,
                COALESCE(SUM(fees), 0) as fees
            FROM all_trades
            GROUP BY symbol
            ORDER BY volume_usd DESC
            "#
        )
        .bind(&start_date)
        .bind(&end_date)
        .fetch_all(&state.db.pool)
        .await
    };

    let by_symbol = match by_symbol_result {
        Ok(rows) => rows.into_iter().map(|(symbol, trade_count, volume_tokens, volume_usd, fees)| {
            TradeVolumeBySymbol {
                symbol,
                trade_count,
                volume_tokens,
                volume_usd,
                fees,
            }
        }).collect(),
        Err(e) => {
            error!("Failed to query trade volume by symbol: {:?}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    success: false,
                    error: format!("Database error: {}", e),
                }),
            ).into_response();
        }
    };

    // Query by time if requested
    let by_time = match query.group_by.as_deref() {
        Some("day") => {
            let result: Result<Vec<(NaiveDate, i64, Decimal, Decimal)>, _> = if let Some(sym) = symbol_filter {
                sqlx::query_as(
                    r#"
                    WITH all_trades AS (
                        SELECT created_at::date as trade_date, amount, price, maker_fee + taker_fee as fees
                        FROM trades
                        WHERE created_at >= $1::date AND created_at < ($2::date + interval '1 day') AND symbol = $3
                        UNION ALL
                        SELECT (to_timestamp(timestamp / 1000.0))::date as trade_date, amount::numeric, price::numeric, 0::numeric
                        FROM virtual_trades
                        WHERE timestamp >= (extract(epoch from $1::date) * 1000)::bigint
                          AND timestamp < (extract(epoch from ($2::date + interval '1 day')) * 1000)::bigint AND symbol = $3
                    )
                    SELECT trade_date as time_bucket, COUNT(*)::bigint, COALESCE(SUM(amount * price), 0), COALESCE(SUM(fees), 0)
                    FROM all_trades GROUP BY trade_date ORDER BY trade_date
                    "#
                )
                .bind(&start_date).bind(&end_date).bind(sym)
                .fetch_all(&state.db.pool).await
            } else {
                sqlx::query_as(
                    r#"
                    WITH all_trades AS (
                        SELECT created_at::date as trade_date, amount, price, maker_fee + taker_fee as fees
                        FROM trades
                        WHERE created_at >= $1::date AND created_at < ($2::date + interval '1 day')
                        UNION ALL
                        SELECT (to_timestamp(timestamp / 1000.0))::date as trade_date, amount::numeric, price::numeric, 0::numeric
                        FROM virtual_trades
                        WHERE timestamp >= (extract(epoch from $1::date) * 1000)::bigint
                          AND timestamp < (extract(epoch from ($2::date + interval '1 day')) * 1000)::bigint
                    )
                    SELECT trade_date as time_bucket, COUNT(*)::bigint, COALESCE(SUM(amount * price), 0), COALESCE(SUM(fees), 0)
                    FROM all_trades GROUP BY trade_date ORDER BY trade_date
                    "#
                )
                .bind(&start_date).bind(&end_date)
                .fetch_all(&state.db.pool).await
            };

            match result {
                Ok(rows) => Some(rows.into_iter().map(|(date, trade_count, volume_usd, fees)| {
                    TradeVolumeByTime {
                        time_bucket: date.to_string(),
                        trade_count,
                        volume_usd,
                        fees,
                    }
                }).collect()),
                Err(e) => {
                    error!("Failed to query trade volume by day: {:?}", e);
                    None
                }
            }
        },
        Some("hour") => {
            let result: Result<Vec<(DateTime<Utc>, i64, Decimal, Decimal)>, _> = if let Some(sym) = symbol_filter {
                sqlx::query_as(
                    r#"
                    WITH all_trades AS (
                        SELECT date_trunc('hour', created_at) as trade_hour, amount, price, maker_fee + taker_fee as fees
                        FROM trades
                        WHERE created_at >= $1::date AND created_at < ($2::date + interval '1 day') AND symbol = $3
                        UNION ALL
                        SELECT date_trunc('hour', to_timestamp(timestamp / 1000.0)) as trade_hour, amount::numeric, price::numeric, 0::numeric
                        FROM virtual_trades
                        WHERE timestamp >= (extract(epoch from $1::date) * 1000)::bigint
                          AND timestamp < (extract(epoch from ($2::date + interval '1 day')) * 1000)::bigint AND symbol = $3
                    )
                    SELECT trade_hour as time_bucket, COUNT(*)::bigint, COALESCE(SUM(amount * price), 0), COALESCE(SUM(fees), 0)
                    FROM all_trades GROUP BY trade_hour ORDER BY trade_hour
                    "#
                )
                .bind(&start_date).bind(&end_date).bind(sym)
                .fetch_all(&state.db.pool).await
            } else {
                sqlx::query_as(
                    r#"
                    WITH all_trades AS (
                        SELECT date_trunc('hour', created_at) as trade_hour, amount, price, maker_fee + taker_fee as fees
                        FROM trades
                        WHERE created_at >= $1::date AND created_at < ($2::date + interval '1 day')
                        UNION ALL
                        SELECT date_trunc('hour', to_timestamp(timestamp / 1000.0)) as trade_hour, amount::numeric, price::numeric, 0::numeric
                        FROM virtual_trades
                        WHERE timestamp >= (extract(epoch from $1::date) * 1000)::bigint
                          AND timestamp < (extract(epoch from ($2::date + interval '1 day')) * 1000)::bigint
                    )
                    SELECT trade_hour as time_bucket, COUNT(*)::bigint, COALESCE(SUM(amount * price), 0), COALESCE(SUM(fees), 0)
                    FROM all_trades GROUP BY trade_hour ORDER BY trade_hour
                    "#
                )
                .bind(&start_date).bind(&end_date)
                .fetch_all(&state.db.pool).await
            };

            match result {
                Ok(rows) => Some(rows.into_iter().map(|(dt, trade_count, volume_usd, fees)| {
                    TradeVolumeByTime {
                        time_bucket: dt.format("%Y-%m-%d %H:00").to_string(),
                        trade_count,
                        volume_usd,
                        fees,
                    }
                }).collect()),
                Err(e) => {
                    error!("Failed to query trade volume by hour: {:?}", e);
                    None
                }
            }
        },
        _ => None,
    };

    // PR 2 (2026-04-29): protocol_fee_ledger window. The window is parsed
    // from the same start_date / end_date strings, treated as UTC midnight
    // boundaries, with end exclusive (consistent with how the existing
    // SQL queries above use the dates).
    let revenue_window: Option<(DateTime<Utc>, DateTime<Utc>)> = (|| {
        let s = NaiveDate::parse_from_str(&start_date, "%Y-%m-%d").ok()?;
        let e = NaiveDate::parse_from_str(&end_date, "%Y-%m-%d").ok()?;
        let start_ts = s.and_hms_opt(0, 0, 0)?.and_utc();
        // end_date is inclusive in the existing queries' BETWEEN semantics;
        // convert to exclusive 24h boundary for the < end ledger query.
        let end_ts = (e + chrono::Duration::days(1)).and_hms_opt(0, 0, 0)?.and_utc();
        Some((start_ts, end_ts))
    })();

    let actual_protocol_revenue = match revenue_window {
        Some((s, e)) => fetch_protocol_revenue(&state.db.pool, s, e).await.unwrap_or_else(|err| {
            error!("Failed to query protocol_fee_ledger: {:?}", err);
            ProtocolRevenueBreakdown::default()
        }),
        None => ProtocolRevenueBreakdown::default(),
    };

    (
        StatusCode::OK,
        Json(TradeVolumeResponse {
            success: true,
            start_date,
            end_date,
            summary,
            by_symbol,
            by_time,
            actual_protocol_revenue,
        }),
    ).into_response()
}
