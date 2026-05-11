//! Account Stats API Handlers
//!
//! Aggregate endpoints that power the account-dashboard summary widgets.
//!
//! - `GET /account/stats` — lifetime-to-date aggregates (single object)
//! - `GET /account/performance-summary` — six fixed time-bucket rows
//!   (today / yesterday / week / month / year / all)
//!
//! Spec: `dev-docs/docs/account/stats.md`
//!        `dev-docs/docs/account/performance-summary.md`
//!
//! All aggregations are computed directly from `trades`, `realized_pnl_events`,
//! and `positions`. No materialised summary table is used. Independent queries
//! are run concurrently via `tokio::try_join!`.

use axum::{
    extract::{Query, State},
    http::StatusCode,
    Extension, Json,
};
use chrono::{DateTime, Datelike, Duration, TimeZone, Utc};
use rust_decimal::{Decimal, RoundingStrategy};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::auth::middleware::AuthUser;
use crate::AppState;

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub code: String,
}

type StatsError = (StatusCode, Json<ErrorResponse>);

fn stats_err(msg: &str, code: &str) -> StatsError {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorResponse {
            error: msg.to_string(),
            code: code.to_string(),
        }),
    )
}

#[derive(Debug, Deserialize)]
pub struct AccountStatsQuery {
    pub symbol: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct PerformanceSummaryQuery {
    pub symbol: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct AccountStatsResponse {
    pub address: String,
    pub symbol: Option<String>,
    pub closed_count: i64,
    pub wins: i64,
    pub losses: i64,
    pub realized_pnl: Decimal,
    pub realized_fees: Decimal,
    pub volume: Decimal,
    pub trade_count: i64,
    pub net_capital: Decimal,
    pub max_capital: Option<Decimal>,
}

pub async fn get_account_stats(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Query(query): Query<AccountStatsQuery>,
) -> Result<Json<AccountStatsResponse>, StatsError> {
    let user_address = auth_user.address.to_lowercase();
    let symbol = query.symbol.as_deref();

    let (
        (volume, trade_count, realized_fees),
        (realized_pnl, wins, closed_count),
        net_capital,
    ) = tokio::try_join!(
        fetch_lifetime_trade_stats(&state, &user_address, symbol),
        fetch_lifetime_realized_pnl(&state, &user_address, symbol),
        fetch_net_capital(&state, &user_address, symbol),
    )?;

    let losses = closed_count - wins;

    Ok(Json(AccountStatsResponse {
        address: user_address,
        symbol: query.symbol,
        closed_count,
        wins,
        losses,
        realized_pnl,
        realized_fees,
        volume,
        trade_count,
        net_capital,
        // v1: max_capital is intentionally not computed; see stats.md.
        max_capital: None,
    }))
}

#[derive(Debug, Serialize)]
pub struct PerformanceBucket {
    pub bucket: String,
    pub start_ts: i64,
    pub end_ts: i64,
    pub volume: Decimal,
    pub trade_count: i64,
    pub realized_pnl: Decimal,
    pub unrealized_pnl: Decimal,
    pub start_unrealized_pnl: Decimal,
    pub pnl: Decimal,
    pub wins: i64,
    pub losses: i64,
    pub win_rate: Decimal,
    pub used_capital: Decimal,
    pub pnl_bps: i64,
}

#[derive(Debug, Serialize)]
pub struct PerformanceSummaryResponse {
    pub symbol: Option<String>,
    pub buckets: Vec<PerformanceBucket>,
}

pub async fn get_performance_summary(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Query(query): Query<PerformanceSummaryQuery>,
) -> Result<Json<PerformanceSummaryResponse>, StatsError> {
    let user_address = auth_user.address.to_lowercase();
    let symbol = query.symbol.as_deref();
    let now = Utc::now();
    let bounds = compute_bucket_bounds(now);

    // Unrealized PnL and current open collateral are independent of the
    // bucket window — compute them once.
    let (unrealized_pnl, open_collateral) = tokio::try_join!(
        compute_unrealized_pnl(&state, &user_address, symbol),
        fetch_open_collateral(&state, &user_address, symbol),
    )?;

    // For each bucket, fetch trade stats and realized PnL in parallel.
    // Six buckets × 2 queries = 12 round-trips, all fired concurrently.
    let mut trade_futs = Vec::with_capacity(bounds.len());
    let mut pnl_futs = Vec::with_capacity(bounds.len());
    for b in &bounds {
        trade_futs.push(tokio::spawn({
            let state = state.clone();
            let addr = user_address.clone();
            let sym = symbol.map(|s| s.to_string());
            let start = b.start;
            let end = b.end;
            async move {
                fetch_bucket_trade_stats(&state, &addr, sym.as_deref(), start, end).await
            }
        }));
        pnl_futs.push(tokio::spawn({
            let state = state.clone();
            let addr = user_address.clone();
            let sym = symbol.map(|s| s.to_string());
            let start = b.start;
            let end = b.end;
            async move {
                fetch_bucket_realized_pnl(&state, &addr, sym.as_deref(), start, end).await
            }
        }));
    }

    // Resolve the futures in order.
    let mut buckets = Vec::with_capacity(bounds.len());
    for ((b, trade_handle), pnl_handle) in bounds.iter().zip(trade_futs).zip(pnl_futs) {
        let trade_res = trade_handle.await.map_err(|e| {
            tracing::error!("performance_summary: join error: {}", e);
            stats_err("Failed to compute performance summary", "PERFORMANCE_SUMMARY_FAILED")
        })??;
        let pnl_res = pnl_handle.await.map_err(|e| {
            tracing::error!("performance_summary: join error: {}", e);
            stats_err("Failed to compute performance summary", "PERFORMANCE_SUMMARY_FAILED")
        })??;

        // fees aggregate is fetched for parity with /account/stats but not yet exposed
        // per-bucket in the response — see PerformanceBucket struct.
        let (volume, trade_count, _fees) = trade_res;
        let (realized_pnl, wins, closed_count) = pnl_res;
        let losses = closed_count - wins;
        let win_rate = if wins + losses > 0 {
            Decimal::from(wins) / Decimal::from(wins + losses) * Decimal::from(100)
        } else {
            Decimal::ZERO
        };

        // v1: start_unrealized_pnl is current unrealized for buckets other
        // than `all`, where it's defined as 0. This is the explicit v1
        // approximation documented in performance-summary.md.
        let start_unrealized_pnl = if b.label == "all" {
            Decimal::ZERO
        } else {
            unrealized_pnl
        };

        let pnl = realized_pnl + unrealized_pnl - start_unrealized_pnl;
        let uc = used_capital(open_collateral, realized_pnl, start_unrealized_pnl);
        let bps = pnl_bps(pnl, uc);

        buckets.push(PerformanceBucket {
            bucket: b.label.to_string(),
            start_ts: b.start.timestamp(),
            end_ts: b.end.timestamp(),
            volume,
            trade_count,
            realized_pnl,
            unrealized_pnl,
            start_unrealized_pnl,
            pnl,
            wins,
            losses,
            win_rate,
            used_capital: uc,
            pnl_bps: bps,
        });
    }

    Ok(Json(PerformanceSummaryResponse {
        symbol: query.symbol,
        buckets,
    }))
}

/// Inclusive-start, exclusive-end window for one performance-summary bucket.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BucketBound {
    pub label: &'static str,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

/// Returns the six fixed buckets in spec order: today, yesterday, week,
/// month, year, all. All boundaries are UTC. The `all` lower bound is the
/// unix epoch; every other bucket end equals `now`.
pub(crate) fn compute_bucket_bounds(now: DateTime<Utc>) -> Vec<BucketBound> {
    let today_start = Utc
        .with_ymd_and_hms(now.year(), now.month(), now.day(), 0, 0, 0)
        .single()
        .expect("midnight UTC always resolves");
    let yesterday_start = today_start - Duration::days(1);
    let week_start = today_start - Duration::days(7);
    let month_start = today_start - Duration::days(30);
    let year_start = Utc
        .with_ymd_and_hms(now.year(), 1, 1, 0, 0, 0)
        .single()
        .expect("Jan 1 UTC always resolves");
    let epoch = Utc.timestamp_opt(0, 0).single().expect("epoch is valid");

    vec![
        BucketBound { label: "today",     start: today_start,     end: now },
        BucketBound { label: "yesterday", start: yesterday_start, end: today_start },
        BucketBound { label: "week",      start: week_start,      end: now },
        BucketBound { label: "month",     start: month_start,     end: now },
        BucketBound { label: "year",      start: year_start,      end: now },
        BucketBound { label: "all",       start: epoch,           end: now },
    ]
}

/// v1 capital-used formula from
/// `dev-docs/docs/account/performance-summary.md#used_capital-semantics`.
///
/// `used_capital = max(open_collateral - realized_pnl + start_unrealized_pnl, 0)`
///
/// Point-in-time at the bucket end; not a peak-over-time figure.
pub(crate) fn used_capital(
    open_collateral: Decimal,
    realized_pnl: Decimal,
    start_unrealized_pnl: Decimal,
) -> Decimal {
    let raw = open_collateral - realized_pnl + start_unrealized_pnl;
    if raw < Decimal::ZERO {
        Decimal::ZERO
    } else {
        raw
    }
}

/// Returns `pnl / used_capital * 10_000` as an integer (basis points),
/// rounded half-up. Returns `0` when `used_capital` is zero (avoids div/0).
pub(crate) fn pnl_bps(pnl: Decimal, used_capital: Decimal) -> i64 {
    if used_capital == Decimal::ZERO {
        return 0;
    }
    let bps = pnl / used_capital * Decimal::from(10_000);
    // Decimal::round defaults to bankers' rounding; force HalfUp for stability.
    let rounded = bps.round_dp_with_strategy(0, RoundingStrategy::MidpointAwayFromZero);
    rounded.try_into().unwrap_or(0_i64)
}

// ─── Account stats (lifetime) — DB query helpers ────────────────────────────

/// Returns `(total_volume_usd, trade_count, total_user_fees)` over all time
/// for the given user (and optional symbol filter). The user's fee portion is
/// `maker_fee` when they were maker, `taker_fee` when they were taker.
async fn fetch_lifetime_trade_stats(
    state: &Arc<AppState>,
    user_address: &str,
    symbol: Option<&str>,
) -> Result<(Decimal, i64, Decimal), StatsError> {
    let sql = if symbol.is_some() {
        r#"SELECT
                COALESCE(SUM(t.price * t.amount), 0)::numeric,
                COUNT(*),
                COALESCE(SUM(CASE WHEN t.maker_address = $1 THEN t.maker_fee ELSE t.taker_fee END), 0)::numeric
           FROM trades t
           WHERE (t.maker_address = $1 OR t.taker_address = $1)
             AND t.symbol = $2"#
    } else {
        r#"SELECT
                COALESCE(SUM(t.price * t.amount), 0)::numeric,
                COUNT(*),
                COALESCE(SUM(CASE WHEN t.maker_address = $1 THEN t.maker_fee ELSE t.taker_fee END), 0)::numeric
           FROM trades t
           WHERE (t.maker_address = $1 OR t.taker_address = $1)"#
    };

    let row: (Decimal, i64, Decimal) = if let Some(sym) = symbol {
        sqlx::query_as(sql)
            .bind(user_address)
            .bind(sym)
            .fetch_one(&state.db.pool)
            .await
    } else {
        sqlx::query_as(sql)
            .bind(user_address)
            .fetch_one(&state.db.pool)
            .await
    }
    .map_err(|e| {
        tracing::error!("account_stats: lifetime trade stats failed: {}", e);
        stats_err("Failed to compute account stats", "ACCOUNT_STATS_FAILED")
    })?;

    Ok(row)
}

/// Returns `(total_realized_pnl, wins, closed_count)` over all time.
/// `losses = closed_count - wins` (matches the existing `pnl.rs` convention
/// where `realized_pnl == 0` counts as a loss).
async fn fetch_lifetime_realized_pnl(
    state: &Arc<AppState>,
    user_address: &str,
    symbol: Option<&str>,
) -> Result<(Decimal, i64, i64), StatsError> {
    let sql = if symbol.is_some() {
        r#"SELECT
                COALESCE(SUM(realized_pnl), 0)::numeric,
                COUNT(*) FILTER (WHERE realized_pnl > 0),
                COUNT(*)
           FROM realized_pnl_events
           WHERE user_address = $1 AND symbol = $2"#
    } else {
        r#"SELECT
                COALESCE(SUM(realized_pnl), 0)::numeric,
                COUNT(*) FILTER (WHERE realized_pnl > 0),
                COUNT(*)
           FROM realized_pnl_events
           WHERE user_address = $1"#
    };

    let row: (Decimal, i64, i64) = if let Some(sym) = symbol {
        sqlx::query_as(sql)
            .bind(user_address)
            .bind(sym)
            .fetch_one(&state.db.pool)
            .await
    } else {
        sqlx::query_as(sql)
            .bind(user_address)
            .fetch_one(&state.db.pool)
            .await
    }
    .map_err(|e| {
        tracing::error!("account_stats: lifetime realized pnl failed: {}", e);
        stats_err("Failed to compute account stats", "ACCOUNT_STATS_FAILED")
    })?;

    Ok(row)
}

/// Sum of `size_in_usd` over the user's currently open positions.
async fn fetch_net_capital(
    state: &Arc<AppState>,
    user_address: &str,
    symbol: Option<&str>,
) -> Result<Decimal, StatsError> {
    let sql = if symbol.is_some() {
        r#"SELECT COALESCE(SUM(size_in_usd), 0)::numeric
           FROM positions
           WHERE user_address = $1 AND status = 'open' AND symbol = $2"#
    } else {
        r#"SELECT COALESCE(SUM(size_in_usd), 0)::numeric
           FROM positions
           WHERE user_address = $1 AND status = 'open'"#
    };

    let (net,): (Decimal,) = if let Some(sym) = symbol {
        sqlx::query_as(sql)
            .bind(user_address)
            .bind(sym)
            .fetch_one(&state.db.pool)
            .await
    } else {
        sqlx::query_as(sql)
            .bind(user_address)
            .fetch_one(&state.db.pool)
            .await
    }
    .map_err(|e| {
        tracing::error!("account_stats: net capital failed: {}", e);
        stats_err("Failed to compute account stats", "ACCOUNT_STATS_FAILED")
    })?;

    Ok(net)
}

// ─── Performance summary (per-bucket) — DB query helpers ────────────────────

/// Returns `(volume, trade_count, fees)` for the user over a half-open
/// `[start, end)` window.
async fn fetch_bucket_trade_stats(
    state: &Arc<AppState>,
    user_address: &str,
    symbol: Option<&str>,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<(Decimal, i64, Decimal), StatsError> {
    let sql = if symbol.is_some() {
        r#"SELECT
                COALESCE(SUM(t.price * t.amount), 0)::numeric,
                COUNT(*),
                COALESCE(SUM(CASE WHEN t.maker_address = $1 THEN t.maker_fee ELSE t.taker_fee END), 0)::numeric
           FROM trades t
           WHERE (t.maker_address = $1 OR t.taker_address = $1)
             AND t.created_at >= $2 AND t.created_at < $3
             AND t.symbol = $4"#
    } else {
        r#"SELECT
                COALESCE(SUM(t.price * t.amount), 0)::numeric,
                COUNT(*),
                COALESCE(SUM(CASE WHEN t.maker_address = $1 THEN t.maker_fee ELSE t.taker_fee END), 0)::numeric
           FROM trades t
           WHERE (t.maker_address = $1 OR t.taker_address = $1)
             AND t.created_at >= $2 AND t.created_at < $3"#
    };

    let row: (Decimal, i64, Decimal) = if let Some(sym) = symbol {
        sqlx::query_as(sql)
            .bind(user_address)
            .bind(start)
            .bind(end)
            .bind(sym)
            .fetch_one(&state.db.pool)
            .await
    } else {
        sqlx::query_as(sql)
            .bind(user_address)
            .bind(start)
            .bind(end)
            .fetch_one(&state.db.pool)
            .await
    }
    .map_err(|e| {
        tracing::error!("performance_summary: bucket trade stats failed: {}", e);
        stats_err("Failed to compute performance summary", "PERFORMANCE_SUMMARY_FAILED")
    })?;

    Ok(row)
}

/// Returns `(realized_pnl, wins, closed_count)` over a half-open
/// `[start, end)` window.
async fn fetch_bucket_realized_pnl(
    state: &Arc<AppState>,
    user_address: &str,
    symbol: Option<&str>,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<(Decimal, i64, i64), StatsError> {
    let sql = if symbol.is_some() {
        r#"SELECT
                COALESCE(SUM(realized_pnl), 0)::numeric,
                COUNT(*) FILTER (WHERE realized_pnl > 0),
                COUNT(*)
           FROM realized_pnl_events
           WHERE user_address = $1 AND symbol = $2
             AND created_at >= $3 AND created_at < $4"#
    } else {
        r#"SELECT
                COALESCE(SUM(realized_pnl), 0)::numeric,
                COUNT(*) FILTER (WHERE realized_pnl > 0),
                COUNT(*)
           FROM realized_pnl_events
           WHERE user_address = $1
             AND created_at >= $2 AND created_at < $3"#
    };

    let row: (Decimal, i64, i64) = if let Some(sym) = symbol {
        sqlx::query_as(sql)
            .bind(user_address)
            .bind(sym)
            .bind(start)
            .bind(end)
            .fetch_one(&state.db.pool)
            .await
    } else {
        sqlx::query_as(sql)
            .bind(user_address)
            .bind(start)
            .bind(end)
            .fetch_one(&state.db.pool)
            .await
    }
    .map_err(|e| {
        tracing::error!("performance_summary: bucket realized pnl failed: {}", e);
        stats_err("Failed to compute performance summary", "PERFORMANCE_SUMMARY_FAILED")
    })?;

    Ok(row)
}

/// Sums unrealized PnL across the user's currently open positions, valued at
/// the latest mark price for each symbol. Mirrors `pnl.rs::get_pnl`'s
/// unrealized-PnL path, including the long-vs-short convention.
async fn compute_unrealized_pnl(
    state: &Arc<AppState>,
    user_address: &str,
    symbol: Option<&str>,
) -> Result<Decimal, StatsError> {
    let sql = if symbol.is_some() {
        r#"SELECT symbol, side::text, size_in_usd, size_in_tokens
           FROM positions
           WHERE user_address = $1 AND status = 'open' AND symbol = $2"#
    } else {
        r#"SELECT symbol, side::text, size_in_usd, size_in_tokens
           FROM positions
           WHERE user_address = $1 AND status = 'open'"#
    };

    let rows: Vec<(String, String, Decimal, Decimal)> = if let Some(sym) = symbol {
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
    .unwrap_or_else(|e| {
        tracing::error!("performance_summary: open positions query failed (treating as no positions): {}", e);
        Vec::new()
    });

    let mut total = Decimal::ZERO;
    for (pos_symbol, side_str, size_in_usd, size_in_tokens) in rows {
        if let Some(mark) = state.price_feed_service.get_mark_price(&pos_symbol).await {
            let value = size_in_tokens * mark;
            let pnl = if side_str.eq_ignore_ascii_case("long") {
                value - size_in_usd
            } else {
                size_in_usd - value
            };
            total += pnl;
        }
    }

    Ok(total)
}

/// Sum of `collateral_amount` over the user's currently open positions.
/// v1 input to `used_capital`. NB: this is point-in-time at request-time, not
/// at bucket end — that approximation is documented in performance-summary.md.
async fn fetch_open_collateral(
    state: &Arc<AppState>,
    user_address: &str,
    symbol: Option<&str>,
) -> Result<Decimal, StatsError> {
    let sql = if symbol.is_some() {
        r#"SELECT COALESCE(SUM(collateral_amount), 0)::numeric
           FROM positions
           WHERE user_address = $1 AND status = 'open' AND symbol = $2"#
    } else {
        r#"SELECT COALESCE(SUM(collateral_amount), 0)::numeric
           FROM positions
           WHERE user_address = $1 AND status = 'open'"#
    };

    let (c,): (Decimal,) = if let Some(sym) = symbol {
        sqlx::query_as(sql)
            .bind(user_address)
            .bind(sym)
            .fetch_one(&state.db.pool)
            .await
    } else {
        sqlx::query_as(sql)
            .bind(user_address)
            .fetch_one(&state.db.pool)
            .await
    }
    .map_err(|e| {
        tracing::error!("performance_summary: open collateral failed: {}", e);
        stats_err("Failed to compute performance summary", "PERFORMANCE_SUMMARY_FAILED")
    })?;

    Ok(c)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use rust_decimal_macros::dec;

    #[test]
    fn bucket_bounds_anchors_on_utc_midnight() {
        // 2026-05-11 14:33:00 UTC
        let now = Utc.with_ymd_and_hms(2026, 5, 11, 14, 33, 0).unwrap();
        let buckets = compute_bucket_bounds(now);
        assert_eq!(buckets.len(), 6);

        // Bucket order is fixed.
        let labels: Vec<&str> = buckets.iter().map(|b| b.label).collect();
        assert_eq!(labels, ["today", "yesterday", "week", "month", "year", "all"]);

        let today_start = Utc.with_ymd_and_hms(2026, 5, 11, 0, 0, 0).unwrap();
        assert_eq!(buckets[0].start, today_start);
        assert_eq!(buckets[0].end, now);

        let yesterday_start = today_start - Duration::days(1);
        assert_eq!(buckets[1].start, yesterday_start);
        assert_eq!(buckets[1].end, today_start);

        assert_eq!(buckets[2].start, today_start - Duration::days(7));
        assert_eq!(buckets[3].start, today_start - Duration::days(30));

        let year_start = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        assert_eq!(buckets[4].start, year_start);

        // `all` lower bound is the unix epoch.
        assert_eq!(buckets[5].start.timestamp(), 0);
        assert_eq!(buckets[5].end, now);
    }

    #[test]
    fn used_capital_clamps_to_zero() {
        // Open collateral 100, realized 200 (made money), start unrealized 0
        // → 100 - 200 + 0 = -100 → clamps to 0.
        assert_eq!(used_capital(dec!(100), dec!(200), dec!(0)), dec!(0));
    }

    #[test]
    fn used_capital_basic_math() {
        // 850 collateral - 30 realized + 20 start_unrealized = 840.
        assert_eq!(used_capital(dec!(850), dec!(30), dec!(20)), dec!(840));
    }

    #[test]
    fn used_capital_zero_collateral_and_loss() {
        // Closed everything, lost 50, no carry-in → 0 - (-50) + 0 = 50.
        assert_eq!(used_capital(dec!(0), dec!(-50), dec!(0)), dec!(50));
    }

    #[test]
    fn pnl_bps_zero_when_capital_zero() {
        assert_eq!(pnl_bps(dec!(50), dec!(0)), 0);
    }

    #[test]
    fn pnl_bps_rounds_to_integer() {
        // 37.65 / 850 * 10000 = 442.94...  → 443 with HalfUp.
        assert_eq!(pnl_bps(dec!(37.65), dec!(850)), 443);
    }

    #[test]
    fn pnl_bps_handles_negative_pnl() {
        // -100 / 1000 * 10000 = -1000.
        assert_eq!(pnl_bps(dec!(-100), dec!(1000)), -1000);
    }

    #[test]
    fn pnl_bps_rounds_half_up_not_bankers() {
        // 0.05 / 1000 * 10000 = 0.5 exactly.
        // MidpointAwayFromZero (HalfUp) → 1.
        // MidpointNearestEven (bankers') → 0.
        // Pinning the strategy is a regression guard.
        assert_eq!(pnl_bps(dec!(0.05), dec!(1000)), 1);
    }
}
