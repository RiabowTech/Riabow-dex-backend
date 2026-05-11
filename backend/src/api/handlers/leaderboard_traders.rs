//! Trader Leaderboard API Handler
//!
//! 公开排行榜接口。聚合数据来自三个 TimescaleDB Continuous Aggregates
//! (`leaderboard_maker_hourly` / `leaderboard_taker_hourly` /
//! `leaderboard_pnl_hourly`),需先跑 `scripts/leaderboard_caggs.sql` 建表。
//!
//! 设计文档: docs/superpowers/specs/2026-04-28-trader-leaderboard-design.md

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::Row;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

use crate::utils::response::ApiResponse;
use crate::AppState;

// ---------------------------------------------------------------------------
// Request / response shapes
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct LeaderboardQuery {
    /// Window start (unix seconds). Required.
    pub from: i64,
    /// Window end (unix seconds). Defaults to NOW().
    pub to: Option<i64>,
    /// Top-N cap. Default 100, max 500.
    pub limit: Option<i64>,
    /// `pnl` (default) or `volume`.
    pub sort: Option<SortKey>,
    /// If set and not in top-N, append this user's row with `has_rank: false`.
    pub account: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum SortKey {
    Pnl,
    Volume,
}

impl Default for SortKey {
    fn default() -> Self {
        SortKey::Pnl
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct TraderRow {
    pub address: String,
    pub rank: i64,
    pub realized_pnl: Decimal,
    pub fees_paid: Decimal,
    pub volume: Decimal,
    pub trade_count: i64,
    pub wins: i64,
    pub losses: i64,
    pub has_rank: bool,
}

#[derive(Debug, Serialize, Clone)]
pub struct LeaderboardResponse {
    pub traders: Vec<TraderRow>,
    pub from: i64,
    pub to: i64,
    pub updated_at: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// In-memory SWR cache for top-N (60 s TTL).
//
// 仅缓存 top-N 主查询;`account` 参数走单独的小查询合并到结果里,不进 cache key。
// 单进程多 pod 场景下每个 pod 各自缓存,可接受——榜单不需要全局一致。
// ---------------------------------------------------------------------------

const CACHE_TTL: Duration = Duration::from_secs(60);

#[derive(Clone, Eq, PartialEq, Hash)]
struct CacheKey {
    from_minute: i64,
    to_minute: i64,
    limit: i64,
    sort: SortKey,
}

struct CacheEntry {
    response: LeaderboardResponse,
    at: Instant,
}

type Slot = Arc<Mutex<Option<CacheEntry>>>;

lazy_static::lazy_static! {
    static ref CACHE: DashMap<CacheKey, Slot> = DashMap::new();
}

fn cache() -> &'static DashMap<CacheKey, Slot> {
    &CACHE
}

// ---------------------------------------------------------------------------
// SQL
// ---------------------------------------------------------------------------

/// Top-N aggregate over the time window, joining maker/taker/pnl CAGGs.
///
/// `{sort_expr}` is interpolated (NOT a bind) to one of two trusted strings —
/// `realized_pnl` or `volume` — chosen by `SortKey`. No user input flows into
/// the substitution, so this is not an injection risk.
fn top_n_sql(sort_expr: &str) -> String {
    format!(
        r#"
        WITH range AS (
            SELECT $1::timestamptz AS from_ts, $2::timestamptz AS to_ts
        ),
        maker AS (
            SELECT user_address,
                   SUM(volume)::numeric  AS volume,
                   SUM(fees)::numeric    AS fees,
                   SUM(trade_count)::bigint AS trade_count
            FROM leaderboard_maker_hourly, range
            WHERE bucket >= range.from_ts AND bucket < range.to_ts
            GROUP BY user_address
        ),
        taker AS (
            SELECT user_address,
                   SUM(volume)::numeric  AS volume,
                   SUM(fees)::numeric    AS fees,
                   SUM(trade_count)::bigint AS trade_count
            FROM leaderboard_taker_hourly, range
            WHERE bucket >= range.from_ts AND bucket < range.to_ts
            GROUP BY user_address
        ),
        pnl AS (
            SELECT user_address,
                   SUM(realized_pnl)::numeric AS realized_pnl,
                   SUM(wins)::bigint          AS wins,
                   SUM(losses)::bigint        AS losses,
                   SUM(closed_count)::bigint  AS closed_count
            FROM leaderboard_pnl_hourly, range
            WHERE bucket >= range.from_ts AND bucket < range.to_ts
            GROUP BY user_address
        ),
        users AS (
            SELECT user_address FROM maker
            UNION SELECT user_address FROM taker
            UNION SELECT user_address FROM pnl
        )
        SELECT
            u.user_address                                              AS address,
            COALESCE(p.realized_pnl, 0)::numeric                        AS realized_pnl,
            COALESCE(m.fees, 0) + COALESCE(t.fees, 0)                   AS fees_paid,
            COALESCE(m.volume, 0) + COALESCE(t.volume, 0)               AS volume,
            (COALESCE(m.trade_count, 0) + COALESCE(t.trade_count, 0))::bigint AS trade_count,
            COALESCE(p.wins, 0)::bigint                                 AS wins,
            COALESCE(p.losses, 0)::bigint                               AS losses
        FROM users u
        LEFT JOIN maker m ON m.user_address = u.user_address
        LEFT JOIN taker t ON t.user_address = u.user_address
        LEFT JOIN pnl   p ON p.user_address = u.user_address
        WHERE COALESCE(p.closed_count, 0) > 0
           OR (COALESCE(m.trade_count, 0) + COALESCE(t.trade_count, 0)) > 0
        ORDER BY {sort_expr} DESC NULLS LAST, u.user_address ASC
        LIMIT $3
        "#,
        sort_expr = sort_expr
    )
}

/// Single-user lookup (for `account` self-row). Same shape as `top_n_sql` but
/// filtered to one address; `rank` is computed by the caller after we know
/// the top-N set.
fn one_user_sql() -> &'static str {
    r#"
    WITH range AS (
        SELECT $1::timestamptz AS from_ts, $2::timestamptz AS to_ts, $3::text AS who
    ),
    maker AS (
        SELECT SUM(volume)::numeric  AS volume,
               SUM(fees)::numeric    AS fees,
               SUM(trade_count)::bigint AS trade_count
        FROM leaderboard_maker_hourly, range
        WHERE bucket >= range.from_ts AND bucket < range.to_ts
          AND user_address = range.who
    ),
    taker AS (
        SELECT SUM(volume)::numeric  AS volume,
               SUM(fees)::numeric    AS fees,
               SUM(trade_count)::bigint AS trade_count
        FROM leaderboard_taker_hourly, range
        WHERE bucket >= range.from_ts AND bucket < range.to_ts
          AND user_address = range.who
    ),
    pnl AS (
        SELECT SUM(realized_pnl)::numeric AS realized_pnl,
               SUM(wins)::bigint          AS wins,
               SUM(losses)::bigint        AS losses,
               SUM(closed_count)::bigint  AS closed_count
        FROM leaderboard_pnl_hourly, range
        WHERE bucket >= range.from_ts AND bucket < range.to_ts
          AND user_address = range.who
    )
    SELECT
        (SELECT who FROM range)                                         AS address,
        COALESCE((SELECT realized_pnl FROM pnl), 0)::numeric             AS realized_pnl,
        COALESCE((SELECT fees FROM maker), 0) + COALESCE((SELECT fees FROM taker), 0)
                                                                         AS fees_paid,
        COALESCE((SELECT volume FROM maker), 0) + COALESCE((SELECT volume FROM taker), 0)
                                                                         AS volume,
        (COALESCE((SELECT trade_count FROM maker), 0) +
         COALESCE((SELECT trade_count FROM taker), 0))::bigint           AS trade_count,
        COALESCE((SELECT wins FROM pnl), 0)::bigint                      AS wins,
        COALESCE((SELECT losses FROM pnl), 0)::bigint                    AS losses,
        COALESCE((SELECT closed_count FROM pnl), 0)::bigint              AS closed_count
    "#
}

/// Latest CAGG materialization timestamp — used for the response's
/// `updated_at` field. We read the most-recent bucket across all three
/// CAGGs; cheap (~ms) because the scan covers only the index tip.
fn updated_at_sql() -> &'static str {
    r#"
    SELECT GREATEST(
        (SELECT MAX(bucket) FROM leaderboard_maker_hourly),
        (SELECT MAX(bucket) FROM leaderboard_taker_hourly),
        (SELECT MAX(bucket) FROM leaderboard_pnl_hourly)
    ) AS updated_at
    "#
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

pub async fn get_leaderboard_traders(
    State(state): State<Arc<AppState>>,
    Query(q): Query<LeaderboardQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<ApiResponse<()>>)> {
    let now_secs = Utc::now().timestamp();
    let from = q.from;
    let to = q.to.unwrap_or(now_secs);
    // `from == 0` is the frontend's "All-time" sentinel
    // (LEADERBOARD_PAGES.leaderboard.timeframe.from). Accept it — the SQL
    // window `bucket >= epoch(0)` naturally includes every materialised bucket.
    if from < 0 || to <= from {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ApiResponse::<()>::error(
                "BAD_REQUEST",
                "Invalid time window: require from >= 0 and to > from",
            )),
        ));
    }
    let limit = q.limit.unwrap_or(100).clamp(1, 500);
    let sort = q.sort.unwrap_or_default();
    let sort_expr = match sort {
        SortKey::Pnl => "realized_pnl",
        SortKey::Volume => "volume",
    };

    // Cache key: floor `from`/`to` to the minute so neighbouring requests
    // collapse onto the same slot.
    let key = CacheKey {
        from_minute: from / 60,
        to_minute: to / 60,
        limit,
        sort,
    };

    // SWR slot.
    let slot: Slot = {
        let entry = cache()
            .entry(key.clone())
            .or_insert_with(|| Arc::new(Mutex::new(None)));
        Arc::clone(entry.value())
    };
    let mut guard = slot.lock().await;

    let response: LeaderboardResponse = if let Some(entry) = guard.as_ref() {
        if entry.at.elapsed() < CACHE_TTL {
            entry.response.clone()
        } else {
            fetch_top_n(&state, from, to, limit, sort_expr)
                .await
                .map_err(internal)?
        }
    } else {
        fetch_top_n(&state, from, to, limit, sort_expr)
            .await
            .map_err(internal)?
    };

    if guard
        .as_ref()
        .map(|e| e.at.elapsed() >= CACHE_TTL)
        .unwrap_or(true)
    {
        *guard = Some(CacheEntry {
            response: response.clone(),
            at: Instant::now(),
        });
    }
    drop(guard);

    // Self-row append (off the cache hot path on purpose: per-account
    // queries are cheap, and we don't want to pollute the shared cache).
    let mut traders = response.traders.clone();
    if let Some(addr_raw) = q.account.as_deref() {
        let addr = addr_raw.to_lowercase();
        if !traders.iter().any(|r| r.address == addr) {
            if let Some(self_row) = fetch_one_user(&state, from, to, &addr)
                .await
                .map_err(internal)?
            {
                traders.push(self_row);
            }
        }
    }

    Ok(Json(ApiResponse::success(LeaderboardResponse {
        traders,
        from: response.from,
        to: response.to,
        updated_at: response.updated_at,
    })))
}

// ---------------------------------------------------------------------------
// DB helpers
// ---------------------------------------------------------------------------

async fn fetch_top_n(
    state: &Arc<AppState>,
    from: i64,
    to: i64,
    limit: i64,
    sort_expr: &str,
) -> Result<LeaderboardResponse, sqlx::Error> {
    let from_ts = DateTime::<Utc>::from_timestamp(from, 0)
        .ok_or_else(|| sqlx::Error::Protocol("invalid from".into()))?;
    let to_ts = DateTime::<Utc>::from_timestamp(to, 0)
        .ok_or_else(|| sqlx::Error::Protocol("invalid to".into()))?;

    let rows = sqlx::query(&top_n_sql(sort_expr))
        .bind(from_ts)
        .bind(to_ts)
        .bind(limit)
        .fetch_all(&state.db.pool)
        .await?;

    let traders: Vec<TraderRow> = rows
        .into_iter()
        .enumerate()
        .map(|(idx, row)| TraderRow {
            address: row.get::<String, _>("address"),
            rank: (idx as i64) + 1,
            realized_pnl: row.get::<Decimal, _>("realized_pnl"),
            fees_paid: row.get::<Decimal, _>("fees_paid"),
            volume: row.get::<Decimal, _>("volume"),
            trade_count: row.get::<i64, _>("trade_count"),
            wins: row.get::<i64, _>("wins"),
            losses: row.get::<i64, _>("losses"),
            has_rank: true,
        })
        .collect();

    let updated_at: DateTime<Utc> = sqlx::query(updated_at_sql())
        .fetch_one(&state.db.pool)
        .await
        .ok()
        .and_then(|r| r.try_get::<Option<DateTime<Utc>>, _>("updated_at").ok().flatten())
        .unwrap_or_else(Utc::now);

    Ok(LeaderboardResponse {
        traders,
        from,
        to,
        updated_at,
    })
}

async fn fetch_one_user(
    state: &Arc<AppState>,
    from: i64,
    to: i64,
    address: &str,
) -> Result<Option<TraderRow>, sqlx::Error> {
    let from_ts = DateTime::<Utc>::from_timestamp(from, 0)
        .ok_or_else(|| sqlx::Error::Protocol("invalid from".into()))?;
    let to_ts = DateTime::<Utc>::from_timestamp(to, 0)
        .ok_or_else(|| sqlx::Error::Protocol("invalid to".into()))?;

    let row = sqlx::query(one_user_sql())
        .bind(from_ts)
        .bind(to_ts)
        .bind(address)
        .fetch_one(&state.db.pool)
        .await?;

    let trade_count: i64 = row.get("trade_count");
    let closed_count: i64 = row.get("closed_count");
    if trade_count == 0 && closed_count == 0 {
        return Ok(None);
    }

    Ok(Some(TraderRow {
        address: row.get::<String, _>("address"),
        rank: 0, // unranked self-row
        realized_pnl: row.get::<Decimal, _>("realized_pnl"),
        fees_paid: row.get::<Decimal, _>("fees_paid"),
        volume: row.get::<Decimal, _>("volume"),
        trade_count,
        wins: row.get::<i64, _>("wins"),
        losses: row.get::<i64, _>("losses"),
        has_rank: false,
    }))
}

fn internal(e: sqlx::Error) -> (StatusCode, Json<ApiResponse<()>>) {
    tracing::error!("leaderboard_traders DB error: {}", e);
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ApiResponse::<()>::error(
            "INTERNAL_ERROR",
            "Failed to query leaderboard data",
        )),
    )
}
