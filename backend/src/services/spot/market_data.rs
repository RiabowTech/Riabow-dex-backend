//! Read-path queries for public market data. Lives in services/ so handlers
//! stay thin.

use sqlx::PgPool;
use chrono::{DateTime, Utc};
use crate::models::spot::{SpotMarket, SpotTrade, SpotKline, SpotTicker24h};

pub async fn list_markets(pool: &PgPool) -> sqlx::Result<Vec<SpotMarket>> {
    sqlx::query_as("SELECT * FROM spot_markets ORDER BY id").fetch_all(pool).await
}

pub async fn get_market(pool: &PgPool, market_id: &str) -> sqlx::Result<Option<SpotMarket>> {
    sqlx::query_as("SELECT * FROM spot_markets WHERE id = $1")
        .bind(market_id).fetch_optional(pool).await
}

pub async fn recent_trades(pool: &PgPool, market_id: &str, limit: i64) -> sqlx::Result<Vec<SpotTrade>> {
    let limit = limit.clamp(1, 1000);
    sqlx::query_as("SELECT * FROM spot_trades WHERE market_id=$1 ORDER BY created_at DESC LIMIT $2")
        .bind(market_id).bind(limit).fetch_all(pool).await
}

pub async fn klines(
    pool: &PgPool,
    market_id: &str,
    interval: &str,
    limit: i64,
    start: Option<DateTime<Utc>>,
    end:   Option<DateTime<Utc>>,
) -> sqlx::Result<Vec<SpotKline>> {
    use sqlx::QueryBuilder;
    let limit = limit.clamp(1, 1000);
    let mut qb: QueryBuilder<sqlx::Postgres> = QueryBuilder::new(
        "SELECT * FROM spot_klines WHERE market_id = "
    );
    qb.push_bind(market_id).push(" AND interval = ").push_bind(interval);
    if let Some(s) = start { qb.push(" AND open_time >= ").push_bind(s); }
    if let Some(e) = end   { qb.push(" AND open_time <= ").push_bind(e); }
    // SQL fetches the latest N candles via DESC LIMIT; we then re-sort
    // ASC so the response matches Binance / perp /markets/.../candles
    // convention (oldest first → newest last). Without this, the
    // TradingView datafeed in interface/ breaks on the first iteration
    // because the newest bar has time > requested `to` and the loop
    // exits immediately with an empty result.
    qb.push(" ORDER BY open_time DESC LIMIT ").push_bind(limit);
    let mut rows: Vec<SpotKline> = qb.build_query_as::<SpotKline>().fetch_all(pool).await?;
    rows.sort_by_key(|k| k.open_time);
    Ok(rows)
}

pub async fn ticker_24h(pool: &PgPool, market_id: Option<&str>) -> sqlx::Result<Vec<SpotTicker24h>> {
    if let Some(m) = market_id {
        sqlx::query_as("SELECT * FROM spot_ticker_24h WHERE market_id=$1").bind(m).fetch_all(pool).await
    } else {
        sqlx::query_as("SELECT * FROM spot_ticker_24h").fetch_all(pool).await
    }
}
