//! 24h ticker aggregator. Periodically recomputes spot_ticker_24h from
//! spot_trades for each listed market. Counterpart to commit_fill's
//! incremental update — this task handles decay (trades falling out of the
//! 24h window).

use sqlx::PgPool;
use tokio::time::{sleep, Duration as TokDur};
use chrono::{Utc, Duration};
use rust_decimal::Decimal;

pub async fn run(pool: PgPool) {
    tracing::info!("spot ticker_24h aggregator starting (60s interval)");
    loop {
        if let Err(e) = recompute_all(&pool).await {
            tracing::error!("ticker recompute failed: {e}");
        }
        sleep(TokDur::from_secs(60)).await;
    }
}

async fn recompute_all(pool: &PgPool) -> sqlx::Result<()> {
    let markets: Vec<(String,)> = sqlx::query_as(
        "SELECT id FROM spot_markets WHERE status='listed'"
    ).fetch_all(pool).await?;
    for (m,) in markets {
        if let Err(e) = recompute_one(pool, &m).await {
            tracing::error!(market = %m, "ticker recompute_one failed: {e}");
        }
    }
    Ok(())
}

async fn recompute_one(pool: &PgPool, market_id: &str) -> sqlx::Result<()> {
    let cutoff = Utc::now() - Duration::hours(24);

    // Open price = first trade price within the 24h window
    let open: Option<Decimal> = sqlx::query_scalar(
        "SELECT price FROM spot_trades
          WHERE market_id=$1 AND created_at>=$2
          ORDER BY created_at ASC LIMIT 1"
    ).bind(market_id).bind(cutoff).fetch_optional(pool).await?;

    // Aggregates over the 24h window
    let agg: Option<(Option<Decimal>, Option<Decimal>, Option<Decimal>, Option<Decimal>, i64)> = sqlx::query_as(
        "SELECT MAX(price), MIN(price), SUM(quantity), SUM(price * quantity), COUNT(*)
         FROM spot_trades WHERE market_id=$1 AND created_at>=$2"
    ).bind(market_id).bind(cutoff).fetch_optional(pool).await?;

    let Some((high, low, volume, qvol, cnt)) = agg else { return Ok(()); };

    // last_price = most recent trade EVER (not just in window) so the ticker
    // doesn't show stale data when there were no trades in the last 24h.
    let last: Option<Decimal> = sqlx::query_scalar(
        "SELECT price FROM spot_trades WHERE market_id=$1 ORDER BY created_at DESC LIMIT 1"
    ).bind(market_id).fetch_optional(pool).await?;

    let Some(last) = last else { return Ok(()); };
    let open = open.unwrap_or(last);
    let high = high.unwrap_or(last);
    let low  = low.unwrap_or(last);
    let volume = volume.unwrap_or(Decimal::ZERO);
    let qvol   = qvol.unwrap_or(Decimal::ZERO);

    sqlx::query(
        "INSERT INTO spot_ticker_24h
            (market_id, last_price, open_price_24h, high_24h, low_24h,
             volume_24h, quote_volume_24h, trade_count_24h, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
         ON CONFLICT (market_id) DO UPDATE SET
             last_price       = EXCLUDED.last_price,
             open_price_24h   = EXCLUDED.open_price_24h,
             high_24h         = EXCLUDED.high_24h,
             low_24h          = EXCLUDED.low_24h,
             volume_24h       = EXCLUDED.volume_24h,
             quote_volume_24h = EXCLUDED.quote_volume_24h,
             trade_count_24h  = EXCLUDED.trade_count_24h,
             updated_at       = NOW()"
    )
    .bind(market_id).bind(last).bind(open).bind(high).bind(low)
    .bind(volume).bind(qvol).bind(cnt)
    .execute(pool).await?;
    Ok(())
}
