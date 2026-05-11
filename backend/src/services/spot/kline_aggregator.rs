//! Kline aggregator. Subscribes to EngineEvent::Fill via tokio::broadcast and
//! upserts spot_klines for each interval (1m/5m/15m/1h/4h/1d). Idempotent
//! per fill (each Fill bumps the relevant kline row by exactly one trade).

use sqlx::PgPool;
use tokio::sync::broadcast;
use chrono::{Duration, DurationRound, Utc, DateTime};

use crate::models::spot::SpotTrade;
use crate::services::spot::matching::types::EngineEvent;
use crate::services::spot::ws_messages::SpotKlinePush;

const INTERVALS: [&str; 6] = ["1m","5m","15m","1h","4h","1d"];

pub async fn run(
    pool: PgPool,
    mut rx: broadcast::Receiver<EngineEvent>,
    kline_tx: broadcast::Sender<SpotKlinePush>,
) {
    tracing::info!("spot kline aggregator starting");
    loop {
        match rx.recv().await {
            Ok(EngineEvent::Fill { trade, .. }) => {
                for iv in INTERVALS {
                    match upsert_kline(&pool, &trade, iv).await {
                        Ok(()) => {
                            push_kline(&pool, &kline_tx, &trade.market_id, iv).await;
                        }
                        Err(e) => {
                            tracing::error!("kline upsert {iv} failed: {e}");
                        }
                    }
                }
            }
            Ok(_) => { /* non-fill events have no kline impact */ }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                tracing::warn!("kline aggregator lagged: {n} events dropped");
                continue;
            }
            Err(broadcast::error::RecvError::Closed) => {
                tracing::warn!("kline aggregator: broadcast channel closed, exiting");
                return;
            }
        }
    }
}

fn floor_to(ts: DateTime<Utc>, iv: &str) -> DateTime<Utc> {
    let d = match iv {
        "1m"  => Duration::minutes(1),
        "5m"  => Duration::minutes(5),
        "15m" => Duration::minutes(15),
        "1h"  => Duration::hours(1),
        "4h"  => Duration::hours(4),
        "1d"  => Duration::days(1),
        _ => unreachable!(),
    };
    ts.duration_trunc(d).unwrap_or(ts)
}

async fn upsert_kline(pool: &PgPool, t: &SpotTrade, iv: &str) -> sqlx::Result<()> {
    let open_time = floor_to(t.created_at, iv);
    let quote_vol = t.price * t.quantity;
    sqlx::query(
        "INSERT INTO spot_klines
            (market_id, interval, open_time,
             open_price, high_price, low_price, close_price,
             volume, quote_volume, trade_count)
         VALUES ($1, $2, $3, $4, $4, $4, $4, $5, $6, 1)
         ON CONFLICT (market_id, interval, open_time)
         DO UPDATE SET
            high_price  = GREATEST(spot_klines.high_price, EXCLUDED.high_price),
            low_price   = LEAST(spot_klines.low_price, EXCLUDED.low_price),
            close_price = EXCLUDED.close_price,
            volume      = spot_klines.volume + EXCLUDED.volume,
            quote_volume = spot_klines.quote_volume + EXCLUDED.quote_volume,
            trade_count = spot_klines.trade_count + 1"
    )
    .bind(&t.market_id).bind(iv).bind(open_time)
    .bind(t.price).bind(t.quantity).bind(quote_vol)
    .execute(pool).await?;
    Ok(())
}

/// Re-read the latest row for (market, interval) and broadcast it.
/// Called after a successful upsert so subscribers receive the live update.
async fn push_kline(
    pool: &PgPool,
    tx: &broadcast::Sender<SpotKlinePush>,
    market: &str,
    iv: &str,
) {
    use crate::models::spot::SpotKline;
    let row: Result<SpotKline, _> = sqlx::query_as(
        "SELECT * FROM spot_klines WHERE market_id=$1 AND interval=$2
         ORDER BY open_time DESC LIMIT 1"
    )
    .bind(market).bind(iv)
    .fetch_one(pool).await;
    if let Ok(k) = row {
        let iv_secs: i64 = match iv {
            "1m"=>60,"5m"=>300,"15m"=>900,"1h"=>3600,"4h"=>14400,"1d"=>86400,_=>0
        };
        let open_ts = k.open_time.timestamp();
        let close_ts = open_ts + iv_secs - 1;
        let now = chrono::Utc::now().timestamp();
        let _ = tx.send(SpotKlinePush {
            symbol: k.market_id,
            interval: k.interval,
            open_time: open_ts,
            close_time: close_ts,
            open: k.open_price.normalize().to_string(),
            high: k.high_price.normalize().to_string(),
            low:  k.low_price.normalize().to_string(),
            close: k.close_price.normalize().to_string(),
            volume: k.volume.normalize().to_string(),
            quote_volume: k.quote_volume.normalize().to_string(),
            trade_count: k.trade_count,
            is_closed: now > close_ts,
        });
    }
}
