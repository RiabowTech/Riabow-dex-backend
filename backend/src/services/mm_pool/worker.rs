//! MM scoring sampling worker.
//!
//! Every `INTERVAL_SECS` we sample each whitelisted MM × symbol and
//! append a row to `mm_quality_snapshots`. The per-tick composite
//! quality score is folded into `mm_points_balance` for the active
//! epoch.

use rust_decimal::Decimal;
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Duration;

use crate::app::state::AppState;
use crate::services::mm_pool::scoring::quality_score;

/// 5 minutes between samples — gives a snapshot rhythm coarse enough
/// for cheap aggregation but fine enough that a 4-week epoch has
/// >8000 samples per MM.
const INTERVAL_SECS: u64 = 300;

pub fn spawn(state: Arc<AppState>) {
    tokio::spawn(async move {
        tracing::info!("MM-pool scoring worker started (interval: {}s)", INTERVAL_SECS);
        let mut ticker = tokio::time::interval(Duration::from_secs(INTERVAL_SECS));
        ticker.tick().await; // skip immediate fire

        loop {
            ticker.tick().await;
            if let Err(e) = tick(&state).await {
                tracing::warn!("MM scoring tick failed: {}", e);
            }
        }
    });
}

async fn tick(state: &Arc<AppState>) -> anyhow::Result<()> {
    // Active epoch is required — without one, no balance row to update.
    let epoch = match state.points_service.get_active_epoch().await? {
        Some(e) => e.epoch_number,
        None => return Ok(()),
    };

    let pool: &PgPool = &state.db.pool;

    // Active MM whitelist.
    let mms: Vec<(String,)> = sqlx::query_as(
        "SELECT address FROM mm_program_members WHERE is_active = true",
    )
    .fetch_all(pool)
    .await?;
    if mms.is_empty() {
        return Ok(());
    }

    // Active symbols. Mirror what's known to the matching engine.
    let symbols: Vec<(String,)> = sqlx::query_as(
        "SELECT symbol FROM market_configs WHERE listing_phase = 'active'",
    )
    .fetch_all(pool)
    .await
    .unwrap_or_default();

    let snapshot_window_secs = INTERVAL_SECS as i64;

    for (mm_addr,) in &mms {
        for (symbol,) in &symbols {
            // 1. Maker volume since previous snapshot (notional value).
            //    Falls back to `INTERVAL_SECS`-window scan if the prior
            //    snapshot row is missing.
            let prev_at: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(
                "SELECT MAX(snapshot_at) FROM mm_quality_snapshots
                 WHERE mm_address = $1 AND symbol = $2",
            )
            .bind(mm_addr)
            .bind(symbol)
            .fetch_one(pool)
            .await
            .unwrap_or(None);

            let since = prev_at
                .unwrap_or_else(|| chrono::Utc::now() - chrono::Duration::seconds(snapshot_window_secs));

            let maker_volume_usd: Decimal = sqlx::query_scalar(
                "SELECT COALESCE(SUM(price * amount), 0) FROM trades
                 WHERE maker_address = $1 AND symbol = $2 AND created_at > $3
                   AND is_self_trade = false",
            )
            .bind(mm_addr)
            .bind(symbol)
            .bind(since)
            .fetch_one(pool)
            .await
            .unwrap_or(Decimal::ZERO);

            // 2. Depth = sum(amount * price) of MM's open orders for this symbol.
            let depth_usd: Decimal = sqlx::query_scalar(
                "SELECT COALESCE(SUM((amount - filled_amount) * price), 0) FROM orders
                 WHERE user_address = $1 AND symbol = $2
                   AND status IN ('pending','open','partially_filled')",
            )
            .bind(mm_addr)
            .bind(symbol)
            .fetch_one(pool)
            .await
            .unwrap_or(Decimal::ZERO);

            // 3. Spread bps from MM's own best bid/ask.
            let mm_best_bid: Option<Decimal> = sqlx::query_scalar(
                "SELECT MAX(price) FROM orders
                 WHERE user_address = $1 AND symbol = $2 AND side = 'buy'
                   AND status IN ('pending','open','partially_filled')",
            )
            .bind(mm_addr)
            .bind(symbol)
            .fetch_one(pool)
            .await
            .unwrap_or(None);
            let mm_best_ask: Option<Decimal> = sqlx::query_scalar(
                "SELECT MIN(price) FROM orders
                 WHERE user_address = $1 AND symbol = $2 AND side = 'sell'
                   AND status IN ('pending','open','partially_filled')",
            )
            .bind(mm_addr)
            .bind(symbol)
            .fetch_one(pool)
            .await
            .unwrap_or(None);

            let spread_bps = match (mm_best_bid, mm_best_ask) {
                (Some(b), Some(a)) if b > Decimal::ZERO && a > b => {
                    let mid = (a + b) / Decimal::from(2);
                    Some((a - b) / mid * Decimal::from(10_000))
                }
                _ => None,
            };

            let is_online = depth_usd > Decimal::ZERO;
            let score = quality_score(maker_volume_usd, spread_bps, depth_usd, is_online);

            // Skip writing snapshots for MMs that are completely silent
            // on this symbol (no positions / no trades / no open orders).
            // This keeps the table from ballooning across all symbols
            // for MMs that only quote a handful.
            if maker_volume_usd.is_zero() && depth_usd.is_zero() {
                continue;
            }

            // Insert snapshot.
            let _ = sqlx::query(
                "INSERT INTO mm_quality_snapshots
                    (mm_address, symbol, maker_volume_usd, spread_bps, depth_usd,
                     is_online, quality_score, epoch_number)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            )
            .bind(mm_addr)
            .bind(symbol)
            .bind(maker_volume_usd)
            .bind(spread_bps)
            .bind(depth_usd)
            .bind(is_online)
            .bind(score)
            .bind(epoch)
            .execute(pool)
            .await;

            // Aggregate into balance row.
            let _ = sqlx::query(
                "INSERT INTO mm_points_balance
                    (mm_address, epoch_number, quality_score_sum, snapshot_count, updated_at)
                 VALUES ($1, $2, $3, 1, NOW())
                 ON CONFLICT (mm_address, epoch_number) DO UPDATE SET
                    quality_score_sum = mm_points_balance.quality_score_sum + EXCLUDED.quality_score_sum,
                    snapshot_count    = mm_points_balance.snapshot_count + 1,
                    updated_at        = NOW()",
            )
            .bind(mm_addr)
            .bind(epoch)
            .bind(score)
            .execute(pool)
            .await;
        }
    }

    Ok(())
}
