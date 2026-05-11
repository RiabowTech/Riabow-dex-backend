//! Phase 3 — Season-end snapshot + token distribution worker.
//!
//! Every minute, we look for seasons where:
//!   * status = 'active'
//!   * every epoch in [start_epoch, end_epoch] has `end_time` past now
//!
//! For each such season we:
//!   1. Aggregate per-user TP/PP/HP/RP across the season's epochs
//!   2. Compute weighted = TP·0.65 + PP·0.20 + HP·0.05 + RP·0.10
//!   3. user_pool tokens are split pro-rata by weighted points
//!   4. mm_pool tokens are split pro-rata by quality_score_sum
//!   5. Rows land in `points_distribution` (claim_status='pending',
//!      claim_deadline = NOW() + 30 days, monotonic per-user nonce)
//!   6. Mark season status='completed' (snapshot_taken_at + distribution_at)
//!
//! Idempotent: re-running on a snapshotted season is a no-op.

use anyhow::{Context, Result};
use rust_decimal::Decimal;
use sqlx::PgPool;
use std::sync::Arc;

use crate::app::state::AppState;

pub const TICK_SECS: u64 = 60;

const W_TP: Decimal = rust_decimal_macros::dec!(0.65);
const W_PP: Decimal = rust_decimal_macros::dec!(0.20);
const W_HP: Decimal = rust_decimal_macros::dec!(0.05);
const W_RP: Decimal = rust_decimal_macros::dec!(0.10);

pub fn spawn(state: Arc<AppState>) {
    tokio::spawn(async move {
        tracing::info!("Season-snapshot worker started (interval: {}s)", TICK_SECS);
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(TICK_SECS));
        ticker.tick().await; // skip immediate
        loop {
            ticker.tick().await;
            if let Err(e) = tick(&state.db.pool).await {
                tracing::warn!("season-snapshot tick failed: {}", e);
            }
        }
    });
}

async fn tick(pool: &PgPool) -> Result<()> {
    // Candidate seasons: active and all member epochs are past end_time.
    // Note: the points system uses `points_epochs` (columns: epoch_number,
    // start_time, end_time). The separate `epochs` table exists for a
    // legacy UI path but is NOT the source of truth for the points flow.
    let candidates: Vec<(uuid::Uuid, i32, i32, i32, i64, i64)> = sqlx::query_as(
        r#"
        SELECT s.season_id, s.season_no, s.start_epoch, s.end_epoch,
               s.user_pool_tokens, s.mm_pool_tokens
        FROM points_seasons s
        WHERE s.status = 'active'
          AND NOT EXISTS (
              SELECT 1 FROM points_epochs e
              WHERE e.epoch_number BETWEEN s.start_epoch AND s.end_epoch
                AND (e.end_time IS NULL OR e.end_time > NOW())
          )
          -- also require at least 1 epoch row exists in the season range
          AND EXISTS (
              SELECT 1 FROM points_epochs e
              WHERE e.epoch_number BETWEEN s.start_epoch AND s.end_epoch
          )
        "#,
    )
    .fetch_all(pool)
    .await?;

    for (season_id, season_no, start_ep, end_ep, user_pool, mm_pool) in candidates {
        if let Err(e) =
            snapshot_season(pool, season_id, season_no, start_ep, end_ep, user_pool, mm_pool).await
        {
            tracing::error!(
                "Snapshot season {} failed: {} (will retry next tick)",
                season_no, e
            );
        }
    }

    Ok(())
}

/// Run the actual snapshot/distribution for one season. Public so admin
/// API can trigger it manually. Idempotent.
pub async fn snapshot_season(
    pool: &PgPool,
    season_id: uuid::Uuid,
    season_no: i32,
    start_ep: i32,
    end_ep: i32,
    user_pool_tokens: i64,
    mm_pool_tokens: i64,
) -> Result<()> {
    let mut tx = pool.begin().await.context("begin tx")?;

    // Lock the season row. If another worker already advanced it, exit.
    let row: Option<(String,)> = sqlx::query_as(
        "SELECT status FROM points_seasons WHERE season_id = $1 FOR UPDATE",
    )
    .bind(season_id)
    .fetch_optional(&mut *tx)
    .await?;
    match row {
        Some((s,)) if s == "active" => {} // proceed
        Some((s,)) => {
            tracing::debug!("Season {} status is {} — skip", season_no, s);
            return Ok(());
        }
        None => return Ok(()),
    }

    // ---- USER POOL ----
    // Sum per-user weighted points across all epochs in the range.
    #[derive(sqlx::FromRow)]
    struct UserAgg {
        user_address: String,
        weighted: Decimal,
    }
    let user_aggs: Vec<UserAgg> = sqlx::query_as(
        r#"
        SELECT user_address,
               SUM(trading_points  * $1
                 + pnl_points      * $2
                 + holding_points  * $3
                 + referral_points * $4) AS weighted
        FROM user_points_summary
        WHERE epoch_number BETWEEN $5 AND $6
        GROUP BY user_address
        HAVING SUM(trading_points * $1 + pnl_points * $2
                 + holding_points * $3 + referral_points * $4) > 0
        "#,
    )
    .bind(W_TP).bind(W_PP).bind(W_HP).bind(W_RP)
    .bind(start_ep).bind(end_ep)
    .fetch_all(&mut *tx)
    .await
    .context("aggregate user points")?;

    let total_user_weighted: Decimal = user_aggs.iter().map(|a| a.weighted).sum();
    let user_pool = Decimal::from(user_pool_tokens);
    let deadline_secs = 30i64 * 24 * 3600;

    if total_user_weighted > Decimal::ZERO {
        for u in &user_aggs {
            let share = u.weighted / total_user_weighted;
            let tokens = user_pool * share;

            // Bump per-user nonce: (max existing nonce for this user) + 1.
            let next_nonce: i64 = sqlx::query_scalar(
                "SELECT COALESCE(MAX(claim_nonce), 0) + 1
                 FROM points_distribution WHERE user_address = $1",
            )
            .bind(&u.user_address)
            .fetch_one(&mut *tx)
            .await?;

            sqlx::query(
                r#"
                INSERT INTO points_distribution
                    (season_id, user_address, pool_type, weighted_points, share_pct,
                     token_amount, claim_status, claim_deadline, claim_nonce)
                VALUES ($1, $2, 'user', $3, $4, $5, 'pending',
                        NOW() + ($6 || ' seconds')::interval, $7)
                ON CONFLICT (season_id, user_address, pool_type) DO NOTHING
                "#,
            )
            .bind(season_id)
            .bind(&u.user_address)
            .bind(u.weighted)
            .bind(share)
            .bind(tokens)
            .bind(deadline_secs.to_string())
            .bind(next_nonce)
            .execute(&mut *tx)
            .await?;
        }
    }

    // ---- MM POOL ----
    #[derive(sqlx::FromRow)]
    struct MmAgg {
        mm_address: String,
        score: Decimal,
    }
    let mm_aggs: Vec<MmAgg> = sqlx::query_as(
        r#"
        SELECT mm_address, SUM(quality_score_sum) AS score
        FROM mm_points_balance
        WHERE epoch_number BETWEEN $1 AND $2
        GROUP BY mm_address
        HAVING SUM(quality_score_sum) > 0
        "#,
    )
    .bind(start_ep).bind(end_ep)
    .fetch_all(&mut *tx)
    .await
    .context("aggregate mm scores")?;

    let total_mm_score: Decimal = mm_aggs.iter().map(|a| a.score).sum();
    let mm_pool = Decimal::from(mm_pool_tokens);

    if total_mm_score > Decimal::ZERO {
        for m in &mm_aggs {
            let share = m.score / total_mm_score;
            let tokens = mm_pool * share;

            let next_nonce: i64 = sqlx::query_scalar(
                "SELECT COALESCE(MAX(claim_nonce), 0) + 1
                 FROM points_distribution WHERE user_address = $1",
            )
            .bind(&m.mm_address)
            .fetch_one(&mut *tx)
            .await?;

            sqlx::query(
                r#"
                INSERT INTO points_distribution
                    (season_id, user_address, pool_type, weighted_points, share_pct,
                     token_amount, claim_status, claim_deadline, claim_nonce)
                VALUES ($1, $2, 'mm', $3, $4, $5, 'pending',
                        NOW() + ($6 || ' seconds')::interval, $7)
                ON CONFLICT (season_id, user_address, pool_type) DO NOTHING
                "#,
            )
            .bind(season_id)
            .bind(&m.mm_address)
            .bind(m.score)
            .bind(share)
            .bind(tokens)
            .bind(deadline_secs.to_string())
            .bind(next_nonce)
            .execute(&mut *tx)
            .await?;
        }
    }

    // Mark season completed.
    sqlx::query(
        "UPDATE points_seasons
         SET status = 'completed',
             snapshot_taken_at = NOW(),
             distribution_at   = NOW(),
             updated_at = NOW()
         WHERE season_id = $1",
    )
    .bind(season_id)
    .execute(&mut *tx)
    .await?;

    tx.commit().await.context("commit snapshot tx")?;

    tracing::warn!(
        "Season {} snapshot complete: {} users (Σ weighted={}), {} MMs (Σ score={})",
        season_no,
        user_aggs.len(),
        total_user_weighted,
        mm_aggs.len(),
        total_mm_score
    );
    Ok(())
}
