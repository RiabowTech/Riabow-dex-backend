//! Referral commission leaderboard — incremental background worker
//!
//! Every 5 minutes the worker:
//!   1. Reads the high-water mark from `referral_lb_watermark`
//!   2. Aggregates new `referral_earnings` rows (watermark < created_at ≤ NOW()-10s)
//!      The 10-second buffer prevents counting rows that are mid-flight (written
//!      with a timestamp ≤ cutoff but not yet committed to Postgres).
//!   3. Upserts the delta into `referral_commission_leaderboard`
//!   4. Advances the watermark to the cutoff timestamp used in step 2
//!
//! The handler (`GET /referral/leaderboard?n=<1-50>`) simply reads the
//! pre-computed table, so query latency is O(1) regardless of history depth.

use std::sync::Arc;

use sqlx::PgPool;
use tokio::time::{interval, Duration};

pub struct ReferralLeaderboardService {
    pool: PgPool,
}

impl ReferralLeaderboardService {
    pub fn new(pool: PgPool) -> Arc<Self> {
        Arc::new(Self { pool })
    }

    /// Run one incremental refresh cycle.
    pub async fn run_incremental(&self) -> anyhow::Result<()> {
        // 1. Read current watermark
        let watermark: chrono::DateTime<chrono::Utc> = sqlx::query_scalar(
            "SELECT last_processed_at FROM referral_lb_watermark WHERE id = 1",
        )
        .fetch_one(&self.pool)
        .await?;

        // 2. Cutoff = NOW() - 10s (buffer against mid-flight writes).
        //    Early-exit if there are no rows in the window.
        let cutoff: chrono::DateTime<chrono::Utc> = sqlx::query_scalar(
            "SELECT NOW() - INTERVAL '10 seconds'",
        )
        .fetch_one(&self.pool)
        .await?;

        if cutoff <= watermark {
            return Ok(()); // window is empty or not yet advanced
        }

        let has_rows: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM referral_earnings WHERE created_at > $1 AND created_at <= $2)",
        )
        .bind(watermark)
        .bind(cutoff)
        .fetch_one(&self.pool)
        .await?;

        if !has_rows {
            return Ok(());
        }

        // 3. Upsert delta: add incremental commission per referrer
        sqlx::query(
            r#"
            INSERT INTO referral_commission_leaderboard (referrer_address, total_commission, computed_at)
            SELECT
                referrer_address,
                COALESCE(SUM(commission), 0),
                NOW()
            FROM referral_earnings
            WHERE created_at > $1 AND created_at <= $2
            GROUP BY referrer_address
            ON CONFLICT (referrer_address) DO UPDATE
                SET total_commission = referral_commission_leaderboard.total_commission
                                       + EXCLUDED.total_commission,
                    computed_at       = EXCLUDED.computed_at
            "#,
        )
        .bind(watermark)
        .bind(cutoff)
        .execute(&self.pool)
        .await?;

        // 4. Advance watermark to the cutoff used above
        sqlx::query(
            "UPDATE referral_lb_watermark SET last_processed_at = $1 WHERE id = 1",
        )
        .bind(cutoff)
        .execute(&self.pool)
        .await?;

        tracing::debug!(
            "[referral_leaderboard] incremental refresh done: watermark {} → {}",
            watermark,
            cutoff
        );

        Ok(())
    }

    /// Spawn the 5-minute background loop.
    pub fn start(self: Arc<Self>) {
        tokio::spawn(async move {
            // Run once immediately on startup so the table is populated
            // before the first request arrives.
            if let Err(e) = self.run_incremental().await {
                tracing::warn!("[referral_leaderboard] initial refresh failed: {}", e);
            }

            let mut ticker = interval(Duration::from_secs(300));
            ticker.tick().await; // consume the first immediate tick
            loop {
                ticker.tick().await;
                if let Err(e) = self.run_incremental().await {
                    tracing::error!("[referral_leaderboard] incremental refresh failed: {}", e);
                }
            }
        });
    }
}
