//! Reaper task: expires withdrawals that are still 'signed' beyond TTL.
//! Reverses the freeze: frozen -= amount, available += amount.

use anyhow::Result;
use sqlx::PgPool;
use std::sync::Arc;
use tokio::time::{sleep, Duration};

use crate::services::spot::config::SpotConfig;

pub struct SpotReaper {
    pool: PgPool,
    ttl_secs: u64,
    interval_secs: u64,
}

impl SpotReaper {
    pub fn new(cfg: &SpotConfig, pool: PgPool) -> Arc<Self> {
        Arc::new(Self {
            pool,
            ttl_secs: cfg.withdraw_nonce_ttl_secs,
            // Poll every 10 minutes regardless of TTL.
            interval_secs: 600,
        })
    }

    pub async fn run(self: Arc<Self>) {
        tracing::info!(ttl_secs = self.ttl_secs, "spot reaper starting");
        loop {
            if let Err(e) = self.reap_once().await {
                tracing::error!("spot reaper error: {e:?}");
            }
            sleep(Duration::from_secs(self.interval_secs)).await;
        }
    }

    async fn reap_once(&self) -> Result<()> {
        // For each row that's been signed but not confirmed within TTL:
        //   1. Refund the freeze: spot_balances frozen -= amount, available += amount
        //   2. Mark the withdrawal expired
        let candidates: Vec<(uuid::Uuid, String, String, rust_decimal::Decimal)> = sqlx::query_as(
            "SELECT id, user_address, token, amount FROM spot_withdrawals
              WHERE status = 'signed'
                AND requested_at < NOW() - ($1 || ' seconds')::interval"
        )
        .bind(self.ttl_secs.to_string())
        .fetch_all(&self.pool).await?;

        if candidates.is_empty() { return Ok(()); }

        for (id, user, token, amount) in candidates {
            let mut tx = self.pool.begin().await?;
            let updated = sqlx::query(
                "UPDATE spot_withdrawals SET status='expired'
                  WHERE id=$1 AND status='signed'"
            ).bind(id).execute(&mut *tx).await?.rows_affected();

            if updated == 1 {
                sqlx::query(
                    "UPDATE spot_balances
                        SET frozen = frozen - $1, available = available + $1, updated_at=NOW()
                      WHERE user_address = $2 AND token = $3"
                )
                .bind(amount).bind(&user).bind(&token)
                .execute(&mut *tx).await?;

                tracing::warn!(
                    id = %id, user = %user, token = %token, amount = %amount,
                    "spot withdrawal expired by reaper, freeze reversed"
                );
            }
            tx.commit().await?;
        }
        Ok(())
    }
}
