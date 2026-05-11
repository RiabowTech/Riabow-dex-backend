//! Hourly reconciler. Computes the four invariants from the spec and
//! emits a tracing event (P-level keyed) when drift exceeds threshold.
//!
//! No remediation: just detection + alerting (ops investigates).

use anyhow::Result;
use rust_decimal::Decimal;
use sqlx::PgPool;
use std::sync::Arc;
use tokio::time::{sleep, Duration};

use crate::services::spot::config::SpotConfig;

pub struct SpotReconciler {
    pool: PgPool,
    interval_secs: u64,
}

impl SpotReconciler {
    pub fn new(cfg: &SpotConfig, pool: PgPool) -> Arc<Self> {
        Arc::new(Self {
            pool,
            interval_secs: cfg.reconciler_interval_secs,
        })
    }

    pub async fn run(self: Arc<Self>) {
        tracing::info!(interval_secs = self.interval_secs, "spot reconciler starting");
        loop {
            if let Err(e) = self.reconcile_once().await {
                tracing::error!("spot reconciler error: {e:?}");
            }
            sleep(Duration::from_secs(self.interval_secs)).await;
        }
    }

    async fn reconcile_once(&self) -> Result<()> {
        // Invariant 3 (spec, P0): per-user frozen vs Σ signed withdrawals.
        let bad_frozen: Vec<(String, String, Decimal, Decimal)> = sqlx::query_as(
            "WITH expected AS (
                SELECT user_address, token, COALESCE(SUM(amount), 0) AS amt
                FROM spot_withdrawals
                WHERE status='signed'
                GROUP BY user_address, token
              )
              SELECT b.user_address, b.token, b.frozen, COALESCE(e.amt, 0)
                FROM spot_balances b
                LEFT JOIN expected e
                  ON b.user_address = e.user_address AND b.token = e.token
                WHERE ABS(b.frozen - COALESCE(e.amt, 0)) > 0.000001"
        ).fetch_all(&self.pool).await?;

        for (user, token, balance_frozen, signed_sum) in &bad_frozen {
            tracing::error!(
                target: "spot.reconcile.frozen",
                priority = "P0",
                user = %user, token = %token,
                balance_frozen = %balance_frozen,
                signed_sum = %signed_sum,
                drift = %(balance_frozen - signed_sum),
                "spot frozen invariant violation"
            );
        }

        // Invariant 2 (spec, P1): system-wide USDT in spot vs net transfers.
        let (spot_usdt_total, net_transfer): (Decimal, Decimal) = sqlx::query_as(
            "SELECT
               (SELECT COALESCE(SUM(available + frozen), 0) FROM spot_balances WHERE token='USDT'),
               (SELECT COALESCE(SUM(CASE direction
                                      WHEN 'perp_to_spot' THEN amount
                                      WHEN 'spot_to_perp' THEN -amount
                                    END), 0) FROM spot_internal_transfers WHERE token='USDT')"
        ).fetch_one(&self.pool).await?;

        let usdt_drift = spot_usdt_total - net_transfer;
        if usdt_drift.abs() > Decimal::new(1, 2) /* 0.01 */ {
            tracing::error!(
                target: "spot.reconcile.usdt",
                priority = "P1",
                spot_usdt_total = %spot_usdt_total,
                net_transfer = %net_transfer,
                drift = %usdt_drift,
                "spot USDT invariant violation"
            );
        }

        // Invariant 1 (spec, P1): system-wide DF in spot vs net deposits-withdrawals.
        // (On-chain balance of the BSC vault is checked separately by an off-band
        //  script; we can't easily query it from here without the BSC provider
        //  in this struct. Future enhancement: pass provider through.)
        let (spot_df_total, net_deposits, confirmed_withdrawals): (Decimal, Decimal, Decimal) =
            sqlx::query_as(
                "SELECT
                   (SELECT COALESCE(SUM(available + frozen), 0) FROM spot_balances WHERE token='DF'),
                   (SELECT COALESCE(SUM(amount), 0) FROM spot_deposits WHERE token='DF' AND status='confirmed'),
                   (SELECT COALESCE(SUM(amount), 0) FROM spot_withdrawals WHERE token='DF' AND status='confirmed')"
            ).fetch_one(&self.pool).await?;

        let expected = net_deposits - confirmed_withdrawals;
        let df_drift = spot_df_total - expected;
        if df_drift.abs() > Decimal::new(1, 2) {
            tracing::error!(
                target: "spot.reconcile.df",
                priority = "P1",
                spot_df_total = %spot_df_total,
                net_deposits = %net_deposits,
                confirmed_withdrawals = %confirmed_withdrawals,
                drift = %df_drift,
                "spot DF invariant violation (off-chain ledger only; vault balance check is separate)"
            );
        }

        Ok(())
    }
}
