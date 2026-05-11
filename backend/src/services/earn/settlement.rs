//! Earn Settlement Scheduler
//!
//! Handles automatic status transitions and settlement triggering for earn products.
//! Now includes automatic on-chain contract calls for state transitions.

use chrono::Utc;
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tracing::Instrument;

use super::{EarnService, EarnProduct};

impl EarnService {
    /// Start the settlement scheduler
    /// Runs every minute to check for:
    /// 1. Products that should transition to subscribing (subscribe_start_time reached)
    /// 2. Products that should transition to active (subscribe_end_time reached)
    /// 3. Products that should be settled (settle_time reached)
    pub async fn start_settlement_scheduler(self: Arc<Self>) {
        let service = self.clone();

        tokio::spawn(async move {
            tracing::info!("Earn settlement scheduler started (interval: 60s, on-chain calls: {})",
                service.has_contract());

            let mut consecutive_errors = 0u32;

            loop {
                match service.check_and_process_products().await {
                    Ok(processed) => {
                        if processed > 0 {
                            tracing::info!("Earn scheduler: processed {} product(s)", processed);
                        }
                        consecutive_errors = 0;
                    }
                    Err(e) => {
                        consecutive_errors += 1;
                        if consecutive_errors <= 3 {
                            tracing::error!("Earn scheduler error: {}", e);
                        } else {
                            tracing::debug!("Earn scheduler error (suppressed): {}", e);
                        }
                    }
                }

                sleep(Duration::from_secs(60)).await;
            }
        }.instrument(tracing::info_span!("earn-settlement")));
    }

    /// Check and process products based on their status and time
    async fn check_and_process_products(&self) -> anyhow::Result<u32> {
        let now = Utc::now();
        let mut processed = 0;

        // 1. Created -> Subscribing (when subscribe_start_time is reached)
        let products_to_open: Vec<EarnProduct> = sqlx::query_as(
            r#"
            SELECT * FROM earn_products
            WHERE status = 'created'
            AND subscribe_start_time <= $1
            "#
        )
        .bind(now)
        .fetch_all(&self.pool)
        .await?;

        for product in products_to_open {
            tracing::info!(
                "Opening subscription for product: {} (chain_id={})",
                product.name, product.chain_product_id
            );

            // MUST call on-chain openPlan before updating DB
            match self.call_open_plan(product.chain_product_id).await {
                Ok(tx_hash) => {
                    tracing::info!(
                        "On-chain openPlan success for plan {}: {:?}",
                        product.chain_product_id, tx_hash
                    );
                }
                Err(e) => {
                    tracing::error!(
                        "On-chain openPlan failed for plan {}: {} (will retry next cycle)",
                        product.chain_product_id, e
                    );
                    // Skip DB update - must not set subscribing without on-chain sync
                    continue;
                }
            }

            // Update database status only after on-chain success
            sqlx::query(
                "UPDATE earn_products SET status = 'subscribing', updated_at = NOW() WHERE id = $1"
            )
            .bind(product.id)
            .execute(&self.pool)
            .await?;

            processed += 1;
        }

        // 2. Subscribing -> Active (when subscribe_end_time is reached)
        let products_to_activate: Vec<EarnProduct> = sqlx::query_as(
            r#"
            SELECT * FROM earn_products
            WHERE status = 'subscribing'
            AND subscribe_end_time <= $1
            "#
        )
        .bind(now)
        .fetch_all(&self.pool)
        .await?;

        for product in products_to_activate {
            tracing::info!(
                "Activating product (subscription closed): {} (chain_id={})",
                product.name, product.chain_product_id
            );

            // Call on-chain activatePlan
            if self.has_contract() {
                match self.call_activate_plan(product.chain_product_id).await {
                    Ok(tx_hash) => {
                        tracing::info!(
                            "On-chain activatePlan success for plan {}: {:?}",
                            product.chain_product_id, tx_hash
                        );
                    }
                    Err(e) => {
                        tracing::error!(
                            "On-chain activatePlan failed for plan {}: {}",
                            product.chain_product_id, e
                        );
                        // Skip DB update if on-chain call fails
                        continue;
                    }
                }
            }

            // Update database status
            sqlx::query(
                "UPDATE earn_products SET status = 'active', updated_at = NOW() WHERE id = $1"
            )
            .bind(product.id)
            .execute(&self.pool)
            .await?;

            // Update all subscriptions to active status
            sqlx::query(
                r#"
                UPDATE earn_subscriptions SET nft_status = 'active'
                WHERE product_id = $1 AND nft_status = 'created'
                "#
            )
            .bind(product.id)
            .execute(&self.pool)
            .await?;

            processed += 1;
        }

        // 3. Active -> Settled (when settle_time is reached)
        let products_to_settle: Vec<EarnProduct> = sqlx::query_as(
            r#"
            SELECT * FROM earn_products
            WHERE status = 'active'
            AND settle_time <= $1
            "#
        )
        .bind(now)
        .fetch_all(&self.pool)
        .await?;

        for product in products_to_settle {
            tracing::info!(
                "Product ready for settlement: {} (chain_id={})",
                product.name, product.chain_product_id
            );

            if self.has_contract() {
                // First check if product can be auto-settled
                // (principalSentToTreasury=true requires returnsFunded=true)
                match self.can_auto_settle(product.chain_product_id).await {
                    Ok((true, _)) => {
                        // Can settle, proceed
                    }
                    Ok((false, reason)) => {
                        // Cannot auto-settle yet (waiting for fundPlanReturns)
                        tracing::warn!(
                            "Product {} cannot be auto-settled yet: {}",
                            product.chain_product_id, reason
                        );
                        // Don't mark as settled, just skip and retry next cycle
                        continue;
                    }
                    Err(e) => {
                        tracing::error!(
                            "Failed to check settlement readiness for product {}: {}",
                            product.chain_product_id, e
                        );
                        continue;
                    }
                }

                // Call on-chain closePlan
                match self.call_close_plan(product.chain_product_id).await {
                    Ok(tx_hash) => {
                        tracing::info!(
                            "On-chain closePlan success for plan {}: {:?}",
                            product.chain_product_id, tx_hash
                        );

                        // Update database status to settled (users can now claim)
                        // Status will change to 'ended' after all users have claimed
                        sqlx::query(
                            "UPDATE earn_products SET status = 'settled', updated_at = NOW() WHERE id = $1"
                        )
                        .bind(product.id)
                        .execute(&self.pool)
                        .await?;

                        // Update all subscriptions to matured (ready for claim)
                        sqlx::query(
                            r#"
                            UPDATE earn_subscriptions SET
                                nft_status = 'matured',
                                settled_at = NOW(),
                                actual_return = expected_return
                            WHERE product_id = $1
                            "#
                        )
                        .bind(product.id)
                        .execute(&self.pool)
                        .await?;

                        processed += 1;
                    }
                    Err(e) => {
                        let err_str = e.to_string();

                        // Check if this is a "waiting for fundPlanReturns" error
                        if err_str.contains("fundPlanReturns") || err_str.contains("principal was sent to treasury") {
                            tracing::warn!(
                                "Plan {} waiting for fundPlanReturns, will retry later",
                                product.chain_product_id
                            );
                            // Don't mark as settled, just skip
                            continue;
                        }

                        tracing::error!(
                            "On-chain closePlan failed for plan {}: {} (will retry next cycle)",
                            product.chain_product_id, e
                        );
                        // Do NOT mark as settled - keep as active so scheduler retries
                        continue;
                    }
                }
            } else {
                // Database-only mode: directly close plan
                self.close_plan_db_only(&product).await?;
                processed += 1;
            }
        }

        Ok(processed)
    }

    /// Close plan in database-only mode (no smart contract)
    async fn close_plan_db_only(&self, product: &EarnProduct) -> anyhow::Result<()> {
        // Calculate total interest
        let stats: (rust_decimal::Decimal, rust_decimal::Decimal, i64) = sqlx::query_as(
            r#"
            SELECT
                COALESCE(SUM(amount), 0),
                COALESCE(SUM(expected_return), 0),
                COUNT(*)
            FROM earn_subscriptions
            WHERE product_id = $1
            "#
        )
        .bind(product.id)
        .fetch_one(&self.pool)
        .await?;

        let total_principal = stats.0;
        let total_interest = stats.1;
        let settled_count = stats.2 as i32;

        // Create settlement record
        sqlx::query(
            r#"
            INSERT INTO earn_settlements
            (id, product_id, chain_product_id, total_principal, total_interest, settled_count)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#
        )
        .bind(uuid::Uuid::new_v4())
        .bind(product.id)
        .bind(product.chain_product_id)
        .bind(total_principal)
        .bind(total_interest)
        .bind(settled_count)
        .execute(&self.pool)
        .await?;

        // Update product status to settled (users can now claim)
        sqlx::query(
            r#"
            UPDATE earn_products SET
                status = 'settled',
                total_interest_paid = $1,
                updated_at = NOW()
            WHERE id = $2
            "#
        )
        .bind(total_interest)
        .bind(product.id)
        .execute(&self.pool)
        .await?;

        // Update all subscriptions to matured (ready for claim)
        sqlx::query(
            r#"
            UPDATE earn_subscriptions SET
                nft_status = 'matured',
                settled_at = NOW(),
                actual_return = expected_return
            WHERE product_id = $1
            "#
        )
        .bind(product.id)
        .execute(&self.pool)
        .await?;

        tracing::info!(
            "Product {} settled (DB only): principal={}, interest={}, subscribers={}",
            product.chain_product_id, total_principal, total_interest, settled_count
        );

        Ok(())
    }
}

/// Start settlement scheduler as a standalone function (for use in main.rs)
pub async fn start_scheduler(service: Arc<EarnService>) {
    service.start_settlement_scheduler().await;
}
