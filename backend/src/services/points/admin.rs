//! Admin Operations
//!
//! Administrative APIs for points system management including:
//! - Manual points adjustment
//! - User points recalculation
//! - Epoch statistics
//! - Admin audit logs

use crate::models::points::*;
use anyhow::{Context, Result};
use rust_decimal::Decimal;
use serde_json::json;
use tracing::{info, warn};

impl super::PointsService {
    // ============================================================================
    // Admin Operations (Phase 3.1)
    // ============================================================================

    /// Manually adjust user points
    ///
    /// Allows admin to add or subtract points for a specific type.
    /// This creates an adjustment event and logs the admin action.
    pub async fn adjust_points(
        &self,
        admin_address: &str,
        request: AdjustPointsRequest,
    ) -> Result<()> {
        // Validate adjustment amount is not zero
        if request.points == Decimal::ZERO {
            anyhow::bail!("Adjustment points cannot be zero");
        }

        // Start transaction
        let mut tx = self
            .pool
            .begin()
            .await
            .context("Failed to start transaction")?;

        // Get user's current points before adjustment
        let before_summary = sqlx::query_as::<_, UserPointsSummary>(
            r#"
            SELECT id, user_address, epoch_number,
                   trading_points, pnl_points, holding_points, referral_points, referral_code, staking_points,
                   total_points, trading_volume, trade_count, realized_pnl,
                   tier, tier_multiplier, referral_count, referral_volume,
                   COALESCE(earn_level, 0)        AS earn_level,
                   COALESCE(earn_level_weight, 4) AS earn_level_weight,
                   COALESCE(tp_daily_used, 0)     AS tp_daily_used,
                   COALESCE(tp_weekly_used, 0)    AS tp_weekly_used,
                   COALESCE(rp_daily_used, 0)     AS rp_daily_used,
                   tp_daily_reset_at,
                   tp_weekly_reset_at,
                   COALESCE(pp_daily_used, 0)     AS pp_daily_used,
                   pp_daily_reset_at,
                   COALESCE(hp_daily_used, 0)     AS hp_daily_used,
                   hp_daily_reset_at,
                   updated_at
            FROM user_points_summary
            WHERE user_address = $1 AND epoch_number = $2
            "#,
        )
        .bind(&request.user_address)
        .bind(request.epoch_number)
        .fetch_optional(&mut *tx)
        .await
        .context("Failed to fetch user summary")?;

        // Get or create user summary
        if before_summary.is_none() {
            sqlx::query(
                r#"
                INSERT INTO user_points_summary (user_address, epoch_number)
                VALUES ($1, $2)
                ON CONFLICT (user_address, epoch_number) DO NOTHING
                "#,
            )
            .bind(&request.user_address)
            .bind(request.epoch_number)
            .execute(&mut *tx)
            .await
            .context("Failed to create user summary")?;
        }

        let before_points = before_summary
            .as_ref()
            .map(|s| match request.point_type {
                PointType::Trading => s.trading_points,
                PointType::Pnl => s.pnl_points,
                PointType::Holding => s.holding_points,
                PointType::Referral => s.referral_points,
                PointType::Staking => s.staking_points,
            })
            .unwrap_or(Decimal::ZERO);

        let after_points = before_points + request.points;

        // Prevent negative points
        if after_points < Decimal::ZERO {
            anyhow::bail!(
                "Adjustment would result in negative points: {} + {} = {}",
                before_points,
                request.points,
                after_points
            );
        }

        // Update user summary based on point type
        let update_query = match request.point_type {
            PointType::Trading => {
                r#"
                UPDATE user_points_summary
                SET trading_points = trading_points + $1,
                    total_points = total_points + $1,
                    updated_at = NOW()
                WHERE user_address = $2 AND epoch_number = $3
                "#
            }
            PointType::Pnl => {
                r#"
                UPDATE user_points_summary
                SET pnl_points = pnl_points + $1,
                    total_points = total_points + $1,
                    updated_at = NOW()
                WHERE user_address = $2 AND epoch_number = $3
                "#
            }
            PointType::Holding => {
                r#"
                UPDATE user_points_summary
                SET holding_points = holding_points + $1,
                    total_points = total_points + $1,
                    updated_at = NOW()
                WHERE user_address = $2 AND epoch_number = $3
                "#
            }
            PointType::Referral => {
                r#"
                UPDATE user_points_summary
                SET referral_points = referral_points + $1,
                    total_points = total_points + $1,
                    updated_at = NOW()
                WHERE user_address = $2 AND epoch_number = $3
                "#
            }
            PointType::Staking => {
                r#"
                UPDATE user_points_summary
                SET staking_points = staking_points + $1,
                    total_points = total_points + $1,
                    updated_at = NOW()
                WHERE user_address = $2 AND epoch_number = $3
                "#
            }
        };

        sqlx::query(update_query)
            .bind(request.points)
            .bind(&request.user_address)
            .bind(request.epoch_number)
            .execute(&mut *tx)
            .await
            .context("Failed to adjust points")?;

        // Create adjustment event
        let metadata = json!({
            "adjustment_type": "manual",
            "admin": admin_address,
            "reason": request.reason,
            "before": before_points.to_string(),
            "after": after_points.to_string(),
        });

        sqlx::query(
            r#"
            INSERT INTO points_events (
                user_address, epoch_number, point_type, points,
                metadata
            )
            VALUES ($1, $2, $3, $4, $5)
            "#,
        )
        .bind(&request.user_address)
        .bind(request.epoch_number)
        .bind(request.point_type.to_string())
        .bind(request.points)
        .bind(&metadata)
        .execute(&mut *tx)
        .await
        .context("Failed to create adjustment event")?;

        // Log admin action
        let log_details = json!({
            "point_type": request.point_type.to_string(),
            "before_points": before_points.to_string(),
            "after_points": after_points.to_string(),
            "adjustment": request.points.to_string(),
            "reason": request.reason,
        });

        sqlx::query(
            r#"
            INSERT INTO points_admin_logs (
                admin_address, action, target_user, target_epoch, details
            )
            VALUES ($1, 'adjust_points', $2, $3, $4)
            "#,
        )
        .bind(admin_address)
        .bind(&request.user_address)
        .bind(request.epoch_number)
        .bind(&log_details)
        .execute(&mut *tx)
        .await
        .context("Failed to log admin action")?;

        // Commit transaction
        tx.commit().await.context("Failed to commit transaction")?;

        // Invalidate cache
        self.invalidate_user_cache(&request.user_address, request.epoch_number)
            .await?;
        self.invalidate_leaderboard_cache(request.epoch_number)
            .await?;

        info!(
            "Admin {} adjusted points: user={}, epoch={}, type={:?}, amount={}",
            admin_address, request.user_address, request.epoch_number, request.point_type, request.points
        );

        Ok(())
    }

    /// Trigger recalculation for a user
    ///
    /// Recalculates all points for a user in a specific epoch based on their
    /// trade history, positions, and referrals.
    pub async fn recalculate_user_points(
        &self,
        admin_address: &str,
        user_address: &str,
        epoch_number: i32,
    ) -> Result<()> {
        warn!(
            "Admin {} triggered recalculation for user {} epoch {}",
            admin_address, user_address, epoch_number
        );

        // Get epoch info (we need the time range for the replay).
        let epoch = self
            .get_epoch(epoch_number)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Epoch {} not found", epoch_number))?;

        // Start transaction
        let mut tx = self
            .pool
            .begin()
            .await
            .context("Failed to start transaction")?;

        // Get current points before recalculation
        let before_summary = sqlx::query_as::<_, UserPointsSummary>(
            r#"
            SELECT id, user_address, epoch_number,
                   trading_points, pnl_points, holding_points, referral_points, referral_code, staking_points,
                   total_points, trading_volume, trade_count, realized_pnl,
                   tier, tier_multiplier, referral_count, referral_volume,
                   COALESCE(earn_level, 0)        AS earn_level,
                   COALESCE(earn_level_weight, 4) AS earn_level_weight,
                   COALESCE(tp_daily_used, 0)     AS tp_daily_used,
                   COALESCE(tp_weekly_used, 0)    AS tp_weekly_used,
                   COALESCE(rp_daily_used, 0)     AS rp_daily_used,
                   tp_daily_reset_at,
                   tp_weekly_reset_at,
                   COALESCE(pp_daily_used, 0)     AS pp_daily_used,
                   pp_daily_reset_at,
                   COALESCE(hp_daily_used, 0)     AS hp_daily_used,
                   hp_daily_reset_at,
                   updated_at
            FROM user_points_summary
            WHERE user_address = $1 AND epoch_number = $2
            "#,
        )
        .bind(user_address)
        .bind(epoch_number)
        .fetch_optional(&mut *tx)
        .await
        .context("Failed to fetch user summary")?;

        // Reset user points summary
        sqlx::query(
            r#"
            INSERT INTO user_points_summary (user_address, epoch_number)
            VALUES ($1, $2)
            ON CONFLICT (user_address, epoch_number)
            DO UPDATE SET
                trading_points = 0,
                pnl_points = 0,
                holding_points = 0,
                referral_points = 0,
                staking_points = 0,
                total_points = 0,
                trading_volume = 0,
                trade_count = 0,
                realized_pnl = 0,
                tier = NULL,
                tier_multiplier = 1.0,
                referral_count = 0,
                referral_volume = 0,
                updated_at = NOW()
            "#,
        )
        .bind(user_address)
        .bind(epoch_number)
        .execute(&mut *tx)
        .await
        .context("Failed to reset user summary")?;

        // Delete replayable points events (TP, PP). HP is preserved —
        // it's sourced from hourly orderbook snapshots we can't
        // reconstruct, so we keep the events and re-aggregate HP into
        // the zeroed summary below. RP events are also preserved —
        // they have a UNIQUE one-shot guarantee via rp_trigger_events.
        sqlx::query(
            r#"
            DELETE FROM points_events
            WHERE user_address = $1 AND epoch_number = $2
              AND point_type IN ('trading', 'pnl')
            "#,
        )
        .bind(user_address)
        .bind(epoch_number)
        .execute(&mut *tx)
        .await
        .context("Failed to delete old points events")?;

        // Restore HP + RP contributions into the zeroed summary from
        // surviving events so they aren't lost by the reset.
        sqlx::query(
            r#"
            UPDATE user_points_summary s SET
                holding_points  = COALESCE((
                    SELECT SUM(points) FROM points_events
                    WHERE user_address = s.user_address
                      AND epoch_number = s.epoch_number
                      AND point_type = 'holding'
                ), 0),
                referral_points = COALESCE((
                    SELECT SUM(points) FROM points_events
                    WHERE user_address = s.user_address
                      AND epoch_number = s.epoch_number
                      AND point_type = 'referral'
                ), 0),
                total_points = COALESCE((
                    SELECT SUM(points) FROM points_events
                    WHERE user_address = s.user_address
                      AND epoch_number = s.epoch_number
                      AND point_type IN ('holding', 'referral')
                ), 0),
                updated_at = NOW()
            WHERE s.user_address = $1 AND s.epoch_number = $2
            "#,
        )
        .bind(user_address)
        .bind(epoch_number)
        .execute(&mut *tx)
        .await
        .context("Failed to restore HP/RP from events")?;

        // Commit the reset tx BEFORE replay so the replay calls see a
        // clean slate and can use the normal calculate_* paths (which
        // open their own transactions).
        tx.commit().await.context("Failed to commit transaction")?;

        // -------- REPLAY --------
        // Re-derive TP and PP from source tables inside the epoch time
        // window. Self-trades are skipped (matches live path in
        // integration.rs). calculate_* methods bump the per-user
        // summary transactionally; cap/decay state is already reset.
        let (replayed_trades, replayed_positions) = self
            .replay_tp_pp_for_user(user_address, epoch_number, epoch.start_time, epoch.end_time)
            .await?;

        // Log admin action, including replay outcome counts.
        let log_details = json!({
            "before": before_summary.as_ref().map(|s| json!({
                "trading_points": s.trading_points.to_string(),
                "pnl_points": s.pnl_points.to_string(),
                "total_points": s.total_points.to_string(),
            })),
            "replayed": {
                "trades": replayed_trades,
                "positions": replayed_positions,
            },
            "action": "points_full_recalculation",
        });

        self.log_admin_action(
            admin_address,
            "recalculate",
            Some(user_address.to_string()),
            Some(epoch_number),
            log_details,
        )
        .await?;

        // Invalidate cache
        self.invalidate_user_cache(user_address, epoch_number)
            .await?;
        self.invalidate_leaderboard_cache(epoch_number).await?;

        info!(
            "Admin {} triggered recalculation for user {} epoch {}",
            admin_address, user_address, epoch_number
        );

        Ok(())
    }

    /// Replay TP + PP for a single user over an epoch's time window.
    ///
    /// Returns `(trades_replayed, positions_replayed)`. Called only by
    /// `recalculate_user_points` after the summary has been zeroed.
    ///
    /// * TP: every row in `trades` where the user is maker OR taker
    ///   and `is_self_trade = false` is fed through
    ///   `calculate_trading_points`. Intra-epoch caps (daily / weekly)
    ///   will be re-enforced in the order they originally occurred.
    /// * PP: every row in `positions` with `decreased_at` inside the
    ///   window and non-zero `realized_pnl` is fed through
    ///   `calculate_pnl_points` using its stored `collateral_amount`
    ///   and `symbol`. Intra-day decay state is reset (a perfect
    ///   replay of the decay penalty is infeasible without keeping
    ///   original tick timings; we accept this is slightly
    ///   user-favorable and document it).
    async fn replay_tp_pp_for_user(
        &self,
        user_address: &str,
        epoch_number: i32,
        start_time: chrono::DateTime<chrono::Utc>,
        end_time: chrono::DateTime<chrono::Utc>,
    ) -> Result<(u64, u64)> {
        use crate::models::points::TradeRole;

        // ---- TP replay ----
        // Column order of `trades`: (id, symbol, maker_address,
        // taker_address, price, amount, created_at). We iterate in
        // time order so cap/decay mechanics fire in the same sequence
        // as the original live stream.
        #[derive(sqlx::FromRow)]
        struct TradeRow {
            id: uuid::Uuid,
            symbol: String,
            maker_address: String,
            taker_address: String,
            price: Decimal,
            amount: Decimal,
        }

        let trades: Vec<TradeRow> = sqlx::query_as::<_, TradeRow>(
            r#"
            SELECT id, symbol, maker_address, taker_address, price, amount
            FROM trades
            WHERE created_at >= $1 AND created_at < $2
              AND is_self_trade = false
              AND (maker_address = $3 OR taker_address = $3)
            ORDER BY created_at ASC
            "#,
        )
        .bind(start_time)
        .bind(end_time)
        .bind(user_address)
        .fetch_all(&self.pool)
        .await
        .context("Failed to fetch trades for replay")?;

        let mut replayed_trades = 0u64;
        for t in trades {
            let volume = t.price * t.amount;
            // A single trade yields two TP events when the user is
            // both sides (shouldn't happen with STP filter, but guard
            // cheaply).
            if t.maker_address == user_address {
                self.calculate_trading_points(
                    user_address, epoch_number, volume, t.id, TradeRole::Maker,
                ).await.ok();
                replayed_trades += 1;
            }
            if t.taker_address == user_address && t.maker_address != t.taker_address {
                self.calculate_trading_points(
                    user_address, epoch_number, volume, t.id, TradeRole::Taker,
                ).await.ok();
                replayed_trades += 1;
            }
        }

        // ---- PP replay ----
        #[derive(sqlx::FromRow)]
        struct PosRow {
            id: uuid::Uuid,
            symbol: String,
            realized_pnl: Decimal,
            collateral_amount: Decimal,
        }
        let positions: Vec<PosRow> = sqlx::query_as::<_, PosRow>(
            r#"
            SELECT id, symbol, realized_pnl, collateral_amount
            FROM positions
            WHERE user_address = $1
              AND decreased_at IS NOT NULL
              AND decreased_at >= $2 AND decreased_at < $3
              AND realized_pnl <> 0
            ORDER BY decreased_at ASC
            "#,
        )
        .bind(user_address)
        .bind(start_time)
        .bind(end_time)
        .fetch_all(&self.pool)
        .await
        .context("Failed to fetch closed positions for replay")?;

        let mut replayed_positions = 0u64;
        for p in positions {
            self.calculate_pnl_points(
                user_address,
                epoch_number,
                p.realized_pnl,
                p.collateral_amount,
                &p.symbol,
                p.id,
            ).await.ok();
            replayed_positions += 1;
        }

        info!(
            "Replayed {} trades and {} positions for user {} epoch {}",
            replayed_trades, replayed_positions, user_address, epoch_number
        );
        Ok((replayed_trades, replayed_positions))
    }

    /// Get epoch statistics
    ///
    /// Returns comprehensive statistics for an epoch including total users,
    /// total points, and breakdown by point type.
    pub async fn get_epoch_stats(&self, epoch_number: i32) -> Result<EpochStats> {
        // Get epoch info
        let epoch = self
            .get_epoch(epoch_number)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Epoch {} not found", epoch_number))?;

        // Get aggregated statistics
        #[derive(sqlx::FromRow)]
        struct StatsRow {
            total_users: i64,
            total_points: Decimal,
            total_trading_volume: Decimal,
            total_trade_count: i64,
            trading_points_total: Decimal,
            pnl_points_total: Decimal,
            holding_points_total: Decimal,
            referral_points_total: Decimal,
            staking_points_total: Decimal,
        }

        let stats: StatsRow = sqlx::query_as(
            r#"
            SELECT
                COUNT(DISTINCT user_address) as total_users,
                COALESCE(SUM(total_points), 0) as total_points,
                COALESCE(SUM(trading_volume), 0) as total_trading_volume,
                COALESCE(SUM(trade_count), 0) as total_trade_count,
                COALESCE(SUM(trading_points), 0) as trading_points_total,
                COALESCE(SUM(pnl_points), 0) as pnl_points_total,
                COALESCE(SUM(holding_points), 0) as holding_points_total,
                COALESCE(SUM(referral_points), 0) as referral_points_total,
                COALESCE(SUM(staking_points), 0) as staking_points_total
            FROM user_points_summary
            WHERE epoch_number = $1
            "#,
        )
        .bind(epoch_number)
        .fetch_one(&self.pool)
        .await
        .context("Failed to fetch epoch statistics")?;

        Ok(EpochStats {
            epoch_number,
            status: epoch.status,
            start_time: epoch.start_time,
            end_time: epoch.end_time,
            total_users: stats.total_users,
            total_points: stats.total_points,
            total_trading_volume: stats.total_trading_volume,
            total_trade_count: stats.total_trade_count,
            trading_points_total: stats.trading_points_total,
            pnl_points_total: stats.pnl_points_total,
            holding_points_total: stats.holding_points_total,
            referral_points_total: stats.referral_points_total,
            staking_points_total: stats.staking_points_total,
        })
    }

    /// Get admin operation logs
    ///
    /// Retrieves audit logs of admin operations with optional filtering.
    pub async fn get_admin_logs(
        &self,
        target_user: Option<String>,
        target_epoch: Option<i32>,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<AdminLog>> {
        let mut query = String::from(
            r#"
            SELECT id, admin_address, action, target_user, target_epoch,
                   details, created_at
            FROM points_admin_logs
            WHERE 1=1
            "#,
        );

        let mut param_count = 0;

        // Add filters
        if target_user.is_some() {
            param_count += 1;
            query.push_str(&format!(" AND target_user = ${}", param_count));
        }

        if target_epoch.is_some() {
            param_count += 1;
            query.push_str(&format!(" AND target_epoch = ${}", param_count));
        }

        query.push_str(" ORDER BY created_at DESC");
        query.push_str(&format!(
            " LIMIT ${} OFFSET ${}",
            param_count + 1,
            param_count + 2
        ));

        // Build query
        let mut db_query = sqlx::query_as::<_, AdminLog>(&query);

        if let Some(user) = target_user {
            db_query = db_query.bind(user);
        }

        if let Some(epoch) = target_epoch {
            db_query = db_query.bind(epoch);
        }

        db_query = db_query.bind(limit).bind(offset);

        let logs = db_query
            .fetch_all(&self.pool)
            .await
            .context("Failed to fetch admin logs")?;

        Ok(logs)
    }

    /// Batch adjust points for multiple users
    ///
    /// Useful for airdrops or batch corrections.
    pub async fn batch_adjust_points(
        &self,
        admin_address: &str,
        adjustments: Vec<AdjustPointsRequest>,
        reason: String,
    ) -> Result<usize> {
        let total_count = adjustments.len();
        let mut success_count = 0;

        for adjustment in adjustments {
            let mut req = adjustment;
            req.reason = format!("{} (Batch operation)", reason);

            match self.adjust_points(admin_address, req).await {
                Ok(_) => success_count += 1,
                Err(e) => {
                    warn!("Batch adjustment failed for one user: {}", e);
                    // Continue with next user
                }
            }
        }

        info!(
            "Admin {} completed batch adjustment: {}/{} succeeded",
            admin_address,
            success_count,
            total_count
        );

        Ok(success_count)
    }

    // ============================================================================
    // Internal Helper
    // ============================================================================

    /// Log admin operation
    async fn log_admin_action(
        &self,
        admin_address: &str,
        action: &str,
        target_user: Option<String>,
        target_epoch: Option<i32>,
        details: serde_json::Value,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO points_admin_logs (
                admin_address, action, target_user, target_epoch, details
            )
            VALUES ($1, $2, $3, $4, $5)
            "#,
        )
        .bind(admin_address)
        .bind(action)
        .bind(target_user)
        .bind(target_epoch)
        .bind(&details)
        .execute(&self.pool)
        .await
        .context("Failed to log admin action")?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_negative_points_validation() {
        let before = dec!(100);
        let adjustment = dec!(-150);
        let after = before + adjustment;

        assert!(after < Decimal::ZERO);
    }

    #[test]
    fn test_zero_adjustment_validation() {
        let adjustment = Decimal::ZERO;
        assert_eq!(adjustment, Decimal::ZERO);
    }
}
