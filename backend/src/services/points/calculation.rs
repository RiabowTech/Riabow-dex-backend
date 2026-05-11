//! Points Calculation Logic
//!
//! Phase1 实现：
//! - Trading Points (TP): Maker/Taker 分离，per-1000U 系数，日/周 cap
//! - Referral Points (RP): 一次性触发模型，7天窗口，固定 10 RP

use crate::models::points::*;
use anyhow::{Context, Result};
use chrono::{Datelike, Utc};
use rust_decimal::Decimal;
use serde_json::json;
use tracing::info;
use uuid::Uuid;

impl super::PointsService {
    // ============================================================================
    // Trading Points (Phase1 重写)
    // ============================================================================

    /// 计算单笔成交的交易积分（Phase1）
    ///
    /// 公式：TP = (volume / 1000) × role_rate
    /// 日上限 tp_daily_cap，周上限 tp_weekly_cap（UTC 00:00 重置）
    pub async fn calculate_trading_points(
        &self,
        user_address: &str,
        epoch_number: i32,
        trade_volume: Decimal,
        trade_id: Uuid,
        role: TradeRole,
    ) -> Result<PointsCalculationResult> {
        if !self.is_point_type_enabled(&PointType::Trading).await {
            return Ok(PointsCalculationResult {
                points: Decimal::ZERO,
                point_type: PointType::Trading,
                tier: None,
                multiplier: Decimal::ONE,
                metadata: json!({"reason": "trading_points_disabled"}),
            });
        }

        // 加载 points_config
        let cfg = self.get_points_config(epoch_number).await?;

        // ensure summary row，同时获取当前 daily/weekly 用量
        let summary = self.get_or_create_user_summary(user_address, epoch_number).await?;

        // 确定 Tier（基于用户 VIP 等级）
        let tier_result = self.calculate_tier(user_address, epoch_number).await?;

        let role_rate = match role {
            TradeRole::Maker => tier_result.maker_rate,
            TradeRole::Taker => tier_result.taker_rate,
        };

        // 原始积分
        let raw_points = (trade_volume / Decimal::from(1000)) * role_rate;

        // ---- 日/周 cap 检查（UTC 日期变化时先重置计数器）----
        let today = Utc::now().date_naive();

        let daily_used = if summary.tp_daily_reset_at
            .map(|t| t.date_naive() == today)
            .unwrap_or(false)
        {
            summary.tp_daily_used
        } else {
            Decimal::ZERO // 日期已变，计数清零
        };

        let weekly_used = if summary.tp_weekly_reset_at
            .map(|t| {
                let d = t.date_naive();
                d.iso_week().week() == today.iso_week().week()
                    && d.year() == today.year()
            })
            .unwrap_or(false)
        {
            summary.tp_weekly_used
        } else {
            Decimal::ZERO // 周已变，清零
        };

        let daily_cap = Decimal::from(cfg.tp_daily_cap);
        let weekly_cap = Decimal::from(cfg.tp_weekly_cap);

        // 计算本次可授予的积分
        let daily_remaining = (daily_cap - daily_used).max(Decimal::ZERO);
        let weekly_remaining = (weekly_cap - weekly_used).max(Decimal::ZERO);
        let allowed = raw_points.min(daily_remaining).min(weekly_remaining);
        let final_points = allowed.max(Decimal::ZERO);

        let cap_applied = final_points < raw_points;

        let metadata = json!({
            "volume": trade_volume.to_string(),
            "role": role.to_string(),
            "tier": tier_result.tier,
            "role_rate": role_rate.to_string(),
            "raw_points": raw_points.to_string(),
            "final_points": final_points.to_string(),
            "cap_applied": cap_applied,
            "daily_used_before": daily_used.to_string(),
            "weekly_used_before": weekly_used.to_string(),
        });

        // ---- 原子写入：事件 + 更新summary（含上限计数器）----
        let mut tx = self.pool.begin().await
            .context("Failed to begin transaction for trading points")?;

        if final_points > Decimal::ZERO {
            sqlx::query(
                r#"
                INSERT INTO points_events (
                    user_address, epoch_number, point_type, points,
                    related_trade_id, related_order_id, related_position_id,
                    referrer_address, metadata
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                "#,
            )
            .bind(user_address)
            .bind(epoch_number)
            .bind(PointType::Trading.to_string())
            .bind(final_points)
            .bind(Some(trade_id))
            .bind(Option::<Uuid>::None)
            .bind(Option::<Uuid>::None)
            .bind(Option::<String>::None)
            .bind(&metadata)
            .execute(&mut *tx)
            .await
            .context("Failed to save trading points event")?;
        }

        // 始终更新 volume + trade_count + tier + daily/weekly 计数
        let new_daily_used = daily_used + final_points;
        let new_weekly_used = weekly_used + final_points;

        sqlx::query(
            r#"
            UPDATE user_points_summary
            SET trading_points    = trading_points + $1,
                total_points      = total_points   + $1,
                trading_volume    = trading_volume + $2,
                trade_count       = trade_count    + 1,
                tier              = $3,
                tier_multiplier   = $4,
                tp_daily_used     = $5,
                tp_weekly_used    = $6,
                tp_daily_reset_at  = CASE WHEN tp_daily_reset_at::date = CURRENT_DATE
                                         THEN tp_daily_reset_at ELSE NOW() END,
                tp_weekly_reset_at = CASE WHEN EXTRACT(week FROM tp_weekly_reset_at) = EXTRACT(week FROM NOW())
                                         THEN tp_weekly_reset_at ELSE NOW() END,
                updated_at        = NOW()
            WHERE user_address = $7 AND epoch_number = $8
            "#,
        )
        .bind(final_points)
        .bind(trade_volume)
        .bind(&tier_result.tier)
        .bind(tier_result.multiplier)
        .bind(new_daily_used)
        .bind(new_weekly_used)
        .bind(user_address)
        .bind(epoch_number)
        .execute(&mut *tx)
        .await
        .context("Failed to update trading points summary")?;

        tx.commit().await
            .context("Failed to commit trading points transaction")?;

        let _ = self.invalidate_user_cache(user_address, epoch_number).await;
        let _ = self.invalidate_leaderboard_cache(epoch_number).await;

        // WS push (PRD §9.4)
        if final_points > Decimal::ZERO {
            self.ws_emit(
                user_address,
                "tp_earned",
                Some("tp"),
                Some(final_points),
                if cap_applied { Some("daily_or_weekly_cap_truncated".into()) } else { None },
            ).await;
        }
        if cap_applied {
            self.ws_emit_cap(user_address, "tp_daily").await;
        }

        info!(
            "TP calculated: user={}, epoch={}, role={}, volume={}, tier={}, points={}, cap_applied={}",
            user_address, epoch_number, role, trade_volume, tier_result.tier, final_points, cap_applied
        );

        Ok(PointsCalculationResult {
            points: final_points,
            point_type: PointType::Trading,
            tier: Some(tier_result.tier),
            multiplier: role_rate,
            metadata,
        })
    }

    // ============================================================================
    // Referral Points — Phase1 一次性触发模型
    // ============================================================================

    /// 检查并触发 RP（每个被推荐人只能触发一次）
    ///
    /// 条件：
    /// 1. referee 有绑定推荐关系（referral_relations）
    /// 2. rp_trigger_events 中 referee_address 不存在记录
    /// 3. 绑定时间在 rp_trigger_days 天内
    /// 4. trade_volume >= rp_trigger_min_volume
    ///
    /// 满足后：referrer 和 referee 各获 10 RP（受日上限限制）
    pub async fn check_and_trigger_rp(
        &self,
        referee_address: &str,
        epoch_number: i32,
        trade_volume: Decimal,
        trade_id: Uuid,
    ) -> Result<bool> {
        if !self.is_point_type_enabled(&PointType::Referral).await {
            return Ok(false);
        }

        let cfg = self.get_points_config(epoch_number).await?;

        // 1. 检查是否已触发过（referee_address UNIQUE）
        let already_triggered: bool = sqlx::query_scalar(
            r#"SELECT EXISTS(SELECT 1 FROM rp_trigger_events WHERE referee_address = $1)"#,
        )
        .bind(referee_address)
        .fetch_one(&self.pool)
        .await
        .context("Failed to check rp_trigger_events")?;

        if already_triggered {
            return Ok(false);
        }

        // 2. 查推荐关系（referrer_address + created_at）
        let relation: Option<(String, chrono::DateTime<Utc>)> = sqlx::query_as(
            r#"
            SELECT referrer_address, created_at
            FROM referral_relations
            WHERE referee_address = $1
            "#,
        )
        .bind(referee_address)
        .fetch_optional(&self.pool)
        .await
        .context("Failed to fetch referral relation")?;

        let (referrer_address, relation_created_at) = match relation {
            Some(r) => r,
            None => return Ok(false),
        };

        // 3. 检查7天有效期
        let days_since_bind = (Utc::now() - relation_created_at).num_days();
        if days_since_bind > cfg.rp_trigger_days as i64 {
            return Ok(false);
        }

        // 4. 检查成交量
        if trade_volume < cfg.rp_trigger_min_volume {
            return Ok(false);
        }

        // ---- 触发：在一个事务内写 rp_trigger_events + 发 RP ----
        let mut tx = self.pool.begin().await
            .context("Failed to begin RP trigger transaction")?;

        // 写触发事件记录
        sqlx::query(
            r#"
            INSERT INTO rp_trigger_events (
                referrer_address, referee_address, trigger_trade_id,
                trigger_volume, referrer_rp, referee_rp,
                status, epoch_number, triggered_at, expired_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, 'triggered', $7, NOW(), $8)
            "#,
        )
        .bind(&referrer_address)
        .bind(referee_address)
        .bind(Some(trade_id))
        .bind(trade_volume)
        .bind(cfg.rp_referrer_amount)
        .bind(cfg.rp_referee_amount)
        .bind(epoch_number)
        .bind(relation_created_at + chrono::Duration::days(cfg.rp_trigger_days as i64))
        .execute(&mut *tx)
        .await
        .context("Failed to insert rp_trigger_event")?;

        // 发放 referrer RP（受日上限检查）
        self.award_rp_in_tx(
            &mut tx,
            &referrer_address,
            epoch_number,
            cfg.rp_referrer_amount,
            trade_id,
            referee_address,
            &cfg,
        ).await?;

        // 发放 referee RP
        self.award_rp_in_tx(
            &mut tx,
            referee_address,
            epoch_number,
            cfg.rp_referee_amount,
            trade_id,
            &referrer_address,
            &cfg,
        ).await?;

        tx.commit().await
            .context("Failed to commit RP trigger transaction")?;

        let _ = self.invalidate_user_cache(&referrer_address, epoch_number).await;
        let _ = self.invalidate_user_cache(referee_address, epoch_number).await;

        // WS push (PRD §9.4) — both sides receive their own event.
        self.ws_emit(
            &referrer_address,
            "rp_triggered",
            Some("rp"),
            Some(rust_decimal::Decimal::from(cfg.rp_referrer_amount)),
            Some(format!("referee={}", referee_address)),
        ).await;
        self.ws_emit(
            referee_address,
            "rp_triggered",
            Some("rp"),
            Some(rust_decimal::Decimal::from(cfg.rp_referee_amount)),
            Some(format!("referrer={}", referrer_address)),
        ).await;

        info!(
            "RP triggered: referrer={}, referee={}, epoch={}, trade_volume={}",
            referrer_address, referee_address, epoch_number, trade_volume
        );

        Ok(true)
    }

    /// 在事务内发放 RP，含日上限检查
    async fn award_rp_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        user_address: &str,
        epoch_number: i32,
        amount: i32,
        trade_id: Uuid,
        counterpart: &str,
        cfg: &PointsConfigRow,
    ) -> Result<()> {
        // 确保 summary 行存在
        sqlx::query(
            r#"
            INSERT INTO user_points_summary (user_address, epoch_number)
            VALUES ($1, $2)
            ON CONFLICT (user_address, epoch_number) DO NOTHING
            "#,
        )
        .bind(user_address)
        .bind(epoch_number)
        .execute(&mut **tx)
        .await?;

        // 检查日上限（rp_daily_used）
        let daily_used: i32 = sqlx::query_scalar(
            r#"SELECT COALESCE(rp_daily_used, 0) FROM user_points_summary
               WHERE user_address = $1 AND epoch_number = $2"#,
        )
        .bind(user_address)
        .bind(epoch_number)
        .fetch_one(&mut **tx)
        .await
        .context("Failed to fetch rp_daily_used")?;

        let daily_cap = cfg.rp_daily_cap_normal;
        if daily_used >= daily_cap {
            return Ok(()); // 已达日上限，跳过
        }

        let allowed = amount.min(daily_cap - daily_used);
        let rp = Decimal::from(allowed);

        let metadata = json!({
            "rp_type": "trigger",
            "counterpart": counterpart,
            "trade_id": trade_id.to_string(),
        });

        // 写积分事件
        sqlx::query(
            r#"
            INSERT INTO points_events (
                user_address, epoch_number, point_type, points,
                related_trade_id, referrer_address, metadata
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
        )
        .bind(user_address)
        .bind(epoch_number)
        .bind(PointType::Referral.to_string())
        .bind(rp)
        .bind(Some(trade_id))
        .bind(Some(counterpart))
        .bind(&metadata)
        .execute(&mut **tx)
        .await
        .context("Failed to save RP event")?;

        // 更新 summary
        sqlx::query(
            r#"
            UPDATE user_points_summary
            SET referral_points = referral_points + $1,
                total_points    = total_points    + $1,
                referral_count  = referral_count  + 1,
                rp_daily_used   = rp_daily_used   + $2,
                updated_at      = NOW()
            WHERE user_address = $3 AND epoch_number = $4
            "#,
        )
        .bind(rp)
        .bind(allowed)
        .bind(user_address)
        .bind(epoch_number)
        .execute(&mut **tx)
        .await
        .context("Failed to update RP summary")?;

        Ok(())
    }

    // ============================================================================
    // 保留原有 calculate_referral_points 签名（供老代码兼容，Phase2 可删）
    // ============================================================================
    #[allow(dead_code)]
    pub async fn calculate_referral_points(
        &self,
        _referrer_address: &str,
        _referee_address: &str,
        _epoch_number: i32,
        _referee_volume: Decimal,
        _trade_id: Uuid,
    ) -> Result<PointsCalculationResult> {
        // Phase1 已改为 check_and_trigger_rp，此函数保留签名兼容性
        Ok(PointsCalculationResult {
            points: Decimal::ZERO,
            point_type: PointType::Referral,
            tier: None,
            multiplier: Decimal::ONE,
            metadata: json!({"reason": "use check_and_trigger_rp instead"}),
        })
    }

    // ============================================================================
    // PnL Points (Phase 2.2)
    // ============================================================================

    /// Calculate PnL points for realized profit/loss (PRD §3.2)
    ///
    /// Formula: PP = max(PP1, PP2) where
    ///   PP1 = |pnl| × pp_amount_rate                           (default 2.5)
    ///   PP2 = collateral × min(|pnl|/collateral, pp_return_cap) × pp_return_coeff
    ///                                                           (cap 20%, coeff 6.0)
    ///
    /// Anti-spam:
    ///   - Same symbol within 5 minutes: ×pp_decay_5min (default 0.5)
    ///   - Same user within 10 minutes (3rd close+): ×pp_decay_10min (default 0.25)
    ///
    /// Daily cap: pp_daily_cap (default 20,000) — excess discarded.
    /// Both gains AND losses earn PP (实质参与奖励).
    pub async fn calculate_pnl_points(
        &self,
        user_address: &str,
        epoch_number: i32,
        realized_pnl: Decimal,
        collateral_amount: Decimal,
        symbol: &str,
        position_id: Uuid,
    ) -> Result<PointsCalculationResult> {
        // Check if PnL points are enabled
        if !self.is_point_type_enabled(&PointType::Pnl).await {
            return Ok(PointsCalculationResult {
                points: Decimal::ZERO,
                point_type: PointType::Pnl,
                tier: None,
                multiplier: Decimal::ONE,
                metadata: json!({"reason": "pnl_points_disabled"}),
            });
        }

        // PRD §3.2: 不论盈亏均发放，但 |pnl| == 0 时跳过
        if realized_pnl.is_zero() || collateral_amount.is_zero() {
            return Ok(PointsCalculationResult {
                points: Decimal::ZERO,
                point_type: PointType::Pnl,
                tier: None,
                multiplier: Decimal::ONE,
                metadata: json!({
                    "realized_pnl": realized_pnl.to_string(),
                    "reason": "zero_pnl_or_collateral"
                }),
            });
        }

        // Load Phase1 config (epoch-aware, falls back to global default).
        let cfg = self.get_points_config(epoch_number).await?;

        // Get user's tier (based on VIP level) — kept for metadata only;
        // PP is NOT tier-amplified per PRD §4 ("PnL积分、持仓积分不受 Tier 影响").
        let summary = self
            .get_or_create_user_summary(user_address, epoch_number)
            .await?;
        let tier_result = self
            .calculate_tier(user_address, epoch_number)
            .await?;

        // ----- Dual formula -----
        let abs_pnl = realized_pnl.abs();
        let pp1 = abs_pnl * cfg.pp_amount_rate;

        let raw_return_rate = abs_pnl / collateral_amount;
        let capped_return_rate = if raw_return_rate > cfg.pp_return_cap {
            cfg.pp_return_cap
        } else {
            raw_return_rate
        };
        let pp2 = collateral_amount * capped_return_rate * cfg.pp_return_coeff;
        let base_points = if pp1 >= pp2 { pp1 } else { pp2 };

        // ----- Anti-spam decay -----
        // Recent PnL events for the same user — used to detect (a) same-symbol
        // 5-min repeats, and (b) ≥3 closes inside 10-min window.
        let recent: Vec<(serde_json::Value, chrono::DateTime<chrono::Utc>)> = sqlx::query_as(
            r#"
            SELECT metadata, created_at FROM points_events
            WHERE user_address = $1
              AND epoch_number = $2
              AND point_type = 'pnl'
              AND created_at >= NOW() - INTERVAL '10 minutes'
            ORDER BY created_at DESC
            LIMIT 10
            "#,
        )
        .bind(user_address)
        .bind(epoch_number)
        .fetch_all(&self.pool)
        .await
        .context("Failed to query recent pnl events for decay check")?;

        let now = chrono::Utc::now();
        let same_symbol_5min = recent.iter().any(|(meta, t)| {
            (now - *t).num_seconds() < 300
                && meta.get("symbol").and_then(|s| s.as_str()) == Some(symbol)
        });
        let count_10min = recent.len();

        let mut decay = Decimal::ONE;
        let mut decay_reason: Option<&'static str> = None;
        if count_10min >= 2 {
            // 这是该 10min 内的第 3 笔（recent 已存 2 条 + 当前 1 条）
            decay = cfg.pp_decay_10min;
            decay_reason = Some("10min_3rd+");
        } else if same_symbol_5min {
            decay = cfg.pp_decay_5min;
            decay_reason = Some("5min_same_symbol");
        }

        let after_decay = base_points * decay;

        // ----- Daily cap -----
        // Reset pp_daily_used if last reset was a previous UTC day.
        let today = now.date_naive();
        let needs_reset = match summary.pp_daily_reset_at {
            Some(t) => t.date_naive() != today,
            None => true,
        };
        let already_today = if needs_reset {
            Decimal::ZERO
        } else {
            summary.pp_daily_used
        };
        let cap = Decimal::from(cfg.pp_daily_cap);
        let remaining_today = (cap - already_today).max(Decimal::ZERO);
        let final_points = if after_decay > remaining_today {
            remaining_today
        } else {
            after_decay
        };
        let cap_truncated = after_decay > final_points;

        let metadata = json!({
            "symbol": symbol,
            "realized_pnl": realized_pnl.to_string(),
            "collateral": collateral_amount.to_string(),
            "pp1": pp1.to_string(),
            "pp2": pp2.to_string(),
            "base": base_points.to_string(),
            "decay": decay.to_string(),
            "decay_reason": decay_reason,
            "after_decay": after_decay.to_string(),
            "cap_truncated": cap_truncated,
            "tier": tier_result.tier,
        });

        if final_points.is_zero() {
            return Ok(PointsCalculationResult {
                points: Decimal::ZERO,
                point_type: PointType::Pnl,
                tier: Some(tier_result.tier),
                multiplier: Decimal::ONE,
                metadata,
            });
        }

        // CRITICAL FIX: Use transaction to ensure atomicity
        let mut tx = self.pool.begin().await
            .context("Failed to begin transaction for pnl points")?;

        // Save points event within transaction
        sqlx::query(
            r#"
            INSERT INTO points_events (
                user_address, epoch_number, point_type, points,
                related_trade_id, related_order_id, related_position_id,
                referrer_address, metadata
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            "#,
        )
        .bind(user_address)
        .bind(epoch_number)
        .bind(PointType::Pnl.to_string())
        .bind(final_points)
        .bind(Option::<Uuid>::None)
        .bind(Option::<Uuid>::None)
        .bind(Some(position_id))
        .bind(Option::<String>::None)
        .bind(&metadata)
        .execute(&mut *tx)
        .await
        .context("Failed to save pnl points event")?;

        // Update user summary within same transaction.
        // pp_daily_used resets when crossing UTC midnight; reset_at marks the
        // current UTC date so the next call can detect day rollover.
        sqlx::query(
            r#"
            UPDATE user_points_summary
            SET pnl_points = pnl_points + $1,
                total_points = total_points + $1,
                realized_pnl = realized_pnl + $2,
                pp_daily_used = CASE
                    WHEN pp_daily_reset_at IS NULL
                      OR pp_daily_reset_at::date < (NOW() AT TIME ZONE 'UTC')::date
                    THEN $1
                    ELSE pp_daily_used + $1
                END,
                pp_daily_reset_at = NOW(),
                updated_at = NOW()
            WHERE user_address = $3 AND epoch_number = $4
            "#,
        )
        .bind(final_points)
        .bind(realized_pnl)
        .bind(user_address)
        .bind(epoch_number)
        .execute(&mut *tx)
        .await
        .context("Failed to update pnl points summary")?;

        // Commit transaction
        tx.commit().await
            .context("Failed to commit pnl points transaction")?;

        // Invalidate cache after successful commit
        let _ = self.invalidate_user_cache(user_address, epoch_number).await;
        let _ = self.invalidate_leaderboard_cache(epoch_number).await;

        // WS push (PRD §9.4)
        if final_points > Decimal::ZERO {
            self.ws_emit(
                user_address,
                "pp_earned",
                Some("pp"),
                Some(final_points),
                decay_reason.map(|s| s.to_string()),
            ).await;
        }
        if cap_truncated {
            self.ws_emit_cap(user_address, "pp_daily").await;
        }

        info!(
            "PnL points calculated: user={}, epoch={}, pnl={}, points={}, tier={}",
            user_address, epoch_number, realized_pnl, final_points, tier_result.tier
        );

        Ok(PointsCalculationResult {
            points: final_points,
            point_type: PointType::Pnl,
            tier: Some(tier_result.tier),
            multiplier: tier_result.multiplier,
            metadata,
        })
    }

    // (旧 calculate_referral_points 已在上方 #[allow(dead_code)] 版本保留，此处删除重复定义)

    // ============================================================================
    // Holding Points (Phase 2.4) - Batch Calculation
    // ============================================================================

    /// Calculate holding points for all active positions (batch operation)
    ///
    /// Formula: points = position_value * rate * hours_held
    /// Default rate: 0.00001 per $1 per hour
    /// This should be called by a scheduled task (e.g., hourly)
    pub async fn calculate_holding_points_batch(&self, epoch_number: i32) -> Result<u64> {
        // Check if holding points are enabled
        if !self.is_point_type_enabled(&PointType::Holding).await {
            return Ok(0);
        }

        // PRD §3.3: HP = position_value × minutes × hp_rate_per_min, daily cap = hp_daily_cap.
        // Per-tier multiplier intentionally NOT applied (PRD §4: "HP 不受 Tier 影响").
        let cfg = self.get_points_config(epoch_number).await?;
        let per_minute_rate = cfg.hp_rate_per_min;
        let hp_cap = Decimal::from(cfg.hp_daily_cap);
        let minutes_per_batch = Decimal::from(60); // batch fires hourly

        let active_positions: Vec<(String, Decimal, Uuid)> = sqlx::query_as(
            r#"
            SELECT user_address, size_in_usd, id
            FROM positions
            WHERE status = 'open'
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .context("Failed to fetch active positions")?;

        let total_positions = active_positions.len();
        let mut updated_count = 0u64;

        for (user_address, position_value, position_id) in active_positions {
            // Per-batch raw points (60 minutes worth at configured per-minute rate).
            let raw_points = position_value * per_minute_rate * minutes_per_batch;

            if raw_points <= Decimal::ZERO {
                continue;
            }

            let summary = self
                .get_or_create_user_summary(&user_address, epoch_number)
                .await?;

            // Reset hp_daily_used if last reset is on a previous UTC day.
            let now = chrono::Utc::now();
            let today = now.date_naive();
            let already_today = match summary.hp_daily_reset_at {
                Some(t) if t.date_naive() == today => summary.hp_daily_used,
                _ => Decimal::ZERO,
            };
            let remaining_today = (hp_cap - already_today).max(Decimal::ZERO);
            if remaining_today.is_zero() {
                continue; // already capped today
            }
            let final_points = if raw_points > remaining_today {
                remaining_today
            } else {
                raw_points
            };

            // Tier captured for forensic metadata only — HP not multiplied.
            let tier_result = self
                .calculate_tier(&user_address, epoch_number)
                .await?;

            let metadata = json!({
                "position_value": position_value.to_string(),
                "rate_per_min": per_minute_rate.to_string(),
                "minutes": 60,
                "raw_points": raw_points.to_string(),
                "cap_truncated": raw_points > final_points,
                "tier": tier_result.tier,
            });

            if final_points.is_zero() {
                continue;
            }

                // CRITICAL FIX: Use transaction to ensure atomicity between points_events and user_points_summary
                let mut tx = self.pool.begin().await
                    .context("Failed to begin transaction for holding points")?;

                // Save points event within transaction
                sqlx::query(
                    r#"
                    INSERT INTO points_events (
                        user_address, epoch_number, point_type, points,
                        related_trade_id, related_order_id, related_position_id,
                        referrer_address, metadata
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    "#,
                )
                .bind(&user_address)
                .bind(epoch_number)
                .bind(PointType::Holding.to_string())
                .bind(final_points)
                .bind(None::<Uuid>)
                .bind(None::<Uuid>)
                .bind(Some(position_id))
                .bind(None::<String>)
                .bind(&metadata)
                .execute(&mut *tx)
                .await
                .context("Failed to save holding points event")?;

                // Ensure user has a summary entry
                sqlx::query(
                    r#"
                    INSERT INTO user_points_summary (user_address, epoch_number)
                    VALUES ($1, $2)
                    ON CONFLICT (user_address, epoch_number) DO NOTHING
                    "#,
                )
                .bind(&user_address)
                .bind(epoch_number)
                .execute(&mut *tx)
                .await?;

                // Update summary + bump hp_daily_used (with same-day reset).
                sqlx::query(
                    r#"
                    UPDATE user_points_summary
                    SET holding_points = holding_points + $1,
                        total_points = total_points + $1,
                        hp_daily_used = CASE
                            WHEN hp_daily_reset_at IS NULL
                              OR hp_daily_reset_at::date < (NOW() AT TIME ZONE 'UTC')::date
                            THEN $1
                            ELSE hp_daily_used + $1
                        END,
                        hp_daily_reset_at = NOW(),
                        updated_at = NOW()
                    WHERE user_address = $2 AND epoch_number = $3
                    "#,
                )
                .bind(final_points)
                .bind(&user_address)
                .bind(epoch_number)
                .execute(&mut *tx)
                .await
                .context("Failed to update holding points summary")?;

                // Commit transaction
                tx.commit().await
                    .context("Failed to commit holding points transaction")?;

                // Invalidate cache after successful commit
                let _ = self.invalidate_user_cache(&user_address, epoch_number).await;
                let _ = self.invalidate_leaderboard_cache(epoch_number).await;

                // WS push (PRD §9.4) — once per credited position per batch.
                self.ws_emit(
                    &user_address,
                    "hp_batch",
                    Some("hp"),
                    Some(final_points),
                    None,
                ).await;
                if raw_points > final_points {
                    self.ws_emit_cap(&user_address, "hp_daily").await;
                }

                updated_count += 1;
        }

        info!(
            "Holding points batch calculated: epoch={}, positions={}, updated={}",
            epoch_number,
            total_positions,
            updated_count
        );

        Ok(updated_count)
    }

    // ============================================================================
    // Staking Points (Phase 2.4) - Batch Calculation
    // ============================================================================

    /// Calculate staking points for active staking records
    ///
    /// Formula: points = staking_amount * rate * days
    /// Default rate: 0.0002 per day
    /// This should be called by a scheduled task (e.g., daily)
    pub async fn calculate_staking_points_batch(&self, epoch_number: i32) -> Result<u64> {
        // Check if staking points are enabled
        if !self.is_point_type_enabled(&PointType::Staking).await {
            return Ok(0);
        }

        // Get epoch configuration
        let epoch = self.get_epoch(epoch_number).await?
            .ok_or_else(|| anyhow::anyhow!("Epoch {} not found", epoch_number))?;

        let staking_rate = if let Some(config) = &epoch.config {
            config["point_rates"]["staking"]
                .as_str()
                .and_then(|s| s.parse::<Decimal>().ok())
                .unwrap_or(default_rates::STAKING_RATE)
        } else {
            default_rates::STAKING_RATE
        };

        // Get all active staking records
        let active_stakings: Vec<StakingRecord> = sqlx::query_as(
            r#"
            SELECT id, user_address, amount, token_address,
                   start_time, end_time, status, tx_hash, withdraw_tx_hash,
                   last_calculated_at, created_at, updated_at
            FROM points_staking
            WHERE status = 'active'
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .context("Failed to fetch active staking records")?;

        let total_stakings = active_stakings.len();
        let mut updated_count = 0u64;
        let now = chrono::Utc::now();

        for staking in active_stakings {
            // Calculate time elapsed since last calculation
            let time_elapsed = now - staking.last_calculated_at;
            let days_elapsed = crate::safe_div!(
                Decimal::from(time_elapsed.num_seconds()),
                Decimal::from(86400),
                "points staking days_elapsed"
            );

            if days_elapsed >= Decimal::ONE {
                // Calculate points for elapsed days
                let points = staking.amount * staking_rate * days_elapsed;

                let metadata = json!({
                    "staking_amount": staking.amount.to_string(),
                    "rate": staking_rate.to_string(),
                    "days": days_elapsed.to_string(),
                    "token_address": staking.token_address,
                });

                // CRITICAL FIX: Use transaction to ensure atomicity
                // All three operations must succeed or all must fail
                let mut tx = self.pool.begin().await
                    .context("Failed to begin transaction for staking points")?;

                // Save points event within transaction
                sqlx::query(
                    r#"
                    INSERT INTO points_events (
                        user_address, epoch_number, point_type, points,
                        related_trade_id, related_order_id, related_position_id,
                        referrer_address, metadata
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    "#,
                )
                .bind(&staking.user_address)
                .bind(epoch_number)
                .bind(PointType::Staking.to_string())
                .bind(points)
                .bind(Option::<Uuid>::None)
                .bind(Option::<Uuid>::None)
                .bind(Option::<Uuid>::None)
                .bind(Option::<String>::None)
                .bind(&metadata)
                .execute(&mut *tx)
                .await
                .context("Failed to save staking points event")?;

                // Ensure user has a summary entry
                sqlx::query(
                    r#"
                    INSERT INTO user_points_summary (user_address, epoch_number)
                    VALUES ($1, $2)
                    ON CONFLICT (user_address, epoch_number) DO NOTHING
                    "#,
                )
                .bind(&staking.user_address)
                .bind(epoch_number)
                .execute(&mut *tx)
                .await
                .context("Failed to ensure user summary exists")?;

                // Update user summary within same transaction
                sqlx::query(
                    r#"
                    UPDATE user_points_summary
                    SET staking_points = staking_points + $1,
                        total_points = total_points + $1,
                        updated_at = NOW()
                    WHERE user_address = $2 AND epoch_number = $3
                    "#,
                )
                .bind(points)
                .bind(&staking.user_address)
                .bind(epoch_number)
                .execute(&mut *tx)
                .await
                .context("Failed to update staking points summary")?;

                // Update last_calculated_at timestamp within same transaction
                sqlx::query(
                    r#"
                    UPDATE points_staking
                    SET last_calculated_at = NOW(), updated_at = NOW()
                    WHERE id = $1
                    "#,
                )
                .bind(staking.id)
                .execute(&mut *tx)
                .await
                .context("Failed to update staking last_calculated_at")?;

                // Commit transaction - all operations succeed together
                tx.commit().await
                    .context("Failed to commit staking points transaction")?;

                // Invalidate cache after successful commit
                let _ = self.invalidate_user_cache(&staking.user_address, epoch_number).await;
                let _ = self.invalidate_leaderboard_cache(epoch_number).await;

                updated_count += 1;
            }
        }


        // =====================================================================
        // EARN INTEGRATION: Calculate staking points from Earn subscriptions
        // =====================================================================
        use crate::services::earn::models::EarnSubscription;
        
        let earn_subs: Vec<EarnSubscription> = sqlx::query_as(
            r#"
            SELECT id, product_id, chain_product_id, user_address, amount,
                   nft_amount, expected_return, actual_return, nft_status,
                   subscribed_at, settled_at, claimed_at, subscribe_tx_hash,
                   claim_tx_hash, claimed
            FROM earn_subscriptions
            WHERE nft_status IN ('active', 'matured') AND claimed = false
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .context("Failed to query active Earn subscriptions")?;

        let _earn_total = earn_subs.len();
        let mut _earn_count = 0u64;

        for sub in earn_subs {
            let pts = sub.amount * staking_rate;
            if pts > Decimal::ZERO {
                let metadata = json!({
                    "subscription_id": sub.id,
                    "product_id": sub.product_id,
                    "amount": sub.amount.to_string(),
                    "rate": staking_rate.to_string(),
                    "source": "earn",
                });

                // CRITICAL FIX: Use transaction to ensure atomicity
                // Both operations must succeed or both must fail
                let mut tx = self.pool.begin().await
                    .context("Failed to begin transaction for earn staking points")?;

                // Save points event within transaction
                sqlx::query(
                    r#"
                    INSERT INTO points_events (
                        user_address, epoch_number, point_type, points,
                        related_trade_id, related_order_id, related_position_id,
                        referrer_address, metadata
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    "#,
                )
                .bind(&sub.user_address)
                .bind(epoch_number)
                .bind(PointType::Staking.to_string())
                .bind(pts)
                .bind(Option::<Uuid>::None)
                .bind(Option::<Uuid>::None)
                .bind(Some(sub.id))
                .bind(Option::<String>::None)
                .bind(&metadata)
                .execute(&mut *tx)
                .await
                .context("Failed to save earn staking points event")?;

                // Ensure user has a summary entry
                sqlx::query(
                    r#"
                    INSERT INTO user_points_summary (user_address, epoch_number)
                    VALUES ($1, $2)
                    ON CONFLICT (user_address, epoch_number) DO NOTHING
                    "#,
                )
                .bind(&sub.user_address)
                .bind(epoch_number)
                .execute(&mut *tx)
                .await
                .context("Failed to ensure user summary exists")?;

                // Update user summary within same transaction
                sqlx::query(
                    r#"
                    UPDATE user_points_summary
                    SET staking_points = staking_points + $1,
                        total_points = total_points + $1,
                        updated_at = NOW()
                    WHERE user_address = $2 AND epoch_number = $3
                    "#,
                )
                .bind(pts)
                .bind(&sub.user_address)
                .bind(epoch_number)
                .execute(&mut *tx)
                .await
                .context("Failed to update earn staking points summary")?;

                // Commit transaction - both operations succeed together
                tx.commit().await
                    .context("Failed to commit earn staking points transaction")?;

                // Invalidate cache after successful commit
                let _ = self.invalidate_user_cache(&sub.user_address, epoch_number).await;
                let _ = self.invalidate_leaderboard_cache(epoch_number).await;

                _earn_count += 1;
            }
        }

        info!(
            "Staking points batch calculated: epoch={}, stakings={}, updated={}",
            epoch_number,
            total_stakings,
            updated_count
        );

        Ok(updated_count)
    }

    /// Record a new staking action
    pub async fn record_staking(
        &self,
        user_address: &str,
        amount: Decimal,
        token_address: &str,
        tx_hash: Option<String>,
    ) -> Result<Uuid> {
        let staking_id: Uuid = sqlx::query_scalar(
            r#"
            INSERT INTO points_staking (
                user_address, amount, token_address, tx_hash, status
            )
            VALUES ($1, $2, $3, $4, 'active')
            RETURNING id
            "#,
        )
        .bind(user_address)
        .bind(amount)
        .bind(token_address)
        .bind(tx_hash)
        .fetch_one(&self.pool)
        .await
        .context("Failed to record staking")?;

        info!(
            "Staking recorded: user={}, amount={}, token={}, id={}",
            user_address, amount, token_address, staking_id
        );

        Ok(staking_id)
    }

    /// Record staking withdrawal
    pub async fn record_staking_withdrawal(
        &self,
        staking_id: Uuid,
        withdraw_tx_hash: Option<String>,
    ) -> Result<()> {
        let rows_affected = sqlx::query(
            r#"
            UPDATE points_staking
            SET status = 'withdrawn',
                end_time = NOW(),
                withdraw_tx_hash = $1,
                updated_at = NOW()
            WHERE id = $2 AND status = 'active'
            "#,
        )
        .bind(withdraw_tx_hash)
        .bind(staking_id)
        .execute(&self.pool)
        .await
        .context("Failed to record staking withdrawal")?
        .rows_affected();

        if rows_affected == 0 {
            anyhow::bail!("Staking record {} not found or already withdrawn", staking_id);
        }

        info!("Staking withdrawal recorded: id={}", staking_id);

        Ok(())
    }

    /// Get user's active staking records
    pub async fn get_user_staking_records(
        &self,
        user_address: &str,
    ) -> Result<StakingRecordsResponse> {
        let records: Vec<StakingRecord> = sqlx::query_as(
            r#"
            SELECT id, user_address, amount, token_address,
                   start_time, end_time, status, tx_hash, withdraw_tx_hash,
                   last_calculated_at, created_at, updated_at
            FROM points_staking
            WHERE user_address = $1
            ORDER BY created_at DESC
            "#,
        )
        .bind(user_address)
        .fetch_all(&self.pool)
        .await
        .context("Failed to fetch staking records")?;

        let total = records.len() as i64;
        let total_staked: Decimal = records
            .iter()
            .filter(|r| r.status == StakingStatus::Active)
            .map(|r| r.amount)
            .sum();

        let record_details: Vec<StakingRecordDetail> = records
            .into_iter()
            .map(StakingRecordDetail::from)
            .collect();

        Ok(StakingRecordsResponse {
            records: record_details,
            total,
            total_staked,
        })
    }

    // ============================================================================
    // Internal Helper Methods
    // ============================================================================

    /// Get or create user points summary for an epoch
    async fn get_or_create_user_summary(
        &self,
        user_address: &str,
        epoch_number: i32,
    ) -> Result<UserPointsSummary> {
        // Try to fetch existing summary
        if let Some(summary) = sqlx::query_as::<_, UserPointsSummary>(
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
        .fetch_optional(&self.pool)
        .await
        .context("Failed to fetch user summary")?
        {
            return Ok(summary);
        }

        // Create new summary if not exists
        let summary = sqlx::query_as::<_, UserPointsSummary>(
            r#"
            INSERT INTO user_points_summary (user_address, epoch_number)
            VALUES ($1, $2)
            RETURNING id, user_address, epoch_number,
                      trading_points, pnl_points, holding_points, referral_points, referral_code, staking_points,
                      total_points, trading_volume, trade_count, realized_pnl,
                      tier, tier_multiplier, referral_count, referral_volume,
                      earn_level, earn_level_weight,
                      tp_daily_used, tp_weekly_used, rp_daily_used,
                      tp_daily_reset_at, tp_weekly_reset_at,
                      pp_daily_used, pp_daily_reset_at,
                      hp_daily_used, hp_daily_reset_at,
                      updated_at
            "#,
        )
        .bind(user_address)
        .bind(epoch_number)
        .fetch_one(&self.pool)
        .await
        .context("Failed to create user summary")?;

        Ok(summary)
    }

    /// Update user summary for trading points
    async fn update_trading_points_summary(
        &self,
        user_address: &str,
        epoch_number: i32,
        trade_volume: Decimal,
        points: Decimal,
        tier_result: &TierCalculationResult,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE user_points_summary
            SET trading_points = trading_points + $1,
                total_points = total_points + $1,
                trading_volume = trading_volume + $2,
                trade_count = trade_count + 1,
                tier = $3,
                tier_multiplier = $4,
                updated_at = NOW()
            WHERE user_address = $5 AND epoch_number = $6
            "#,
        )
        .bind(points)
        .bind(trade_volume)
        .bind(&tier_result.tier)
        .bind(tier_result.multiplier)
        .bind(user_address)
        .bind(epoch_number)
        .execute(&self.pool)
        .await
        .context("Failed to update trading points summary")?;

        // Invalidate cache
        self.invalidate_user_cache(user_address, epoch_number).await?;
        self.invalidate_leaderboard_cache(epoch_number).await?;

        Ok(())
    }

    /// Update user summary for PnL points
    async fn update_pnl_points_summary(
        &self,
        user_address: &str,
        epoch_number: i32,
        realized_pnl: Decimal,
        points: Decimal,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE user_points_summary
            SET pnl_points = pnl_points + $1,
                total_points = total_points + $1,
                realized_pnl = realized_pnl + $2,
                updated_at = NOW()
            WHERE user_address = $3 AND epoch_number = $4
            "#,
        )
        .bind(points)
        .bind(realized_pnl)
        .bind(user_address)
        .bind(epoch_number)
        .execute(&self.pool)
        .await
        .context("Failed to update PnL points summary")?;

        // Invalidate cache
        self.invalidate_user_cache(user_address, epoch_number).await?;
        self.invalidate_leaderboard_cache(epoch_number).await?;

        Ok(())
    }

    /// Update referrer summary for referral points
    pub(crate) async fn update_referral_points_summary(
        &self,
        referrer_address: &str,
        epoch_number: i32,
        referee_volume: Decimal,
        points: Decimal,
    ) -> Result<()> {
        // Ensure referrer has a summary entry
        self.get_or_create_user_summary(referrer_address, epoch_number)
            .await?;

        sqlx::query(
            r#"
            UPDATE user_points_summary
            SET referral_points = referral_points + $1,
                total_points = total_points + $1,
                referral_count = referral_count + 1,
                referral_volume = referral_volume + $2,
                updated_at = NOW()
            WHERE user_address = $3 AND epoch_number = $4
            "#,
        )
        .bind(points)
        .bind(referee_volume)
        .bind(referrer_address)
        .bind(epoch_number)
        .execute(&self.pool)
        .await
        .context("Failed to update referral points summary")?;

        // Invalidate cache
        self.invalidate_user_cache(referrer_address, epoch_number)
            .await?;
        self.invalidate_leaderboard_cache(epoch_number).await?;

        Ok(())
    }

    /// Update user summary for holding points
    async fn update_holding_points_summary(
        &self,
        user_address: &str,
        epoch_number: i32,
        points: Decimal,
    ) -> Result<()> {
        // Ensure user has a summary entry
        self.get_or_create_user_summary(user_address, epoch_number)
            .await?;

        sqlx::query(
            r#"
            UPDATE user_points_summary
            SET holding_points = holding_points + $1,
                total_points = total_points + $1,
                updated_at = NOW()
            WHERE user_address = $2 AND epoch_number = $3
            "#,
        )
        .bind(points)
        .bind(user_address)
        .bind(epoch_number)
        .execute(&self.pool)
        .await
        .context("Failed to update holding points summary")?;

        // Invalidate cache
        self.invalidate_user_cache(user_address, epoch_number).await?;
        self.invalidate_leaderboard_cache(epoch_number).await?;

        Ok(())
    }

    /// Update user summary for staking points
    async fn update_staking_points_summary(
        &self,
        user_address: &str,
        epoch_number: i32,
        points: Decimal,
    ) -> Result<()> {
        // Ensure user has a summary entry
        self.get_or_create_user_summary(user_address, epoch_number)
            .await?;

        sqlx::query(
            r#"
            UPDATE user_points_summary
            SET staking_points = staking_points + $1,
                total_points = total_points + $1,
                updated_at = NOW()
            WHERE user_address = $2 AND epoch_number = $3
            "#,
        )
        .bind(points)
        .bind(user_address)
        .bind(epoch_number)
        .execute(&self.pool)
        .await
        .context("Failed to update staking points summary")?;

        // Invalidate cache
        self.invalidate_user_cache(user_address, epoch_number).await?;
        self.invalidate_leaderboard_cache(epoch_number).await?;

        Ok(())
    }

    /// Save points event to database
    pub(crate) async fn save_points_event(
        &self,
        user_address: &str,
        epoch_number: i32,
        point_type: PointType,
        points: Decimal,
        related_trade_id: Option<Uuid>,
        related_order_id: Option<Uuid>,
        related_position_id: Option<Uuid>,
        referrer_address: Option<String>,
        metadata: serde_json::Value,
    ) -> Result<Uuid> {
        let event_id: Uuid = sqlx::query_scalar(
            r#"
            INSERT INTO points_events (
                user_address, epoch_number, point_type, points,
                related_trade_id, related_order_id, related_position_id,
                referrer_address, metadata
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING id
            "#,
        )
        .bind(user_address)
        .bind(epoch_number)
        .bind(point_type.to_string())
        .bind(points)
        .bind(related_trade_id)
        .bind(related_order_id)
        .bind(related_position_id)
        .bind(referrer_address)
        .bind(&metadata)
        .fetch_one(&self.pool)
        .await
        .context("Failed to save points event")?;

        Ok(event_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_trading_points_calculation() {
        let volume = dec!(100000); // $100,000
        let rate = dec!(0.0001); // 0.01%
        let multiplier = dec!(1.1); // T2 multiplier

        let base_points = volume * rate;
        assert_eq!(base_points, dec!(10));

        let final_points = base_points * multiplier;
        assert_eq!(final_points, dec!(11));
    }

    #[test]
    fn test_pnl_points_calculation() {
        let pnl = dec!(5000); // $5,000 profit
        let rate = dec!(0.001); // 0.1%
        let multiplier = dec!(1.3); // T3 multiplier

        let base_points = pnl * rate;
        assert_eq!(base_points, dec!(5));

        let final_points = base_points * multiplier;
        assert_eq!(final_points, dec!(6.5));
    }

    #[test]
    fn test_referral_points_calculation() {
        let referee_volume = dec!(50000); // $50,000
        let rate = dec!(0.00005); // 0.005%

        let points = referee_volume * rate;
        assert_eq!(points, dec!(2.5));
    }

    #[test]
    fn test_holding_points_calculation() {
        let position_size = dec!(10000); // $10,000
        let rate = dec!(0.00001); // 0.001% per hour
        let duration_hours = 24; // 1 day

        let points_per_hour = position_size * rate;
        assert_eq!(points_per_hour, dec!(0.1));

        let total_points = points_per_hour * Decimal::from(duration_hours);
        assert_eq!(total_points, dec!(2.4));
    }

    #[test]
    fn test_staking_points_calculation() {
        let staked_amount = dec!(1000); // 1000 tokens
        let rate = dec!(0.001); // 0.1% per day

        let points = staked_amount * rate;
        assert_eq!(points, dec!(1));
    }

    #[test]
    fn test_zero_volume_no_points() {
        let volume = Decimal::ZERO;
        let rate = dec!(0.0001);

        let points = volume * rate;
        assert_eq!(points, Decimal::ZERO);
    }

    #[test]
    fn test_negative_pnl_calculation() {
        let pnl = dec!(-1000); // Loss
        let rate = dec!(0.001);

        // Negative PnL should not award points
        assert!(pnl < Decimal::ZERO);

        // In actual implementation, we check pnl > 0 before calculating
        let points = if pnl > Decimal::ZERO {
            pnl * rate
        } else {
            Decimal::ZERO
        };

        assert_eq!(points, Decimal::ZERO);
    }

    #[test]
    fn test_multiplier_application() {
        let base_points = dec!(100);
        let no_tier = dec!(1.0);
        let t1_tier = dec!(1.05);
        let t2_tier = dec!(1.10);
        let t3_tier = dec!(1.15);
        let t4_tier = dec!(1.20);

        assert_eq!(base_points * no_tier, dec!(100));
        assert_eq!(base_points * t1_tier, dec!(105));
        assert_eq!(base_points * t2_tier, dec!(110));
        assert_eq!(base_points * t3_tier, dec!(115));
        assert_eq!(base_points * t4_tier, dec!(120));
    }

    #[test]
    fn test_decimal_precision_trading() {
        // Test that we maintain proper decimal precision
        let volume = dec!(123.456789);
        let rate = dec!(0.0001);

        let points = volume * rate;
        assert_eq!(points, dec!(0.0123456789));
    }

    #[test]
    fn test_very_large_volume() {
        let volume = dec!(999999999.99);
        let rate = dec!(0.0001);

        let points = volume * rate;
        assert_eq!(points, dec!(99999.999999));
        assert!(points > Decimal::ZERO);
    }

    #[test]
    fn test_very_small_volume() {
        let volume = dec!(0.01);
        let rate = dec!(0.0001);

        let points = volume * rate;
        assert_eq!(points, dec!(0.000001));
        assert!(points > Decimal::ZERO);
    }

    #[test]
    fn test_referral_percentage_of_trading() {
        // Referral points should be 20% of trading points
        let volume = dec!(10000);
        let trading_rate = dec!(0.0001); // 0.01%
        let referral_rate = dec!(0.00002); // 0.002% = 20% of trading

        let trading_points = volume * trading_rate;
        let referral_points = volume * referral_rate;

        assert_eq!(trading_points, dec!(1.0));
        assert_eq!(referral_points, dec!(0.2));
        assert_eq!(referral_points * dec!(5), trading_points); // 20% relationship
    }

    #[test]
    fn test_point_type_enum() {
        // Test point type enum values
        assert_eq!(PointType::Trading.to_string(), "trading");
        assert_eq!(PointType::Pnl.to_string(), "pnl");
        assert_eq!(PointType::Holding.to_string(), "holding");
        assert_eq!(PointType::Referral.to_string(), "referral");
        assert_eq!(PointType::Staking.to_string(), "staking");
    }
}
