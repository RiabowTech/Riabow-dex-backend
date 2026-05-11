//! Tier Calculation and Management (Phase1 rewrite)
//!
//! 基于用户 VIP 等级（来自 user_vip_tiers.current_tier）确定积分 Tier T1/T2/T3：
//!   T1 = VIP 0
//!   T2 = VIP 1 / VIP 2
//!   T3 = VIP 3 / VIP 4 / VIP 5
//! 返回 Maker/Taker 两套费率系数（不再使用单一 multiplier）。

use crate::models::points::*;
use anyhow::{Context, Result};
use rust_decimal::Decimal;
use tracing::info;

impl super::PointsService {
    // =========================================================================
    // Phase1: points_config 读写
    // =========================================================================

    /// 读取 points_config：先找 epoch-specific，找不到则回退到 epoch_number=0 的全局默认
    pub async fn get_points_config(&self, epoch_number: i32) -> Result<PointsConfigRow> {
        // 尝试 epoch-specific
        let row = sqlx::query_as::<_, PointsConfigRow>(
            r#"
            SELECT id, epoch_number,
                   tp_t1_maker, tp_t1_taker, tp_t2_maker, tp_t2_taker,
                   tp_t3_maker, tp_t3_taker, tp_daily_cap, tp_weekly_cap,
                   tier_t2_min, tier_t3_min,
                   rp_trigger_min_volume, rp_trigger_days,
                   rp_referrer_amount, rp_referee_amount, rp_daily_cap_normal,
                   season_weights,
                   pp_amount_rate, pp_return_cap, pp_return_coeff, pp_daily_cap,
                   pp_decay_5min, pp_decay_10min, hp_rate_per_min, hp_daily_cap,
                   updated_at, updated_by
            FROM points_config
            WHERE epoch_number = $1
            "#,
        )
        .bind(epoch_number)
        .fetch_optional(&self.pool)
        .await
        .context("Failed to fetch points_config for epoch")?;

        if let Some(r) = row {
            return Ok(r);
        }

        // 回退到全局默认 (epoch_number=0)
        sqlx::query_as::<_, PointsConfigRow>(
            r#"
            SELECT id, epoch_number,
                   tp_t1_maker, tp_t1_taker, tp_t2_maker, tp_t2_taker,
                   tp_t3_maker, tp_t3_taker, tp_daily_cap, tp_weekly_cap,
                   tier_t2_min, tier_t3_min,
                   rp_trigger_min_volume, rp_trigger_days,
                   rp_referrer_amount, rp_referee_amount, rp_daily_cap_normal,
                   season_weights,
                   pp_amount_rate, pp_return_cap, pp_return_coeff, pp_daily_cap,
                   pp_decay_5min, pp_decay_10min, hp_rate_per_min, hp_daily_cap,
                   updated_at, updated_by
            FROM points_config
            WHERE epoch_number = 0
            "#,
        )
        .fetch_one(&self.pool)
        .await
        .context("No points_config found (neither epoch-specific nor global default)")
    }

    /// Admin: 写入或更新 points_config
    pub async fn upsert_points_config(&self, row: &PointsConfigRow) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO points_config (
                epoch_number,
                tp_t1_maker, tp_t1_taker, tp_t2_maker, tp_t2_taker,
                tp_t3_maker, tp_t3_taker, tp_daily_cap, tp_weekly_cap,
                tier_t2_min, tier_t3_min,
                rp_trigger_min_volume, rp_trigger_days,
                rp_referrer_amount, rp_referee_amount, rp_daily_cap_normal,
                season_weights,
                pp_amount_rate, pp_return_cap, pp_return_coeff, pp_daily_cap,
                pp_decay_5min, pp_decay_10min, hp_rate_per_min, hp_daily_cap,
                updated_at, updated_by
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9,
                    $10, $11, $12, $13, $14, $15, $16, $17,
                    $18, $19, $20, $21, $22, $23, $24, $25,
                    NOW(), $26)
            ON CONFLICT (epoch_number) DO UPDATE SET
                tp_t1_maker            = EXCLUDED.tp_t1_maker,
                tp_t1_taker            = EXCLUDED.tp_t1_taker,
                tp_t2_maker            = EXCLUDED.tp_t2_maker,
                tp_t2_taker            = EXCLUDED.tp_t2_taker,
                tp_t3_maker            = EXCLUDED.tp_t3_maker,
                tp_t3_taker            = EXCLUDED.tp_t3_taker,
                tp_daily_cap           = EXCLUDED.tp_daily_cap,
                tp_weekly_cap          = EXCLUDED.tp_weekly_cap,
                tier_t2_min            = EXCLUDED.tier_t2_min,
                tier_t3_min            = EXCLUDED.tier_t3_min,
                rp_trigger_min_volume  = EXCLUDED.rp_trigger_min_volume,
                rp_trigger_days        = EXCLUDED.rp_trigger_days,
                rp_referrer_amount     = EXCLUDED.rp_referrer_amount,
                rp_referee_amount      = EXCLUDED.rp_referee_amount,
                rp_daily_cap_normal    = EXCLUDED.rp_daily_cap_normal,
                season_weights         = EXCLUDED.season_weights,
                pp_amount_rate         = EXCLUDED.pp_amount_rate,
                pp_return_cap          = EXCLUDED.pp_return_cap,
                pp_return_coeff        = EXCLUDED.pp_return_coeff,
                pp_daily_cap           = EXCLUDED.pp_daily_cap,
                pp_decay_5min          = EXCLUDED.pp_decay_5min,
                pp_decay_10min         = EXCLUDED.pp_decay_10min,
                hp_rate_per_min        = EXCLUDED.hp_rate_per_min,
                hp_daily_cap           = EXCLUDED.hp_daily_cap,
                updated_at             = NOW(),
                updated_by             = EXCLUDED.updated_by
            "#,
        )
        .bind(row.epoch_number)
        .bind(row.tp_t1_maker)
        .bind(row.tp_t1_taker)
        .bind(row.tp_t2_maker)
        .bind(row.tp_t2_taker)
        .bind(row.tp_t3_maker)
        .bind(row.tp_t3_taker)
        .bind(row.tp_daily_cap)
        .bind(row.tp_weekly_cap)
        .bind(row.tier_t2_min)
        .bind(row.tier_t3_min)
        .bind(row.rp_trigger_min_volume)
        .bind(row.rp_trigger_days)
        .bind(row.rp_referrer_amount)
        .bind(row.rp_referee_amount)
        .bind(row.rp_daily_cap_normal)
        .bind(&row.season_weights)
        .bind(row.pp_amount_rate)
        .bind(row.pp_return_cap)
        .bind(row.pp_return_coeff)
        .bind(row.pp_daily_cap)
        .bind(row.pp_decay_5min)
        .bind(row.pp_decay_10min)
        .bind(row.hp_rate_per_min)
        .bind(row.hp_daily_cap)
        .bind(&row.updated_by)
        .execute(&self.pool)
        .await
        .context("Failed to upsert points_config")?;

        info!("Upserted points_config for epoch_number={}", row.epoch_number);
        Ok(())
    }

    // =========================================================================
    // Phase1: Tier 计算（基于用户 VIP 等级）
    // =========================================================================

    /// 根据用户 VIP 等级（user_vip_tiers.current_tier）确定积分 Tier
    ///
    /// 映射规则：T1=VIP0, T2=VIP1/VIP2, T3=VIP3/VIP4/VIP5
    /// 若用户在 user_vip_tiers 中无记录，默认 VIP0 → T1
    pub async fn calculate_tier(
        &self,
        user_address: &str,
        epoch_number: i32,
    ) -> Result<TierCalculationResult> {
        let cfg = self.get_points_config(epoch_number).await?;

        let vip_level: i16 = sqlx::query_scalar(
            "SELECT current_tier FROM user_vip_tiers WHERE user_address = $1",
        )
        .bind(user_address)
        .fetch_optional(&self.pool)
        .await
        .context("Failed to fetch user VIP level from user_vip_tiers")?
        .unwrap_or(0);

        Ok(Self::resolve_tier_from_vip_u8(vip_level.max(0) as u8, &cfg))
    }

    /// 纯函数：将 VIP 等级（0–5）映射为积分 Tier（无DB调用，测试友好）
    ///
    /// T1 = VIP0 | T2 = VIP1/VIP2 | T3 = VIP3/VIP4/VIP5 | 越界 → T1
    pub fn resolve_tier_from_vip_u8(
        vip_level: u8,
        cfg: &PointsConfigRow,
    ) -> TierCalculationResult {
        match vip_level {
            1 | 2 => TierCalculationResult {
                tier: "T2".to_string(),
                multiplier: cfg.tp_t2_maker,
                maker_rate: cfg.tp_t2_maker,
                taker_rate: cfg.tp_t2_taker,
                min_volume: Decimal::ZERO,
                max_volume: None,
            },
            3 | 4 | 5 => TierCalculationResult {
                tier: "T3".to_string(),
                multiplier: cfg.tp_t3_maker,
                maker_rate: cfg.tp_t3_maker,
                taker_rate: cfg.tp_t3_taker,
                min_volume: Decimal::ZERO,
                max_volume: None,
            },
            _ => TierCalculationResult {
                // VIP0 或越界
                tier: "T1".to_string(),
                multiplier: cfg.tp_t1_maker,
                maker_rate: cfg.tp_t1_maker,
                taker_rate: cfg.tp_t1_taker,
                min_volume: Decimal::ZERO,
                max_volume: None,
            },
        }
    }

    // =========================================================================
    // 用户 Tier 信息查询（对外 API）
    // =========================================================================

    pub async fn get_user_tier_info(
        &self,
        user_address: &str,
        epoch_number: Option<i32>,
    ) -> Result<Option<TierInfoResponse>> {
        let epoch_num = if let Some(n) = epoch_number {
            n
        } else {
            match self.get_active_epoch().await? {
                Some(e) => e.epoch_number,
                None => return Ok(None),
            }
        };

        let cfg = self.get_points_config(epoch_num).await?;

        // 从 user_vip_tiers 查询 VIP 等级（SMALLINT）+ 14 天累计交易量
        let row: Option<(i16, Decimal)> = sqlx::query_as(
            "SELECT current_tier, last_volume_14d FROM user_vip_tiers WHERE user_address = $1",
        )
        .bind(user_address)
        .fetch_optional(&self.pool)
        .await
        .context("Failed to fetch user VIP level")?;

        let (vip_level, current_volume) =
            row.unwrap_or((0, Decimal::ZERO));

        let tier_result = Self::resolve_tier_from_vip_u8(vip_level.max(0) as u8, &cfg);

        Ok(Some(TierInfoResponse {
            tier: tier_result.tier,
            multiplier: tier_result.maker_rate,
            min_volume: tier_result.min_volume,
            max_volume: tier_result.max_volume,
            current_volume,
            next_tier: None, // Tier 由 VIP 等级决定，升级路径由手续费体系管理
        }))
    }

    // =========================================================================
    // 兼容旧代码的 get_tier_config（仍可用）
    // =========================================================================

    pub async fn get_tier_config(&self, epoch_number: Option<i32>) -> Result<Vec<TierConfig>> {
        let epoch_num = epoch_number.unwrap_or(0);
        sqlx::query_as::<_, TierConfig>(
            r#"
            SELECT id, tier_name, min_volume, max_volume, multiplier,
                   epoch_number, is_active, created_at
            FROM trading_tier_config
            WHERE (epoch_number = $1 OR ($1 = 0 AND epoch_number IS NULL))
              AND is_active = true
            ORDER BY min_volume ASC
            "#,
        )
        .bind(epoch_num)
        .fetch_all(&self.pool)
        .await
        .context("Failed to fetch tier config")
    }
}

// =========================================================================
// Tests
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use uuid::Uuid;
    use chrono::Utc;

    fn make_cfg() -> PointsConfigRow {
        PointsConfigRow {
            id: Uuid::new_v4(),
            epoch_number: 0,
            tp_t1_maker: dec!(1.2),
            tp_t1_taker: dec!(0.8),
            tp_t2_maker: dec!(1.5),
            tp_t2_taker: dec!(1.0),
            tp_t3_maker: dec!(2.0),
            tp_t3_taker: dec!(1.3),
            tp_daily_cap: 5000,
            tp_weekly_cap: 25000,
            tier_t2_min: dec!(5000000),
            tier_t3_min: dec!(100000000),
            rp_trigger_min_volume: dec!(1000),
            rp_trigger_days: 7,
            rp_referrer_amount: 10,
            rp_referee_amount: 10,
            rp_daily_cap_normal: 100,
            season_weights: serde_json::json!({"tp":0.65,"pp":0.20,"hp":0.05,"rp":0.10}),
            pp_amount_rate: dec!(2.5),
            pp_return_cap: dec!(0.20),
            pp_return_coeff: dec!(6.0),
            pp_daily_cap: 20000,
            pp_decay_5min: dec!(0.5),
            pp_decay_10min: dec!(0.25),
            hp_rate_per_min: dec!(0.00003),
            hp_daily_cap: 40000,
            updated_at: Utc::now(),
            updated_by: None,
        }
    }

    #[test]
    fn test_resolve_tier_t1_vip0() {
        let cfg = make_cfg();
        let result = super::super::PointsService::resolve_tier_from_vip_u8(0, &cfg);
        assert_eq!(result.tier, "T1");
        assert_eq!(result.maker_rate, dec!(1.2));
        assert_eq!(result.taker_rate, dec!(0.8));
    }

    #[test]
    fn test_resolve_tier_t1_out_of_range_defaults() {
        let cfg = make_cfg();
        // VIP 体系只有 0..=5，越界也落到 T1，防止脏数据把高权重给谁。
        let result = super::super::PointsService::resolve_tier_from_vip_u8(9, &cfg);
        assert_eq!(result.tier, "T1");
    }

    #[test]
    fn test_resolve_tier_t2_vip1_vip2() {
        let cfg = make_cfg();
        for level in &[1u8, 2u8] {
            let r = super::super::PointsService::resolve_tier_from_vip_u8(*level, &cfg);
            assert_eq!(r.tier, "T2", "VIP{} should map to T2", level);
            assert_eq!(r.maker_rate, dec!(1.5));
            assert_eq!(r.taker_rate, dec!(1.0));
        }
    }

    #[test]
    fn test_resolve_tier_t3_vip3_to_vip5() {
        let cfg = make_cfg();
        for level in &[3u8, 4u8, 5u8] {
            let r = super::super::PointsService::resolve_tier_from_vip_u8(*level, &cfg);
            assert_eq!(r.tier, "T3", "VIP{} should map to T3", level);
            assert_eq!(r.maker_rate, dec!(2.0));
            assert_eq!(r.taker_rate, dec!(1.3));
        }
    }

    #[test]
    fn test_tp_formula() {
        // PRD案例：T2 Maker，$500,000 → TP = (500000/1000) × 1.5 = 750
        let volume = dec!(500000);
        let rate = dec!(1.5);
        let tp = (volume / dec!(1000)) * rate;
        assert_eq!(tp, dec!(750));
    }
}
