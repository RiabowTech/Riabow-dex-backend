//! Earn Level 引擎（Phase1）
//!
//! PRD规则：
//! - 基于当前赛季原始总积分（TP+PP+HP+RP累计，不含加权），每日UTC 00:00刷新
//! - L0: <1,000      权重 4
//! - L1: 1,000~9,999  权重 8
//! - L2: 10,000~49,999 权重 12
//! - L3: 50,000~199,999 权重 25
//! - L4: 200,000~499,999 权重 60
//! - L5: >=500,000    权重 120
//!
//! Earn Level决定用户在Earn产品中的申购优先级/配额权重。

use crate::models::points::{EarnLevel, EarnLevelConfig};
use anyhow::{Context, Result};
use rust_decimal::Decimal;
use tracing::info;

impl super::PointsService {
    // ============================================================================
    // Earn Level 计算与刷新
    // ============================================================================

    /// 计算单用户Earn Level并写入user_points_summary
    pub async fn calculate_user_earn_level(
        &self,
        user_address: &str,
        epoch_number: i32,
    ) -> Result<EarnLevel> {
        let total_points: Decimal = sqlx::query_scalar(
            r#"
            SELECT COALESCE(total_points, 0)
            FROM user_points_summary
            WHERE user_address = $1 AND epoch_number = $2
            "#,
        )
        .bind(user_address)
        .bind(epoch_number)
        .fetch_optional(&self.pool)
        .await
        .context("Failed to fetch total_points for earn level")?
        .unwrap_or(Decimal::ZERO);

        let level = self.resolve_earn_level(total_points).await?;
        let weight = level.weight();

        sqlx::query(
            r#"
            UPDATE user_points_summary
            SET earn_level        = $1,
                earn_level_weight = $2,
                updated_at        = NOW()
            WHERE user_address = $3 AND epoch_number = $4
            "#,
        )
        .bind(level.as_i32())
        .bind(weight as i32)
        .bind(user_address)
        .bind(epoch_number)
        .execute(&self.pool)
        .await
        .context("Failed to update earn_level in user_points_summary")?;

        Ok(level)
    }

    /// 根据积分量从DB配置（earn_level_config）解析等级
    pub async fn resolve_earn_level(&self, total_points: Decimal) -> Result<EarnLevel> {
        let configs: Vec<EarnLevelConfig> = sqlx::query_as::<_, EarnLevelConfig>(
            r#"
            SELECT level, points_min, points_max, weight, updated_at, updated_by
            FROM earn_level_config
            ORDER BY points_min DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .context("Failed to fetch earn_level_config")?;

        if configs.is_empty() {
            return Ok(EarnLevel::from_points(total_points));
        }

        let total_i64: i64 = total_points.try_into().unwrap_or(i64::MAX);

        for cfg in &configs {
            if total_i64 >= cfg.points_min {
                return Ok(match cfg.level {
                    0 => EarnLevel::L0,
                    1 => EarnLevel::L1,
                    2 => EarnLevel::L2,
                    3 => EarnLevel::L3,
                    4 => EarnLevel::L4,
                    _ => EarnLevel::L5,
                });
            }
        }

        Ok(EarnLevel::L0)
    }

    // ============================================================================
    // 每日批量刷新（scheduler调用）
    // ============================================================================

    /// 批量刷新所有活跃用户的Earn Level（每日UTC 00:00执行）
    pub async fn run_daily_earn_level_refresh(&self) -> Result<u64> {
        let epoch = match self.get_active_epoch().await? {
            Some(e) => e,
            None => {
                info!("No active epoch, skip earn level refresh");
                return Ok(0);
            }
        };
        let epoch_number = epoch.epoch_number;

        let users: Vec<String> = sqlx::query_scalar(
            r#"
            SELECT user_address
            FROM user_points_summary
            WHERE epoch_number = $1
            "#,
        )
        .bind(epoch_number)
        .fetch_all(&self.pool)
        .await
        .context("Failed to fetch user list for earn level refresh")?;

        let total = users.len() as u64;
        let mut updated = 0u64;

        for user_address in &users {
            match self.calculate_user_earn_level(user_address, epoch_number).await {
                Ok(_) => updated += 1,
                Err(e) => {
                    tracing::warn!("Failed to refresh earn level for {}: {}", user_address, e);
                }
            }
        }

        if let Err(e) = self.write_earn_weight_snapshot(epoch_number).await {
            tracing::warn!("Failed to write earn_weight_snapshot: {}", e);
        }

        info!(
            "Earn level refresh done: epoch={}, total={}, updated={}",
            epoch_number, total, updated
        );

        Ok(updated)
    }

    async fn write_earn_weight_snapshot(&self, epoch_number: i32) -> Result<()> {
        let rows: Vec<(i32, i64, i32)> = sqlx::query_as(
            r#"
            SELECT earn_level, COUNT(*)::BIGINT, earn_level_weight
            FROM user_points_summary
            WHERE epoch_number = $1
            GROUP BY earn_level, earn_level_weight
            ORDER BY earn_level
            "#,
        )
        .bind(epoch_number)
        .fetch_all(&self.pool)
        .await
        .context("Failed to aggregate earn level stats")?;

        let mut total_effective_weight: i64 = 0;
        let mut level_breakdown = serde_json::Map::new();

        for (level, count, weight) in &rows {
            let total_w = count * (*weight as i64);
            total_effective_weight += total_w;
            level_breakdown.insert(
                format!("L{}", level),
                serde_json::json!({
                    "count": count,
                    "weight": weight,
                    "total_weight": total_w,
                }),
            );
        }

        sqlx::query(
            r#"
            INSERT INTO earn_weight_snapshot (
                snapshot_date, total_effective_weight, level_breakdown, calculated_at
            )
            VALUES (CURRENT_DATE, $1, $2, NOW())
            ON CONFLICT (snapshot_date) DO UPDATE
              SET total_effective_weight = EXCLUDED.total_effective_weight,
                  level_breakdown        = EXCLUDED.level_breakdown,
                  calculated_at          = NOW()
            "#,
        )
        .bind(total_effective_weight)
        .bind(serde_json::Value::Object(level_breakdown))
        .execute(&self.pool)
        .await
        .context("Failed to upsert earn_weight_snapshot")?;

        Ok(())
    }

    // ============================================================================
    // Earn 申购配额查询
    // ============================================================================

    /// 返回用户当日Earn申购配额权重信息
    pub async fn get_user_earn_quota(
        &self,
        user_address: &str,
        epoch_number: i32,
    ) -> Result<EarnQuotaInfo> {
        // 一次查询取出所有需要的用户字段
        let row: Option<(i32, i32, Decimal, chrono::DateTime<chrono::Utc>)> = sqlx::query_as(
            r#"
            SELECT COALESCE(earn_level, 0),
                   COALESCE(earn_level_weight, 4),
                   COALESCE(total_points, 0),
                   updated_at
            FROM user_points_summary
            WHERE user_address = $1 AND epoch_number = $2
            "#,
        )
        .bind(user_address)
        .bind(epoch_number)
        .fetch_optional(&self.pool)
        .await
        .context("Failed to fetch earn level for quota")?;

        let (earn_level, earn_level_weight, total_raw_points, level_updated_at) = row
            .unwrap_or((0, 4, Decimal::ZERO, chrono::Utc::now()));

        let total_weight: i64 = sqlx::query_scalar(
            r#"
            SELECT COALESCE(total_effective_weight, 0)
            FROM earn_weight_snapshot
            WHERE snapshot_date = CURRENT_DATE
            "#,
        )
        .fetch_optional(&self.pool)
        .await
        .context("Failed to fetch today's earn_weight_snapshot")?
        .flatten()
        .unwrap_or(0);

        let level = match earn_level {
            0 => EarnLevel::L0,
            1 => EarnLevel::L1,
            2 => EarnLevel::L2,
            3 => EarnLevel::L3,
            4 => EarnLevel::L4,
            _ => EarnLevel::L5,
        };

        let points_to_next = level.points_to_next(total_raw_points);

        let my_share_pct = if total_weight > 0 {
            rust_decimal::Decimal::from(earn_level_weight) / rust_decimal::Decimal::from(total_weight)
        } else {
            rust_decimal::Decimal::ZERO
        };

        // Per-product quotas (PRD §6.0):
        //   share_quota = my_share_pct × capacity
        //   capped      = min(share_quota, capacity × concentration_cap_pct)
        // Aggregate cross-product quota = max single-product quota ×
        // cross_product_multiplier (taken from the most restrictive product
        // row; defaulted to 3 when no products configured).
        #[derive(sqlx::FromRow)]
        struct ProductRow {
            product_id: uuid::Uuid,
            product_name: String,
            capacity: rust_decimal::Decimal,
            concentration_cap_pct: rust_decimal::Decimal,
            cross_product_multiplier: i32,
        }
        let products_rows: Vec<ProductRow> = sqlx::query_as(
            r#"SELECT product_id, product_name, capacity, concentration_cap_pct,
                       cross_product_multiplier
                FROM earn_product_config
                WHERE is_active = true
                ORDER BY product_name"#,
        )
        .fetch_all(&self.pool)
        .await
        .context("Failed to load earn_product_config")?;

        let mut products = Vec::with_capacity(products_rows.len());
        let mut max_single_quota = rust_decimal::Decimal::ZERO;
        let mut min_multiplier: i32 = 3;
        for p in products_rows {
            let share_quota = my_share_pct * p.capacity;
            let cap_quota = p.capacity * p.concentration_cap_pct;
            let (effective, capped) = if share_quota > cap_quota {
                (cap_quota, true)
            } else {
                (share_quota, false)
            };
            if effective > max_single_quota {
                max_single_quota = effective;
            }
            if p.cross_product_multiplier < min_multiplier {
                min_multiplier = p.cross_product_multiplier;
            }
            products.push(EarnProductQuota {
                product_id: p.product_id,
                product_name: p.product_name,
                capacity: p.capacity,
                concentration_cap_pct: p.concentration_cap_pct,
                quota_today: effective,
                concentration_cap_applied: capped,
            });
        }
        let cross_product_quota_today =
            max_single_quota * rust_decimal::Decimal::from(min_multiplier.max(1));

        Ok(EarnQuotaInfo {
            level,
            weight: earn_level_weight as u32,
            total_raw_points,
            next_level_points_needed: points_to_next,
            platform_total_effective_weight: total_weight,
            my_share_pct,
            level_updated_at,
            products,
            cross_product_quota_today,
        })
    }

    /// Admin: 写入或更新 earn_level_config
    pub async fn upsert_earn_level_config(&self, configs: &[EarnLevelConfig]) -> Result<()> {
        for cfg in configs {
            sqlx::query(
                r#"
                INSERT INTO earn_level_config (level, points_min, points_max, weight, updated_at)
                VALUES ($1, $2, $3, $4, NOW())
                ON CONFLICT (level) DO UPDATE
                  SET points_min = EXCLUDED.points_min,
                      points_max = EXCLUDED.points_max,
                      weight     = EXCLUDED.weight,
                      updated_at = NOW()
                "#,
            )
            .bind(cfg.level)
            .bind(cfg.points_min)
            .bind(cfg.points_max)
            .bind(cfg.weight)
            .execute(&self.pool)
            .await
            .context("Failed to upsert earn_level_config")?;
        }

        info!("Updated {} earn_level_config rows", configs.len());
        Ok(())
    }

    /// 查询当前 earn_level_config 列表
    pub async fn get_earn_level_config(&self) -> Result<Vec<EarnLevelConfig>> {
        let configs = sqlx::query_as::<_, EarnLevelConfig>(
            r#"
            SELECT level, points_min, points_max, weight, updated_at, updated_by
            FROM earn_level_config
            ORDER BY level ASC
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .context("Failed to fetch earn_level_config")?;

        Ok(configs)
    }
}

// ============================================================================
// EarnQuotaInfo 响应结构
// ============================================================================

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EarnQuotaInfo {
    pub level: EarnLevel,
    pub weight: u32,
    pub total_raw_points: rust_decimal::Decimal,
    pub next_level_points_needed: rust_decimal::Decimal,
    pub platform_total_effective_weight: i64,
    pub my_share_pct: rust_decimal::Decimal,
    pub level_updated_at: chrono::DateTime<chrono::Utc>,
    /// Per-product quota breakdown (PRD §6.0). Empty when no products
    /// are configured. Each entry already accounts for the per-product
    /// concentration cap (`min(share_quota, capacity × cap_pct)`).
    #[serde(default)]
    pub products: Vec<EarnProductQuota>,
    /// Sum of all `quota_today` after concentration cap, capped further
    /// by the cross-product multiplier (default: any single product's
    /// quota × 3).
    pub cross_product_quota_today: rust_decimal::Decimal,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EarnProductQuota {
    pub product_id: uuid::Uuid,
    pub product_name: String,
    pub capacity: rust_decimal::Decimal,
    pub concentration_cap_pct: rust_decimal::Decimal,
    /// Effective today's quota for this user on this product.
    pub quota_today: rust_decimal::Decimal,
    /// Whether the per-product 5% cap was binding.
    pub concentration_cap_applied: bool,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;
    use crate::models::points::EarnLevel;

    #[test]
    fn test_earn_level_from_points() {
        assert_eq!(EarnLevel::from_points(dec!(0)), EarnLevel::L0);
        assert_eq!(EarnLevel::from_points(dec!(999)), EarnLevel::L0);
        assert_eq!(EarnLevel::from_points(dec!(1000)), EarnLevel::L1);
        assert_eq!(EarnLevel::from_points(dec!(9999)), EarnLevel::L1);
        assert_eq!(EarnLevel::from_points(dec!(10000)), EarnLevel::L2);
        assert_eq!(EarnLevel::from_points(dec!(49999)), EarnLevel::L2);
        assert_eq!(EarnLevel::from_points(dec!(50000)), EarnLevel::L3);
        assert_eq!(EarnLevel::from_points(dec!(199999)), EarnLevel::L3);
        assert_eq!(EarnLevel::from_points(dec!(200000)), EarnLevel::L4);
        assert_eq!(EarnLevel::from_points(dec!(499999)), EarnLevel::L4);
        assert_eq!(EarnLevel::from_points(dec!(500000)), EarnLevel::L5);
        assert_eq!(EarnLevel::from_points(dec!(1000000)), EarnLevel::L5);
    }

    #[test]
    fn test_earn_level_weights() {
        assert_eq!(EarnLevel::L0.weight(), 4);
        assert_eq!(EarnLevel::L1.weight(), 8);
        assert_eq!(EarnLevel::L2.weight(), 12);
        assert_eq!(EarnLevel::L3.weight(), 25);
        assert_eq!(EarnLevel::L4.weight(), 60);
        assert_eq!(EarnLevel::L5.weight(), 120);
    }

    #[test]
    fn test_earn_level_ordering() {
        assert!(EarnLevel::L0 < EarnLevel::L1);
        assert!(EarnLevel::L5 > EarnLevel::L4);
    }
}
