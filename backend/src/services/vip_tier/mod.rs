//! VIP 阶梯持久化 + 懒惰重算 + 每日 00:00 worker。
//!
//! 模型：
//! - `user_vip_tiers.current_tier` 是 *当前生效* 的 VIP 等级。
//! - `user_vip_tiers.pending_tier` 非空时表示已计划的降级，
//!   到 `pending_effective_at` (一般是次日 00:00 UTC) 才会落到 current。
//! - 升级永远立即写入 current_tier；降级只计划 pending。
//!
//! 进入点：
//! - `resolve(pool, sender, user)`：被 `/account/fee-info`、下单、preview 调用；
//!   懒惰重算 + 生效 upgrade 会广播事件。
//! - `apply_pending_downgrades(pool, sender)`：每日 00:00 worker 调用。
//! - `spawn(state)`：注册背景 worker。

use chrono::{DateTime, Duration, TimeZone, Timelike, Utc};
use rust_decimal::Decimal;
use serde::Serialize;
use sqlx::PgPool;
use std::sync::Arc;
use tokio::sync::broadcast;

use crate::app::state::AppState;
use crate::utils::fee_tiers::{self, VipTier};
use crate::utils::user_volume;

/// Ensure schema exists — called from bootstrap so the service works
/// on the first deploy without a separate migration step.
pub async fn ensure_schema(pool: &PgPool) {
    let stmts = [
        "CREATE TABLE IF NOT EXISTS user_vip_tiers (\
            user_address         TEXT        PRIMARY KEY,\
            current_tier         SMALLINT    NOT NULL DEFAULT 0,\
            effective_since      TIMESTAMPTZ NOT NULL DEFAULT NOW(),\
            pending_tier         SMALLINT,\
            pending_effective_at TIMESTAMPTZ,\
            last_volume_14d      NUMERIC(30,10) NOT NULL DEFAULT 0,\
            updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()\
        )",
        "CREATE INDEX IF NOT EXISTS idx_user_vip_tiers_pending \
            ON user_vip_tiers (pending_effective_at) \
            WHERE pending_effective_at IS NOT NULL",
        "CREATE TABLE IF NOT EXISTS vip_tier_events (\
            id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),\
            user_address  TEXT        NOT NULL,\
            old_tier      SMALLINT    NOT NULL,\
            new_tier      SMALLINT    NOT NULL,\
            volume_14d    NUMERIC(30,10) NOT NULL,\
            reason        TEXT        NOT NULL,\
            created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()\
        )",
        "CREATE INDEX IF NOT EXISTS idx_vip_tier_events_user_time \
            ON vip_tier_events (user_address, created_at DESC)",
        // 14d/30d 卷从 trades 表 aggregate，加复合索引避免全表扫。
        // CONCURRENTLY 在 trades 已较大时更安全，但 `CREATE INDEX IF NOT EXISTS`
        // + 后台 autovacuum 即可；这里保持简单。
        "CREATE INDEX IF NOT EXISTS idx_trades_maker_created_desc \
            ON trades (maker_address, created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_trades_taker_created_desc \
            ON trades (taker_address, created_at DESC)",
    ];
    for s in stmts.iter() {
        if let Err(e) = sqlx::query(s).execute(pool).await {
            tracing::error!("VIP tier ensure_schema failed on `{}`: {}", &s[..60.min(s.len())], e);
        }
    }
    tracing::info!("VIP tier schema ensured");
}

/// WebSocket 推送事件。
#[derive(Debug, Clone, Serialize)]
pub struct VipTierEvent {
    pub user_address: String,
    pub old_tier: u8,
    pub new_tier: u8,
    pub volume_14d: Decimal,
    /// `upgrade_immediate` / `downgrade_scheduled` / `downgrade_applied`。
    pub reason: &'static str,
    pub timestamp: i64,
}

/// `resolve` 的返回：当前生效档位 + 待生效降级。
#[derive(Debug, Clone)]
pub struct EffectiveTier {
    pub current: &'static VipTier,
    pub pending_tier: Option<u8>,
    pub pending_effective_at: Option<DateTime<Utc>>,
    pub volume_14d: Decimal,
}

/// 下一个 UTC 00:00。用于安排降级生效点。
fn next_utc_midnight() -> DateTime<Utc> {
    let now = Utc::now();
    let start_of_day = Utc
        .with_ymd_and_hms(now.year(), now.month(), now.day(), 0, 0, 0)
        .single()
        .unwrap_or(now);
    if now >= start_of_day {
        start_of_day + Duration::days(1)
    } else {
        start_of_day
    }
}

trait DateTimeExt {
    fn year(&self) -> i32;
    fn month(&self) -> u32;
    fn day(&self) -> u32;
}

impl DateTimeExt for DateTime<Utc> {
    fn year(&self) -> i32 { chrono::Datelike::year(self) }
    fn month(&self) -> u32 { chrono::Datelike::month(self) }
    fn day(&self) -> u32 { chrono::Datelike::day(self) }
}

async fn log_event(
    pool: &PgPool,
    user: &str,
    old: u8,
    new: u8,
    volume_14d: Decimal,
    reason: &str,
) {
    let _ = sqlx::query(
        "INSERT INTO vip_tier_events (user_address, old_tier, new_tier, volume_14d, reason) \
         VALUES ($1, $2, $3, $4, $5)"
    )
    .bind(user)
    .bind(old as i16)
    .bind(new as i16)
    .bind(volume_14d)
    .bind(reason)
    .execute(pool)
    .await;
}

fn emit(sender: &broadcast::Sender<VipTierEvent>, evt: VipTierEvent) {
    let _ = sender.send(evt);
}

/// 懒惰重算：读当前行 → 根据 14d volume 决定是否升档 / 计划降档。
/// 返回当前生效档位（升档会立即更新 current_tier）。
pub async fn resolve(
    pool: &PgPool,
    sender: &broadcast::Sender<VipTierEvent>,
    user_address: &str,
) -> EffectiveTier {
    let addr = user_address.to_ascii_lowercase();
    let volume_14d = user_volume::get_14d_volume(pool, &addr).await;
    let target = fee_tiers::classify(volume_14d);

    // 1) 确保行存在。
    let _ = sqlx::query(
        "INSERT INTO user_vip_tiers (user_address, current_tier, last_volume_14d) \
         VALUES ($1, 0, $2) ON CONFLICT (user_address) DO NOTHING"
    )
    .bind(&addr)
    .bind(volume_14d)
    .execute(pool)
    .await;

    // 2) 读取当前状态。
    let row: Option<(i16, Option<i16>, Option<DateTime<Utc>>)> = sqlx::query_as(
        "SELECT current_tier, pending_tier, pending_effective_at \
         FROM user_vip_tiers WHERE user_address = $1"
    )
    .bind(&addr)
    .fetch_optional(pool)
    .await
    .unwrap_or(None);

    let (current_tier_i, pending_tier_i, pending_effective_at) =
        row.unwrap_or((0, None, None));
    let current_tier_level = current_tier_i.max(0) as u8;

    let now = Utc::now();

    // 3) 升档：立即写库，清空 pending，广播事件。
    if target.level > current_tier_level {
        let _ = sqlx::query(
            "UPDATE user_vip_tiers SET \
                current_tier = $2, effective_since = NOW(), \
                pending_tier = NULL, pending_effective_at = NULL, \
                last_volume_14d = $3, updated_at = NOW() \
             WHERE user_address = $1"
        )
        .bind(&addr)
        .bind(target.level as i16)
        .bind(volume_14d)
        .execute(pool)
        .await;

        log_event(pool, &addr, current_tier_level, target.level, volume_14d, "upgrade_immediate").await;
        emit(sender, VipTierEvent {
            user_address: addr.clone(),
            old_tier: current_tier_level,
            new_tier: target.level,
            volume_14d,
            reason: "upgrade_immediate",
            timestamp: now.timestamp_millis(),
        });

        return EffectiveTier {
            current: target,
            pending_tier: None,
            pending_effective_at: None,
            volume_14d,
        };
    }

    // 4) 降档：仅当尚未有计划（或计划目标不一致）才写 pending。
    if target.level < current_tier_level {
        let should_schedule = match pending_tier_i {
            Some(p) => p as u8 != target.level,
            None => true,
        };
        if should_schedule {
            let effective_at = next_utc_midnight();
            let _ = sqlx::query(
                "UPDATE user_vip_tiers SET \
                    pending_tier = $2, pending_effective_at = $3, \
                    last_volume_14d = $4, updated_at = NOW() \
                 WHERE user_address = $1"
            )
            .bind(&addr)
            .bind(target.level as i16)
            .bind(effective_at)
            .bind(volume_14d)
            .execute(pool)
            .await;

            log_event(pool, &addr, current_tier_level, target.level, volume_14d, "downgrade_scheduled").await;
            emit(sender, VipTierEvent {
                user_address: addr.clone(),
                old_tier: current_tier_level,
                new_tier: target.level,
                volume_14d,
                reason: "downgrade_scheduled",
                timestamp: now.timestamp_millis(),
            });

            return EffectiveTier {
                current: fee_tiers::by_level(current_tier_level),
                pending_tier: Some(target.level),
                pending_effective_at: Some(effective_at),
                volume_14d,
            };
        }
    }

    // 5) 无变化。为减少写放大，仅当 14d 量与上次落库值相差 ≥ 1% 时才回写。
    let should_persist = {
        let last: Option<Decimal> = sqlx::query_scalar(
            "SELECT last_volume_14d FROM user_vip_tiers WHERE user_address = $1"
        )
        .bind(&addr)
        .fetch_optional(pool)
        .await
        .unwrap_or(None);
        match last {
            Some(prev) if !prev.is_zero() => {
                let delta = (volume_14d - prev).abs();
                delta / prev >= Decimal::new(1, 2)
            }
            _ => true,
        }
    };
    if should_persist {
        let _ = sqlx::query(
            "UPDATE user_vip_tiers SET last_volume_14d = $2, updated_at = NOW() \
             WHERE user_address = $1"
        )
        .bind(&addr)
        .bind(volume_14d)
        .execute(pool)
        .await;
    }

    EffectiveTier {
        current: fee_tiers::by_level(current_tier_level),
        pending_tier: pending_tier_i.map(|v| v as u8),
        pending_effective_at,
        volume_14d,
    }
}

/// 读取用户当前生效档位的 taker / maker 费率，叠加 referral + staking
/// 折扣并按 6dp 取整 —— 与 `preview_order` 报给用户的费率口径一致。
///
/// 不触发任何 tier 状态写入或事件广播；适用于撮合后台路径（keeper、恢复），
/// 这些路径不应该作为升档事件的触发点。交互式下单路径仍然走 `resolve()`。
///
/// 返回 `(taker_rate, maker_rate)`。如果用户没有 VIP 行（首次下单），按 VIP0
/// 落档；折扣按"可享受推荐返佣"语义传 true，与 preview 一致。
pub async fn current_fee_rates(pool: &PgPool, user_address: &str) -> (Decimal, Decimal) {
    let addr = user_address.to_ascii_lowercase();
    let current: i16 = sqlx::query_scalar(
        "SELECT current_tier FROM user_vip_tiers WHERE user_address = $1",
    )
    .bind(&addr)
    .fetch_optional(pool)
    .await
    .ok()
    .flatten()
    .unwrap_or(0);
    let tier = fee_tiers::by_level(current.max(0) as u8);
    let mult = fee_tiers::discount_multiplier(&addr, true);
    (
        fee_tiers::round_fee(tier.taker * mult),
        fee_tiers::round_fee(tier.maker * mult),
    )
}

/// 应用所有到期的 pending 降级。每日 00:00 worker 调用。
pub async fn apply_pending_downgrades(
    pool: &PgPool,
    sender: &broadcast::Sender<VipTierEvent>,
) -> usize {
    let rows: Vec<(String, i16, i16, Decimal)> = sqlx::query_as(
        "SELECT user_address, current_tier, pending_tier, last_volume_14d \
         FROM user_vip_tiers \
         WHERE pending_tier IS NOT NULL AND pending_effective_at <= NOW()"
    )
    .fetch_all(pool)
    .await
    .unwrap_or_default();

    let mut applied = 0usize;
    for (user, old_tier, new_tier, vol) in rows {
        let result = sqlx::query(
            "UPDATE user_vip_tiers SET \
                current_tier = $2, effective_since = NOW(), \
                pending_tier = NULL, pending_effective_at = NULL, \
                updated_at = NOW() \
             WHERE user_address = $1 \
               AND pending_tier = $2 \
               AND pending_effective_at IS NOT NULL \
               AND pending_effective_at <= NOW()"
        )
        .bind(&user)
        .bind(new_tier)
        .execute(pool)
        .await;
        if matches!(result, Ok(r) if r.rows_affected() > 0) {
            applied += 1;
            log_event(pool, &user, old_tier as u8, new_tier as u8, vol, "downgrade_applied").await;
            emit(sender, VipTierEvent {
                user_address: user,
                old_tier: old_tier as u8,
                new_tier: new_tier as u8,
                volume_14d: vol,
                reason: "downgrade_applied",
                timestamp: Utc::now().timestamp_millis(),
            });
        }
    }
    applied
}

/// 后台 worker：每小时 tick，在 UTC 整点 00:00 执行：
///   1) 应用到期降级；
///   2) 触发近 14 天活跃用户的重算（懒惰升级）。
pub fn spawn(state: Arc<AppState>) {
    tokio::spawn(async move {
        tracing::info!("VIP tier worker started");
        let mut ticker = tokio::time::interval(tokio::time::Duration::from_secs(600));
        loop {
            ticker.tick().await;
            let now = Utc::now();
            // 只在 UTC 00:00–00:10 的窗口里触发，避免每 10 分钟跑全量。
            if !(now.hour() == 0 && now.minute() < 10) {
                continue;
            }

            let pool = &state.db.pool;
            let sender = &state.vip_tier_event_sender;

            let applied = apply_pending_downgrades(pool, sender).await;
            tracing::info!("VIP tier: applied {} pending downgrades", applied);

            // 14 天内有过交易的用户做一次重算（升级会生效）。
            let users: Vec<(String,)> = sqlx::query_as(
                "SELECT DISTINCT user_addr FROM ( \
                    SELECT maker_address AS user_addr FROM trades \
                        WHERE created_at >= NOW() - INTERVAL '14 days' \
                    UNION \
                    SELECT taker_address FROM trades \
                        WHERE created_at >= NOW() - INTERVAL '14 days' \
                 ) t"
            )
            .fetch_all(pool)
            .await
            .unwrap_or_default();
            tracing::info!("VIP tier: recomputing {} active users", users.len());
            for (u,) in users {
                user_volume::invalidate(&u);
                let _ = resolve(pool, sender, &u).await;
            }
        }
    });
}
