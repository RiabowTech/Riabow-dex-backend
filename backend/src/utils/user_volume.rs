//! Per-user rolling trading volume with a short TTL cache.
//!
//! VIP 阶梯判定走 14 天滚动窗口；保留 30d 作为 UI 兼容字段。

use dashmap::DashMap;
use rust_decimal::Decimal;
use sqlx::PgPool;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

// Bumped 60s → 300s on 2026-04-25. The underlying SUM-on-trades query was
// the single largest CPU consumer on prod (28.6% of total DB CPU; 469k sec
// across 359k calls; mean 1.3s) — and EXPLAIN showed it's not a planner
// bug, it's genuine work over the 30-day window. Cutting call rate 5× was
// the cheapest immediately-shippable win. Tradeoff: a user crossing a VIP
// fee tier sees the new tier take effect within 5 min instead of 1 min.
// `invalidate()` still bypasses the TTL when the VIP daily task fires.
const TTL: Duration = Duration::from_secs(300);

#[derive(Clone, Copy)]
struct Entry {
    v14: Decimal,
    v30: Decimal,
    at: Instant,
}

// Per-key mutex slot. Holding the mutex across the DB fetch is the
// singleflight guarantee: N concurrent readers for the same user serialize
// on this mutex; the first to enter populates the cache, the rest wake up
// to a fresh cached value and return without re-querying. Different users
// hold different mutexes, so cross-user concurrency is preserved.
type Slot = Arc<Mutex<Option<Entry>>>;
static CACHE: OnceLock<DashMap<String, Slot>> = OnceLock::new();

fn cache() -> &'static DashMap<String, Slot> {
    CACHE.get_or_init(DashMap::new)
}

async fn fetch_volumes(pool: &PgPool, user: &str) -> (Decimal, Decimal) {
    // OR-predicate form is intentional. UNION ALL was tried 2026-04-26 (PR #47)
    // because EXPLAIN ANALYZE on one heavy user (538k matching rows in 30d)
    // showed a 40% improvement (3.4s → 2.0s, disk reads -99.99%). After deploy,
    // prod aggregate moved the wrong way: windowed mean rose from 4.7s → 13.3s
    // over the next 22 min on the same workload. The root cause is workload
    // distribution: under UNION ALL the planner probes both `idx_trades_maker_*`
    // and `idx_trades_taker_*` across all 11 chunks for both halves (22 chunk
    // lookups per call); for the typical sparse user this overhead exceeds
    // what OR's fallback-to-seqscan on chunk 335 costs. Heavy users were the
    // exception, not the rule. Reverted same day.
    //
    // Do not re-attempt without first sampling per-call cost across the
    // actual user-population distribution (e.g. EXPLAIN on users at p10 /
    // p50 / p90 of trade-row count), not just one outlier.
    //
    // The proper fix is a Timescale continuous aggregate bucketed daily by
    // user (L3 in the perf plan); see follow-up issue. That sidesteps the
    // OR-vs-UNION-ALL choice entirely and drops per-call cost to <1 ms.
    let row: Result<(Decimal, Decimal), _> = sqlx::query_as(
        r#"
        SELECT
            COALESCE(SUM(CASE WHEN created_at >= NOW() - INTERVAL '14 days' THEN amount * price ELSE 0 END), 0)::numeric AS v14,
            COALESCE(SUM(CASE WHEN created_at >= NOW() - INTERVAL '30 days' THEN amount * price ELSE 0 END), 0)::numeric AS v30
        FROM trades
        WHERE (maker_address = $1 OR taker_address = $1)
          AND created_at >= NOW() - INTERVAL '30 days'
        "#,
    )
    .bind(user)
    .fetch_one(pool)
    .await;
    match row {
        Ok((a, b)) => (a, b),
        Err(e) => {
            tracing::warn!("user volume query failed for {}: {}", user, e);
            (Decimal::ZERO, Decimal::ZERO)
        }
    }
}

/// 返回 (14d, 30d)。5min TTL 缓存，并发同一用户的 cache miss 通过 per-key mutex 合并为单次查询。
pub async fn get_volumes(pool: &PgPool, user_address: &str) -> (Decimal, Decimal) {
    let key = user_address.to_ascii_lowercase();

    // Acquire (or create) the per-key slot. The dashmap RefMut is dropped
    // at the end of this block so the shard lock is released before we
    // await on the inner mutex.
    let slot: Slot = {
        let entry = cache()
            .entry(key.clone())
            .or_insert_with(|| Arc::new(Mutex::new(None)));
        Arc::clone(entry.value())
    };

    let mut guard = slot.lock().await;

    if let Some(e) = guard.as_ref() {
        if e.at.elapsed() < TTL {
            return (e.v14, e.v30);
        }
    }

    let (v14, v30) = fetch_volumes(pool, &key).await;
    *guard = Some(Entry { v14, v30, at: Instant::now() });
    (v14, v30)
}

/// 14 天滚动交易额。
pub async fn get_14d_volume(pool: &PgPool, user_address: &str) -> Decimal {
    get_volumes(pool, user_address).await.0
}

/// 30 天滚动交易额（兼容 UI 展示）。
pub async fn get_30d_volume(pool: &PgPool, user_address: &str) -> Decimal {
    get_volumes(pool, user_address).await.1
}

/// VIP 每日任务触发时手动失效单个用户的缓存，避免 5min 内看到旧值。
///
/// Best-effort: if a fetch is currently in flight for this user, the
/// in-flight result will still populate the slot when it completes. The
/// next read after that will see the fresh value. Removing the slot from
/// the map also lets a brand-new fetch start immediately on the next
/// reader rather than waiting for the in-flight one.
pub fn invalidate(user_address: &str) {
    cache().remove(&user_address.to_ascii_lowercase());
}
