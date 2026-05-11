//! Unified-margin risk worker.
//!
//! Every `TICK_SECS` seconds we iterate every user whose
//! `users.margin_mode = 'unified'`, recompute their uniMMR snapshot,
//! persist it into `unified_margin_accounts`, and — when the status
//! changes — enforce side-effects (reduce_only order cancellation) and
//! publish a `UnifiedAccountEvent` over the broadcast channel so the
//! WebSocket layer can push it to the owning user.
//!
//! Side-effects on transition / sustained state:
//!   * → `reduce_only` / `liquidating`: cancel all non-terminal orders
//!     and release frozen margin (`enforcement::cancel_all_open_orders`).
//!   * sustained `liquidating`: per-tick `liquidation::liquidate_one`
//!     fully closes the worst-PnL position via the shared
//!     `LiquidationService` (insurance fund + ADL handled there).

use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use rust_decimal::Decimal;

use crate::app::state::{AppState, UnifiedAccountEvent};
use crate::models::position::{Position, PositionStatus};
use crate::models::unified_margin::{UnifiedAccountStatus, UnifiedRiskSnapshot};
use crate::services::unified_margin::{compute_risk_with_tiers, enforcement, liquidation, MMR_DEFAULT};

const TICK_SECS: u64 = 2;

/// Spawn the worker. Call from `app::workers::start_workers`.
pub fn spawn(state: Arc<AppState>) {
    tokio::spawn(async move {
        tracing::info!(
            "Unified-margin risk worker started (interval: {}s)",
            TICK_SECS
        );
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(TICK_SECS));
        // Skip the immediate first tick — let other services initialize.
        ticker.tick().await;

        loop {
            ticker.tick().await;
            if let Err(e) = tick(&state).await {
                tracing::warn!("unified-margin risk worker tick failed: {}", e);
            }
        }
    });
}

/// Hard ceiling on users processed per tick. PRD doesn't define one,
/// but unbounded fan-out is a foot-gun. When exceeded, we still process
/// the first N (sorted by address for determinism) and emit a metric so
/// ops can size up the worker / shorten interval / shard.
const PER_TICK_USER_CAP: usize = 5_000;

async fn tick(state: &Arc<AppState>) -> anyhow::Result<()> {
    // 1. Load every unified-mode user. Sorted for deterministic
    //    truncation when above PER_TICK_USER_CAP.
    let users: Vec<(String, String)> = sqlx::query_as(
        "SELECT u.address, \
                COALESCE(uma.account_status, 'normal') AS account_status \
         FROM users u \
         LEFT JOIN unified_margin_accounts uma ON uma.user_address = u.address \
         WHERE u.margin_mode = 'unified' \
         ORDER BY u.address",
    )
    .fetch_all(&state.db.pool)
    .await?;

    crate::services::metrics::UNIFIED_ACCOUNTS_TOTAL.set(users.len() as f64);

    if users.is_empty() {
        // Reset per-status gauges so Grafana doesn't show a stale
        // warning/liquidating bucket from a previous population.
        for status in &["normal", "warning_1", "warning_2", "reduce_only", "liquidating"] {
            crate::services::metrics::UNIFIED_ACCOUNTS_BY_STATUS
                .with_label_values(&[status])
                .set(0.0);
        }
        return Ok(());
    }

    // Truncate (with metric) to keep tick latency bounded.
    let total = users.len();
    let users: Vec<(String, String)> = if total > PER_TICK_USER_CAP {
        tracing::warn!(
            "unified-risk-worker: {} users > cap {}, truncating; \
             consider sharding or shortening tick interval",
            total, PER_TICK_USER_CAP
        );
        users.into_iter().take(PER_TICK_USER_CAP).collect()
    } else {
        users
    };

    // ---- Batch-load balances + positions for ALL users in 2 queries ----
    let collateral_symbol = state.config.collateral_symbol();
    let addrs: Vec<String> = users.iter().map(|(a, _)| a.clone()).collect();

    let balance_rows: Vec<(String, Decimal, Decimal)> = sqlx::query_as(
        "SELECT user_address, available, frozen FROM balances \
         WHERE token = $1 AND user_address = ANY($2)",
    )
    .bind(collateral_symbol)
    .bind(&addrs)
    .fetch_all(&state.db.pool)
    .await?;
    let mut balance_by_user: HashMap<String, Decimal> = HashMap::new();
    for (addr, avail, frozen) in balance_rows {
        balance_by_user.insert(addr, avail + frozen);
    }

    let position_rows: Vec<Position> = sqlx::query_as::<_, Position>(
        "SELECT * FROM positions WHERE status = $1 AND user_address = ANY($2)",
    )
    .bind(PositionStatus::Open)
    .bind(&addrs)
    .fetch_all(&state.db.pool)
    .await?;
    let mut positions_by_user: HashMap<String, Vec<Position>> = HashMap::new();
    let mut all_symbols: std::collections::HashSet<String> =
        std::collections::HashSet::new();
    for p in position_rows {
        all_symbols.insert(p.symbol.clone());
        positions_by_user
            .entry(p.user_address.clone())
            .or_default()
            .push(p);
    }

    // Snapshot mark prices once for every distinct symbol — eliminates
    // the per-position lookup that previously dominated tick latency.
    let mut mark_prices_global: HashMap<String, Decimal> = HashMap::new();
    for sym in &all_symbols {
        if let Some(mp) = state.price_feed_service.get_mark_price(sym).await {
            mark_prices_global.insert(sym.clone(), mp);
        }
    }

    // Aggregate buckets for this tick.
    let mut status_counts: std::collections::HashMap<&'static str, u64> =
        std::collections::HashMap::new();

    for (addr, prev_status_str) in &users {
        let wallet_balance = balance_by_user.get(addr).copied().unwrap_or(Decimal::ZERO);
        let positions = positions_by_user.remove(addr).unwrap_or_default();
        match tick_one(
            state, addr, prev_status_str,
            wallet_balance, &positions, &mark_prices_global,
        ).await {
            Ok(new_status) => {
                *status_counts.entry(new_status).or_insert(0) += 1;
            }
            Err(e) => tracing::warn!("risk tick for {} failed: {}", addr, e),
        }
    }

    for status in &["normal", "warning_1", "warning_2", "reduce_only", "liquidating"] {
        let n = status_counts.get(status).copied().unwrap_or(0);
        crate::services::metrics::UNIFIED_ACCOUNTS_BY_STATUS
            .with_label_values(&[status])
            .set(n as f64);
    }

    crate::services::metrics::TASK_LAST_RUN_TIMESTAMP
        .with_label_values(&["unified-risk-worker"])
        .set(chrono::Utc::now().timestamp() as f64);

    Ok(())
}

async fn tick_one(
    state: &Arc<AppState>,
    addr: &str,
    prev_status_str: &str,
    wallet_balance: Decimal,
    positions: &[Position],
    mark_prices: &HashMap<String, Decimal>,
) -> anyhow::Result<&'static str> {

    let tiers_guard = state.margin_tiers.read().await;
    let snapshot: UnifiedRiskSnapshot = compute_risk_with_tiers(
        wallet_balance,
        &positions,
        &mark_prices,
        MMR_DEFAULT,
        Some(&*tiers_guard),
    );
    drop(tiers_guard);

    // Surface stale-price events. compute_risk_with_tiers has already
    // forced the account into reduce_only when symbols are missing —
    // we just emit observability here so ops can react.
    if !snapshot.missing_mark_symbols.is_empty() {
        for sym in &snapshot.missing_mark_symbols {
            crate::services::metrics::UNIFIED_MISSING_MARK_PRICE_TOTAL
                .with_label_values(&[sym])
                .inc();
        }
        tracing::warn!(
            "unified-margin missing mark price: user={} symbols={:?} → forced reduce_only",
            addr, snapshot.missing_mark_symbols
        );
    }

    // Dirty-check: skip the UPDATE when nothing meaningful has changed.
    // "Meaningful" = status moved OR uniMMR moved by ≥0.01 absolute OR
    // equity moved by ≥0.1% relative. Cuts ~95% of write traffic for
    // idle accounts (PRD-spec'd 2 s tick rate is for risk evaluation,
    // not for grinding the DB).
    let new_status_str = snapshot.account_status.to_string();
    let prev_row: Option<(Decimal, Option<Decimal>, String)> = sqlx::query_as(
        "SELECT total_equity, uni_mmr, account_status \
         FROM unified_margin_accounts WHERE user_address = $1",
    )
    .bind(addr)
    .fetch_optional(&state.db.pool)
    .await?;

    let should_write = match &prev_row {
        None => true, // first sighting
        Some((prev_eq, prev_mmr, prev_status)) => {
            if prev_status != &new_status_str {
                true
            } else {
                let mmr_delta = match (prev_mmr, snapshot.uni_mmr) {
                    (Some(a), Some(b)) => (a - b).abs(),
                    (None, None) => Decimal::ZERO,
                    _ => Decimal::MAX, // null transition — write
                };
                let equity_rel = if !prev_eq.is_zero() {
                    ((*prev_eq - snapshot.total_equity) / *prev_eq).abs()
                } else {
                    Decimal::MAX
                };
                mmr_delta >= rust_decimal_macros::dec!(0.01)
                    || equity_rel >= rust_decimal_macros::dec!(0.001)
            }
        }
    };

    if should_write {
        sqlx::query(
            "INSERT INTO unified_margin_accounts \
                (user_address, total_equity, available_balance, total_initial_margin, \
                 total_maint_margin, total_unrealized_pnl, uni_mmr, account_status, \
                 is_reduce_only, updated_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW()) \
             ON CONFLICT (user_address) DO UPDATE SET \
                total_equity = EXCLUDED.total_equity, \
                available_balance = EXCLUDED.available_balance, \
                total_initial_margin = EXCLUDED.total_initial_margin, \
                total_maint_margin = EXCLUDED.total_maint_margin, \
                total_unrealized_pnl = EXCLUDED.total_unrealized_pnl, \
                uni_mmr = EXCLUDED.uni_mmr, \
                account_status = EXCLUDED.account_status, \
                is_reduce_only = EXCLUDED.is_reduce_only, \
                updated_at = NOW()",
        )
        .bind(addr)
        .bind(snapshot.total_equity)
        .bind(snapshot.available_balance)
        .bind(snapshot.total_initial_margin)
        .bind(snapshot.total_maint_margin)
        .bind(snapshot.total_unrealized_pnl)
        .bind(snapshot.uni_mmr)
        .bind(&new_status_str)
        .bind(matches!(
            snapshot.account_status,
            UnifiedAccountStatus::ReduceOnly | UnifiedAccountStatus::Liquidating
        ))
        .execute(&state.db.pool)
        .await?;
        crate::services::metrics::UNIFIED_PERSIST_WRITES_TOTAL
            .with_label_values(&["written"])
            .inc();
    } else {
        crate::services::metrics::UNIFIED_PERSIST_WRITES_TOTAL
            .with_label_values(&["skipped_clean"])
            .inc();
    }

    // Detect transition (new_status_str already computed above).
    let transitioned = prev_status_str != new_status_str;

    let now_ms = Utc::now().timestamp_millis();

    // Build the baseline update event (always emit so subscribed
    // front-ends can update uniMMR live without polling).
    let mut event = UnifiedAccountEvent {
        user_address: addr.to_string(),
        event: "update".into(),
        uni_mmr: snapshot.uni_mmr,
        total_equity: snapshot.total_equity,
        available_balance: snapshot.available_balance,
        account_status: new_status_str.clone(),
        reason: None,
        orders_cancelled: None,
        timestamp: now_ms,
    };

    if transitioned {
        event.event = match snapshot.account_status {
            UnifiedAccountStatus::Warning1 => "margin_call".into(),
            UnifiedAccountStatus::Warning2 => "margin_call".into(),
            UnifiedAccountStatus::ReduceOnly => "reduce_only".into(),
            UnifiedAccountStatus::Liquidating => "liquidating".into(),
            UnifiedAccountStatus::Normal => "status_change".into(),
        };
        event.reason = Some(format!(
            "uniMMR={} status: {} → {}",
            snapshot
                .uni_mmr
                .map(|m| m.to_string())
                .unwrap_or_else(|| "N/A".into()),
            prev_status_str,
            new_status_str
        ));

        tracing::warn!(
            "unified-margin status change: user={} {} → {} (uniMMR={:?})",
            addr,
            prev_status_str,
            new_status_str,
            snapshot.uni_mmr
        );

        // Side effect: entering reduce_only / liquidating cancels orders.
        if matches!(
            snapshot.account_status,
            UnifiedAccountStatus::ReduceOnly | UnifiedAccountStatus::Liquidating
        ) {
            let cancelled = enforcement::cancel_all_open_orders(state, addr).await;
            event.orders_cancelled = Some(cancelled);
            if cancelled > 0 {
                // Gauge tracks cumulative count since process start.
                let m = &crate::services::metrics::UNIFIED_REDUCE_ONLY_CANCELLED_TOTAL;
                m.set(m.get() + cancelled as f64);
            }
        }
    }

    // Forced liquidation: fire once per tick while account is in the
    // Liquidating state. `liquidate_one` closes the single worst-PnL
    // position; the next tick will recompute uniMMR and stop once the
    // account recovers above 1.05.
    if matches!(snapshot.account_status, UnifiedAccountStatus::Liquidating) {
        match liquidation::liquidate_one(state, addr, &snapshot).await {
            Ok(Some(step)) => {
                event.event = "liquidation_step".into();
                event.reason = Some(format!(
                    "liquidated {} {} size_usd={} pnl={}",
                    step.symbol, step.side, step.closed_size_usd, step.pnl_realized
                ));
            }
            Ok(None) => {
                tracing::warn!(
                    "unified liquidation: user={} in Liquidating but has no positions",
                    addr
                );
            }
            Err(e) => {
                tracing::error!(
                    "unified liquidation failed: user={} err={}",
                    addr, e
                );
            }
        }
    }

    // Observe uniMMR distribution (skip None / no-positions accounts).
    if let Some(mmr) = snapshot.uni_mmr {
        use rust_decimal::prelude::ToPrimitive;
        if let Some(v) = mmr.to_f64() {
            crate::services::metrics::UNIFIED_UNI_MMR
                .with_label_values(&["all"])
                .observe(v.min(1e6).max(0.0));
        }
    }

    // Best-effort broadcast — `send` only errs if there are no
    // subscribers, which is fine.
    let _ = state.unified_account_sender.send(event);

    Ok(new_status_str_static(&snapshot.account_status))
}

fn new_status_str_static(status: &UnifiedAccountStatus) -> &'static str {
    match status {
        UnifiedAccountStatus::Normal => "normal",
        UnifiedAccountStatus::Warning1 => "warning_1",
        UnifiedAccountStatus::Warning2 => "warning_2",
        UnifiedAccountStatus::ReduceOnly => "reduce_only",
        UnifiedAccountStatus::Liquidating => "liquidating",
    }
}
