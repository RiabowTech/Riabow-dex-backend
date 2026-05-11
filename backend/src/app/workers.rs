//! Background worker tasks
//!
//! Contains all background workers for trade processing, K-line updates, and Redis pub/sub.

use rust_decimal::Decimal;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::Instrument;

use crate::app::state::AppState;
use crate::cache::keys::{self, CacheKey};
use crate::constants::dynamic_fee;
use crate::services::metrics;

/// Start all background workers
pub async fn start_workers(state: Arc<AppState>) {
    start_trade_persistence_worker(state.clone()).await;
    start_kline_update_worker(state.clone()).await;
    start_price_feed_worker(state.clone()).await;
    // Hyperliquid → PriceCache + KLine bridge intentionally removed:
    // PriceCache and K-lines must be driven solely by the matching engine's
    // own trades. Earlier this worker wrote external HL prices into both,
    // which surfaced as the XAUUSDT "$4645 ↔ $0.85" flicker on the frontend
    // (HL spot @182 quoted XAUT0/USDC at ~$0.857, not $/oz gold).
    // /ws/external still streams raw HL data via lazy fetcher startup.
    start_redis_workers(state.clone()).await;
    start_points_workers(state.clone()).await;
    start_fee_adjustment_worker(state.clone()).await;
    start_volume_refresh_worker(state.clone()).await;
    start_withdrawal_expiry_worker(state.clone()).await;
    start_tokio_metrics_worker().await;
    start_orderbook_depth_monitor(state.clone()).await;
    start_engine_db_integrity_monitor(state.clone()).await;
    crate::services::unified_margin::risk_worker::spawn(state.clone());
    crate::services::mm_pool::spawn(state.clone());
    crate::services::points::season_snapshot::spawn(state.clone());
    crate::services::market_config::coingecko_worker::spawn(state.clone());
    crate::services::vip_tier::spawn(state.clone());
    crate::services::binance_kline_sync::spawn(state.clone());
}

/// Periodically collect Tokio runtime metrics and expose via Prometheus
async fn start_tokio_metrics_worker() {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(tokio::time::Duration::from_secs(5));
        tracing::info!("Tokio runtime metrics worker started (5s interval)");

        loop {
            ticker.tick().await;
            metrics::collect_tokio_runtime_metrics();
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            metrics::TASK_LAST_RUN_TIMESTAMP
                .with_label_values(&["tokio-metrics"])
                .set(now as f64);
        }
    }.instrument(tracing::info_span!("tokio-metrics-worker")));
}

/// Periodically collect orderbook depth metrics and expose via Prometheus
/// Monitors for empty orderbooks which indicate all market orders will be cancelled (P0 action item #8)
async fn start_orderbook_depth_monitor(state: Arc<AppState>) {
    let engine = state.matching_engine.clone();

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(tokio::time::Duration::from_secs(10));
        tracing::info!("Orderbook depth monitor started (10s interval)");

        loop {
            ticker.tick().await;

            let timer = metrics::TaskTimer::start("orderbook-depth-monitor", "all");

            let stats = engine.stats();
            metrics::ORDERBOOK_TOTAL_ORDERS.set(stats.total_orders_in_book as f64);

            // Per-symbol metrics
            for symbol_stats in engine.per_symbol_stats() {
                let symbol = &symbol_stats.symbol;

                let bid_depth_f64 = symbol_stats.bid_depth.to_string().parse::<f64>().unwrap_or(0.0);
                let ask_depth_f64 = symbol_stats.ask_depth.to_string().parse::<f64>().unwrap_or(0.0);
                let spread_f64 = symbol_stats.spread.to_string().parse::<f64>().unwrap_or(0.0);

                metrics::ORDERBOOK_BID_DEPTH
                    .with_label_values(&[symbol])
                    .set(bid_depth_f64);
                metrics::ORDERBOOK_ASK_DEPTH
                    .with_label_values(&[symbol])
                    .set(ask_depth_f64);
                metrics::ORDERBOOK_ORDER_COUNT
                    .with_label_values(&[symbol])
                    .set(symbol_stats.order_count as f64);
                metrics::ORDERBOOK_SPREAD
                    .with_label_values(&[symbol])
                    .set(spread_f64);

                // Alert if orderbook is empty
                let is_empty = symbol_stats.order_count == 0;
                metrics::ORDERBOOK_EMPTY
                    .with_label_values(&[symbol])
                    .set(if is_empty { 1.0 } else { 0.0 });

                if is_empty {
                    tracing::warn!(
                        "⚠️ ORDERBOOK EMPTY for {}! All market orders will be cancelled!",
                        symbol
                    );
                }
            }

            timer.success();
            metrics::TASK_LAST_RUN_TIMESTAMP
                .with_label_values(&["orderbook-depth-monitor"])
                .set(chrono::Utc::now().timestamp() as f64);
        }
    }.instrument(tracing::info_span!("orderbook-depth-monitor")));
}

/// Engine ↔ DB integrity probe (PR #63: 2026-04-26).
///
/// Periodically scans `orders` rows the DB believes are still resting
/// (status `open` / `partially_filled`, type `limit`) and asks the live
/// orderbook whether each id is still indexed. Counts agreement vs
/// silent-remove discrepancy per symbol; logs a structured warn for
/// each missing id so ops can correlate with the place- and
/// cancel-side logs.
///
/// This exists because PR #59 concluded the `cancel_order: TRUE
/// ORPHAN` warn was 95% benign double-cancel and downgraded it. T46c
/// repro 2026-04-26 (place far-from-market LIMIT BUY → modify or
/// DELETE returns -2011 ~30-50% of the time, no trade rows for the
/// order) showed the cohort hides a real silent-remove path.
/// `cancel_not_found_total` only fires when something tries to cancel,
/// missing the population that's silently removed but never
/// cancel-attempted; this probe samples that population directly.
async fn start_engine_db_integrity_monitor(state: Arc<AppState>) {
    let pool = state.db.pool.clone();
    let engine = state.matching_engine.clone();
    let shard = state.sharding.clone();

    tokio::spawn(async move {
        // 30s strikes a balance: short enough to catch orders inside their
        // typical resting lifetime, long enough that the row scan stays cheap
        // even under MM bursts. The 5min lookback keeps the row set small.
        let mut ticker = tokio::time::interval(tokio::time::Duration::from_secs(30));
        tracing::info!(
            "Engine↔DB integrity monitor started (30s interval, 5min lookback, sharding_enabled={})",
            shard.enabled && shard.replica_count > 1
        );

        loop {
            ticker.tick().await;
            let timer = metrics::TaskTimer::start("engine-db-integrity-monitor", "all");

            let rows: Vec<(uuid::Uuid, String, chrono::DateTime<chrono::Utc>)> = match sqlx::query_as(
                "SELECT id, symbol, created_at \
                 FROM orders \
                 WHERE status IN ('open','partially_filled') \
                   AND order_type = 'limit' \
                   AND created_at > NOW() - INTERVAL '5 minutes' \
                 ORDER BY created_at DESC \
                 LIMIT 500",
            )
            .fetch_all(&pool)
            .await
            {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!("integrity-monitor: row scan failed: {}", e);
                    timer.failure(&e.to_string());
                    continue;
                }
            };

            // Aggregate per (symbol, kind) so we batch counter increments.
            let mut agg: std::collections::HashMap<(String, &'static str), u64> =
                std::collections::HashMap::new();
            let mut warned = 0usize;
            const WARN_LIMIT_PER_TICK: usize = 20;

            for (oid, symbol, created_at) in &rows {
                // Sharding ownership filter — with multi-replica routing on,
                // each pod only owns a deterministic slice of symbols. Orders
                // for non-owned symbols are correctly absent from this pod's
                // engine; counting them as `missing_from_engine` would be a
                // false positive. The sharding-aware recovery
                // (engine::recover_orders_from_db) already enforces this on
                // boot; the probe must agree at runtime.
                if shard.enabled && shard.replica_count > 1
                    && shard.owner_for(symbol) != shard.my_ordinal
                {
                    continue;
                }

                let book = match engine.get_orderbook_ref(symbol) {
                    Some(b) => b,
                    None => continue,
                };
                let in_engine = book.has_order(oid);
                let kind = if in_engine { "present" } else { "missing_from_engine" };
                *agg.entry((symbol.clone(), kind)).or_insert(0) += 1;

                if !in_engine && warned < WARN_LIMIT_PER_TICK {
                    let age_ms = chrono::Utc::now()
                        .signed_duration_since(*created_at)
                        .num_milliseconds();
                    tracing::warn!(
                        "integrity-monitor: silent-remove detected — order {} symbol={} status_in_db=open/partial age_ms={} \
                         (DB believes resting, engine.has_order=false). Bumping engine_db_integrity_total{{kind=missing_from_engine}}.",
                        oid, symbol, age_ms
                    );
                    warned += 1;
                }
            }

            for ((symbol, kind), n) in &agg {
                metrics::ENGINE_DB_INTEGRITY_TOTAL
                    .with_label_values(&[symbol.as_str(), kind])
                    .inc_by(*n as f64);
            }

            // Leak-side check: per-symbol engine size vs DB-open size.
            // Discovered 2026-04-26 alongside the silent-remove finding —
            // BNBUSDT engine reported 10211 orders while DB had 4506
            // status-open rows, a 2.27× over-count that pointed at a
            // separate cleanup-side bug. Expose both as gauges instead
            // of enumerating leaked ids (engine doesn't have a public
            // order_index iterator; adding one means a write-lock pause
            // across the whole book).
            let db_counts: Vec<(String, i64)> = match sqlx::query_as(
                "SELECT symbol, COUNT(*)::bigint \
                 FROM orders \
                 WHERE status IN ('open','partially_filled') AND order_type = 'limit' \
                 GROUP BY symbol",
            )
            .fetch_all(&pool)
            .await
            {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!("integrity-monitor: DB-count query failed: {}", e);
                    Vec::new()
                }
            };
            // Same ownership filter as above. Without this, the db_open
            // gauge for B-owned symbols would be the global count while
            // EC2-A's `engine` gauge for that symbol would be 0,
            // producing an apparent infinite over-count gap. We only
            // emit gauges for symbols this pod owns; the *other* pod
            // emits its own slice. Sum across pods in queries (or just
            // pick the owner) to reconstruct the global view.
            for (symbol, db_count) in &db_counts {
                if shard.enabled && shard.replica_count > 1
                    && shard.owner_for(symbol) != shard.my_ordinal
                {
                    continue;
                }
                metrics::ENGINE_DB_SIZE_GAUGE
                    .with_label_values(&[symbol, "db_open"])
                    .set(*db_count as f64);
            }
            for sym_stats in engine.per_symbol_stats() {
                if shard.enabled && shard.replica_count > 1
                    && shard.owner_for(&sym_stats.symbol) != shard.my_ordinal
                {
                    continue;
                }
                metrics::ENGINE_DB_SIZE_GAUGE
                    .with_label_values(&[&sym_stats.symbol, "engine"])
                    .set(sym_stats.order_count as f64);
            }

            timer.success();
            metrics::TASK_LAST_RUN_TIMESTAMP
                .with_label_values(&["engine-db-integrity-monitor"])
                .set(chrono::Utc::now().timestamp() as f64);
        }
    }.instrument(tracing::info_span!("engine-db-integrity-monitor")));
}

/// Start points calculation worker for trades
/// NOTE: Trade persistence is handled by OrderFlowOrchestrator.start_persistence_worker()
/// This worker ONLY handles points calculation for trades
async fn start_trade_persistence_worker(state: Arc<AppState>) {
    let mut trade_receiver = state.matching_engine.subscribe_trades();

    tokio::spawn(async move {
        tracing::info!("Trade points calculation worker started (persistence handled by orchestrator)");

        // Points calc runs synchronously in this loop. Previous implementation
        // spawned a task per trade gated by Semaphore::new(5); under MM load
        // new spawns piled up unbounded on the semaphore (~22k tokio tasks
        // alive on 2026-04-22), because arrivals outran the 5-slot drain.
        // Running inline lets the broadcast channel provide natural backpressure:
        // if points calc can't keep up, Lagged(n) fires below and
        // CHANNEL_LAGGED_TOTAL{worker="trade-points-worker"} makes the drop
        // rate observable. Trade persistence is NOT on this path (it's in the
        // orchestrator's mpsc worker), so lag here only loses some points
        // calculations, never trade rows.

        let mut processed_count = 0u64;
        let mut last_stats_log = std::time::Instant::now();

        loop {
            let trade_event = match trade_receiver.recv().await {
                Ok(event) => event,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!(
                        "⚠️ Trade points worker lagged {} messages - consider scaling",
                        n
                    );
                    metrics::CHANNEL_LAGGED_TOTAL
                        .with_label_values(&["trade-points-worker"])
                        .inc_by(n as f64);
                    continue;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    tracing::warn!("Trade broadcast channel closed, stopping points worker");
                    break;
                }
            };

            processed_count += 1;
            let timer = metrics::TaskTimer::start("trade-points-worker", &trade_event.symbol);
            metrics::TRADE_PERSISTENCE_TOTAL
                .with_label_values(&["processed"])
                .inc();
            metrics::TASK_LAST_RUN_TIMESTAMP
                .with_label_values(&["trade-points-worker"])
                .set(chrono::Utc::now().timestamp() as f64);

            // =================================================================
            // Points System Integration (inline, no per-trade spawn)
            // =================================================================
            let points_service = &state.points_service;
            if let Ok(Some(epoch)) = points_service.get_active_epoch().await {
                use crate::models::points::TradeRole;

                // 成交额（名义价值）
                let volume = trade_event.price * trade_event.amount;
                let epoch_number = epoch.epoch_number;

                // 1. Maker TP
                if let Err(e) = points_service.calculate_trading_points(
                    &trade_event.maker_address,
                    epoch_number,
                    volume,
                    trade_event.trade_id,
                    TradeRole::Maker,
                ).await {
                    tracing::warn!("Maker TP failed for {}: {}", trade_event.maker_address, e);
                }

                // 2. Taker TP
                if let Err(e) = points_service.calculate_trading_points(
                    &trade_event.taker_address,
                    epoch_number,
                    volume,
                    trade_event.trade_id,
                    TradeRole::Taker,
                ).await {
                    tracing::warn!("Taker TP failed for {}: {}", trade_event.taker_address, e);
                }

                // 3. RP 触发检查（Maker）
                if let Err(e) = points_service.check_and_trigger_rp(
                    &trade_event.maker_address,
                    epoch_number,
                    volume,
                    trade_event.trade_id,
                ).await {
                    tracing::warn!("Maker RP trigger failed for {}: {}", trade_event.maker_address, e);
                }

                // 4. RP 触发检查（Taker）
                if let Err(e) = points_service.check_and_trigger_rp(
                    &trade_event.taker_address,
                    epoch_number,
                    volume,
                    trade_event.trade_id,
                ).await {
                    tracing::warn!("Taker RP trigger failed for {}: {}", trade_event.taker_address, e);
                }
            }
            timer.success();

            // Log statistics every 5 minutes
            if last_stats_log.elapsed().as_secs() >= 300 {
                tracing::info!(
                    "📊 Trade points calculation stats: {} trades processed",
                    processed_count
                );
                last_stats_log = std::time::Instant::now();
            }
        }
        tracing::warn!(
            "Trade points calculation worker stopped - total: {} trades processed",
            processed_count
        );
    }.instrument(tracing::info_span!("trade-points-worker")));
    tracing::info!("Trade points calculation worker spawned");
}

/// Start K-line update worker
/// Listens to trade events and updates K-line data in real-time
async fn start_kline_update_worker(state: Arc<AppState>) {
    let mut kline_trade_receiver = state.matching_engine.subscribe_trades();
    let kline_service_clone = state.kline_service.clone();

    tokio::spawn(async move {
        tracing::info!("K-line update worker started");

        loop {
            match kline_trade_receiver.recv().await {
                Ok(trade_event) => {
                    let timer = metrics::TaskTimer::start("kline-update", &trade_event.symbol);
                    kline_service_clone.process_trade(&trade_event).await;
                    timer.success();
                    metrics::TASK_LAST_RUN_TIMESTAMP
                        .with_label_values(&["kline-update"])
                        .set(chrono::Utc::now().timestamp() as f64);
                    tracing::debug!(
                        "Updated K-lines for {} trade: price={}, amount={}",
                        trade_event.symbol,
                        trade_event.price,
                        trade_event.amount
                    );
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!("⚠️ K-line worker lagged {} trade events", n);
                    metrics::CHANNEL_LAGGED_TOTAL
                        .with_label_values(&["kline-update"])
                        .inc_by(n as f64);
                    continue;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    tracing::warn!("K-line update worker stopped - channel closed");
                    break;
                }
            }
        }
    }.instrument(tracing::info_span!("kline-update-worker")));
    tracing::info!("K-line update worker spawned - real-time K-line generation enabled");
}

/// Start price feed worker
/// Listens to trade events and updates price feed service
async fn start_price_feed_worker(state: Arc<AppState>) {
    let price_feed_for_trades = state.price_feed_service.clone();
    let mut trade_receiver = state.matching_engine.subscribe_trades();

    tokio::spawn(async move {
        tracing::info!("📊 Started price feed update loop from internal trades");

        loop {
            match trade_receiver.recv().await {
                Ok(trade_event) => {
                    let timer = metrics::TaskTimer::start("price-feed", &trade_event.symbol);
                    price_feed_for_trades
                        .update_price_from_trade(
                            &trade_event.symbol,
                            trade_event.price,
                            trade_event.amount,
                        )
                        .await;
                    timer.success();

                    metrics::TASK_LAST_RUN_TIMESTAMP
                        .with_label_values(&["price-feed"])
                        .set(chrono::Utc::now().timestamp() as f64);

                    tracing::debug!(
                        "📈 Price updated from trade: {}@{} (amount: {})",
                        trade_event.symbol,
                        trade_event.price,
                        trade_event.amount
                    );
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!("⚠️  Price feed lagged {} trade events", n);
                    metrics::CHANNEL_LAGGED_TOTAL
                        .with_label_values(&["price-feed"])
                        .inc_by(n as f64);
                }
                Err(broadcast::error::RecvError::Closed) => {
                    tracing::error!("❌ Trade event channel closed, price feed will stop updating");
                    break;
                }
            }
        }
    }.instrument(tracing::info_span!("price-feed-worker")));

    tracing::info!("Price feed: Using internal trade data (prices updated from each trade)");
}

/// Start Redis pub/sub workers for trades and orderbook
async fn start_redis_workers(state: Arc<AppState>) {
    if !state.cache.is_available() {
        tracing::warn!("Redis pub/sub workers not started - Redis is unavailable");
        return;
    }

    // Trade pub/sub worker
    let mut redis_trade_receiver = state.matching_engine.subscribe_trades();
    let cache_clone = state.cache.clone();
    tokio::spawn(async move {
        tracing::info!("Redis pub/sub worker started");

        loop {
            match redis_trade_receiver.recv().await {
                Ok(trade_event) => {
                    if let Some(pubsub) = cache_clone.pubsub_opt() {
                        let publisher = pubsub.publisher();
                        match publisher
                            .publish_trade(&trade_event.symbol, &trade_event)
                            .await
                        {
                            Ok(n) => {
                                tracing::debug!(
                                    "Published trade to Redis ({} subscribers): symbol={}, price={}, amount={}",
                                    n,
                                    trade_event.symbol,
                                    trade_event.price,
                                    trade_event.amount
                                );
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "Failed to publish trade to Redis: {} (symbol={}, price={})",
                                    e,
                                    trade_event.symbol,
                                    trade_event.price
                                );
                            }
                        }
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!("⚠️ Redis trade pub/sub worker lagged {} messages", n);
                    metrics::CHANNEL_LAGGED_TOTAL
                        .with_label_values(&["redis-trade-pubsub"])
                        .inc_by(n as f64);
                    continue;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    tracing::warn!("Redis pub/sub worker stopped - channel closed");
                    break;
                }
            }
        }
    }.instrument(tracing::info_span!("redis-trade-pubsub")));
    tracing::info!("Redis pub/sub worker spawned - trade events will be published to Redis");

    // Orderbook pub/sub worker
    let mut redis_orderbook_receiver = state.matching_engine.subscribe_orderbook();
    let cache_clone = state.cache.clone();
    let shard_for_orderbook = state.sharding.clone();
    tokio::spawn(async move {
        tracing::info!(
            "Redis orderbook pub/sub worker started (sharding_enabled={})",
            shard_for_orderbook.enabled && shard_for_orderbook.replica_count > 1
        );

        loop {
            match redis_orderbook_receiver.recv().await {
                Ok(orderbook_update) => {
                    // Sharding ownership filter — only the owning pod writes
                    // the per-symbol orderbook to Redis. Without this, any
                    // engine event on the non-owning pod (trigger fires,
                    // liquidation, reduce-only enforcement, drift fallback)
                    // emits a snapshot of B's empty engine for an A-owned
                    // symbol; `set_orderbook` then clear-then-writes Redis,
                    // wiping A's correct data. With sharding off, every
                    // pod owns everything → predicate always false →
                    // pre-PoC behaviour preserved.
                    if shard_for_orderbook.enabled
                        && shard_for_orderbook.replica_count > 1
                        && shard_for_orderbook.owner_for(&orderbook_update.symbol)
                            != shard_for_orderbook.my_ordinal
                    {
                        continue;
                    }

                    // Write orderbook data to cache sorted sets for REST API queries
                    if let Some(ob_cache) = cache_clone.orderbook_opt() {
                        let mut parsed_bids = Vec::with_capacity(orderbook_update.bids.len());
                        let mut parsed_asks = Vec::with_capacity(orderbook_update.asks.len());
                        for pair in &orderbook_update.bids {
                            if let (Ok(price), Ok(amount)) = (pair[0].parse::<rust_decimal::Decimal>(), pair[1].parse::<rust_decimal::Decimal>()) {
                                parsed_bids.push(crate::cache::orderbook_cache::PriceLevel { price, amount });
                            }
                        }
                        for pair in &orderbook_update.asks {
                            if let (Ok(price), Ok(amount)) = (pair[0].parse::<rust_decimal::Decimal>(), pair[1].parse::<rust_decimal::Decimal>()) {
                                parsed_asks.push(crate::cache::orderbook_cache::PriceLevel { price, amount });
                            }
                        }
                        if let Err(e) = ob_cache.set_orderbook(&orderbook_update.symbol, &parsed_bids, &parsed_asks).await {
                            tracing::warn!(
                                "Failed to update orderbook cache: {} (symbol={})",
                                e,
                                orderbook_update.symbol
                            );
                        }
                    }

                    // Publish to Redis pub/sub for WebSocket clients
                    if let Some(pubsub) = cache_clone.pubsub_opt() {
                        let publisher = pubsub.publisher();
                        match publisher
                            .publish_orderbook(&orderbook_update.symbol, &orderbook_update)
                            .await
                        {
                            Ok(n) => {
                                tracing::debug!(
                                    "Published orderbook to Redis ({} subscribers): symbol={}, bids={}, asks={}",
                                    n,
                                    orderbook_update.symbol,
                                    orderbook_update.bids.len(),
                                    orderbook_update.asks.len()
                                );
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "Failed to publish orderbook to Redis: {} (symbol={})",
                                    e,
                                    orderbook_update.symbol
                                );
                            }
                        }
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!("⚠️ Redis orderbook pub/sub worker lagged {} messages", n);
                    metrics::CHANNEL_LAGGED_TOTAL
                        .with_label_values(&["redis-orderbook-pubsub"])
                        .inc_by(n as f64);
                    continue;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    tracing::warn!("Redis orderbook pub/sub worker stopped - channel closed");
                    break;
                }
            }
        }
    }.instrument(tracing::info_span!("redis-orderbook-pubsub")));
    tracing::info!(
        "Redis orderbook pub/sub worker spawned - orderbook updates will be published to Redis"
    );
}

/// Start Points System background workers
async fn start_points_workers(state: Arc<AppState>) {
    // ---- Earn Level 每日 UTC 00:00 刷新 ----
    {
        let ps = state.points_service.clone();
        tokio::spawn(async move {
            use chrono::{Duration, Utc};
            tracing::info!("📊 Points System: Earn Level daily refresh task started");
            loop {
                let now = Utc::now();
                let next_midnight = (now + Duration::days(1))
                    .date_naive()
                    .and_hms_opt(0, 0, 0)
                    .unwrap()
                    .and_utc();
                let wait = (next_midnight - now).to_std().unwrap_or_default();
                tracing::info!("⏰ Next Earn Level refresh at {} (in {}s)", next_midnight, wait.as_secs());
                tokio::time::sleep(wait).await;

                tracing::info!("🎯 Running daily Earn Level refresh...");
                match ps.run_daily_earn_level_refresh().await {
                    Ok(n) => tracing::info!("✅ Earn Level refresh done: {} users updated", n),
                    Err(e) => tracing::error!("❌ Earn Level refresh failed: {}", e),
                }
            }
        }.instrument(tracing::info_span!("earn-level-daily")));
    }

    // ---- Staking 每日任务（Phase1 disabled，框架保留）----
    {
        let points_service = state.points_service.clone();
        tokio::spawn(async move {
            use chrono::{Duration, Utc};
            tracing::info!("📊 Points System: Staking points daily task started (Phase1 disabled)");
            loop {
                let now = Utc::now();
                let next_midnight = (now + Duration::days(1))
                    .date_naive()
                    .and_hms_opt(0, 0, 0)
                    .unwrap()
                    .and_utc();
                let wait = (next_midnight - now).to_std().unwrap_or_default();
                tokio::time::sleep(wait).await;

                match points_service.get_active_epoch().await {
                    Ok(Some(epoch)) => {
                        match points_service.calculate_staking_points_batch(epoch.epoch_number).await {
                            Ok(count) => tracing::info!("✅ Staking points: {} users", count),
                            Err(e) => tracing::error!("❌ Staking points failed: {}", e),
                        }
                    }
                    Ok(None) => tracing::warn!("⚠️  No active epoch, skip staking points"),
                    Err(e) => tracing::error!("❌ Get active epoch failed: {}", e),
                }
            }
        }.instrument(tracing::info_span!("staking-points-daily")));
    }

    // ---- 每日积分增量排行榜 每5分钟刷新 ----
    {
        let ps = state.points_service.clone();
        tokio::spawn(async move {
            // 错开启动，避免与其他 worker 竞争 DB 连接。
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
            tracing::info!("📊 Points System: Daily leaderboard refresh task started (5 min interval)");

            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(300));
            ticker.tick().await; // skip immediate first tick

            loop {
                ticker.tick().await;

                let epoch_number = match ps.get_active_epoch().await {
                    Ok(Some(e)) => e.epoch_number,
                    Ok(None) => {
                        tracing::debug!("No active epoch, skipping daily leaderboard refresh");
                        continue;
                    }
                    Err(e) => {
                        tracing::error!("Failed to get active epoch for daily leaderboard: {}", e);
                        continue;
                    }
                };

                metrics::TASK_LAST_RUN_TIMESTAMP
                    .with_label_values(&["daily-leaderboard-refresh"])
                    .set(chrono::Utc::now().timestamp() as f64);

                match ps.refresh_daily_leaderboard(epoch_number).await {
                    Ok(n) => tracing::info!("✅ Daily leaderboard refreshed: {} entries", n),
                    Err(e) => tracing::error!("❌ Daily leaderboard refresh failed: {}", e),
                }
            }
        }.instrument(tracing::info_span!("daily-leaderboard-refresh")));
    }

    tracing::info!("Points System workers initialized");
}

/// Start volume refresh worker
/// Periodically recomputes 24h trade volume from the database to keep the
/// in-memory price cache accurate (the per-trade accumulation can drift).
async fn start_volume_refresh_worker(state: Arc<AppState>) {
    let price_feed = state.price_feed_service.clone();
    let pool = state.db.pool.clone();

    tokio::spawn(async move {
        // Initial delay to let other services start
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;

        tracing::info!("📊 Volume refresh worker started - recomputing 24h volumes every 60s");

        loop {
            let timer = metrics::TaskTimer::start("volume-refresh", "all");

            match price_feed.refresh_volumes_from_db(&pool).await {
                Ok(_) => timer.success(),
                Err(e) => {
                    tracing::warn!("Failed to refresh volumes from DB: {}", e);
                    timer.failure(&e.to_string());
                }
            }

            metrics::TASK_LAST_RUN_TIMESTAMP
                .with_label_values(&["volume-refresh"])
                .set(chrono::Utc::now().timestamp() as f64);

            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        }
    }.instrument(tracing::info_span!("volume-refresh")));

    tracing::info!("Volume refresh worker spawned");
}

/// Start withdrawal expiry worker
/// Periodically checks for expired withdrawals and unfreezes the funds
async fn start_withdrawal_expiry_worker(state: Arc<AppState>) {
    let withdraw_service = state.withdraw_service.clone();

    tokio::spawn(async move {
        tracing::info!("💰 Withdrawal expiry worker started - checking every 60 seconds");

        loop {
            // Wait 60 seconds between checks
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;

            let timer = metrics::TaskTimer::start("withdrawal-expiry", "all");

            // Process expired withdrawals
            match withdraw_service.process_expired_withdrawals().await {
                Ok(count) => {
                    if count > 0 {
                        tracing::info!(
                            "✅ Processed {} expired withdrawals - funds unfrozen",
                            count
                        );
                        metrics::WITHDRAWAL_EXPIRED_TOTAL
                            .with_label_values(&["success"])
                            .inc_by(count as f64);
                    }
                    timer.success();
                }
                Err(e) => {
                    tracing::error!("❌ Failed to process expired withdrawals: {}", e);
                    timer.failure(&e.to_string());
                }
            }

            metrics::TASK_LAST_RUN_TIMESTAMP
                .with_label_values(&["withdrawal-expiry"])
                .set(chrono::Utc::now().timestamp() as f64);
        }
    }.instrument(tracing::info_span!("withdrawal-expiry")));

    tracing::info!("Withdrawal expiry worker spawned");
}

/// Start Hyperliquid price sync worker
/// Start dynamic fee adjustment worker
/// Runs every 60 seconds, calculates OI imbalance per market,
/// adjusts taker fees, writes to Redis and saves fee snapshots.
async fn start_fee_adjustment_worker(state: Arc<AppState>) {
    let market_config_service = state.market_config_service.clone();
    let cache = state.cache.clone();

    tokio::spawn(async move {
        tracing::info!("Dynamic fee adjustment worker started - running every {}s", dynamic_fee::ADJUSTMENT_INTERVAL_SECS);

        let mut interval = tokio::time::interval(std::time::Duration::from_secs(dynamic_fee::ADJUSTMENT_INTERVAL_SECS));
        // First tick fires immediately, skip it to let services initialize
        interval.tick().await;

        loop {
            interval.tick().await;

            metrics::TASK_LAST_RUN_TIMESTAMP
                .with_label_values(&["fee-adjustment"])
                .set(chrono::Utc::now().timestamp() as f64);

            let configs = market_config_service.get_all_active().await;

            for config in &configs {
                if !config.auto_fee_adjust_enabled {
                    continue;
                }

                let timer = metrics::TaskTimer::start("fee-adjustment", &config.symbol);

                // 1. Query open interest from DB
                let (long_oi, short_oi) = match market_config_service.get_open_interest(&config.symbol).await {
                    Ok(oi) => oi,
                    Err(e) => {
                        tracing::warn!("Failed to get OI for {}: {}", config.symbol, e);
                        timer.failure(&format!("{}", e));
                        continue;
                    }
                };

                let total_oi = long_oi + short_oi;

                // 2. Calculate imbalance ratio
                let imbalance = if total_oi > Decimal::ZERO {
                    (long_oi - short_oi) / total_oi
                } else {
                    Decimal::ZERO
                };

                // 3. Calculate new dynamic fees
                let k = config.fee_sensitivity;
                let base = config.base_taker_fee_rate;
                let one = Decimal::ONE;

                let long_taker_raw = base * (one + k * imbalance);
                let short_taker_raw = base * (one - k * imbalance);

                let long_taker = clamp_decimal(long_taker_raw, config.fee_floor, config.fee_ceiling);
                let short_taker = clamp_decimal(short_taker_raw, config.fee_floor, config.fee_ceiling);
                let maker_fee = config.base_maker_fee_rate;

                // 4. Write to Redis (with 90s TTL, degrades to base fee if Redis fails)
                if let Some(redis) = cache.redis() {
                    let symbol = &config.symbol;
                    let ttl = keys::ttl::DYNAMIC_FEE;

                    let _ = redis.set_ex(
                        &CacheKey::dynamic_long_taker_fee(symbol),
                        long_taker.to_string(),
                        ttl,
                    ).await;

                    let _ = redis.set_ex(
                        &CacheKey::dynamic_short_taker_fee(symbol),
                        short_taker.to_string(),
                        ttl,
                    ).await;

                    let _ = redis.set_ex(
                        &CacheKey::dynamic_maker_fee(symbol),
                        maker_fee.to_string(),
                        ttl,
                    ).await;

                    let _ = redis.set_ex(
                        &CacheKey::dynamic_imbalance(symbol),
                        imbalance.to_string(),
                        ttl,
                    ).await;

                    tracing::debug!(
                        "Fee adjusted for {}: imbalance={:.4}, long_taker={:.6}, short_taker={:.6}",
                        symbol, imbalance, long_taker, short_taker
                    );
                }

                // Record fee metrics
                use rust_decimal::prelude::ToPrimitive;
                metrics::FEE_IMBALANCE_RATIO
                    .with_label_values(&[&config.symbol])
                    .set(imbalance.to_f64().unwrap_or(0.0));
                metrics::FEE_LONG_TAKER
                    .with_label_values(&[&config.symbol])
                    .set(long_taker.to_f64().unwrap_or(0.0));
                metrics::FEE_SHORT_TAKER
                    .with_label_values(&[&config.symbol])
                    .set(short_taker.to_f64().unwrap_or(0.0));
                timer.success();

                // 5. Save fee snapshot to DB (async, non-blocking)
                let mcs = market_config_service.clone();
                let symbol = config.symbol.clone();
                let snap_symbol = symbol.clone();
                tokio::spawn(async move {
                    if let Err(e) = mcs.save_fee_snapshot(
                        &symbol,
                        long_oi,
                        short_oi,
                        imbalance,
                        long_taker,
                        short_taker,
                        maker_fee,
                    ).await {
                        tracing::warn!("Failed to save fee snapshot for {}: {}", symbol, e);
                    }
                }.instrument(tracing::info_span!("fee-snapshot", symbol = %snap_symbol)));
            }

            tracing::debug!(
                "Fee adjustment cycle completed for {} active markets",
                configs.len()
            );
        }
    }.instrument(tracing::info_span!("dynamic-fee-adjustment")));

    tracing::info!("Dynamic fee adjustment worker spawned");
}

/// Clamp a Decimal value between min and max
fn clamp_decimal(value: Decimal, min: Decimal, max: Decimal) -> Decimal {
    if value < min {
        min
    } else if value > max {
        max
    } else {
        value
    }
}

