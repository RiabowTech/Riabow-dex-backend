//! Keeper Service - Automated Order Execution
//!
//! This service monitors trigger orders and automatically executes them
//! through the matching engine when price conditions are met.
//! Similar to GMX V2's keeper system.

use anyhow::Result;
use rust_decimal::Decimal;
use sqlx::PgPool;
use std::sync::Arc;
use tokio::time::{interval, Duration};
use tracing::Instrument;
use uuid::Uuid;

use crate::models::{OrderSide as ModelOrderSide, OrderType};
use crate::services::matching::{MatchingEngine, Side as MatchingSide, OrderType as MatchingOrderType, TimeInForce as MatchingTimeInForce};
use crate::services::metrics;
use crate::services::price_feed::PriceFeedService;
use crate::services::trigger_orders::{OrderSide, TriggerOrder, TriggerOrderStatus, TriggerOrderType};

/// Result of a single market scan for metrics collection
struct ScanResult {
    matched: u64,
    pending_buy: u64,
    pending_sell: u64,
}

/// Keeper Service for automated order execution
pub struct KeeperService {
    pool: PgPool,
    matching_engine: Arc<MatchingEngine>,
    price_feed_service: Arc<PriceFeedService>,
    check_interval_ms: u64,
}

impl KeeperService {
    pub fn new(
        pool: PgPool,
        matching_engine: Arc<MatchingEngine>,
        price_feed_service: Arc<PriceFeedService>,
    ) -> Self {
        Self {
            pool,
            matching_engine,
            price_feed_service,
            check_interval_ms: 500,
        }
    }

    /// Maximum number of active trigger orders the system is designed to handle.
    /// Used for queue capacity alerting (keeper_trigger_queue_capacity metric).
    const QUEUE_CAPACITY: f64 = 10_000.0;

    /// Start the keeper monitoring loop
    pub async fn start(self: Arc<Self>, markets: Vec<String>) {
        tracing::info!("Starting Keeper service for markets: {:?}", markets);

        // Set queue capacity gauge once at startup
        metrics::KEEPER_TRIGGER_QUEUE_CAPACITY.set(Self::QUEUE_CAPACITY);

        // Start trigger order monitoring loop
        let service = self.clone();
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_millis(service.check_interval_ms));
            let mut last_run_at = std::time::Instant::now();

            loop {
                ticker.tick().await;

                // --- 2.2 Task health: heartbeat & scan interval ---
                let now_instant = std::time::Instant::now();
                let interval_secs = now_instant.duration_since(last_run_at).as_secs_f64();
                let now_ts = chrono::Utc::now().timestamp();
                metrics::TASK_LAST_RUN_TIMESTAMP
                    .with_label_values(&["keeper-trigger-orders"])
                    .set(now_ts as f64);
                metrics::KEEPER_TRIGGER_SCAN_INTERVAL.set(interval_secs);
                last_run_at = now_instant;

                // --- 2.3 Scan execution: time the full cycle ---
                let cycle_start = std::time::Instant::now();
                let mut total_matched: u64 = 0;
                let mut total_pending_buy: u64 = 0;
                let mut total_pending_sell: u64 = 0;

                for market in &markets {
                    let market_start = std::time::Instant::now();
                    match service.process_market_instrumented(market).await {
                        Ok(scan_result) => {
                            total_matched += scan_result.matched;
                            total_pending_buy += scan_result.pending_buy;
                            total_pending_sell += scan_result.pending_sell;
                            metrics::KEEPER_TRIGGER_ACTIVE_PER_MARKET
                                .with_label_values(&[market])
                                .set((scan_result.pending_buy + scan_result.pending_sell) as f64);
                            metrics::KEEPER_TRIGGER_SCAN_DURATION
                                .with_label_values(&["per_market"])
                                .observe(market_start.elapsed().as_secs_f64());
                        }
                        Err(e) => {
                            tracing::error!("Keeper error for {}: {:?}", market, e);
                            metrics::TASK_FAILED_TOTAL
                                .with_label_values(&["keeper-trigger-orders", market, &metrics::classify_trigger_error(&format!("{:?}", e))])
                                .inc();
                        }
                    }
                }

                // --- Record cycle-level metrics ---
                let total_pending = (total_pending_buy + total_pending_sell) as f64;
                metrics::KEEPER_TRIGGER_SCAN_DURATION
                    .with_label_values(&["full_cycle"])
                    .observe(cycle_start.elapsed().as_secs_f64());
                metrics::KEEPER_TRIGGER_MATCHED_PER_SCAN.set(total_matched as f64);
                metrics::KEEPER_TRIGGER_PENDING_ORDERS
                    .with_label_values(&["buy"])
                    .set(total_pending_buy as f64);
                metrics::KEEPER_TRIGGER_PENDING_ORDERS
                    .with_label_values(&["sell"])
                    .set(total_pending_sell as f64);
                metrics::KEEPER_TRIGGER_PENDING_ORDERS_TOTAL.set(total_pending);

                // --- 2.1 Queue depth (mirrors pending total for queue-centric alerting) ---
                metrics::KEEPER_TRIGGER_QUEUE_DEPTH.set(total_pending);
            }
        }.instrument(tracing::info_span!("keeper-trigger-orders")));

        // Start referral tier update loop (every hour)
        let service = self.clone();
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(3600)); // 1 hour

            loop {
                ticker.tick().await;

                if let Err(e) = service.update_referral_tiers().await {
                    tracing::error!("Failed to update referral tiers: {:?}", e);
                }
            }
        }.instrument(tracing::info_span!("keeper-referral-tiers")));

        // Fresh-orphan detection loop (every 5 min).
        //
        // Detects orders marked 'filled' with no corresponding trade row. In a
        // healthy pipeline this must be zero. Intentionally NO auto-fix — the
        // only correct response is for an engineer to look at why the trade
        // persistence path is dropping events. Emits to ORPHAN_ORDERS_RECENT
        // gauge for Prometheus/Grafana alerting; also logs a warn! so it is
        // visible in Loki.
        //
        // Window: `updated_at` between 1 and 10 minutes ago. The 1-minute
        // lower bound gives the async persistence worker a grace period to
        // write the `trades` row after `order.rs` synchronously flips
        // `orders.status='filled'`. Without this, a busy pipeline whose
        // queue sits at a few hundred briefly flashes 1–5 orphans every
        // detector tick simply because the worker hasn't gotten to those
        // trades yet — a false-positive that masks real stalls. Using
        // `updated_at` rather than `created_at` also correctly scopes to
        // when the order reached 'filled', since `persist_trade_with_tx`
        // bumps `updated_at` on the maker fill flip.
        //
        // Tick interval: 5 min. The window must be ≥ tick + lower bound to
        // guarantee every row is seen at least once before it ages out, so
        // the SQL below uses a 1–10 min window (4 min slack over the tick).
        // Detection latency: ~5 min post-fill; Grafana alert
        // (`> 0 for 2m`) triggers within ~7 min of an actual orphan.
        //
        // History:
        //   Was 60s; reduced 2026-04-26 to 5 min to cut sustained read IOPS.
        //   Bumped to 30 min + 1-35 min window on 2026-04-30 after the
        //   5-min cadence still triggered TigerCloudHighDiskReadIOPS — at
        //   the time the planner did `Parallel Index Scan using orders_pkey`
        //   over all 236M orders rows because no index covered
        //   (status, updated_at). 31% of database lifetime block reads.
        //   Restored to 5 min + 1-10 min window on 2026-05-02 after
        //   `idx_orders_orphan_detector` (partial btree on `updated_at`
        //   WHERE status='filled' AND filled_amount>0 AND signature IS
        //   DISTINCT FROM 'close-position') was created. The planner now
        //   uses a Bitmap Index Scan; the orders side dropped from 1.57M
        //   to ~3k disk blocks per call (~500x). Trades-side anti-join
        //   chunk seq scans still dominate wall-time (~28s cold cache,
        //   single-digit seconds warm) but no longer dominate IOPS.
        //
        // Index dependency: changing the SQL filter clause below WILL
        // break the partial-index match — the index's WHERE clause must
        // exactly equal the query's `status = 'filled' AND filled_amount
        // > 0 AND signature IS DISTINCT FROM 'close-position'` predicate
        // for the planner to use it. If you need to change the filter,
        // also rebuild the index (see scripts/orphan_detector_partial_index.sql).
        let pool = self.pool.clone();
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(300));

            loop {
                ticker.tick().await;

                // Wrap in a short-lived transaction so `SET LOCAL
                // statement_timeout` scopes to just this query and reverts
                // when the tx drops. Without this guard, a ticker under load
                // could queue multiple copies of this anti-join server-side
                // (observed 2026-04-24: two copies ran concurrently for
                // 1h07m, each contending on `LWLock:BufferMapping` with the
                // `orders` autovacuum, until pg_terminate_backend). 90s is
                // far below the 5 min tick so overruns cancel cleanly
                // before the next fires.
                //
                // History:
                //   60s briefly attempted on 2026-05-02 (PR #112 "relax")
                //   based on a 28.7s EXPLAIN ANALYZE done in a low-traffic
                //   moment. Production cold-cache execution was actually
                //   >60s — back-to-back ticks at 14:51 and 14:56 both
                //   hit the 60s limit, gauge silently stuck. Restored to
                //   90s; PR #109 already proved 90s is safe.
                //
                // Real fix in flight: limit the trades-side anti-join to
                // `t.created_at >= NOW() - INTERVAL '10 minutes'`. Orders
                // matched are 1-10 min old, so any matching trade is also
                // 1-10 min old; this collapses 15+ trades chunks down to
                // 1, dropping wall time to ~1s. After that lands, this
                // 90s can safely come back to 30s or even less.
                let count: Result<i64, sqlx::Error> = async {
                    let mut tx = pool.begin().await?;
                    sqlx::query("SET LOCAL statement_timeout = '90s'")
                        .execute(&mut *tx)
                        .await?;
                    sqlx::query_scalar::<_, i64>(
                        r#"
                        SELECT COUNT(*)::bigint FROM (
                          SELECT 1 FROM orders o
                          WHERE o.status = 'filled'
                            AND o.filled_amount > 0
                            AND o.signature IS DISTINCT FROM 'close-position'
                            AND o.updated_at < NOW() - INTERVAL '1 minute'
                            AND o.updated_at > NOW() - INTERVAL '10 minutes'
                            AND NOT EXISTS (SELECT 1 FROM trades t WHERE t.maker_order_id = o.id)
                            AND NOT EXISTS (SELECT 1 FROM trades t WHERE t.taker_order_id = o.id)
                          LIMIT 10000
                        ) sub
                        "#,
                    )
                    .fetch_one(&mut *tx)
                    .await
                    // tx drops → implicit rollback (read-only, nothing to commit)
                }
                .await;

                match count {
                    Ok(n) => {
                        metrics::ORPHAN_ORDERS_RECENT.set(n as f64);
                        if n > 0 {
                            tracing::warn!(
                                "orphan detector: {} orders filled 1–10m ago with no trade row — persistence pipeline dropping events",
                                n
                            );
                        }
                    }
                    Err(e) => {
                        tracing::error!("orphan detector query failed: {}", e);
                    }
                }
            }
        }.instrument(tracing::info_span!("keeper-orphan-detect")));

        tracing::info!("Keeper service started (trigger orders + referral tier updates + orphan detector)");
    }

    /// Process all active trigger orders for a market (instrumented version)
    async fn process_market_instrumented(&self, market_symbol: &str) -> Result<ScanResult> {
        // Get current price
        let current_price = match self.price_feed_service.get_mark_price(market_symbol).await {
            Some(price) => price,
            None => {
                tracing::debug!("No price available for {}", market_symbol);
                return Ok(ScanResult { matched: 0, pending_buy: 0, pending_sell: 0 });
            }
        };

        // Fetch active trigger orders
        let active_orders = self.get_active_trigger_orders(market_symbol).await?;

        // Count pending orders by side
        let pending_buy = active_orders.iter().filter(|o| matches!(o.side, OrderSide::Buy)).count() as u64;
        let pending_sell = active_orders.iter().filter(|o| matches!(o.side, OrderSide::Sell)).count() as u64;
        let mut matched: u64 = 0;

        for order in &active_orders {
            // Check if order should trigger
            if self.should_trigger(order, current_price).await? {
                matched += 1;

                tracing::info!(
                    "Triggering order {} for {} at price {}",
                    order.id,
                    market_symbol,
                    current_price
                );

                let side_label = match order.side {
                    OrderSide::Buy => "buy",
                    OrderSide::Sell => "sell",
                };
                let type_label = match order.trigger_type {
                    TriggerOrderType::StopLoss => "stop_loss",
                    TriggerOrderType::TakeProfit => "take_profit",
                    TriggerOrderType::StopLimit => "stop_limit",
                    TriggerOrderType::TakeProfitLimit => "take_profit_limit",
                    TriggerOrderType::TrailingStop => "trailing_stop",
                };

                // Execute the order
                match self.execute_trigger_order(order, current_price).await {
                    Ok(()) => {
                        metrics::KEEPER_TRIGGER_FIRED_TOTAL
                            .with_label_values(&[side_label, type_label])
                            .inc();
                    }
                    Err(e) => {
                        tracing::error!("Failed to execute trigger order {}: {:?}", order.id, e);
                        let reason = metrics::classify_trigger_error(&format!("{:?}", e));
                        metrics::KEEPER_TRIGGER_FAILED_TOTAL
                            .with_label_values(&[reason])
                            .inc();
                        self.mark_order_failed(order.id, &e.to_string()).await?;
                    }
                }
            }

            // Update trailing stop peak price if needed
            if order.trigger_type == TriggerOrderType::TrailingStop {
                self.update_trailing_stop_peak(order, current_price).await?;
            }
        }

        // Expire old orders
        let expired_count = self.expire_orders_counted(market_symbol).await?;
        if expired_count > 0 {
            metrics::KEEPER_TRIGGER_EXPIRED_TOTAL.inc_by(expired_count as f64);
        }

        Ok(ScanResult { matched, pending_buy, pending_sell })
    }

    /// Process all active trigger orders for a market (original, kept for compatibility)
    #[allow(dead_code)]
    async fn process_market(&self, market_symbol: &str) -> Result<()> {
        self.process_market_instrumented(market_symbol).await?;
        Ok(())
    }

    /// Get active trigger orders for a market
    async fn get_active_trigger_orders(&self, market_symbol: &str) -> Result<Vec<TriggerOrder>> {
        let orders = sqlx::query_as::<_, TriggerOrder>(
            r#"
            SELECT * FROM trigger_orders
            WHERE market_symbol = $1
              AND status = 'active'
              AND (expires_at IS NULL OR expires_at > NOW())
            ORDER BY created_at ASC
            "#,
        )
        .bind(market_symbol)
        .fetch_all(&self.pool)
        .await?;

        Ok(orders)
    }

    /// Check if a trigger order should fire
    async fn should_trigger(&self, order: &TriggerOrder, current_price: Decimal) -> Result<bool> {
        match order.trigger_type {
            TriggerOrderType::TrailingStop => {
                self.check_trailing_stop_trigger(order, current_price).await
            }
            _ => Ok(self.check_price_trigger(order, current_price)),
        }
    }

    /// Check regular price trigger
    fn check_price_trigger(&self, order: &TriggerOrder, current_price: Decimal) -> bool {
        use crate::services::trigger_orders::TriggerCondition;

        match order.trigger_condition {
            TriggerCondition::PriceAbove => current_price >= order.trigger_price,
            TriggerCondition::PriceBelow => current_price <= order.trigger_price,
        }
    }

    /// Check trailing stop trigger
    async fn check_trailing_stop_trigger(
        &self,
        order: &TriggerOrder,
        current_price: Decimal,
    ) -> Result<bool> {
        let delta = order.trailing_delta.unwrap_or(Decimal::ZERO);
        let delta_type = order.trailing_delta_type.as_deref().unwrap_or("absolute");
        let peak = order.peak_price.unwrap_or(current_price);

        // Calculate trigger threshold
        let trigger_threshold = if delta_type == "percentage" {
            match order.side {
                OrderSide::Sell => peak * (Decimal::ONE - delta / Decimal::from(100)),
                OrderSide::Buy => peak * (Decimal::ONE + delta / Decimal::from(100)),
            }
        } else {
            match order.side {
                OrderSide::Sell => peak - delta,
                OrderSide::Buy => peak + delta,
            }
        };

        // Check if we should trigger
        let should_trigger = match order.side {
            OrderSide::Sell => current_price <= trigger_threshold,
            OrderSide::Buy => current_price >= trigger_threshold,
        };

        Ok(should_trigger)
    }

    /// Update trailing stop peak price
    async fn update_trailing_stop_peak(
        &self,
        order: &TriggerOrder,
        current_price: Decimal,
    ) -> Result<()> {
        let peak = order.peak_price.unwrap_or(current_price);

        let new_peak = match order.side {
            OrderSide::Sell => {
                // For sell (closing long), track highest price
                if current_price > peak {
                    Some(current_price)
                } else {
                    None
                }
            }
            OrderSide::Buy => {
                // For buy (closing short), track lowest price
                if current_price < peak {
                    Some(current_price)
                } else {
                    None
                }
            }
        };

        if let Some(new_peak_price) = new_peak {
            sqlx::query(
                "UPDATE trigger_orders SET peak_price = $1, updated_at = NOW() WHERE id = $2",
            )
            .bind(new_peak_price)
            .bind(order.id)
            .execute(&self.pool)
            .await?;
        }

        Ok(())
    }

    /// Execute a triggered order by placing it in the matching engine
    async fn execute_trigger_order(
        &self,
        trigger_order: &TriggerOrder,
        mark_price: Decimal,
    ) -> Result<()> {
        // Mark as triggered first (only if still active to prevent race conditions)
        let result = sqlx::query(
            r#"
            UPDATE trigger_orders
            SET status = 'triggered', triggered_at = NOW(), triggered_price = $1, updated_at = NOW()
            WHERE id = $2 AND status = 'active'
            "#,
        )
        .bind(mark_price)
        .bind(trigger_order.id)
        .execute(&self.pool)
        .await?;

        // If no rows were affected, the order was already processed by another service
        if result.rows_affected() == 0 {
            tracing::debug!("Order {} already processed or not active, skipping", trigger_order.id);
            return Ok(());
        }

        // Determine order size in tokens (matching engine expects token quantity, not USD)
        let order_size = if trigger_order.close_position {
            // Get position size in tokens to close entire position
            self.get_position_size(&trigger_order.user_address, &trigger_order.market_symbol)
                .await?
                .unwrap_or_else(|| {
                    // Fallback: convert trigger_order.size (USD) to tokens using mark price
                    if mark_price > Decimal::ZERO {
                        trigger_order.size / mark_price
                    } else {
                        trigger_order.size
                    }
                })
        } else {
            // trigger_order.size is in USD, convert to tokens using mark price
            if mark_price > Decimal::ZERO {
                trigger_order.size / mark_price
            } else {
                tracing::warn!("Cannot convert size to tokens: mark_price is zero");
                return Err(anyhow::anyhow!("Cannot execute trigger order: invalid mark price"));
            }
        };

        if order_size <= Decimal::ZERO {
            return Err(anyhow::anyhow!("Invalid order size"));
        }

        // Create the actual order
        let order_id = Uuid::new_v4();
        let order_side = match trigger_order.side {
            OrderSide::Buy => ModelOrderSide::Buy,
            OrderSide::Sell => ModelOrderSide::Sell,
        };

        // Determine order type and price
        let (order_type, limit_price) = match trigger_order.trigger_type {
            TriggerOrderType::StopLimit | TriggerOrderType::TakeProfitLimit => {
                (OrderType::Limit, trigger_order.limit_price)
            }
            _ => (OrderType::Market, None),
        };

        // Get leverage from position or use default
        let leverage = self
            .get_position_leverage(&trigger_order.user_address, &trigger_order.market_symbol)
            .await?
            .unwrap_or(1);

        // Insert the order into database
        sqlx::query(
            r#"
            INSERT INTO orders (
                id, user_address, symbol, order_type, side, amount, price,
                leverage, status, signature, reduce_only, trigger_order_id, created_at, updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'open', $9, $10, $11, NOW(), NOW())
            "#,
        )
        .bind(order_id)
        .bind(&trigger_order.user_address)
        .bind(&trigger_order.market_symbol)
        .bind(order_type)
        .bind(order_side)
        .bind(order_size)
        .bind(limit_price)
        .bind(leverage)
        .bind("keeper-automated")  // System signature for automated orders
        .bind(trigger_order.reduce_only)
        .bind(trigger_order.id)
        .execute(&self.pool)
        .await?;

        // Convert to matching engine types
        let matching_side = match order_side {
            ModelOrderSide::Buy => MatchingSide::Buy,
            ModelOrderSide::Sell => MatchingSide::Sell,
        };
        let matching_order_type = match order_type {
            OrderType::Limit | OrderType::TakeProfitLimit | OrderType::StopLossLimit => MatchingOrderType::Limit,
            OrderType::Market | OrderType::TakeProfitMarket | OrderType::StopLossMarket => MatchingOrderType::Market,
        };

        // Resolve the user's current VIP-tier rates without broadcasting a
        // tier upgrade — keeper-driven fills are background events, not the
        // place where a user expects to see "tier upgraded" notifications.
        let (taker_fee_rate, maker_fee_rate) =
            crate::services::vip_tier::current_fee_rates(
                &self.pool,
                &trigger_order.user_address,
            )
            .await;

        // Submit to matching engine (use the same order_id that was inserted into orders table)
        let match_result = self.matching_engine.submit_order(
            order_id,
            &trigger_order.market_symbol,
            &trigger_order.user_address,
            matching_side,
            matching_order_type,
            order_size,
            limit_price,
            MatchingTimeInForce::GTC,
            leverage as u32,
            taker_fee_rate,
            maker_fee_rate,
        )?;

        // Persist the taker's filled_amount / status / avg price absolutely,
        // mirroring api/handlers/order.rs. The async trade-persistence worker
        // in matching/orchestrator.rs only updates the maker side (see the
        // comment there at "Taker order filled_amount is already set
        // absolutely by the create_order handler"). Keeper-initiated orders
        // skip that handler, so without this UPDATE they stay stuck at
        // status='open', filled_amount=0 even after the match completes —
        // surfacing to users as a TP/SL trigger that "didn't fire" and
        // producing orphan-order alerts (533+ rows accumulated on 2026-04-21).
        sqlx::query(
            "UPDATE orders SET status = $1::order_status, filled_amount = $2, price = COALESCE($3, price) WHERE id = $4"
        )
        .bind(match_result.status.to_string())
        .bind(match_result.filled_amount)
        .bind(match_result.average_price)
        .bind(order_id)
        .execute(&self.pool)
        .await?;

        // Record execution
        let execution_success = match_result.filled_amount > Decimal::ZERO;
        sqlx::query(
            r#"
            INSERT INTO trigger_order_executions (
                trigger_order_id, user_address, market_symbol, trigger_type,
                trigger_price, mark_price, execution_price, size, side,
                success, resulting_order_id
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            "#,
        )
        .bind(trigger_order.id)
        .bind(&trigger_order.user_address)
        .bind(&trigger_order.market_symbol)
        .bind(&trigger_order.trigger_type)
        .bind(trigger_order.trigger_price)
        .bind(mark_price)
        .bind(match_result.average_price)
        .bind(order_size)
        .bind(&trigger_order.side)
        .bind(execution_success)
        .bind(order_id)
        .execute(&self.pool)
        .await?;

        // Update trigger order status
        let final_status = if execution_success {
            TriggerOrderStatus::Executed
        } else {
            TriggerOrderStatus::Failed
        };

        sqlx::query(
            r#"
            UPDATE trigger_orders
            SET status = $1, executed_order_id = $2, executed_price = $3,
                executed_at = NOW(), updated_at = NOW()
            WHERE id = $4
            "#,
        )
        .bind(final_status)
        .bind(order_id)
        .bind(match_result.average_price)
        .bind(trigger_order.id)
        .execute(&self.pool)
        .await?;

        // Update position if this was a position reduction/close
        if trigger_order.reduce_only || trigger_order.close_position {
            self.update_position_after_trigger(trigger_order, &match_result).await?;
        }

        // Update triggered count in user stats (only on successful execution)
        if execution_success {
            let count_col = match trigger_order.trigger_type {
                TriggerOrderType::StopLoss | TriggerOrderType::StopLimit => "triggered_stop_loss_orders",
                TriggerOrderType::TakeProfit | TriggerOrderType::TakeProfitLimit => "triggered_take_profit_orders",
                TriggerOrderType::TrailingStop => "triggered_trailing_stop_orders",
            };

            let count_query = format!(
                r#"
                INSERT INTO user_trigger_order_stats (user_address, market_symbol, {count_col})
                VALUES ($1, $2, 1)
                ON CONFLICT (user_address, market_symbol) DO UPDATE
                SET {count_col} = user_trigger_order_stats.{count_col} + 1, updated_at = NOW()
                "#,
                count_col = count_col
            );

            sqlx::query(&count_query)
                .bind(&trigger_order.user_address)
                .bind(&trigger_order.market_symbol)
                .execute(&self.pool)
                .await?;
        }

        tracing::info!(
            "Trigger order {} executed: filled {}/{} at avg price {:?}",
            trigger_order.id,
            match_result.filled_amount,
            order_size,
            match_result.average_price
        );

        Ok(())
    }

    /// Get user's position size in tokens for a market
    /// Returns size_in_tokens (coin quantity) for use with matching engine
    async fn get_position_size(
        &self,
        user_address: &str,
        market_symbol: &str,
    ) -> Result<Option<Decimal>> {
        let size: Option<Decimal> = sqlx::query_scalar(
            r#"
            SELECT ABS(size_in_tokens) FROM positions
            WHERE user_address = $1 AND symbol = $2 AND status = 'open'
            "#,
        )
        .bind(user_address)
        .bind(market_symbol)
        .fetch_optional(&self.pool)
        .await?;

        Ok(size)
    }

    /// Get user's position leverage for a market
    async fn get_position_leverage(
        &self,
        user_address: &str,
        market_symbol: &str,
    ) -> Result<Option<i32>> {
        let leverage: Option<i32> = sqlx::query_scalar(
            r#"
            SELECT leverage FROM positions
            WHERE user_address = $1 AND symbol = $2 AND status = 'open'
            "#,
        )
        .bind(user_address)
        .bind(market_symbol)
        .fetch_optional(&self.pool)
        .await?;

        Ok(leverage)
    }

    /// Record trigger-order analytics after a successful fill.
    ///
    /// Position state itself (size, realized_pnl, status, orphan-trigger
    /// cancellation, position_tp_sl cleanup) is **not** touched here — it is
    /// owned by `matching::orchestrator::update_positions_after_trade` →
    /// `position_service::increase_position` → opposing-side
    /// `decrease_position`, which also writes the canonical
    /// `realized_pnl_events` row. Earlier this function held a duplicate
    /// `UPDATE positions … realized_pnl += pnl` block; under the async
    /// persistence pipeline that produced two failure modes:
    ///   * keeper UPDATE wins the race → the matching path sees no opposing
    ///     open position and creates a new phantom in the *opposite*
    ///     direction (e.g. a TP on a long that materialises a never-asked-for
    ///     short, see incident on user 0x3f7b…D800 around 2026-04-22).
    ///   * matching path wins → both paths credit the same PnL and
    ///     `positions.realized_pnl` is double-counted.
    /// In both cases `realized_pnl_events` was *also* skipped here, so Trade
    /// History silently dropped the close.
    ///
    /// We still maintain trigger-specific accounting (`trigger_order_executions`
    /// and `user_trigger_order_stats`) here because those are denormalised
    /// for trigger analytics and the matching path doesn't know about them.
    async fn update_position_after_trigger(
        &self,
        trigger_order: &TriggerOrder,
        match_result: &crate::services::matching::MatchResult,
    ) -> Result<()> {
        if match_result.filled_amount <= Decimal::ZERO {
            return Ok(());
        }

        // Resolve the position used to price this trigger. Used only for
        // entry_price lookup so we can attribute approximate PnL to the
        // trigger-stats tables.
        let position_id: Option<Uuid> = if let Some(pid) = trigger_order.position_id {
            Some(pid)
        } else {
            sqlx::query_scalar(
                r#"
                SELECT id FROM positions
                WHERE user_address = $1 AND symbol = $2 AND status = 'open'
                "#,
            )
            .bind(&trigger_order.user_address)
            .bind(&trigger_order.market_symbol)
            .fetch_optional(&self.pool)
            .await?
        };

        let Some(pos_id) = position_id else {
            return Ok(());
        };

        let entry_price: Option<Decimal> = sqlx::query_scalar(
            "SELECT entry_price FROM positions WHERE id = $1",
        )
        .bind(pos_id)
        .fetch_optional(&self.pool)
        .await?;

        let (Some(entry), Some(exit)) = (entry_price, match_result.average_price) else {
            return Ok(());
        };

        // filled_amount is in tokens. Sign convention: trigger.side reflects
        // the closing side — Sell closes a long, Buy closes a short.
        let pnl = match trigger_order.side {
            OrderSide::Sell => (exit - entry) * match_result.filled_amount,
            OrderSide::Buy => (entry - exit) * match_result.filled_amount,
        };

        // Trigger-execution receipt: stamp the approximate PnL the user will
        // see in the trigger-history pane.
        sqlx::query(
            r#"
            UPDATE trigger_order_executions
            SET realized_pnl = $1
            WHERE trigger_order_id = $2
            "#,
        )
        .bind(pnl)
        .bind(trigger_order.id)
        .execute(&self.pool)
        .await?;

        // Per-(user, market) running totals broken down by trigger type.
        let pnl_col = match trigger_order.trigger_type {
            TriggerOrderType::StopLoss => "stop_loss_pnl",
            TriggerOrderType::TakeProfit => "take_profit_pnl",
            TriggerOrderType::TrailingStop => "trailing_stop_pnl",
            _ => "stop_loss_pnl",
        };

        let stats_query = format!(
            r#"
            INSERT INTO user_trigger_order_stats (user_address, market_symbol, {pnl_col})
            VALUES ($1, $2, $3)
            ON CONFLICT (user_address, market_symbol) DO UPDATE
            SET {pnl_col} = user_trigger_order_stats.{pnl_col} + $3, updated_at = NOW()
            "#,
            pnl_col = pnl_col
        );

        sqlx::query(&stats_query)
            .bind(&trigger_order.user_address)
            .bind(&trigger_order.market_symbol)
            .bind(pnl)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    /// Mark an order as failed
    async fn mark_order_failed(&self, order_id: Uuid, error: &str) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE trigger_orders
            SET status = 'failed', error_message = $1, updated_at = NOW()
            WHERE id = $2
            "#,
        )
        .bind(error)
        .bind(order_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Expire old orders
    #[allow(dead_code)]
    async fn expire_orders(&self, market_symbol: &str) -> Result<()> {
        self.expire_orders_counted(market_symbol).await?;
        Ok(())
    }

    /// Expire old orders and return count of expired
    async fn expire_orders_counted(&self, market_symbol: &str) -> Result<u64> {
        let result = sqlx::query(
            r#"
            UPDATE trigger_orders
            SET status = 'expired', updated_at = NOW()
            WHERE market_symbol = $1 AND status = 'active' AND expires_at <= NOW()
            "#,
        )
        .bind(market_symbol)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected())
    }

    /// Update referral tiers using dual AND criteria (referral count + referred volume).
    ///
    /// Tier table (per PRD):
    ///   0 Starter 10%  ≥1  + ≥$1K
    ///   1 Bronze  12%  ≥5  + ≥$10K
    ///   2 Silver  17%  ≥20 + ≥$100K
    ///   3 Gold    22%  ≥50 + ≥$500K
    ///   4 Diamond 25%  ≥100+ ≥$2M
    ///
    /// Users that do not meet the Starter threshold remain at tier=0 with
    /// commission_rate=0.10; the API layer surfaces the "below minimum" message.
    async fn update_referral_tiers(&self) -> Result<()> {
        let updated = sqlx::query(
            r#"
            WITH volumes AS (
                SELECT referrer_address,
                       COALESCE(SUM(volume), 0) AS total_vol
                FROM referral_earnings
                GROUP BY referrer_address
            ),
            new_tiers AS (
                SELECT
                    rc.owner_address,
                    CASE
                        WHEN rc.total_referrals >= 100 AND COALESCE(v.total_vol, 0) >= 2000000 THEN 4
                        WHEN rc.total_referrals >= 50  AND COALESCE(v.total_vol, 0) >= 500000  THEN 3
                        WHEN rc.total_referrals >= 20  AND COALESCE(v.total_vol, 0) >= 100000  THEN 2
                        WHEN rc.total_referrals >= 5   AND COALESCE(v.total_vol, 0) >= 10000   THEN 1
                        ELSE 0
                    END AS new_tier,
                    CASE
                        WHEN rc.total_referrals >= 100 AND COALESCE(v.total_vol, 0) >= 2000000 THEN 0.25
                        WHEN rc.total_referrals >= 50  AND COALESCE(v.total_vol, 0) >= 500000  THEN 0.22
                        WHEN rc.total_referrals >= 20  AND COALESCE(v.total_vol, 0) >= 100000  THEN 0.17
                        WHEN rc.total_referrals >= 5   AND COALESCE(v.total_vol, 0) >= 10000   THEN 0.12
                        ELSE 0.10
                    END AS new_rate
                FROM referral_codes rc
                LEFT JOIN volumes v ON v.referrer_address = rc.owner_address
            )
            UPDATE referral_codes rc
            SET tier            = nt.new_tier,
                commission_rate = nt.new_rate
            FROM new_tiers nt
            WHERE nt.owner_address = rc.owner_address
              AND (rc.tier != nt.new_tier OR rc.commission_rate != nt.new_rate)
            "#,
        )
        .execute(&self.pool)
        .await?;

        if updated.rows_affected() > 0 {
            tracing::info!("Updated {} referral code tiers", updated.rows_affected());
        }

        Ok(())
    }
}
