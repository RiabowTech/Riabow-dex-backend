//! Order Flow Orchestrator
//!
//! Orchestrates the complete order processing flow:
//! 1. Receive order from API
//! 2. Execute matching via MatchingEngine
//! 3. Process match results
//! 4. Persist to database asynchronously with transactions
//! 5. Broadcast updates via WebSocket
//!
//! # Data Integrity Features
//!
//! - **Transactions**: Trade persistence uses database transactions
//! - **Retry**: Failed operations are retried with exponential backoff
//! - **Dead Letter Queue**: Permanently failed trades are stored for recovery

use super::engine::MatchingEngine;
use super::retry::{with_retry, RetryConfig};
use super::types::*;
use futures::FutureExt;
use rust_decimal::Decimal;
use sqlx::PgPool;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};
use tracing::Instrument;
use uuid::Uuid;

use crate::models::PositionSide;
use crate::services::metrics;
use crate::services::points::{handle_trade_event_async, PointsService};
use crate::services::position::PositionService;
use crate::services::referral::{ReferralService, PendingTradeRecord};

/// Dynamically calculate referral commission rate based on how many referrals a user has.
/// Uses tier thresholds: Bronze(0)=5%, Silver(1+)=10%, Gold(10+)=15%, Platinum(50+)=20%, Diamond(100+)=25%.
async fn get_referral_commission_rate<'e, E>(executor: E, referrer_address: &str) -> Decimal
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM referral_relations WHERE referrer_address = $1"
    )
    .bind(referrer_address)
    .fetch_one(executor)
    .await
    .unwrap_or(0);

    if count >= 100 {
        Decimal::new(25, 2)  // 2500 bps = 25%
    } else if count >= 50 {
        Decimal::new(20, 2)  // 2000 bps = 20%
    } else if count >= 10 {
        Decimal::new(15, 2)  // 1500 bps = 15%
    } else if count >= 1 {
        Decimal::new(10, 2)  // 1000 bps = 10%
    } else {
        Decimal::new(5, 2)   // 500 bps = 5%
    }
}

/// Order flow orchestrator
///
/// Connects matching engine with database persistence and WebSocket broadcasting.
/// All database operations are async and non-blocking.
pub struct OrderFlowOrchestrator {
    /// The matching engine
    engine: Arc<MatchingEngine>,

    /// Database connection pool
    pool: PgPool,

    /// Trade event receiver for persistence
    trade_receiver: Option<broadcast::Receiver<TradeEvent>>,
}

#[allow(dead_code)]
impl OrderFlowOrchestrator {
    /// Create a new orchestrator
    pub fn new(engine: Arc<MatchingEngine>, pool: PgPool) -> Self {
        let trade_receiver = Some(engine.subscribe_trades());

        info!("OrderFlowOrchestrator initialized");

        Self {
            engine,
            pool,
            trade_receiver,
        }
    }

    /// Get reference to matching engine
    pub fn engine(&self) -> &Arc<MatchingEngine> {
        &self.engine
    }

    /// Start the background persistence worker
    ///
    /// The worker:
    /// 1. Receives trades from broadcast channel AND fallback queue
    /// 2. Attempts persistence with retry on transient failures
    /// 3. Writes to dead letter queue if all retries fail
    /// 4. Calculates trading points after successful persistence
    pub fn start_persistence_worker(
        mut self,
        points_service: Option<Arc<PointsService>>,
        referral_service: Option<Arc<ReferralService>>,
    ) -> Arc<MatchingEngine> {
        let pool = self.pool.clone();
        let engine = Arc::clone(&self.engine);
        let broadcast_receiver = self.trade_receiver.take();
        let fallback_receiver = engine.take_fallback_receiver();
        let retry_config = RetryConfig::default();

        // Helper function to persist a single trade and calculate points
        async fn persist_trade_helper(
            pool: &PgPool,
            trade: TradeEvent,
            retry_config: &RetryConfig,
            points_service: Option<Arc<PointsService>>,
            referral_service: Option<Arc<ReferralService>>,
        ) {
            let start = std::time::Instant::now();

            let pool_clone = pool.clone();
            let trade_clone = trade.clone();
            let ref_svc = referral_service.clone();

            // Attempt persistence with retry
            let ps_clone = points_service.clone();
            let result = with_retry(retry_config, "persist_trade", || {
                let p = pool_clone.clone();
                let t = trade_clone.clone();
                let rs = ref_svc.clone();
                let ps = ps_clone.clone();
                async move { OrderFlowOrchestrator::persist_trade_with_tx(&p, &t, rs.as_ref(), ps).await }
            })
            .await;

            let elapsed = start.elapsed().as_secs_f64();

            match &result {
                Ok(()) => {
                    info!(
                        "✅ Trade {} persisted successfully to database",
                        trade.trade_id
                    );
                    metrics::PERSISTENCE_LATENCY
                        .with_label_values(&["success"])
                        .observe(elapsed);

                    // Calculate trading points after successful persistence
                    if let Some(ref ps) = points_service {
                        handle_trade_event_async(Arc::clone(ps), trade.clone());
                    }

                    // Create deferred TP/SL for limit orders that stored
                    // tp_price/sl_price but weren't immediately filled.
                    OrderFlowOrchestrator::create_deferred_tp_sl(pool, &trade).await;
                }
                Err(e) => {
                    error!(
                        "💀 Trade {} failed all retries, writing to DLQ: {}",
                        trade.trade_id, e
                    );
                    metrics::PERSISTENCE_LATENCY
                        .with_label_values(&["failure"])
                        .observe(elapsed);
                    metrics::PERSISTENCE_DLQ_TOTAL.inc();

                    if let Err(dlq_err) = OrderFlowOrchestrator::write_to_dead_letter_queue(
                        pool,
                        &trade,
                        &e.to_string(),
                        retry_config.max_attempts,
                    )
                    .await
                    {
                        // Last resort: log everything for manual recovery
                        error!(
                            "❌ CRITICAL: Failed to write trade {} to DLQ: {}",
                            trade.trade_id, dlq_err
                        );
                        error!(
                            "Trade data for manual recovery: {:?}",
                            serde_json::to_string(&trade)
                        );
                    }
                }
            }
        }

        // Start the main persistence worker consuming from the mpsc fallback queue
        // All trades are now sent to fallback queue (mpsc) which guarantees no message loss,
        // unlike broadcast channel which drops messages when receivers lag behind.
        //
        // Uses a semaphore to allow up to 10 concurrent trade persists, preventing
        // a single slow position update from blocking the entire pipeline.
        {
            let pool_clone = pool.clone();
            let retry_config_clone = retry_config.clone();
            let points_service_clone = points_service.clone();
            let referral_service_clone = referral_service.clone();

            // Drop broadcast receiver - we don't use it for persistence anymore
            drop(broadcast_receiver);

            tokio::spawn(async move {
                // Semaphore cap is tunable via PERSISTENCE_MAX_CONCURRENT_TASKS so
                // ops can widen the pipeline when the DB is momentarily bottlenecked
                // by something external (e.g. an operator holding row locks on
                // orders/balances during a maintenance script). Default 10 matches
                // the historical hard-coded value; accepted range 1..=1024.
                let max_concurrent: usize = std::env::var("PERSISTENCE_MAX_CONCURRENT_TASKS")
                    .ok()
                    .and_then(|v| v.parse::<usize>().ok())
                    .filter(|n| (1..=1024).contains(n))
                    .unwrap_or(10);
                info!(
                    "Trade persistence worker started (mpsc queue, max_concurrent={}, with retry, DLQ, and points integration)",
                    max_concurrent
                );

                // Semaphore limits concurrent trade persistence to avoid DB connection exhaustion
                let semaphore = Arc::new(tokio::sync::Semaphore::new(max_concurrent));

                if let Some(mut fallback_rx) = fallback_receiver {
                    while let Some(trade) = fallback_rx.recv().await {
                        let pool = pool_clone.clone();
                        let retry_config = retry_config_clone.clone();
                        let points_service = points_service_clone.clone();
                        let referral_service = referral_service_clone.clone();
                        let sem = semaphore.clone();

                        // Count trades that have been received off the mpsc queue
                        // but have not yet acquired a persistence permit. The earlier
                        // gauge definition was never wired up here, so operators saw
                        // `persistence_queue_depth=0` even while ~12k tokio tasks were
                        // backed up on the semaphore — which directly misled the
                        // 2026-04-21 orphan alert triage.
                        metrics::PERSISTENCE_QUEUE_DEPTH.inc();

                        tokio::spawn(async move {
                            let _permit = sem.acquire().await.expect("semaphore closed");
                            metrics::PERSISTENCE_QUEUE_DEPTH.dec();
                            metrics::PERSISTENCE_CONCURRENT_TASKS.inc();
                            metrics::TRADE_PERSIST_ENTRY_TOTAL.inc();

                            let trade_id = trade.trade_id;
                            info!(
                                "🔄 Persisting trade {} (symbol={}, price={}, amount={}, maker={}, taker={})",
                                trade.trade_id, trade.symbol, trade.price, trade.amount,
                                trade.maker_address, trade.taker_address
                            );

                            // Previously this was wrapped in `tokio::time::timeout(120s, ...)`.
                            // That was removed: cancelling persist_trade_helper mid-flight
                            // aborts any in-progress sqlx transaction at an await point. The
                            // Transaction's Drop-based rollback is not always polled on tokio
                            // cancellation, so the Postgres session stays in `idle in transaction`
                            // — advisory xact locks never release and sqlx pool connections leak.
                            // That single bug was responsible for ~1.4M rows piling up in the
                            // dead_letter_trades queue and trades table stalling completely.
                            //
                            // Bounding per-trade latency is still desirable: this is enforced
                            // internally by with_retry (bounded attempts) + DB_ACQUIRE_TIMEOUT_SECS
                            // on each sqlx query. A trade that legitimately needs more than a few
                            // seconds must be allowed to finish instead of being cancelled.
                            //
                            // AssertUnwindSafe + catch_unwind converts a panic inside
                            // persist_trade_helper into an observable counter increment +
                            // log line, instead of the tokio-runtime default where a
                            // panicking task vanishes silently and the permit/metric
                            // counters leak. The 2026-04-24 `orphan_orders_recent` spike
                            // (~38% of filled-order fills in 2m window with DLQ=0 /
                            // overflow=0 / timeout=0) was the motivating incident: a
                            // panic somewhere in the persist path would leave no trace
                            // in any of the existing metrics. See trade_persist_panic_total.
                            let persist_fut = persist_trade_helper(
                                &pool, trade.clone(), &retry_config, points_service, referral_service,
                            );
                            match std::panic::AssertUnwindSafe(persist_fut)
                                .catch_unwind()
                                .await
                            {
                                Ok(()) => {}
                                Err(payload) => {
                                    metrics::TRADE_PERSIST_PANIC_TOTAL.inc();
                                    let msg = if let Some(s) = payload.downcast_ref::<&str>() {
                                        (*s).to_string()
                                    } else if let Some(s) = payload.downcast_ref::<String>() {
                                        s.clone()
                                    } else {
                                        "<non-string panic payload>".to_string()
                                    };
                                    error!(
                                        "❌ panic in persist_trade_helper for trade {}: {}",
                                        trade_id, msg
                                    );
                                }
                            }

                            metrics::PERSISTENCE_CONCURRENT_TASKS.dec();
                        }.instrument(tracing::info_span!("trade-persistence")));
                    }
                } else {
                    warn!("⚠️ No persistence queue receiver available!");
                }

                info!("Trade persistence worker stopped");
            });
        }

        engine
    }

    /// Process a new order
    ///
    /// This is the main entry point for order processing:
    /// 1. Validates the order
    /// 2. Submits to matching engine
    /// 3. Spawns async task for database persistence
    /// 4. Returns immediately with match result
    pub async fn process_order(
        &self,
        symbol: &str,
        user_address: &str,
        side: Side,
        order_type: OrderType,
        amount: Decimal,
        price: Option<Decimal>,
        leverage: u32,
    ) -> Result<MatchResult, MatchingError> {
        debug!(
            "Processing order: symbol={}, user={}, side={:?}, type={:?}, amount={}, price={:?}",
            symbol, user_address, side, order_type, amount, price
        );

        // Generate order ID
        let order_id = Uuid::new_v4();

        // Submit to matching engine (synchronous, in-memory). Resolve the
        // user's current VIP-tier rates before handing off so the engine
        // applies discounts correctly. `current_fee_rates` is non-mutating
        // and won't trigger upgrade events.
        let (taker_fee_rate, maker_fee_rate) =
            crate::services::vip_tier::current_fee_rates(&self.pool, user_address).await;
        let result = self.engine.submit_order(
            order_id,
            symbol,
            user_address,
            side,
            order_type,
            amount,
            price,
            TimeInForce::GTC,
            leverage,
            taker_fee_rate,
            maker_fee_rate,
        )?;

        // Spawn async task for database persistence
        let pool = self.pool.clone();
        let symbol = symbol.to_string();
        let user_address = user_address.to_string();
        let result_clone = result.clone();

        tokio::spawn(async move {
            if let Err(e) = Self::persist_order(
                &pool,
                &symbol,
                &user_address,
                &result_clone,
                side,
                order_type,
                amount,
                price,
                leverage,
            ).await {
                error!("Failed to persist order {}: {}", order_id, e);
            }
        }.instrument(tracing::info_span!("order-persist")));

        info!(
            "Order processed: id={}, status={:?}, filled={}",
            result.order_id, result.status, result.filled_amount
        );

        Ok(result)
    }

    /// Cancel an order
    pub async fn cancel_order(
        &self,
        symbol: &str,
        order_id: Uuid,
        user_address: &str,
    ) -> Result<bool, MatchingError> {
        debug!("Cancelling order: id={}, symbol={}", order_id, symbol);

        // Cancel in matching engine
        let cancelled = self.engine.cancel_order(symbol, order_id, user_address)?;

        if cancelled {
            // Update database asynchronously
            let pool = self.pool.clone();
            let order_id = order_id;

            tokio::spawn(async move {
                if let Err(e) = Self::update_order_status(&pool, order_id, "cancelled").await {
                    error!("Failed to update order status: {}", e);
                }
            }.instrument(tracing::info_span!("order-cancel")));

            info!("Order cancelled: id={}", order_id);
        }

        Ok(cancelled)
    }

    /// Get orderbook
    pub fn get_orderbook(&self, symbol: &str, depth: usize) -> Result<OrderbookSnapshot, MatchingError> {
        self.engine.get_orderbook(symbol, depth)
    }

    // ========================================================================
    // Database Persistence
    // ========================================================================

    /// Persist a trade to database with transaction support
    ///
    /// This is the primary persistence method that wraps all operations
    /// in a database transaction to ensure atomicity.
    ///
    /// # Transaction Scope
    /// - Trade record insertion
    /// - Referral commission recording
    ///
    /// Note: Position updates are handled separately by PositionService
    /// as they may involve complex calculations and external price lookups.
    pub async fn persist_trade_with_tx(pool: &PgPool, trade: &TradeEvent, referral_service: Option<&Arc<ReferralService>>, points_service: Option<Arc<PointsService>>) -> Result<(), sqlx::Error> {
        // Start transaction
        let mut tx = pool.begin().await?;

        // Calculate fees (0.02% maker, 0.05% taker)
        let trade_value = trade.amount * trade.price;
        let maker_fee = trade_value * Decimal::from_str_exact("0.0002").unwrap();
        let taker_fee = trade_value * Decimal::from_str_exact("0.0005").unwrap();

        // 1. Insert trade record (with leverage info for position recovery)
        // Use WHERE NOT EXISTS instead of ON CONFLICT for TimescaleDB compatibility.
        // RETURNING 1 lets us detect whether this call actually created a new row —
        // critical because downstream UPDATE orders filled_amount += amount is NOT
        // idempotent, and `with_retry` wrapper can retry after a successful commit
        // whose ack was lost (network/DB outage). QA batch 6 saw 39,633 orders with
        // filled_amount = 2× amount caused by exactly this double-increment path.
        let inserted_row: Option<(i32,)> = sqlx::query_as(
            r#"
            INSERT INTO trades (id, symbol, maker_order_id, taker_order_id, maker_address, taker_address, side, price, amount, maker_fee, taker_fee, created_at, maker_leverage, taker_leverage, position_synced, is_self_trade)
            SELECT $1, $2, $3, $4, $5, $6, $7::order_side, $8, $9, $10, $11, to_timestamp($12::double precision / 1000), $13, $14, FALSE, $15
            WHERE NOT EXISTS (SELECT 1 FROM trades WHERE id = $1)
            RETURNING 1
            "#
        )
        .bind(trade.trade_id)
        .bind(&trade.symbol)
        .bind(trade.maker_order_id)
        .bind(trade.taker_order_id)
        .bind(&trade.maker_address)
        .bind(&trade.taker_address)
        .bind(&trade.side)
        .bind(trade.price)
        .bind(trade.amount)
        .bind(maker_fee)
        .bind(taker_fee)
        .bind(trade.timestamp as f64)
        .bind(trade.maker_leverage as i32)
        .bind(trade.taker_leverage as i32)
        .bind(trade.is_self_trade)
        .fetch_optional(&mut *tx)
        .await?;

        // If the trade already existed (retry after ack-loss), skip all the
        // side effects below — they've already been applied by the prior commit.
        //
        // BUT: verify the trade row actually exists before skipping. Round-2 QA
        // found 191k orders with status='filled' and no matching trade row
        // (N7: filled-without-trade). Counters showed
        // `persistence_dlq_total = 0`, `trade_emit_total{outcome=ok} = 73k`,
        // `persistence_latency_seconds_count{success} = 82k` — all green —
        // yet 191k phantom-filled orders accumulated. The most plausible
        // explanation is that this idempotency branch fires when the trade
        // row was rolled back by a prior tx (e.g. tx commit failed after the
        // INSERT was visible to a concurrent NOT EXISTS read), and the retry
        // skips side effects while the maker-side UPDATE never happens.
        //
        // To make the skip safe, re-check that the trade row really exists
        // inside this same tx. If it doesn't, fall through and run the side
        // effects so we don't silently leave a filled-without-trade orphan.
        if inserted_row.is_none() {
            let exists: Option<(uuid::Uuid,)> = sqlx::query_as(
                "SELECT id FROM trades WHERE id = $1"
            )
            .bind(trade.trade_id)
            .fetch_optional(&mut *tx)
            .await?;
            if exists.is_some() {
                tracing::debug!(
                    "persist_trade_with_tx: trade {} already persisted, skipping side effects (retry idempotency)",
                    trade.trade_id
                );
                tx.commit().await?;
                return Ok(());
            }
            // Skip-fallthrough: trade was NOT actually persisted.
            // The earlier INSERT must have racy-NOT-EXISTS-failed. Force-
            // INSERT the trade row now and proceed with side effects.
            tracing::warn!(
                "persist_trade_with_tx: trade {} NOT EXISTS check returned existing but \
                 SELECT could not find the row — racy NOT EXISTS suspected. \
                 Force-inserting + running side effects to avoid \
                 filled-without-trade orphan (P0 N7: 2026-04-25 QA round 2).",
                trade.trade_id
            );
            // Use WHERE NOT EXISTS again (trades is a TimescaleDB hypertable
            // — see initial INSERT above for the same compatibility caveat).
            // If a concurrent attempt wins between our previous SELECT and
            // this INSERT, it returns no row (NOT EXISTS false) and we
            // simply rely on the other tx to run the side effects.
            let force_inserted: Option<(i32,)> = sqlx::query_as(
                r#"
                INSERT INTO trades (id, symbol, maker_order_id, taker_order_id, maker_address, taker_address, side, price, amount, maker_fee, taker_fee, created_at, maker_leverage, taker_leverage, position_synced, is_self_trade)
                SELECT $1, $2, $3, $4, $5, $6, $7::order_side, $8, $9, $10, $11, to_timestamp($12::double precision / 1000), $13, $14, FALSE, $15
                WHERE NOT EXISTS (SELECT 1 FROM trades WHERE id = $1)
                RETURNING 1
                "#
            )
            .bind(trade.trade_id)
            .bind(&trade.symbol)
            .bind(trade.maker_order_id)
            .bind(trade.taker_order_id)
            .bind(&trade.maker_address)
            .bind(&trade.taker_address)
            .bind(&trade.side)
            .bind(trade.price)
            .bind(trade.amount)
            .bind(maker_fee)
            .bind(taker_fee)
            .bind(trade.timestamp as f64)
            .bind(trade.maker_leverage as i32)
            .bind(trade.taker_leverage as i32)
            .bind(trade.is_self_trade)
            .fetch_optional(&mut *tx)
            .await?;
            if force_inserted.is_none() {
                // Lost the race to another tx, which is now responsible for
                // running side effects. Commit and return.
                tracing::debug!(
                    "persist_trade_with_tx: trade {} won by concurrent tx during force-insert, skipping",
                    trade.trade_id
                );
                tx.commit().await?;
                return Ok(());
            }
            // Fall through to run side effects (referral, maker UPDATE, balance release).
        }

        // 2. Handle referral commission for maker (within transaction)
        // Only count commission for trades that occur AFTER the referral relationship was established
        let maker_referrer: Option<(String,)> = sqlx::query_as(
            r#"
            SELECT rr.referrer_address
            FROM referral_relations rr
            WHERE rr.referee_address = $1
              AND to_timestamp($2::double precision / 1000) >= rr.created_at
            "#
        )
        .bind(&trade.maker_address.to_lowercase())
        .bind(trade.timestamp as f64)
        .fetch_optional(&mut *tx)
        .await?;

        if let Some((referrer_address,)) = maker_referrer {
            let commission_rate = get_referral_commission_rate(&mut *tx, &referrer_address).await;
            let commission = maker_fee * commission_rate;
            sqlx::query(
                r#"
                INSERT INTO referral_earnings
                (id, referrer_address, referee_address, trade_id, event_type, volume, commission, token, status, created_at)
                VALUES ($1, $2, $3, $4, 'trade', $5, $6, 'USDT', 'pending', to_timestamp($7::double precision / 1000))
                "#
            )
            .bind(uuid::Uuid::new_v4())
            .bind(&referrer_address)
            .bind(&trade.maker_address.to_lowercase())
            .bind(trade.trade_id)
            .bind(trade_value)
            .bind(commission)
            .bind(trade.timestamp as f64)
            .execute(&mut *tx)
            .await?;

            // Queue for on-chain sync
            if let Some(ref referral_svc) = referral_service {
                referral_svc.queue_trade(PendingTradeRecord {
                    trader: referrer_address.clone(),
                    volume_usd: trade_value,
                    fee_usd: commission,
                    trade_id: trade.trade_id.to_string(),
                }).await;
            }
        }

        // 3. Handle referral commission for taker (within transaction)
        // Only count commission for trades that occur AFTER the referral relationship was established
        let taker_referrer: Option<(String,)> = sqlx::query_as(
            r#"
            SELECT rr.referrer_address
            FROM referral_relations rr
            WHERE rr.referee_address = $1
              AND to_timestamp($2::double precision / 1000) >= rr.created_at
            "#
        )
        .bind(&trade.taker_address.to_lowercase())
        .bind(trade.timestamp as f64)
        .fetch_optional(&mut *tx)
        .await?;

        if let Some((referrer_address,)) = taker_referrer {
            let commission_rate = get_referral_commission_rate(&mut *tx, &referrer_address).await;
            let commission = taker_fee * commission_rate;
            sqlx::query(
                r#"
                INSERT INTO referral_earnings
                (id, referrer_address, referee_address, trade_id, event_type, volume, commission, token, status, created_at)
                VALUES ($1, $2, $3, $4, 'trade', $5, $6, 'USDT', 'pending', to_timestamp($7::double precision / 1000))
                "#
            )
            .bind(uuid::Uuid::new_v4())
            .bind(&referrer_address)
            .bind(&trade.taker_address.to_lowercase())
            .bind(trade.trade_id)
            .bind(trade_value)
            .bind(commission)
            .bind(trade.timestamp as f64)
            .execute(&mut *tx)
            .await?;

            // Queue for on-chain sync
            if let Some(ref referral_svc) = referral_service {
                referral_svc.queue_trade(PendingTradeRecord {
                    trader: referrer_address.clone(),
                    volume_usd: trade_value,
                    fee_usd: commission,
                    trade_id: trade.trade_id.to_string(),
                }).await;
            }
        }

        // 4. Update maker order filled_amount and status (within transaction).
        //
        // Sub-lot drift handling: rust_decimal carries 28 significant figures
        // and the orderbook can produce trade amounts whose per-fill sums
        // converge to (amount - 1e-18). A strict `>= amount` comparison
        // therefore left the order in `partially_filled` even though no
        // meaningful quantity remained. R4 (P0 #4) caught a 0.1 BTC LIMIT
        // stuck at 0.099999999999999999. The tolerance is 1e-12, several
        // orders of magnitude below any tradeable lot size.
        //
        // Also clamp `filled_amount` to `amount` so the persisted value
        // never overshoots when float-style drift goes the other way.
        sqlx::query(
            r#"
            UPDATE orders
            SET filled_amount = LEAST(filled_amount + $1, amount),
                status = CASE
                    WHEN filled_amount + $1 >= amount - 0.000000000001
                        THEN 'filled'::order_status
                    ELSE 'partially_filled'::order_status
                END,
                updated_at = NOW()
            WHERE id = $2
            "#
        )
        .bind(trade.amount)
        .bind(trade.maker_order_id)
        .execute(&mut *tx)
        .await?;

        // 4b. Release the maker's frozen collateral for this fill portion.
        //
        // Historical bug: this block did not exist. balances.frozen accumulated
        // and never drained on maker fills, producing ~$419M phantom frozen
        // across 30+ accounts (20M orders with stale orders.frozen_margin).
        //
        // Per-fill accounting:
        //   collateral  = trade.amount × trade.price / maker_leverage   (goes to position)
        //   buffer      = collateral × 0.5%                              (returns to available)
        //   frozen_delta = collateral + buffer                           (leaves frozen)
        //
        // The collateral portion has already been credited to
        // positions.collateral_amount by update_positions_after_trade
        // (which runs outside this tx); here we just mirror the bookkeeping
        // on balances + orders.frozen_margin.
        let maker_leverage = trade.maker_leverage.max(1);
        let maker_collateral = trade.amount * trade.price / Decimal::from(maker_leverage);
        let maker_buffer = maker_collateral * Decimal::new(5, 3); // 0.5%
        let maker_frozen_delta = maker_collateral + maker_buffer;

        sqlx::query(
            r#"
            UPDATE balances
            SET frozen = GREATEST(frozen - $1, 0),
                available = available + $2,
                updated_at = NOW()
            WHERE user_address = $3 AND token = 'USDT'
            "#
        )
        .bind(maker_frozen_delta)
        .bind(maker_buffer)
        .bind(&trade.maker_address)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            UPDATE orders
            SET frozen_margin = GREATEST(frozen_margin - $1, 0)
            WHERE id = $2
            "#
        )
        .bind(maker_frozen_delta)
        .bind(trade.maker_order_id)
        .execute(&mut *tx)
        .await?;

        // NOTE: Taker order filled_amount is already set absolutely by the create_order handler
        // (order.rs UPDATE orders SET filled_amount = $2 WHERE id = $4).
        // Do NOT increment it here to avoid double-counting.
        // Taker's frozen release is likewise handled in order.rs (the create_order path),
        // not here, because the taker's absolute filled_amount / status is known synchronously
        // when the submit returns.

        // Commit transaction
        tx.commit().await?;

        debug!("Persisted trade {} with transaction", trade.trade_id);

        // Position updates are done outside transaction with retry mechanism
        // This is intentional: position updates may fail without rolling back the trade
        let mut position_synced = false;
        let max_retries = 3;

        for attempt in 1..=max_retries {
            position_synced = Self::update_positions_after_trade(pool, trade, maker_fee, taker_fee, points_service.clone()).await;

            if position_synced {
                break;
            }

            if attempt < max_retries {
                warn!(
                    "⚠️ Position update attempt {}/{} failed for trade {}, retrying in 100ms...",
                    attempt, max_retries, trade.trade_id
                );
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            } else {
                error!(
                    "❌ All {} position update attempts failed for trade {}. Will be recovered later.",
                    max_retries, trade.trade_id
                );
            }
        }

        // Mark trade as position_synced if both positions were updated successfully
        if position_synced {
            if let Err(e) = Self::mark_trade_position_synced(pool, trade.trade_id).await {
                error!("Failed to mark trade {} as position_synced: {}", trade.trade_id, e);
            }
        }

        Ok(())
    }

    /// Ensure the auxiliary sync table exists.
    ///
    /// `trades.position_synced` cannot be relied on as the source of truth: in
    /// production the `trades` hypertable can fall into a "chunk not found"
    /// state (orphan chunk metadata), which makes UPDATE on `trades` fail and
    /// breaks idempotency — periodic recovery would then re-apply the same
    /// trade to positions over and over, double-counting PnL.
    ///
    /// `trade_position_sync` is a plain table (no Timescale chunks), so its
    /// writes always succeed and serve as the durable idempotency marker.
    pub async fn ensure_sync_table_schema(pool: &PgPool) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS trade_position_sync (
                trade_id  UUID        PRIMARY KEY,
                synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
        )
        .execute(pool)
        .await?;
        Ok(())
    }

    /// Returns true if the trade's positions have already been applied
    /// (according to the durable idempotency table).
    async fn is_trade_position_synced(pool: &PgPool, trade_id: Uuid) -> bool {
        match sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (SELECT 1 FROM trade_position_sync WHERE trade_id = $1)",
        )
        .bind(trade_id)
        .fetch_one(pool)
        .await
        {
            Ok(b) => b,
            Err(e) => {
                warn!("is_trade_position_synced check failed for {}: {}", trade_id, e);
                false
            }
        }
    }

    /// Record that a trade's positions have been applied. Uses the durable
    /// `trade_position_sync` plain table so the marker is reliable even when
    /// the `trades` hypertable is degraded.
    async fn record_trade_position_synced(pool: &PgPool, trade_id: Uuid) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO trade_position_sync (trade_id) VALUES ($1) ON CONFLICT (trade_id) DO NOTHING",
        )
        .bind(trade_id)
        .execute(pool)
        .await?;
        Ok(())
    }

    /// Best-effort UPDATE of `trades.position_synced`. Kept for backwards
    /// compatibility with environments that read this column, but no longer
    /// the source of truth — see `record_trade_position_synced` above.
    /// A `chunk not found` failure here is logged at warn level and ignored.
    async fn mark_trade_position_synced(pool: &PgPool, trade_id: Uuid) -> Result<(), sqlx::Error> {
        match sqlx::query("UPDATE trades SET position_synced = TRUE WHERE id = $1")
            .bind(trade_id)
            .execute(pool)
            .await
        {
            Ok(_) => {
                debug!("Marked trade {} as position_synced", trade_id);
                Ok(())
            }
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("chunk not found") {
                    // Hypertable is degraded — skip silently at warn level.
                    // Idempotency is preserved by trade_position_sync.
                    warn!(
                        "trades.position_synced UPDATE skipped for {} (degraded hypertable: {})",
                        trade_id, msg
                    );
                    Ok(())
                } else {
                    Err(e)
                }
            }
        }
    }

    /// Recover unsynced trades on startup
    ///
    /// This should be called during application startup to process any trades
    /// that were persisted but whose positions weren't updated (e.g., due to restart).
    pub async fn recover_unsynced_trades(pool: &PgPool) -> Result<usize, sqlx::Error> {
        Self::recover_unsynced_trades_with_limit(pool, 10000).await
    }

    /// Recover unsynced trades with configurable batch size
    /// This function loops until all unsynced trades are processed
    pub async fn recover_unsynced_trades_with_limit(pool: &PgPool, batch_size: i64) -> Result<usize, sqlx::Error> {
        info!("🔄 Starting recovery of unsynced trades (batch_size={})...", batch_size);

        let mut total_recovered = 0;
        let mut iteration = 0;
        const MAX_ITERATIONS: usize = 1000; // Safety limit to prevent infinite loops

        loop {
            iteration += 1;
            if iteration > MAX_ITERATIONS {
                warn!("⚠️ Reached max iterations ({}) for trade recovery, stopping", MAX_ITERATIONS);
                break;
            }

            // Fetch batch of trades where position_synced = FALSE *and* the
            // durable sync marker (trade_position_sync) does not yet exist.
            // The latter is what stops periodic recovery from double-counting
            // when trades.position_synced can't be UPDATEd (degraded hypertable).
            // Note: Cast ENUM 'side' to TEXT to avoid sqlx decode errors
            let unsynced_trades: Vec<(Uuid, String, String, String, String, Decimal, Decimal, i32, i32)> = sqlx::query_as(
                r#"
                SELECT t.id, t.symbol, t.maker_address, t.taker_address, t.side::TEXT, t.price, t.amount,
                       COALESCE(t.maker_leverage, 1) as maker_leverage,
                       COALESCE(t.taker_leverage, 1) as taker_leverage
                FROM trades t
                LEFT JOIN trade_position_sync s ON s.trade_id = t.id
                WHERE t.position_synced = FALSE AND s.trade_id IS NULL
                ORDER BY t.created_at ASC
                LIMIT $1
                FOR UPDATE OF t SKIP LOCKED
                "#
            )
            .bind(batch_size)
            .fetch_all(pool)
            .await?;

            let count = unsynced_trades.len();
            if count == 0 {
                if total_recovered > 0 {
                    info!("✅ Trade recovery complete: {} total trades recovered", total_recovered);
                } else {
                    info!("✅ No unsynced trades found");
                }
                return Ok(total_recovered);
            }

            info!("🔧 Batch {}: Found {} unsynced trades to recover", iteration, count);

            let mut recovered = 0;
            for (trade_id, symbol, maker_address, taker_address, side, price, amount, maker_leverage, taker_leverage) in unsynced_trades {
                // Reconstruct trade event for position update
                let is_self_trade = maker_address.eq_ignore_ascii_case(&taker_address);
                let trade = TradeEvent {
                    symbol: symbol.clone(),
                    trade_id,
                    maker_order_id: Uuid::nil(), // Not needed for position update
                    taker_order_id: Uuid::nil(),
                    maker_address: maker_address.clone(),
                    taker_address: taker_address.clone(),
                    side: side.clone(),
                    price,
                    amount,
                    maker_fee: Decimal::ZERO,
                    taker_fee: Decimal::ZERO,
                    timestamp: 0, // Not needed for position update
                    maker_leverage: maker_leverage as u32,
                    taker_leverage: taker_leverage as u32,
                    is_self_trade,
                };

                // Calculate fees for position update
                let trade_value = amount * price;
                let maker_fee = trade_value * Decimal::from_str_exact("0.0002").unwrap();
                let taker_fee = trade_value * Decimal::from_str_exact("0.0005").unwrap();

                // Attempt to update positions
                let success = Self::update_positions_after_trade(pool, &trade, maker_fee, taker_fee, None).await;

                if success {
                    if let Err(e) = Self::mark_trade_position_synced(pool, trade_id).await {
                        error!("Failed to mark recovered trade {} as synced: {}", trade_id, e);
                    } else {
                        recovered += 1;
                        if recovered % 100 == 0 {
                            info!("✅ Recovered {} trades so far...", total_recovered + recovered);
                        }
                    }
                } else {
                    warn!("⚠️ Failed to recover positions for trade {}", trade_id);
                }
            }

            total_recovered += recovered;
            info!("🔄 Batch {} complete: {}/{} trades recovered (total: {})",
                  iteration, recovered, count, total_recovered);

            // If we recovered fewer than batch_size, we've processed all remaining
            if (count as i64) < batch_size {
                break;
            }

            // Small delay between batches to avoid overwhelming the database
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }

        info!("🔄 Trade recovery complete: {} total trades recovered", total_recovered);
        Ok(total_recovered)
    }

    /// Update positions after trade is persisted
    ///
    /// Called after the main transaction commits successfully.
    /// Returns true if both maker and taker positions were updated successfully.
    async fn update_positions_after_trade(
        pool: &PgPool,
        trade: &TradeEvent,
        maker_fee: Decimal,
        taker_fee: Decimal,
        points_service: Option<Arc<PointsService>>,
    ) -> bool {
        // Idempotency guard: if this trade's positions were already applied,
        // do NOT re-apply them. Without this guard, periodic recovery will
        // double-count positions every 5 minutes when trades.position_synced
        // can't be updated (e.g. degraded hypertable).
        if Self::is_trade_position_synced(pool, trade.trade_id).await {
            debug!(
                "Skipping update_positions_after_trade for {} — already recorded as synced",
                trade.trade_id
            );
            return true;
        }

        // Use leverage directly from trade event (no DB query needed)
        let maker_leverage = trade.maker_leverage as i32;
        let taker_leverage = trade.taker_leverage as i32;

        let mut position_service = PositionService::new(pool.clone());
        if let Some(ref ps) = points_service {
            position_service.set_points_service(Arc::clone(ps));
        }

        let maker_side = match trade.side.as_str() {
            "buy" => PositionSide::Short,
            "sell" => PositionSide::Long,
            _ => {
                warn!("Unknown trade side: {}", trade.side);
                return false;
            }
        };

        let size_in_usd = trade.amount * trade.price;
        let collateral_amount = size_in_usd / Decimal::from(maker_leverage);

        let mut maker_success = false;
        let mut taker_success = false;

        match position_service
            .increase_position(
                &trade.maker_address,
                &trade.symbol,
                maker_side,
                collateral_amount,
                maker_leverage,
                trade.price,
                true,
                Some(trade.trade_id),
            )
            .await
        {
            Err(e) => {
                error!(
                    "❌ Failed to update maker position for {} on {}: {:?}",
                    trade.maker_address, trade.symbol, e
                );
            }
            Ok(result) => {
                info!(
                    "✅ Updated maker position for {} on {} (leverage={}, collateral={})",
                    trade.maker_address, trade.symbol, maker_leverage, collateral_amount
                );
                maker_success = true;
                // Accumulate the actual maker fee from this fill onto the
                // position. position/mod.rs reads this on close (in lieu of
                // the legacy `position_fee_rate × close_size` formula).
                // Failure here is logged but does NOT roll back the trade —
                // the cost is under-charge on close, not phantom credit;
                // the reconciliation worker will surface any drift.
                if let Err(e) = sqlx::query(
                    "UPDATE positions
                     SET accumulated_trading_fee = accumulated_trading_fee + $1,
                         updated_at = NOW()
                     WHERE id = $2",
                )
                .bind(maker_fee)
                .bind(result.position.position_id)
                .execute(pool)
                .await
                {
                    error!(
                        "Failed to accumulate maker_fee on position {}: {}",
                        result.position.position_id, e
                    );
                }
            }
        }

        // Update taker position
        let taker_side = match trade.side.as_str() {
            "buy" => PositionSide::Long,
            "sell" => PositionSide::Short,
            _ => {
                warn!("Unknown trade side: {}", trade.side);
                return false;
            }
        };

        let collateral_amount = size_in_usd / Decimal::from(taker_leverage);

        match position_service
            .increase_position(
                &trade.taker_address,
                &trade.symbol,
                taker_side,
                collateral_amount,
                taker_leverage,
                trade.price,
                true,
                Some(trade.trade_id),
            )
            .await
        {
            Err(e) => {
                error!(
                    "❌ Failed to update taker position for {} on {}: {:?}",
                    trade.taker_address, trade.symbol, e
                );
            }
            Ok(result) => {
                info!(
                    "✅ Updated taker position for {} on {} (leverage={}, collateral={})",
                    trade.taker_address, trade.symbol, taker_leverage, collateral_amount
                );
                taker_success = true;
                // Accumulate the actual taker fee — symmetric with the
                // maker side; same error-handling rationale.
                if let Err(e) = sqlx::query(
                    "UPDATE positions
                     SET accumulated_trading_fee = accumulated_trading_fee + $1,
                         updated_at = NOW()
                     WHERE id = $2",
                )
                .bind(taker_fee)
                .bind(result.position.position_id)
                .execute(pool)
                .await
                {
                    error!(
                        "Failed to accumulate taker_fee on position {}: {}",
                        result.position.position_id, e
                    );
                }
            }
        }

        let success = maker_success && taker_success;
        if success {
            // Durable idempotency marker — written even if the legacy
            // `trades.position_synced` UPDATE later fails.
            if let Err(e) = Self::record_trade_position_synced(pool, trade.trade_id).await {
                error!(
                    "Failed to record trade_position_sync for {}: {} — recovery may re-process this trade",
                    trade.trade_id, e
                );
            }
        }
        success
    }

    /// Write a failed trade to the dead letter queue
    ///
    /// Called when all persistence retries have been exhausted.
    /// Stores the complete trade data for manual recovery.
    async fn write_to_dead_letter_queue(
        pool: &PgPool,
        trade: &TradeEvent,
        error_message: &str,
        attempts: u32,
    ) -> Result<(), sqlx::Error> {
        let trade_json = serde_json::to_value(trade).unwrap_or_else(|_| serde_json::json!({}));

        sqlx::query(
            r#"
            INSERT INTO dead_letter_trades (trade_id, trade_data, error_message, attempts, last_attempt_at)
            VALUES ($1, $2, $3, $4, NOW())
            "#,
        )
        .bind(trade.trade_id)
        .bind(&trade_json)
        .bind(error_message)
        .bind(attempts as i32)
        .execute(pool)
        .await?;

        warn!(
            "💀 Trade {} written to dead letter queue after {} attempts",
            trade.trade_id, attempts
        );

        Ok(())
    }

    /// Persist a trade to database and update positions (legacy, non-transactional)
    ///
    /// DEPRECATED: Use persist_trade_with_tx instead.
    /// Kept for backwards compatibility with batch operations.
    #[deprecated(note = "Use persist_trade_with_tx for transactional safety")]
    pub async fn persist_trade(pool: &PgPool, trade: &TradeEvent, referral_service: Option<&Arc<ReferralService>>) -> Result<(), sqlx::Error> {
        // Calculate fees (0.02% maker, 0.05% taker)
        let trade_value = trade.amount * trade.price;
        let maker_fee = trade_value * Decimal::from_str_exact("0.0002").unwrap();
        let taker_fee = trade_value * Decimal::from_str_exact("0.0005").unwrap();

        // 1. Save trade record
        // Use WHERE NOT EXISTS instead of ON CONFLICT for TimescaleDB compatibility
        sqlx::query(
            r#"
            INSERT INTO trades (id, symbol, maker_order_id, taker_order_id, maker_address, taker_address, side, price, amount, maker_fee, taker_fee, created_at)
            SELECT $1, $2, $3, $4, $5, $6, $7::order_side, $8, $9, $10, $11, to_timestamp($12::double precision / 1000)
            WHERE NOT EXISTS (SELECT 1 FROM trades WHERE id = $1)
            "#
        )
        .bind(trade.trade_id)
        .bind(&trade.symbol)
        .bind(trade.maker_order_id)
        .bind(trade.taker_order_id)
        .bind(&trade.maker_address)
        .bind(&trade.taker_address)
        .bind(&trade.side)
        .bind(trade.price)
        .bind(trade.amount)
        .bind(maker_fee)
        .bind(taker_fee)
        .bind(trade.timestamp as f64)
        .execute(pool)
        .await?;

        debug!("Persisted trade: {}", trade.trade_id);

        // 2. Use leverage directly from trade event
        let maker_leverage = trade.maker_leverage as i32;
        let taker_leverage = trade.taker_leverage as i32;

        info!(
            "Trade {}: maker_leverage={}, taker_leverage={}",
            trade.trade_id, maker_leverage, taker_leverage
        );

        // 3. Update positions for maker and taker
        let position_service = PositionService::new(pool.clone());

        // Maker position (opposite side from trade, because maker provides liquidity on the other side)
        {
            info!(
                "Updating maker position: address={}, symbol={}, leverage={}",
                trade.maker_address, trade.symbol, maker_leverage
            );
            let maker_side = match trade.side.as_str() {
                "buy" => PositionSide::Short,  // Taker buys, maker sells
                "sell" => PositionSide::Long,   // Taker sells, maker buys
                _ => {
                    warn!("Unknown trade side: {}", trade.side);
                    return Ok(());
                }
            };

            // Calculate collateral (size_in_usd / leverage)
            let size_in_usd = trade.amount * trade.price;
            let collateral_amount = size_in_usd / Decimal::from(maker_leverage);

            if let Err(e) = position_service.increase_position(
                &trade.maker_address,
                &trade.symbol,
                maker_side,
                collateral_amount,
                maker_leverage,
                trade.price,
                true, // Skip min size check - trade already executed
                Some(trade.trade_id),
            ).await {
                error!(
                    "❌ CRITICAL: Failed to update maker position for {} on {}: {:?}",
                    trade.maker_address, trade.symbol, e
                );
                error!(
                    "Maker position details: trade_id={}, side={:?}, size_usd={}, collateral={}, leverage={}, price={}",
                    trade.trade_id, maker_side, size_in_usd, collateral_amount, maker_leverage, trade.price
                );
            } else {
                info!(
                    "✅ Updated maker position for {} on {} (side={:?}, collateral={})",
                    trade.maker_address, trade.symbol, maker_side, collateral_amount
                );
            }
        }

        // Taker position (same side as trade, because taker initiates the trade)
        {
            info!(
                "Updating taker position: address={}, symbol={}, leverage={}",
                trade.taker_address, trade.symbol, taker_leverage
            );
            let taker_side = match trade.side.as_str() {
                "buy" => PositionSide::Long,  // Taker buys, opens long
                "sell" => PositionSide::Short, // Taker sells, opens short
                _ => {
                    warn!("Unknown trade side: {}", trade.side);
                    return Ok(());
                }
            };

            // Calculate collateral (size_in_usd / leverage)
            let size_in_usd = trade.amount * trade.price;
            let collateral_amount = size_in_usd / Decimal::from(taker_leverage);

            if let Err(e) = position_service.increase_position(
                &trade.taker_address,
                &trade.symbol,
                taker_side,
                collateral_amount,
                taker_leverage,
                trade.price,
                true, // Skip min size check - trade already executed
                Some(trade.trade_id),
            ).await {
                error!(
                    "❌ CRITICAL: Failed to update taker position for {} on {}: {:?}",
                    trade.taker_address, trade.symbol, e
                );
                error!(
                    "Taker position details: trade_id={}, side={:?}, size_usd={}, collateral={}, leverage={}, price={}",
                    trade.trade_id, taker_side, size_in_usd, collateral_amount, taker_leverage, trade.price
                );
            } else {
                info!(
                    "✅ Updated taker position for {} on {} (side={:?}, collateral={})",
                    trade.taker_address, trade.symbol, taker_side, collateral_amount
                );
            }
        }

        // 4. Handle referral commission
        // Check if maker has a referrer (only for trades AFTER referral binding)
        let maker_referrer: Option<(String,)> = sqlx::query_as(
            r#"
            SELECT rr.referrer_address
            FROM referral_relations rr
            WHERE rr.referee_address = $1
              AND to_timestamp($2::double precision / 1000) >= rr.created_at
            "#
        )
        .bind(&trade.maker_address.to_lowercase())
        .bind(trade.timestamp as f64)
        .fetch_optional(pool)
        .await?;

        if let Some((referrer_address,)) = maker_referrer {
            let commission_rate = get_referral_commission_rate(pool, &referrer_address).await;
            let commission = maker_fee * commission_rate;
            sqlx::query(
                r#"
                INSERT INTO referral_earnings
                (id, referrer_address, referee_address, trade_id, event_type, volume, commission, token, status, created_at)
                VALUES ($1, $2, $3, $4, 'trade', $5, $6, 'USDT', 'pending', to_timestamp($7::double precision / 1000))
                "#
            )
            .bind(uuid::Uuid::new_v4())
            .bind(&referrer_address)
            .bind(&trade.maker_address.to_lowercase())
            .bind(trade.trade_id)
            .bind(trade_value)
            .bind(commission)
            .bind(trade.timestamp as f64)
            .execute(pool)
            .await?;

            // Queue for on-chain sync
            if let Some(ref referral_svc) = referral_service {
                referral_svc.queue_trade(PendingTradeRecord {
                    trader: referrer_address.clone(),
                    volume_usd: trade_value,
                    fee_usd: commission,
                    trade_id: trade.trade_id.to_string(),
                }).await;
            }

            debug!("Recorded referral commission {} for maker {} (referrer: {})", commission, trade.maker_address, referrer_address);
        }

        // Check if taker has a referrer (only for trades AFTER referral binding)
        let taker_referrer: Option<(String,)> = sqlx::query_as(
            r#"
            SELECT rr.referrer_address
            FROM referral_relations rr
            WHERE rr.referee_address = $1
              AND to_timestamp($2::double precision / 1000) >= rr.created_at
            "#
        )
        .bind(&trade.taker_address.to_lowercase())
        .bind(trade.timestamp as f64)
        .fetch_optional(pool)
        .await?;

        if let Some((referrer_address,)) = taker_referrer {
            let commission_rate = get_referral_commission_rate(pool, &referrer_address).await;
            let commission = taker_fee * commission_rate;
            sqlx::query(
                r#"
                INSERT INTO referral_earnings
                (id, referrer_address, referee_address, trade_id, event_type, volume, commission, token, status, created_at)
                VALUES ($1, $2, $3, $4, 'trade', $5, $6, 'USDT', 'pending', to_timestamp($7::double precision / 1000))
                "#
            )
            .bind(uuid::Uuid::new_v4())
            .bind(&referrer_address)
            .bind(&trade.taker_address.to_lowercase())
            .bind(trade.trade_id)
            .bind(trade_value)
            .bind(commission)
            .bind(trade.timestamp as f64)
            .execute(pool)
            .await?;

            // Queue for on-chain sync
            if let Some(ref referral_svc) = referral_service {
                referral_svc.queue_trade(PendingTradeRecord {
                    trader: referrer_address.clone(),
                    volume_usd: trade_value,
                    fee_usd: commission,
                    trade_id: trade.trade_id.to_string(),
                }).await;
            }

            debug!("Recorded referral commission {} for taker {} (referrer: {})", commission, trade.taker_address, referrer_address);
        }

        Ok(())
    }

    /// Persist an order to database
    async fn persist_order(
        pool: &PgPool,
        symbol: &str,
        user_address: &str,
        result: &MatchResult,
        side: Side,
        order_type: OrderType,
        amount: Decimal,
        price: Option<Decimal>,
        leverage: u32,
    ) -> Result<(), sqlx::Error> {
        let status = match result.status {
            OrderStatus::Open => "open",
            OrderStatus::PartiallyFilled => "partially_filled",
            OrderStatus::Filled => "filled",
            OrderStatus::Cancelled => "cancelled",
            OrderStatus::Rejected => "rejected",
        };

        let side_str = match side {
            Side::Buy => "buy",
            Side::Sell => "sell",
        };

        let order_type_str = match order_type {
            OrderType::Limit => "limit",
            OrderType::Market => "market",
            OrderType::TakeProfitLimit => "take_profit_limit",
            OrderType::StopLossLimit => "stop_loss_limit",
            OrderType::TakeProfitMarket => "take_profit_market",
            OrderType::StopLossMarket => "stop_loss_market",
        };

        sqlx::query(
            r#"
            INSERT INTO orders (id, symbol, user_address, side, order_type, status, price, amount, filled_amount, leverage, created_at)
            VALUES ($1, $2, $3, $4::order_side, $5::order_type, $6::order_status, $7, $8, $9, $10, NOW())
            ON CONFLICT (id) DO UPDATE SET
                status = $6::order_status,
                filled_amount = $9,
                updated_at = NOW()
            "#
        )
        .bind(result.order_id)
        .bind(symbol)
        .bind(user_address)
        .bind(side_str)
        .bind(order_type_str)
        .bind(status)
        .bind(price)
        .bind(amount)
        .bind(result.filled_amount)
        .bind(leverage as i32)
        .execute(pool)
        .await?;

        // Update maker orders if there were trades
        for trade in &result.trades {
            sqlx::query(
                r#"
                UPDATE orders
                SET filled_amount = filled_amount + $1,
                    status = CASE
                        WHEN filled_amount + $1 >= amount THEN 'filled'::order_status
                        ELSE 'partially_filled'::order_status
                    END,
                    updated_at = NOW()
                WHERE id = $2
                "#
            )
            .bind(trade.amount)
            .bind(trade.maker_order_id)
            .execute(pool)
            .await?;
        }

        debug!("Persisted order: {}", result.order_id);
        Ok(())
    }

    /// Update order status
    async fn update_order_status(pool: &PgPool, order_id: Uuid, status: &str) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            UPDATE orders
            SET status = $1::order_status, updated_at = NOW()
            WHERE id = $2
            "#
        )
        .bind(status)
        .bind(order_id)
        .execute(pool)
        .await?;

        debug!("Updated order status: id={}, status={}", order_id, status);
        Ok(())
    }

    /// Batch persist trades
    pub async fn batch_persist_trades(pool: &PgPool, trades: &[TradeEvent], referral_service: Option<&Arc<ReferralService>>) -> Result<usize, sqlx::Error> {
        if trades.is_empty() {
            return Ok(0);
        }

        let mut tx = pool.begin().await?;
        let mut count = 0;

        for trade in trades {
            // Calculate fees
            let trade_value = trade.amount * trade.price;
            let maker_fee = trade_value * Decimal::from_str_exact("0.0002").unwrap();
            let taker_fee = trade_value * Decimal::from_str_exact("0.0005").unwrap();

            // Use WHERE NOT EXISTS instead of ON CONFLICT for TimescaleDB compatibility
            sqlx::query(
                r#"
                INSERT INTO trades (id, symbol, maker_order_id, taker_order_id, maker_address, taker_address, side, price, amount, maker_fee, taker_fee, created_at)
                SELECT $1, $2, $3, $4, $5, $6, $7::order_side, $8, $9, $10, $11, to_timestamp($12::double precision / 1000)
                WHERE NOT EXISTS (SELECT 1 FROM trades WHERE id = $1)
                "#
            )
            .bind(trade.trade_id)
            .bind(&trade.symbol)
            .bind(trade.maker_order_id)
            .bind(trade.taker_order_id)
            .bind(&trade.maker_address)
            .bind(&trade.taker_address)
            .bind(&trade.side)
            .bind(trade.price)
            .bind(trade.amount)
            .bind(maker_fee)
            .bind(taker_fee)
            .bind(trade.timestamp as f64)
            .execute(&mut *tx)
            .await?;

            // Handle referral commission for maker
            let maker_referrer: Option<(String,)> = sqlx::query_as(
                r#"
                SELECT u.referrer_address
                FROM users u
                WHERE u.address = $1 AND u.referrer_address IS NOT NULL
                "#
            )
            .bind(&trade.maker_address.to_lowercase())
            .fetch_optional(&mut *tx)
            .await?;

            if let Some((referrer_address,)) = maker_referrer {
                let commission_rate = get_referral_commission_rate(&mut *tx, &referrer_address).await;
                let commission = maker_fee * commission_rate;
                sqlx::query(
                    r#"
                    INSERT INTO referral_earnings 
                    (id, referrer_address, referee_address, trade_id, event_type, volume, commission, token, status, created_at)
                    VALUES ($1, $2, $3, $4, 'trade', $5, $6, 'USDT', 'pending', to_timestamp($7::double precision / 1000))
                    "#
                )
                .bind(uuid::Uuid::new_v4())
                .bind(&referrer_address)
                .bind(&trade.maker_address.to_lowercase())
                .bind(trade.trade_id)
                .bind(trade_value)
                .bind(commission)
                .bind(trade.timestamp as f64)
                .execute(&mut *tx)
                .await?;

                // Queue for on-chain sync
                if let Some(ref referral_svc) = referral_service {
                    referral_svc.queue_trade(PendingTradeRecord {
                        trader: referrer_address.clone(),
                        volume_usd: trade_value,
                        fee_usd: commission,
                        trade_id: trade.trade_id.to_string(),
                    }).await;
                }
            }

            // Handle referral commission for taker
            let taker_referrer: Option<(String,)> = sqlx::query_as(
                r#"
                SELECT u.referrer_address
                FROM users u
                WHERE u.address = $1 AND u.referrer_address IS NOT NULL
                "#
            )
            .bind(&trade.taker_address.to_lowercase())
            .fetch_optional(&mut *tx)
            .await?;

            if let Some((referrer_address,)) = taker_referrer {
                let commission_rate = get_referral_commission_rate(&mut *tx, &referrer_address).await;
                let commission = taker_fee * commission_rate;
                sqlx::query(
                    r#"
                    INSERT INTO referral_earnings 
                    (id, referrer_address, referee_address, trade_id, event_type, volume, commission, token, status, created_at)
                    VALUES ($1, $2, $3, $4, 'trade', $5, $6, 'USDT', 'pending', to_timestamp($7::double precision / 1000))
                    "#
                )
                .bind(uuid::Uuid::new_v4())
                .bind(&referrer_address)
                .bind(&trade.taker_address.to_lowercase())
                .bind(trade.trade_id)
                .bind(trade_value)
                .bind(commission)
                .bind(trade.timestamp as f64)
                .execute(&mut *tx)
                .await?;

                // Queue for on-chain sync
                if let Some(ref referral_svc) = referral_service {
                    referral_svc.queue_trade(PendingTradeRecord {
                        trader: referrer_address.clone(),
                        volume_usd: trade_value,
                        fee_usd: commission,
                        trade_id: trade.trade_id.to_string(),
                    }).await;
                }
            }

            count += 1;
        }

        tx.commit().await?;
        info!("Batch persisted {} trades", count);
        Ok(count)
    }

    /// Create deferred TP/SL trigger orders for limit orders.
    ///
    /// When a limit order is placed with tp_price/sl_price, those values are
    /// stored in the orders table but TP/SL trigger orders aren't created
    /// until the order fills. This method is called after a trade is
    /// persisted and checks both taker and maker orders.
    async fn create_deferred_tp_sl(pool: &PgPool, trade: &TradeEvent) {
        let is_taker_buy = trade.side == "buy";

        // Check both sides of the trade
        for &(order_id, ref user_addr, is_buy) in &[
            (trade.taker_order_id, &trade.taker_address, is_taker_buy),
            (trade.maker_order_id, &trade.maker_address, !is_taker_buy),
        ] {
            // 1. Read tp/sl from order (NULL if not set or already consumed)
            let row: Option<(Option<Decimal>, Option<Decimal>)> = match sqlx::query_as(
                "SELECT tp_price, sl_price FROM orders WHERE id = $1",
            )
            .bind(order_id)
            .fetch_optional(pool)
            .await
            {
                Ok(r) => r,
                Err(_) => continue,
            };

            let Some((tp_price, sl_price)) = row else {
                continue;
            };
            if tp_price.is_none() && sl_price.is_none() {
                continue;
            }

            // 2. Find the user's open position
            let position_side_str = if is_buy { "long" } else { "short" };
            let closing_side_str = if is_buy { "sell" } else { "buy" };

            let pos: Option<(Uuid,)> = sqlx::query_as(
                "SELECT id FROM positions \
                 WHERE user_address = $1 AND symbol = $2 \
                   AND side = $3::position_side AND status = 'open' \
                 ORDER BY updated_at DESC LIMIT 1",
            )
            .bind(user_addr.to_lowercase())
            .bind(&trade.symbol)
            .bind(position_side_str)
            .fetch_optional(pool)
            .await
            .ok()
            .flatten();

            let Some((position_id,)) = pos else {
                continue;
            };

            // 3. Skip if position already has TP/SL
            let has_tp_sl: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM position_tp_sl WHERE position_id = $1)",
            )
            .bind(position_id)
            .fetch_one(pool)
            .await
            .unwrap_or(false);

            if has_tp_sl {
                continue;
            }

            // 4. Create trigger orders
            let mut tp_order_id: Option<Uuid> = None;
            let mut sl_order_id: Option<Uuid> = None;

            if let Some(tp) = tp_price {
                let id = Uuid::new_v4();
                // Long TP → triggered when price goes above; Short TP → below
                let cond = if is_buy { "above" } else { "below" };
                if sqlx::query(
                    "INSERT INTO trigger_orders \
                     (id, user_address, position_id, market_symbol, trigger_type, \
                      side, size, trigger_price, trigger_condition, \
                      reduce_only, close_position, status, created_at, updated_at) \
                     VALUES ($1, $2, $3, $4, 'take_profit', \
                             $5::order_side, 0, $6, $7::trigger_condition, \
                             true, true, 'active', NOW(), NOW())",
                )
                .bind(id)
                .bind(user_addr.to_lowercase())
                .bind(position_id)
                .bind(&trade.symbol)
                .bind(closing_side_str)
                .bind(tp)
                .bind(cond)
                .execute(pool)
                .await
                .is_ok()
                {
                    tp_order_id = Some(id);
                }
            }

            if let Some(sl) = sl_price {
                let id = Uuid::new_v4();
                // Long SL → triggered when price falls below; Short SL → above
                let cond = if is_buy { "below" } else { "above" };
                if sqlx::query(
                    "INSERT INTO trigger_orders \
                     (id, user_address, position_id, market_symbol, trigger_type, \
                      side, size, trigger_price, trigger_condition, \
                      reduce_only, close_position, status, created_at, updated_at) \
                     VALUES ($1, $2, $3, $4, 'stop_loss', \
                             $5::order_side, 0, $6, $7::trigger_condition, \
                             true, true, 'active', NOW(), NOW())",
                )
                .bind(id)
                .bind(user_addr.to_lowercase())
                .bind(position_id)
                .bind(&trade.symbol)
                .bind(closing_side_str)
                .bind(sl)
                .bind(cond)
                .execute(pool)
                .await
                .is_ok()
                {
                    sl_order_id = Some(id);
                }
            }

            // 5. Upsert position_tp_sl record
            if tp_order_id.is_some() || sl_order_id.is_some() {
                let _ = sqlx::query(
                    "INSERT INTO position_tp_sl \
                     (id, position_id, user_address, market_symbol, \
                      take_profit_price, take_profit_trigger_order_id, \
                      stop_loss_price, stop_loss_trigger_order_id, \
                      created_at, updated_at) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), NOW()) \
                     ON CONFLICT (position_id) DO UPDATE SET \
                      take_profit_price = COALESCE(EXCLUDED.take_profit_price, position_tp_sl.take_profit_price), \
                      take_profit_trigger_order_id = COALESCE(EXCLUDED.take_profit_trigger_order_id, position_tp_sl.take_profit_trigger_order_id), \
                      stop_loss_price = COALESCE(EXCLUDED.stop_loss_price, position_tp_sl.stop_loss_price), \
                      stop_loss_trigger_order_id = COALESCE(EXCLUDED.stop_loss_trigger_order_id, position_tp_sl.stop_loss_trigger_order_id), \
                      updated_at = NOW()",
                )
                .bind(Uuid::new_v4())
                .bind(position_id)
                .bind(user_addr.to_lowercase())
                .bind(&trade.symbol)
                .bind(tp_price)
                .bind(tp_order_id)
                .bind(sl_price)
                .bind(sl_order_id)
                .execute(pool)
                .await;

                info!(
                    "Deferred TP/SL created: order={}, position={}, tp={:?}, sl={:?}",
                    order_id, position_id, tp_price, sl_price
                );
            }

            // 6. Clear tp/sl from order (consumed, won't re-create on restart)
            let _ = sqlx::query(
                "UPDATE orders SET tp_price = NULL, sl_price = NULL WHERE id = $1",
            )
            .bind(order_id)
            .execute(pool)
            .await;
        }
    }
}

#[cfg(test)]
mod tests {
    // Integration tests would require a database connection
    // Unit tests are in engine.rs
}
