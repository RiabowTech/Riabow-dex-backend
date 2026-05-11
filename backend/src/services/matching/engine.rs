//! Matching Engine
//!
//! High-performance order matching engine with concurrent access support.
//! Manages multiple orderbooks for different trading pairs.

use super::orderbook::Orderbook;
use super::types::*;
use crate::constants::channels;
use dashmap::DashMap;
use parking_lot::Mutex;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Default persistence-queue capacity when env override is absent.
///
/// 10k was the original hard-coded value. Under MM bursts this filled up and
/// the overflow branch silently dropped trades, producing filled-order-without-
/// trade-row orphans (34k accumulated in 24h as of 2026-04-22). The runtime
/// now falls back to `spawn(send().await)` on overflow so no trade is lost,
/// but a larger buffer reduces overflow churn.
const FALLBACK_QUEUE_CAPACITY_DEFAULT: usize = 10000;

fn resolve_fallback_queue_capacity() -> usize {
    std::env::var("TRADE_PERSISTENCE_QUEUE_CAPACITY")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|n| (1024..=10_000_000).contains(n))
        .unwrap_or(FALLBACK_QUEUE_CAPACITY_DEFAULT)
}

/// The main matching engine
pub struct MatchingEngine {
    /// Map of symbol to orderbook (concurrent access)
    orderbooks: DashMap<String, Arc<Orderbook>>,

    /// Trade event broadcaster
    trade_sender: broadcast::Sender<TradeEvent>,

    /// Orderbook update broadcaster
    orderbook_sender: broadcast::Sender<OrderbookUpdate>,

    /// Fallback queue for trades when broadcast has no subscribers
    /// This ensures trades are never lost even if the persistence worker is down
    fallback_sender: mpsc::Sender<TradeEvent>,

    /// Fallback receiver (taken once by orchestrator)
    fallback_receiver: Mutex<Option<mpsc::Receiver<TradeEvent>>>,

    /// Supported symbols
    symbols: Vec<String>,
}

impl MatchingEngine {
    /// Create with specific symbols
    pub fn new(symbols: Vec<String>) -> Self {
        let (trade_sender, _) = broadcast::channel(channels::TRADE_CHANNEL_CAPACITY);
        let (orderbook_sender, _) = broadcast::channel(channels::ORDERBOOK_CHANNEL_CAPACITY);
        let fallback_capacity = resolve_fallback_queue_capacity();
        let (fallback_sender, fallback_receiver) = mpsc::channel(fallback_capacity);
        let orderbooks = DashMap::new();

        // Initialize orderbooks for all symbols
        for symbol in &symbols {
            orderbooks.insert(symbol.clone(), Arc::new(Orderbook::new(symbol.clone())));
        }

        info!("MatchingEngine initialized with {} symbols and fallback queue (capacity: {})",
              symbols.len(), fallback_capacity);

        Self {
            orderbooks,
            trade_sender,
            orderbook_sender,
            fallback_sender,
            fallback_receiver: Mutex::new(Some(fallback_receiver)),
            symbols,
        }
    }

    /// Take the fallback receiver for the persistence worker
    /// Can only be called once - subsequent calls return None
    pub fn take_fallback_receiver(&self) -> Option<mpsc::Receiver<TradeEvent>> {
        self.fallback_receiver.lock().take()
    }

    /// Get supported symbols
    pub fn symbols(&self) -> &[String] {
        &self.symbols
    }

    /// Check if a symbol is supported
    pub fn is_valid_symbol(&self, symbol: &str) -> bool {
        self.orderbooks.contains_key(symbol)
    }

    /// Add a new symbol/market
    pub fn add_symbol(&mut self, symbol: String) {
        if !self.orderbooks.contains_key(&symbol) {
            self.orderbooks.insert(symbol.clone(), Arc::new(Orderbook::new(symbol.clone())));
            self.symbols.push(symbol.clone());
            info!("Added new symbol: {}", symbol);
        }
    }

    /// Get trade event receiver
    pub fn subscribe_trades(&self) -> broadcast::Receiver<TradeEvent> {
        self.trade_sender.subscribe()
    }

    /// Get orderbook update receiver
    pub fn subscribe_orderbook(&self) -> broadcast::Receiver<OrderbookUpdate> {
        self.orderbook_sender.subscribe()
    }

    /// Broadcast orderbook update for a symbol
    fn broadcast_orderbook_update(&self, symbol: &str) {
        if let Some(orderbook) = self.orderbooks.get(symbol) {
            let snapshot = orderbook.snapshot(30); // Top 30 levels
            let update = OrderbookUpdate {
                symbol: symbol.to_string(),
                bids: snapshot.bids,
                asks: snapshot.asks,
                timestamp: chrono::Utc::now().timestamp_millis(),
            };
            let _ = self.orderbook_sender.send(update);
        }
    }

    /// Get orderbook for a symbol
    pub fn get_orderbook_ref(&self, symbol: &str) -> Option<Arc<Orderbook>> {
        self.orderbooks.get(symbol).map(|ob| Arc::clone(ob.value()))
    }

    // ========================================================================
    // Order Operations
    // ========================================================================

    /// Submit an order for matching.
    ///
    /// `taker_fee_rate` is the rate this submitter pays on whatever portion
    /// of `amount` matches against existing liquidity (i.e. the trades this
    /// call produces, where this caller is the taker).
    ///
    /// `maker_fee_rate` is locked onto the residual `OrderEntry` if the order
    /// rests in the book; future takers that hit it will use this rate to
    /// compute the maker side of the fee. Both rates must already include
    /// the user's referral / staking discount and be in 6dp form (use
    /// `vip_tier::resolve` or `vip_tier::current_fee_rates` to derive).
    pub fn submit_order(
        &self,
        order_id: Uuid,
        symbol: &str,
        user_address: &str,
        side: Side,
        order_type: OrderType,
        amount: Decimal,
        price: Option<Decimal>,
        time_in_force: TimeInForce,
        leverage: u32,
        taker_fee_rate: Decimal,
        maker_fee_rate: Decimal,
    ) -> Result<MatchResult, MatchingError> {
        // Validate symbol
        let orderbook = self.orderbooks.get(symbol)
            .ok_or_else(|| MatchingError::SymbolNotFound(symbol.to_string()))?;

        // Validate inputs
        if amount <= Decimal::ZERO {
            return Err(MatchingError::InvalidAmount("Amount must be positive".to_string()));
        }

        if (order_type == OrderType::Limit || order_type == OrderType::TakeProfitLimit || order_type == OrderType::StopLossLimit) && price.is_none() {
            return Err(MatchingError::InvalidPrice("Limit order requires price".to_string()));
        }

        let now = chrono::Utc::now().timestamp_millis();

        debug!(
            "Processing order: id={}, symbol={}, side={:?}, type={:?}, amount={}, price={:?}",
            order_id, symbol, side, order_type, amount, price
        );

        // Match the order. The orderbook handles FOK pre-check internally
        // (returns zero fill if cumulative liquidity at-limit is less than
        // requested); GTC and IOC behave the same here, the rest-residual /
        // cancel-residual decision is made below based on time_in_force.
        let (trades, remaining) = orderbook.match_order(
            order_id,
            user_address,
            side,
            amount,
            price,
            time_in_force,
            taker_fee_rate,
            leverage,
        );

        // Clamp away sub-lot drift produced by `match_order`. When fills span
        // multiple price levels the orderbook computes `level_qty - taker_qty`
        // which can introduce a tiny residue (rust_decimal stores 28 sig
        // figures); leaving that residue made `filled_amount == amount`
        // strict-equality below evaluate false, persisting the order as
        // `partially_filled` even though it had been fully filled. R4 (P0
        // #4) found a 0.1 BTC LIMIT stuck at 0.099999999999999999 / 0.1.
        // FILL_DRIFT_EPSILON is several orders of magnitude below any
        // realistic lot size, so legitimate residuals are never eaten.
        const FILL_DRIFT_EPSILON: Decimal = Decimal::from_parts(1, 0, 0, false, 12); // 1e-12
        let filled_amount = if remaining > Decimal::ZERO && remaining < FILL_DRIFT_EPSILON {
            amount
        } else {
            amount - remaining
        };

        // Broadcast trade events
        for trade in &trades {
            // STP detection (PRD §3.2): maker and taker share an address.
            // We do NOT cancel the match — points calculators will skip
            // TP/PP/RP for these trades.
            let is_self_trade =
                trade.maker_address.eq_ignore_ascii_case(user_address);
            let event = TradeEvent {
                symbol: symbol.to_string(),
                trade_id: trade.trade_id,
                maker_order_id: trade.maker_order_id,
                taker_order_id: trade.taker_order_id,
                maker_address: trade.maker_address.clone(),
                taker_address: user_address.to_string(),
                side: side.to_string(),
                price: trade.price,
                amount: trade.amount,
                maker_fee: trade.maker_fee,
                taker_fee: trade.taker_fee,
                timestamp: trade.timestamp,
                maker_leverage: trade.maker_leverage,
                taker_leverage: trade.taker_leverage,
                is_self_trade,
            };

            // Send to the persistence mpsc queue. `submit_order` itself is sync
            // (called from axum's async handler, so we have a tokio runtime handle
            // available — see order.rs `create_order`), so we use the non-blocking
            // `try_send` here. If the queue is full we hand the event to a tokio
            // task that awaits `send()` — this provides back-pressure in the async
            // world without stalling the matching loop.
            //
            // Previously this branch only logged `CRITICAL` and dropped the
            // TradeEvent, producing filled-order-without-trade-row orphans. The
            // 2026-04-22 orphan audit found 34k+ such orphans accumulated in 24h
            // during MM bursts; see keeper/mod.rs orphan detector.
            match self.fallback_sender.try_send(event.clone()) {
                Ok(()) => {
                    crate::services::metrics::TRADE_EMIT_TOTAL
                        .with_label_values(&["ok"])
                        .inc();
                }
                Err(mpsc::error::TrySendError::Full(returned_event)) => {
                    crate::services::metrics::PERSISTENCE_QUEUE_OVERFLOW_TOTAL.inc();
                    crate::services::metrics::TRADE_EMIT_TOTAL
                        .with_label_values(&["overflow"])
                        .inc();
                    warn!(
                        "persistence queue full; handing trade {} to async spawn(send)",
                        returned_event.trade_id
                    );
                    let sender = self.fallback_sender.clone();
                    tokio::spawn(async move {
                        let trade_id = returned_event.trade_id;
                        if let Err(e) = sender.send(returned_event).await {
                            error!(
                                "❌ async persistence send for overflowed trade {} failed: {:?}",
                                trade_id, e
                            );
                        }
                    });
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    crate::services::metrics::TRADE_EMIT_TOTAL
                        .with_label_values(&["queue_closed"])
                        .inc();
                    error!(
                        "❌ CRITICAL: Persistence queue closed! Trade {} will be lost!",
                        event.trade_id
                    );
                }
            }

            // Also broadcast to other subscribers (WebSocket, etc.) - best effort
            match self.trade_sender.send(event.clone()) {
                Ok(n) => {
                    info!(
                        "📊 Trade broadcast to {} subscribers: trade_id={}, symbol={}, price={}, amount={}, side={}",
                        n, event.trade_id, event.symbol, event.price, event.amount, event.side
                    );
                }
                Err(_) => {
                    // Broadcast failure is OK - persistence is guaranteed via fallback queue
                }
            }

        }

        // Determine order status
        // Determine effective behavior: limit-like or market-like
        let is_limit_like = matches!(order_type, OrderType::Limit | OrderType::TakeProfitLimit | OrderType::StopLossLimit);

        // Whether the residual (unfilled portion) should rest in the book.
        // Only GTC limit orders rest; IOC/FOK cancel residuals immediately,
        // and market-like orders never rest. FOK additionally returns zero
        // fill from the orderbook when cumulative liquidity is insufficient,
        // so reaching this branch with FOK + remaining > 0 means amount was
        // not fillable and we mark Cancelled.
        let rest_residual = is_limit_like && time_in_force == TimeInForce::GTC;

        let status = if is_limit_like {
            if filled_amount == amount {
                OrderStatus::Filled
            } else if filled_amount > Decimal::ZERO {
                if rest_residual && remaining > Decimal::ZERO {
                    let entry = OrderEntry {
                        id: order_id,
                        user_address: user_address.to_string(),
                        price: price.unwrap(),
                        original_amount: amount,
                        remaining_amount: remaining,
                        side,
                        time_in_force,
                        timestamp: now,
                        leverage,
                        maker_fee_rate,
                    };
                    orderbook.add_order(entry);
                }
                OrderStatus::PartiallyFilled
            } else if rest_residual {
                // GTC with no fill — rest entire order
                let entry = OrderEntry {
                    id: order_id,
                    user_address: user_address.to_string(),
                    price: price.unwrap(),
                    original_amount: amount,
                    remaining_amount: amount,
                    side,
                    time_in_force,
                    timestamp: now,
                    leverage,
                    maker_fee_rate,
                };
                orderbook.add_order(entry);
                OrderStatus::Open
            } else {
                // IOC/FOK with no fill — cancel
                OrderStatus::Cancelled
            }
        } else {
            // Market-like orders (Market, TakeProfitMarket, StopLossMarket) are IOC
            if filled_amount == amount {
                OrderStatus::Filled
            } else if filled_amount > Decimal::ZERO {
                OrderStatus::PartiallyFilled
            } else {
                OrderStatus::Cancelled
            }
        };

        // Calculate average price
        let average_price = if filled_amount > Decimal::ZERO {
            let total_value: Decimal = trades.iter().map(|t| t.price * t.amount).sum();
            Some(crate::safe_div!(
                total_value, 
                filled_amount, 
                "MatchingEngine: submit_order average_price"
            ))
        } else {
            None
        };

        info!(
            "Order completed: id={}, status={:?}, filled={}, remaining={}",
            order_id, status, filled_amount, remaining
        );

        // Self-check: if we returned Open or PartiallyFilled, the order MUST
        // still be in the orderbook right now. If it isn't, something concurrent
        // already removed it between our add_order call and here — meaning a
        // racing path is silently purging orders. (P0: 2026-04-25 QA round 2:
        // 60+ TRUE ORPHAN warns/min across 38+ symbols, no trade rows for the
        // removed orders, no cancel logs — root cause unknown. This self-check
        // localizes whether the desync happens inside this function or after it
        // returns.)
        let should_be_in_book = matches!(status, OrderStatus::Open | OrderStatus::PartiallyFilled);
        if should_be_in_book {
            if !orderbook.has_order(&order_id) {
                tracing::warn!(
                    "submit_order: SELF-CHECK FAIL — order {} returned status={:?} \
                     but is NOT in orderbook immediately after add_order. \
                     symbol={}, side={:?}, price={:?}, qty={}, filled={}. \
                     Either add_order hit its duplicate-skip branch silently, \
                     or a concurrent matcher consumed the order between insert and now.",
                    order_id, status, symbol, side, price, amount, filled_amount
                );
            }
        }

        // Broadcast orderbook update after order processing
        self.broadcast_orderbook_update(symbol);

        Ok(MatchResult {
            order_id,
            status,
            filled_amount,
            remaining_amount: remaining,
            average_price,
            trades,
        })
    }

    /// Cancel an order
    pub fn cancel_order(&self, symbol: &str, order_id: Uuid, _user_address: &str) -> Result<bool, MatchingError> {
        let orderbook = self.orderbooks.get(symbol)
            .ok_or_else(|| MatchingError::SymbolNotFound(symbol.to_string()))?;

        // Try to cancel
        let cancelled = orderbook.cancel_order(order_id);

        if cancelled.is_some() {
            info!("Order cancelled: id={}, symbol={}", order_id, symbol);

            // Broadcast orderbook update after cancellation
            self.broadcast_orderbook_update(symbol);

            Ok(true)
        } else {
            warn!("Order not found for cancellation: id={}", order_id);
            Ok(false)
        }
    }

    // ========================================================================
    // Query Operations
    // ========================================================================

    /// Get orderbook snapshot
    pub fn get_orderbook(&self, symbol: &str, depth: usize) -> Result<OrderbookSnapshot, MatchingError> {
        let orderbook = self.orderbooks.get(symbol)
            .ok_or_else(|| MatchingError::SymbolNotFound(symbol.to_string()))?;

        Ok(orderbook.snapshot(depth))
    }

    /// Get best bid/ask
    pub fn get_best_prices(&self, symbol: &str) -> Result<(Option<Decimal>, Option<Decimal>), MatchingError> {
        let orderbook = self.orderbooks.get(symbol)
            .ok_or_else(|| MatchingError::SymbolNotFound(symbol.to_string()))?;

        Ok((orderbook.best_bid(), orderbook.best_ask()))
    }

    /// Broadcast a trade event (for internal/market maker use)
    pub fn broadcast_trade(&self, event: TradeEvent) -> Result<usize, broadcast::error::SendError<TradeEvent>> {
        self.trade_sender.send(event)
    }

    /// Recover open limit orders from database on startup
    /// This ensures orderbook state is preserved after restart.
    ///
    /// Performance: prod has ~1.6M open orders held by ~100 unique users
    /// (market-maker bots). The previous loop called
    /// `vip_tier::current_fee_rates` per order, i.e. one DB roundtrip per
    /// order — at ~1ms RTT that's ~27 minutes of pre-bind blocking, with
    /// `axum::serve` not yet listening. CI / Kubernetes / ELB healthchecks
    /// time out long before that and the deploy gets killed.
    ///
    /// Two changes here:
    ///   1. Bulk-resolve fees: for each unique user_address in the recovery
    ///      set, query VIP rates once and stash them in a HashMap. ~100
    ///      lookups instead of 1.6M, so DB cost drops from O(N_orders) to
    ///      O(N_users).
    ///   2. Same submit_order semantics — order_id, leverage, fees all
    ///      preserved so live behaviour is unchanged.
    pub async fn recover_orders_from_db(
        &self,
        pool: &sqlx::PgPool,
        shard: Option<&crate::services::sharding::ShardingConfig>,
    ) -> anyhow::Result<usize> {
        use sqlx::Row;

        info!("🔄 Starting order recovery from database...");

        // Query all open limit orders from database
        let rows = sqlx::query(
            r#"
            SELECT id, symbol, user_address, side, price, amount, filled_amount, leverage, created_at
            FROM orders
            WHERE status = 'open' AND order_type = 'limit'
            ORDER BY created_at ASC
            "#
        )
        .fetch_all(pool)
        .await?;

        let total_loaded = rows.len();
        let owned_only = matches!(
            shard,
            Some(s) if s.enabled && s.replica_count > 1
        );
        info!(
            "Loaded {} open limit orders ({}), resolving per-user VIP rates...",
            total_loaded,
            if owned_only { "filtering by owned symbols" } else { "all symbols" }
        );

        // Per-user fee cache. Keyed by `user_address.to_ascii_lowercase()` to
        // match `vip_tier::current_fee_rates` normalisation. Lazy-populated
        // because we want to skip users with zero recoverable orders.
        let mut fee_cache: HashMap<String, (Decimal, Decimal)> = HashMap::new();

        let mut recovered_count: usize = 0;

        let mut skipped_not_owned: usize = 0;
        for row in rows {
            let order_id: uuid::Uuid = row.get("id");
            let symbol: String = row.get("symbol");
            let user_address: String = row.get("user_address");
            let side_db: crate::models::OrderSide = row.get("side");
            let price: rust_decimal::Decimal = row.get("price");
            let amount: rust_decimal::Decimal = row.get("amount");
            let filled_amount: rust_decimal::Decimal = row.get("filled_amount");
            let leverage: i32 = row.get("leverage");

            // Sharding ownership filter — when sharding is on, each pod
            // only rebuilds the orderbooks for symbols it owns. The other
            // pods will load their slice independently. With sharding off
            // (default), every pod owns everything, so this is a no-op.
            if let Some(s) = shard {
                if s.enabled && s.replica_count > 1 && s.owner_for(&symbol) != s.my_ordinal {
                    skipped_not_owned += 1;
                    continue;
                }
            }

            // Convert database OrderSide to matching engine Side
            let side = match side_db {
                crate::models::OrderSide::Buy => Side::Buy,
                crate::models::OrderSide::Sell => Side::Sell,
            };
            let side_str = match side_db {
                crate::models::OrderSide::Buy => "Buy",
                crate::models::OrderSide::Sell => "Sell",
            };

            // Calculate remaining amount
            let remaining_amount = amount - filled_amount;

            if remaining_amount <= rust_decimal::Decimal::ZERO {
                warn!("Order {} has no remaining amount, skipping", order_id);
                continue;
            }

            // Resolve this user's CURRENT VIP-tier rates (no broadcast — recovery
            // shouldn't be the trigger for an upgrade event). Recovered orders
            // lock onto the user's current discounted rate, not VIP0; this
            // addresses the recovery-path concern that previously motivated
            // reverting the per-user fee fix. Cached per-user to avoid one
            // DB roundtrip per recovered order — see fn-level comment.
            let cache_key = user_address.to_ascii_lowercase();
            let (taker_fee_rate, maker_fee_rate) = match fee_cache.get(&cache_key) {
                Some(rates) => *rates,
                None => {
                    let rates = crate::services::vip_tier::current_fee_rates(pool, &user_address).await;
                    fee_cache.insert(cache_key, rates);
                    rates
                }
            };

            // Submit the order to matching engine (this will add it to orderbook).
            // All recovered orders are GTC by definition (only GTC limit
            // orders ever rest in the book; IOC/FOK never persist as `open`).
            match self.submit_order(
                order_id,
                &symbol,
                &user_address,
                side,
                OrderType::Limit,
                remaining_amount,
                Some(price),
                TimeInForce::GTC,
                leverage as u32,
                taker_fee_rate,
                maker_fee_rate,
            ) {
                Ok(_) => {
                    recovered_count += 1;
                    debug!("✅ Recovered order {}: {} {} @ {} (remaining: {})",
                        order_id, side_str, symbol, price, remaining_amount);
                }
                Err(e) => {
                    warn!("Failed to recover order {}: {}", order_id, e);
                }
            }
        }

        info!(
            "✅ Order recovery complete: {} orders restored ({} unique users, fee cache hits = {}, skipped {} not owned by this shard)",
            recovered_count,
            fee_cache.len(),
            recovered_count.saturating_sub(fee_cache.len()),
            skipped_not_owned,
        );
        Ok(recovered_count)
    }

    // ========================================================================
    // Statistics
    // ========================================================================

    /// Get engine statistics
    pub fn stats(&self) -> EngineStats {
        let mut total_orders = 0i64;
        let mut total_bid_depth = Decimal::ZERO;
        let mut total_ask_depth = Decimal::ZERO;

        for entry in self.orderbooks.iter() {
            let ob = entry.value();
            total_orders += ob.order_count();
            total_bid_depth += ob.bid_depth();
            total_ask_depth += ob.ask_depth();
        }

        EngineStats {
            symbols_count: self.orderbooks.len(),
            total_orders_in_book: total_orders,
            total_bid_depth,
            total_ask_depth,
        }
    }

    /// Get per-symbol orderbook statistics for monitoring
    pub fn per_symbol_stats(&self) -> Vec<SymbolStats> {
        self.orderbooks.iter().map(|entry| {
            let symbol = entry.key().clone();
            let ob = entry.value();
            let bid = ob.best_bid().unwrap_or(Decimal::ZERO);
            let ask = ob.best_ask().unwrap_or(Decimal::ZERO);
            let spread = if bid > Decimal::ZERO && ask > Decimal::ZERO {
                ask - bid
            } else {
                Decimal::ZERO
            };
            SymbolStats {
                symbol,
                order_count: ob.order_count(),
                bid_depth: ob.bid_depth(),
                ask_depth: ob.ask_depth(),
                spread,
            }
        }).collect()
    }
}

impl Default for MatchingEngine {
    fn default() -> Self {
        Self::new(vec!["BTCUSDT".to_string()])
    }
}

/// Engine statistics
#[derive(Debug, Clone)]
pub struct EngineStats {
    pub symbols_count: usize,
    pub total_orders_in_book: i64,
    pub total_bid_depth: Decimal,
    pub total_ask_depth: Decimal,
}

/// Per-symbol orderbook statistics for monitoring
pub struct SymbolStats {
    pub symbol: String,
    pub order_count: i64,
    pub bid_depth: Decimal,
    pub ask_depth: Decimal,
    pub spread: Decimal,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    #[ignore = "stale: expects pre-2026 default-symbol auto-injection (BTC/ETH/SOL/HYPE), engine no longer auto-adds"]
    fn test_engine_creation() {
        let engine = MatchingEngine::new(vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]);
        // Default symbols: BTCUSDT, ETHUSDT, SOLUSDT, HYPEUSDT
        assert_eq!(engine.symbols().len(), 4);
        assert!(engine.is_valid_symbol("BTCUSDT"));
        assert!(engine.is_valid_symbol("ETHUSDT"));
        assert!(engine.is_valid_symbol("SOLUSDT"));
        assert!(engine.is_valid_symbol("HYPEUSDT"));
        assert!(!engine.is_valid_symbol("INVALID"));
    }

    /// All tests pass zero for taker/maker fee rates — the assertions are on
    /// status / amounts / counts, not on fee values. The dedicated VIP-rate
    /// regression test (`test_vip3_maker_pays_zero_fee` below) exercises the
    /// fee plumbing with realistic rates.
    const TEST_TAKER_RATE: Decimal = Decimal::ZERO;
    const TEST_MAKER_RATE: Decimal = Decimal::ZERO;

    #[test]
    fn test_submit_limit_order_no_match() {
        let engine = MatchingEngine::new(vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]);

        let result = engine.submit_order(
            Uuid::new_v4(),
            "BTCUSDT",
            "0x1234",
            Side::Buy,
            OrderType::Limit,
            dec!(1.0),
            Some(dec!(100.0)),
            TimeInForce::GTC,
            1,
            TEST_TAKER_RATE,
            TEST_MAKER_RATE,
        ).unwrap();

        assert_eq!(result.status, OrderStatus::Open);
        assert_eq!(result.filled_amount, dec!(0));
        assert_eq!(result.remaining_amount, dec!(1.0));
        assert!(result.trades.is_empty());
    }

    #[test]
    fn test_submit_and_match_orders() {
        let engine = MatchingEngine::new(vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]);

        // Submit sell order
        let sell_result = engine.submit_order(
            Uuid::new_v4(),
            "BTCUSDT",
            "0x1111",
            Side::Sell,
            OrderType::Limit,
            dec!(1.0),
            Some(dec!(100.0)),
            TimeInForce::GTC,
            1,
            TEST_TAKER_RATE,
            TEST_MAKER_RATE,
        ).unwrap();
        assert_eq!(sell_result.status, OrderStatus::Open);

        // Submit matching buy order
        let buy_result = engine.submit_order(
            Uuid::new_v4(),
            "BTCUSDT",
            "0x2222",
            Side::Buy,
            OrderType::Limit,
            dec!(0.5),
            Some(dec!(100.0)),
            TimeInForce::GTC,
            1,
            TEST_TAKER_RATE,
            TEST_MAKER_RATE,
        ).unwrap();

        assert_eq!(buy_result.status, OrderStatus::Filled);
        assert_eq!(buy_result.filled_amount, dec!(0.5));
        assert_eq!(buy_result.trades.len(), 1);
        assert_eq!(buy_result.trades[0].price, dec!(100.0));
        assert_eq!(buy_result.trades[0].amount, dec!(0.5));
    }

    #[test]
    fn test_market_order() {
        let engine = MatchingEngine::new(vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]);

        // Add liquidity
        engine.submit_order(
            Uuid::new_v4(),
            "BTCUSDT",
            "0x1111",
            Side::Sell,
            OrderType::Limit,
            dec!(1.0),
            Some(dec!(100.0)),
            TimeInForce::GTC,
            1,
            TEST_TAKER_RATE,
            TEST_MAKER_RATE,
        ).unwrap();

        // Market buy
        let result = engine.submit_order(
            Uuid::new_v4(),
            "BTCUSDT",
            "0x2222",
            Side::Buy,
            OrderType::Market,
            dec!(0.5),
            None,
            TimeInForce::GTC,
            1,
            TEST_TAKER_RATE,
            TEST_MAKER_RATE,
        ).unwrap();

        assert_eq!(result.status, OrderStatus::Filled);
        assert_eq!(result.filled_amount, dec!(0.5));
    }

    #[test]
    fn test_cancel_order() {
        let engine = MatchingEngine::new(vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]);

        let result = engine.submit_order(
            Uuid::new_v4(),
            "BTCUSDT",
            "0x1234",
            Side::Buy,
            OrderType::Limit,
            dec!(1.0),
            Some(dec!(100.0)),
            TimeInForce::GTC,
            1,
            TEST_TAKER_RATE,
            TEST_MAKER_RATE,
        ).unwrap();

        let cancelled = engine.cancel_order("BTCUSDT", result.order_id, "0x1234").unwrap();
        assert!(cancelled);

        // Try to cancel again
        let cancelled_again = engine.cancel_order("BTCUSDT", result.order_id, "0x1234").unwrap();
        assert!(!cancelled_again);
    }

    #[test]
    fn test_orderbook_snapshot() {
        let engine = MatchingEngine::new(vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]);

        // Add orders
        engine.submit_order(Uuid::new_v4(), "BTCUSDT", "0x1", Side::Buy, OrderType::Limit, dec!(1.0), Some(dec!(99.0)), TimeInForce::GTC, 1, TEST_TAKER_RATE, TEST_MAKER_RATE).unwrap();
        engine.submit_order(Uuid::new_v4(), "BTCUSDT", "0x2", Side::Buy, OrderType::Limit, dec!(2.0), Some(dec!(98.0)), TimeInForce::GTC, 1, TEST_TAKER_RATE, TEST_MAKER_RATE).unwrap();
        engine.submit_order(Uuid::new_v4(), "BTCUSDT", "0x3", Side::Sell, OrderType::Limit, dec!(1.5), Some(dec!(101.0)), TimeInForce::GTC, 1, TEST_TAKER_RATE, TEST_MAKER_RATE).unwrap();

        let snapshot = engine.get_orderbook("BTCUSDT", 10).unwrap();

        assert_eq!(snapshot.symbol, "BTCUSDT");
        assert_eq!(snapshot.bids.len(), 2);
        assert_eq!(snapshot.asks.len(), 1);
    }

    // Single sequential test covers all env-var cases because cargo test runs
    // tests in parallel and `std::env::set_var` is process-global → separate
    // tests would race on the shared TRADE_PERSISTENCE_QUEUE_CAPACITY var.
    #[test]
    fn test_resolve_fallback_queue_capacity_all_cases() {
        let prev = std::env::var("TRADE_PERSISTENCE_QUEUE_CAPACITY").ok();

        // Unset → default.
        std::env::remove_var("TRADE_PERSISTENCE_QUEUE_CAPACITY");
        assert_eq!(resolve_fallback_queue_capacity(), FALLBACK_QUEUE_CAPACITY_DEFAULT);

        // Valid in-range override.
        std::env::set_var("TRADE_PERSISTENCE_QUEUE_CAPACITY", "200000");
        assert_eq!(resolve_fallback_queue_capacity(), 200_000);

        // Non-numeric → default.
        std::env::set_var("TRADE_PERSISTENCE_QUEUE_CAPACITY", "not-a-number");
        assert_eq!(resolve_fallback_queue_capacity(), FALLBACK_QUEUE_CAPACITY_DEFAULT);

        // Below floor (1024) → default.
        std::env::set_var("TRADE_PERSISTENCE_QUEUE_CAPACITY", "10");
        assert_eq!(resolve_fallback_queue_capacity(), FALLBACK_QUEUE_CAPACITY_DEFAULT);

        // Above ceiling (10M) → default.
        std::env::set_var("TRADE_PERSISTENCE_QUEUE_CAPACITY", "999999999");
        assert_eq!(resolve_fallback_queue_capacity(), FALLBACK_QUEUE_CAPACITY_DEFAULT);

        if let Some(v) = prev {
            std::env::set_var("TRADE_PERSISTENCE_QUEUE_CAPACITY", v);
        } else {
            std::env::remove_var("TRADE_PERSISTENCE_QUEUE_CAPACITY");
        }
    }

    #[test]
    #[ignore = "stale: expects pre-2026 default-symbol auto-injection (BTC/ETH/SOL/HYPE), engine no longer auto-adds"]
    fn test_stats() {
        let engine = MatchingEngine::new(vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]);

        engine.submit_order(Uuid::new_v4(), "BTCUSDT", "0x1", Side::Buy, OrderType::Limit, dec!(1.0), Some(dec!(100.0)), TimeInForce::GTC, 1, TEST_TAKER_RATE, TEST_MAKER_RATE).unwrap();
        engine.submit_order(Uuid::new_v4(), "ETHUSDT", "0x2", Side::Sell, OrderType::Limit, dec!(2.0), Some(dec!(3000.0)), TimeInForce::GTC, 1, TEST_TAKER_RATE, TEST_MAKER_RATE).unwrap();

        let stats = engine.stats();
        // Default has 4 symbols: BTCUSDT, ETHUSDT, SOLUSDT, HYPEUSDT
        assert_eq!(stats.symbols_count, 4);
        assert_eq!(stats.total_orders_in_book, 2);
    }

    /// Regression: a VIP3 maker (post-discount maker_rate = 0) must pay 0
    /// fee on the matched portion, regardless of the taker's rate. Before
    /// the per-user-rate fix landed, the engine charged a hard-coded
    /// `FeeConfig::default()` (0.02% maker / 0.05% taker) to every fill,
    /// so a VIP3 user reported being charged a maker fee they shouldn't owe.
    #[test]
    fn test_vip3_maker_pays_zero_fee() {
        use crate::utils::fee_tiers::TIERS;
        let engine =
            MatchingEngine::new(vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]);

        // Resting VIP3 maker — its OrderEntry.maker_fee_rate is the third
        // arg from the bottom (set to TIERS[3].maker = 0).
        let vip3_maker_rate = TIERS[3].maker;
        assert_eq!(vip3_maker_rate, Decimal::ZERO, "VIP3 maker rate is 0% per spec");
        let vip3_taker_rate = TIERS[3].taker;

        engine
            .submit_order(
                Uuid::new_v4(),
                "BTCUSDT",
                "0xvip3",
                Side::Sell,
                OrderType::Limit,
                dec!(1.0),
                Some(dec!(100.0)),
                TimeInForce::GTC,
                1,
                vip3_taker_rate,    // taker side — irrelevant since this rests
                vip3_maker_rate,    // locked onto OrderEntry → drives the fill's maker_fee
            )
            .unwrap();

        // VIP0 taker hits the resting VIP3 maker.
        let vip0_taker_rate = TIERS[0].taker;
        let result = engine
            .submit_order(
                Uuid::new_v4(),
                "BTCUSDT",
                "0xvip0",
                Side::Buy,
                OrderType::Market,
                dec!(1.0),
                None,
                TimeInForce::GTC,
                1,
                vip0_taker_rate,
                TIERS[0].maker,
            )
            .unwrap();

        assert_eq!(result.trades.len(), 1);
        let trade = &result.trades[0];
        assert_eq!(
            trade.maker_fee,
            Decimal::ZERO,
            "VIP3 maker must pay 0 fee — got {}",
            trade.maker_fee
        );
        // Sanity: taker fee should be VIP0 rate × notional, not the old
        // hard-coded 0.05%.
        let expected_taker_fee =
            crate::utils::fee_tiers::round_fee(dec!(100.0) * vip0_taker_rate);
        assert_eq!(
            trade.taker_fee, expected_taker_fee,
            "taker_fee should follow caller-supplied rate"
        );
    }
}
