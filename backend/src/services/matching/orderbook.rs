//! Orderbook Implementation
//!
//! High-performance orderbook with lock-free concurrent access.

use super::types::*;
use crate::utils::fee_tiers::round_fee;
use dashmap::DashMap;
use parking_lot::RwLock;
use rust_decimal::Decimal;
use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicI64, Ordering as AtomicOrdering};
use uuid::Uuid;

/// A single market orderbook with concurrent access support
pub struct Orderbook {
    pub symbol: String,

    /// Bids sorted by price descending (highest first)
    /// Using RwLock for price level operations
    bids: RwLock<BTreeMap<PriceLevel, VecDeque<OrderEntry>>>,

    /// Asks sorted by price ascending (lowest first)
    asks: RwLock<BTreeMap<PriceLevel, VecDeque<OrderEntry>>>,

    /// Order ID to (side, price_level) mapping for O(1) cancellation
    order_index: DashMap<Uuid, (Side, PriceLevel)>,

    /// Last trade price
    last_trade_price: AtomicI64,

    /// Order count
    order_count: AtomicI64,
}

impl Orderbook {
    /// Create a new orderbook for a symbol
    pub fn new(symbol: String) -> Self {
        Self {
            symbol,
            bids: RwLock::new(BTreeMap::new()),
            asks: RwLock::new(BTreeMap::new()),
            order_index: DashMap::new(),
            last_trade_price: AtomicI64::new(0),
            order_count: AtomicI64::new(0),
        }
    }

    /// Get the symbol
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    /// Get total order count
    pub fn order_count(&self) -> i64 {
        self.order_count.load(AtomicOrdering::Relaxed)
    }

    /// Get last trade price
    pub fn last_trade_price(&self) -> Option<Decimal> {
        let raw = self.last_trade_price.load(AtomicOrdering::Relaxed);
        if raw == 0 {
            None
        } else {
            Some(Decimal::from(raw) / Decimal::from(100_000_000))
        }
    }

    /// Set last trade price
    pub fn set_last_trade_price(&self, price: Decimal) {
        let raw = (price * Decimal::from(100_000_000)).to_string().parse::<i64>().unwrap_or(0);
        self.last_trade_price.store(raw, AtomicOrdering::Relaxed);
    }

    /// Get best bid price
    pub fn best_bid(&self) -> Option<Decimal> {
        let bids = self.bids.read();
        bids.keys().next_back().map(|p| p.to_decimal())
    }

    /// Get best ask price
    pub fn best_ask(&self) -> Option<Decimal> {
        let asks = self.asks.read();
        asks.keys().next().map(|p| p.to_decimal())
    }

    /// Get spread
    pub fn spread(&self) -> Option<Decimal> {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) => Some(ask - bid),
            _ => None,
        }
    }

    /// Add an order to the orderbook
    ///
    /// Idempotent: if an order with the same id is already present, the call
    /// is a no-op. This prevents the 2× filled_amount bug where a maker was
    /// pushed into the VecDeque twice (e.g. via restart recovery racing with
    /// a still-in-memory copy, or multi-instance recovery against a shared DB)
    /// and then matched twice by takers.
    pub fn add_order(&self, entry: OrderEntry) {
        let order_id = entry.id;
        let price_level = PriceLevel::from_decimal(entry.price);
        let side = entry.side;

        // Both the side queue (bids/asks) write and the order_index insert
        // must happen UNDER the same write lock so concurrent callers see
        // either both or neither. The previous implementation released the
        // bids/asks lock before inserting into order_index, leaving a
        // window where an immediately-following cancel_order would see
        // order_index missing the entry and return Ok(false) — even though
        // the queue already had the order. The order would then sit in
        // the book uncancellable and could be matched later by a
        // marketable opposite. (P0: 2026-04-25 QA — observed 60% race rate
        // on rapid place+cancel pairs.)
        match side {
            Side::Buy => {
                let mut bids = self.bids.write();
                if self.order_index.contains_key(&order_id) {
                    tracing::warn!(
                        "add_order: order {} already in book for {}, skipping duplicate push (original_amount={}, side={:?})",
                        order_id, self.symbol, entry.original_amount, entry.side
                    );
                    return;
                }
                bids.entry(price_level)
                    .or_insert_with(VecDeque::new)
                    .push_back(entry);
                self.order_index.insert(order_id, (side, price_level));
            }
            Side::Sell => {
                let mut asks = self.asks.write();
                if self.order_index.contains_key(&order_id) {
                    tracing::warn!(
                        "add_order: order {} already in book for {}, skipping duplicate push (original_amount={}, side={:?})",
                        order_id, self.symbol, entry.original_amount, entry.side
                    );
                    return;
                }
                asks.entry(price_level)
                    .or_insert_with(VecDeque::new)
                    .push_back(entry);
                self.order_index.insert(order_id, (side, price_level));
            }
        }
        self.order_count.fetch_add(1, AtomicOrdering::Relaxed);
    }

    /// Cancel an order by ID.
    ///
    /// Fast path: O(1) lookup via `order_index`. Slow path: full scan over
    /// both bid and ask queues when the index says we don't have it.
    ///
    /// Why the slow path exists: the 2026-04-25 QA round 2 saw real orders
    /// (DB `status='open'`, surfaced by `/fapi/v1/openOrders`) that
    /// `cancel_order` could not find via `order_index` — at a 60% rate for
    /// extreme-price LIMIT orders. PR #40 made `add_order` atomic on
    /// `order_index`+`bids/asks`, which closed one window, but a residual
    /// asymmetric path remains: the cancel side does
    /// `order_index.remove()` BEFORE acquiring `bids.write()/asks.write()`,
    /// so a concurrent matcher (which removes a fully-filled maker from
    /// `order_index` at line ~304/390) can race with a place-then-cancel
    /// pair and leave behind a queue entry whose index is gone.
    ///
    /// Rather than re-engineer the locking (which would slow the hot
    /// matching path), we accept that the index can lag and keep the
    /// queues authoritative: if the index miss, scan both queues. This
    /// preserves the O(1) fast path for the common case.
    pub fn cancel_order(&self, order_id: Uuid) -> Option<OrderEntry> {
        // Fast path: index hit.
        if let Some((_, (side, price_level))) = self.order_index.remove(&order_id) {
            let entry = self.remove_from_queue(side, price_level, order_id);
            if entry.is_some() {
                self.order_count.fetch_sub(1, AtomicOrdering::Relaxed);
            }
            return entry;
        }

        // Slow path: index miss. Scan both sides for the order id.
        // This is the orphan-recovery path (P0: 2026-04-25 QA round 2).
        if let Some(entry) = self.scan_and_remove(Side::Buy, order_id) {
            tracing::warn!(
                "cancel_order: orphan recovered via slow scan (Buy side) — \
                 order {} was in bids queue but missing from order_index. \
                 symbol={}, price={}",
                order_id, self.symbol, entry.price
            );
            self.order_count.fetch_sub(1, AtomicOrdering::Relaxed);
            return Some(entry);
        }
        if let Some(entry) = self.scan_and_remove(Side::Sell, order_id) {
            tracing::warn!(
                "cancel_order: orphan recovered via slow scan (Sell side) — \
                 order {} was in asks queue but missing from order_index. \
                 symbol={}, price={}",
                order_id, self.symbol, entry.price
            );
            self.order_count.fetch_sub(1, AtomicOrdering::Relaxed);
            return Some(entry);
        }

        // Cancel hit an order missing from BOTH the index AND both queues.
        // Until 2026-04-26 this branch warned loudly under the assumption
        // it indicated a silent-remove bug; PR #58 added the maker-consumed
        // counter to disambiguate. A prod investigation on the same day
        // (sample of 80 orphan ids from the 09:52 burst, cross-checked
        // against the orders+trades tables) classified the entire backlog:
        //
        //   - 95% (76/80): order's DB row already in `cancelled` status
        //     with zero trades → DOUBLE-CANCEL race. Two cancel paths
        //     (single + cancel_all, or two concurrent singles) each pass
        //     their `status='open'` pre-check before either's UPDATE
        //     commits, both call engine.cancel_order, the second hits this
        //     branch.
        //   - 4% (3/80): order in `filled` status with maker-side trades
        //     equal to amount → CANCEL-AFTER-FILL. Matcher fully consumed
        //     the maker (counted by orderbook_matcher_maker_consumed_total),
        //     a cancel arrived afterwards and landed here.
        //   - 1% (1/80): order in `cancelled` status but with full maker
        //     fills → cancel-vs-match race in a different layer (separate
        //     issue, not the orphan-flood cause).
        //
        // None of the 80 had `status='open' / 'partially_filled'` at the
        // time of the orphan log, so there is NO silent-remove bug to
        // chase here. The counter pair will not be 1:1 even on healthy
        // traffic — a single matcher consumption can absorb N redundant
        // cancel attempts. Treat the metric as a double-cancel-rate gauge,
        // not a desync signal.
        //
        // Reproduction SQL (substitute the candidate ids):
        //   WITH cand AS (SELECT id::uuid FROM (VALUES ('...')) t(id)),
        //        mt   AS (SELECT maker_order_id oid, SUM(amount) qty
        //                 FROM trades WHERE maker_order_id IN (SELECT id FROM cand)
        //                 GROUP BY 1),
        //        tt   AS (SELECT taker_order_id oid, SUM(amount) qty
        //                 FROM trades WHERE taker_order_id IN (SELECT id FROM cand)
        //                 GROUP BY 1)
        //   SELECT o.status, COUNT(*) FROM orders o
        //   LEFT JOIN mt ON mt.oid=o.id LEFT JOIN tt ON tt.oid=o.id
        //   WHERE o.id IN (SELECT id FROM cand) GROUP BY 1;
        //
        // Demoting to debug: the per-id signal is now noise (4k+/min on
        // active majors), the counter is the real ops-facing surface, and
        // a future real silent-remove bug would still be visible as a
        // delta against the population of orders that the DB believes are
        // currently `open` (to be added if/when the question recurs).
        crate::services::metrics::ORDERBOOK_CANCEL_NOT_FOUND_TOTAL
            .with_label_values(&[&self.symbol, "true_orphan"])
            .inc();
        tracing::debug!(
            "cancel_order: orphan — order {} missing from index AND both queues. \
             symbol={}. Almost always double-cancel or cancel-after-fill; see \
             orderbook_cancel_not_found_total for rate.",
            order_id, self.symbol
        );
        None
    }

    /// Helper: remove an order from a specific queue at a specific price level.
    fn remove_from_queue(&self, side: Side, price_level: PriceLevel, order_id: Uuid) -> Option<OrderEntry> {
        match side {
            Side::Buy => {
                let mut bids = self.bids.write();
                let queue = bids.get_mut(&price_level)?;
                let pos = queue.iter().position(|o| o.id == order_id)?;
                let entry = queue.remove(pos);
                if queue.is_empty() {
                    bids.remove(&price_level);
                }
                entry
            }
            Side::Sell => {
                let mut asks = self.asks.write();
                let queue = asks.get_mut(&price_level)?;
                let pos = queue.iter().position(|o| o.id == order_id)?;
                let entry = queue.remove(pos);
                if queue.is_empty() {
                    asks.remove(&price_level);
                }
                entry
            }
        }
    }

    /// Helper: scan all price levels of one side for a given order id, remove
    /// if found, and clean up the index entry if it later reappears. Used as
    /// the slow-path recovery in `cancel_order`.
    fn scan_and_remove(&self, side: Side, order_id: Uuid) -> Option<OrderEntry> {
        let removed = match side {
            Side::Buy => {
                let mut bids = self.bids.write();
                let mut found_at: Option<PriceLevel> = None;
                let mut found_entry: Option<OrderEntry> = None;
                for (price_level, queue) in bids.iter_mut() {
                    if let Some(pos) = queue.iter().position(|o| o.id == order_id) {
                        let entry = queue.remove(pos)?;
                        found_at = Some(*price_level);
                        found_entry = Some(entry);
                        break;
                    }
                }
                if let Some(level) = found_at {
                    if bids.get(&level).map(|q| q.is_empty()).unwrap_or(false) {
                        bids.remove(&level);
                    }
                }
                found_entry
            }
            Side::Sell => {
                let mut asks = self.asks.write();
                let mut found_at: Option<PriceLevel> = None;
                let mut found_entry: Option<OrderEntry> = None;
                for (price_level, queue) in asks.iter_mut() {
                    if let Some(pos) = queue.iter().position(|o| o.id == order_id) {
                        let entry = queue.remove(pos)?;
                        found_at = Some(*price_level);
                        found_entry = Some(entry);
                        break;
                    }
                }
                if let Some(level) = found_at {
                    if asks.get(&level).map(|q| q.is_empty()).unwrap_or(false) {
                        asks.remove(&level);
                    }
                }
                found_entry
            }
        };
        // Stale index defensive: if the index regrew an entry for this id
        // while we were scanning, drop it — the queue is the source of truth.
        if removed.is_some() {
            self.order_index.remove(&order_id);
        }
        removed
    }

    /// Match an incoming order against the orderbook
    /// Returns (trades, remaining_amount)
    ///
    /// `time_in_force` controls FOK semantics: when `FOK`, this function
    /// inspects the opposite-side book under the same lock as the actual
    /// match and returns `(vec![], amount)` (zero fill) if the cumulative
    /// matchable amount at-or-better than `limit_price` is less than the
    /// requested `amount`. GTC and IOC behave identically here — the
    /// "rest residual" / "cancel residual" decision is made by the engine
    /// after this returns.
    pub fn match_order(
        &self,
        taker_order_id: Uuid,
        _taker_address: &str,
        side: Side,
        mut amount: Decimal,
        limit_price: Option<Decimal>,
        time_in_force: TimeInForce,
        taker_fee_rate: Decimal,
        leverage: u32,
    ) -> (Vec<TradeExecution>, Decimal) {
        let mut trades = Vec::new();
        let now = chrono::Utc::now().timestamp_millis();

        match side {
            Side::Buy => {
                // Match against asks (lowest first)
                let mut asks = self.asks.write();

                // FOK pre-check: count matchable amount at or below limit;
                // if insufficient, abort without consuming any liquidity.
                if time_in_force == TimeInForce::FOK {
                    let mut fillable = Decimal::ZERO;
                    for (level, queue) in asks.iter() {
                        if let Some(limit) = limit_price {
                            if level.to_decimal() > limit { break; }
                        }
                        for o in queue.iter() { fillable += o.remaining_amount; }
                        if fillable >= amount { break; }
                    }
                    if fillable < amount {
                        return (Vec::new(), amount);
                    }
                }

                let price_levels: Vec<PriceLevel> = asks.keys().cloned().collect();

                for price_level in price_levels {
                    if amount <= Decimal::ZERO {
                        break;
                    }

                    let level_price = price_level.to_decimal();

                    // Check price limit for limit orders
                    if let Some(limit) = limit_price {
                        if level_price > limit {
                            break;
                        }
                    }

                    if let Some(queue) = asks.get_mut(&price_level) {
                        while let Some(maker) = queue.front_mut() {
                            if amount <= Decimal::ZERO {
                                break;
                            }

                            let trade_amount = amount.min(maker.remaining_amount);
                            let trade_price = maker.price;

                            // Calculate fees from per-side rates: maker pays
                            // its locked OrderEntry.maker_fee_rate (VIP tier
                            // at placement time, post-discount), taker pays
                            // the rate the caller resolved at submit time.
                            // round_fee normalises to 6dp so the charged fee
                            // matches what /preview-order quoted.
                            let trade_value = trade_amount * trade_price;
                            let maker_fee = round_fee(trade_value * maker.maker_fee_rate);
                            let taker_fee = round_fee(trade_value * taker_fee_rate);

                            let trade = TradeExecution {
                                trade_id: Uuid::new_v4(),
                                maker_order_id: maker.id,
                                taker_order_id,
                                maker_address: maker.user_address.clone(),
                                price: trade_price,
                                amount: trade_amount,
                                maker_fee,
                                taker_fee,
                                timestamp: now,
                                maker_leverage: maker.leverage,
                                taker_leverage: leverage,
                            };

                            trades.push(trade);
                            amount -= trade_amount;
                            maker.remaining_amount -= trade_amount;

                            // Update last trade price
                            self.set_last_trade_price(trade_price);

                            // Remove fully filled maker order
                            if maker.remaining_amount <= Decimal::ZERO {
                                let maker_id = maker.id;
                                queue.pop_front();
                                self.order_index.remove(&maker_id);
                                self.order_count.fetch_sub(1, AtomicOrdering::Relaxed);
                                // Diagnostic for the orphan investigation
                                // (PR #58): pair this with the cancel_order
                                // TRUE ORPHAN warn so ops can tell whether a
                                // missing-from-engine id was just consumed
                                // here (expected) vs disappeared via some
                                // unidentified path (unexpected).
                                tracing::debug!(
                                    "matcher: maker fully consumed — id={}, symbol={}, side=Sell-side queue (taker=Buy)",
                                    maker_id, self.symbol
                                );
                                crate::services::metrics::ORDERBOOK_MATCHER_MAKER_CONSUMED_TOTAL
                                    .with_label_values(&[&self.symbol])
                                    .inc();
                            }
                        }

                        if queue.is_empty() {
                            asks.remove(&price_level);
                        }
                    }
                }
            }
            Side::Sell => {
                // Match against bids (highest first)
                let mut bids = self.bids.write();

                if time_in_force == TimeInForce::FOK {
                    let mut fillable = Decimal::ZERO;
                    for (level, queue) in bids.iter().rev() {
                        if let Some(limit) = limit_price {
                            if level.to_decimal() < limit { break; }
                        }
                        for o in queue.iter() { fillable += o.remaining_amount; }
                        if fillable >= amount { break; }
                    }
                    if fillable < amount {
                        return (Vec::new(), amount);
                    }
                }

                let price_levels: Vec<PriceLevel> = bids.keys().rev().cloned().collect();

                for price_level in price_levels {
                    if amount <= Decimal::ZERO {
                        break;
                    }

                    let level_price = price_level.to_decimal();

                    // Check price limit for limit orders
                    if let Some(limit) = limit_price {
                        if level_price < limit {
                            break;
                        }
                    }

                    if let Some(queue) = bids.get_mut(&price_level) {
                        while let Some(maker) = queue.front_mut() {
                            if amount <= Decimal::ZERO {
                                break;
                            }

                            let trade_amount = amount.min(maker.remaining_amount);
                            let trade_price = maker.price;

                            // See Buy branch above for the per-side rate
                            // semantics — maker pays its locked rate, taker
                            // pays the caller-resolved rate.
                            let trade_value = trade_amount * trade_price;
                            let maker_fee = round_fee(trade_value * maker.maker_fee_rate);
                            let taker_fee = round_fee(trade_value * taker_fee_rate);

                            let trade = TradeExecution {
                                trade_id: Uuid::new_v4(),
                                maker_order_id: maker.id,
                                taker_order_id,
                                maker_address: maker.user_address.clone(),
                                price: trade_price,
                                amount: trade_amount,
                                maker_fee,
                                taker_fee,
                                timestamp: now,
                                maker_leverage: maker.leverage,
                                taker_leverage: leverage,
                            };

                            trades.push(trade);
                            amount -= trade_amount;
                            maker.remaining_amount -= trade_amount;

                            // Update last trade price
                            self.set_last_trade_price(trade_price);

                            // Remove fully filled maker order
                            if maker.remaining_amount <= Decimal::ZERO {
                                let maker_id = maker.id;
                                queue.pop_front();
                                self.order_index.remove(&maker_id);
                                self.order_count.fetch_sub(1, AtomicOrdering::Relaxed);
                                // See Buy-branch comment above for rationale.
                                tracing::debug!(
                                    "matcher: maker fully consumed — id={}, symbol={}, Bid-side queue (taker=Sell)",
                                    maker_id, self.symbol
                                );
                                crate::services::metrics::ORDERBOOK_MATCHER_MAKER_CONSUMED_TOTAL
                                    .with_label_values(&[&self.symbol])
                                    .inc();
                            }
                        }

                        if queue.is_empty() {
                            bids.remove(&price_level);
                        }
                    }
                }
            }
        }

        (trades, amount)
    }

    /// Get orderbook snapshot
    pub fn snapshot(&self, depth: usize) -> OrderbookSnapshot {
        let mut bids_vec: Vec<[String; 2]> = Vec::new();
        let mut asks_vec: Vec<[String; 2]> = Vec::new();

        // Get bids (highest first)
        {
            let bids = self.bids.read();
            for (price_level, orders) in bids.iter().rev().take(depth) {
                let total: Decimal = orders.iter().map(|o| o.remaining_amount).sum();
                bids_vec.push([price_level.to_decimal().to_string(), total.to_string()]);
            }
        }

        // Get asks (lowest first)
        {
            let asks = self.asks.read();
            for (price_level, orders) in asks.iter().take(depth) {
                let total: Decimal = orders.iter().map(|o| o.remaining_amount).sum();
                asks_vec.push([price_level.to_decimal().to_string(), total.to_string()]);
            }
        }

        OrderbookSnapshot {
            symbol: self.symbol.clone(),
            bids: bids_vec,
            asks: asks_vec,
            last_price: self.last_trade_price(),
            timestamp: chrono::Utc::now().timestamp_millis(),
        }
    }

    /// Get bid depth (total bids volume)
    pub fn bid_depth(&self) -> Decimal {
        let bids = self.bids.read();
        bids.values()
            .flat_map(|q| q.iter())
            .map(|o| o.remaining_amount)
            .sum()
    }

    /// Get ask depth (total asks volume)
    pub fn ask_depth(&self) -> Decimal {
        let asks = self.asks.read();
        asks.values()
            .flat_map(|q| q.iter())
            .map(|o| o.remaining_amount)
            .sum()
    }

    /// Check if an order exists
    pub fn has_order(&self, order_id: &Uuid) -> bool {
        self.order_index.contains_key(order_id)
    }

    /// Get order by ID
    pub fn get_order(&self, order_id: &Uuid) -> Option<OrderEntry> {
        let (side, price_level) = self.order_index.get(order_id)?.clone();

        match side {
            Side::Buy => {
                let bids = self.bids.read();
                bids.get(&price_level)?
                    .iter()
                    .find(|o| o.id == *order_id)
                    .cloned()
            }
            Side::Sell => {
                let asks = self.asks.read();
                asks.get(&price_level)?
                    .iter()
                    .find(|o| o.id == *order_id)
                    .cloned()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn create_test_order(id: Uuid, price: Decimal, amount: Decimal, side: Side) -> OrderEntry {
        OrderEntry {
            id,
            user_address: "0x1234".to_string(),
            price,
            original_amount: amount,
            remaining_amount: amount,
            side,
            time_in_force: TimeInForce::GTC,
            timestamp: chrono::Utc::now().timestamp_millis(),
            leverage: 10,
            // Tests don't assert on fee values — VIP3 maker (0%) by default.
            maker_fee_rate: Decimal::ZERO,
        }
    }

    #[test]
    fn test_add_and_cancel_order() {
        let book = Orderbook::new("BTCUSDT".to_string());
        let order_id = Uuid::new_v4();
        let order = create_test_order(order_id, dec!(100.0), dec!(1.0), Side::Buy);

        book.add_order(order);
        assert_eq!(book.order_count(), 1);
        assert!(book.has_order(&order_id));

        let cancelled = book.cancel_order(order_id);
        assert!(cancelled.is_some());
        assert_eq!(book.order_count(), 0);
        assert!(!book.has_order(&order_id));
    }

    /// Regression for the 2026-04-25 round-2 orphan: even when `order_index`
    /// gets out of sync with the queue (real users hit this at a 60% rate
    /// for extreme-price LIMIT orders), `cancel_order` must still find the
    /// order via the slow scan and remove it from the queue.
    #[test]
    fn test_cancel_order_recovers_orphan_via_slow_scan() {
        let book = Orderbook::new("BTCUSDT".to_string());
        let order_id = Uuid::new_v4();
        let order = create_test_order(order_id, dec!(38821.9), dec!(0.001), Side::Buy);

        book.add_order(order);
        assert_eq!(book.order_count(), 1);

        // Simulate the desync: queue still has the order, index does not.
        book.order_index.remove(&order_id);
        assert!(!book.has_order(&order_id), "index should report missing");

        // Slow path must find and remove it.
        let cancelled = book.cancel_order(order_id);
        assert!(cancelled.is_some(), "slow scan should find the orphan");
        let entry = cancelled.unwrap();
        assert_eq!(entry.id, order_id);
        assert_eq!(entry.price, dec!(38821.9));
        assert_eq!(book.order_count(), 0);
        assert!(!book.has_order(&order_id));

        // Bids map should be empty (last entry at this level was removed).
        assert!(book.bids.read().is_empty(), "empty price level cleaned up");
    }

    #[test]
    fn test_cancel_order_recovers_orphan_sell_side() {
        let book = Orderbook::new("BTCUSDT".to_string());
        let order_id = Uuid::new_v4();
        let order = create_test_order(order_id, dec!(99999.9), dec!(0.001), Side::Sell);

        book.add_order(order);
        book.order_index.remove(&order_id);

        let cancelled = book.cancel_order(order_id);
        assert!(cancelled.is_some());
        assert_eq!(cancelled.unwrap().id, order_id);
        assert_eq!(book.order_count(), 0);
    }

    #[test]
    fn test_cancel_order_returns_none_for_truly_missing_order() {
        let book = Orderbook::new("BTCUSDT".to_string());
        let result = book.cancel_order(Uuid::new_v4());
        assert!(result.is_none());
    }

    #[test]
    fn test_best_bid_ask() {
        let book = Orderbook::new("BTCUSDT".to_string());

        // Add bids
        book.add_order(create_test_order(Uuid::new_v4(), dec!(100.0), dec!(1.0), Side::Buy));
        book.add_order(create_test_order(Uuid::new_v4(), dec!(101.0), dec!(1.0), Side::Buy));

        // Add asks
        book.add_order(create_test_order(Uuid::new_v4(), dec!(102.0), dec!(1.0), Side::Sell));
        book.add_order(create_test_order(Uuid::new_v4(), dec!(103.0), dec!(1.0), Side::Sell));

        assert_eq!(book.best_bid(), Some(dec!(101.0)));
        assert_eq!(book.best_ask(), Some(dec!(102.0)));
        assert_eq!(book.spread(), Some(dec!(1.0)));
    }

    #[test]
    fn test_match_buy_order() {
        let book = Orderbook::new("BTCUSDT".to_string());

        // Add sell orders (asks)
        let ask1_id = Uuid::new_v4();
        book.add_order(create_test_order(ask1_id, dec!(100.0), dec!(1.0), Side::Sell));

        let ask2_id = Uuid::new_v4();
        book.add_order(create_test_order(ask2_id, dec!(101.0), dec!(2.0), Side::Sell));

        // Match a buy order — taker_fee_rate=0 (test asserts amounts, not fees)
        let taker_id = Uuid::new_v4();
        let (trades, remaining) = book.match_order(
            taker_id,
            "0x5678",
            Side::Buy,
            dec!(1.5),
            Some(dec!(101.0)),
            TimeInForce::GTC,
            Decimal::ZERO,
            10, // leverage
        );

        assert_eq!(trades.len(), 2);
        assert_eq!(remaining, dec!(0.0));

        // First trade should be at 100.0
        assert_eq!(trades[0].price, dec!(100.0));
        assert_eq!(trades[0].amount, dec!(1.0));

        // Second trade should be at 101.0
        assert_eq!(trades[1].price, dec!(101.0));
        assert_eq!(trades[1].amount, dec!(0.5));

        // Check remaining ask
        assert!(!book.has_order(&ask1_id)); // Fully filled
        assert!(book.has_order(&ask2_id));  // Partially filled
    }

    #[test]
    fn test_snapshot() {
        let book = Orderbook::new("BTCUSDT".to_string());

        book.add_order(create_test_order(Uuid::new_v4(), dec!(100.0), dec!(1.0), Side::Buy));
        book.add_order(create_test_order(Uuid::new_v4(), dec!(100.0), dec!(2.0), Side::Buy));
        book.add_order(create_test_order(Uuid::new_v4(), dec!(102.0), dec!(1.5), Side::Sell));

        let snapshot = book.snapshot(10);

        assert_eq!(snapshot.symbol, "BTCUSDT");
        assert_eq!(snapshot.bids.len(), 1);
        assert_eq!(snapshot.asks.len(), 1);
        assert_eq!(snapshot.bids[0][1], "3.0"); // Total bid at 100.0 (1.0 + 2.0)
        assert_eq!(snapshot.asks[0][1], "1.5");
    }

    /// Regression: submitting the same order_id twice must not double-push
    /// into the VecDeque. Without the idempotency guard in add_order, a
    /// taker consuming the full price level would match the maker TWICE,
    /// producing filled_amount = 2 × original_amount.
    #[test]
    fn test_add_order_idempotent_same_id() {
        let book = Orderbook::new("BTCUSDT".to_string());
        let maker_id = Uuid::new_v4();

        book.add_order(create_test_order(maker_id, dec!(100.0), dec!(1.0), Side::Sell));
        book.add_order(create_test_order(maker_id, dec!(100.0), dec!(1.0), Side::Sell));

        // Count and index should both reflect a single maker.
        assert_eq!(book.order_count(), 1, "duplicate add_order must not increase count");
        assert!(book.has_order(&maker_id));

        // A taker sweeping 2.0 at the price should only match 1.0 (the single maker),
        // not 2.0 (which would happen if the maker was pushed twice).
        let taker_id = Uuid::new_v4();
        let (trades, remaining) = book.match_order(
            taker_id,
            "0x5678",
            Side::Buy,
            dec!(2.0),
            Some(dec!(100.0)),
            TimeInForce::GTC,
            Decimal::ZERO,
            10,
        );
        let filled: Decimal = trades.iter().map(|t| t.amount).sum();
        assert_eq!(trades.len(), 1, "exactly one match expected, got {}", trades.len());
        assert_eq!(filled, dec!(1.0), "maker should only fill its stated amount once");
        assert_eq!(remaining, dec!(1.0));
    }
}
