use rust_decimal::Decimal;
use std::collections::{BTreeMap, HashMap, VecDeque};
use uuid::Uuid;
use crate::models::spot::Side;
use super::types::RestingOrder;

#[derive(Debug, Default)]
pub struct OrderBook {
    bids: BTreeMap<Decimal, VecDeque<RestingOrder>>,
    asks: BTreeMap<Decimal, VecDeque<RestingOrder>>,
    by_id: HashMap<Uuid, (Side, Decimal)>,
}

impl OrderBook {
    pub fn new() -> Self { Self::default() }

    pub fn add(&mut self, order: RestingOrder) {
        let (side, price, id) = (order.side, order.price, order.id);
        let level = match side {
            Side::Buy  => self.bids.entry(price).or_default(),
            Side::Sell => self.asks.entry(price).or_default(),
        };
        level.push_back(order);
        self.by_id.insert(id, (side, price));
    }

    pub fn cancel(&mut self, id: Uuid) -> Option<RestingOrder> {
        let (side, price) = self.by_id.remove(&id)?;
        let level = match side {
            Side::Buy  => self.bids.get_mut(&price),
            Side::Sell => self.asks.get_mut(&price),
        }?;
        let idx = level.iter().position(|o| o.id == id)?;
        let order = level.remove(idx)?;
        if level.is_empty() {
            match side {
                Side::Buy  => { self.bids.remove(&price); }
                Side::Sell => { self.asks.remove(&price); }
            }
        }
        Some(order)
    }

    /// Alias for `cancel` — used at engine call sites where the semantic is
    /// "this maker fully filled, drop it" rather than user-cancel.
    pub fn remove_by_id(&mut self, id: Uuid) -> Option<RestingOrder> { self.cancel(id) }

    pub fn best_bid(&self) -> Option<Decimal> { self.bids.keys().next_back().copied() }
    pub fn best_ask(&self) -> Option<Decimal> { self.asks.keys().next().copied() }

    pub fn iter_takeable_mut<'a>(
        &'a mut self,
        taker_side: Side,
        limit_price: Option<Decimal>,
    ) -> Box<dyn Iterator<Item = &'a mut RestingOrder> + 'a> {
        match taker_side {
            // Buy taker walks asks ascending; stop when ask price > limit
            Side::Buy => Box::new(
                self.asks.iter_mut()
                    .take_while(move |(price, _)| match limit_price {
                        Some(lim) => **price <= lim,
                        None      => true,
                    })
                    .flat_map(|(_, orders)| orders.iter_mut()),
            ),
            // Sell taker walks bids descending; stop when bid price < limit
            Side::Sell => Box::new(
                self.bids.iter_mut().rev()
                    .take_while(move |(price, _)| match limit_price {
                        Some(lim) => **price >= lim,
                        None      => true,
                    })
                    .flat_map(|(_, orders)| orders.iter_mut()),
            ),
        }
    }

    /// Drop levels that have empty VecDeques. Call after a match round.
    pub fn remove_empty_levels(&mut self) {
        self.bids.retain(|_, v| !v.is_empty());
        self.asks.retain(|_, v| !v.is_empty());
    }

    /// Sum of `remaining_qty` across every order resting at `price` on the
    /// given side. Returns `Decimal::ZERO` when the level is empty / absent
    /// (e.g. last order at the level was just removed) — this also signals
    /// "remove this level" in a depth diff.
    pub fn level_total(&self, side: Side, price: Decimal) -> Decimal {
        let map = match side { Side::Buy => &self.bids, Side::Sell => &self.asks };
        map.get(&price)
            .map(|orders| orders.iter().map(|o| o.remaining_qty).sum())
            .unwrap_or(Decimal::ZERO)
    }

    pub fn snapshot(&self, depth: usize) -> (Vec<(Decimal, Decimal)>, Vec<(Decimal, Decimal)>) {
        let bids: Vec<_> = self.bids.iter().rev().take(depth)
            .map(|(p, orders)| (*p, orders.iter().map(|o| o.remaining_qty).sum()))
            .collect();
        let asks: Vec<_> = self.asks.iter().take(depth)
            .map(|(p, orders)| (*p, orders.iter().map(|o| o.remaining_qty).sum()))
            .collect();
        (bids, asks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;
    use rust_decimal_macros::dec;

    fn order(side: Side, price: Decimal, qty: Decimal, ts: u128) -> RestingOrder {
        RestingOrder { id: Uuid::new_v4(), user_address: "u".into(),
            side, price, original_qty: qty, remaining_qty: qty, ts_ns: ts }
    }

    #[test]
    fn empty_book() {
        let b = OrderBook::new();
        assert_eq!(b.best_bid(), None);
        assert_eq!(b.best_ask(), None);
    }

    #[test]
    fn best_bid_is_highest_price() {
        let mut b = OrderBook::new();
        b.add(order(Side::Buy, dec!(0.50), dec!(10), 1));
        b.add(order(Side::Buy, dec!(0.49), dec!(20), 2));
        b.add(order(Side::Buy, dec!(0.51), dec!(30), 3));
        assert_eq!(b.best_bid(), Some(dec!(0.51)));
    }

    #[test]
    fn best_ask_is_lowest_price() {
        let mut b = OrderBook::new();
        b.add(order(Side::Sell, dec!(0.52), dec!(10), 1));
        b.add(order(Side::Sell, dec!(0.51), dec!(20), 2));
        b.add(order(Side::Sell, dec!(0.53), dec!(30), 3));
        assert_eq!(b.best_ask(), Some(dec!(0.51)));
    }

    #[test]
    fn cancel_returns_order_and_removes_it() {
        let mut b = OrderBook::new();
        let o = order(Side::Buy, dec!(0.5), dec!(10), 1);
        let id = o.id;
        b.add(o);
        let cancelled = b.cancel(id).expect("present");
        assert_eq!(cancelled.id, id);
        assert!(b.cancel(id).is_none());
        assert_eq!(b.best_bid(), None);
    }

    #[test]
    fn iter_takeable_buy_yields_asks_ascending() {
        let mut b = OrderBook::new();
        b.add(order(Side::Sell, dec!(0.52), dec!(10), 1));
        b.add(order(Side::Sell, dec!(0.51), dec!(20), 2));
        b.add(order(Side::Sell, dec!(0.51), dec!(30), 3));
        let prices: Vec<_> = b.iter_takeable_mut(Side::Buy, Some(dec!(0.55)))
            .map(|o| (o.price, o.ts_ns)).collect();
        assert_eq!(prices, vec![(dec!(0.51), 2), (dec!(0.51), 3), (dec!(0.52), 1)]);
    }

    #[test]
    fn iter_takeable_respects_limit_price() {
        let mut b = OrderBook::new();
        b.add(order(Side::Sell, dec!(0.51), dec!(10), 1));
        b.add(order(Side::Sell, dec!(0.55), dec!(20), 2));
        let prices: Vec<_> = b.iter_takeable_mut(Side::Buy, Some(dec!(0.52)))
            .map(|o| o.price).collect();
        assert_eq!(prices, vec![dec!(0.51)]);
    }

    #[test]
    fn snapshot_aggregates_per_level() {
        let mut b = OrderBook::new();
        b.add(order(Side::Buy, dec!(0.50), dec!(10), 1));
        b.add(order(Side::Buy, dec!(0.50), dec!(15), 2));
        b.add(order(Side::Buy, dec!(0.49), dec!(7), 3));
        b.add(order(Side::Sell, dec!(0.51), dec!(20), 4));
        let (bids, asks) = b.snapshot(10);
        assert_eq!(bids, vec![(dec!(0.50), dec!(25)), (dec!(0.49), dec!(7))]);
        assert_eq!(asks, vec![(dec!(0.51), dec!(20))]);
    }
}
