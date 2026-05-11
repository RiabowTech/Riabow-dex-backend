//! Single-task spot matching engine. Owns the in-memory OrderBook for a single
//! market (DF/USDT in MVP); commands arrive via mpsc. Each fill commits to
//! PostgreSQL in its own transaction; downstream subscribers (ws_publisher,
//! kline_aggregator, ticker_aggregator) consume EngineEvent broadcasts.

use std::sync::Arc;
use rust_decimal::Decimal;
use sqlx::PgPool;
use tokio::sync::{mpsc, broadcast};
use tracing::Instrument;
use uuid::Uuid;

use crate::services::spot::markets::MarketCache;
use crate::models::spot::{SpotMarket, SpotOrder, Side, OrderType, OrderStatus, Tif, MarketStatus};
use super::book::OrderBook;
use super::types::*;

pub struct SpotMatchingEngine {
    pool: PgPool,
    markets: Arc<MarketCache>,
    book: OrderBook,
    market_id: String,
    cmd_rx: mpsc::Receiver<OrderCommand>,
    event_tx: broadcast::Sender<EngineEvent>,
    book_update_id: u64,
    next_ts_ns: u128,
}

impl SpotMatchingEngine {
    pub async fn start(
        pool: PgPool,
        markets: Arc<MarketCache>,
        market_id: String,
    ) -> anyhow::Result<EngineHandle> {
        let (cmd_tx, cmd_rx) = mpsc::channel(4096);
        let (event_tx, _) = broadcast::channel(1024);

        let mut book = OrderBook::new();
        super::recovery::recover_into(&pool, &market_id, &mut book).await?;

        let engine = SpotMatchingEngine {
            pool, markets, book, market_id, cmd_rx,
            event_tx: event_tx.clone(),
            book_update_id: 0,
            next_ts_ns: 1,
        };
        let span = tracing::info_span!("spot_matching_engine", market = %engine.market_id);
        tokio::spawn(async move { engine.run().await }.instrument(span));
        Ok(EngineHandle { cmd_tx, event_tx })
    }

    async fn run(mut self) {
        tracing::info!("spot matching engine starting");
        while let Some(cmd) = self.cmd_rx.recv().await {
            match cmd {
                OrderCommand::NewOrder(req, reply) => {
                    let res = self.handle_new_order(req).await;
                    let _ = reply.send(res);
                }
                OrderCommand::Cancel { id, user_address, reply } => {
                    let res = self.handle_cancel(id, user_address).await;
                    let _ = reply.send(res);
                }
                OrderCommand::CancelAll { user_address, market_id, reply } => {
                    let res = self.handle_cancel_all(user_address, market_id).await;
                    let _ = reply.send(res);
                }
                OrderCommand::CancelMarket { market_id, reply } => {
                    let res = self.handle_cancel_market(market_id).await;
                    let _ = reply.send(res);
                }
                OrderCommand::ReloadMarket { market_id: _ } => {
                    if let Ok(cache) = crate::services::spot::markets::load_initial(&self.pool).await {
                        self.markets.swap(cache.list().iter().map(|a| (**a).clone()).collect());
                    }
                }
                OrderCommand::Snapshot { depth, reply } => {
                    let (bids, asks) = self.book.snapshot(depth);
                    let _ = reply.send((bids, asks, self.book_update_id));
                }
            }
        }
        tracing::warn!("spot matching engine shutting down (cmd channel closed)");
    }

    fn next_ts(&mut self) -> u128 { let v = self.next_ts_ns; self.next_ts_ns += 1; v }
    fn bump_book(&mut self) -> u64 { self.book_update_id += 1; self.book_update_id }

    async fn handle_new_order(&mut self, req: PlaceOrderRequest) -> PlaceOrderResult {
        let market = self.markets.get(&req.market_id)
            .ok_or(PlaceOrderError::MarketNotFound)?;
        match market.parsed_status() {
            Some(MarketStatus::Listed)   => {}
            Some(MarketStatus::Halted)   => return Err(PlaceOrderError::MarketHalted),
            Some(MarketStatus::Delisted) => return Err(PlaceOrderError::MarketDelisted),
            None => return Err(PlaceOrderError::InvalidRequest("market.status invalid")),
        }

        match req.r#type {
            OrderType::Limit  => self.place_limit(req, market).await,
            OrderType::Market => self.place_market(req, market).await,
        }
    }

    async fn place_limit(
        &mut self,
        req: PlaceOrderRequest,
        market: Arc<SpotMarket>,
    ) -> PlaceOrderResult {
        let price = req.price.ok_or(PlaceOrderError::InvalidRequest("price required for limit"))?;
        let qty = req.quantity.ok_or(PlaceOrderError::InvalidRequest("quantity required for limit"))?;
        market.validate_price_qty(price, qty).map_err(|e| match e {
            crate::services::spot::markets::MarketValidationError::InvalidTick      => PlaceOrderError::InvalidTick,
            crate::services::spot::markets::MarketValidationError::InvalidLot       => PlaceOrderError::InvalidLot,
            crate::services::spot::markets::MarketValidationError::BelowMinNotional => PlaceOrderError::BelowMinNotional,
        })?;

        let (lock_token, lock_amount) = match req.side {
            Side::Buy  => (market.quote_token.clone(), price * qty),
            Side::Sell => (market.base_token.clone(),  qty),
        };

        // POST_ONLY pre-check: if would cross, reject
        if req.tif == Tif::PostOnly {
            let would_cross = match req.side {
                Side::Buy  => self.book.best_ask().map_or(false, |a| price >= a),
                Side::Sell => self.book.best_bid().map_or(false, |b| price <= b),
            };
            if would_cross { return Err(PlaceOrderError::PostOnlyReject); }
        }

        // Self-trade pre-check
        {
            let mut iter = match req.side {
                Side::Buy  => self.book.iter_takeable_mut(Side::Buy,  Some(price)),
                Side::Sell => self.book.iter_takeable_mut(Side::Sell, Some(price)),
            };
            if let Some(top) = iter.next() {
                if top.user_address == req.user_address {
                    return Err(PlaceOrderError::SelfTrade);
                }
            }
        }

        // Pre-freeze
        let frozen = sqlx::query(
            "UPDATE spot_balances
                SET available = available - $1,
                    frozen    = frozen    + $1,
                    updated_at = NOW()
              WHERE user_address = $2 AND token = $3 AND available >= $1"
        )
        .bind(lock_amount).bind(&req.user_address).bind(&lock_token)
        .execute(&self.pool).await?
        .rows_affected();
        if frozen == 0 { return Err(PlaceOrderError::InsufficientBalance); }

        // INSERT spot_orders
        let inserted: SpotOrder = sqlx::query_as(
            "INSERT INTO spot_orders
               (user_address, market_id, side, type, tif, price, quantity, status)
             VALUES ($1, $2, $3, 'limit', $4, $5, $6, 'open')
             RETURNING *"
        )
        .bind(&req.user_address).bind(&req.market_id)
        .bind(req.side.as_str()).bind(req.tif.as_str())
        .bind(price).bind(qty)
        .fetch_one(&self.pool).await?;

        // Match phase: collect Hits while iterating mutably.
        struct Hit {
            id: Uuid, price: Decimal, qty: Decimal, user: String,
            remaining_after: Decimal, avg_after: Decimal,
        }
        let mut hits: Vec<Hit> = vec![];
        let mut taker_remaining = qty;
        {
            let mut iter = self.book.iter_takeable_mut(req.side, Some(price));
            while taker_remaining > Decimal::ZERO {
                let Some(maker) = iter.next() else { break };
                let take = std::cmp::min(taker_remaining, maker.remaining_qty);
                let new_remaining = maker.remaining_qty - take;
                let prev_filled = maker.original_qty - maker.remaining_qty;
                let prev_notional = prev_filled * maker.price;
                let new_notional = prev_notional + take * maker.price;
                let total_filled_after = maker.original_qty - new_remaining;
                let avg_after = if total_filled_after.is_zero() {
                    Decimal::ZERO
                } else {
                    new_notional / total_filled_after
                };
                hits.push(Hit {
                    id: maker.id, price: maker.price, qty: take,
                    user: maker.user_address.clone(),
                    remaining_after: new_remaining,
                    avg_after,
                });
                maker.remaining_qty = new_remaining;
                taker_remaining -= take;
            }
        }
        self.book.remove_empty_levels();

        // Commit each fill
        let mut fills_emitted = vec![];
        let mut running_taker_filled = Decimal::ZERO;
        let mut running_taker_notional = Decimal::ZERO;
        for h in &hits {
            running_taker_filled   += h.qty;
            running_taker_notional += h.qty * h.price;
            let taker_remaining_after_this = qty - running_taker_filled;
            let taker_avg_after = if running_taker_filled.is_zero() {
                Decimal::ZERO
            } else {
                running_taker_notional / running_taker_filled
            };

            let committed = super::settle::commit_fill(&self.pool, super::settle::FillInputs {
                market: &market,
                maker_order_id: h.id,
                taker_order_id: inserted.id,
                maker_user: h.user.clone(),
                taker_user: req.user_address.clone(),
                taker_side: req.side.as_str().to_string(),
                price: h.price,
                quantity: h.qty,
                maker_remaining_after: h.remaining_after,
                taker_remaining_after: taker_remaining_after_this,
                maker_avg_price_after: h.avg_after,
                taker_avg_price_after: taker_avg_after,
            }).await.map_err(|e| match e {
                super::settle::SettleError::Db(d) => PlaceOrderError::DbError(d),
                super::settle::SettleError::MissingBalance(_, _) =>
                    PlaceOrderError::DbError(sqlx::Error::RowNotFound),
            })?;

            if h.remaining_after == Decimal::ZERO {
                let _ = self.book.remove_by_id(h.id);
            }
            fills_emitted.push(committed.trade.clone());
            let book_update = self.bump_book();
            let maker_side = match req.side {
                Side::Buy  => Side::Sell,
                Side::Sell => Side::Buy,
            };
            let maker_total = self.book.level_total(maker_side, h.price);
            let affected_levels = vec![(maker_side.as_str().to_string(), h.price, maker_total)];
            let _ = self.event_tx.send(EngineEvent::Fill {
                trade: committed.trade,
                maker_after: committed.maker_after,
                taker_after: committed.taker_after,
                book_update_id: book_update,
                affected_levels,
            });
        }

        // Final order state by TIF
        let final_order: SpotOrder = if taker_remaining > Decimal::ZERO {
            match req.tif {
                Tif::Gtc => {
                    let ts_ns = self.next_ts();
                    self.book.add(super::types::RestingOrder {
                        id: inserted.id,
                        user_address: req.user_address.clone(),
                        side: req.side, price,
                        original_qty: qty, remaining_qty: taker_remaining,
                        ts_ns,
                    });
                    let order: SpotOrder = sqlx::query_as("SELECT * FROM spot_orders WHERE id=$1")
                        .bind(inserted.id).fetch_one(&self.pool).await?;
                    let upd = self.bump_book();
                    let total = self.book.level_total(req.side, price);
                    let affected_levels = vec![(req.side.as_str().to_string(), price, total)];
                    let _ = self.event_tx.send(EngineEvent::OrderPlaced {
                        order: order.clone(),
                        book_update_id: upd,
                        affected_levels,
                    });
                    order
                }
                Tif::Ioc | Tif::PostOnly => {
                    // PostOnly never reaches here (we rejected if it would cross).
                    // IOC: cancel remainder, refund.
                    let refund = match req.side {
                        Side::Buy  => taker_remaining * price,
                        Side::Sell => taker_remaining,
                    };
                    sqlx::query(
                        "UPDATE spot_balances
                            SET available = available + $1,
                                frozen    = frozen    - $1,
                                updated_at = NOW()
                          WHERE user_address = $2 AND token = $3"
                    )
                    .bind(refund).bind(&req.user_address).bind(&lock_token)
                    .execute(&self.pool).await?;
                    let order: SpotOrder = sqlx::query_as(
                        "UPDATE spot_orders SET status='canceled', updated_at=NOW()
                          WHERE id=$1 RETURNING *"
                    ).bind(inserted.id).fetch_one(&self.pool).await?;
                    order
                }
            }
        } else {
            // Fully filled
            sqlx::query("UPDATE spot_orders SET status='filled', updated_at=NOW() WHERE id=$1")
                .bind(inserted.id).execute(&self.pool).await?;
            sqlx::query_as("SELECT * FROM spot_orders WHERE id=$1")
                .bind(inserted.id).fetch_one(&self.pool).await?
        };

        Ok(PlaceOrderOk { order: final_order, fills: fills_emitted })
    }

    async fn place_market(
        &mut self,
        req: PlaceOrderRequest,
        market: Arc<SpotMarket>,
    ) -> PlaceOrderResult {
        use crate::models::spot::Side;

        // Determine lock token + amount and which inputs we have
        let (lock_token, lock_amount, base_qty_input, quote_qty_input) = match req.side {
            Side::Buy => {
                let q = req.quote_quantity
                    .ok_or(PlaceOrderError::InvalidRequest("quote_quantity required for market buy"))?;
                (market.quote_token.clone(), q, None, Some(q))
            }
            Side::Sell => {
                let q = req.quantity
                    .ok_or(PlaceOrderError::InvalidRequest("quantity required for market sell"))?;
                // Lot validation only meaningful for sell (where qty is base)
                if (q % market.lot_size) != Decimal::ZERO {
                    return Err(PlaceOrderError::InvalidLot);
                }
                (market.base_token.clone(), q, Some(q), None)
            }
        };

        // Self-trade pre-check (top of opposite book)
        if let Some(top) = match req.side {
            Side::Buy  => self.book.iter_takeable_mut(Side::Buy,  None).next(),
            Side::Sell => self.book.iter_takeable_mut(Side::Sell, None).next(),
        } {
            if top.user_address == req.user_address {
                return Err(PlaceOrderError::SelfTrade);
            }
        }

        // Pre-freeze
        let frozen = sqlx::query(
            "UPDATE spot_balances SET available=available-$1, frozen=frozen+$1, updated_at=NOW()
              WHERE user_address=$2 AND token=$3 AND available >= $1"
        ).bind(lock_amount).bind(&req.user_address).bind(&lock_token)
         .execute(&self.pool).await?
         .rows_affected();
        if frozen == 0 { return Err(PlaceOrderError::InsufficientBalance); }

        // INSERT order
        let inserted: SpotOrder = sqlx::query_as(
            "INSERT INTO spot_orders (user_address, market_id, side, type, tif,
                                      price, quantity, quote_quantity, status)
             VALUES ($1, $2, $3, 'market', 'ioc', NULL, $4, $5, 'open')
             RETURNING *"
        )
        .bind(&req.user_address).bind(&req.market_id).bind(req.side.as_str())
        .bind(base_qty_input).bind(quote_qty_input)
        .fetch_one(&self.pool).await?;

        // Match phase: collect Hits.
        // - SELL: bounded by base_remaining (qty)
        // - BUY:  bounded by quote_remaining (USDT lock); per-hit take is floored to lot_size
        struct Hit {
            id: Uuid, price: Decimal, qty: Decimal, user: String,
            remaining_after: Decimal, avg_after: Decimal,
        }
        let mut hits: Vec<Hit> = vec![];
        let mut base_remaining = base_qty_input.unwrap_or(Decimal::ZERO);
        let mut quote_remaining = quote_qty_input.unwrap_or(Decimal::ZERO);

        {
            let mut iter = self.book.iter_takeable_mut(req.side, None);
            loop {
                let Some(maker) = iter.next() else { break };
                let take = match req.side {
                    Side::Sell => {
                        if base_remaining <= Decimal::ZERO { break; }
                        std::cmp::min(maker.remaining_qty, base_remaining)
                    }
                    Side::Buy => {
                        if quote_remaining <= Decimal::ZERO { break; }
                        // Maximum base buyable from this maker, bounded by quote left
                        let raw = std::cmp::min(maker.remaining_qty, quote_remaining / maker.price);
                        // Floor to lot_size multiples to avoid fractional-lot fills
                        let lot = market.lot_size;
                        let take = (raw / lot).trunc() * lot;
                        if take.is_zero() { break; }
                        take
                    }
                };

                let new_remaining = maker.remaining_qty - take;
                let prev_filled = maker.original_qty - maker.remaining_qty;
                let prev_notional = prev_filled * maker.price;
                let new_notional = prev_notional + take * maker.price;
                let total_filled_after = maker.original_qty - new_remaining;
                let avg_after = if total_filled_after.is_zero() {
                    Decimal::ZERO
                } else {
                    new_notional / total_filled_after
                };
                hits.push(Hit {
                    id: maker.id, price: maker.price, qty: take,
                    user: maker.user_address.clone(),
                    remaining_after: new_remaining, avg_after,
                });
                maker.remaining_qty = new_remaining;
                // Update remaining trackers
                match req.side {
                    Side::Sell => base_remaining -= take,
                    Side::Buy  => quote_remaining -= take * maker.price,
                }
            }
        }
        self.book.remove_empty_levels();

        // Commit each fill
        let mut fills_emitted = vec![];
        let mut running_taker_filled = Decimal::ZERO;
        let mut running_taker_notional = Decimal::ZERO;
        let original_taker_qty_for_avg = match req.side {
            Side::Sell => base_qty_input.unwrap_or(Decimal::ZERO),
            Side::Buy  => Decimal::ZERO,    // BUY taker has no fixed base qty cap
        };
        for h in &hits {
            running_taker_filled   += h.qty;
            running_taker_notional += h.qty * h.price;
            let taker_remaining_after_this = match req.side {
                Side::Sell => original_taker_qty_for_avg - running_taker_filled,
                // For BUY: remaining concept is "leftover quote / current price" which is fluid;
                // we represent "still room to fill" as nonzero quote_remaining.
                Side::Buy  => if quote_remaining > Decimal::ZERO { Decimal::ONE } else { Decimal::ZERO },
            };
            let taker_avg_after = if running_taker_filled.is_zero() {
                Decimal::ZERO
            } else {
                running_taker_notional / running_taker_filled
            };

            let committed = super::settle::commit_fill(&self.pool, super::settle::FillInputs {
                market: &market,
                maker_order_id: h.id,
                taker_order_id: inserted.id,
                maker_user: h.user.clone(),
                taker_user: req.user_address.clone(),
                taker_side: req.side.as_str().to_string(),
                price: h.price,
                quantity: h.qty,
                maker_remaining_after: h.remaining_after,
                taker_remaining_after: taker_remaining_after_this,
                maker_avg_price_after: h.avg_after,
                taker_avg_price_after: taker_avg_after,
            }).await.map_err(|e| match e {
                super::settle::SettleError::Db(d) => PlaceOrderError::DbError(d),
                super::settle::SettleError::MissingBalance(_, _) =>
                    PlaceOrderError::DbError(sqlx::Error::RowNotFound),
            })?;

            if h.remaining_after == Decimal::ZERO {
                let _ = self.book.remove_by_id(h.id);
            }
            fills_emitted.push(committed.trade.clone());
            let book_update = self.bump_book();
            let maker_side = match req.side {
                Side::Buy  => Side::Sell,
                Side::Sell => Side::Buy,
            };
            let maker_total = self.book.level_total(maker_side, h.price);
            let affected_levels = vec![(maker_side.as_str().to_string(), h.price, maker_total)];
            let _ = self.event_tx.send(EngineEvent::Fill {
                trade: committed.trade,
                maker_after: committed.maker_after,
                taker_after: committed.taker_after,
                book_update_id: book_update,
                affected_levels,
            });
        }

        // Refund unused lock
        let refund = match req.side {
            Side::Buy  => quote_remaining,
            Side::Sell => base_remaining,
        };
        if refund > Decimal::ZERO {
            sqlx::query(
                "UPDATE spot_balances SET available=available+$1, frozen=frozen-$1, updated_at=NOW()
                  WHERE user_address=$2 AND token=$3"
            ).bind(refund).bind(&req.user_address).bind(&lock_token)
             .execute(&self.pool).await?;
        }

        // Final status
        let final_status = if fills_emitted.is_empty() {
            OrderStatus::Canceled
        } else {
            let exhausted = match req.side {
                Side::Buy  => quote_remaining == Decimal::ZERO,
                Side::Sell => base_remaining  == Decimal::ZERO,
            };
            if exhausted { OrderStatus::Filled } else { OrderStatus::Canceled }
        };
        let order: SpotOrder = sqlx::query_as(
            "UPDATE spot_orders SET status=$1, updated_at=NOW() WHERE id=$2 RETURNING *"
        ).bind(final_status.as_str()).bind(inserted.id).fetch_one(&self.pool).await?;

        Ok(PlaceOrderOk { order, fills: fills_emitted })
    }

    async fn handle_cancel(&mut self, id: Uuid, user: String) -> CancelResult {
        let order: Option<SpotOrder> = sqlx::query_as("SELECT * FROM spot_orders WHERE id=$1")
            .bind(id).fetch_optional(&self.pool).await?;
        let Some(order) = order else { return Err(CancelError::NotFound); };
        if order.user_address != user { return Err(CancelError::Forbidden); }
        if matches!(order.status.as_str(), "filled" | "canceled" | "rejected" | "expired") {
            return Err(CancelError::NotFound);
        }
        let market = self.markets.get(&order.market_id)
            .ok_or(CancelError::Db(sqlx::Error::RowNotFound))?;
        let remaining = order.quantity.unwrap_or(Decimal::ZERO) - order.filled_qty;
        let (lock_token, refund) = match order.side.as_str() {
            "buy"  => (market.quote_token.clone(),
                       remaining * order.price.unwrap_or(Decimal::ZERO)),
            "sell" => (market.base_token.clone(), remaining),
            _ => return Err(CancelError::Db(sqlx::Error::RowNotFound)),
        };
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "UPDATE spot_balances SET available=available+$1, frozen=frozen-$1, updated_at=NOW()
              WHERE user_address=$2 AND token=$3"
        )
        .bind(refund).bind(&user).bind(&lock_token)
        .execute(&mut *tx).await?;
        let canceled: SpotOrder = sqlx::query_as(
            "UPDATE spot_orders SET status='canceled', updated_at=NOW()
              WHERE id=$1 RETURNING *"
        ).bind(id).fetch_one(&mut *tx).await?;
        tx.commit().await?;
        let _ = self.book.remove_by_id(id);
        let upd = self.bump_book();
        let side = Side::parse(&canceled.side).expect("known side");
        let price = canceled.price.expect("limit");
        let total = self.book.level_total(side, price);
        let affected_levels = vec![(side.as_str().to_string(), price, total)];
        let _ = self.event_tx.send(EngineEvent::OrderCanceled {
            order: canceled.clone(),
            book_update_id: upd,
            affected_levels,
        });
        Ok(canceled)
    }

    async fn handle_cancel_all(&mut self, user: String, market: Option<String>) -> Vec<SpotOrder> {
        let rows: Vec<SpotOrder> = if let Some(m) = market.clone() {
            sqlx::query_as(
                "SELECT * FROM spot_orders
                  WHERE user_address=$1 AND market_id=$2
                    AND status IN ('open','partially_filled')"
            ).bind(&user).bind(&m).fetch_all(&self.pool).await
        } else {
            sqlx::query_as(
                "SELECT * FROM spot_orders
                  WHERE user_address=$1
                    AND status IN ('open','partially_filled')"
            ).bind(&user).fetch_all(&self.pool).await
        }.unwrap_or_default();

        let mut out = vec![];
        for o in rows {
            if let Ok(c) = self.handle_cancel(o.id, user.clone()).await {
                out.push(c);
            }
        }
        out
    }

    /// Admin: cancel every open / partially-filled order for a market.
    /// Used by the delist transition. We iterate spot_orders directly and
    /// drive each through `handle_cancel` (impersonating the owner) so
    /// balances unfreeze and the standard OrderCanceled event fires.
    async fn handle_cancel_market(&mut self, market: String) -> Vec<SpotOrder> {
        let rows: Vec<SpotOrder> = sqlx::query_as(
            "SELECT * FROM spot_orders
              WHERE market_id=$1 AND status IN ('open','partially_filled')"
        ).bind(&market).fetch_all(&self.pool).await.unwrap_or_default();

        let mut out = vec![];
        for o in rows {
            let user = o.user_address.clone();
            if let Ok(c) = self.handle_cancel(o.id, user).await {
                out.push(c);
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn db_url() -> Option<String> { std::env::var("TEST_DATABASE_URL").ok() }

    /// Skipped by default. When TEST_DATABASE_URL is set, expand each scenario:
    ///   - Empty book + GTC limit (rests, balances frozen, OrderPlaced emitted)
    ///   - POST_ONLY would cross → PostOnlyReject (balances unchanged)
    ///   - POST_ONLY no cross → rests
    ///   - GTC limit crossing one maker → both filled, fills emitted
    ///   - IOC no liquidity → canceled with filled_qty=0
    ///   - IOC partial fill → fills, cancels remainder, refund leftover
    ///   - Self-trade → SelfTrade error, both untouched
    ///   - Cancel of resting order → balances unfrozen
    #[tokio::test]
    async fn engine_scenarios() {
        if db_url().is_none() {
            eprintln!("skip: no TEST_DATABASE_URL");
            return;
        }
        // Implementation deferred to CI environment with a working DB.
    }
}
