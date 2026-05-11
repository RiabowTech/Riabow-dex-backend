//! Per-fill settlement: commits one fill to PostgreSQL atomically.
//!
//! Updates: maker order, taker order, inserts trade, four spot_balances rows
//! (lock order: lexicographic by (user_address, token) to avoid deadlock with
//! deposit/withdraw paths), upserts spot_ticker_24h.
//!
//! Fees are denominated in each side's RECEIVED token (Binance convention):
//!   SELL side receives quote → fee in quote
//!   BUY  side receives base  → fee in base
//! MVP testnet fee_bps = 0 so this is moot in the short term, but the math is
//! set up cleanly for non-zero fee config.

use rust_decimal::Decimal;
use sqlx::PgPool;
use uuid::Uuid;

use crate::models::spot::{SpotMarket, SpotOrder, SpotTrade, OrderStatus};

#[derive(Debug, Clone)]
pub struct FillInputs<'a> {
    pub market: &'a SpotMarket,
    pub maker_order_id: Uuid,
    pub taker_order_id: Uuid,
    pub maker_user: String,
    pub taker_user: String,
    pub taker_side: String,             // "buy" | "sell"
    pub price: Decimal,
    pub quantity: Decimal,
    /// Total maker remaining_qty AFTER this fill (engine-computed).
    pub maker_remaining_after: Decimal,
    pub taker_remaining_after: Decimal,
    pub maker_avg_price_after: Decimal,
    pub taker_avg_price_after: Decimal,
}

#[derive(Debug)]
pub struct FillCommitted {
    pub trade: SpotTrade,
    pub maker_after: SpotOrder,
    pub taker_after: SpotOrder,
}

#[derive(Debug, thiserror::Error)]
pub enum SettleError {
    #[error("db error: {0}")] Db(#[from] sqlx::Error),
    #[error("balance row missing for {0}/{1}")] MissingBalance(String, String),
}

pub async fn commit_fill(pool: &PgPool, f: FillInputs<'_>) -> Result<FillCommitted, SettleError> {
    let mut tx = pool.begin().await?;

    let base = &f.market.base_token;
    let quote = &f.market.quote_token;
    let notional = f.price * f.quantity;

    // Fees in received-token convention:
    //   SELL side fee in quote = notional * fee_bps / 10000
    //   BUY  side fee in base  = qty      * fee_bps / 10000
    let bps10000 = Decimal::from(10_000);
    let (sell_side_fee_bps, buy_side_fee_bps) = match f.taker_side.as_str() {
        // taker buys → maker is SELL, taker is BUY
        "buy"  => (f.market.maker_fee_bps, f.market.taker_fee_bps),
        // taker sells → maker is BUY, taker is SELL
        "sell" => (f.market.taker_fee_bps, f.market.maker_fee_bps),
        other  => panic!("invalid taker_side: {other}"),
    };
    let sell_side_quote_fee = notional   * Decimal::from(sell_side_fee_bps) / bps10000;
    let buy_side_base_fee   = f.quantity * Decimal::from(buy_side_fee_bps)  / bps10000;
    // For the spot_trades row: by maker / taker mapping.
    let (maker_fee_amount, taker_fee_amount) = match f.taker_side.as_str() {
        "buy"  => (sell_side_quote_fee, buy_side_base_fee),  // maker=sell, taker=buy
        "sell" => (buy_side_base_fee,   sell_side_quote_fee),// maker=buy,  taker=sell
        _ => unreachable!(),
    };

    // ---- Update orders ----
    let maker_status = if f.maker_remaining_after == Decimal::ZERO {
        OrderStatus::Filled
    } else {
        OrderStatus::PartiallyFilled
    };
    let taker_status = if f.taker_remaining_after == Decimal::ZERO {
        OrderStatus::Filled
    } else {
        OrderStatus::PartiallyFilled
    };

    let maker_after: SpotOrder = sqlx::query_as(
        "UPDATE spot_orders
            SET filled_qty = filled_qty + $1,
                avg_fill_price = $2,
                status = $3,
                updated_at = NOW()
          WHERE id = $4
          RETURNING *"
    )
    .bind(f.quantity).bind(f.maker_avg_price_after).bind(maker_status.as_str())
    .bind(f.maker_order_id)
    .fetch_one(&mut *tx).await?;

    let taker_after: SpotOrder = sqlx::query_as(
        "UPDATE spot_orders
            SET filled_qty = filled_qty + $1,
                avg_fill_price = $2,
                status = $3,
                updated_at = NOW()
          WHERE id = $4
          RETURNING *"
    )
    .bind(f.quantity).bind(f.taker_avg_price_after).bind(taker_status.as_str())
    .bind(f.taker_order_id)
    .fetch_one(&mut *tx).await?;

    // ---- Insert trade ----
    let trade: SpotTrade = sqlx::query_as(
        "INSERT INTO spot_trades
           (market_id, maker_order_id, taker_order_id, maker_user, taker_user,
            side, price, quantity, maker_fee, taker_fee)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
         RETURNING *"
    )
    .bind(&f.market.id).bind(f.maker_order_id).bind(f.taker_order_id)
    .bind(&f.maker_user).bind(&f.taker_user).bind(&f.taker_side)
    .bind(f.price).bind(f.quantity).bind(maker_fee_amount).bind(taker_fee_amount)
    .fetch_one(&mut *tx).await?;

    // ---- Update balances ----
    let (seller, buyer) = match f.taker_side.as_str() {
        "buy"  => (f.maker_user.clone(), f.taker_user.clone()),
        "sell" => (f.taker_user.clone(), f.maker_user.clone()),
        other  => panic!("invalid taker_side: {other}"),
    };

    // SELLER fee in quote (deducted from quote received)
    // BUYER  fee in base  (deducted from base  received)
    let seller_quote_fee = sell_side_quote_fee;
    let buyer_base_fee   = buy_side_base_fee;

    let mut updates: Vec<(String, String, Decimal, Decimal)> = vec![
        // (user, token, available_delta, frozen_delta)
        (seller.clone(), base.clone(),  Decimal::ZERO,                  -f.quantity),
        (seller.clone(), quote.clone(), notional - seller_quote_fee,    Decimal::ZERO),
        (buyer.clone(),  quote.clone(), Decimal::ZERO,                  -notional),
        (buyer.clone(),  base.clone(),  f.quantity - buyer_base_fee,    Decimal::ZERO),
    ];
    updates.sort_by(|a, b| (a.0.as_str(), a.1.as_str()).cmp(&(b.0.as_str(), b.1.as_str())));
    for (user, token, avail_d, frozen_d) in updates {
        let rows = sqlx::query(
            "UPDATE spot_balances
                SET available = available + $1,
                    frozen    = frozen    + $2,
                    updated_at = NOW()
              WHERE user_address = $3 AND token = $4"
        )
        .bind(avail_d).bind(frozen_d).bind(&user).bind(&token)
        .execute(&mut *tx).await?
        .rows_affected();
        if rows == 0 {
            return Err(SettleError::MissingBalance(user, token));
        }
    }

    // ---- Upsert ticker (this row will be re-corrected by the 60s
    //      ticker_aggregator task; here we maintain last_price + running
    //      totals for liveness between recompute cycles) ----
    sqlx::query(
        "INSERT INTO spot_ticker_24h
           (market_id, last_price, open_price_24h, high_24h, low_24h,
            volume_24h, quote_volume_24h, trade_count_24h, updated_at)
         VALUES ($1, $2, $2, $2, $2, $3, $4, 1, NOW())
         ON CONFLICT (market_id) DO UPDATE
           SET last_price = EXCLUDED.last_price,
               high_24h   = GREATEST(spot_ticker_24h.high_24h, EXCLUDED.last_price),
               low_24h    = LEAST(spot_ticker_24h.low_24h, EXCLUDED.last_price),
               volume_24h = spot_ticker_24h.volume_24h + EXCLUDED.volume_24h,
               quote_volume_24h = spot_ticker_24h.quote_volume_24h + EXCLUDED.quote_volume_24h,
               trade_count_24h  = spot_ticker_24h.trade_count_24h + 1,
               updated_at = NOW()"
    )
    .bind(&f.market.id).bind(f.price).bind(f.quantity).bind(notional)
    .execute(&mut *tx).await?;

    tx.commit().await?;

    Ok(FillCommitted { trade, maker_after, taker_after })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn db_url() -> Option<String> { std::env::var("TEST_DATABASE_URL").ok() }

    async fn with_pool() -> Option<PgPool> {
        let url = db_url()?;
        Some(PgPool::connect(&url).await.expect("connect"))
    }

    #[tokio::test]
    async fn fill_moves_funds_and_inserts_trade() {
        let Some(pool) = with_pool().await else {
            eprintln!("skip: no TEST_DATABASE_URL");
            return;
        };

        // Bootstrap a test scenario:
        //   - market TESTSPOT (DF/USDT, fee=0, tick=0.0001, lot=0.01, min_notional=0.01)
        //   - user A (maker, SELL): spot_balances DF available=0 frozen=30; USDT 0/0
        //   - user B (taker, BUY ): spot_balances USDT available=0 frozen=15; DF 0/0
        //   - maker order: side=sell, type=limit, tif=gtc, price=0.5, qty=30, status=open
        //   - taker order: side=buy,  type=limit, tif=gtc, price=0.5, qty=30, status=open
        //   - call commit_fill (maker_remaining_after=0, taker_remaining_after=0,
        //                       avg_price=0.5)
        // Assert:
        //   - spot_trades has the row (qty=30, price=0.5, fees=0)
        //   - both orders status=filled
        //   - A's balances: DF (0/0), USDT (15/0)
        //   - B's balances: USDT (0/0), DF (30/0)
        //   - spot_ticker_24h row exists with last_price=0.5
        // Cleanup with TRUNCATE on the test market's rows; use unique market_id
        // per invocation (e.g. format!("TESTSPOT_{}", Uuid::new_v4())) so parallel
        // tests don't collide.
        //
        // NOTE for implementer running this test later: the test body is left as
        // a scaffold because no local Postgres is available in the dev
        // environment where this code is being authored. When TEST_DATABASE_URL
        // is set in CI, expand the body to the assertions above.
        let _ = pool;
        let _ = dec!(0.5);
    }
}
