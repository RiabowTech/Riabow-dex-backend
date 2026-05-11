//! Reduce-only enforcement for unified-margin accounts.
//!
//! When a user crosses into the `reduce_only` (or `liquidating`) state,
//! all of their outstanding open / pending / partially_filled orders
//! must be cancelled and the frozen margin released. This module is the
//! shared implementation used by the risk worker.

use rust_decimal::Decimal;
use sqlx::PgPool;
use std::sync::Arc;

use crate::app::state::AppState;

/// Represents a row selected from the `orders` table for cancellation.
#[derive(sqlx::FromRow)]
struct CancellableOrder {
    id: uuid::Uuid,
    symbol: String,
    amount: Decimal,
    filled_amount: Decimal,
    price: Option<Decimal>,
    leverage: i32,
    frozen_margin: Option<Decimal>,
}

/// Cancel every non-terminal order owned by `user_address`, releasing
/// the unfilled portion of each order's frozen margin back to the
/// user's available balance.
///
/// Returns the number of orders actually cancelled. Errors during the
/// cancellation of an individual order are logged and skipped; the
/// routine always tries the whole set.
pub async fn cancel_all_open_orders(state: &Arc<AppState>, user_address: &str) -> i64 {
    let pool: &PgPool = &state.db.pool;
    let addr = user_address.to_lowercase();
    let collateral_symbol = state.config.collateral_symbol();

    let orders: Vec<CancellableOrder> = match sqlx::query_as(
        "SELECT id, symbol, amount, filled_amount, price, leverage, frozen_margin \
         FROM orders WHERE user_address = $1 \
         AND status IN ('pending','open','partially_filled')",
    )
    .bind(&addr)
    .fetch_all(pool)
    .await
    {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(
                "reduce_only: failed to list cancellable orders for {}: {}",
                addr,
                e
            );
            return 0;
        }
    };

    if orders.is_empty() {
        return 0;
    }

    let mut cancelled: i64 = 0;
    for o in &orders {
        // 1. Tell matching engine to drop it from the book.
        //    Failure here (e.g. order already completed on a concurrent
        //    fill) is non-fatal — we still mark the DB row as cancelled.
        if let Err(e) = state
            .matching_engine
            .cancel_order(&o.symbol, o.id, &addr)
        {
            tracing::debug!(
                "reduce_only: matching_engine.cancel_order({}) returned: {}",
                o.id,
                e
            );
        }

        // 2. Flip DB status to cancelled.
        if let Err(e) = sqlx::query(
            "UPDATE orders SET status = 'cancelled'::order_status, updated_at = NOW() \
             WHERE id = $1 AND status IN ('pending','open','partially_filled')",
        )
        .bind(o.id)
        .execute(pool)
        .await
        {
            tracing::warn!(
                "reduce_only: failed to mark order {} cancelled: {}",
                o.id,
                e
            );
            continue;
        }

        // 3. Release the unfilled portion of the margin.
        //    Mirrors logic in handlers/order.rs::cancel_order.
        let remaining_amount = if o.amount > Decimal::ZERO {
            o.amount - o.filled_amount
        } else {
            Decimal::ZERO
        };
        if remaining_amount <= Decimal::ZERO {
            cancelled += 1;
            continue;
        }

        let fill_ratio = if o.amount.is_zero() {
            Decimal::ZERO
        } else {
            remaining_amount / o.amount
        };

        let remaining_margin = if let Some(frozen) = o.frozen_margin {
            if frozen > Decimal::ZERO {
                frozen * fill_ratio
            } else {
                Decimal::ZERO
            }
        } else if let Some(price) = o.price {
            if price.is_zero() || o.leverage <= 0 {
                Decimal::ZERO
            } else {
                let notional = remaining_amount * price;
                let base = notional / Decimal::from(o.leverage);
                base + base * Decimal::new(5, 3) // 0.5% buffer
            }
        } else {
            Decimal::ZERO
        };

        if remaining_margin > Decimal::ZERO {
            let _ = sqlx::query(
                "UPDATE balances SET available = available + $1, \
                 frozen = GREATEST(frozen - $1, 0) \
                 WHERE user_address = $2 AND token = $3",
            )
            .bind(remaining_margin)
            .bind(&addr)
            .bind(collateral_symbol)
            .execute(pool)
            .await;
        }

        cancelled += 1;
    }

    if cancelled > 0 {
        tracing::warn!(
            "reduce_only: cancelled {} orders for user {}",
            cancelled,
            addr
        );
    }
    cancelled
}
