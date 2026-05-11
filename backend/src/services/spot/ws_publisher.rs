//! Spot WS publisher. Single Tokio task that:
//!   1. Subscribes to EngineEvent broadcast (engine.event_tx).
//!   2. Translates each event into pushes on per-channel broadcast senders
//!      that live on AppState.
//!   3. The existing /ws handler reads from those senders directly — this
//!      module never sees individual connections.
//!
//! Tasks 3 / 4 wired the public side (depth / trade / ticker / kline);
//! Task 5 adds the private side (user orders + user balances) plus a
//! `pub async fn push_balance_now` helper that REST handlers call directly
//! after admin credit / transfer to refresh the UI without a REST refetch.

use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::Instrument;

use crate::services::spot::matching::types::EngineEvent;
use crate::services::spot::ws_messages::*;
use crate::AppState;

pub fn spawn(state: Arc<AppState>, mut engine_rx: broadcast::Receiver<EngineEvent>) {
    let span = tracing::info_span!("spot_ws_publisher");
    tokio::spawn(async move {
        tracing::info!("spot ws publisher starting");
        loop {
            match engine_rx.recv().await {
                Ok(ev) => handle_engine_event(&state, ev).await,
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!("spot ws publisher lagged: {n} events dropped");
                }
                Err(broadcast::error::RecvError::Closed) => {
                    tracing::warn!("spot ws publisher: engine event channel closed, exiting");
                    return;
                }
            }
        }
    }.instrument(span));
}

async fn handle_engine_event(state: &Arc<AppState>, ev: EngineEvent) {
    match ev {
        EngineEvent::Fill { trade, maker_after, taker_after, book_update_id, affected_levels } => {
            // ===== Public (Task 3) =====
            let _ = state.spot_trade_sender.send(SpotTradePush {
                trade_id: trade.id,
                symbol:   trade.market_id.clone(),
                side:     trade.side.clone(),
                price:    trade.price.normalize().to_string(),
                quantity: trade.quantity.normalize().to_string(),
                ts:       trade.created_at.timestamp_millis(),
            });
            push_depth_diff(state, &trade.market_id, book_update_id, affected_levels);
            push_ticker(state, &trade.market_id).await;

            // ===== Private — order updates for both sides =====
            // Maker fee is in the maker's RECEIVED token (opposite of taker side).
            //   taker side = "buy"  → maker is SELL → received quote → fee in quote (USDT)
            //   taker side = "sell" → maker is BUY  → received base  → fee in base (DF)
            // Taker fee is in the taker's RECEIVED token (taker side).
            let (maker_fee_token, taker_fee_token) = match trade.side.as_str() {
                "buy"  => ("USDT", "DF"),       // maker SELL, taker BUY
                "sell" => ("DF",   "USDT"),     // maker BUY,  taker SELL
                _      => ("USDT", "USDT"),
            };
            push_user_order(state, &maker_after, Some(SpotUserOrderFill {
                trade_id: trade.id,
                price:    trade.price.normalize().to_string(),
                quantity: trade.quantity.normalize().to_string(),
                fee:      trade.maker_fee.normalize().to_string(),
                fee_token: maker_fee_token.to_string(),
            }));
            push_user_order(state, &taker_after, Some(SpotUserOrderFill {
                trade_id: trade.id,
                price:    trade.price.normalize().to_string(),
                quantity: trade.quantity.normalize().to_string(),
                fee:      trade.taker_fee.normalize().to_string(),
                fee_token: taker_fee_token.to_string(),
            }));

            // ===== Private — balance updates for both users × {base, quote} =====
            push_user_balance_for_order(state, &maker_after).await;
            push_user_balance_for_order(state, &taker_after).await;
        }
        EngineEvent::OrderPlaced { order, book_update_id, affected_levels } => {
            push_depth_diff(state, &order.market_id, book_update_id, affected_levels);
            push_user_order(state, &order, None);
            push_user_balance_for_order(state, &order).await;
        }
        EngineEvent::OrderCanceled { order, book_update_id, affected_levels } => {
            push_depth_diff(state, &order.market_id, book_update_id, affected_levels);
            push_user_order(state, &order, None);
            push_user_balance_for_order(state, &order).await;
        }
        EngineEvent::OrderRejected { order } => {
            // No public side effect; private order push only.
            push_user_order(state, &order, None);
        }
    }
}

fn push_depth_diff(
    state: &Arc<AppState>,
    symbol: &str,
    book_update_id: u64,
    affected_levels: Vec<(String, rust_decimal::Decimal, rust_decimal::Decimal)>,
) {
    let mut bids: Vec<[String; 2]> = vec![];
    let mut asks: Vec<[String; 2]> = vec![];
    for (side, price, total) in affected_levels {
        let entry = [price.normalize().to_string(), total.normalize().to_string()];
        if side == "buy" { bids.push(entry); } else { asks.push(entry); }
    }
    let _ = state.spot_depth_sender.send(SpotDepthDiff {
        symbol: symbol.to_string(),
        update_id_first: book_update_id,
        update_id_last:  book_update_id,
        bids, asks,
    });
}

async fn push_ticker(state: &Arc<AppState>, symbol: &str) {
    let row: Result<crate::models::spot::SpotTicker24h, _> = sqlx::query_as(
        "SELECT * FROM spot_ticker_24h WHERE market_id=$1"
    ).bind(symbol).fetch_one(&state.db.pool).await;
    if let Ok(t) = row {
        let now = chrono::Utc::now().timestamp();
        let _ = state.spot_ticker_sender.send(SpotTickerPush {
            symbol: t.market_id,
            last_price:   t.last_price.normalize().to_string(),
            open_price:   t.open_price_24h.normalize().to_string(),
            high:         t.high_24h.normalize().to_string(),
            low:          t.low_24h.normalize().to_string(),
            volume:       t.volume_24h.normalize().to_string(),
            quote_volume: t.quote_volume_24h.normalize().to_string(),
            trade_count:  t.trade_count_24h,
            open_time:    now - 24 * 3600,
            close_time:   now,
            ts:           now,
        });
    }
}

// ===== Private push helpers =====

fn push_user_order(
    state: &Arc<AppState>,
    o: &crate::models::spot::SpotOrder,
    last_fill: Option<SpotUserOrderFill>,
) {
    let _ = state.spot_user_order_sender.send(SpotUserOrderPush {
        user_address: o.user_address.to_lowercase(),
        id: o.id,
        symbol: o.market_id.clone(),
        side: o.side.clone(),
        r#type: o.r#type.clone(),
        tif: o.tif.clone(),
        price: o.price.map(|d| d.normalize().to_string()),
        quantity: o.quantity.map(|d| d.normalize().to_string()),
        quote_quantity: o.quote_quantity.map(|d| d.normalize().to_string()),
        filled_qty: o.filled_qty.normalize().to_string(),
        avg_fill_price: o.avg_fill_price.normalize().to_string(),
        status: o.status.clone(),
        reject_reason: o.reject_reason.clone(),
        updated_at: o.updated_at.timestamp(),
        last_fill,
    });
}

/// Helper that's also exported for handlers (admin credit, transfer) to call
/// directly so a successful REST mutation immediately refreshes the UI without
/// a follow-up REST fetch.
pub async fn push_balance_now(state: &Arc<AppState>, user: &str, token: &str) {
    let row: Result<(rust_decimal::Decimal, rust_decimal::Decimal), _> = sqlx::query_as(
        "SELECT available, frozen FROM spot_balances WHERE user_address=$1 AND token=$2"
    ).bind(user).bind(token).fetch_one(&state.db.pool).await;
    if let Ok((available, frozen)) = row {
        let _ = state.spot_user_balance_sender.send(SpotUserBalancePush {
            user_address: user.to_lowercase(),
            token: token.to_string(),
            available: available.normalize().to_string(),
            frozen: frozen.normalize().to_string(),
            ts: chrono::Utc::now().timestamp(),
        });
    }
}

async fn push_user_balance_for_order(state: &Arc<AppState>, o: &crate::models::spot::SpotOrder) {
    // Look up market metadata to know the (base, quote) tokens, then push both rows.
    let row: Result<(String, String), _> = sqlx::query_as(
        "SELECT base_token, quote_token FROM spot_markets WHERE id=$1"
    ).bind(&o.market_id).fetch_one(&state.db.pool).await;
    if let Ok((base, quote)) = row {
        push_balance_now(state, &o.user_address, &base).await;
        push_balance_now(state, &o.user_address, &quote).await;
    }
}
