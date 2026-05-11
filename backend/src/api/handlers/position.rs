//! Position API handlers

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Extension, Json,
};
use chrono::Utc;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;
use uuid::Uuid;

use crate::auth::middleware::AuthUser;
use crate::models::{ClosePositionRequest, OpenPositionRequest, PositionResponse, PositionSide};
use crate::models::order::{OrderResponse, OrderSide, OrderStatus, OrderType};
use crate::{AppState, OrderUpdateEvent};

/// Error response for position operations
#[derive(Debug, Serialize)]
pub struct PositionErrorResponse {
    pub error: String,
    pub code: String,
}

/// Response for position list
#[derive(Debug, Serialize)]
pub struct PositionsListResponse {
    pub positions: Vec<PositionResponse>,
    pub total_unrealized_pnl: Decimal,
    pub total_collateral: Decimal,
}

/// Response for position action
#[derive(Debug, Serialize)]
pub struct PositionActionResponse {
    pub success: bool,
    pub message: String,
    pub position: Option<PositionResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order: Option<OrderResponse>,
}

/// Add collateral request
#[derive(Debug, Deserialize)]
pub struct AddCollateralRequest {
    pub amount: Decimal,
}

/// Remove collateral request
#[derive(Debug, Deserialize)]
pub struct RemoveCollateralRequest {
    pub amount: Decimal,
}

/// Get all positions for authenticated user
pub async fn get_positions(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
) -> Result<Json<PositionsListResponse>, StatusCode> {
    let positions = state
        .position_service
        .get_user_positions(&auth_user.address)
        .await
        .map_err(|e| {
            tracing::error!("Failed to get user positions: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // Batch fetch all mark prices in one read (avoids N+1 queries)
    let symbols: Vec<String> = positions.iter().map(|p| p.symbol.clone()).collect();
    let prices = state
        .price_feed_service
        .batch_get_mark_prices(&symbols)
        .await;

    // Get current prices for PnL calculation
    let mut responses = Vec::new();
    let mut total_unrealized_pnl = Decimal::ZERO;
    let mut total_collateral = Decimal::ZERO;

    for position in positions {
        // Use pre-fetched price, fallback to entry price
        let mark_price = prices
            .get(&position.symbol)
            .copied()
            .unwrap_or(position.entry_price);

        let response = state
            .position_service
            .position_to_response(&position, mark_price);

        total_unrealized_pnl += response.unrealized_pnl;
        total_collateral += response.collateral_amount;
        responses.push(response);
    }

    Ok(Json(PositionsListResponse {
        positions: responses,
        total_unrealized_pnl,
        total_collateral,
    }))
}

/// Get a specific position by ID
pub async fn get_position(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path(position_id): Path<Uuid>,
) -> Result<Json<PositionResponse>, StatusCode> {
    let position = state
        .position_service
        .get_position_by_id(position_id)
        .await
        .map_err(|e| {
            tracing::error!("Failed to get position: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or(StatusCode::NOT_FOUND)?;

    // Verify ownership
    if position.user_address.to_lowercase() != auth_user.address.to_lowercase() {
        return Err(StatusCode::FORBIDDEN);
    }

    // Get mark price
    let mark_price = state
        .price_feed_service
        .get_mark_price(&position.symbol)
        .await
        .unwrap_or(position.entry_price);

    let response = state
        .position_service
        .position_to_response(&position, mark_price);

    Ok(Json(response))
}

/// Open a new position or increase existing
pub async fn open_position(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Json(req): Json<OpenPositionRequest>,
) -> Result<Json<PositionActionResponse>, (StatusCode, Json<PositionErrorResponse>)> {
    let user_address = auth_user.address.to_lowercase();

    // Validate collateral amount
    if req.collateral_amount <= Decimal::ZERO {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(PositionErrorResponse {
                error: "保证金金额必须大于0".to_string(),
                code: "INVALID_COLLATERAL_AMOUNT".to_string(),
            }),
        ));
    }

    // Validate leverage (1-50)
    if req.leverage < 1 || req.leverage > 50 {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(PositionErrorResponse {
                error: "杠杆倍数必须在1-50之间".to_string(),
                code: "INVALID_LEVERAGE".to_string(),
            }),
        ));
    }

    // Check user balance before opening position
    let collateral_symbol = state.config.collateral_symbol();
    let balance: Option<(Decimal, Decimal)> = sqlx::query_as(
        "SELECT available, frozen FROM balances WHERE user_address = $1 AND token = $2"
    )
    .bind(&user_address)
    .bind(collateral_symbol)
    .fetch_optional(&state.db.pool)
    .await
    .map_err(|e| {
        tracing::error!("Failed to fetch balance: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(PositionErrorResponse {
                error: "获取余额失败".to_string(),
                code: "BALANCE_FETCH_FAILED".to_string(),
            }),
        )
    })?;

    let available_balance = balance.map(|(available, _)| available).unwrap_or(Decimal::ZERO);

    // Check if user has enough balance for the collateral
    if available_balance < req.collateral_amount {
        tracing::warn!(
            "Insufficient balance for user {}: available={}, required={}",
            user_address,
            available_balance,
            req.collateral_amount
        );
        return Err((
            StatusCode::BAD_REQUEST,
            Json(PositionErrorResponse {
                error: format!(
                    "余额不足: 可用余额 {} {}, 需要 {} {}",
                    available_balance, collateral_symbol, req.collateral_amount, collateral_symbol
                ),
                code: "INSUFFICIENT_BALANCE".to_string(),
            }),
        ));
    }

    // Get current mark price
    let mark_price = state
        .price_feed_service
        .get_mark_price(&req.symbol)
        .await
        .ok_or_else(|| {
            tracing::error!("No mark price available for symbol: {}", req.symbol);
            (
                StatusCode::BAD_REQUEST,
                Json(PositionErrorResponse {
                    error: "无法获取当前市场价格".to_string(),
                    code: "PRICE_UNAVAILABLE".to_string(),
                }),
            )
        })?;

    // Deduct collateral from user balance (freeze it)
    sqlx::query(
        r#"
        UPDATE balances
        SET available = available - $1, frozen = frozen + $1, updated_at = NOW()
        WHERE user_address = $2 AND token = $3
        "#
    )
    .bind(req.collateral_amount)
    .bind(&user_address)
    .bind(collateral_symbol)
    .execute(&state.db.pool)
    .await
    .map_err(|e| {
        tracing::error!("Failed to update balance: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(PositionErrorResponse {
                error: "更新余额失败".to_string(),
                code: "BALANCE_UPDATE_FAILED".to_string(),
            }),
        )
    })?;

    // Use the unified increase_position method which handles both new and existing positions
    let result = state
        .position_service
        .increase_position(
            &user_address,
            &req.symbol,
            req.side,
            req.collateral_amount,
            req.leverage,
            mark_price,
            false, // Enforce min size check for user-initiated position increases
            None,  // No trade_id — user-initiated path doesn't go through matching
        )
        .await;

    // Handle error with async rollback (avoids block_in_place deadlock)
    if let Err(ref e) = result {
        // Rollback the balance change on error - async execution
        let rollback_result = sqlx::query(
            r#"
            UPDATE balances
            SET available = available + $1, frozen = frozen - $1, updated_at = NOW()
            WHERE user_address = $2 AND token = $3
            "#
        )
        .bind(req.collateral_amount)
        .bind(&user_address)
        .bind(collateral_symbol)
        .execute(&state.db.pool)
        .await;

        if let Err(rollback_err) = rollback_result {
            tracing::error!("Failed to rollback balance: {}", rollback_err);
        }

        tracing::error!("Failed to open/increase position: {:?}", e);
        return Err((
            StatusCode::BAD_REQUEST,
            Json(PositionErrorResponse {
                error: format!("开仓失败: {}", e),
                code: "POSITION_OPEN_FAILED".to_string(),
            }),
        ));
    }

    let result = result.unwrap();

    Ok(Json(PositionActionResponse {
        success: true,
        message: "Position opened successfully".to_string(),
        position: Some(result.position),
        order: None,
    }))
}

/// Close a position (fully or partially)
/// Uses database transaction with FOR UPDATE lock to prevent race conditions
pub async fn close_position(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path(position_id): Path<Uuid>,
    Json(req): Json<ClosePositionRequest>,
) -> Result<Json<PositionActionResponse>, StatusCode> {
    use crate::models::{Position, PositionStatus};

    let collateral_symbol = state.config.collateral_symbol();
    let user_address = auth_user.address.to_lowercase();

    // Start database transaction for atomic operation
    let mut tx = state.db.pool.begin().await.map_err(|e| {
        tracing::error!("Failed to start transaction: {:?}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    // Lock the position with FOR UPDATE to prevent concurrent modifications
    let position: Position = sqlx::query_as::<_, Position>(
        "SELECT * FROM positions WHERE id = $1 FOR UPDATE"
    )
    .bind(position_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| {
        tracing::error!("Failed to get position with lock: {:?}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?
    .ok_or(StatusCode::NOT_FOUND)?;

    // Verify ownership
    if position.user_address.to_lowercase() != user_address {
        return Err(StatusCode::FORBIDDEN);
    }

    // Check position status - prevent double close
    if position.status != PositionStatus::Open {
        tracing::warn!(
            "Attempted to close already closed position {}: status = {:?}",
            position_id, position.status
        );
        return Err(StatusCode::CONFLICT);
    }

    // Get mark price
    let execution_price = req.price.unwrap_or(
        state
            .price_feed_service
            .get_mark_price(&position.symbol)
            .await
            .unwrap_or(position.entry_price),
    );

    // Frontend now sends size in USD directly, no conversion needed
    // Check if it's a full close: if requested size >= 95% of position size, treat as full close
    // Using 95% threshold to account for position size changes from concurrent netting/funding
    let size_ratio = if position.size_in_usd > Decimal::ZERO {
        req.size / position.size_in_usd
    } else {
        Decimal::ONE
    };
    let is_nearly_full = size_ratio >= Decimal::from_str("0.95").unwrap();

    tracing::info!(
        "Close position: requested_size={}, position_size_in_usd={}, size_ratio={}, is_nearly_full={}",
        req.size, position.size_in_usd, size_ratio, is_nearly_full
    );

    // Calculate close parameters
    // If is_nearly_full, use full position size to ensure complete close
    let close_size_usd = if is_nearly_full {
        position.size_in_usd
    } else {
        req.size.min(position.size_in_usd)
    };
    let is_fully_closed = close_size_usd >= position.size_in_usd;

    // Spec §2.1: dust prevention. If the requested partial close would
    // leave a residual smaller than the per-market min_order_size_usd,
    // promote to a full close. Avoids creating dust positions that are
    // economically infeasible to close (gas + fees > residual value).
    let min_size_threshold = state
        .market_config_service
        .get_config(&position.symbol)
        .await
        .map(|c| c.min_order_size_usd)
        .unwrap_or_else(|| Decimal::from(10));
    let residual_size_usd = position.size_in_usd - close_size_usd;
    let is_fully_closed = is_fully_closed || residual_size_usd < min_size_threshold;
    let close_size_usd = if is_fully_closed {
        position.size_in_usd
    } else {
        close_size_usd
    };

    // .round_dp(10): see services/position/mod.rs decrease_position for why.
    // Prevents rust_decimal panic at fee (18-scale) * close_ratio.
    let close_ratio = if position.size_in_usd > Decimal::ZERO {
        (close_size_usd / position.size_in_usd).round_dp(10)
    } else {
        Decimal::ONE
    };

    // Calculate proportional values
    let close_size_tokens = position.size_in_tokens * close_ratio;
    let collateral_to_return = position.collateral_amount * close_ratio;

    // Calculate realized PnL
    let position_value = position.size_in_tokens * execution_price;
    let total_pnl = match position.side {
        PositionSide::Long => position_value - position.size_in_usd,
        PositionSide::Short => position.size_in_usd - position_value,
    };
    let realized_pnl = total_pnl * close_ratio;

    // Calculate fees.
    //
    // PR 2 (2026-04-29): replaced legacy `position_fee = close_size_usd ×
    // position_fee_rate` with `proportional_trading` driven by
    // `accumulated_trading_fee`. See spec §2.2.
    let proportional_trading   = position.accumulated_trading_fee   * close_ratio;
    let proportional_funding   = position.accumulated_funding_fee   * close_ratio;
    let proportional_borrowing = position.accumulated_borrowing_fee * close_ratio;
    let total_fees = proportional_trading + proportional_funding + proportional_borrowing;

    // Final amount to return (capped at 0 minimum)
    let collateral_returned: Decimal = (collateral_to_return + realized_pnl - total_fees).max(Decimal::ZERO);

    // Update position within transaction
    if is_fully_closed {
        sqlx::query(
            r#"
            UPDATE positions SET
                status = 'closed',
                size_in_usd = 0,
                size_in_tokens = 0,
                collateral_amount = 0,
                realized_pnl = realized_pnl + $2,
                updated_at = NOW(),
                decreased_at = NOW()
            WHERE id = $1 AND status = 'open'
            "#
        )
        .bind(position_id)
        .bind(realized_pnl)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            tracing::error!("Failed to close position: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    } else {
        // Partial close
        let new_size_usd = position.size_in_usd - close_size_usd;
        let new_size_tokens = position.size_in_tokens - close_size_tokens;
        let new_collateral = position.collateral_amount - collateral_to_return;
        let new_trading = position.accumulated_trading_fee - proportional_trading;
        let new_funding = position.accumulated_funding_fee - proportional_funding;
        let new_borrowing = position.accumulated_borrowing_fee - proportional_borrowing;

        sqlx::query(
            r#"
            UPDATE positions SET
                size_in_usd = $2,
                size_in_tokens = $3,
                collateral_amount = $4,
                accumulated_funding_fee = $5,
                accumulated_borrowing_fee = $6,
                accumulated_trading_fee = $8,
                realized_pnl = realized_pnl + $7,
                updated_at = NOW(),
                decreased_at = NOW()
            WHERE id = $1 AND status = 'open'
            "#
        )
        .bind(position_id)
        .bind(new_size_usd)
        .bind(new_size_tokens)
        .bind(new_collateral)
        .bind(new_funding)
        .bind(new_borrowing)
        .bind(realized_pnl)
        .bind(new_trading)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            tracing::error!("Failed to partially close position: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    }

    // The 0.5% frozen buffer (margin × 1.005 at order creation) is ALREADY released to
    // `available` at fill time, by both fill paths:
    //   - taker cross-spread fill: `order.rs` ~L805 sets
    //         avail_delta = required_margin - collateral_to_position   (≈ buffer)
    //     and the frozen credit + debit happen atomically inside the same submit handler,
    //     so externally `frozen` never appears non-zero for this order.
    //   - maker resting fill: `orchestrator.rs` ~L697 explicitly does
    //         frozen -= (collateral + buffer)  ;  available += buffer
    // By the time we run close, the buffer is already in `available`. Adding it again here
    // would double-credit the user — confirmed empirically 2026-04-26: a round-trip open+close
    // at lev=1 produced a windfall of exactly buffer = 0.5% × notional, reproduced on two
    // independent cycles.
    //
    // We keep `total_frozen_release` as a generous "drain anything still frozen" amount
    // (the GREATEST clamp on the UPDATE makes it a no-op if frozen is already 0; residual
    // frozen from edge cases is also handled by the cleanup block below for full closes).
    let proportional_margin = close_size_usd / Decimal::from(position.leverage);
    let frozen_buffer = proportional_margin * Decimal::new(5, 3); // 0.5% buffer (informational, not re-credited)
    // PR 2 (2026-04-29): no opening_fee was frozen at place_order, so nothing
    // to release here. Kept as Decimal::ZERO for one release cycle as a no-op
    // — `total_frozen_release` is then equal to `collateral_to_return + frozen_buffer`
    // and the GREATEST(frozen - X, 0) clamp on the UPDATE makes any over-shoot a no-op.
    let opening_fee_frozen = Decimal::ZERO;

    let total_frozen_release = collateral_to_return + frozen_buffer + opening_fee_frozen;
    let total_available_add = collateral_returned;

    // Update balance within the same transaction
    // Use GREATEST to prevent frozen from going negative
    let balance_update = sqlx::query(
        r#"
        UPDATE balances
        SET frozen = GREATEST(frozen - $1, 0),
            available = available + $2,
            updated_at = NOW()
        WHERE user_address = $3 AND token = $4
        "#
    )
    .bind(total_frozen_release)
    .bind(total_available_add)
    .bind(&user_address)
    .bind(collateral_symbol)
    .execute(&mut *tx)
    .await;

    match &balance_update {
        Ok(update_result) => {
            tracing::info!(
                "Updated balance for user {} after close: frozen -{} (collateral={}, buffer={}, open_fee={}), available +{} (returned={}, buffer={}), rows affected: {}",
                user_address,
                total_frozen_release,
                collateral_to_return,
                frozen_buffer,
                opening_fee_frozen,
                total_available_add,
                collateral_returned,
                frozen_buffer,
                update_result.rows_affected()
            );
        }
        Err(e) => {
            tracing::error!(
                "Failed to update balance after close position: {}. User: {}, frozen: {}, returned: {}",
                e,
                user_address,
                total_frozen_release,
                total_available_add
            );
            // Rollback transaction on balance update failure
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    }

    // After full close: clean up any residual frozen balance caused by
    // price slippage between order creation (mark_price) and position fill (fill_price).
    if is_fully_closed {
        let cleanup = sqlx::query(
            r#"
            UPDATE balances
            SET available = available + frozen,
                frozen = 0,
                updated_at = NOW()
            WHERE user_address = $1 AND token = $2 AND frozen > 0
              AND NOT EXISTS (
                SELECT 1 FROM positions
                WHERE user_address = $1 AND status = 'open' AND size_in_usd > 0
              )
              AND NOT EXISTS (
                SELECT 1 FROM orders
                WHERE user_address = $1 AND status = 'pending'
              )
            "#
        )
        .bind(&user_address)
        .bind(collateral_symbol)
        .execute(&mut *tx)
        .await;

        match &cleanup {
            Ok(result) if result.rows_affected() > 0 => {
                tracing::info!(
                    "Cleaned up residual frozen balance for user {} (no open positions or pending orders)",
                    user_address
                );
            }
            Err(e) => {
                tracing::warn!("Failed to cleanup residual frozen for user {}: {}", user_address, e);
            }
            _ => {}
        }
    }

    // Create close order record within transaction
    let order_side = match position.side {
        PositionSide::Long => OrderSide::Sell,
        PositionSide::Short => OrderSide::Buy,
    };
    let closed_amount_tokens = close_size_usd / execution_price;
    let order_id = Uuid::new_v4();
    let now = Utc::now();

    let insert_result = sqlx::query(
        r#"
        INSERT INTO orders (id, user_address, symbol, side, order_type, price, amount, filled_amount, leverage, status, signature, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $7, $8, 'filled', $9, $10, $10)
        "#
    )
    .bind(order_id)
    .bind(&user_address)
    .bind(&position.symbol)
    .bind(order_side)
    .bind(OrderType::Market)
    .bind(execution_price)
    .bind(closed_amount_tokens)
    .bind(position.leverage)
    .bind("close-position")
    .bind(now)
    .execute(&mut *tx)
    .await;

    // Synthetic self-matched trade for the manual close. Without this the close
    // never lands in `trades`, so Trade History only shows the user's opens and
    // the realized-pnl column stays $0. `position_synced = TRUE` keeps the
    // recovery loop in matching::orchestrator from re-processing this trade.
    let trade_id = Uuid::new_v4();
    let side_str = order_side.to_string();
    // PR 2 (2026-04-29): synthetic trade row records the actual fee paid on
    // this close (= proportional_trading), not the legacy
    // `close_size × position_fee_rate`. This keeps the user's trade-history
    // page consistent with what was actually deducted from collateral.
    let close_trade_fee = proportional_trading;
    let trade_insert = sqlx::query(
        r#"
        INSERT INTO trades (
            id, symbol, maker_order_id, taker_order_id,
            maker_address, taker_address, side, price, amount,
            maker_fee, taker_fee, created_at,
            maker_leverage, taker_leverage,
            position_synced, is_self_trade
        ) VALUES ($1, $2, $3, $3, $4, $4, $5, $6, $7, 0, $8, $9, $10, $10, TRUE, TRUE)
        "#
    )
    .bind(trade_id)
    .bind(&position.symbol)
    .bind(order_id)
    .bind(&user_address)
    .bind(&side_str)
    .bind(execution_price)
    .bind(closed_amount_tokens)
    .bind(close_trade_fee)
    .bind(now)
    .bind(position.leverage)
    .execute(&mut *tx)
    .await;
    if let Err(e) = &trade_insert {
        tracing::error!(
            "Failed to insert synthetic close trade for position {}: {}",
            position_id, e
        );
    }

    // Record realized PnL event linked to the synthetic trade so
    // /account/trades can attribute the PnL back to this row.
    let event_insert = sqlx::query(
        r#"
        INSERT INTO realized_pnl_events (
            user_address, symbol, position_id, realized_pnl,
            execution_price, size_delta_usd, is_full_close, trade_id
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        "#
    )
    .bind(&user_address)
    .bind(&position.symbol)
    .bind(position_id)
    .bind(realized_pnl)
    .bind(execution_price)
    .bind(close_size_usd)
    .bind(is_fully_closed)
    .bind(trade_id)
    .execute(&mut *tx)
    .await;
    if let Err(e) = &event_insert {
        tracing::error!(
            "Failed to insert realized_pnl_event for position {}: {}",
            position_id, e
        );
    }

    // Cancel active trigger orders when position is fully closed
    if is_fully_closed {
        let cancel_result = sqlx::query(
            r#"
            UPDATE trigger_orders
            SET status = 'cancelled', updated_at = NOW()
            WHERE position_id = $1 AND status = 'active'
            "#
        )
        .bind(position_id)
        .execute(&mut *tx)
        .await;

        if let Err(e) = cancel_result {
            tracing::warn!("Failed to cancel trigger orders for closed position {}: {}", position_id, e);
        } else if let Ok(result) = cancel_result {
            if result.rows_affected() > 0 {
                tracing::info!("Cancelled {} trigger orders for closed position {}", result.rows_affected(), position_id);
            }
        }

        // Also clean up position_tp_sl record
        let _ = sqlx::query("DELETE FROM position_tp_sl WHERE position_id = $1")
            .bind(position_id)
            .execute(&mut *tx)
            .await;
    }

    // PR 2 (2026-04-29): write protocol_fee_ledger rows inside this same tx
    // so the ledger update is atomic with the balance + position UPDATEs above.
    // Zero-amount events are silently dropped by record_fee_event.
    {
        use crate::services::protocol_fee_ledger::{record_fee_event, FeeType};
        let metadata = serde_json::json!({
            "close_ratio":    close_ratio.to_string(),
            "close_size_usd": close_size_usd.to_string(),
            "is_full_close":  is_fully_closed,
        });
        for (fee_type, amount) in [
            (FeeType::TradingFee,   proportional_trading),
            (FeeType::FundingFee,   proportional_funding),
            (FeeType::BorrowingFee, proportional_borrowing),
        ] {
            if let Err(e) = record_fee_event(
                &mut *tx,
                &user_address,
                Some(position_id),
                Some(trade_id),
                fee_type,
                amount,
                &metadata,
            ).await {
                tracing::error!(
                    target: "audit.protocol_fee_ledger",
                    user = %user_address,
                    position_id = %position_id,
                    trade_id = %trade_id,
                    fee_type = fee_type.as_str(),
                    amount = %amount,
                    "Failed to write protocol_fee_ledger row on user-close: {}", e
                );
            }
        }
    }

    // Commit transaction
    tx.commit().await.map_err(|e| {
        tracing::error!("Failed to commit transaction: {:?}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    // Invalidate cache after successful commit
    state.position_service.invalidate_position_cache(position_id).await;

    // Trigger PnL points calculation (async, non-blocking)
    // collateral_to_return is the proportional collateral from the position before close
    crate::services::points::handle_position_close_async(
        Arc::clone(&state.points_service),
        user_address.clone(),
        position_id,
        realized_pnl,
        collateral_to_return,
        position.symbol.clone(),
    );

    let order_response = match insert_result {
        Ok(_) => {
            tracing::info!(
                "Created close order {} for position {}: {} {} {} @ {}",
                order_id, position_id, order_side, closed_amount_tokens, position.symbol, execution_price
            );
            let order = OrderResponse {
                order_id,
                symbol: position.symbol.clone(),
                side: order_side,
                order_type: OrderType::Market,
                price: execution_price,
                size: close_size_usd,
                amount: closed_amount_tokens,
                filled_amount: closed_amount_tokens,
                remaining_amount: Decimal::ZERO,
                leverage: position.leverage,
                status: OrderStatus::Filled,
                created_at: now,
                reduce_only: true,
                trigger_price: None,
            };

            // Send order update to WebSocket broadcast channel
            let event = OrderUpdateEvent {
                user_address: user_address.clone(),
                order: order.clone(),
            };
            if let Err(e) = state.order_update_sender.send(event) {
                tracing::warn!("Failed to broadcast order update: {} (no receivers)", e);
            } else {
                tracing::info!("Broadcasted close order {} to WebSocket", order_id);
            }

            Some(order)
        }
        Err(e) => {
            tracing::error!("Failed to create close order: {}", e);
            None
        }
    };

    // Build response position if partial close
    let response_position = if !is_fully_closed {
        let new_size_usd = position.size_in_usd - close_size_usd;
        let new_size_tokens = position.size_in_tokens - close_size_tokens;
        let new_collateral = position.collateral_amount - collateral_to_return;
        let remaining_pnl = total_pnl - realized_pnl;
        let unrealized_pnl_percent = if new_collateral > Decimal::ZERO {
            remaining_pnl / new_collateral * Decimal::from(100)
        } else {
            Decimal::ZERO
        };
        let margin_ratio = if new_size_usd > Decimal::ZERO {
            new_collateral / new_size_usd
        } else {
            Decimal::ZERO
        };
        let net_value = new_collateral + remaining_pnl
            - (position.accumulated_funding_fee - proportional_funding)
            - (position.accumulated_borrowing_fee - proportional_borrowing);

        Some(PositionResponse {
            position_id: position.id,
            symbol: position.symbol.clone(),
            side: position.side,
            status: PositionStatus::Open,
            size_in_usd: new_size_usd,
            size: new_size_usd,
            size_in_tokens: new_size_tokens,
            amount: new_size_tokens,
            collateral_amount: new_collateral,
            entry_price: position.entry_price,
            mark_price: execution_price,
            liquidation_price: position.liquidation_price,
            leverage: position.leverage,
            margin_ratio,
            unrealized_pnl: remaining_pnl,
            unrealized_pnl_percent,
            realized_pnl: position.realized_pnl + realized_pnl,
            accumulated_funding_fee: position.accumulated_funding_fee - proportional_funding,
            accumulated_borrowing_fee: position.accumulated_borrowing_fee - proportional_borrowing,
            accumulated_trading_fee: position.accumulated_trading_fee - proportional_trading,
            net_value,
            created_at: position.created_at,
            updated_at: Utc::now(),
        })
    } else {
        // Return a closed PositionResponse with zeroed sizes
        Some(PositionResponse {
            position_id: position.id,
            symbol: position.symbol.clone(),
            side: position.side,
            status: PositionStatus::Closed,
            size_in_usd: Decimal::ZERO,
            size: Decimal::ZERO,
            size_in_tokens: Decimal::ZERO,
            amount: Decimal::ZERO,
            collateral_amount: Decimal::ZERO,
            entry_price: position.entry_price,
            mark_price: execution_price,
            liquidation_price: Decimal::ZERO,
            leverage: position.leverage,
            margin_ratio: Decimal::ZERO,
            unrealized_pnl: Decimal::ZERO,
            unrealized_pnl_percent: Decimal::ZERO,
            realized_pnl: position.realized_pnl + realized_pnl,
            accumulated_funding_fee: Decimal::ZERO,
            accumulated_borrowing_fee: Decimal::ZERO,
            accumulated_trading_fee: Decimal::ZERO,
            net_value: Decimal::ZERO,
            created_at: position.created_at,
            updated_at: Utc::now(),
        })
    };

    Ok(Json(PositionActionResponse {
        success: true,
        message: if is_fully_closed {
            "Position fully closed".to_string()
        } else {
            "Position partially closed".to_string()
        },
        position: response_position,
        order: order_response,
    }))
}

/// Add collateral to a position
pub async fn add_collateral(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path(position_id): Path<Uuid>,
    Json(req): Json<AddCollateralRequest>,
) -> Result<Json<PositionActionResponse>, (StatusCode, Json<PositionErrorResponse>)> {
    let user_address = auth_user.address.to_lowercase();

    // Validate amount
    if req.amount <= Decimal::ZERO {
        tracing::warn!("Invalid collateral amount: {}", req.amount);
        return Err((
            StatusCode::BAD_REQUEST,
            Json(PositionErrorResponse {
                error: "保证金金额必须大于0".to_string(),
                code: "INVALID_COLLATERAL_AMOUNT".to_string(),
            }),
        ));
    }

    // Verify ownership
    let position = state
        .position_service
        .get_position_by_id(position_id)
        .await
        .map_err(|e| {
            tracing::error!("Failed to get position: {:?}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(PositionErrorResponse {
                    error: "获取仓位失败".to_string(),
                    code: "POSITION_FETCH_FAILED".to_string(),
                }),
            )
        })?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(PositionErrorResponse {
                    error: "仓位不存在".to_string(),
                    code: "POSITION_NOT_FOUND".to_string(),
                }),
            )
        })?;

    if position.user_address.to_lowercase() != user_address {
        return Err((
            StatusCode::FORBIDDEN,
            Json(PositionErrorResponse {
                error: "无权操作该仓位".to_string(),
                code: "FORBIDDEN".to_string(),
            }),
        ));
    }

    // Check user balance before adding collateral
    let collateral_symbol = state.config.collateral_symbol();
    let balance: Option<(Decimal, Decimal)> = sqlx::query_as(
        "SELECT available, frozen FROM balances WHERE user_address = $1 AND token = $2"
    )
    .bind(&user_address)
    .bind(collateral_symbol)
    .fetch_optional(&state.db.pool)
    .await
    .map_err(|e| {
        tracing::error!("Failed to fetch balance: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(PositionErrorResponse {
                error: "获取余额失败".to_string(),
                code: "BALANCE_FETCH_FAILED".to_string(),
            }),
        )
    })?;

    let available_balance = balance.map(|(available, _)| available).unwrap_or(Decimal::ZERO);

    if available_balance < req.amount {
        tracing::warn!(
            "Insufficient balance for add_collateral user {}: available={}, required={}",
            user_address, available_balance, req.amount
        );
        return Err((
            StatusCode::BAD_REQUEST,
            Json(PositionErrorResponse {
                error: format!(
                    "余额不足: 可用余额 {} {}, 需要 {} {}",
                    available_balance, collateral_symbol, req.amount, collateral_symbol
                ),
                code: "INSUFFICIENT_BALANCE".to_string(),
            }),
        ));
    }

    // Deduct from available balance and add to frozen
    sqlx::query(
        r#"
        UPDATE balances
        SET available = available - $1, frozen = frozen + $1, updated_at = NOW()
        WHERE user_address = $2 AND token = $3
        "#
    )
    .bind(req.amount)
    .bind(&user_address)
    .bind(collateral_symbol)
    .execute(&state.db.pool)
    .await
    .map_err(|e| {
        tracing::error!("Failed to update balance for add_collateral: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(PositionErrorResponse {
                error: "更新余额失败".to_string(),
                code: "BALANCE_UPDATE_FAILED".to_string(),
            }),
        )
    })?;

    // Add collateral to position
    let result = state
        .position_service
        .add_collateral(position_id, req.amount)
        .await;

    if let Err(ref e) = result {
        // Rollback balance change on error
        let rollback_result = sqlx::query(
            r#"
            UPDATE balances
            SET available = available + $1, frozen = frozen - $1, updated_at = NOW()
            WHERE user_address = $2 AND token = $3
            "#
        )
        .bind(req.amount)
        .bind(&user_address)
        .bind(collateral_symbol)
        .execute(&state.db.pool)
        .await;

        if let Err(rollback_err) = rollback_result {
            tracing::error!("Failed to rollback balance after add_collateral failure: {}", rollback_err);
        }

        tracing::error!("Failed to add collateral: {:?}", e);
        return Err((
            StatusCode::BAD_REQUEST,
            Json(PositionErrorResponse {
                error: format!("增加保证金失败: {}", e),
                code: "ADD_COLLATERAL_FAILED".to_string(),
            }),
        ));
    }

    let updated = result.unwrap();

    // Get mark price
    let mark_price = state
        .price_feed_service
        .get_mark_price(&updated.symbol)
        .await
        .unwrap_or(updated.entry_price);

    let response = state
        .position_service
        .position_to_response(&updated, mark_price);

    Ok(Json(PositionActionResponse {
        success: true,
        message: "Collateral added successfully".to_string(),
        position: Some(response),
        order: None,
    }))
}

/// Remove collateral from a position
pub async fn remove_collateral(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path(position_id): Path<Uuid>,
    Json(req): Json<RemoveCollateralRequest>,
) -> Result<Json<PositionActionResponse>, (StatusCode, Json<PositionErrorResponse>)> {
    let user_address = auth_user.address.to_lowercase();

    // Validate amount
    if req.amount <= Decimal::ZERO {
        tracing::warn!("Invalid collateral removal amount: {}", req.amount);
        return Err((
            StatusCode::BAD_REQUEST,
            Json(PositionErrorResponse {
                error: "保证金金额必须大于0".to_string(),
                code: "INVALID_COLLATERAL_AMOUNT".to_string(),
            }),
        ));
    }

    // Verify ownership
    let position = state
        .position_service
        .get_position_by_id(position_id)
        .await
        .map_err(|e| {
            tracing::error!("Failed to get position: {:?}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(PositionErrorResponse {
                    error: "获取仓位失败".to_string(),
                    code: "POSITION_FETCH_FAILED".to_string(),
                }),
            )
        })?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(PositionErrorResponse {
                    error: "仓位不存在".to_string(),
                    code: "POSITION_NOT_FOUND".to_string(),
                }),
            )
        })?;

    if position.user_address.to_lowercase() != user_address {
        return Err((
            StatusCode::FORBIDDEN,
            Json(PositionErrorResponse {
                error: "无权操作该仓位".to_string(),
                code: "FORBIDDEN".to_string(),
            }),
        ));
    }

    // Get mark price for validation
    let mark_price = state
        .price_feed_service
        .get_mark_price(&position.symbol)
        .await
        .unwrap_or(position.entry_price);

    let updated = state
        .position_service
        .remove_collateral(position_id, req.amount, mark_price)
        .await
        .map_err(|e| {
            tracing::error!("Failed to remove collateral: {:?}", e);
            (
                StatusCode::BAD_REQUEST,
                Json(PositionErrorResponse {
                    error: format!("减少保证金失败: {}", e),
                    code: "REMOVE_COLLATERAL_FAILED".to_string(),
                }),
            )
        })?;

    // Return collateral from frozen to available balance
    let collateral_symbol = state.config.collateral_symbol();
    sqlx::query(
        r#"
        UPDATE balances
        SET frozen = GREATEST(frozen - $1, 0), available = available + $1, updated_at = NOW()
        WHERE user_address = $2 AND token = $3
        "#
    )
    .bind(req.amount)
    .bind(&user_address)
    .bind(collateral_symbol)
    .execute(&state.db.pool)
    .await
    .map_err(|e| {
        tracing::error!("Failed to update balance after remove_collateral: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(PositionErrorResponse {
                error: "更新余额失败".to_string(),
                code: "BALANCE_UPDATE_FAILED".to_string(),
            }),
        )
    })?;

    let response = state
        .position_service
        .position_to_response(&updated, mark_price);

    Ok(Json(PositionActionResponse {
        success: true,
        message: "Collateral removed successfully".to_string(),
        position: Some(response),
        order: None,
    }))
}

/// Check liquidation status for a position
pub async fn check_liquidation(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path(position_id): Path<Uuid>,
) -> Result<Json<crate::models::LiquidationInfo>, StatusCode> {
    // Verify ownership
    let position = state
        .position_service
        .get_position_by_id(position_id)
        .await
        .map_err(|e| {
            tracing::error!("Failed to get position: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or(StatusCode::NOT_FOUND)?;

    if position.user_address.to_lowercase() != auth_user.address.to_lowercase() {
        return Err(StatusCode::FORBIDDEN);
    }

    // Get mark price
    let mark_price = state
        .price_feed_service
        .get_mark_price(&position.symbol)
        .await
        .unwrap_or(position.entry_price);

    let info = state.position_service.check_liquidation(&position, mark_price);

    Ok(Json(info))
}
