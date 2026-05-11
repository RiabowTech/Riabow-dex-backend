//! Trigger Orders API Handlers
//!
//! Handlers for stop-loss, take-profit, trailing stop, and other trigger orders

use axum::{
    extract::{Extension, Path, Query, State},
    http::StatusCode,
    Json,
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

use crate::auth::middleware::AuthUser;
use crate::services::trigger_orders::{
    CreateTriggerOrderRequest, SetPositionTpSlRequest, TriggerOrder, TriggerOrderConfig,
    TriggerOrderExecution, TriggerOrderStatus, TriggerOrderType, TriggerCondition,
    OrderSide, PositionTpSl, UserTriggerOrderStats,
};
use crate::AppState;

/// Enhanced trigger order response with mark_price and amount
#[derive(Debug, Serialize)]
pub struct TriggerOrderResponse {
    pub id: Uuid,
    pub user_address: String,
    pub position_id: Option<Uuid>,
    pub market_symbol: String,
    pub trigger_type: TriggerOrderType,
    pub side: OrderSide,
    /// Size in USD
    pub size: Decimal,
    /// Amount in tokens (calculated from size / mark_price)
    pub amount: Decimal,
    pub trigger_price: Decimal,
    pub trigger_condition: TriggerCondition,
    pub limit_price: Option<Decimal>,
    pub trailing_delta: Option<Decimal>,
    pub trailing_delta_type: Option<String>,
    pub peak_price: Option<Decimal>,
    pub status: TriggerOrderStatus,
    /// Current mark price
    pub mark_price: Decimal,
    pub triggered_at: Option<DateTime<Utc>>,
    pub triggered_price: Option<Decimal>,
    pub executed_order_id: Option<Uuid>,
    pub executed_price: Option<Decimal>,
    pub executed_at: Option<DateTime<Utc>>,
    pub reduce_only: bool,
    pub close_position: bool,
    pub expires_at: Option<DateTime<Utc>>,
    pub client_order_id: Option<String>,
    pub error_message: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl TriggerOrderResponse {
    pub fn from_order(order: TriggerOrder, mark_price: Decimal) -> Self {
        let amount = if mark_price > Decimal::ZERO {
            order.size / mark_price
        } else {
            Decimal::ZERO
        };

        Self {
            id: order.id,
            user_address: order.user_address,
            position_id: order.position_id,
            market_symbol: order.market_symbol,
            trigger_type: order.trigger_type,
            side: order.side,
            size: order.size,
            amount,
            trigger_price: order.trigger_price,
            trigger_condition: order.trigger_condition,
            limit_price: order.limit_price,
            trailing_delta: order.trailing_delta,
            trailing_delta_type: order.trailing_delta_type,
            peak_price: order.peak_price,
            status: order.status,
            mark_price,
            triggered_at: order.triggered_at,
            triggered_price: order.triggered_price,
            executed_order_id: order.executed_order_id,
            executed_price: order.executed_price,
            executed_at: order.executed_at,
            reduce_only: order.reduce_only,
            close_position: order.close_position,
            expires_at: order.expires_at,
            client_order_id: order.client_order_id,
            error_message: order.error_message,
            created_at: order.created_at,
            updated_at: order.updated_at,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    pub data: Option<T>,
    pub error: Option<String>,
}

impl<T> ApiResponse<T> {
    pub fn success(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
        }
    }

    pub fn error(msg: &str) -> Self {
        Self {
            success: false,
            data: None,
            error: Some(msg.to_string()),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct TriggerOrdersQuery {
    pub market_symbol: Option<String>,
    pub status: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct ExecutionsQuery {
    pub limit: Option<i64>,
}

/// Create a new trigger order
/// POST /trigger-orders
pub async fn create_trigger_order(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Json(request): Json<CreateTriggerOrderRequest>,
) -> (StatusCode, Json<ApiResponse<TriggerOrderResponse>>) {
    let market_symbol = request.market_symbol.clone();

    match state
        .trigger_orders_service
        .create_trigger_order(&auth_user.address, request)
        .await
    {
        Ok(order) => {
            let mark_price = state.price_feed_service.get_mark_price(&market_symbol).await.unwrap_or(Decimal::ZERO);
            let response = TriggerOrderResponse::from_order(order, mark_price);
            (StatusCode::CREATED, Json(ApiResponse::success(response)))
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse::error(&e.to_string())),
        ),
    }
}

/// Get user's trigger orders
/// GET /trigger-orders
pub async fn get_trigger_orders(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Query(query): Query<TriggerOrdersQuery>,
) -> (StatusCode, Json<ApiResponse<Vec<TriggerOrderResponse>>>) {
    let status = query.status.as_deref().and_then(|s| {
        match s.to_lowercase().as_str() {
            "active" => Some(TriggerOrderStatus::Active),
            "triggered" => Some(TriggerOrderStatus::Triggered),
            "executed" => Some(TriggerOrderStatus::Executed),
            "cancelled" => Some(TriggerOrderStatus::Cancelled),
            "expired" => Some(TriggerOrderStatus::Expired),
            "failed" => Some(TriggerOrderStatus::Failed),
            _ => None,
        }
    });

    let limit = query.limit.unwrap_or(100);

    match state
        .trigger_orders_service
        .get_user_trigger_orders(&auth_user.address, query.market_symbol.as_deref(), status, limit)
        .await
    {
        Ok(orders) => {
            // Collect unique symbols and fetch mark prices
            let symbols: Vec<String> = orders.iter().map(|o| o.market_symbol.clone()).collect();
            let prices = state.price_feed_service.batch_get_mark_prices(&symbols).await;

            // Convert to response with mark_price and amount
            let responses: Vec<TriggerOrderResponse> = orders
                .into_iter()
                .map(|order| {
                    let mark_price = prices.get(&order.market_symbol).copied().unwrap_or(Decimal::ZERO);
                    TriggerOrderResponse::from_order(order, mark_price)
                })
                .collect();

            (StatusCode::OK, Json(ApiResponse::success(responses)))
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::error(&e.to_string())),
        ),
    }
}

/// Get a specific trigger order
/// GET /trigger-orders/:order_id
pub async fn get_trigger_order(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path(order_id): Path<Uuid>,
) -> (StatusCode, Json<ApiResponse<TriggerOrderResponse>>) {
    match state
        .trigger_orders_service
        .get_trigger_order(&auth_user.address, order_id)
        .await
    {
        Ok(Some(order)) => {
            let mark_price = state.price_feed_service.get_mark_price(&order.market_symbol).await.unwrap_or(Decimal::ZERO);
            let response = TriggerOrderResponse::from_order(order, mark_price);
            (StatusCode::OK, Json(ApiResponse::success(response)))
        }
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(ApiResponse::error("Trigger order not found")),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::error(&e.to_string())),
        ),
    }
}

/// Cancel a trigger order
/// DELETE /trigger-orders/:order_id
pub async fn cancel_trigger_order(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path(order_id): Path<Uuid>,
) -> (StatusCode, Json<ApiResponse<TriggerOrderResponse>>) {
    match state
        .trigger_orders_service
        .cancel_trigger_order(&auth_user.address, order_id)
        .await
    {
        Ok(order) => {
            let mark_price = state.price_feed_service.get_mark_price(&order.market_symbol).await.unwrap_or(Decimal::ZERO);
            let response = TriggerOrderResponse::from_order(order, mark_price);
            (StatusCode::OK, Json(ApiResponse::success(response)))
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse::error(&e.to_string())),
        ),
    }
}

/// Set position TP/SL
/// POST /positions/:position_id/tp-sl
pub async fn set_position_tp_sl(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path(position_id): Path<Uuid>,
    Json(request): Json<SetPositionTpSlRequest>,
) -> (StatusCode, Json<ApiResponse<PositionTpSl>>) {
    // Validate take_profit_size and stop_loss_size must be greater than 0 if provided
    if let Some(tp_size) = request.take_profit_size {
        if tp_size <= Decimal::ZERO {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiResponse::error("take_profit_size must be greater than 0")),
            );
        }
    }
    if let Some(sl_size) = request.stop_loss_size {
        if sl_size <= Decimal::ZERO {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiResponse::error("stop_loss_size must be greater than 0")),
            );
        }
    }

    // Limit-price validation: it only makes sense alongside a trigger price,
    // and must be strictly positive.
    if let Some(tp_limit) = request.take_profit_limit_price {
        if tp_limit <= Decimal::ZERO {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiResponse::error("take_profit_limit_price must be greater than 0")),
            );
        }
        if request.take_profit_price.is_none() {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiResponse::error("take_profit_limit_price requires take_profit_price")),
            );
        }
    }
    if let Some(sl_limit) = request.stop_loss_limit_price {
        if sl_limit <= Decimal::ZERO {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiResponse::error("stop_loss_limit_price must be greater than 0")),
            );
        }
        if request.stop_loss_price.is_none() {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiResponse::error("stop_loss_limit_price requires stop_loss_price")),
            );
        }
    }

    // First get the position to verify ownership, status, and get market symbol
    match state.position_service.get_position_by_id(position_id).await {
        Ok(Some(position)) if position.user_address.eq_ignore_ascii_case(&auth_user.address) => {
            // Check if position is open - only open positions can have TP/SL set
            if position.status != crate::models::PositionStatus::Open {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ApiResponse::error("Cannot set TP/SL on a closed or liquidated position")),
                );
            }

            // Direction validation for limit prices.
            // Long positions close via SELL → limit must be ≤ trigger (floor price).
            // Short positions close via BUY  → limit must be ≥ trigger (ceiling price).
            // Violating this means the limit order would never fill after trigger.
            let is_long = position.side == crate::models::PositionSide::Long;
            if let (Some(tp_trigger), Some(tp_limit)) = (request.take_profit_price, request.take_profit_limit_price) {
                if is_long && tp_limit > tp_trigger {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(ApiResponse::error("Long TP: take_profit_limit_price must be <= take_profit_price (sell floor)")),
                    );
                }
                if !is_long && tp_limit < tp_trigger {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(ApiResponse::error("Short TP: take_profit_limit_price must be >= take_profit_price (buy ceiling)")),
                    );
                }
            }
            if let (Some(sl_trigger), Some(sl_limit)) = (request.stop_loss_price, request.stop_loss_limit_price) {
                if is_long && sl_limit > sl_trigger {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(ApiResponse::error("Long SL: stop_loss_limit_price must be <= stop_loss_price (sell floor)")),
                    );
                }
                if !is_long && sl_limit < sl_trigger {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(ApiResponse::error("Short SL: stop_loss_limit_price must be >= stop_loss_price (buy ceiling)")),
                    );
                }
            }

            match state
                .trigger_orders_service
                .set_position_tp_sl(&auth_user.address, position_id, &position.symbol, position.side, request)
                .await
            {
                Ok(tp_sl) => (StatusCode::OK, Json(ApiResponse::success(tp_sl))),
                Err(e) => (
                    StatusCode::BAD_REQUEST,
                    Json(ApiResponse::error(&e.to_string())),
                ),
            }
        }
        Ok(Some(_)) => (
            StatusCode::FORBIDDEN,
            Json(ApiResponse::error("Position does not belong to you")),
        ),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(ApiResponse::error("Position not found")),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::error(&e.to_string())),
        ),
    }
}

/// Get position TP/SL settings
/// GET /positions/:position_id/tp-sl
pub async fn get_position_tp_sl(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path(position_id): Path<Uuid>,
) -> (StatusCode, Json<ApiResponse<Option<PositionTpSl>>>) {
    // Verify position ownership first
    match state.position_service.get_position_by_id(position_id).await {
        Ok(Some(position)) if position.user_address.eq_ignore_ascii_case(&auth_user.address) => {
            match state.trigger_orders_service.get_position_tp_sl(position_id).await {
                Ok(tp_sl) => (StatusCode::OK, Json(ApiResponse::success(tp_sl))),
                Err(e) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiResponse::error(&e.to_string())),
                ),
            }
        }
        Ok(Some(_)) => (
            StatusCode::FORBIDDEN,
            Json(ApiResponse::error("Position does not belong to you")),
        ),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(ApiResponse::error("Position not found")),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::error(&e.to_string())),
        ),
    }
}

/// Update position TP/SL
/// PUT /positions/:position_id/tp-sl
pub async fn update_position_tp_sl(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path(position_id): Path<Uuid>,
    Json(request): Json<SetPositionTpSlRequest>,
) -> (StatusCode, Json<ApiResponse<PositionTpSl>>) {
    // Reuse the same logic as set_position_tp_sl (UPSERT)
    set_position_tp_sl(State(state), Extension(auth_user), Path(position_id), Json(request)).await
}

/// Delete position TP/SL
/// DELETE /positions/:position_id/tp-sl
pub async fn delete_position_tp_sl(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path(position_id): Path<Uuid>,
) -> (StatusCode, Json<ApiResponse<String>>) {
    // Verify position ownership first
    match state.position_service.get_position_by_id(position_id).await {
        Ok(Some(position)) if position.user_address.eq_ignore_ascii_case(&auth_user.address) => {
            match state.trigger_orders_service.delete_position_tp_sl(position_id).await {
                Ok(()) => (
                    StatusCode::OK,
                    Json(ApiResponse::success("TP/SL deleted successfully".to_string())),
                ),
                Err(e) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiResponse::error(&e.to_string())),
                ),
            }
        }
        Ok(Some(_)) => (
            StatusCode::FORBIDDEN,
            Json(ApiResponse::error("Position does not belong to you")),
        ),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(ApiResponse::error("Position not found")),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::error(&e.to_string())),
        ),
    }
}


/// Get trigger order config for a market
/// GET /trigger-orders/:symbol/config
pub async fn get_trigger_order_config(
    State(state): State<Arc<AppState>>,
    Path(symbol): Path<String>,
) -> (StatusCode, Json<ApiResponse<TriggerOrderConfig>>) {
    match state.trigger_orders_service.get_config(&symbol).await {
        Ok(config) => (StatusCode::OK, Json(ApiResponse::success(config))),
        Err(e) => (
            StatusCode::NOT_FOUND,
            Json(ApiResponse::error(&e.to_string())),
        ),
    }
}

/// Get user's trigger order execution history
/// GET /trigger-orders/executions
pub async fn get_user_executions(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Query(query): Query<ExecutionsQuery>,
) -> (StatusCode, Json<ApiResponse<Vec<TriggerOrderExecution>>>) {
    let limit = query.limit.unwrap_or(100);

    match state
        .trigger_orders_service
        .get_user_executions(&auth_user.address, limit)
        .await
    {
        Ok(executions) => (StatusCode::OK, Json(ApiResponse::success(executions))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::error(&e.to_string())),
        ),
    }
}

/// Get user's trigger order stats for a market
/// GET /trigger-orders/:symbol/stats
pub async fn get_user_stats(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path(symbol): Path<String>,
) -> (StatusCode, Json<ApiResponse<Option<UserTriggerOrderStats>>>) {
    match state
        .trigger_orders_service
        .get_user_stats(&auth_user.address, &symbol)
        .await
    {
        Ok(stats) => (StatusCode::OK, Json(ApiResponse::success(stats))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::error(&e.to_string())),
        ),
    }
}
