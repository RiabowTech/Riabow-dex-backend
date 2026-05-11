//! Order API Handlers
//!
//! Phase 8: Complete order execution pipeline with balance checking and matching engine integration

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Extension, Json,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

use crate::auth::middleware::AuthUser;
use crate::auth::eip712::{
    verify_create_order_signature_with_debug, verify_cancel_order_signature, verify_batch_cancel_signature,
    get_create_order_typed_data,
    CreateOrderMessage, CancelOrderMessage, BatchCancelMessage,
};
use crate::models::{CreateOrderRequest, Order, OrderResponse, OrderStatus, OrderType, OrderSide};
use crate::services::matching::{Side as MatchingSide, OrderType as MatchingOrderType, OrderStatus as MatchingOrderStatus, TimeInForce as MatchingTimeInForce};
use crate::AppState;

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct CancelOrderRequest {
    /// EIP-712 signature — required for JWT auth, optional for API Key auth
    #[serde(default)]
    pub signature: Option<String>,
    #[serde(default)]
    pub timestamp: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BatchCancelRequest {
    pub order_ids: Vec<Uuid>,
    #[serde(default)]
    pub signature: Option<String>,
    #[serde(default)]
    pub timestamp: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BatchCancelResponse {
    pub cancelled: Vec<Uuid>,
    pub failed: Vec<Uuid>,
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub code: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateOrderResponse {
    pub order_id: Uuid,
    pub status: OrderStatus,
    pub filled_amount: Decimal,
    pub remaining_amount: Decimal,
    pub average_price: Decimal,  // Changed from Option<Decimal> to Decimal - use 0 if no fill
    #[serde(serialize_with = "serialize_datetime_as_millis")]
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// Symbol-shard routing shim for the JWT path. Mirrors the developer_trade
/// counterpart in semantics — see services/sharding/mod.rs and the
/// `maybe_forward_*` family in api/handlers/developer_trade.rs.
async fn maybe_forward_create_order(
    state: &Arc<AppState>,
    headers: &axum::http::HeaderMap,
    original_uri: &axum::http::Uri,
    symbol: &str,
    req: &CreateOrderRequest,
) -> Result<Option<Json<CreateOrderResponse>>, (StatusCode, Json<ErrorResponse>)> {
    use crate::services::sharding::{proxy, RouteDecision, FORWARDED_HEADER};

    let already_forwarded = headers.contains_key(FORWARDED_HEADER);
    match state.sharding.route_for(symbol) {
        RouteDecision::Local => Ok(None),
        RouteDecision::Forward { target_host, owner_ordinal } => {
            if already_forwarded {
                tracing::warn!(
                    "shard-routing: hash drift on JWT create_order for {} (owner thinks: {}); handling locally as fallback.",
                    symbol, owner_ordinal
                );
                return Ok(None);
            }
            let resp: CreateOrderResponse = proxy::forward_json(
                &target_host,
                axum::http::Method::POST,
                original_uri.path(),
                headers,
                &[],
                Some(req),
            )
            .await
            .map_err(|e| {
                (
                    StatusCode::BAD_GATEWAY,
                    Json(ErrorResponse {
                        error: format!("shard-proxy create_order: {}", e),
                        code: "SHARD_PROXY_ERROR".to_string(),
                    }),
                )
            })?;
            Ok(Some(Json(resp)))
        }
    }
}

/// Look up a single order's symbol by id. Returns Ok(None) if the row
/// doesn't exist — the caller can let the existing handler return its
/// own 404 instead of pre-empting.
///
/// Used by the order-id-keyed shims (cancel / update / batch_cancel)
/// since the request URL doesn't carry the symbol but the routing
/// decision needs it. PK lookup, ~sub-ms.
async fn lookup_order_symbol(
    pool: &sqlx::PgPool,
    order_id: Uuid,
) -> Result<Option<String>, (StatusCode, Json<ErrorResponse>)> {
    sqlx::query_scalar::<_, String>("SELECT symbol FROM orders WHERE id = $1")
        .bind(order_id)
        .fetch_optional(pool)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("shard-route lookup: {}", e),
                    code: "DB_ERROR".to_string(),
                }),
            )
        })
}

/// Forward a DELETE /orders/:order_id to the symbol owner. Body is
/// optional — JWT path may or may not include the EIP-712 signature
/// in the body. We re-serialise via Option<&CancelOrderRequest>.
async fn maybe_forward_cancel_order_jwt(
    state: &Arc<AppState>,
    headers: &axum::http::HeaderMap,
    original_uri: &axum::http::Uri,
    order_id: Uuid,
    body: Option<&CancelOrderRequest>,
) -> Result<Option<Json<OrderResponse>>, (StatusCode, Json<ErrorResponse>)> {
    use crate::services::sharding::{proxy, RouteDecision, FORWARDED_HEADER};

    // Disabled-path early exit — keeps the JWT cancel handler's pre-PoC
    // DB call profile (one lookup, not two). Same guard in update_order
    // and batch_cancel below.
    if !state.sharding.enabled {
        return Ok(None);
    }

    let already_forwarded = headers.contains_key(FORWARDED_HEADER);
    let symbol = match lookup_order_symbol(&state.db.pool, order_id).await? {
        Some(s) => s,
        // Order doesn't exist — defer to the local handler's NOT_FOUND.
        None => return Ok(None),
    };

    match state.sharding.route_for(&symbol) {
        RouteDecision::Local => Ok(None),
        RouteDecision::Forward { target_host, owner_ordinal } => {
            if already_forwarded {
                tracing::warn!(
                    "shard-routing: hash drift on JWT cancel_order id={} symbol={} (owner thinks: {}); handling locally as fallback.",
                    order_id, symbol, owner_ordinal
                );
                return Ok(None);
            }
            let resp: OrderResponse = proxy::forward_json(
                &target_host,
                axum::http::Method::DELETE,
                original_uri.path(),
                headers,
                &[],
                body,
            )
            .await
            .map_err(|e| {
                (
                    StatusCode::BAD_GATEWAY,
                    Json(ErrorResponse {
                        error: format!("shard-proxy cancel_order: {}", e),
                        code: "SHARD_PROXY_ERROR".to_string(),
                    }),
                )
            })?;
            Ok(Some(Json(resp)))
        }
    }
}

/// Forward a PUT /orders/:order_id to the symbol owner.
async fn maybe_forward_update_order(
    state: &Arc<AppState>,
    headers: &axum::http::HeaderMap,
    original_uri: &axum::http::Uri,
    order_id: Uuid,
    body: &crate::models::UpdateOrderRequest,
) -> Result<Option<Json<serde_json::Value>>, (StatusCode, Json<ErrorResponse>)> {
    use crate::services::sharding::{proxy, RouteDecision, FORWARDED_HEADER};

    if !state.sharding.enabled {
        return Ok(None);
    }

    let already_forwarded = headers.contains_key(FORWARDED_HEADER);
    let symbol = match lookup_order_symbol(&state.db.pool, order_id).await? {
        Some(s) => s,
        None => return Ok(None),
    };

    match state.sharding.route_for(&symbol) {
        RouteDecision::Local => Ok(None),
        RouteDecision::Forward { target_host, owner_ordinal } => {
            if already_forwarded {
                tracing::warn!(
                    "shard-routing: hash drift on JWT update_order id={} symbol={} (owner thinks: {}); handling locally as fallback.",
                    order_id, symbol, owner_ordinal
                );
                return Ok(None);
            }
            // Use raw JSON pass-through so we don't need UpdateOrderRequest
            // to derive Deserialize on the response — the response from
            // update_order is a free-form serde_json::Value.
            let (status, val) = proxy::forward_json_raw(
                &target_host,
                axum::http::Method::PUT,
                original_uri.path(),
                headers,
                &[],
                Some(body),
            )
            .await
            .map_err(|e| {
                (
                    StatusCode::BAD_GATEWAY,
                    Json(ErrorResponse {
                        error: format!("shard-proxy update_order: {}", e),
                        code: "SHARD_PROXY_ERROR".to_string(),
                    }),
                )
            })?;
            if !status.is_success() {
                return Err((
                    status,
                    Json(ErrorResponse {
                        error: format!("upstream {}: {}", status, val),
                        code: "UPSTREAM_ERROR".to_string(),
                    }),
                ));
            }
            Ok(Some(Json(val)))
        }
    }
}

/// Forward a POST /orders/batch when the entire batch belongs to ONE
/// non-self owner. Returns:
///   - Ok(None) if all orders are local OR the batch spans multiple
///     owners (the latter logs a warn — fan-out is a follow-up). The
///     caller proceeds with local execution; the not-owned slice will
///     re-create divergence on its way through, which is the
///     pre-PoC behaviour we already accept until fan-out lands.
///   - Ok(Some(resp)) if forwarded to a single remote owner.
async fn maybe_forward_batch_cancel(
    state: &Arc<AppState>,
    headers: &axum::http::HeaderMap,
    original_uri: &axum::http::Uri,
    req: &BatchCancelRequest,
) -> Result<Option<Json<BatchCancelResponse>>, (StatusCode, Json<ErrorResponse>)> {
    use crate::services::sharding::{proxy, RouteDecision, FORWARDED_HEADER};

    if !state.sharding.enabled {
        return Ok(None);
    }

    if req.order_ids.is_empty() {
        return Ok(None);
    }

    let already_forwarded = headers.contains_key(FORWARDED_HEADER);

    // Bulk symbol lookup — single round-trip vs N for the per-id path.
    let rows: Vec<(Uuid, String)> = sqlx::query_as(
        "SELECT id, symbol FROM orders WHERE id = ANY($1)",
    )
    .bind(&req.order_ids)
    .fetch_all(&state.db.pool)
    .await
    .map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("shard-route batch lookup: {}", e),
                code: "DB_ERROR".to_string(),
            }),
        )
    })?;

    if rows.is_empty() {
        // None of the ids resolve. Let the local handler emit its own
        // per-id "failed" entries so the response shape is preserved.
        return Ok(None);
    }

    // Compute the owner ordinal per row and dedup.
    use std::collections::HashSet;
    let mut owners: HashSet<u32> = HashSet::new();
    for (_, sym) in &rows {
        owners.insert(state.sharding.owner_for(sym));
    }

    // All-local: nothing to forward.
    if owners.iter().all(|o| *o == state.sharding.my_ordinal) {
        return Ok(None);
    }

    // Multi-owner split: punt to local execution with a warn. Single
    // forward would route part of the batch to the wrong pod. Fan-out
    // (split-merge) is intentionally deferred — most user batches
    // target a single symbol or a small set that hashes to one owner,
    // so this case is expected to be rare. When it happens, the
    // not-owned slice re-creates divergence on this pod, same as
    // pre-PoC behaviour.
    if owners.len() > 1 {
        tracing::warn!(
            "shard-routing: batch_cancel spans {} owners (ids={:?}); handling locally without fan-out.",
            owners.len(), req.order_ids
        );
        return Ok(None);
    }

    // Single non-self owner — forward whole batch.
    let owner_ordinal = *owners.iter().next().unwrap();

    if already_forwarded {
        tracing::warn!(
            "shard-routing: hash drift on JWT batch_cancel owner={}; handling locally as fallback.",
            owner_ordinal
        );
        return Ok(None);
    }

    // Resolve target_host via route_for on a representative symbol
    // from the batch (any symbol owned by this owner works).
    let representative_symbol = rows.iter()
        .find(|(_, s)| state.sharding.owner_for(s) == owner_ordinal)
        .map(|(_, s)| s.clone())
        .unwrap();
    let target_host = match state.sharding.route_for(&representative_symbol) {
        RouteDecision::Forward { target_host, .. } => target_host,
        RouteDecision::Local => {
            // Race: ownership flipped between owners.len() check and
            // route_for. Drop to local.
            return Ok(None);
        }
    };

    let resp: BatchCancelResponse = proxy::forward_json(
        &target_host,
        axum::http::Method::POST,
        original_uri.path(),
        headers,
        &[],
        Some(req),
    )
    .await
    .map_err(|e| {
        (
            StatusCode::BAD_GATEWAY,
            Json(ErrorResponse {
                error: format!("shard-proxy batch_cancel: {}", e),
                code: "SHARD_PROXY_ERROR".to_string(),
            }),
        )
    })?;
    Ok(Some(Json(resp)))
}

// Helper function to serialize DateTime as milliseconds timestamp
fn serialize_datetime_as_millis<S>(
    dt: &chrono::DateTime<chrono::Utc>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_i64(dt.timestamp_millis())
}


/// Validate timestamp (within 5 minutes)
fn validate_timestamp(timestamp: u64) -> bool {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    now.abs_diff(timestamp) <= 300
}

// Note: is_valid_symbol now uses config.is_valid_trading_pair() instead of hardcoded values

/// Create a new order
/// POST /orders
pub async fn create_order(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    headers: axum::http::HeaderMap,
    axum::extract::OriginalUri(original_uri): axum::extract::OriginalUri,
    Json(mut req): Json<CreateOrderRequest>,
) -> Result<Json<CreateOrderResponse>, (StatusCode, Json<ErrorResponse>)> {
    // Validate symbol using market_config_service (DB-driven)
    if !state.market_config_service.is_tradeable(&req.symbol).await {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: format!("不支持的交易对: {}", req.symbol),
                code: "INVALID_SYMBOL".to_string(),
            }),
        ));
    }

    // Symbol-shard routing. Default OFF; once enabled the receiving pod
    // re-runs auth + EIP-712 verification against the same JSON body, so
    // forwarding before signature check is safe and saves the local pod's
    // CPU. See services/sharding/mod.rs.
    if let Some(resp) =
        maybe_forward_create_order(&state, &headers, &original_uri, &req.symbol, &req).await?
    {
        return Ok(resp);
    }

    // Validate timestamp (skip for API Key auth and dev mode)
    if !auth_user.is_api_key && !state.config.is_auth_disabled() {
        let timestamp = req.timestamp.ok_or_else(|| (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "缺少 timestamp 字段".to_string(),
                code: "MISSING_TIMESTAMP".to_string(),
            }),
        ))?;
        if !validate_timestamp(timestamp) {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "时间戳已过期".to_string(),
                    code: "TIMESTAMP_EXPIRED".to_string(),
                }),
            ));
        }
    }

    // Validate leverage using per-symbol max from market_config
    let max_leverage = state.market_config_service
        .get_config(&req.symbol).await
        .map(|c| c.max_leverage)
        .unwrap_or(50);

    if req.leverage < 1 || req.leverage > max_leverage {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: format!("杠杆倍数必须在1-{}之间", max_leverage),
                code: "INVALID_LEVERAGE".to_string(),
            }),
        ));
    }

    // Validate amount
    if req.amount <= Decimal::ZERO {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "订单数量必须大于0".to_string(),
                code: "INVALID_AMOUNT".to_string(),
            }),
        ));
    }

    // Validate limit-like order has price
    let is_limit_like = matches!(req.order_type, OrderType::Limit | OrderType::TakeProfitLimit | OrderType::StopLossLimit);
    if is_limit_like && req.price.is_none() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "限价单必须指定价格".to_string(),
                code: "PRICE_REQUIRED".to_string(),
            }),
        ));
    }

    // Trigger price requirement for new order types
    let is_trigger_type = matches!(
        req.order_type,
        OrderType::TakeProfitLimit | OrderType::StopLossLimit | OrderType::TakeProfitMarket | OrderType::StopLossMarket
    );
    if is_trigger_type && req.trigger_price.is_none() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "触发单必须指定触发价格 (trigger_price)".to_string(),
                code: "TRIGGER_PRICE_REQUIRED".to_string(),
            }),
        ));
    }

    // Limit 单价格必须 > 0。之前 price=0 会被接受并挂进 orderbook 成为永远
    // 不会 cross 的垃圾挂单，污染深度并给前端渲染带来边界 case。
    if is_limit_like {
        if let Some(p) = req.price {
            if p <= Decimal::ZERO {
                return Err((
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: "限价单价格必须大于0".to_string(),
                        code: "INVALID_PRICE".to_string(),
                    }),
                ));
            }
        }
    }

    // Min order notional 校验（粉尘单过滤）
    // 之前没有这个检查，QA 实测 amount=0.000001 BTC @ price=1（notional ≈ $1e-6）可挂单。
    // 市场配置里有 min_order_size_usd（默认 10），应在订单创建阶段就拦截。
    // - Limit 单用 req.price 计 notional
    // - Market 单若传了 price（当 max-slippage 用），同样用它；没传则跳过（由撮合阶段拦）
    let notional_check_price = req.price;
    if let Some(check_price) = notional_check_price {
        if let Some(market_cfg) = state.market_config_service.get_config(&req.symbol).await {
            let notional = check_price * req.amount;
            if notional < market_cfg.min_order_size_usd {
                return Err((
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: format!(
                            "订单名义价值 {} 低于最小限额 {}",
                            notional, market_cfg.min_order_size_usd
                        ),
                        code: "NOTIONAL_TOO_SMALL".to_string(),
                    }),
                ));
            }
        }
    }

    // EIP-712 签名验证 — API Key 认证跳过签名，仅 JWT (钱包) 认证需要
    if auth_user.is_api_key {
        tracing::info!("Order via API Key auth (skip EIP-712) for address: {}", auth_user.address);
    } else if !state.config.is_auth_disabled() {
        let signature = req.signature.as_deref().ok_or_else(|| (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "缺少 signature 字段".to_string(),
                code: "MISSING_SIGNATURE".to_string(),
            }),
        ))?;
        let timestamp = req.timestamp.unwrap_or(0);

        let order_msg = CreateOrderMessage {
            wallet: auth_user.address.to_lowercase(),
            symbol: req.symbol.clone(),
            side: format!("{}", req.side),
            order_type: format!("{}", req.order_type),
            price: req.price.map(|p| p.to_string()).unwrap_or_else(|| "0".to_string()),
            amount: req.amount.to_string(),
            leverage: req.leverage as u32,
            timestamp,
        };

        let expected_typed_data = get_create_order_typed_data(&order_msg);
        tracing::debug!(
            "Create order message: wallet={}, symbol={}, side={}, order_type={}, price={}, amount={}, leverage={}, timestamp={}",
            order_msg.wallet, order_msg.symbol, order_msg.side, order_msg.order_type,
            order_msg.price, order_msg.amount, order_msg.leverage, order_msg.timestamp
        );
        tracing::debug!("Expected typed data for signing: {}", serde_json::to_string(&expected_typed_data).unwrap_or_default());

        let verify_result = match verify_create_order_signature_with_debug(&order_msg, signature, &auth_user.address) {
            Ok(result) => result,
            Err(e) => {
                tracing::error!("Create order signature verification error: {}", e);
                return Err((
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: "签名格式无效".to_string(),
                        code: "INVALID_SIGNATURE_FORMAT".to_string(),
                    }),
                ));
            }
        };

        if !verify_result.is_valid {
            tracing::warn!(
                "Create order signature verification failed: recovered={}, expected={}, domain_separator={}, struct_hash={}, message_hash={}",
                verify_result.recovered_address,
                verify_result.expected_address,
                verify_result.domain_separator,
                verify_result.struct_hash,
                verify_result.message_hash
            );
            tracing::warn!("Expected typed data: {}", serde_json::to_string_pretty(&expected_typed_data).unwrap_or_default());
            return Err((
                StatusCode::UNAUTHORIZED,
                Json(ErrorResponse {
                    error: "订单签名验证失败".to_string(),
                    code: "SIGNATURE_INVALID".to_string(),
                }),
            ));
        }

        tracing::info!("EIP-712 order signature verified for address: {}", auth_user.address);
    }

    // ─── reduceOnly enforcement ───────────────────────────────────────────
    // (P0: 2026-04-26 prod MM matrix confirmed bug.)
    //
    // Until this fix the JWT/HMAC `/api/v1/orders` path stored req.reduce_only
    // in the orders row and otherwise ignored it. The matching engine has
    // no concept of reduceOnly, so a `SELL 0.005 reduce_only=true` against a
    // LONG 0.001 position would fully match 0.005 — flattening 0.001 of long
    // and OPENING a new SHORT 0.004, the exact opposite of reduceOnly's
    // contract. The /fapi/v1/order path already had this fix in
    // developer_trade.rs (search "reduceOnly handling (P0 N2"); this block
    // ports it so the two API entry points behave the same.
    //
    // Done before calculate_required_margin so the margin calc uses the
    // post-cap amount; the EIP-712 signature verifies against the original
    // `req.amount` so capping doesn't invalidate it (reduce_only=true is
    // itself the user's opt-in to "reduce up to the cap" semantics).
    if req.reduce_only {
        let pos: Option<(String, Decimal)> = sqlx::query_as(
            "SELECT side::text, size_in_tokens FROM positions WHERE user_address = $1 AND symbol = $2 AND status = 'open'"
        )
        .bind(&auth_user.address.to_lowercase())
        .bind(&req.symbol)
        .fetch_optional(&state.db.pool)
        .await
        .unwrap_or(None);

        let opposite_pos_size = match &pos {
            Some((pos_side, sz)) => {
                let is_buy = matches!(req.side, OrderSide::Buy);
                let is_opposite = (is_buy && pos_side.to_lowercase() == "short")
                              || (!is_buy && pos_side.to_lowercase() == "long");
                if is_opposite { *sz } else { Decimal::ZERO }
            }
            None => Decimal::ZERO,
        };

        if opposite_pos_size <= Decimal::ZERO {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "reduce_only 订单要求存在对手方仓位".to_string(),
                    code: "REDUCE_ONLY_NO_POSITION".to_string(),
                }),
            ));
        }

        if req.amount > opposite_pos_size {
            tracing::info!(
                "reduceOnly cap: user={} symbol={} requested={} pos_size={} → capping to pos_size",
                auth_user.address, req.symbol, req.amount, opposite_pos_size
            );
            req.amount = opposite_pos_size;
        }
    }

    // Spec §1.4: per-market per-side OI cap. Hard-reject orders whose
    // NET OI delta would push the in-memory aggregate past the configured
    // cap. reduce_only orders are already capped to the opposite-side
    // position size above, so they cannot increase OI — skip them.
    //
    // Cap is a soft target — no per-symbol mutex; brief overshoot under
    // concurrent placement is acceptable. See spec §1.5.
    if !req.reduce_only {
        use crate::models::PositionSide;

        let est_price = match req.price {
            Some(p) if p > Decimal::ZERO => p,
            _ => state
                .price_feed_service
                .get_mark_price(&req.symbol)
                .await
                .unwrap_or(Decimal::ZERO),
        };

        if est_price > Decimal::ZERO {
            let order_size_usd = req.amount * est_price;

            let opposite_side_str = match req.side {
                OrderSide::Buy => "short",
                OrderSide::Sell => "long",
            };
            let opposite_size_usd: Decimal = sqlx::query_scalar(
                "SELECT COALESCE(size_in_usd, 0) FROM positions WHERE user_address = $1 AND symbol = $2 AND side = $3 AND status = 'open'"
            )
            .bind(&auth_user.address.to_lowercase())
            .bind(&req.symbol)
            .bind(opposite_side_str)
            .fetch_optional(&state.db.pool)
            .await
            .unwrap_or(None)
            .unwrap_or(Decimal::ZERO);

            let cap_side = match req.side {
                OrderSide::Buy => PositionSide::Long,
                OrderSide::Sell => PositionSide::Short,
            };
            // Pure close (order ≤ opposite): delta = 0
            // Flip (order > opposite, opposite > 0): delta = order - opposite
            // Pure new: delta = order_size_usd
            let delta_usd = (order_size_usd - opposite_size_usd).max(Decimal::ZERO);

            if delta_usd > Decimal::ZERO {
                if let Some(cfg) = state.market_config_service.get_config(&req.symbol).await {
                    let cap_usd = match cap_side {
                        PositionSide::Long => cfg.max_long_oi_usd,
                        PositionSide::Short => cfg.max_short_oi_usd,
                    };
                    if let Err(e) = state
                        .funding_rate_service
                        .check_oi_cap_with_cap(&req.symbol, cap_side, delta_usd, cap_usd)
                        .await
                    {
                        tracing::warn!(
                            "OI cap rejection: {} {:?} delta={} current={} cap={}",
                            e.symbol, e.side, e.requested_delta_usd, e.current_oi_usd, e.cap_usd
                        );
                        return Err((
                            StatusCode::BAD_REQUEST,
                            Json(ErrorResponse {
                                error: format!(
                                    "{} OI cap exceeded on {:?} side: current ${}, cap ${}, requested delta ${}",
                                    e.symbol, e.side, e.current_oi_usd, e.cap_usd, e.requested_delta_usd
                                ),
                                code: "OI_CAP_EXCEEDED".to_string(),
                            }),
                        ));
                    }
                }
            }
        }
    }

    // Check balance (collateral token from config) - skip if auth disabled for development
    let _collateral_token = state.config.collateral_token();
    let collateral_symbol = state.config.collateral_symbol();
    let required_margin = calculate_required_margin_with_user(&req, &state, &auth_user.address.to_lowercase()).await;

    // --- BYPASS MARGIN CHECK FOR CLOSING POSITIONS ---
    let mut should_skip_balance_check = false;
    if !state.config.is_auth_disabled() {
        // Check if user has an open position that this order would close
        // Use size_in_tokens for comparison with order amount (both in tokens)
        let pos_check: Option<(String, Decimal)> = sqlx::query_as(
             "SELECT side::text, size_in_tokens FROM positions WHERE user_address = $1 AND symbol = $2 AND status = 'open'"
        )
        .bind(&auth_user.address.to_lowercase())
        .bind(&req.symbol)
        .fetch_optional(&state.db.pool)
        .await
        .unwrap_or(None);

        if let Some((pos_side, pos_size_tokens)) = pos_check {
             let is_buy = matches!(req.side, OrderSide::Buy);
             let is_short = pos_side.to_lowercase() == "short";
             let is_long = pos_side.to_lowercase() == "long";

             // Buy closes Short, Sell closes Long
             if (is_buy && is_short) || (!is_buy && is_long) {
                 if req.amount <= pos_size_tokens {
                     tracing::info!("Order {} {} <= Position {} {} tokens: Closing position, skipping balance check and margin freeze",
                        if is_buy { "Buy" } else { "Sell" }, req.amount, pos_side, pos_size_tokens);
                     should_skip_balance_check = true;
                 } else {
                     tracing::info!("Order {} {} > Position {} {} tokens: Partial close + new position",
                        if is_buy { "Buy" } else { "Sell" }, req.amount, pos_side, pos_size_tokens);
                 }
             }
        }
    }
    // ------------------------------------------------

    // ============================================================
    // Unified-margin branch: if user is in `unified` mode, gate the
    // order on simulated uniMMR ≥ 1.10 instead of the per-order
    // isolated margin check. Closing/reducing orders still bypass.
    // See design_docs/08_统一保证金模式设计.md §6.2.
    // ============================================================
    if !state.config.is_auth_disabled() && !should_skip_balance_check {
        let margin_mode: Option<String> = sqlx::query_scalar(
            "SELECT margin_mode FROM users WHERE address = $1",
        )
        .bind(&auth_user.address.to_lowercase())
        .fetch_optional(&state.db.pool)
        .await
        .unwrap_or(None);

        if margin_mode.as_deref() == Some("unified") {
            let addr = auth_user.address.to_lowercase();
            let snapshot = match crate::api::handlers::unified_margin::load_unified_snapshot(&state, &addr).await {
                Ok(s) => s,
                Err((status, body)) => {
                    // Re-map into this handler's ErrorResponse shape.
                    return Err((status, Json(ErrorResponse {
                        error: body.0.error,
                        code: body.0.code,
                    })));
                }
            };

            // Use limit price or mark price for notional estimation
            // (same convention as calculate_required_margin above).
            let price_for_notional = match req.price {
                Some(p) => p,
                None => state.price_feed_service
                    .get_mark_price(&req.symbol)
                    .await
                    .unwrap_or(Decimal::ZERO),
            };
            let new_notional = req.amount * price_for_notional;

            let tiers_guard = state.margin_tiers.read().await;
            let sim = crate::services::unified_margin::simulate_open_with_tiers(
                &snapshot,
                &req.symbol,
                new_notional,
                req.leverage,
                crate::services::unified_margin::MMR_DEFAULT,
                Some(&*tiers_guard),
            );
            drop(tiers_guard);

            if !sim.can_open {
                let reason = sim.reason.unwrap_or("unified margin check failed");
                tracing::warn!(
                    "Unified margin rejected order: user={}, reason={}, current_uniMMR={:?}, sim_uniMMR={:?}",
                    auth_user.address, reason, sim.current_uni_mmr, sim.simulated_uni_mmr
                );
                return Err((
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: format!("统一保证金校验未通过: {}", reason),
                        code: "UNIFIED_MARGIN_REJECTED".to_string(),
                    }),
                ));
            }

            tracing::debug!(
                "Unified margin ok: user={}, sim_uniMMR={:?}, new_IM={}",
                auth_user.address, sim.simulated_uni_mmr, sim.new_initial_margin
            );
        }
    }

    if !state.config.is_auth_disabled() {
        let balance: Option<(Decimal, Decimal)> = sqlx::query_as(
            "SELECT available, frozen FROM balances WHERE user_address = $1 AND token = $2"
        )
        .bind(&auth_user.address.to_lowercase())
        .bind(collateral_symbol)
        .fetch_optional(&state.db.pool)
        .await
        .map_err(|e| {
            tracing::error!("Failed to check balance: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "检查余额失败".to_string(),
                    code: "BALANCE_CHECK_FAILED".to_string(),
                }),
            )
        })?;

        let available_balance = balance.map(|(a, _)| a).unwrap_or(Decimal::ZERO);

        if available_balance < required_margin && !should_skip_balance_check {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: format!("可用余额不足，需要 {} {} 作为保证金", required_margin, collateral_symbol),
                    code: "INSUFFICIENT_BALANCE".to_string(),
                }),
            ));
        }
    } else {
        tracing::debug!("Auth disabled - skipping balance check");
    }

    // Create order in database
    let order_id = Uuid::new_v4();
    let now = chrono::Utc::now();

    // Get mark price at order creation time
    let mark_price_at_creation = state.price_feed_service
        .get_mark_price(&req.symbol)
        .await
        .unwrap_or_else(|| req.price.unwrap_or(Decimal::ZERO));

    // Begin transaction
    let mut tx = state.db.pool.begin().await.map_err(|e| {
        tracing::error!("Failed to begin transaction: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "数据库事务失败".to_string(),
                code: "DB_ERROR".to_string(),
            }),
        )
    })?;

    // Freeze margin (skip if auth disabled or if closing position)
    if !state.config.is_auth_disabled() && !should_skip_balance_check {
        sqlx::query(
            r#"
            INSERT INTO balances (user_address, token, available, frozen)
            VALUES ($1, $2, 0, $3)
            ON CONFLICT (user_address, token)
            DO UPDATE SET
                available = balances.available - $3,
                frozen = balances.frozen + $3
            "#
        )
        .bind(&auth_user.address.to_lowercase())
        .bind(collateral_symbol)
        .bind(required_margin)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            tracing::error!("Failed to freeze margin: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "冻结保证金失败".to_string(),
                    code: "MARGIN_FREEZE_FAILED".to_string(),
                }),
            )
        })?;
    } else if should_skip_balance_check {
        tracing::info!("Skipping margin freeze for closing position order");
    }

    // Determine frozen_margin: actual margin frozen, or 0 if skipped
    let frozen_margin = if !state.config.is_auth_disabled() && !should_skip_balance_check {
        required_margin
    } else {
        Decimal::ZERO
    };

    // Insert order into database (tp_price / sl_price persisted so that
    // limit orders that fill later can still create TP/SL trigger orders).
    sqlx::query(
        r#"
        INSERT INTO orders (id, user_address, symbol, side, order_type, price, amount, filled_amount, leverage, status, signature, created_at, updated_at, mark_price_at_creation, frozen_margin, reduce_only, tp_price, sl_price, trigger_price)
        VALUES ($1, $2, $3, $4, $5, $6, $7, 0, $8, 'pending', $9, $10, $10, $11, $12, $13, $14, $15, $16)
        "#
    )
    .bind(order_id)
    .bind(&auth_user.address.to_lowercase())
    .bind(&req.symbol)
    .bind(req.side)
    .bind(req.order_type)
    .bind(req.price)
    .bind(req.amount)
    .bind(req.leverage)
    .bind(req.signature.as_deref().unwrap_or("api_key"))
    .bind(now)
    .bind(mark_price_at_creation)
    .bind(frozen_margin)
    .bind(req.reduce_only)
    .bind(req.tp_price)
    .bind(req.sl_price)
    .bind(req.trigger_price)
    .execute(&mut *tx)
    .await
    .map_err(|e| {
        tracing::error!("Failed to insert order: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "创建订单失败".to_string(),
                code: "ORDER_CREATE_FAILED".to_string(),
            }),
        )
    })?;

    tx.commit().await.map_err(|e| {
        tracing::error!("Failed to commit transaction: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "事务提交失败".to_string(),
                code: "TX_COMMIT_FAILED".to_string(),
            }),
        )
    })?;

    // Determine if this is a standalone trigger order
    let is_trigger_type = matches!(
        req.order_type,
        OrderType::TakeProfitLimit | OrderType::StopLossLimit | OrderType::TakeProfitMarket | OrderType::StopLossMarket
    );

    if is_trigger_type {
        // Divert to trigger_orders_service
        use crate::services::trigger_orders::{CreateTriggerOrderRequest, TriggerOrderType as ModelTriggerType, OrderSide as TriggerSide};
        
        let trigger_type = match req.order_type {
            OrderType::TakeProfitLimit => ModelTriggerType::TakeProfitLimit,
            OrderType::StopLossLimit => ModelTriggerType::StopLimit,
            OrderType::TakeProfitMarket => ModelTriggerType::TakeProfit,
            OrderType::StopLossMarket => ModelTriggerType::StopLoss,
            _ => unreachable!(),
        };

        let trigger_side = match req.side {
            OrderSide::Buy => TriggerSide::Buy,
            OrderSide::Sell => TriggerSide::Sell,
        };

        let trigger_req = CreateTriggerOrderRequest {
            position_id: None, // Standalone order
            market_symbol: req.symbol.clone(),
            trigger_type,
            side: trigger_side,
            size: req.amount * req.trigger_price.unwrap_or(mark_price_at_creation), // Approximate USD size
            trigger_price: req.trigger_price.unwrap_or(Decimal::ZERO),
            limit_price: req.price,
            trailing_delta: None,
            trailing_delta_type: None,
            reduce_only: Some(req.reduce_only),
            close_position: Some(false),
            expires_at: None,
            client_order_id: None,
        };

        match state.trigger_orders_service.create_trigger_order(&auth_user.address.to_lowercase(), trigger_req).await {
            Ok(trigger_order) => {
                // Update the parent order with the trigger_order_id
                let _ = sqlx::query("UPDATE orders SET trigger_order_id = $1 WHERE id = $2")
                    .bind(trigger_order.id)
                    .bind(order_id)
                    .execute(&state.db.pool)
                    .await;
                
                let response = CreateOrderResponse {
                    order_id,
                    status: OrderStatus::Pending,
                    filled_amount: Decimal::ZERO,
                    remaining_amount: req.amount,
                    average_price: Decimal::ZERO,
                    created_at: now,
                };
                return Ok(Json(response));
            },
            Err(e) => {
                tracing::error!("Failed to create trigger order: {}", e);
                // Rollback the parent order? For now just return error
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: format!("创建触发单失败: {}", e),
                        code: "TRIGGER_CREATE_FAILED".to_string(),
                    }),
                ));
            }
        }
    }

    // Submit regular order to matching engine
    let matching_side = match req.side {
        OrderSide::Buy => MatchingSide::Buy,
        OrderSide::Sell => MatchingSide::Sell,
    };
    let matching_order_type = match req.order_type {
        OrderType::Limit => MatchingOrderType::Limit,
        OrderType::Market => MatchingOrderType::Market,
        OrderType::TakeProfitLimit => MatchingOrderType::TakeProfitLimit,
        OrderType::StopLossLimit => MatchingOrderType::StopLossLimit,
        OrderType::TakeProfitMarket => MatchingOrderType::TakeProfitMarket,
        OrderType::StopLossMarket => MatchingOrderType::StopLossMarket,
    };

    // Market-order 滑点保护：若用户预期 `max_slippage`，基于当前盘口模拟
    // 扫单，超过阈值直接拒单。此时订单已落库但状态仍为 pending，这里
    // 需要补一笔 rejected 并解冻保证金。
    let is_market_like = matches!(req.order_type, OrderType::Market | OrderType::TakeProfitMarket | OrderType::StopLossMarket);
    if is_market_like {
        let max_slip = req.max_slippage.unwrap_or(DEFAULT_MAX_SLIPPAGE);
        if max_slip > Decimal::ZERO {
            let snapshot = state.matching_engine.get_orderbook(&req.symbol, 100).ok();
            if let Some(snap) = snapshot {
                let side_levels = match req.side {
                    OrderSide::Buy => snap.asks.as_slice(),
                    OrderSide::Sell => snap.bids.as_slice(),
                };
                let sim = crate::utils::slippage::simulate_fill(side_levels, req.amount);
                let reference = if mark_price_at_creation > Decimal::ZERO {
                    mark_price_at_creation
                } else {
                    req.price.unwrap_or(Decimal::ZERO)
                };
                let slip = sim.slippage_vs(reference);
                if slip > max_slip {
                    tracing::warn!(
                        "Rejecting market order {}: simulated slippage {} exceeds max {}",
                        order_id, slip, max_slip
                    );
                    // 回滚：标记订单 rejected + 解冻保证金
                    let _ = sqlx::query(
                        "UPDATE orders SET status = 'rejected' WHERE id = $1"
                    )
                    .bind(order_id)
                    .execute(&state.db.pool)
                    .await;
                    if frozen_margin > Decimal::ZERO {
                        let _ = sqlx::query(
                            "UPDATE balances SET available = available + $1, frozen = frozen - $1 WHERE user_address = $2 AND token = $3"
                        )
                        .bind(frozen_margin)
                        .bind(&auth_user.address.to_lowercase())
                        .bind(collateral_symbol)
                        .execute(&state.db.pool)
                        .await;
                    }
                    return Err((
                        StatusCode::BAD_REQUEST,
                        Json(ErrorResponse {
                            error: format!(
                                "预估滑点 {:.4} 超过上限 {:.4}",
                                slip, max_slip
                            ),
                            code: "SLIPPAGE_EXCEEDED".to_string(),
                        }),
                    ));
                }
            }
        }
    }

    // Resolve VIP-tier rates for this user. `resolve` is the interactive
    // entry point — it lazily promotes the user if 14d volume now justifies
    // a higher tier and broadcasts the upgrade event. Apply the same
    // referral / staking discount stack and 6dp rounding as preview_order
    // (see line ~1180) so the fee actually charged matches the rate quoted.
    let user_addr = auth_user.address.to_lowercase();
    let effective_tier = crate::services::vip_tier::resolve(
        &state.db.pool,
        &state.vip_tier_event_sender,
        &user_addr,
    ).await;
    let multiplier = crate::utils::fee_tiers::discount_multiplier(&user_addr, true);
    let taker_fee_rate =
        crate::utils::fee_tiers::round_fee(effective_tier.current.taker * multiplier);
    let maker_fee_rate =
        crate::utils::fee_tiers::round_fee(effective_tier.current.maker * multiplier);

    let match_result = state.matching_engine.submit_order(
        order_id,
        &req.symbol,
        &user_addr,
        matching_side,
        matching_order_type,
        req.amount,
        req.price,
        MatchingTimeInForce::GTC,
        req.leverage as u32,
        taker_fee_rate,
        maker_fee_rate,
    ).map_err(|e| {
        tracing::error!("Matching engine error: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "撮合引擎错误".to_string(),
                code: "MATCHING_ERROR".to_string(),
            }),
        )
    })?;

    // Post-match check: If market order is still not fully filled (unlikely with pre-check)
    // this serves as a fallback
    if is_market_like && match_result.filled_amount < req.amount {
        tracing::warn!(
            "Market order {} not fully filled despite pre-check: {}/{}",
            order_id,
            match_result.filled_amount,
            req.amount
        );
    }

    // Convert matching engine status back to model status
    let order_status = match match_result.status {
        MatchingOrderStatus::Open => OrderStatus::Open,
        MatchingOrderStatus::PartiallyFilled => OrderStatus::PartiallyFilled,
        MatchingOrderStatus::Filled => OrderStatus::Filled,
        MatchingOrderStatus::Cancelled => OrderStatus::Cancelled,
        MatchingOrderStatus::Rejected => OrderStatus::Rejected,
    };

    // Update order status in database
    // For market orders, also update price to the average fill price
    // Convert OrderStatus enum to database string format
    let status_str = match order_status {
        OrderStatus::Open => "open",
        OrderStatus::PartiallyFilled => "partially_filled",  // Must use underscore for PostgreSQL enum
        OrderStatus::Filled => "filled",
        OrderStatus::Cancelled => "cancelled",
        OrderStatus::Rejected => "rejected",
        OrderStatus::Pending => "pending",
    };
    
    sqlx::query(
        "UPDATE orders SET status = $1::order_status, filled_amount = $2, price = COALESCE($3, price) WHERE id = $4"
    )
    .bind(status_str)
    .bind(match_result.filled_amount)
    .bind(match_result.average_price)  // Update price with average fill price
    .bind(order_id)
    .execute(&state.db.pool)
    .await
    .map_err(|e| {
        tracing::error!("Failed to update order status: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "更新订单状态失败".to_string(),
                code: "ORDER_UPDATE_FAILED".to_string(),
            }),
        )
    })?;

    // Release the taker's frozen collateral for whatever actually filled at
    // submit time + whatever is no longer needed (on Cancelled/Rejected).
    //
    // Historical bug: the old branch was
    //     Filled    => remaining_margin = 0    (nothing released — WRONG)
    //     Cancelled => remaining_margin = required_margin
    // which leaked the frozen on every fill and accumulated ~$419M phantom
    // frozen across accounts. Corrected logic per fill slice:
    //     collateral = filled × avg_price / leverage         → goes to position
    //     buffer     = collateral × 0.5%                     → returns to available
    //     frozen_delta = what leaves frozen this moment
    //     available_delta = what credits back to available (buffer + unused)
    if match_result.filled_amount > Decimal::ZERO
        || order_status == OrderStatus::Cancelled
        || order_status == OrderStatus::Rejected
    {
        let avg_price = match_result
            .average_price
            .unwrap_or(req.price.unwrap_or(Decimal::ZERO));
        let leverage_d = Decimal::from(req.leverage.max(1));
        let collateral_to_position = if avg_price > Decimal::ZERO {
            match_result.filled_amount * avg_price / leverage_d
        } else {
            Decimal::ZERO
        };

        // frozen_delta = what to subtract from balances.frozen and orders.frozen_margin
        // available_delta = what to add back to balances.available
        let (frozen_delta, available_delta) = match order_status {
            OrderStatus::Filled | OrderStatus::Cancelled | OrderStatus::Rejected => {
                // Full teardown: release everything still frozen; available gets
                // back whatever did not transfer to position.
                let avail = (required_margin - collateral_to_position).max(Decimal::ZERO);
                (required_margin, avail)
            }
            OrderStatus::PartiallyFilled if match_result.filled_amount > Decimal::ZERO => {
                // Pro-rata: release the portion corresponding to what filled.
                let per_unit = crate::safe_div!(
                    required_margin,
                    req.amount,
                    "order.rs: taker per_unit_frozen"
                );
                let frozen = match_result.filled_amount * per_unit;
                let avail = (frozen - collateral_to_position).max(Decimal::ZERO);
                (frozen, avail)
            }
            _ => (Decimal::ZERO, Decimal::ZERO),
        };

        if frozen_delta > Decimal::ZERO {
            sqlx::query(
                "UPDATE balances SET frozen = GREATEST(frozen - $1, 0), available = available + $2, updated_at = NOW() WHERE user_address = $3 AND token = $4"
            )
            .bind(frozen_delta)
            .bind(available_delta)
            .bind(&auth_user.address.to_lowercase())
            .bind(collateral_symbol)
            .execute(&state.db.pool)
            .await
            .ok();

            sqlx::query(
                "UPDATE orders SET frozen_margin = GREATEST(frozen_margin - $1, 0) WHERE id = $2"
            )
            .bind(frozen_delta)
            .bind(order_id)
            .execute(&state.db.pool)
            .await
            .ok();
        }
    }

    tracing::info!(
        "Order {} created: user={}, side={}, amount={}, price={:?}, filled_amount={}, status={:?}. (DEBUG: Requested Amount={})",
        order_id,
        auth_user.address,
        req.side,
        req.amount,
        req.price,
        match_result.filled_amount,
        order_status,
        req.amount
    );

    // Auto-create TP/SL trigger orders if requested and order was filled
    if (order_status == OrderStatus::Filled || order_status == OrderStatus::PartiallyFilled)
        && (req.tp_price.is_some() || req.sl_price.is_some())
    {
        let tp_price = req.tp_price;
        let sl_price = req.sl_price;
        let symbol = req.symbol.clone();
        let side = req.side;
        let user_addr = auth_user.address.to_lowercase();
        let trigger_service = state.trigger_orders_service.clone();

        // 订阅 trade 广播：position 的落库在 orchestrator 侧是 trade 之后
        // 的动作，为避免 sleep(500ms) 竞态这里用轮询 + 事件通知双保险：
        //   1) 如果在超时前看到匹配本订单的 TradeEvent，说明 orchestrator
        //      收到了同一条事件，我们再让一小段时间让它落库；
        //   2) 随后以 10×200ms 的节奏轮询 positions，直到看到最新的 open
        //      仓位（updated_at ≥ 订单创建时间）。
        let mut trade_rx = state.matching_engine.subscribe_trades();
        let order_id_for_spawn = order_id;
        let order_created_at = now;
        tokio::spawn(async move {
            use crate::services::trigger_orders::SetPositionTpSlRequest;
            use crate::models::PositionSide;

            let position_side = match side {
                OrderSide::Buy => PositionSide::Long,
                OrderSide::Sell => PositionSide::Short,
            };

            // 1) 等同一 order 的 trade 广播（最多 2s）。broadcast 可能已被消费，
            //    超时也没关系——直接进入轮询阶段。
            let _ = tokio::time::timeout(
                tokio::time::Duration::from_secs(2),
                async {
                    while let Ok(ev) = trade_rx.recv().await {
                        if ev.taker_order_id == order_id_for_spawn
                            || ev.maker_order_id == order_id_for_spawn
                        {
                            return;
                        }
                    }
                },
            ).await;

            // 2) 轮询 positions 表，最多 3 秒。筛选条件收紧为 updated_at ≥
            //    订单创建时间，避免取到无关的老仓位。
            let pool = trigger_service.get_pool();
            let mut position_id: Option<uuid::Uuid> = None;
            for _ in 0..15 {
                let row: Option<(uuid::Uuid,)> = match sqlx::query_as(
                    "SELECT id FROM positions \
                     WHERE user_address = $1 AND symbol = $2 AND side = $3 \
                       AND status = 'open' AND updated_at >= $4 \
                     ORDER BY updated_at DESC LIMIT 1"
                )
                .bind(&user_addr)
                .bind(&symbol)
                .bind(position_side)
                .bind(order_created_at)
                .fetch_optional(pool)
                .await
                {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::warn!("TP/SL position lookup failed: {}", e);
                        None
                    }
                };
                if let Some((id,)) = row {
                    position_id = Some(id);
                    break;
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
            }

            let Some(position_id) = position_id else {
                tracing::warn!(
                    "TP/SL auto-create timed out waiting for position: order={}, user={}, symbol={}, side={:?}",
                    order_id_for_spawn, user_addr, symbol, side
                );
                return;
            };

            let request = SetPositionTpSlRequest {
                take_profit_price: tp_price,
                stop_loss_price: sl_price,
                take_profit_size: None,
                stop_loss_size: None,
                take_profit_limit_price: None,
                stop_loss_limit_price: None,
                trailing_stop_delta: None,
                trailing_stop_delta_type: None,
                trailing_stop_size: None,
            };
            match trigger_service
                .set_position_tp_sl(&user_addr, position_id, &symbol, position_side, request)
                .await
            {
                Ok(_) => tracing::info!(
                    "Auto-created TP/SL for position {} (tp={:?}, sl={:?})",
                    position_id, tp_price, sl_price
                ),
                Err(e) => tracing::warn!(
                    "Failed to auto-create TP/SL for position {}: {}",
                    position_id, e
                ),
            }
        });
    }

    Ok(Json(CreateOrderResponse {
        order_id,
        status: order_status,
        filled_amount: match_result.filled_amount,
        remaining_amount: match_result.remaining_amount,
        average_price: match_result.average_price.unwrap_or(Decimal::ZERO),  // Use 0 if no fill
        created_at: now,
    }))
}

// ==================== Order Preview ====================

/// Request for order preview — identical shape to CreateOrderRequest but no signature needed
#[derive(Debug, Deserialize)]
pub struct OrderPreviewRequest {
    pub symbol: String,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub price: Option<Decimal>,
    pub amount: Decimal,
    pub leverage: i32,
    #[serde(default)]
    pub reduce_only: bool,
    /// 用户可选滑点上限（小数，例如 0.005 = 0.5%）。省略时回显默认 1%。
    #[serde(default)]
    pub max_slippage: Option<Decimal>,
}

/// Response for order preview — all the summary data shown beneath the "Place Order" button
#[derive(Debug, Serialize)]
pub struct OrderPreviewResponse {
    /// Order size in tokens
    pub order_size: Decimal,
    /// Order size label (e.g., "50 SKY")
    pub order_size_symbol: String,
    /// Order notional value in USD
    pub order_value: Decimal,
    /// Estimated execution price (mark price for market, limit price for limit)
    pub est_price: Decimal,
    /// Estimated liquidation price for the resulting position
    pub est_liq_price: Decimal,
    /// Current position margin before this order
    pub position_margin_before: Decimal,
    /// Position margin after this order
    pub position_margin_after: Decimal,
    /// Estimated slippage percentage (0.0 for limit orders)
    pub est_slippage: Decimal,
    /// Max slippage tolerance configured
    pub max_slippage: Decimal,
    /// Taker fee rate for this user
    pub taker_fee_rate: Decimal,
    /// Maker fee rate for this user
    pub maker_fee_rate: Decimal,
    /// Estimated fee for this order in USD
    pub est_fee: Decimal,
    /// Whether this order would reduce an existing position
    pub is_reduce_only: bool,
    /// Available balance for trading
    pub available_balance: Decimal,
    /// Required margin for this order
    pub required_margin: Decimal,
    /// Whether the user has sufficient balance
    pub has_sufficient_balance: bool,
}

/// 默认市价单滑点保护上限（1%）。用户可通过 `OrderPreviewRequest::max_slippage` 覆盖。
const DEFAULT_MAX_SLIPPAGE: Decimal = rust_decimal_macros::dec!(0.01);

/// Preview an order before submitting — calculates all UI summary fields
/// POST /orders/preview
pub async fn preview_order(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Json(req): Json<OrderPreviewRequest>,
) -> Result<Json<OrderPreviewResponse>, (StatusCode, Json<ErrorResponse>)> {
    use crate::models::PositionSide;
    use crate::services::position::PositionService;
    use crate::utils::order_classify::classify;
    use crate::utils::slippage::simulate_fill;

    let user_address = auth_user.address.to_lowercase();
    let collateral_symbol = state.config.collateral_symbol();

    // 1. Mark price（用于平均/预估参考）。
    let mark_price = state.price_feed_service
        .get_mark_price(&req.symbol)
        .await
        .unwrap_or(Decimal::ZERO);

    // 2. 预估成交价。Market 单稍后覆盖为盘口加权平均。
    let is_market_like = matches!(req.order_type, OrderType::Market | OrderType::TakeProfitMarket | OrderType::StopLossMarket);
    let mut est_price = match req.order_type {
        OrderType::Limit | OrderType::TakeProfitLimit | OrderType::StopLossLimit => req.price.unwrap_or(mark_price),
        OrderType::Market | OrderType::TakeProfitMarket | OrderType::StopLossMarket => mark_price,
    };

    if est_price.is_zero() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: format!("No price available for {}", req.symbol),
                code: "NO_PRICE".to_string(),
            }),
        ));
    }

    let same_side = match req.side {
        OrderSide::Buy => PositionSide::Long,
        OrderSide::Sell => PositionSide::Short,
    };
    let opposite_side = match req.side {
        OrderSide::Buy => PositionSide::Short,
        OrderSide::Sell => PositionSide::Long,
    };

    let existing_same: Option<(Decimal, Decimal, Decimal, Decimal, i32)> = match sqlx::query_as(
        "SELECT size_in_usd, size_in_tokens, collateral_amount, entry_price, leverage FROM positions WHERE user_address = $1 AND symbol = $2 AND side = $3 AND status = 'open'"
    )
    .bind(&user_address)
    .bind(&req.symbol)
    .bind(same_side)
    .fetch_optional(&state.db.pool)
    .await
    {
        Ok(v) => v,
        Err(e) => { tracing::warn!("preview same-side position query failed: {}", e); None }
    };

    let existing_opposite: Option<(Decimal, Decimal, Decimal)> = match sqlx::query_as(
        "SELECT size_in_usd, size_in_tokens, collateral_amount FROM positions WHERE user_address = $1 AND symbol = $2 AND side = $3 AND status = 'open'"
    )
    .bind(&user_address)
    .bind(&req.symbol)
    .bind(opposite_side)
    .fetch_optional(&state.db.pool)
    .await
    {
        Ok(v) => v,
        Err(e) => { tracing::warn!("preview opposite-side position query failed: {}", e); None }
    };

    // is_closing 判定：走共享语义，reduce_only 且无对手仓位时显式报错。
    let classification = classify(
        req.reduce_only,
        req.amount,
        existing_opposite.map(|(_, t, _)| t),
    );
    if classification.reject_reduce_only_no_position {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "reduce_only 订单要求存在对手方仓位".to_string(),
                code: "REDUCE_ONLY_NO_POSITION".to_string(),
            }),
        ));
    }
    let is_closing = classification.is_closing;

    // 3. Market 单走盘口撮合模拟得到加权平均成交价 + 实际滑点。
    //    Limit 单滑点恒为 0（挂单成交价即报价）。
    let (est_slippage, simulated_fill_price) = if is_market_like && !is_closing {
        let snapshot = state.matching_engine.get_orderbook(&req.symbol, 100).ok();
        if let Some(snap) = snapshot {
            let side_levels = match req.side {
                OrderSide::Buy => snap.asks.as_slice(),
                OrderSide::Sell => snap.bids.as_slice(),
            };
            let sim = simulate_fill(side_levels, req.amount);
            let reference = if mark_price > Decimal::ZERO { mark_price } else { est_price };
            (sim.slippage_vs(reference), sim.avg_price)
        } else {
            (Decimal::ZERO, None)
        }
    } else {
        (Decimal::ZERO, None)
    };
    if let Some(px) = simulated_fill_price {
        est_price = px;
    }

    let order_value = req.amount * est_price;
    let token_symbol = req.symbol.replace("USDT", "");

    let position_margin_before = existing_same
        .map(|(_, _, coll, _, _)| coll)
        .unwrap_or(Decimal::ZERO);

    // PR 2 (2026-04-29): the legacy 0.1% opening_fee has been removed.
    // Users now pay exclusively through the maker/taker rate (`est_fee`
    // computed below). `opening_fee` stays in the response as 0 for one
    // release cycle so any frontend reading the field doesn't crash;
    // remove the field entirely in a follow-up. Spec §2.5.
    let new_margin = order_value / Decimal::from(req.leverage);
    let opening_fee = Decimal::ZERO;
    let margin_after_fee = new_margin;

    let position_margin_after = if is_closing {
        let opp_coll = existing_opposite.map(|(_, _, c)| c).unwrap_or(Decimal::ZERO);
        (opp_coll - margin_after_fee).max(Decimal::ZERO)
    } else if existing_same.is_some() {
        let (_, _, existing_coll, _, _) = existing_same.unwrap();
        existing_coll + margin_after_fee
    } else {
        margin_after_fee
    };

    let maintenance_margin_rate = state.position_service.config.maintenance_margin_rate;
    let est_liq_price = if is_closing {
        Decimal::ZERO
    } else if let Some((existing_size, existing_tokens, _, _, _)) = existing_same {
        let total_size_usd = existing_size + order_value;
        let total_tokens = existing_tokens + req.amount;
        let blended_entry = if total_tokens > Decimal::ZERO {
            total_size_usd / total_tokens
        } else {
            est_price
        };
        PositionService::calculate_liquidation_price(
            blended_entry, req.leverage, same_side, maintenance_margin_rate,
        )
    } else {
        PositionService::calculate_liquidation_price(
            est_price, req.leverage, same_side, maintenance_margin_rate,
        )
    };

    // VIP 阶梯：以 14d 滚动量落档，叠加 discount_multiplier（referral + staking）。
    let effective = crate::services::vip_tier::resolve(
        &state.db.pool,
        &state.vip_tier_event_sender,
        &user_address,
    ).await;
    let multiplier = crate::utils::fee_tiers::discount_multiplier(&user_address, true);
    let taker_fee_rate = crate::utils::fee_tiers::round_fee(effective.current.taker * multiplier);
    let maker_fee_rate = crate::utils::fee_tiers::round_fee(effective.current.maker * multiplier);
    let rate = if is_market_like { taker_fee_rate } else { maker_fee_rate };
    let est_fee = crate::utils::fee_tiers::round_fee(order_value * rate);

    // Available balance — DB 错误降级为 0 并记录。
    let available_balance: Decimal = match sqlx::query_scalar::<_, Decimal>(
        "SELECT COALESCE(available, 0) FROM balances WHERE user_address = $1 AND token = $2"
    )
    .bind(&user_address)
    .bind(collateral_symbol)
    .fetch_optional(&state.db.pool)
    .await
    {
        Ok(opt) => opt.unwrap_or(Decimal::ZERO),
        Err(e) => {
            tracing::warn!("preview balance query failed for {}: {}", user_address, e);
            Decimal::ZERO
        }
    };

    let required_margin = if is_closing {
        Decimal::ZERO
    } else {
        let buffer = new_margin * Decimal::new(5, 3);
        new_margin + buffer
    };

    let has_sufficient_balance = available_balance >= required_margin || is_closing;

    Ok(Json(OrderPreviewResponse {
        order_size: req.amount,
        order_size_symbol: format!("{} {}", req.amount, token_symbol),
        order_value,
        est_price,
        est_liq_price: est_liq_price.max(Decimal::ZERO),
        position_margin_before,
        position_margin_after,
        est_slippage,
        max_slippage: req.max_slippage.unwrap_or(DEFAULT_MAX_SLIPPAGE),
        taker_fee_rate,
        maker_fee_rate,
        est_fee,
        is_reduce_only: req.reduce_only || is_closing,
        available_balance,
        required_margin,
        has_sufficient_balance,
    }))
}

/// Calculate required margin for an order
async fn calculate_required_margin(req: &CreateOrderRequest, state: &Arc<AppState>) -> Decimal {
    // Get current price for market orders
    let price = match req.price {
        Some(p) => p,
        None => {
            // Use mark price for market orders
            state.price_feed_service
                .get_mark_price(&req.symbol)
                .await
                .unwrap_or(Decimal::ZERO)
        }
    };

    // Notional value = size * price
    let notional_value = req.amount * price;

    // Required margin = notional value / leverage
    // Add 0.5% buffer for fees and slippage
    let margin = notional_value / Decimal::from(req.leverage);
    let buffer = margin * Decimal::new(5, 3); // 0.5%

    margin + buffer
}

async fn calculate_required_margin_with_user(
    req: &CreateOrderRequest,
    state: &Arc<AppState>,
    user_address: &str,
) -> Decimal {
    use crate::models::PositionSide;

    // Get current price for market orders
    let price = match req.price {
        Some(p) => p,
        None => {
            // Use mark price for market orders
            state.price_feed_service
                .get_mark_price(&req.symbol)
                .await
                .unwrap_or(Decimal::ZERO)
        }
    };

    if price.is_zero() {
        // If no price available, use the old calculation
        return calculate_required_margin(req, state).await;
    }

    // Check if user has an opposite position (indicating this might be a closing order)
    let opposite_side = match req.side {
        OrderSide::Buy => PositionSide::Short,  // Buying closes short
        OrderSide::Sell => PositionSide::Long,  // Selling closes long
    };

    // Try to get the opposite position
    let existing_position = sqlx::query_as::<_, (Decimal, Decimal)>(
        r#"
        SELECT size_in_usd, size_in_tokens 
        FROM positions 
        WHERE user_address = $1 AND symbol = $2 AND side = $3 AND status = 'open'
        "#
    )
    .bind(user_address)
    .bind(&req.symbol)
    .bind(opposite_side)
    .fetch_optional(&state.db.pool)
    .await
    .ok()
    .flatten();

    if let Some((position_size_usd, _position_size_tokens)) = existing_position {
        // User has an opposite position - this order might be closing it
        let order_size_usd = req.amount * price;

        if order_size_usd <= position_size_usd {
            // Order is fully within the existing position - this is a pure close
            // No additional margin required! Just need to cover fees
            let fee_estimate = order_size_usd * Decimal::new(5, 3); // 0.5% for fees
            
            tracing::info!(
                "Order for {} {} {} @ {} is closing existing {} position (size: {} USD). Required margin (fees only): {}",
                req.amount, req.symbol, req.side, price, opposite_side, position_size_usd, fee_estimate
            );
            
            return fee_estimate;
        } else {
            // Order is larger than existing position - partially closing
            // Only require margin for the net new position
            let net_new_size_usd = order_size_usd - position_size_usd;
            let net_new_margin = net_new_size_usd / Decimal::from(req.leverage);
            let buffer = net_new_margin * Decimal::new(5, 3); // 0.5%
            
            tracing::info!(
                "Order for {} {} {} @ {} is partially closing {} position (existing: {} USD, new: {} USD). Required margin: {}",
                req.amount, req.symbol, req.side, price, opposite_side, position_size_usd, net_new_size_usd, net_new_margin + buffer
            );
            
            return net_new_margin + buffer;
        }
    }

    // No opposite position - this is a pure open/increase
    let notional_value = req.amount * price;
    let margin = notional_value / Decimal::from(req.leverage);
    let buffer = margin * Decimal::new(5, 3); // 0.5%

    margin + buffer
}

/// Modify an open order (price and/or amount).
/// PUT /orders/:order_id
///
/// Internally cancels the old order from the matching engine, updates the DB,
/// and re-submits with the new parameters. No EIP-712 re-signing required.
pub async fn update_order(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    headers: axum::http::HeaderMap,
    axum::extract::OriginalUri(original_uri): axum::extract::OriginalUri,
    Path(order_id): Path<Uuid>,
    Json(req): Json<crate::models::UpdateOrderRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    if let Some(resp) =
        maybe_forward_update_order(&state, &headers, &original_uri, order_id, &req).await?
    {
        return Ok(resp);
    }
    let user_addr = auth_user.address.to_lowercase();

    // 1. Fetch order + verify ownership
    let order: crate::models::Order = sqlx::query_as("SELECT * FROM orders WHERE id = $1")
        .bind(order_id)
        .fetch_optional(&state.db.pool)
        .await
        .map_err(|e| {
            tracing::error!("update_order: DB error: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: e.to_string(), code: "DB_ERROR".to_string() }))
        })?
        .ok_or_else(|| (StatusCode::NOT_FOUND, Json(ErrorResponse { error: "订单不存在".into(), code: "NOT_FOUND".into() })))?;

    if order.user_address.to_lowercase() != user_addr {
        return Err((StatusCode::FORBIDDEN, Json(ErrorResponse { error: "无权修改此订单".into(), code: "FORBIDDEN".into() })));
    }
    if order.status != crate::models::OrderStatus::Open && order.status != crate::models::OrderStatus::PartiallyFilled {
        return Err((StatusCode::BAD_REQUEST, Json(ErrorResponse { error: "仅 open/partially_filled 订单可修改".into(), code: "INVALID_STATUS".into() })));
    }

    let new_price = req.price.and_then(|p| p.parse::<rust_decimal::Decimal>().ok()).unwrap_or(order.price.unwrap_or_default());
    let new_amount = req.amount.and_then(|a| a.parse::<rust_decimal::Decimal>().ok()).unwrap_or(order.amount);

    if new_price <= rust_decimal::Decimal::ZERO {
        return Err((StatusCode::BAD_REQUEST, Json(ErrorResponse { error: "价格必须大于0".into(), code: "INVALID_PRICE".into() })));
    }

    // 2. Validate: new_amount must not be below what's already been filled.
    //    Otherwise re-submit would immediately put filled_amount > amount.
    if new_amount < order.filled_amount {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: format!(
                    "new amount {} < already filled {}",
                    new_amount, order.filled_amount
                ),
                code: "AMOUNT_BELOW_FILLED".into(),
            }),
        ));
    }

    // 3. Cancel from matching engine
    let _ = state.matching_engine.cancel_order(&order.symbol, order_id, &user_addr);

    // 4. Update DB
    sqlx::query("UPDATE orders SET price = $1, amount = $2, updated_at = NOW() WHERE id = $3")
        .bind(new_price)
        .bind(new_amount)
        .bind(order_id)
        .execute(&state.db.pool)
        .await
        .map_err(|e| {
            tracing::error!("update_order: failed to update DB: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: e.to_string(), code: "DB_ERROR".into() }))
        })?;

    // 5. Re-submit to matching engine — ONLY the unfilled portion.
    //
    // Historical bug: this call used to pass `new_amount` (the full total),
    // so the engine would put a fresh N-token maker into the book. When it
    // filled, persist_trade_with_tx incremented filled_amount by another N
    // on top of the old filled value, making filled = old_filled + N and
    // producing the exact-2× overfill we observed for XAU/XAG when the MM
    // bot PATCH-amended prices frequently. Passing remaining = new_amount -
    // filled fixes the semantic.
    let remaining_after_fills =
        (new_amount - order.filled_amount).max(rust_decimal::Decimal::ZERO);

    if remaining_after_fills > rust_decimal::Decimal::ZERO {
        let side = match order.side {
            crate::models::OrderSide::Buy => crate::services::matching::Side::Buy,
            crate::models::OrderSide::Sell => crate::services::matching::Side::Sell,
        };
        let me_order_type = match order.order_type {
            crate::models::OrderType::Limit => crate::services::matching::OrderType::Limit,
            _ => crate::services::matching::OrderType::Market,
        };

        // Resolve the user's current VIP rates for the resubmit (mirror
        // create_order). Update is an interactive action, so use `resolve`.
        let effective_tier = crate::services::vip_tier::resolve(
            &state.db.pool,
            &state.vip_tier_event_sender,
            &user_addr,
        ).await;
        let multiplier = crate::utils::fee_tiers::discount_multiplier(&user_addr, true);
        let taker_fee_rate =
            crate::utils::fee_tiers::round_fee(effective_tier.current.taker * multiplier);
        let maker_fee_rate =
            crate::utils::fee_tiers::round_fee(effective_tier.current.maker * multiplier);

        let match_result = state.matching_engine.submit_order(
            order_id,
            &order.symbol,
            &user_addr,
            side,
            me_order_type,
            remaining_after_fills,
            Some(new_price),
            MatchingTimeInForce::GTC,
            order.leverage as u32,
            taker_fee_rate,
            maker_fee_rate,
        );

        // Persist the taker side of the re-submit absolutely, mirroring
        // create_order (order.rs:737). The async trade-persistence worker
        // (orchestrator.rs:605) only updates the maker, so if the amended
        // price crosses the market and immediately fills, no one writes
        // back the taker's new filled_amount / status — the row sits at the
        // pre-amend filled_amount with status='open' forever even though
        // trades exist. 2026-04-21 alone produced 532 such orphans from MM
        // PATCH-churn.
        //
        // filled_amount is incremented on top of `order.filled_amount`
        // (the pre-PATCH accumulated fills), NOT set absolutely: the
        // matching engine only saw `remaining_after_fills`, so
        // match_result.filled_amount is the delta from this re-submit
        // alone. Total must include prior fills.
        match match_result {
            Ok(mr) => {
                let new_filled = order.filled_amount + mr.filled_amount;
                let new_status_str = if new_filled >= new_amount {
                    "filled"
                } else if new_filled > rust_decimal::Decimal::ZERO {
                    "partially_filled"
                } else {
                    "open"
                };

                if let Err(e) = sqlx::query(
                    "UPDATE orders SET status = $1::order_status, filled_amount = $2, updated_at = NOW() WHERE id = $3"
                )
                .bind(new_status_str)
                .bind(new_filled)
                .bind(order_id)
                .execute(&state.db.pool)
                .await
                {
                    tracing::error!(
                        "update_order: re-submit UPDATE failed for {}: {}",
                        order_id, e
                    );
                }
            }
            Err(e) => {
                tracing::error!(
                    "update_order: re-submit to matching engine failed for {}: {}",
                    order_id, e
                );
            }
        }
    }

    tracing::info!("Order {} modified: price={}, amount={}", order_id, new_price, new_amount);

    Ok(Json(serde_json::json!({
        "order_id": order_id.to_string(),
        "status": "open",
        "price": new_price.to_string(),
        "amount": new_amount.to_string(),
        "updated_at": chrono::Utc::now().timestamp_millis()
    })))
}

/// Cancel a single order
/// DELETE /orders/:order_id
///
/// Body 是可选的 —— 对于 API Key (HMAC) 认证路径，签名已经在 middleware
/// 验过，body 里的 signature/timestamp 只对 JWT 钱包认证路径需要。
/// 之前强制要求 body，客户端不发或发无效 body 都会被 axum 的 Json
/// extractor 拦成 400「EOF while parsing a value」，违反 Binance 惯例
/// （DELETE 无需 body）。改成 `Option<Json<_>>` 允许空 body。
pub async fn cancel_order(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    headers: axum::http::HeaderMap,
    axum::extract::OriginalUri(original_uri): axum::extract::OriginalUri,
    Path(order_id): Path<Uuid>,
    body: Option<Json<CancelOrderRequest>>,
) -> Result<Json<OrderResponse>, (StatusCode, Json<ErrorResponse>)> {
    let req = body.map(|Json(r)| r).unwrap_or_default();

    // Symbol-shard routing: PK-lookup the symbol, then forward if owned
    // by another pod. Receiving pod re-runs auth + EIP-712 verify
    // against the same body.
    if let Some(resp) = maybe_forward_cancel_order_jwt(
        &state,
        &headers,
        &original_uri,
        order_id,
        Some(&req),
    )
    .await?
    {
        return Ok(resp);
    }

    // EIP-712 签名验证 — API Key 认证跳过签名
    if auth_user.is_api_key {
        tracing::info!("Cancel order via API Key auth (skip EIP-712) for order: {}", order_id);
    } else if !state.config.is_auth_disabled() {
        let signature = req.signature.as_deref().ok_or_else(|| (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "缺少 signature 字段".to_string(),
                code: "MISSING_SIGNATURE".to_string(),
            }),
        ))?;
        let timestamp = req.timestamp.ok_or_else(|| (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "缺少 timestamp 字段".to_string(),
                code: "MISSING_TIMESTAMP".to_string(),
            }),
        ))?;

        if !validate_timestamp(timestamp) {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "时间戳已过期".to_string(),
                    code: "TIMESTAMP_EXPIRED".to_string(),
                }),
            ));
        }

        let cancel_msg = CancelOrderMessage {
            wallet: auth_user.address.to_lowercase(),
            order_id: order_id.to_string(),
            timestamp,
        };

        let valid = match verify_cancel_order_signature(&cancel_msg, signature, &auth_user.address) {
            Ok(v) => v,
            Err(e) => {
                tracing::error!("Cancel order signature verification error: {}", e);
                return Err((
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: "签名格式无效".to_string(),
                        code: "INVALID_SIGNATURE_FORMAT".to_string(),
                    }),
                ));
            }
        };

        if !valid {
            tracing::warn!("Cancel order signature verification failed for order: {}", order_id);
            return Err((
                StatusCode::UNAUTHORIZED,
                Json(ErrorResponse {
                    error: "取消订单签名验证失败".to_string(),
                    code: "SIGNATURE_INVALID".to_string(),
                }),
            ));
        }

        tracing::info!("EIP-712 cancel order signature verified for order: {}", order_id);
    }

    // Check order exists and belongs to user
    let order: Option<Order> = sqlx::query_as(
        "SELECT * FROM orders WHERE id = $1"
    )
    .bind(order_id)
    .fetch_optional(&state.db.pool)
    .await
    .map_err(|e| {
        tracing::error!("Failed to fetch order: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "查询订单失败".to_string(),
                code: "ORDER_FETCH_FAILED".to_string(),
            }),
        )
    })?;

    let order = order.ok_or((
        StatusCode::NOT_FOUND,
        Json(ErrorResponse {
            error: "订单不存在".to_string(),
            code: "ORDER_NOT_FOUND".to_string(),
        }),
    ))?;

    if order.user_address.to_lowercase() != auth_user.address.to_lowercase() {
        return Err((
            StatusCode::FORBIDDEN,
            Json(ErrorResponse {
                error: "无权取消此订单".to_string(),
                code: "ORDER_NOT_OWNED".to_string(),
            }),
        ));
    }

    if order.status != OrderStatus::Open && order.status != OrderStatus::PartiallyFilled && order.status != OrderStatus::Pending {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: format!("订单状态为 {:?}，无法取消", order.status),
                code: "ORDER_NOT_CANCELLABLE".to_string(),
            }),
        ));
    }

    // Cancel in matching engine
    let cancelled = state.matching_engine.cancel_order(&order.symbol, order_id, &auth_user.address.to_lowercase()).map_err(|e| {
        tracing::error!("Failed to cancel order in matching engine: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "撮合引擎取消订单失败".to_string(),
                code: "MATCHING_CANCEL_FAILED".to_string(),
            }),
        )
    })?;

    if !cancelled {
        // Order might already be filled, update from DB
        tracing::warn!("Order {} not found in matching engine", order_id);
    }

    // Update database
    sqlx::query("UPDATE orders SET status = 'cancelled' WHERE id = $1")
        .bind(order_id)
        .execute(&state.db.pool)
        .await
        .map_err(|e| {
            tracing::error!("Failed to update order status: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "更新订单状态失败".to_string(),
                    code: "ORDER_UPDATE_FAILED".to_string(),
                }),
            )
        })?;

    // If this parent order is linked to a trigger_orders row, cascade the
    // cancellation. Without this the parent moves to `cancelled` but the
    // trigger row stays `active`, and the keeper will still fire it once
    // the price condition is met — silently creating a fresh order the
    // user thought was cancelled. Also pull the trigger_price so the refund
    // fallback below has a usable price for *_MARKET trigger orders, whose
    // parent rows have NULL `price`.
    let trigger_link: Option<(Uuid, Decimal)> = sqlx::query_as(
        "SELECT t.id, t.trigger_price \
         FROM orders o \
         JOIN trigger_orders t ON t.id = o.trigger_order_id \
         WHERE o.id = $1",
    )
    .bind(order_id)
    .fetch_optional(&state.db.pool)
    .await
    .ok()
    .flatten();

    if let Some((trigger_id, _)) = trigger_link {
        let r = sqlx::query(
            "UPDATE trigger_orders SET status = 'cancelled', updated_at = NOW() \
             WHERE id = $1 AND status = 'active'",
        )
        .bind(trigger_id)
        .execute(&state.db.pool)
        .await;
        match r {
            Ok(res) if res.rows_affected() > 0 => {
                tracing::info!(
                    "Cascade-cancelled trigger_orders {} for parent {}",
                    trigger_id, order_id
                );
            }
            Ok(_) => {
                // Already triggered/executed/cancelled — no-op.
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to cascade-cancel trigger_orders {} for parent {}: {}",
                    trigger_id, order_id, e
                );
            }
        }
    }

    // Unfreeze remaining margin - use frozen_margin from order if available
    let collateral_symbol = state.config.collateral_symbol();

    // Calculate the margin to release based on the proportion not filled
    let fill_ratio = if order.amount.is_zero() {
        Decimal::ZERO
    } else {
        (order.amount - order.filled_amount) / order.amount
    };

    // Use frozen_margin from order record if available, otherwise calculate
    let remaining_margin = if let Some(frozen) = order.frozen_margin {
        if frozen > Decimal::ZERO {
            frozen * fill_ratio
        } else {
            Decimal::ZERO
        }
    } else {
        // Fallback: calculate margin (for legacy orders without frozen_margin)
        // But only if we're not in a state where this could cause negative frozen
        let remaining_amount = order.amount - order.filled_amount;
        // For *_MARKET trigger orders the parent row has NULL price; the
        // useful anchor in that case is the linked trigger's trigger_price.
        // Without this, cancelling a STOP_MARKET / TAKE_PROFIT_MARKET refunded
        // 0 and leaked the full freeze.
        let price = order
            .price
            .or_else(|| trigger_link.as_ref().map(|(_, tp)| *tp))
            .unwrap_or(Decimal::ZERO);

        if price.is_zero() {
            Decimal::ZERO
        } else {
            let notional_value = remaining_amount * price;
            let base_margin = notional_value / Decimal::from(order.leverage);
            let buffer = base_margin * Decimal::new(5, 3); // 0.5% buffer
            base_margin + buffer
        }
    };

    // Only update balance if there's margin to release
    if remaining_margin > Decimal::ZERO {
        sqlx::query(
            "UPDATE balances SET available = available + $1, frozen = GREATEST(frozen - $1, 0) WHERE user_address = $2 AND token = $3"
        )
        .bind(remaining_margin)
        .bind(&auth_user.address.to_lowercase())
        .bind(collateral_symbol)
        .execute(&state.db.pool)
        .await
        .ok();

        tracing::info!(
            "Released margin {} for cancelled order {} (frozen_margin: {:?})",
            remaining_margin, order_id, order.frozen_margin
        );
    }

    tracing::info!("Order cancelled: {} by {}", order_id, auth_user.address);

    let price = order.price.unwrap_or(Decimal::ZERO);
    let size = order.amount * price;

    Ok(Json(OrderResponse {
        order_id,
        symbol: order.symbol,
        side: order.side,
        order_type: order.order_type,
        price,
        size,
        amount: order.amount,
        filled_amount: order.filled_amount,
        remaining_amount: order.amount - order.filled_amount,
        leverage: order.leverage,
        status: OrderStatus::Cancelled,
        created_at: order.created_at,
        reduce_only: order.reduce_only,
        trigger_price: order.trigger_price,
    }))
}

/// Batch cancel orders
/// POST /orders/batch
pub async fn batch_cancel(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    headers: axum::http::HeaderMap,
    axum::extract::OriginalUri(original_uri): axum::extract::OriginalUri,
    Json(req): Json<BatchCancelRequest>,
) -> Result<Json<BatchCancelResponse>, (StatusCode, Json<ErrorResponse>)> {
    // Symbol-shard routing: bulk-resolve owners. Forward whole batch to
    // a single non-self owner; locally execute on all-local or
    // multi-owner-split (warn).
    if let Some(resp) =
        maybe_forward_batch_cancel(&state, &headers, &original_uri, &req).await?
    {
        return Ok(resp);
    }

    // EIP-712 签名验证 — API Key 认证跳过签名
    if auth_user.is_api_key {
        tracing::info!("Batch cancel via API Key auth (skip EIP-712) for {} orders", req.order_ids.len());
    } else if !state.config.is_auth_disabled() {
        let signature = req.signature.as_deref().ok_or_else(|| (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "缺少 signature 字段".to_string(),
                code: "MISSING_SIGNATURE".to_string(),
            }),
        ))?;
        let timestamp = req.timestamp.ok_or_else(|| (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "缺少 timestamp 字段".to_string(),
                code: "MISSING_TIMESTAMP".to_string(),
            }),
        ))?;

        if !validate_timestamp(timestamp) {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "时间戳已过期".to_string(),
                    code: "TIMESTAMP_EXPIRED".to_string(),
                }),
            ));
        }

        let batch_cancel_msg = BatchCancelMessage {
            wallet: auth_user.address.to_lowercase(),
            order_ids: req.order_ids.iter().map(|id| id.to_string()).collect::<Vec<_>>().join(","),
            timestamp,
        };

        let valid = match verify_batch_cancel_signature(&batch_cancel_msg, signature, &auth_user.address) {
            Ok(v) => v,
            Err(e) => {
                tracing::error!("Batch cancel signature verification error: {}", e);
                return Err((
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: "签名格式无效".to_string(),
                        code: "INVALID_SIGNATURE_FORMAT".to_string(),
                    }),
                ));
            }
        };

        if !valid {
            tracing::warn!("Batch cancel signature verification failed for {} orders", req.order_ids.len());
            return Err((
                StatusCode::UNAUTHORIZED,
                Json(ErrorResponse {
                    error: "批量取消订单签名验证失败".to_string(),
                    code: "SIGNATURE_INVALID".to_string(),
                }),
            ));
        }

        tracing::info!("EIP-712 batch cancel signature verified for {} orders", req.order_ids.len());
    }

    let mut cancelled = Vec::new();
    let mut failed = Vec::new();
    let mut total_margin_to_release = Decimal::ZERO;

    for order_id in req.order_ids {
        // Check order ownership
        let order: Option<Order> = sqlx::query_as(
            "SELECT * FROM orders WHERE id = $1 AND user_address = $2 AND status IN ('open', 'partially_filled', 'pending')"
        )
        .bind(order_id)
        .bind(&auth_user.address.to_lowercase())
        .fetch_optional(&state.db.pool)
        .await
        .ok()
        .flatten();

        if let Some(order) = order {
            // Try to cancel in matching engine
            let result = state.matching_engine.cancel_order(&order.symbol, order_id, &auth_user.address.to_lowercase());

            // Update database regardless
            let db_result = sqlx::query("UPDATE orders SET status = 'cancelled' WHERE id = $1")
                .bind(order_id)
                .execute(&state.db.pool)
                .await;

            if result.is_ok() || db_result.is_ok() {
                // Cascade: if this parent is linked to a trigger_orders row,
                // cancel that row and remember its trigger_price for the
                // refund fallback below. See cancel_order at line ~1700 for
                // the rationale (without this, the keeper still fires the
                // trigger after the user thinks it's gone, and *_MARKET
                // triggers refund 0 because the parent's `price` is NULL).
                let trigger_link: Option<(Uuid, Decimal)> = sqlx::query_as(
                    "SELECT t.id, t.trigger_price \
                     FROM orders o \
                     JOIN trigger_orders t ON t.id = o.trigger_order_id \
                     WHERE o.id = $1",
                )
                .bind(order_id)
                .fetch_optional(&state.db.pool)
                .await
                .ok()
                .flatten();

                if let Some((trigger_id, _)) = trigger_link {
                    let _ = sqlx::query(
                        "UPDATE trigger_orders SET status = 'cancelled', updated_at = NOW() \
                         WHERE id = $1 AND status = 'active'",
                    )
                    .bind(trigger_id)
                    .execute(&state.db.pool)
                    .await;
                }

                // Calculate margin to release
                let fill_ratio = if order.amount.is_zero() {
                    Decimal::ZERO
                } else {
                    (order.amount - order.filled_amount) / order.amount
                };

                // Use frozen_margin from order record if available; otherwise
                // recompute from the order shape (mirrors the fallback in the
                // single-cancel handler at order.rs:1711). The single-cancel
                // path has had this fallback for legacy orders for some time;
                // batch_cancel previously returned ZERO in the same branch,
                // which is why /fapi/v1/order rows (which never populate
                // `orders.frozen_margin`) leaked their full freeze on
                // batch_cancel — observed on prod 2026-04-26 when 3 fapi
                // LIMITs at 75k/74k/73k × 0.001 lev=100 were batch-cancelled
                // and 2.23 USDT stayed frozen indefinitely.
                let remaining_margin = if let Some(frozen) = order.frozen_margin {
                    if frozen > Decimal::ZERO {
                        frozen * fill_ratio
                    } else {
                        Decimal::ZERO
                    }
                } else {
                    let remaining_amount = order.amount - order.filled_amount;
                    // For *_MARKET trigger orders the parent `price` is NULL;
                    // fall through to the linked trigger_price as the anchor.
                    let price = order
                        .price
                        .or_else(|| trigger_link.as_ref().map(|(_, tp)| *tp))
                        .unwrap_or(Decimal::ZERO);
                    if price.is_zero() || order.leverage == 0 {
                        // Market-like or otherwise pathological row — refunding
                        // a guessed amount risks negative frozen. Stay safe.
                        Decimal::ZERO
                    } else {
                        let notional_value = remaining_amount * price;
                        let base_margin = notional_value / Decimal::from(order.leverage);
                        let buffer = base_margin * Decimal::new(5, 3); // 0.5% buffer
                        base_margin + buffer
                    }
                };

                total_margin_to_release += remaining_margin;
                cancelled.push(order_id);
            } else {
                failed.push(order_id);
            }
        } else {
            failed.push(order_id);
        }
    }

    // Perform single balance update if needed
    if total_margin_to_release > Decimal::ZERO {
        let collateral_symbol = state.config.collateral_symbol();
        sqlx::query(
            "UPDATE balances SET available = available + $1, frozen = GREATEST(frozen - $1, 0) WHERE user_address = $2 AND token = $3"
        )
        .bind(total_margin_to_release)
        .bind(&auth_user.address.to_lowercase())
        .bind(collateral_symbol)
        .execute(&state.db.pool)
        .await
        .ok();
        
        tracing::info!(
            "Batch cancel released total margin {} for {} orders",
            total_margin_to_release,
            cancelled.len()
        );
    }

    tracing::info!(
        "Batch cancel: {} cancelled, {} failed by {}",
        cancelled.len(),
        failed.len(),
        auth_user.address
    );

    Ok(Json(BatchCancelResponse { cancelled, failed }))


}

/// List the calling user's orders. By default returns open + partially_filled
/// orders only; pass `?status=all` to include terminal states (filled,
/// cancelled). Optional `?symbol=BTCUSDT` filter. Used by both
///   GET /api/v1/orders        — generic listing (was 405 before R4 P0 #6)
///   GET /api/v1/orders/open   — explicit "active" alias (route-collision fix
///                                for R4 P1 #20: previously parsed as
///                                `:order_id="open"` and 400'd UUID parse).
#[derive(Debug, serde::Deserialize)]
pub struct ListOrdersQuery {
    pub symbol: Option<String>,
    pub status: Option<String>,
    pub limit: Option<i64>,
}

pub async fn list_orders(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    axum::extract::Query(q): axum::extract::Query<ListOrdersQuery>,
) -> Result<Json<Vec<OrderResponse>>, (StatusCode, Json<ErrorResponse>)> {
    let user_addr = auth_user.address.to_lowercase();
    let limit = q.limit.unwrap_or(200).clamp(1, 1000);

    // status=all → no status filter; default → only active (open / partial)
    let want_all = q
        .status
        .as_deref()
        .map(|s| s.eq_ignore_ascii_case("all"))
        .unwrap_or(false);

    let rows: Vec<Order> = if want_all {
        match q.symbol {
            Some(sym) => sqlx::query_as(
                "SELECT * FROM orders WHERE user_address = $1 AND symbol = $2 \
                 ORDER BY created_at DESC LIMIT $3",
            )
            .bind(&user_addr)
            .bind(sym.to_uppercase())
            .bind(limit)
            .fetch_all(&state.db.pool)
            .await,
            None => sqlx::query_as(
                "SELECT * FROM orders WHERE user_address = $1 \
                 ORDER BY created_at DESC LIMIT $2",
            )
            .bind(&user_addr)
            .bind(limit)
            .fetch_all(&state.db.pool)
            .await,
        }
    } else {
        match q.symbol {
            Some(sym) => sqlx::query_as(
                "SELECT * FROM orders WHERE user_address = $1 AND symbol = $2 \
                 AND status IN ('open', 'partially_filled', 'pending') \
                 ORDER BY created_at DESC LIMIT $3",
            )
            .bind(&user_addr)
            .bind(sym.to_uppercase())
            .bind(limit)
            .fetch_all(&state.db.pool)
            .await,
            None => sqlx::query_as(
                "SELECT * FROM orders WHERE user_address = $1 \
                 AND status IN ('open', 'partially_filled', 'pending') \
                 ORDER BY created_at DESC LIMIT $2",
            )
            .bind(&user_addr)
            .bind(limit)
            .fetch_all(&state.db.pool)
            .await,
        }
    }
    .map_err(|e| {
        tracing::error!("Failed to list orders: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "Failed to list orders".to_string(),
                code: "ORDER_LIST_FAILED".to_string(),
            }),
        )
    })?;

    let responses: Vec<OrderResponse> = rows.into_iter().map(OrderResponse::from).collect();
    Ok(Json(responses))
}

/// Get a single order by ID
/// GET /orders/:order_id
pub async fn get_order(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path(order_id): Path<Uuid>,
) -> Result<Json<OrderResponse>, (StatusCode, Json<ErrorResponse>)> {
    // Fetch order from database
    let order: Option<Order> = sqlx::query_as(
        "SELECT * FROM orders WHERE id = $1"
    )
    .bind(order_id)
    .fetch_optional(&state.db.pool)
    .await
    .map_err(|e| {
        tracing::error!("Failed to fetch order: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "查询订单失败".to_string(),
                code: "ORDER_FETCH_FAILED".to_string(),
            }),
        )
    })?;

    let order = order.ok_or((
        StatusCode::NOT_FOUND,
        Json(ErrorResponse {
            error: "订单不存在".to_string(),
            code: "ORDER_NOT_FOUND".to_string(),
        }),
    ))?;

    // Check if user owns the order
    if order.user_address.to_lowercase() != auth_user.address.to_lowercase() {
        return Err((
            StatusCode::FORBIDDEN,
            Json(ErrorResponse {
                error: "无权访问此订单".to_string(),
                code: "ORDER_ACCESS_DENIED".to_string(),
            }),
        ));
    }

    let price = order.price.unwrap_or(Decimal::ZERO);
    let size = order.amount * price;

    Ok(Json(OrderResponse {
        order_id: order.id,
        symbol: order.symbol,
        side: order.side,
        order_type: order.order_type,
        price,
        size,
        amount: order.amount,
        filled_amount: order.filled_amount,
        remaining_amount: order.amount - order.filled_amount,
        leverage: order.leverage,
        status: order.status,
        created_at: order.created_at,
        reduce_only: order.reduce_only,
        trigger_price: order.trigger_price,
    }))
}
