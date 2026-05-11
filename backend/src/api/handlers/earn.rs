//! Earn Service API handlers
//!
//! Handles fixed-term financial products (理财服务) API requests.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Extension, Json,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::auth::middleware::AuthUser;
use crate::cache::keys::{CacheKey, ttl};
use crate::services::earn::{
    EarnProductStatus, ProductListResponse, ProductDetail, UserSubscriptionDetail,
    PrepareJoinPlanResponse, PrepareJoinPlanRequest, HistoricalPerformance, CreateProductRequest,
    UpdateProductStatusRequest, AdminSubscriptionQuery, AdminSubscriptionListResponse,
};
use crate::AppState;

// ============================================
// EARN EIP-712 DOMAIN CONSTANTS
// ============================================
// Re-exported from the central pin module so a brand-rename sed cannot
// silently break the ZtdxTermYield contract handshake. See
// constants/eip712_domains.rs — DO NOT RENAME.

use crate::constants::eip712_domains::{
    earn_domain_name, domain_version,
};

// ============================================
// REQUEST/RESPONSE TYPES
// ============================================

/// Query parameters for product list
#[derive(Debug, Deserialize)]
pub struct ProductListQuery {
    pub status: Option<String>,
    #[serde(default = "default_page")]
    pub page: i32,
    #[serde(default = "default_page_size")]
    pub page_size: i32,
}

fn default_page() -> i32 { 1 }
fn default_page_size() -> i32 { 20 }

/// Query parameters for historical performance
#[derive(Debug, Deserialize)]
pub struct PerformanceQuery {
    #[serde(default = "default_limit")]
    pub limit: i32,
}

fn default_limit() -> i32 { 10 }

/// Generic success response
#[derive(Debug, Serialize)]
pub struct SuccessResponse {
    pub success: bool,
    pub message: Option<String>,
}

/// Error response
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub code: String,
}

/// EIP-712 Domain response for Earn contract
#[derive(Debug, Serialize)]
pub struct EarnDomainResponse {
    pub types: serde_json::Value,
    #[serde(rename = "primaryType")]
    pub primary_type: String,
    pub domain: EarnDomain,
}

/// Earn contract EIP-712 domain
#[derive(Debug, Serialize)]
pub struct EarnDomain {
    pub name: String,
    pub version: String,
    #[serde(rename = "chainId")]
    pub chain_id: u64,
    #[serde(rename = "verifyingContract")]
    pub verifying_contract: String,
}

// ============================================
// PUBLIC ENDPOINTS
// ============================================

/// GET /api/v1/earn/domain
/// Get EIP-712 domain information for Earn contract
/// Used by frontend to construct typed data for signing
pub async fn get_domain(
    State(state): State<Arc<AppState>>,
) -> Json<EarnDomainResponse> {
    let contract_address = state.earn_service.get_contract_address();
    let chain_id = state.config.chain_id;

    Json(EarnDomainResponse {
        types: serde_json::json!({
            "EIP712Domain": [
                { "name": "name", "type": "string" },
                { "name": "version", "type": "string" },
                { "name": "chainId", "type": "uint256" },
                { "name": "verifyingContract", "type": "address" }
            ],
            "JoinPlan": [
                { "name": "account", "type": "address" },
                { "name": "planId", "type": "uint256" },
                { "name": "principalAmount", "type": "uint256" },
                { "name": "deadline", "type": "uint256" }
            ]
        }),
        primary_type: "JoinPlan".to_string(),
        domain: EarnDomain {
            name: earn_domain_name().to_string(),
            version: domain_version().to_string(),
            chain_id,
            verifying_contract: contract_address.to_string(),
        },
    })
}

/// GET /api/v1/earn/products
/// List all products (with optional status filter)
pub async fn list_products(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ProductListQuery>,
) -> Result<Json<ProductListResponse>, (StatusCode, Json<ErrorResponse>)> {
    let status = query.status.and_then(|s| match s.to_lowercase().as_str() {
        "created" => Some(EarnProductStatus::Created),
        "subscribing" => Some(EarnProductStatus::Subscribing),
        "active" => Some(EarnProductStatus::Active),
        "settled" => Some(EarnProductStatus::Settled),
        "ended" => Some(EarnProductStatus::Ended),
        "cancelled" => Some(EarnProductStatus::Cancelled),
        _ => None,
    });

    match state.earn_service.list_plans(status, query.page, query.page_size).await {
        Ok(response) => Ok(Json(response)),
        Err(e) => {
            tracing::error!("Failed to list products: {:?}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Failed to list products".to_string(),
                    code: "INTERNAL_ERROR".to_string(),
                }),
            ))
        }
    }
}

/// GET /api/v1/earn/products/:id
/// Get product details
pub async fn get_product(
    State(state): State<Arc<AppState>>,
    Path(product_id): Path<String>,
) -> Result<Json<ProductDetail>, (StatusCode, Json<ErrorResponse>)> {
    match state.earn_service.get_plan(&product_id).await {
        Ok(product) => Ok(Json(product)),
        Err(e) => {
            let error_msg = e.to_string();
            if error_msg.contains("not found") {
                Err((
                    StatusCode::NOT_FOUND,
                    Json(ErrorResponse {
                        error: "Product not found".to_string(),
                        code: "PRODUCT_NOT_FOUND".to_string(),
                    }),
                ))
            } else {
                tracing::error!("Failed to get product: {:?}", e);
                Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: "Failed to get product".to_string(),
                        code: "INTERNAL_ERROR".to_string(),
                    }),
                ))
            }
        }
    }
}

/// GET /api/v1/earn/performance
/// Get historical performance of ended products
/// Results are cached in Redis for 60 seconds to reduce DB load.
pub async fn get_performance(
    State(state): State<Arc<AppState>>,
    Query(query): Query<PerformanceQuery>,
) -> Result<Json<Vec<HistoricalPerformance>>, (StatusCode, Json<ErrorResponse>)> {
    let cache_key = CacheKey::earn_performance(query.limit);

    // Try Redis cache first
    if let Some(redis) = state.cache.redis() {
        if let Ok(Some(cached)) = redis.get::<String>(&cache_key).await {
            if let Ok(data) = serde_json::from_str::<Vec<HistoricalPerformance>>(&cached) {
                return Ok(Json(data));
            }
        }
    }

    // Cache miss: query DB
    match state.earn_service.get_historical_performance(query.limit).await {
        Ok(history) => {
            // Write result back to Redis with TTL
            if let Some(redis) = state.cache.redis() {
                if let Ok(json) = serde_json::to_string(&history) {
                    let _ = redis.set_ex(&cache_key, json, ttl::EARN_PERFORMANCE).await;
                }
            }
            Ok(Json(history))
        }
        Err(e) => {
            tracing::error!("Failed to get performance history: {:?}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Failed to get performance history".to_string(),
                    code: "INTERNAL_ERROR".to_string(),
                }),
            ))
        }
    }
}

// ============================================
// PROTECTED ENDPOINTS (Auth Required)
// ============================================

/// GET /api/v1/earn/subscriptions
/// Get user's earn plan positions
pub async fn get_positions(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
) -> Result<Json<Vec<UserSubscriptionDetail>>, (StatusCode, Json<ErrorResponse>)> {
    match state.earn_service.get_user_positions(&auth_user.address).await {
        Ok(subscriptions) => Ok(Json(subscriptions)),
        Err(e) => {
            tracing::error!("Failed to get user subscriptions: {:?}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Failed to get subscriptions".to_string(),
                    code: "INTERNAL_ERROR".to_string(),
                }),
            ))
        }
    }
}

/// POST /api/v1/earn/subscribe/prepare
/// Prepare join plan - returns EIP-712 JoinPlan signature for on-chain transaction
pub async fn prepare_join_plan(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Json(req): Json<PrepareJoinPlanRequest>,
) -> Result<Json<PrepareJoinPlanResponse>, (StatusCode, Json<ErrorResponse>)> {
    // Parse amount
    let amount = match rust_decimal::Decimal::from_str_exact(&req.amount) {
        Ok(a) => a,
        Err(_) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "Invalid amount format".to_string(),
                    code: "INVALID_AMOUNT".to_string(),
                }),
            ));
        }
    };

    if amount <= rust_decimal::Decimal::ZERO {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "Amount must be positive".to_string(),
                code: "INVALID_AMOUNT".to_string(),
            }),
        ));
    }

    match state.earn_service.prepare_join_plan(&auth_user.address, &req.product_id, amount).await {
        Ok(response) => Ok(Json(response)),
        Err(e) => {
            let error_msg = e.to_string();

            let (status, code) = if error_msg.contains("not found") {
                (StatusCode::NOT_FOUND, "PRODUCT_NOT_FOUND")
            } else if error_msg.contains("not open") || error_msg.contains("not active") {
                (StatusCode::BAD_REQUEST, "SUBSCRIPTION_CLOSED")
            } else if error_msg.contains("minimum") {
                (StatusCode::BAD_REQUEST, "AMOUNT_TOO_LOW")
            } else if error_msg.contains("limit") || error_msg.contains("Exceeds") {
                (StatusCode::BAD_REQUEST, "EXCEEDS_LIMIT")
            } else if error_msg.contains("quota") || error_msg.contains("Insufficient") {
                (StatusCode::BAD_REQUEST, "INSUFFICIENT_QUOTA")
            } else if error_msg.contains("Signer") {
                (StatusCode::SERVICE_UNAVAILABLE, "SERVICE_NOT_CONFIGURED")
            } else {
                (StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL_ERROR")
            };

            tracing::error!("Failed to prepare join plan signature: {:?}", e);
            Err((
                status,
                Json(ErrorResponse {
                    error: error_msg,
                    code: code.to_string(),
                }),
            ))
        }
    }
}

// ============================================
// ADMIN ENDPOINTS (API Key Required)
// ============================================

/// POST /api/v1/admin/earn/products
/// Create a new earn plan (admin only)
/// First creates the plan on-chain via ZtdxTermYield, then saves to database
pub async fn admin_create_plan(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateProductRequest>,
) -> Result<Json<ProductDetail>, (StatusCode, Json<ErrorResponse>)> {
    let chain_product_id = chrono::Utc::now().timestamp_millis();
    let creator_address = "0x0000000000000000000000000000000000000000"; // Admin address

    // Parse amounts for on-chain call (same parsing as create_plan service)
    let total_quota = rust_decimal::Decimal::from_str_exact(&req.total_quota).map_err(|e| {
        (StatusCode::BAD_REQUEST, Json(ErrorResponse {
            error: format!("Invalid total_quota: {}", e),
            code: "INVALID_PARAM".to_string(),
        }))
    })?;
    let min_amount = rust_decimal::Decimal::from_str_exact(&req.min_amount).map_err(|e| {
        (StatusCode::BAD_REQUEST, Json(ErrorResponse {
            error: format!("Invalid min_amount: {}", e),
            code: "INVALID_PARAM".to_string(),
        }))
    })?;
    let max_amount_per_user = rust_decimal::Decimal::from_str_exact(&req.max_amount_per_user).map_err(|e| {
        (StatusCode::BAD_REQUEST, Json(ErrorResponse {
            error: format!("Invalid max_amount_per_user: {}", e),
            code: "INVALID_PARAM".to_string(),
        }))
    })?;

    // Step 1: Create plan on-chain
    if let Err(e) = state.earn_service.call_create_plan(
        chain_product_id,
        &req.name,
        req.annual_rate_bps,
        req.duration_seconds,
        total_quota,
        min_amount,
        max_amount_per_user,
        req.subscribe_start_time,
        req.subscribe_end_time,
    ).await {
        tracing::error!("Failed to create plan on-chain: {:?}", e);
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("Failed to create plan on-chain: {}", e),
                code: "ONCHAIN_CREATE_FAILED".to_string(),
            }),
        ));
    }

    tracing::info!("Plan created on-chain with id={}", chain_product_id);

    // Step 2: Always try to call openPlan right after creation
    // This transitions the contract from Created -> Subscribing
    // If start_time hasn't arrived yet, the contract may reject it (that's OK, scheduler retries)
    let opened = match state.earn_service.call_open_plan(chain_product_id).await {
        Ok(tx_hash) => {
            tracing::info!("Plan {} openPlan tx: {:?}", chain_product_id, tx_hash);
            true
        }
        Err(e) => {
            tracing::warn!("openPlan not ready yet for plan {} (scheduler will retry): {}", chain_product_id, e);
            false
        }
    };

    // Step 3: Save to database
    match state.earn_service.create_plan(req, creator_address, chain_product_id).await {
        Ok(mut product) => {
            // If openSubscription succeeded on-chain, sync DB status
            if opened {
                let _ = state.earn_service.update_plan_status(
                    &product.id.to_string(),
                    crate::services::earn::EarnProductStatus::Subscribing,
                ).await;
                product.status = crate::services::earn::EarnProductStatus::Subscribing;
            }
            let detail = ProductDetail::from_product(product);
            Ok(Json(detail))
        }
        Err(e) => {
            tracing::error!("Failed to save plan to database (on-chain plan {} already created): {:?}", chain_product_id, e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("Plan created on-chain (id={}) but failed to save to database: {}", chain_product_id, e),
                    code: "DB_CREATE_FAILED".to_string(),
                }),
            ))
        }
    }
}

/// POST /api/v1/admin/earn/products/:id/status
/// Update product status (admin only)
pub async fn admin_update_status(
    State(state): State<Arc<AppState>>,
    Path(product_id): Path<String>,
    Json(req): Json<UpdateProductStatusRequest>,
) -> Result<Json<ProductDetail>, (StatusCode, Json<ErrorResponse>)> {
    let new_status = match req.status.to_lowercase().as_str() {
        "created" => EarnProductStatus::Created,
        "subscribing" => EarnProductStatus::Subscribing,
        "active" => EarnProductStatus::Active,
        "settled" => EarnProductStatus::Settled,
        "ended" => EarnProductStatus::Ended,
        "cancelled" => EarnProductStatus::Cancelled,
        _ => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: format!("Invalid status: {}", req.status),
                    code: "INVALID_STATUS".to_string(),
                }),
            ));
        }
    };

    // For status transitions that have on-chain counterparts, call contract first
    {
        // Get the product to find chain_product_id
        let product_detail = state.earn_service.get_plan(&product_id).await.map_err(|e| {
            (StatusCode::NOT_FOUND, Json(ErrorResponse {
                error: format!("Product not found: {}", e),
                code: "PRODUCT_NOT_FOUND".to_string(),
            }))
        })?;
        let chain_id = product_detail.chain_product_id;

        match new_status {
            EarnProductStatus::Subscribing => {
                if let Err(e) = state.earn_service.call_open_plan(chain_id).await {
                    tracing::error!("On-chain openPlan failed for plan {}: {}", chain_id, e);
                    return Err((StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse {
                        error: format!("On-chain openPlan failed: {}", e),
                        code: "ONCHAIN_FAILED".to_string(),
                    })));
                }
            }
            EarnProductStatus::Active => {
                if let Err(e) = state.earn_service.call_activate_plan(chain_id).await {
                    tracing::error!("On-chain activatePlan failed for plan {}: {}", chain_id, e);
                    return Err((StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse {
                        error: format!("On-chain activatePlan failed: {}", e),
                        code: "ONCHAIN_FAILED".to_string(),
                    })));
                }
            }
            _ => {} // Other transitions don't have on-chain counterparts
        }
    }

    match state.earn_service.update_plan_status(&product_id, new_status).await {
        Ok(product) => {
            let detail = ProductDetail::from_product(product);
            Ok(Json(detail))
        }
        Err(e) => {
            let error_msg = e.to_string();
            let (status, code) = if error_msg.contains("not found") {
                (StatusCode::NOT_FOUND, "PRODUCT_NOT_FOUND")
            } else if error_msg.contains("Invalid status transition") {
                (StatusCode::BAD_REQUEST, "INVALID_TRANSITION")
            } else {
                (StatusCode::INTERNAL_SERVER_ERROR, "UPDATE_FAILED")
            };

            tracing::error!("Failed to update product status: {:?}", e);
            Err((
                status,
                Json(ErrorResponse {
                    error: error_msg,
                    code: code.to_string(),
                }),
            ))
        }
    }
}

/// GET /api/v1/admin/earn/products/:id/subscriptions
/// Get plan positions for a plan (admin only)
pub async fn admin_get_plan_positions(
    State(state): State<Arc<AppState>>,
    Path(product_id): Path<String>,
    Query(query): Query<AdminSubscriptionQuery>,
) -> Result<Json<AdminSubscriptionListResponse>, (StatusCode, Json<ErrorResponse>)> {
    match state.earn_service.get_plan_positions(&product_id, query).await {
        Ok(response) => Ok(Json(response)),
        Err(e) => {
            let error_msg = e.to_string();
            if error_msg.contains("not found") {
                Err((
                    StatusCode::NOT_FOUND,
                    Json(ErrorResponse {
                        error: "Product not found".to_string(),
                        code: "PRODUCT_NOT_FOUND".to_string(),
                    }),
                ))
            } else {
                tracing::error!("Failed to get product subscriptions: {:?}", e);
                Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: "Failed to get subscriptions".to_string(),
                        code: "INTERNAL_ERROR".to_string(),
                    }),
                ))
            }
        }
    }
}

/// POST /api/v1/admin/earn/products/:id/settle
/// Trigger closePlan (settlement) for an earn plan (admin only)
pub async fn admin_close_plan(
    State(state): State<Arc<AppState>>,
    Path(product_id): Path<String>,
) -> Result<Json<SuccessResponse>, (StatusCode, Json<ErrorResponse>)> {
    // First update status to settled
    match state.earn_service.update_plan_status(&product_id, EarnProductStatus::Settled).await {
        Ok(_) => {
            // In a real implementation, this would trigger the on-chain settlement
            // For now, just return success
            Ok(Json(SuccessResponse {
                success: true,
                message: Some("Settlement initiated".to_string()),
            }))
        }
        Err(e) => {
            let error_msg = e.to_string();
            let (status, code) = if error_msg.contains("not found") {
                (StatusCode::NOT_FOUND, "PRODUCT_NOT_FOUND")
            } else if error_msg.contains("Invalid status transition") {
                (StatusCode::BAD_REQUEST, "INVALID_STATE")
            } else {
                (StatusCode::INTERNAL_SERVER_ERROR, "SETTLE_FAILED")
            };

            tracing::error!("Failed to settle product: {:?}", e);
            Err((
                status,
                Json(ErrorResponse {
                    error: error_msg,
                    code: code.to_string(),
                }),
            ))
        }
    }
}
