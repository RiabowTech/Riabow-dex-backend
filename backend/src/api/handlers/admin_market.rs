//! Admin Market Config API Handlers
//!
//! CRUD operations for dynamic trading pair management.
//! All endpoints require X-API-Key authentication.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use crate::services::funding_rate::FundingConfig;
use crate::services::market_config::MarketConfigRequest;
use crate::AppState;

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub code: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_status: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct FeeHistoryQuery {
    pub limit: Option<i64>,
}

// ============================================================================
// List / Get
// ============================================================================

/// GET /admin/markets — List all market configs
pub async fn list_markets(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let markets = state
        .market_config_service
        .list_all_from_db()
        .await
        .map_err(|e| {
            tracing::error!("Failed to list market configs: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "获取交易对列表失败".to_string(),
                    code: "LIST_FAILED".to_string(),
                    current_status: None,
                }),
            )
        })?;

    let markets: Vec<_> = markets.into_iter().map(|m| m.with_effective_phase()).collect();
    let total = markets.len();
    Ok(Json(serde_json::json!({
        "markets": markets,
        "total": total
    })))
}

/// GET /admin/markets/:symbol — Get single market config
pub async fn get_market(
    State(state): State<Arc<AppState>>,
    Path(symbol): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let symbol = symbol.to_uppercase();
    let config = state
        .market_config_service
        .get_from_db(&symbol)
        .await
        .map_err(|e| {
            tracing::error!("Failed to get market config: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "获取交易对配置失败".to_string(),
                    code: "GET_FAILED".to_string(),
                    current_status: None,
                }),
            )
        })?;

    match config {
        Some(c) => Ok(Json(serde_json::to_value(c.with_effective_phase()).unwrap())),
        None => Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("交易对不存在: {}", symbol),
                code: "NOT_FOUND".to_string(),
                current_status: None,
            }),
        )),
    }
}

// ============================================================================
// Create / Update / Delete
// ============================================================================

/// POST /admin/markets — Create a new market config
pub async fn create_market(
    State(state): State<Arc<AppState>>,
    Json(req): Json<MarketConfigRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<ErrorResponse>)> {
    // Validate required fields
    if req.symbol.is_none() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "symbol 为必填字段".to_string(),
                code: "MISSING_SYMBOL".to_string(),
                current_status: None,
            }),
        ));
    }
    if req.base_asset.is_none() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "base_asset 为必填字段".to_string(),
                code: "MISSING_BASE_ASSET".to_string(),
                current_status: None,
            }),
        ));
    }

    // Check for duplicate
    let symbol = req.symbol.as_ref().unwrap().to_uppercase();
    if state
        .market_config_service
        .get_from_db(&symbol)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: e.to_string(),
                    code: "DB_ERROR".to_string(),
                    current_status: None,
                }),
            )
        })?
        .is_some()
    {
        return Err((
            StatusCode::CONFLICT,
            Json(ErrorResponse {
                error: format!("交易对已存在: {}", symbol),
                code: "DUPLICATE".to_string(),
                current_status: None,
            }),
        ));
    }

    let config = state
        .market_config_service
        .create(&req)
        .await
        .map_err(|e| {
            tracing::error!("Failed to create market config: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("创建失败: {}", e),
                    code: "CREATE_FAILED".to_string(),
                    current_status: None,
                }),
            )
        })?;

    // Auto-create associated per-market configs with defaults
    let pool = &state.db.pool;

    // ADL config
    if let Err(e) = sqlx::query(
        r#"INSERT INTO adl_config (
            id, market_symbol, insurance_fund_threshold, max_positions_per_adl,
            min_reduction_percentage, max_reduction_percentage,
            pnl_weight, leverage_weight, size_weight,
            min_interval_seconds, enabled, created_at, updated_at
        ) VALUES (gen_random_uuid(), $1, 0, 100, 0.1, 1.0, 0.5, 0.3, 0.2, 60, true, NOW(), NOW())
        ON CONFLICT (market_symbol) DO NOTHING"#,
    )
    .bind(&config.symbol)
    .execute(pool)
    .await
    {
        tracing::warn!("Failed to create default adl_config for {}: {}", config.symbol, e);
    }

    // Liquidation config
    if let Err(e) = sqlx::query(
        r#"INSERT INTO liquidation_config (
            symbol, liquidation_fee_rate, max_leverage, maintenance_margin_rate,
            min_collateral_usd, insurance_fund_fee_rate, max_insurance_payout_rate,
            liquidator_reward_rate, created_at, updated_at
        ) VALUES ($1, 0.005, $2, $3, 10, 0.001, 0.5, 0.001, NOW(), NOW())
        ON CONFLICT (symbol) DO NOTHING"#,
    )
    .bind(&config.symbol)
    .bind(config.max_leverage)
    .bind(config.maintenance_margin_rate)
    .execute(pool)
    .await
    {
        tracing::warn!("Failed to create default liquidation_config for {}: {}", config.symbol, e);
    }

    // Trigger order config
    if let Err(e) = sqlx::query(
        r#"INSERT INTO trigger_order_config (
            id, market_symbol, max_trigger_orders_per_user, max_trigger_orders_per_position,
            min_trigger_distance_pct, max_trigger_distance_pct,
            min_trailing_delta_pct, max_trailing_delta_pct,
            trigger_check_interval_ms, slippage_tolerance_pct,
            enabled, created_at, updated_at
        ) VALUES (gen_random_uuid(), $1, 50, 5, 0.005, 50.0, 0.1, 20.0, 100, 1.0, true, NOW(), NOW())
        ON CONFLICT (market_symbol) DO NOTHING"#,
    )
    .bind(&config.symbol)
    .execute(pool)
    .await
    {
        tracing::warn!("Failed to create default trigger_order_config for {}: {}", config.symbol, e);
    }

    tracing::info!("Created market config with associated configs: {}", config.symbol);
    Ok((
        StatusCode::CREATED,
        Json(serde_json::to_value(config).unwrap()),
    ))
}

/// PUT /admin/markets/:symbol — Update market config
pub async fn update_market(
    State(state): State<Arc<AppState>>,
    Path(symbol): Path<String>,
    Json(req): Json<MarketConfigRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let symbol = symbol.to_uppercase();

    let config = state
        .market_config_service
        .update(&symbol, &req)
        .await
        .map_err(|e| {
            tracing::error!("Failed to update market config: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("更新失败: {}", e),
                    code: "UPDATE_FAILED".to_string(),
                    current_status: None,
                }),
            )
        })?;

    tracing::info!("Updated market config: {}", config.symbol);
    Ok(Json(serde_json::to_value(config).unwrap()))
}

/// Slim payload for the dedicated OI cap endpoint. Spec §1.6.
#[derive(Debug, Deserialize)]
pub struct UpdateOiCapsRequest {
    pub max_long_oi_usd: Option<rust_decimal::Decimal>,
    pub max_short_oi_usd: Option<rust_decimal::Decimal>,
}

/// PUT /admin/markets/:symbol/oi-caps — admin-only.
///
/// Dedicated endpoint for live-tuning per-market per-side OI caps. Reuses
/// MarketConfigService.update() so cache invalidation + DB write happen
/// via the existing path. Spec §1.6.
pub async fn update_oi_caps(
    State(state): State<Arc<AppState>>,
    Path(symbol): Path<String>,
    Json(req): Json<UpdateOiCapsRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let symbol = symbol.to_uppercase();

    let update = MarketConfigRequest {
        max_long_oi_usd: req.max_long_oi_usd,
        max_short_oi_usd: req.max_short_oi_usd,
        ..Default::default()
    };

    state
        .market_config_service
        .update(&symbol, &update)
        .await
        .map_err(|e| {
            tracing::error!("Failed to update oi caps for {}: {}", symbol, e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("更新失败: {}", e),
                    code: "UPDATE_FAILED".to_string(),
                    current_status: None,
                }),
            )
        })?;

    let cfg = state
        .market_config_service
        .get_config(&symbol)
        .await
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: format!("交易对不存在: {}", symbol),
                    code: "NOT_FOUND".to_string(),
                    current_status: None,
                }),
            )
        })?;

    tracing::info!(
        "Updated OI caps for {}: long={}, short={}",
        cfg.symbol, cfg.max_long_oi_usd, cfg.max_short_oi_usd
    );

    Ok(Json(serde_json::json!({
        "symbol": cfg.symbol,
        "max_long_oi_usd": cfg.max_long_oi_usd,
        "max_short_oi_usd": cfg.max_short_oi_usd,
    })))
}

// ============================================================================
// Funding rate cap admin (PUT /admin/markets/:symbol/funding-caps)
// ============================================================================

/// 5%/interval — sanity ceiling on funding-rate caps. Centralised so the
/// validator and any future audit-log formatter agree on the magnitude.
fn funding_cap_ceiling() -> Decimal {
    dec!(0.05)
}

/// Pure validator for funding-rate caps. Returns `Err((code, message))` so the
/// caller can wrap into the project's standard `(StatusCode, Json<ErrorResponse>)`
/// shape, while keeping the helper trivially unit-testable without HTTP types.
///
/// Rules — applied in this order so the most specific failure is reported:
/// 1. `max < 0`            → INVALID_FUNDING_CAP_MAX_NEGATIVE
/// 2. `min > 0`            → INVALID_FUNDING_CAP_MIN_POSITIVE
/// 3. `min > max`          → INVALID_FUNDING_CAP_RANGE_INVERTED
///                            (unreachable via inputs that pass rules 1–2: `min ≤ 0 ≤ max`
///                            implies `min ≤ max`. Retained for defense-in-depth against a
///                            corrupted DB row whose pair survives the merge into the
///                            validator with both signs intact but values inverted.)
/// 4. `|max|` or `|min|` > 5%/interval ceiling → INVALID_FUNDING_CAP_OUT_OF_BOUND
fn validate_funding_caps(max: Decimal, min: Decimal) -> Result<(), (&'static str, String)> {
    let ceiling = funding_cap_ceiling();
    if max < Decimal::ZERO {
        return Err((
            "INVALID_FUNDING_CAP_MAX_NEGATIVE",
            "max_funding_rate cannot be negative".into(),
        ));
    }
    if min > Decimal::ZERO {
        return Err((
            "INVALID_FUNDING_CAP_MIN_POSITIVE",
            "min_funding_rate cannot be positive".into(),
        ));
    }
    if min > max {
        return Err((
            "INVALID_FUNDING_CAP_RANGE_INVERTED",
            "min_funding_rate must be ≤ max_funding_rate".into(),
        ));
    }
    if max.abs() > ceiling || min.abs() > ceiling {
        return Err((
            "INVALID_FUNDING_CAP_OUT_OF_BOUND",
            format!("|funding_rate| must be ≤ {} ({:.2}%/interval)", ceiling, ceiling * Decimal::from(100)),
        ));
    }
    Ok(())
}

/// Merge a partial admin request with the current DB row to produce the effective
/// `(max, min)` pair that will be UPSERTed. Returns
/// `Err("FUNDING_CAP_INCOMPLETE_FOR_NEW_SYMBOL")` when the symbol has no row yet
/// AND the request omits either field — we deliberately do not silently apply
/// `FundingConfig::default()`, to avoid the surprise of an operator setting
/// `max = 0.02` and unknowingly inheriting a default `min = -0.01`.
fn merge_funding_caps_request(
    req_max: Option<Decimal>,
    req_min: Option<Decimal>,
    current: Option<&FundingConfig>,
) -> Result<(Decimal, Decimal), &'static str> {
    match (req_max, req_min, current) {
        (Some(m), Some(n), _)        => Ok((m, n)),
        (Some(m), None,    Some(c))  => Ok((m, c.min_funding_rate)),
        (None,    Some(n), Some(c))  => Ok((c.max_funding_rate, n)),
        (None,    None,    Some(c))  => Ok((c.max_funding_rate, c.min_funding_rate)),
        (_,       _,       None)     => Err("FUNDING_CAP_INCOMPLETE_FOR_NEW_SYMBOL"),
    }
}

/// Slim payload for the funding-cap admin endpoint. Both fields optional —
/// see §5.2 of the spec for the merge semantics.
#[derive(Debug, Deserialize)]
pub struct UpdateFundingCapsRequest {
    pub max_funding_rate: Option<Decimal>,
    pub min_funding_rate: Option<Decimal>,
}

/// PUT /admin/markets/:symbol/funding-caps — admin-only.
///
/// Read-merge-validate-UPSERT. New symbols (no `market_funding_config` row yet)
/// must specify BOTH fields; partial input on a new symbol is rejected with
/// `FUNDING_CAP_INCOMPLETE_FOR_NEW_SYMBOL`. Empty body on an existing row is a
/// no-op (returns the current row, no UPSERT, no audit log).
///
/// No cache invalidation needed — `FundingRateService::get_or_create_config`
/// re-reads `market_funding_config` on every funding tick.
pub async fn update_funding_caps(
    State(state): State<Arc<AppState>>,
    Path(symbol): Path<String>,
    Json(req): Json<UpdateFundingCapsRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let symbol = symbol.to_uppercase();

    // Step 1 — read current row.
    let before = state
        .funding_rate_service
        .get_funding_config(&symbol)
        .await
        .map_err(|e| {
            tracing::error!("Failed to read funding config for {}: {}", symbol, e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("读取失败: {}", e),
                    code: "UPDATE_FAILED".to_string(),
                    current_status: None,
                }),
            )
        })?;

    // Empty-body fast path: handle before any merge logic so the merge function
    // only ever sees inputs where at least one field is Some. Removes the
    // load-bearing expect() that the previous shape relied on.
    if req.max_funding_rate.is_none() && req.min_funding_rate.is_none() {
        return match before {
            Some(b) => Ok(Json(serde_json::json!({
                "symbol": b.symbol,
                "max_funding_rate": b.max_funding_rate,
                "min_funding_rate": b.min_funding_rate,
                "funding_interval_hours": b.funding_interval_hours,
            }))),
            None => Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "Both max_funding_rate and min_funding_rate are required for symbols with no existing config".to_string(),
                    code: "FUNDING_CAP_INCOMPLETE_FOR_NEW_SYMBOL".to_string(),
                    current_status: None,
                }),
            )),
        };
    }

    // Step 2 — merge. By this point at least one request field is Some, so the
    // merge can only fail with FUNDING_CAP_INCOMPLETE_FOR_NEW_SYMBOL when the
    // row is absent — the `before` outcome is the only thing that distinguishes
    // success from rejection.
    let (effective_max, effective_min) =
        merge_funding_caps_request(req.max_funding_rate, req.min_funding_rate, before.as_ref())
            .map_err(|code| {
                (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: "Both max_funding_rate and min_funding_rate are required for symbols with no existing config".to_string(),
                        code: code.to_string(),
                        current_status: None,
                    }),
                )
            })?;

    // Step 3 — validate post-merge values.
    validate_funding_caps(effective_max, effective_min).map_err(|(code, message)| {
        (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: message,
                code: code.to_string(),
                current_status: None,
            }),
        )
    })?;

    // Step 4 — UPSERT.
    let after = state
        .funding_rate_service
        .set_funding_caps(&symbol, effective_max, effective_min)
        .await
        .map_err(|e| {
            tracing::error!("Failed to set funding caps for {}: {}", symbol, e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("更新失败: {}", e),
                    code: "UPDATE_FAILED".to_string(),
                    current_status: None,
                }),
            )
        })?;

    // Audit log — distinguish create vs update.
    match before {
        None => tracing::info!(
            symbol = %symbol,
            max = %after.max_funding_rate,
            min = %after.min_funding_rate,
            "Created funding caps for new symbol",
        ),
        Some(b) => tracing::info!(
            symbol = %symbol,
            old_max = %b.max_funding_rate,
            old_min = %b.min_funding_rate,
            new_max = %after.max_funding_rate,
            new_min = %after.min_funding_rate,
            "Updated funding caps",
        ),
    }

    Ok(Json(serde_json::json!({
        "symbol": after.symbol,
        "max_funding_rate": after.max_funding_rate,
        "min_funding_rate": after.min_funding_rate,
        "funding_interval_hours": after.funding_interval_hours,
    })))
}

/// DELETE /admin/markets/:symbol — Delete market config (must be delisted)
pub async fn delete_market(
    State(state): State<Arc<AppState>>,
    Path(symbol): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let symbol = symbol.to_uppercase();

    // Check it exists and is delisted
    let existing = state
        .market_config_service
        .get_from_db(&symbol)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: e.to_string(),
                    code: "DB_ERROR".to_string(),
                    current_status: None,
                }),
            )
        })?;

    match existing {
        None => Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("交易对不存在: {}", symbol),
                code: "NOT_FOUND".to_string(),
                current_status: None,
            }),
        )),
        Some(config) if config.status != "delisted" => Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "只有已下架的交易对才能删除".to_string(),
                code: "NOT_DELISTED".to_string(),
                current_status: None,
            }),
        )),
        _ => {
            state
                .market_config_service
                .delete(&symbol)
                .await
                .map_err(|e| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(ErrorResponse {
                            error: e.to_string(),
                            code: "DELETE_FAILED".to_string(),
                            current_status: None,
                        }),
                    )
                })?;

            tracing::info!("Deleted market config: {}", symbol);
            Ok(Json(serde_json::json!({"success": true})))
        }
    }
}

// ============================================================================
// Status Actions
// ============================================================================

/// POST /admin/markets/:symbol/suspend — Suspend trading
pub async fn suspend_market(
    State(state): State<Arc<AppState>>,
    Path(symbol): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let symbol = symbol.to_uppercase();

    let existing = state
        .market_config_service
        .get_from_db(&symbol)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: e.to_string(),
                    code: "DB_ERROR".to_string(),
                    current_status: None,
                }),
            )
        })?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: format!("交易对不存在: {}", symbol),
                    code: "NOT_FOUND".to_string(),
                    current_status: None,
                }),
            )
        })?;

    if existing.status != "active" {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "只有活跃状态的交易对才能暂停".to_string(),
                code: "INVALID_STATUS".to_string(),
                current_status: None,
            }),
        ));
    }

    let config = state
        .market_config_service
        .update_status(&symbol, "suspended")
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: e.to_string(),
                    code: "UPDATE_FAILED".to_string(),
                    current_status: None,
                }),
            )
        })?;

    tracing::info!("Suspended market: {}", symbol);
    Ok(Json(serde_json::to_value(config).unwrap()))
}

/// POST /admin/markets/:symbol/resume — Resume trading
pub async fn resume_market(
    State(state): State<Arc<AppState>>,
    Path(symbol): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let symbol = symbol.to_uppercase();

    let existing = state
        .market_config_service
        .get_from_db(&symbol)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: e.to_string(),
                    code: "DB_ERROR".to_string(),
                    current_status: None,
                }),
            )
        })?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: format!("交易对不存在: {}", symbol),
                    code: "NOT_FOUND".to_string(),
                    current_status: None,
                }),
            )
        })?;

    if existing.status != "suspended" {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: format!("只有暂停状态的交易对才能恢复 (当前状态: {})", existing.status),
                code: "INVALID_STATUS".to_string(),
                current_status: Some(existing.status.clone()),
            }),
        ));
    }

    let config = state
        .market_config_service
        .update_status(&symbol, "active")
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: e.to_string(),
                    code: "UPDATE_FAILED".to_string(),
                    current_status: None,
                }),
            )
        })?;

    tracing::info!("Resumed market: {}", symbol);
    Ok(Json(serde_json::to_value(config).unwrap()))
}

/// POST /admin/markets/:symbol/delist — Delist market
pub async fn delist_market(
    State(state): State<Arc<AppState>>,
    Path(symbol): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let symbol = symbol.to_uppercase();

    let existing = state
        .market_config_service
        .get_from_db(&symbol)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: e.to_string(),
                    code: "DB_ERROR".to_string(),
                    current_status: None,
                }),
            )
        })?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: format!("交易对不存在: {}", symbol),
                    code: "NOT_FOUND".to_string(),
                    current_status: None,
                }),
            )
        })?;

    if existing.status == "delisted" {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "交易对已下架".to_string(),
                code: "ALREADY_DELISTED".to_string(),
                current_status: None,
            }),
        ));
    }

    // Check for open positions
    let open_count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM positions WHERE symbol = $1 AND status = 'open'",
    )
    .bind(&symbol)
    .fetch_one(&state.db.pool)
    .await
    .unwrap_or((0,));

    if open_count.0 > 0 {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: format!(
                    "无法下架: 该交易对还有 {} 个未平仓位",
                    open_count.0
                ),
                code: "HAS_OPEN_POSITIONS".to_string(),
                current_status: None,
            }),
        ));
    }

    let config = state
        .market_config_service
        .update_status(&symbol, "delisted")
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: e.to_string(),
                    code: "UPDATE_FAILED".to_string(),
                    current_status: None,
                }),
            )
        })?;

    tracing::info!("Delisted market: {}", symbol);
    Ok(Json(serde_json::to_value(config).unwrap()))
}

// ============================================================================
// Fee History & Open Interest
// ============================================================================

/// GET /admin/markets/:symbol/fee-history — Get fee adjustment history
pub async fn get_fee_history(
    State(state): State<Arc<AppState>>,
    Path(symbol): Path<String>,
    Query(query): Query<FeeHistoryQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let symbol = symbol.to_uppercase();
    let limit = query.limit.unwrap_or(100).min(500);

    let history = state
        .market_config_service
        .get_fee_history(&symbol, limit)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: e.to_string(),
                    code: "QUERY_FAILED".to_string(),
                    current_status: None,
                }),
            )
        })?;

    let current = history.first().cloned();

    Ok(Json(serde_json::json!({
        "symbol": symbol,
        "current": current,
        "history": history,
    })))
}

/// GET /admin/markets/:symbol/open-interest — Get real-time open interest
pub async fn get_open_interest(
    State(state): State<Arc<AppState>>,
    Path(symbol): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let symbol = symbol.to_uppercase();

    let (long_oi, short_oi) = state
        .market_config_service
        .get_open_interest(&symbol)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: e.to_string(),
                    code: "QUERY_FAILED".to_string(),
                    current_status: None,
                }),
            )
        })?;

    let total = long_oi + short_oi;
    let imbalance = if total > Decimal::ZERO {
        (long_oi - short_oi) / total
    } else {
        Decimal::ZERO
    };

    Ok(Json(serde_json::json!({
        "symbol": symbol,
        "long_oi_usd": long_oi,
        "short_oi_usd": short_oi,
        "total_oi_usd": total,
        "imbalance_ratio": imbalance,
    })))
}

#[cfg(test)]
mod funding_cap_validation_tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn accepts_typical_symmetric_caps() {
        assert!(validate_funding_caps(dec!(0.01), dec!(-0.01)).is_ok());
    }

    #[test]
    fn accepts_asymmetric_caps() {
        assert!(validate_funding_caps(dec!(0.005), dec!(-0.01)).is_ok());
    }

    #[test]
    fn accepts_zero_zero_degenerate_clamp() {
        assert!(validate_funding_caps(dec!(0), dec!(0)).is_ok());
    }

    #[test]
    fn accepts_5_percent_boundary_inclusive() {
        assert!(validate_funding_caps(dec!(0.05), dec!(-0.05)).is_ok());
    }

    #[test]
    fn rejects_negative_max() {
        let (code, _) = validate_funding_caps(dec!(-0.001), dec!(-0.01)).unwrap_err();
        assert_eq!(code, "INVALID_FUNDING_CAP_MAX_NEGATIVE");
    }

    #[test]
    fn rejects_positive_min() {
        let (code, _) = validate_funding_caps(dec!(0.01), dec!(0.001)).unwrap_err();
        assert_eq!(code, "INVALID_FUNDING_CAP_MIN_POSITIVE");
    }

    #[test]
    fn rejects_max_above_ceiling() {
        let (code, _) = validate_funding_caps(dec!(0.06), dec!(-0.01)).unwrap_err();
        assert_eq!(code, "INVALID_FUNDING_CAP_OUT_OF_BOUND");
    }

    #[test]
    fn rejects_min_below_negative_ceiling() {
        let (code, _) = validate_funding_caps(dec!(0.01), dec!(-0.06)).unwrap_err();
        assert_eq!(code, "INVALID_FUNDING_CAP_OUT_OF_BOUND");
    }
}

#[cfg(test)]
mod funding_cap_merge_tests {
    use super::*;
    use crate::services::funding_rate::FundingConfig;
    use rust_decimal_macros::dec;

    fn config(max: Decimal, min: Decimal) -> FundingConfig {
        FundingConfig {
            symbol: "BTCUSDT".to_string(),
            funding_interval_hours: 8,
            max_funding_rate: max,
            min_funding_rate: min,
            impact_pool_size: Decimal::ZERO,
        }
    }

    #[test]
    fn both_provided_passes_through() {
        let result = merge_funding_caps_request(Some(dec!(0.02)), Some(dec!(-0.02)), None);
        assert_eq!(result, Ok((dec!(0.02), dec!(-0.02))));
    }

    #[test]
    fn both_provided_ignores_existing_row() {
        let cur = config(dec!(0.01), dec!(-0.01));
        let result = merge_funding_caps_request(Some(dec!(0.02)), Some(dec!(-0.02)), Some(&cur));
        assert_eq!(result, Ok((dec!(0.02), dec!(-0.02))));
    }

    #[test]
    fn partial_max_keeps_existing_min() {
        let cur = config(dec!(0.01), dec!(-0.01));
        let result = merge_funding_caps_request(Some(dec!(0.02)), None, Some(&cur));
        assert_eq!(result, Ok((dec!(0.02), dec!(-0.01))));
    }

    #[test]
    fn partial_min_keeps_existing_max() {
        let cur = config(dec!(0.01), dec!(-0.01));
        let result = merge_funding_caps_request(None, Some(dec!(-0.02)), Some(&cur));
        assert_eq!(result, Ok((dec!(0.01), dec!(-0.02))));
    }

    #[test]
    fn empty_body_returns_existing_pair() {
        let cur = config(dec!(0.01), dec!(-0.01));
        let result = merge_funding_caps_request(None, None, Some(&cur));
        assert_eq!(result, Ok((dec!(0.01), dec!(-0.01))));
    }

    #[test]
    fn partial_with_no_existing_row_is_rejected() {
        let result = merge_funding_caps_request(Some(dec!(0.02)), None, None);
        assert_eq!(result, Err("FUNDING_CAP_INCOMPLETE_FOR_NEW_SYMBOL"));
        let result = merge_funding_caps_request(None, Some(dec!(-0.02)), None);
        assert_eq!(result, Err("FUNDING_CAP_INCOMPLETE_FOR_NEW_SYMBOL"));
    }

    #[test]
    fn empty_body_with_no_existing_row_is_rejected() {
        let result = merge_funding_caps_request(None, None, None);
        assert_eq!(result, Err("FUNDING_CAP_INCOMPLETE_FOR_NEW_SYMBOL"));
    }
}
