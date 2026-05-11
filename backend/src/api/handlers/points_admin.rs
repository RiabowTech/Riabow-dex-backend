//! Points System Admin API Handlers (Phase 3.4)
//!
//! Admin endpoints for managing points system configuration.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

use crate::models::points::{AdjustPointsRequest, EarnLevelConfig, PointsConfigRow};
use crate::services::points::PointsConfig;
use crate::utils::response::AppError;
use crate::AppState;

// ============================================================================
// Request/Response Types
// ============================================================================

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct UpdateConfigRequest {
    pub enabled: Option<bool>,
    pub trading_enabled: Option<bool>,
    pub pnl_enabled: Option<bool>,
    pub holding_enabled: Option<bool>,
    pub referral_enabled: Option<bool>,
    pub staking_enabled: Option<bool>,
    pub cache_ttl: Option<u64>,
    pub leaderboard_limit: Option<usize>,
}

#[allow(dead_code)]
#[derive(Debug, Serialize)]
pub struct ConfigResponse {
    pub enabled: bool,
    pub trading_enabled: bool,
    pub pnl_enabled: bool,
    pub holding_enabled: bool,
    pub referral_enabled: bool,
    pub staking_enabled: bool,
    pub cache_ttl: u64,
    pub leaderboard_limit: usize,
}

impl From<PointsConfig> for ConfigResponse {
    fn from(config: PointsConfig) -> Self {
        Self {
            enabled: config.enabled,
            trading_enabled: config.trading_enabled,
            pnl_enabled: config.pnl_enabled,
            holding_enabled: config.holding_enabled,
            referral_enabled: config.referral_enabled,
            staking_enabled: config.staking_enabled,
            cache_ttl: config.cache_ttl,
            leaderboard_limit: config.leaderboard_limit,
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct TriggerTaskQuery {
    pub task: String, // "holding", "staking", "leaderboard"
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct RecalculateRequest {
    pub user_address: String,
    pub epoch_number: i32,
}

// ============================================================================
// Admin Handlers
// ============================================================================

/// GET /admin/points/config - Get current points system configuration
#[allow(dead_code)]
pub async fn get_config(
    State(state): State<Arc<AppState>>,
) -> Result<Json<ConfigResponse>, AppError> {
    let config = state.points_service.get_config().await;
    Ok(Json(config.into()))
}

/// PUT /admin/points/config - Update points system configuration
#[allow(dead_code)]
pub async fn update_config(
    State(state): State<Arc<AppState>>,
    Json(request): Json<UpdateConfigRequest>,
) -> Result<Json<ConfigResponse>, AppError> {
    let mut config = state.points_service.get_config().await;

    if let Some(v) = request.enabled { config.enabled = v; }
    if let Some(v) = request.trading_enabled { config.trading_enabled = v; }
    if let Some(v) = request.pnl_enabled { config.pnl_enabled = v; }
    if let Some(v) = request.holding_enabled { config.holding_enabled = v; }
    if let Some(v) = request.referral_enabled { config.referral_enabled = v; }
    if let Some(v) = request.staking_enabled { config.staking_enabled = v; }
    if let Some(v) = request.cache_ttl { config.cache_ttl = v; }
    if let Some(v) = request.leaderboard_limit { config.leaderboard_limit = v; }

    state.points_service.update_config(config.clone()).await;

    Ok(Json(config.into()))
}

/// POST /admin/points/trigger - Manually trigger background tasks
#[allow(dead_code)]
pub async fn trigger_task(
    State(state): State<Arc<AppState>>,
    Query(query): Query<TriggerTaskQuery>,
) -> Result<Json<String>, AppError> {
    let epoch = state.points_service.get_active_epoch().await
        .map_err(|e| AppError::internal(&e.to_string()))?
        .ok_or(AppError::bad_request("No active epoch"))?;
    
    let epoch_number = epoch.epoch_number;

    match query.task.as_str() {
        "holding" => {
            state.points_service.calculate_holding_points_batch(epoch_number).await
                .map_err(|e| AppError::internal(&e.to_string()))?;
            Ok(Json("Holding points calculation triggered".to_string()))
        },
        "staking" => {
            state.points_service.calculate_staking_points_batch(epoch_number).await
                .map_err(|e| AppError::internal(&e.to_string()))?;
            Ok(Json("Staking points calculation triggered".to_string()))
        },
        "leaderboard" => {
            state.points_service.refresh_leaderboard(epoch_number).await
                .map_err(|e| AppError::internal(&e.to_string()))?;
            Ok(Json("Leaderboard refresh triggered".to_string()))
        },
         _ => Err(AppError::bad_request("Invalid task name")),
    }
}

/// GET /admin/points/stats/:epoch - Get epoch statistics
#[allow(dead_code)]
pub async fn get_epoch_stats(
    State(state): State<Arc<AppState>>,
    Path(epoch): Path<i32>,
) -> Result<Json<Value>, AppError> {
    let stats = state.points_service.get_epoch_stats(epoch).await
        .map_err(|e| AppError::internal(&e.to_string()))?;
    
    Ok(Json(serde_json::to_value(stats).unwrap()))
}

/// POST /admin/points/adjust - Adjust user points manually
#[allow(dead_code)]
pub async fn adjust_points(
    State(state): State<Arc<AppState>>,
    Json(request): Json<AdjustPointsRequest>,
) -> Result<StatusCode, AppError> {
    // TODO: Extract admin address from auth
    let admin_address = "admin_action"; 

    state.points_service.adjust_points(admin_address, request).await
        .map_err(|e| AppError::internal(&e.to_string()))?;

    Ok(StatusCode::OK)
}

/// POST /admin/points/recalculate - Recalculate user points
#[allow(dead_code)]
pub async fn recalculate_points(
    State(state): State<Arc<AppState>>,
    Json(request): Json<RecalculateRequest>,
) -> Result<StatusCode, AppError> {
    // TODO: Extract admin address from auth
    let admin_address = "admin_action";

    state.points_service.recalculate_user_points(admin_address, &request.user_address, request.epoch_number).await
        .map_err(|e| AppError::internal(&e.to_string()))?;

    Ok(StatusCode::OK)
}

// ============================================================================
// Phase 1: Points Config (points_config table)
// ============================================================================

/// GET /admin/points/points-config/:epoch - Get points config for epoch
pub async fn get_points_config(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(epoch): axum::extract::Path<i32>,
) -> Result<Json<PointsConfigRow>, AppError> {
    let cfg = state.points_service.get_points_config(epoch).await
        .map_err(|e| AppError::internal(&e.to_string()))?;
    Ok(Json(cfg))
}

/// POST /admin/points/points-config - Upsert points config
pub async fn upsert_points_config(
    State(state): State<Arc<AppState>>,
    Json(row): Json<PointsConfigRow>,
) -> Result<StatusCode, AppError> {
    state.points_service.upsert_points_config(&row).await
        .map_err(|e| AppError::internal(&e.to_string()))?;
    Ok(StatusCode::OK)
}

// ============================================================================
// Phase 1: Earn Level Config (earn_level_config table)
// ============================================================================

/// GET /admin/points/earn-level-config - List earn level configs
pub async fn get_earn_level_config(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<EarnLevelConfig>>, AppError> {
    let configs = state.points_service.get_earn_level_config().await
        .map_err(|e| AppError::internal(&e.to_string()))?;
    Ok(Json(configs))
}

/// POST /admin/points/earn-level-config - Upsert earn level configs
pub async fn upsert_earn_level_config(
    State(state): State<Arc<AppState>>,
    Json(configs): Json<Vec<EarnLevelConfig>>,
) -> Result<StatusCode, AppError> {
    state.points_service.upsert_earn_level_config(&configs).await
        .map_err(|e| AppError::internal(&e.to_string()))?;
    Ok(StatusCode::OK)
}

/// POST /admin/points/trigger-earn-refresh - Manually trigger earn level refresh
pub async fn trigger_earn_refresh(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, AppError> {
    let updated = state.points_service.run_daily_earn_level_refresh().await
        .map_err(|e| AppError::internal(&e.to_string()))?;
    Ok(Json(serde_json::json!({ "updated": updated })))
}

// ============================================================================
// Phase 3: Manual season snapshot trigger (admin override)
// ============================================================================

/// POST /admin/points/snapshot/{season_id} — force snapshot regardless
/// of epoch end_at. Useful for catch-up after a missed worker tick or
/// for testing on a season whose epochs haven't all elapsed.
pub async fn admin_trigger_season_snapshot(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(season_id): axum::extract::Path<uuid::Uuid>,
) -> Result<Json<serde_json::Value>, AppError> {
    #[derive(sqlx::FromRow)]
    struct S {
        season_no: i32,
        start_epoch: i32,
        end_epoch: i32,
        user_pool_tokens: i64,
        mm_pool_tokens: i64,
    }
    let s: Option<S> = sqlx::query_as::<_, S>(
        "SELECT season_no, start_epoch, end_epoch, user_pool_tokens, mm_pool_tokens
         FROM points_seasons WHERE season_id = $1",
    )
    .bind(season_id)
    .fetch_optional(state.points_service.pool())
    .await
    .map_err(|e| AppError::internal(&e.to_string()))?;
    let s = s.ok_or_else(|| AppError::not_found("season not found"))?;

    crate::services::points::season_snapshot::snapshot_season(
        state.points_service.pool(),
        season_id,
        s.season_no,
        s.start_epoch,
        s.end_epoch,
        s.user_pool_tokens,
        s.mm_pool_tokens,
    )
    .await
    .map_err(|e| AppError::internal(&e.to_string()))?;

    Ok(Json(serde_json::json!({
        "season_no": s.season_no,
        "snapshot_triggered": true
    })))
}
