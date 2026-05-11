use axum::{
    extract::{Query, State},
    response::IntoResponse,
    Extension,
    Json,
};
use serde::Deserialize;
use std::sync::Arc;
use crate::models::points::SimulatePointsRequest;

use crate::app::state::AppState;
use crate::utils::response::ApiResponse;
use crate::models::points::{LeaderboardType, PointType};
use crate::auth::middleware::AuthUser;

#[derive(Debug, Deserialize)]
pub struct DailyLeaderboardQuery {
    pub epoch: Option<i32>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct EpochQuery {
    pub epoch: Option<i32>,
}

#[derive(Debug, Deserialize)]
pub struct LeaderboardQuery {
    pub epoch: Option<i32>,
    pub type_: Option<LeaderboardType>,
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct TierQuery {
    pub epoch: Option<i32>,
}

#[derive(Debug, Deserialize)]
pub struct HistoryQuery {
    pub epoch: Option<i32>,
    #[serde(rename = "type")]
    pub point_type: Option<PointType>,
    pub page: Option<i32>,
    pub page_size: Option<i32>,
}

// GET /api/v1/points (Protected)
pub async fn get_user_points(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Query(query): Query<EpochQuery>,
) -> impl IntoResponse {
    let user_address = auth_user.address.to_lowercase();
    
    match state.points_service.get_user_points(&user_address, query.epoch).await {
        Ok(Some(summary)) => Json(ApiResponse::success(summary)).into_response(),
        Ok(None) => {
            // Get active epoch info for defaults
            let (epoch_num, epoch_status) = match state.points_service.get_active_epoch().await {
                Ok(Some(e)) => (e.epoch_number, e.status.to_string()),
                _ => (1, "active".to_string()),
            };

            Json(ApiResponse::success(serde_json::json!({
                "user_address": user_address,
                "epoch_number": epoch_num,
                "epoch_status": epoch_status,

                "trading_points": "0.00",
                "pnl_points": "0.00",
                "holding_points": "0.00",
                "referral_points": "0.00",
                "referral_code": null,
                "staking_points": "0.00",

                "total_points": "0.00",
                "tier": "T1",
                "tier_multiplier": "1.0",

                "earn_level": 0,
                "earn_level_weight": 4,
                "earn_level_points_to_next": "1000.00",

                "rank": null,

                "trading_volume": "0.00",
                "trade_count": 0,
                "referral_count": 0,

                "updated_at": chrono::Utc::now()
            }))).into_response()
        },
        Err(e) => {
             tracing::error!("Failed to get user points: {}", e);
             Json(ApiResponse::<()>::error("500", "Internal server error")).into_response()
        }
    }
}

// GET /api/v1/points/leaderboard (Public)
pub async fn get_leaderboard(
    State(state): State<Arc<AppState>>,
    Query(query): Query<LeaderboardQuery>,
) -> impl IntoResponse {
    let epoch = query.epoch.unwrap_or(1); // Default to epoch 1 or current
    let lb_type = query.type_.unwrap_or(LeaderboardType::Total);
    let limit = query.limit.unwrap_or(100);

    match state.points_service.get_leaderboard(epoch, lb_type, limit).await {
        Ok(leaderboard) => Json(ApiResponse::success(leaderboard)).into_response(),
        Err(e) => {
            tracing::error!("Failed to get leaderboard: {}", e);
            Json(ApiResponse::<()>::error("500", "Internal server error")).into_response()
        }
    }
}

// GET /api/v1/points/history (Protected)
pub async fn get_points_history(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Query(query): Query<HistoryQuery>,
) -> impl IntoResponse {
    let user_address = auth_user.address.to_lowercase();
    let page = query.page.unwrap_or(1).max(1);
    let page_size = query.page_size.unwrap_or(20).clamp(1, 100);

    match state.points_service
        .get_user_points_history(&user_address, query.epoch, query.point_type, page, page_size)
        .await
    {
        Ok(history) => Json(ApiResponse::success(history)).into_response(),
        Err(e) => {
            tracing::error!("Failed to get points history for {}: {}", user_address, e);
            Json(ApiResponse::<()>::error("500", "Internal server error")).into_response()
        }
    }
}

// GET /api/v1/points/tier (Protected)
pub async fn get_tier_info(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Query(query): Query<TierQuery>,
) -> impl IntoResponse {
    let user_address = auth_user.address.to_lowercase();

    match state.points_service.get_user_tier_info(&user_address, query.epoch).await {
        Ok(Some(info)) => Json(ApiResponse::success(info)).into_response(),
        Ok(None) => Json(ApiResponse::<()>::error("404", "No active epoch")).into_response(),
        Err(e) => {
            tracing::error!("Failed to get tier info for {}: {}", user_address, e);
            Json(ApiResponse::<()>::error("500", "Internal server error")).into_response()
        }
    }
}

// GET /api/v1/points/earn-quota (Protected)
pub async fn get_earn_quota(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Query(query): Query<EpochQuery>,
) -> impl IntoResponse {
    let user_address = auth_user.address.to_lowercase();

    let epoch_number = match query.epoch {
        Some(e) => e,
        None => match state.points_service.get_active_epoch().await {
            Ok(Some(e)) => e.epoch_number,
            _ => 1,
        },
    };

    match state.points_service.get_user_earn_quota(&user_address, epoch_number).await {
        Ok(quota) => Json(ApiResponse::success(quota)).into_response(),
        Err(e) => {
            tracing::error!("Failed to get earn quota for {}: {}", user_address, e);
            Json(ApiResponse::<()>::error("500", "Internal server error")).into_response()
        }
    }
}

// POST /api/v1/points/simulate (Protected)
pub async fn simulate_points(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Query(query): Query<EpochQuery>,
    Json(request): Json<SimulatePointsRequest>,
) -> impl IntoResponse {
    let user_address = auth_user.address.to_lowercase();

    let epoch_number = match query.epoch {
        Some(e) => e,
        None => match state.points_service.get_active_epoch().await {
            Ok(Some(e)) => e.epoch_number,
            _ => 1,
        },
    };

    match state.points_service.simulate_points(&user_address, epoch_number, &request).await {
        Ok(result) => Json(ApiResponse::success(result)).into_response(),
        Err(e) => {
            tracing::error!("Failed to simulate points for {}: {}", user_address, e);
            Json(ApiResponse::<()>::error("500", "Internal server error")).into_response()
        }
    }
}

// GET /api/v1/points/earn-level-config (Public)
pub async fn get_earn_level_config(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    match state.points_service.get_earn_level_config().await {
        Ok(configs) => Json(ApiResponse::success(configs)).into_response(),
        Err(e) => {
            tracing::error!("Failed to get earn level config: {}", e);
            Json(ApiResponse::<()>::error("500", "Internal server error")).into_response()
        }
    }
}

// =============================================================================
// Phase 3: Seasons + Token Distribution + Claim Signature
// =============================================================================

#[derive(Debug, serde::Serialize, sqlx::FromRow)]
pub struct SeasonRow {
    pub season_id: uuid::Uuid,
    pub season_no: i32,
    pub label: String,
    pub start_epoch: i32,
    pub end_epoch: i32,
    pub user_pool_tokens: i64,
    pub mm_pool_tokens: i64,
    pub status: String,
    pub snapshot_at: Option<chrono::DateTime<chrono::Utc>>,
    pub snapshot_taken_at: Option<chrono::DateTime<chrono::Utc>>,
    pub distribution_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// GET /api/v1/points/seasons — public
pub async fn get_seasons(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let rows: Result<Vec<SeasonRow>, sqlx::Error> = sqlx::query_as::<_, SeasonRow>(
        "SELECT season_id, season_no, label, start_epoch, end_epoch,
                user_pool_tokens, mm_pool_tokens, status,
                snapshot_at, snapshot_taken_at, distribution_at
         FROM points_seasons ORDER BY season_no",
    )
    .fetch_all(state.points_service.pool())
    .await;
    match rows {
        Ok(rs) => Json(ApiResponse::success(rs)).into_response(),
        Err(e) => {
            tracing::error!("get_seasons: {}", e);
            Json(ApiResponse::<()>::error("DB_ERROR", "db")).into_response()
        }
    }
}

#[derive(Debug, serde::Serialize, sqlx::FromRow)]
pub struct DistributionRow {
    pub id: uuid::Uuid,
    pub season_id: uuid::Uuid,
    pub user_address: String,
    pub pool_type: String,
    pub weighted_points: rust_decimal::Decimal,
    pub share_pct: rust_decimal::Decimal,
    pub token_amount: rust_decimal::Decimal,
    pub claim_status: String,
    pub claim_deadline: chrono::DateTime<chrono::Utc>,
    pub claim_nonce: i64,
    pub claimed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub claim_tx_hash: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// GET /api/v1/points/distribution/:season_id — auth
pub async fn get_distribution(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    axum::extract::Path(season_id): axum::extract::Path<uuid::Uuid>,
) -> impl IntoResponse {
    let addr = auth_user.address.to_lowercase();
    let rows: Result<Vec<DistributionRow>, sqlx::Error> = sqlx::query_as::<_, DistributionRow>(
        "SELECT id, season_id, user_address, pool_type, weighted_points, share_pct,
                token_amount, claim_status, claim_deadline, claim_nonce,
                claimed_at, claim_tx_hash, created_at
         FROM points_distribution
         WHERE season_id = $1 AND user_address = $2
         ORDER BY pool_type",
    )
    .bind(season_id)
    .bind(&addr)
    .fetch_all(state.points_service.pool())
    .await;
    match rows {
        Ok(rs) => Json(ApiResponse::success(rs)).into_response(),
        Err(e) => {
            tracing::error!("get_distribution: {}", e);
            Json(ApiResponse::<()>::error("DB_ERROR", "db")).into_response()
        }
    }
}

/// POST /api/v1/points/claim/:distribution_id — auth
/// Returns an EIP-712 signature the user can submit to the (future)
/// distribution contract. Idempotent — re-issues the same signature
/// for the same (user, distribution, nonce) tuple.
pub async fn claim_distribution(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    axum::extract::Path(distribution_id): axum::extract::Path<uuid::Uuid>,
) -> impl IntoResponse {
    let addr = auth_user.address.to_lowercase();

    // Load + ownership + status check.
    let row: Result<Option<DistributionRow>, sqlx::Error> = sqlx::query_as::<_, DistributionRow>(
        "SELECT id, season_id, user_address, pool_type, weighted_points, share_pct,
                token_amount, claim_status, claim_deadline, claim_nonce,
                claimed_at, claim_tx_hash, created_at
         FROM points_distribution WHERE id = $1",
    )
    .bind(distribution_id)
    .fetch_optional(state.points_service.pool())
    .await;

    let dist = match row {
        Ok(Some(r)) if r.user_address == addr => r,
        Ok(Some(_)) => return Json(ApiResponse::<()>::error("FORBIDDEN", "Not your distribution")).into_response(),
        Ok(None)    => return Json(ApiResponse::<()>::error("NOT_FOUND", "Distribution not found")).into_response(),
        Err(e)      => {
            tracing::error!("claim load: {}", e);
            return Json(ApiResponse::<()>::error("DB_ERROR", "db")).into_response();
        }
    };

    if dist.claim_status == "claimed" {
        return Json(ApiResponse::<()>::error("ALREADY_CLAIMED", "Already claimed")).into_response();
    }
    let now = chrono::Utc::now();
    if now > dist.claim_deadline {
        return Json(ApiResponse::<()>::error("EXPIRED", "Claim window expired")).into_response();
    }

    let chain_id = state.config.chain_id;
    let signer = match crate::services::points::claim_signer::ClaimSigner::from_env(chain_id) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("claim signer init: {}", e);
            return Json(ApiResponse::<()>::error("SIGNER_UNAVAILABLE", "Signer not configured"))
                .into_response();
        }
    };

    let payload = signer
        .sign_claim(
            &addr,
            distribution_id,
            &dist.token_amount.to_string(),
            dist.claim_nonce as u64,
            dist.claim_deadline.timestamp() as u64,
        )
        .await;
    match payload {
        Ok(p) => Json(ApiResponse::success(p)).into_response(),
        Err(e) => {
            tracing::error!("claim sign: {}", e);
            Json(ApiResponse::<()>::error("SIGN_FAILED", "Failed to sign")).into_response()
        }
    }
}

// =============================================================================

// GET /api/v1/epochs (Public)
pub async fn get_epochs(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    match state.points_service.list_epochs(100, 0).await {
        Ok(mut epochs) => {
            // Sort ascending by epoch number (list_epochs returns DESC)
            epochs.sort_by_key(|e| e.epoch_number);
            
            let epoch_options: Vec<serde_json::Value> = epochs.into_iter().map(|e| {
                let start_date = e.start_time.format("%Y-%m-%d").to_string();
                let end_date = e.end_time.format("%Y-%m-%d").to_string();
                // Format: 2026.1.1 (no zero padding for month/day if possible, or standard)
                // User example: 2026.1.1. %-m and %-d remove padding on unix, usually works in chrono.
                let label_start = e.start_time.format("%Y.%-m.%-d").to_string();
                let label_end = e.end_time.format("%Y.%-m.%-d").to_string();
                
                serde_json::json!({
                    "id": e.epoch_number,
                    "label": format!("Epoch {}: {} - {}", e.epoch_number, label_start, label_end),
                    "startDate": start_date,
                    "endDate": end_date,
                    "status": e.status
                })
            }).collect();
            
            Json(ApiResponse::success(epoch_options)).into_response()
        },
        Err(e) => {
            tracing::error!("Failed to get epochs: {}", e);
            Json(ApiResponse::<()>::error("500", "Internal server error")).into_response()
        }
    }
}

// GET /api/v1/points/leaderboard/daily (Public)
pub async fn get_daily_leaderboard(
    State(state): State<Arc<AppState>>,
    Query(query): Query<DailyLeaderboardQuery>,
) -> impl IntoResponse {
    let epoch = match query.epoch {
        Some(e) => e,
        None => match state.points_service.get_active_epoch().await {
            Ok(Some(e)) => e.epoch_number,
            _ => 1,
        },
    };
    let limit = query.limit.unwrap_or(10).clamp(1, 50);
    let offset = query.offset.unwrap_or(0).max(0);

    match state.points_service.get_daily_leaderboard(epoch, limit, offset).await {
        Ok(resp) => Json(ApiResponse::success(resp)).into_response(),
        Err(e) => {
            tracing::error!("Failed to get daily leaderboard: {}", e);
            Json(ApiResponse::<()>::error("500", "Internal server error")).into_response()
        }
    }
}
