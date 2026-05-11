//! ADL (Auto-Deleveraging) API handlers

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Extension, Json,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::auth::middleware::AuthUser;
use crate::services::adl::{AdlConfig, AdlEvent, AdlRanking, AdlReduction, UserAdlStats};
use crate::AppState;

/// Query parameters for ADL rankings
#[derive(Debug, Deserialize)]
pub struct AdlRankingsQuery {
    pub side: String,  // "long" or "short"
    #[serde(default = "default_limit")]
    pub limit: i64,
}

/// Query parameters for ADL history
#[derive(Debug, Deserialize)]
pub struct AdlHistoryQuery {
    #[serde(default = "default_limit")]
    pub limit: i64,
}

fn default_limit() -> i64 {
    50
}

/// Response for ADL rankings
#[derive(Debug, Serialize)]
pub struct AdlRankingsResponse {
    pub market_symbol: String,
    pub side: String,
    pub rankings: Vec<AdlRanking>,
}

/// Response for ADL config
#[derive(Debug, Serialize)]
pub struct AdlConfigResponse {
    pub config: AdlConfig,
}

/// Response for ADL events list
#[derive(Debug, Serialize)]
pub struct AdlEventsResponse {
    pub events: Vec<AdlEvent>,
}

/// Response for user ADL history
#[derive(Debug, Serialize)]
pub struct UserAdlHistoryResponse {
    pub reductions: Vec<AdlReduction>,
}

/// Response for user ADL stats
#[derive(Debug, Serialize)]
pub struct UserAdlStatsResponse {
    pub stats: Option<UserAdlStats>,
}

/// Get ADL configuration for a market
pub async fn get_adl_config(
    State(state): State<Arc<AppState>>,
    Path(symbol): Path<String>,
) -> Result<Json<AdlConfigResponse>, StatusCode> {
    let config = state
        .adl_service
        .get_config(&symbol)
        .await
        .map_err(|e| {
            tracing::error!("Failed to get ADL config: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(AdlConfigResponse { config }))
}

/// Get ADL rankings for a market (public)
pub async fn get_adl_rankings(
    State(state): State<Arc<AppState>>,
    Path(symbol): Path<String>,
    Query(query): Query<AdlRankingsQuery>,
) -> Result<Json<AdlRankingsResponse>, StatusCode> {
    // Validate side
    if query.side != "long" && query.side != "short" {
        return Err(StatusCode::BAD_REQUEST);
    }

    let rankings = state
        .adl_service
        .get_rankings(&symbol, &query.side, query.limit)
        .await
        .map_err(|e| {
            tracing::error!("Failed to get ADL rankings: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(AdlRankingsResponse {
        market_symbol: symbol,
        side: query.side,
        rankings,
    }))
}

/// Get ADL events history for a market (public)
pub async fn get_market_adl_events(
    State(state): State<Arc<AppState>>,
    Path(symbol): Path<String>,
    Query(query): Query<AdlHistoryQuery>,
) -> Result<Json<AdlEventsResponse>, StatusCode> {
    let events = state
        .adl_service
        .get_market_adl_history(&symbol, query.limit)
        .await
        .map_err(|e| {
            tracing::error!("Failed to get market ADL events: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(AdlEventsResponse { events }))
}

/// Get user's ADL history (requires auth)
pub async fn get_user_adl_history(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Query(query): Query<AdlHistoryQuery>,
) -> Result<Json<UserAdlHistoryResponse>, StatusCode> {
    let reductions = state
        .adl_service
        .get_user_adl_history(&auth_user.address, query.limit)
        .await
        .map_err(|e| {
            tracing::error!("Failed to get user ADL history: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(UserAdlHistoryResponse { reductions }))
}

/// Get user's ADL statistics for a market (requires auth)
pub async fn get_user_adl_stats(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path(symbol): Path<String>,
) -> Result<Json<UserAdlStatsResponse>, StatusCode> {
    let stats = state
        .adl_service
        .get_user_stats(&auth_user.address, &symbol)
        .await
        .map_err(|e| {
            tracing::error!("Failed to get user ADL stats: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(UserAdlStatsResponse { stats }))
}
