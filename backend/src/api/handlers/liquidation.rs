//! Liquidation API handlers

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Extension, Json,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::auth::middleware::AuthUser;
use crate::services::liquidation::{InsuranceFund, LiquidationConfig, LiquidationRecord};
use crate::AppState;

/// Query parameters for liquidation history
#[derive(Debug, Deserialize)]
pub struct LiquidationHistoryQuery {
    #[serde(default = "default_limit")]
    pub limit: i64,
}

fn default_limit() -> i64 {
    50
}

/// Response for liquidation records list
#[derive(Debug, Serialize)]
pub struct LiquidationsResponse {
    pub liquidations: Vec<LiquidationRecord>,
}

/// Response for insurance fund info
#[derive(Debug, Serialize)]
pub struct InsuranceFundResponse {
    pub fund: InsuranceFund,
}

/// Response for liquidation config
#[derive(Debug, Serialize)]
pub struct LiquidationConfigResponse {
    pub config: LiquidationConfig,
}

/// Get liquidation config for a market
pub async fn get_liquidation_config(
    State(state): State<Arc<AppState>>,
    Path(symbol): Path<String>,
) -> Result<Json<LiquidationConfigResponse>, StatusCode> {
    let config = state
        .liquidation_service
        .get_config(&symbol)
        .await
        .map_err(|e| {
            tracing::error!("Failed to get liquidation config: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(LiquidationConfigResponse { config }))
}

/// Get insurance fund balance for a market
pub async fn get_insurance_fund(
    State(state): State<Arc<AppState>>,
    Path(symbol): Path<String>,
) -> Result<Json<InsuranceFundResponse>, StatusCode> {
    let fund = state
        .liquidation_service
        .get_insurance_fund(&symbol)
        .await
        .map_err(|e| {
            tracing::error!("Failed to get insurance fund: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(InsuranceFundResponse { fund }))
}

/// Get recent liquidations for a market (public)
pub async fn get_market_liquidations(
    State(state): State<Arc<AppState>>,
    Path(symbol): Path<String>,
    Query(query): Query<LiquidationHistoryQuery>,
) -> Result<Json<LiquidationsResponse>, StatusCode> {
    let liquidations = state
        .liquidation_service
        .get_market_liquidations(&symbol, query.limit)
        .await
        .map_err(|e| {
            tracing::error!("Failed to get market liquidations: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(LiquidationsResponse { liquidations }))
}

/// Get user's liquidation history (requires auth)
pub async fn get_user_liquidations(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Query(query): Query<LiquidationHistoryQuery>,
) -> Result<Json<LiquidationsResponse>, StatusCode> {
    let liquidations = state
        .liquidation_service
        .get_user_liquidations(&auth_user.address, query.limit)
        .await
        .map_err(|e| {
            tracing::error!("Failed to get user liquidations: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(LiquidationsResponse { liquidations }))
}
