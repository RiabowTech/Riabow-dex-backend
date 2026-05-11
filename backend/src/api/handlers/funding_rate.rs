//! Funding Rate API handlers

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Extension, Json,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::auth::middleware::AuthUser;
use crate::services::funding_rate::{FundingRateInfo, FundingSettlement};
use crate::AppState;

/// Query parameters for funding rate history
#[derive(Debug, Deserialize)]
pub struct FundingHistoryQuery {
    #[serde(default = "default_limit")]
    pub limit: i64,
    /// Optional lookback window: "24h", "1w", "1m" (30d)
    pub period: Option<String>,
}

fn default_limit() -> i64 {
    100
}

/// Resolve an optional period string into a lookback start time.
/// Accepts: "24h" / "1d", "1w" / "7d", "1m" / "30d". Case-insensitive.
fn period_to_since(period: Option<&str>) -> Result<Option<chrono::DateTime<chrono::Utc>>, StatusCode> {
    let Some(p) = period else { return Ok(None); };
    let dur = match p.trim().to_lowercase().as_str() {
        "24h" | "1d" => chrono::Duration::hours(24),
        "1w" | "7d" => chrono::Duration::days(7),
        "1m" | "30d" => chrono::Duration::days(30),
        _ => return Err(StatusCode::BAD_REQUEST),
    };
    Ok(Some(chrono::Utc::now() - dur))
}

/// Response for funding rate list
#[derive(Debug, Serialize)]
pub struct FundingRatesResponse {
    pub rates: Vec<FundingRateInfo>,
}

/// Response for funding settlements
#[derive(Debug, Serialize)]
pub struct FundingSettlementsResponse {
    pub settlements: Vec<FundingSettlement>,
}

/// Get current funding rate for a market
pub async fn get_funding_rate(
    State(state): State<Arc<AppState>>,
    Path(symbol): Path<String>,
) -> Result<Json<FundingRateInfo>, StatusCode> {
    let symbol = symbol.to_uppercase();
    let rate = state
        .funding_rate_service
        .get_funding_rate(&symbol)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;

    Ok(Json(rate))
}

/// Get all current funding rates
pub async fn get_all_funding_rates(
    State(state): State<Arc<AppState>>,
) -> Result<Json<FundingRatesResponse>, StatusCode> {
    let rates = state.funding_rate_service.get_all_funding_rates().await;

    Ok(Json(FundingRatesResponse { rates }))
}

/// Get funding rate history for a market
pub async fn get_funding_history(
    State(state): State<Arc<AppState>>,
    Path(symbol): Path<String>,
    Query(query): Query<FundingHistoryQuery>,
) -> Result<Json<FundingRatesResponse>, StatusCode> {
    let symbol = symbol.to_uppercase();
    let since = period_to_since(query.period.as_deref())?;
    let rates = state
        .funding_rate_service
        .get_funding_history(&symbol, query.limit, since)
        .await
        .map_err(|e| {
            tracing::error!("Failed to get funding history: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(FundingRatesResponse { rates }))
}

/// Get user's funding settlement history (requires auth)
pub async fn get_user_settlements(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Query(query): Query<FundingHistoryQuery>,
) -> Result<Json<FundingSettlementsResponse>, StatusCode> {
    let since = period_to_since(query.period.as_deref())?;
    let settlements = state
        .funding_rate_service
        .get_user_settlements(&auth_user.address, query.limit, since)
        .await
        .map_err(|e| {
            tracing::error!("Failed to get user settlements: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(FundingSettlementsResponse { settlements }))
}
