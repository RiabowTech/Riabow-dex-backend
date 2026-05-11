use axum::{
    extract::{Path, Query},
    http::StatusCode,
    Json,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::api::handlers::market::ErrorResponse;
use crate::services::rwa::registry::{RwaAssetClass, SYMBOL_TO_RWA};
use crate::services::rwa::{RwaService, RwaTickerSnapshot};

#[derive(Debug, Deserialize)]
pub struct AssetsQuery {
    pub class: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct AssetsResponse {
    pub assets: Vec<RwaTickerSnapshot>,
    pub total: usize,
    pub updated_at: i64,
}

#[derive(Debug, Serialize)]
pub struct PricesResponse {
    pub prices: HashMap<String, Decimal>,
    pub updated_at: i64,
}

pub async fn list_assets(
    Query(query): Query<AssetsQuery>,
) -> Result<Json<AssetsResponse>, (StatusCode, Json<ErrorResponse>)> {
    let assets = if let Some(class_str) = &query.class {
        let class = RwaAssetClass::from_str(class_str).ok_or_else(|| {
            (StatusCode::BAD_REQUEST, Json(ErrorResponse {
                error: format!("Invalid asset class: {}. Valid values: precious_metal, stock, index", class_str),
                code: "INVALID_CLASS".to_string(),
            }))
        })?;
        RwaService::get_tickers_by_class(class)
    } else {
        RwaService::get_all_tickers()
    };

    let total = assets.len();
    let updated_at = chrono::Utc::now().timestamp_millis();

    Ok(Json(AssetsResponse { assets, total, updated_at }))
}

pub async fn get_asset(
    Path(symbol): Path<String>,
) -> Result<Json<RwaTickerSnapshot>, (StatusCode, Json<ErrorResponse>)> {
    let symbol_upper = symbol.to_uppercase();

    if !SYMBOL_TO_RWA.contains_key(symbol_upper.as_str()) {
        return Err((StatusCode::NOT_FOUND, Json(ErrorResponse {
            error: format!("Unknown RWA asset: {}", symbol_upper),
            code: "ASSET_NOT_FOUND".to_string(),
        })));
    }

    RwaService::get_ticker(&symbol_upper).ok_or_else(|| {
        (StatusCode::SERVICE_UNAVAILABLE, Json(ErrorResponse {
            error: "Price data not yet available. The service may still be starting.".to_string(),
            code: "PRICE_UNAVAILABLE".to_string(),
        }))
    }).map(Json)
}

pub async fn get_prices() -> Json<PricesResponse> {
    let prices = RwaService::get_prices();
    let updated_at = chrono::Utc::now().timestamp_millis();
    Json(PricesResponse { prices, updated_at })
}
