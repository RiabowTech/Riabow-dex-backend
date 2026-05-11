use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;
use tracing::info;

use crate::AppState;

// ==================== Request/Response Types ====================

#[derive(Debug, Deserialize)]
pub struct SetOrderbookRequest {
    pub symbol: String,
    pub bids: Vec<OrderbookPriceLevel>,
    pub asks: Vec<OrderbookPriceLevel>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct OrderbookPriceLevel {
    pub price: String,
    pub amount: String,
}

#[derive(Debug, Serialize)]
pub struct SetOrderbookResponse {
    pub success: bool,
    pub symbol: String,
    pub bids_count: usize,
    pub asks_count: usize,
}

// ==================== Handler Functions ====================

/// Set orderbook data (for market making)
/// POST /api/v1/internal/orderbook
pub async fn set_orderbook(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SetOrderbookRequest>,
) -> impl IntoResponse {
    // Normalize symbol
    let symbol = normalize_symbol(&req.symbol);
    
    // Validate symbol
    if symbol.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(SetOrderbookResponse {
                success: false,
                symbol: req.symbol,
                bids_count: 0,
                asks_count: 0,
            }),
        )
            .into_response();
    }

    // Get orderbook cache
    let orderbook_cache = match state.cache.orderbook_opt() {
        Some(cache) => cache,
        None => {
            tracing::error!("Orderbook cache is not available");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(SetOrderbookResponse {
                    success: false,
                    symbol,
                    bids_count: 0,
                    asks_count: 0,
                }),
            )
                .into_response();
        }
    };

    // Convert and validate price levels
    let mut parsed_bids = Vec::new();
    for bid in &req.bids {
        let price = match Decimal::from_str(&bid.price) {
            Ok(p) => p,
            Err(e) => {
                tracing::error!("Invalid bid price {}: {}", bid.price, e);
                return (
                    StatusCode::BAD_REQUEST,
                    Json(SetOrderbookResponse {
                        success: false,
                        symbol,
                        bids_count: 0,
                        asks_count: 0,
                    }),
                )
                    .into_response();
            }
        };
        
        let amount = match Decimal::from_str(&bid.amount) {
            Ok(a) => a,
            Err(e) => {
                tracing::error!("Invalid bid amount {}: {}", bid.amount, e);
                return (
                    StatusCode::BAD_REQUEST,
                    Json(SetOrderbookResponse {
                        success: false,
                        symbol,
                        bids_count: 0,
                        asks_count: 0,
                    }),
                )
                    .into_response();
            }
        };
        
        parsed_bids.push(crate::cache::orderbook_cache::PriceLevel { price, amount });
    }

    let mut parsed_asks = Vec::new();
    for ask in &req.asks {
        let price = match Decimal::from_str(&ask.price) {
            Ok(p) => p,
            Err(e) => {
                tracing::error!("Invalid ask price {}: {}", ask.price, e);
                return (
                    StatusCode::BAD_REQUEST,
                    Json(SetOrderbookResponse {
                        success: false,
                        symbol,
                        bids_count: 0,
                        asks_count: 0,
                    }),
                )
                    .into_response();
            }
        };
        
        let amount = match Decimal::from_str(&ask.amount) {
            Ok(a) => a,
            Err(e) => {
                tracing::error!("Invalid ask amount {}: {}", ask.amount, e);
                return (
                    StatusCode::BAD_REQUEST,
                    Json(SetOrderbookResponse {
                        success: false,
                        symbol,
                        bids_count: 0,
                        asks_count: 0,
                    }),
                )
                    .into_response();
            }
        };
        
        parsed_asks.push(crate::cache::orderbook_cache::PriceLevel { price, amount });
    }

    // Set orderbook in cache
    match orderbook_cache
        .set_orderbook(&symbol, &parsed_bids, &parsed_asks)
        .await
    {
        Ok(_) => {
            info!(
                "Updated orderbook for {}: {} bids, {} asks",
                symbol,
                parsed_bids.len(),
                parsed_asks.len()
            );

            (
                StatusCode::OK,
                Json(SetOrderbookResponse {
                    success: true,
                    symbol,
                    bids_count: parsed_bids.len(),
                    asks_count: parsed_asks.len(),
                }),
            )
                .into_response()
        }
        Err(e) => {
            tracing::error!("Failed to update orderbook cache: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(SetOrderbookResponse {
                    success: false,
                    symbol,
                    bids_count: 0,
                    asks_count: 0,
                }),
            )
                .into_response()
        }
    }
}

// ==================== Helper Functions ====================

fn normalize_symbol(symbol: &str) -> String {
    symbol.to_uppercase().replace("/", "").replace("-", "")
}
