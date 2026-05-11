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
use tracing::{error, info};
use uuid::Uuid;

use tracing::Instrument;
use crate::services::matching::TradeEvent;
use crate::AppState;

// ==================== Request/Response Types ====================

#[derive(Debug, Deserialize)]
pub struct CreateVirtualOrderRequest {
    pub symbol: String,
    pub side: String, // "buy" or "sell"
    pub price: String,
    pub amount: String,
    pub source: Option<String>, // External exchange source like "binance"
}

#[derive(Debug, Deserialize)]
pub struct BatchVirtualOrdersRequest {
    pub orders: Vec<CreateVirtualOrderRequest>,
}

#[derive(Debug, Deserialize)]
pub struct CreateVirtualTradeRequest {
    pub symbol: String,
    pub side: String,
    pub price: String,
    pub amount: String,
    pub timestamp: Option<i64>, // Unix timestamp in seconds
    pub source: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct BatchVirtualTradesRequest {
    pub trades: Vec<CreateVirtualTradeRequest>,
}

#[derive(Debug, Serialize)]
pub struct VirtualOrderResponse {
    pub success: bool,
    pub order_id: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct BatchVirtualOrdersResponse {
    pub success: bool,
    pub created: usize,
    pub failed: usize,
}

#[derive(Debug, Serialize)]
pub struct VirtualTradeResponse {
    pub success: bool,
    pub trade_id: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct BatchVirtualTradesResponse {
    pub success: bool,
    pub created: usize,
    pub failed: usize,
}

// ==================== Handler Functions ====================

/// Create a single virtual order (for display only, doesn't go through matching engine)
/// POST /api/v1/internal/virtual/order
pub async fn create_virtual_order(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateVirtualOrderRequest>,
) -> impl IntoResponse {
    let order_id = Uuid::new_v4();
    let symbol = normalize_symbol(&req.symbol);
    
    // Validate side
    if req.side != "buy" && req.side != "sell" {
        return (
            StatusCode::BAD_REQUEST,
            Json(VirtualOrderResponse {
                success: false,
                order_id: None,
                error: Some("Invalid side, must be 'buy' or 'sell'".to_string()),
            }),
        );
    }
    
    // Parse and validate price and amount
    let price = match Decimal::from_str(&req.price) {
        Ok(p) if p > Decimal::ZERO => p,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(VirtualOrderResponse {
                    success: false,
                    order_id: None,
                    error: Some("Invalid price".to_string()),
                }),
            );
        }
    };
    
    let amount = match Decimal::from_str(&req.amount) {
        Ok(a) if a > Decimal::ZERO => a,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(VirtualOrderResponse {
                    success: false,
                    order_id: None,
                    error: Some("Invalid amount".to_string()),
                }),
            );
        }
    };
    
    // Insert into virtual_orders table
    let query = r#"
        INSERT INTO virtual_orders (id, symbol, side, price, amount, source, status)
        VALUES ($1, $2, $3::order_side, $4, $5, $6, 'open'::order_status)
    "#;
    
    match sqlx::query(query)
        .bind(order_id)
        .bind(&symbol)
        .bind(&req.side)
        .bind(price)
        .bind(amount)
        .bind(req.source.as_deref().unwrap_or("internal"))
        .execute(&state.db.pool)
        .await
    {
        Ok(_) => {
            info!(
                "Created virtual order: {} {} {} @ {} (source: {})",
                req.side,
                amount,
                symbol,
                price,
                req.source.as_deref().unwrap_or("internal")
            );
            
            (
                StatusCode::OK,
                Json(VirtualOrderResponse {
                    success: true,
                    order_id: Some(order_id.to_string()),
                    error: None,
                }),
            )
        }
        Err(e) => {
            error!("Failed to create virtual order: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(VirtualOrderResponse {
                    success: false,
                    order_id: None,
                    error: Some(format!("Database error: {}", e)),
                }),
            )
        }
    }
}

/// Create multiple virtual orders
/// POST /api/v1/internal/virtual/orders/batch
pub async fn batch_create_virtual_orders(
    State(state): State<Arc<AppState>>,
    Json(req): Json<BatchVirtualOrdersRequest>,
) -> impl IntoResponse {
    let mut created = 0;
    let mut failed = 0;
    
    for order in req.orders {
        let order_id = Uuid::new_v4();
        let symbol = normalize_symbol(&order.symbol);
        
        // Validate
        if order.side != "buy" && order.side != "sell" {
            failed += 1;
            continue;
        }
        
        let price = match Decimal::from_str(&order.price) {
            Ok(p) if p > Decimal::ZERO => p,
            _ => {
                failed += 1;
                continue;
            }
        };
        
        let amount = match Decimal::from_str(&order.amount) {
            Ok(a) if a > Decimal::ZERO => a,
            _ => {
                failed += 1;
                continue;
            }
        };
        
        // Insert
        let query = r#"
            INSERT INTO virtual_orders (id, symbol, side, price, amount, source, status)
            VALUES ($1, $2, $3::order_side, $4, $5, $6, 'open'::order_status)
        "#;
        
        match sqlx::query(query)
            .bind(order_id)
            .bind(&symbol)
            .bind(&order.side)
            .bind(price)
            .bind(amount)
            .bind(order.source.as_deref().unwrap_or("internal"))
            .execute(&state.db.pool)
            .await
        {
            Ok(_) => created += 1,
            Err(e) => {
                error!("Failed to create virtual order: {}", e);
                failed += 1;
            }
        }
    }
    
    info!(
        "Batch created virtual orders: {} created, {} failed",
        created, failed
    );
    
    (
        StatusCode::OK,
        Json(BatchVirtualOrdersResponse {
            success: created > 0,
            created,
            failed,
        }),
    )
}

/// Create a virtual trade (affects K-lines, doesn't match with real orders)
/// POST /api/v1/internal/virtual/trade
pub async fn create_virtual_trade(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateVirtualTradeRequest>,
) -> impl IntoResponse {
    let trade_id = Uuid::new_v4();
    let symbol = normalize_symbol(&req.symbol);
    let timestamp = req.timestamp.unwrap_or_else(|| chrono::Utc::now().timestamp());
    
    // Validate side
    if req.side != "buy" && req.side != "sell" {
        return (
            StatusCode::BAD_REQUEST,
            Json(VirtualTradeResponse {
                success: false,
                trade_id: None,
                error: Some("Invalid side".to_string()),
            }),
        );
    }
    
    // Parse price and amount
    let price = match Decimal::from_str(&req.price) {
        Ok(p) if p > Decimal::ZERO => p,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(VirtualTradeResponse {
                    success: false,
                    trade_id: None,
                    error: Some("Invalid price".to_string()),
                }),
            );
        }
    };
    
    let amount = match Decimal::from_str(&req.amount) {
        Ok(a) if a > Decimal::ZERO => a,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(VirtualTradeResponse {
                    success: false,
                    trade_id: None,
                    error: Some("Invalid amount".to_string()),
                }),
            );
        }
    };
    
    // Store in virtual_trades table
    let query = r#"
        INSERT INTO virtual_trades (id, symbol, side, price, amount, timestamp, source)
        VALUES ($1, $2, $3::order_side, $4, $5, $6, $7)
    "#;
    
    match sqlx::query(query)
        .bind(trade_id)
        .bind(&symbol)
        .bind(&req.side)
        .bind(price)
        .bind(amount)
        .bind(timestamp * 1000) // Convert to milliseconds
        .bind(req.source.as_deref().unwrap_or("internal"))
        .execute(&state.db.pool)
        .await
    {
        Ok(_) => {}
        Err(e) => {
            error!("Failed to store virtual trade: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(VirtualTradeResponse {
                    success: false,
                    trade_id: None,
                    error: Some(format!("Database error: {}", e)),
                }),
            );
        }
    }
    
    // Update price cache
    if let Some(price_cache) = state.cache.price_opt() {
        let _price_f64 = price.to_string().parse::<f64>().unwrap_or(0.0);
        let _ = price_cache
            .set_last_price(&symbol, price)
            .await
            .map_err(|e| error!("Failed to update price cache: {}", e));
    }
    
    // Publish trade to WebSocket
    if let Some(pubsub) = state.cache.pubsub_opt() {
        let trade_data = serde_json::json!({
            "type": "trade",
            "symbol": symbol,
            "price": req.price,
            "amount": req.amount,
            "side": req.side,
            "timestamp": timestamp * 1000,
            "is_virtual": true,
        });
        
        let channel = format!("trades:{}", symbol);
        let _ = pubsub
            .publisher()
            .publish(&channel, &trade_data.to_string())
            .await;
    }
    
    // Spawn background tasks for K-line and price feed updates
    // to avoid blocking the response (optimization for high-frequency trading)
    let state_clone = state.clone();
    let symbol_clone = symbol.clone();
    let side_clone = req.side.clone();
    let price_f64 = price.to_string().parse::<f64>().unwrap_or(0.0);
    let amount_f64 = amount.to_string().parse::<f64>().unwrap_or(0.0);
    
    tokio::spawn(async move {
        // Update K-line data
        if let Err(e) = update_kline(&state_clone.db, &symbol_clone, price_f64, amount_f64, timestamp).await {
            error!("Failed to update kline: {}", e);
        }

        // Process in KlineService (broadcasts to WebSocket)
        let trade_event = TradeEvent {
            symbol: symbol_clone.clone(),
            trade_id,
            maker_order_id: Uuid::nil(),
            taker_order_id: Uuid::nil(),
            maker_address: String::new(),
            taker_address: String::new(),
            side: side_clone,
            price,
            amount,
            maker_fee: Decimal::ZERO,
            taker_fee: Decimal::ZERO,
            timestamp: timestamp * 1000,
            maker_leverage: 1,
            taker_leverage: 1,
            is_self_trade: false,
        };
        state_clone.kline_service.process_trade(&trade_event).await;

        // Update price feed service for 24h volume tracking
        state_clone.price_feed_service.update_price_from_trade(&symbol_clone, price, amount).await;
    }.instrument(tracing::info_span!("virtual-order-process")));
    
    info!(
        "Created virtual trade: {} {} {} @ {} (source: {})",
        req.side,
        amount,
        symbol,
        price,
        req.source.as_deref().unwrap_or("internal")
    );
    
    (
        StatusCode::OK,
        Json(VirtualTradeResponse {
            success: true,
            trade_id: Some(trade_id.to_string()),
            error: None,
        }),
    )
}

/// Create multiple virtual trades
/// POST /api/v1/internal/virtual/trades/batch
pub async fn batch_create_virtual_trades(
    State(state): State<Arc<AppState>>,
    Json(req): Json<BatchVirtualTradesRequest>,
) -> impl IntoResponse {
    let mut created = 0;
    let mut failed = 0;
    
    for trade in req.trades {
        let trade_id = Uuid::new_v4();
        let symbol = normalize_symbol(&trade.symbol);
        let timestamp = trade.timestamp.unwrap_or_else(|| chrono::Utc::now().timestamp());
        
        // Validate
        if trade.side != "buy" && trade.side != "sell" {
            failed += 1;
            continue;
        }
        
        let price = match Decimal::from_str(&trade.price) {
            Ok(p) if p > Decimal::ZERO => p,
            _ => {
                failed += 1;
                continue;
            }
        };
        
        let amount = match Decimal::from_str(&trade.amount) {
            Ok(a) if a > Decimal::ZERO => a,
            _ => {
                failed += 1;
                continue;
            }
        };
        
        // Store in database
        let query = r#"
            INSERT INTO virtual_trades (id, symbol, side, price, amount, timestamp, source)
            VALUES ($1, $2, $3::order_side, $4, $5, $6, $7)
        "#;
        
        match sqlx::query(query)
            .bind(trade_id)
            .bind(&symbol)
            .bind(&trade.side)
            .bind(price)
            .bind(amount)
            .bind(timestamp * 1000)
            .bind(trade.source.as_deref().unwrap_or("internal"))
            .execute(&state.db.pool)
            .await
        {
            Ok(_) => {
                // Update caches and K-lines
                if let Some(price_cache) = state.cache.price_opt() {
                    let _ = price_cache.set_last_price(&symbol, price).await;
                }
                
                if let Some(pubsub) = state.cache.pubsub_opt() {
                    let trade_data = serde_json::json!({
                        "type": "trade",
                        "symbol": symbol,
                        "price": trade.price,
                        "amount": trade.amount,
                        "side": trade.side,
                        "timestamp": timestamp * 1000,
                        "is_virtual": true,
                    });
                    let channel = format!("trades:{}", symbol);
                    let _ = pubsub.publisher().publish(&channel, &trade_data.to_string()).await;
                }
                
                let price_f64 = price.to_string().parse::<f64>().unwrap_or(0.0);
                let amount_f64 = amount.to_string().parse::<f64>().unwrap_or(0.0);
                let _ = update_kline(&state.db, &symbol, price_f64, amount_f64, timestamp).await;
                
                // Process in KlineService
                let trade_event = TradeEvent {
                    symbol: symbol.clone(),
                    trade_id,
                    maker_order_id: Uuid::nil(),
                    taker_order_id: Uuid::nil(),
                    maker_address: String::new(),
                    taker_address: String::new(),
                    side: trade.side.clone(),
                    price,
                    amount,
                    maker_fee: Decimal::ZERO,
                    taker_fee: Decimal::ZERO,
                    timestamp: timestamp * 1000,
                    maker_leverage: 1,
                    taker_leverage: 1,
                    is_self_trade: false,
                };
                state.kline_service.process_trade(&trade_event).await;
                
                // Update price feed service for 24h volume tracking
                state.price_feed_service.update_price_from_trade(&symbol, price, amount).await;
                
                created += 1;
            }
            Err(e) => {
                error!("Failed to create virtual trade: {}", e);
                failed += 1;
            }
        }
    }
    
    info!(
        "Batch created virtual trades: {} created, {} failed",
        created, failed
    );
    
    (
        StatusCode::OK,
        Json(BatchVirtualTradesResponse {
            success: created > 0,
            created,
            failed,
        }),
    )
}

// ==================== Helper Functions ====================

fn normalize_symbol(symbol: &str) -> String {
    symbol.to_uppercase().replace("/", "").replace("-", "")
}

async fn update_kline(
    db: &crate::db::Database,
    symbol: &str,
    price: f64,
    volume: f64,
    timestamp: i64,
) -> Result<(), String> {
    let kline_time = (timestamp / 60) * 60; // Round to minute
    
    let query = r#"
        INSERT INTO historical_klines (symbol, period, time, open, high, low, close, volume, is_final)
        VALUES ($1, '1m', to_timestamp($2), $3, $3, $3, $3, $4, false)
        ON CONFLICT (symbol, period, time) 
        DO UPDATE SET
            high = GREATEST(historical_klines.high, EXCLUDED.high),
            low = LEAST(historical_klines.low, EXCLUDED.low),
            close = EXCLUDED.close,
            volume = historical_klines.volume + EXCLUDED.volume,
            updated_at = NOW()
    "#;
    
    sqlx::query(query)
        .bind(symbol)
        .bind(kline_time)
        .bind(price)
        .bind(volume)
        .execute(&db.pool)
        .await
        .map_err(|e| e.to_string())?;
    
    Ok(())
}
