use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{error, info};
use uuid::Uuid;
use std::str::FromStr;

use crate::services::matching::TradeEvent;
use crate::AppState;

// ==================== Request/Response Types ====================

#[derive(Debug, Deserialize)]
pub struct CreateInternalTradeRequest {
    pub symbol: String,
    #[serde(deserialize_with = "deserialize_string_or_number")]
    pub price: String,
    #[serde(deserialize_with = "deserialize_string_or_number")]
    pub amount: String,
    pub side: String,
    pub timestamp: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct BatchTradesRequest {
    pub trades: Vec<CreateInternalTradeRequest>,
}

#[derive(Debug, Serialize)]
pub struct InternalTradeResponse {
    pub success: bool,
    pub trade_id: String,
    pub symbol: String,
    pub price: String,
    pub amount: String,
    pub side: String,
    pub timestamp: i64,
}

#[derive(Debug, Serialize)]
pub struct BatchTradesResponse {
    pub success: bool,
    pub created: usize,
    pub failed: usize,
}

#[derive(Debug, Serialize)]
pub struct ClearKlinesResponse {
    pub success: bool,
    pub message: String,
}

#[derive(Debug, Deserialize)]
pub struct ClearKlinesQuery {
    pub symbol: Option<String>,
    pub period: Option<String>,
}

// Helper function to deserialize string or number
fn deserialize_string_or_number<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;
    use serde_json::Value;

    let value = Value::deserialize(deserializer)?;
    match value {
        Value::String(s) => Ok(s),
        Value::Number(n) => Ok(n.to_string()),
        _ => Err(Error::custom("expected string or number")),
    }
}

// ==================== Handler Functions ====================

/// Create a single internal trade
/// POST /api/v1/internal/trade
pub async fn create_internal_trade(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateInternalTradeRequest>,
) -> impl IntoResponse {
    // Validate input
    if let Err(e) = validate_trade(&req) {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": e.message,
                "code": e.code
            })),
        )
            .into_response();
    }

    let trade_id = Uuid::new_v4().to_string();
    let timestamp = req.timestamp.unwrap_or_else(|| chrono::Utc::now().timestamp());

    // Parse price and amount
    let price: f64 = match req.price.parse() {
        Ok(p) => p,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "Invalid price format",
                    "code": "INVALID_PRICE"
                })),
            )
                .into_response();
        }
    };

    let amount: f64 = match req.amount.parse() {
        Ok(a) => a,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "Invalid amount format",
                    "code": "INVALID_AMOUNT"
                })),
            )
                .into_response();
        }
    };

    // Normalize symbol
    let symbol = normalize_symbol(&req.symbol);

    // Store internal trade to virtual_trades table for price cache hydration
    // This ensures ticker data survives system restarts
    let timestamp_ms = timestamp * 1000; // Convert to milliseconds
    let query = r#"
        INSERT INTO virtual_trades (id, symbol, side, price, amount, timestamp, source)
        VALUES ($1, $2, $3::order_side, $4, $5, $6, $7)
        ON CONFLICT (id) DO NOTHING
    "#;
    
    if let Err(e) = sqlx::query(query)
        .bind(&trade_id)
        .bind(&symbol)
        .bind(&req.side)
        .bind(&req.price)
        .bind(&req.amount)
        .bind(timestamp_ms)
        .bind("internal")
        .execute(&state.db.pool)
        .await
    {
        error!("Failed to store internal trade to virtual_trades: {}", e);
        // Don't fail the request, just log the error
    }

    // Update price cache
    if let Some(price_cache) = state.cache.price_opt() {
        if let Err(e) = update_price_cache(&price_cache, &symbol, price).await {
            error!("Failed to update price cache: {}", e);
        }
    }

    // Publish trade to WebSocket subscribers
    if let Some(pubsub) = state.cache.pubsub_opt() {
        if let Err(e) = publish_trade(&pubsub, &symbol, price, amount, &req.side, timestamp).await
        {
            error!("Failed to publish trade: {}", e);
        }
    }

    // Update kline data (DB persistence for current candle)
    if let Err(e) = update_kline(&state.db, &symbol, price, amount, timestamp).await {
        error!("Failed to update kline: {}", e);
    }

    // Process trade in KlineService (updates in-memory state and broadcasts to WebSocket)
    if let (Ok(price_dec), Ok(amount_dec)) = (Decimal::from_str(&req.price), Decimal::from_str(&req.amount)) {
        let trade_event = TradeEvent {
            symbol: symbol.clone(),
            trade_id: Uuid::new_v4(),
            maker_order_id: Uuid::nil(),
            taker_order_id: Uuid::nil(),
            maker_address: String::new(),
            taker_address: String::new(),
            side: req.side.clone(),
            price: price_dec,
            amount: amount_dec,
            maker_fee: Decimal::ZERO,
            taker_fee: Decimal::ZERO,
            timestamp: timestamp * 1000,
            maker_leverage: 1,
            taker_leverage: 1,
            is_self_trade: false,
        };
        state.kline_service.process_trade(&trade_event).await;
        
        // Update price feed service for ticker data
        state.price_feed_service.update_price_from_trade(&symbol, price_dec, amount_dec).await;
    }

    info!(
        "Created internal trade: {} {} {} @ {}",
        req.side, amount, symbol, price
    );

    (
        StatusCode::OK,
        Json(InternalTradeResponse {
            success: true,
            trade_id,
            symbol: req.symbol,
            price: req.price,
            amount: req.amount,
            side: req.side,
            timestamp,
        }),
    )
        .into_response()
}

/// Create multiple internal trades
/// POST /api/v1/internal/trades/batch
pub async fn batch_create_trades(
    State(state): State<Arc<AppState>>,
    Json(req): Json<BatchTradesRequest>,
) -> impl IntoResponse {
    let mut created = 0;
    let mut failed = 0;

    for trade in req.trades {
        // Validate trade
        if let Err(e) = validate_trade(&trade) {
            error!("Invalid trade in batch: {}", e.message);
            failed += 1;
            continue;
        }

        let trade_id = Uuid::new_v4().to_string();
        let timestamp = trade.timestamp.unwrap_or_else(|| chrono::Utc::now().timestamp());

        // Parse price and amount
        let price: f64 = match trade.price.parse() {
            Ok(p) => p,
            Err(_) => {
                failed += 1;
                continue;
            }
        };

        let amount: f64 = match trade.amount.parse() {
            Ok(a) => a,
            Err(_) => {
                failed += 1;
                continue;
            }
        };

        let symbol = normalize_symbol(&trade.symbol);

        // Store internal trade to virtual_trades table for price cache hydration
        let timestamp_ms = timestamp * 1000; // Convert to milliseconds
        let query = r#"
            INSERT INTO virtual_trades (id, symbol, side, price, amount, timestamp, source)
            VALUES ($1, $2, $3::order_side, $4, $5, $6, $7)
            ON CONFLICT (id) DO NOTHING
        "#;
        
        if let Err(e) = sqlx::query(query)
            .bind(&trade_id)
            .bind(&symbol)
            .bind(&trade.side)
            .bind(&trade.price)
            .bind(&trade.amount)
            .bind(timestamp_ms)
            .bind("internal_batch")
            .execute(&state.db.pool)
            .await
        {
            error!("Failed to store batch trade to virtual_trades: {}", e);
            // Continue processing other trades
        }
        
        created += 1;

        // Update price cache (don't fail batch if this fails)
        if let Some(price_cache) = state.cache.price_opt() {
            let _ = update_price_cache(&price_cache, &symbol, price).await;
        }

        // Publish trade  
        if let Some(pubsub) = state.cache.pubsub_opt() {
            let _ =
                publish_trade(&pubsub, &symbol, price, amount, &trade.side, timestamp)
                    .await;
        }

        // Update kline
        let _ = update_kline(&state.db, &symbol, price, amount, timestamp).await;

        // Process trade in KlineService (updates in-memory state and broadcasts to WebSocket)
        if let (Ok(price_dec), Ok(amount_dec)) = (Decimal::from_str(&trade.price), Decimal::from_str(&trade.amount)) {
        let trade_event = TradeEvent {
            symbol: symbol.clone(),
            trade_id: Uuid::new_v4(),
            maker_order_id: Uuid::nil(),
            taker_order_id: Uuid::nil(),
            maker_address: String::new(),
            taker_address: String::new(),
            side: trade.side.clone(),
            price: price_dec,
            amount: amount_dec,
            maker_fee: Decimal::ZERO,
            taker_fee: Decimal::ZERO,
            timestamp: timestamp * 1000,
            maker_leverage: 1,
            taker_leverage: 1,
            is_self_trade: false,
        };
            state.kline_service.process_trade(&trade_event).await;
            
            // Update price feed service for ticker data
            state.price_feed_service.update_price_from_trade(&symbol, price_dec, amount_dec).await;
        }
    }

    info!("Batch create trades: {} created, {} failed", created, failed);

    (
        StatusCode::OK,
        Json(BatchTradesResponse {
            success: created > 0,
            created,
            failed,
        }),
    )
        .into_response()
}

/// Clear K-line data for a symbol or all symbols
/// DELETE /api/v1/internal/klines/clear?symbol=BTCUSDT&period=5m
pub async fn clear_klines(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ClearKlinesQuery>,
) -> impl IntoResponse {
    // Use the kline_service.delete_klines method which handles all cases correctly
    let result = state
        .kline_service
        .delete_klines(
            query.symbol.as_deref(),
            query.period.as_deref(),
        )
        .await;

    match result {
        Ok(count) => {
            let message = match (&query.symbol, &query.period) {
                (Some(sym), Some(per)) => {
                    format!("Cleared {} K-line records for {} {}", count, sym, per)
                }
                (Some(sym), None) => {
                    format!("Cleared {} K-line records for {}", count, sym)
                }
                (None, Some(per)) => {
                    format!("Cleared {} K-line records for period {}", count, per)
                }
                (None, None) => {
                    format!("Cleared all {} K-line records", count)
                }
            };
            
            info!("{}", message);

            (
                StatusCode::OK,
                Json(ClearKlinesResponse {
                    success: true,
                    message,
                }),
            )
                .into_response()
        }
        Err(e) => {
            error!("Failed to clear klines: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ClearKlinesResponse {
                    success: false,
                    message: format!("Failed to clear klines: {}", e),
                }),
            )
                .into_response()
        }
    }
}

// ==================== Helper Functions ====================

fn validate_trade(trade: &CreateInternalTradeRequest) -> Result<(), ValidationError> {
    // Validate symbol
    if trade.symbol.is_empty() {
        return Err(ValidationError {
            code: "INVALID_SYMBOL".to_string(),
            message: "Symbol cannot be empty".to_string(),
        });
    }

    // Validate side
    if trade.side != "buy" && trade.side != "sell" {
        return Err(ValidationError {
            code: "INVALID_SIDE".to_string(),
            message: "Side must be 'buy' or 'sell'".to_string(),
        });
    }

    // Validate price
    let price: f64 = trade.price.parse().map_err(|_| ValidationError {
        code: "INVALID_PRICE".to_string(),
        message: "Invalid price format".to_string(),
    })?;

    if price <= 0.0 {
        return Err(ValidationError {
            code: "INVALID_PRICE".to_string(),
            message: "Price must be positive".to_string(),
        });
    }

    // Validate amount
    let amount: f64 = trade.amount.parse().map_err(|_| ValidationError {
        code: "INVALID_AMOUNT".to_string(),
        message: "Invalid amount format".to_string(),
    })?;

    if amount <= 0.0 {
        return Err(ValidationError {
            code: "INVALID_AMOUNT".to_string(),
            message: "Amount must be positive".to_string(),
        });
    }

    Ok(())
}

#[derive(Debug)]
struct ValidationError {
    code: String,
    message: String,
}

fn normalize_symbol(symbol: &str) -> String {
    symbol.to_uppercase().replace("/", "").replace("-", "")
}

#[allow(dead_code)]
async fn store_trade(
    db: &crate::db::Database,
    trade_id: &str,
    symbol: &str,
    price: f64,
    amount: f64,
    side: &str,
    timestamp: i64,
) -> Result<(), String> {
    let query = r#"
        INSERT INTO trades (id, symbol, price, amount, side, trade_time, created_at)
        VALUES ($1, $2, $3, $4, $5, to_timestamp($6), NOW())
    "#;

    sqlx::query(query)
        .bind(trade_id)
        .bind(symbol)
        .bind(price)
        .bind(amount)
        .bind(side)
        .bind(timestamp)
        .execute(&db.pool)
        .await
        .map_err(|e| e.to_string())?;

    Ok(())
}

async fn update_price_cache(
    price_cache: &crate::cache::price_cache::PriceCache,
    symbol: &str,
    price: f64,
) -> Result<(), String> {
    use std::str::FromStr;

    let price_decimal = Decimal::from_str(&price.to_string()).map_err(|e| e.to_string())?;

    price_cache
        .set_last_price(symbol, price_decimal)
        .await
        .map_err(|e| e.to_string())
}

async fn publish_trade(
    pubsub: &crate::cache::pubsub::PubSubManager,
    symbol: &str,
    price: f64,
    amount: f64,
    side: &str,
    timestamp: i64,
) -> Result<(), String> {
    let trade_data = serde_json::json!({
        "type": "trade",
        "symbol": symbol,
        "price": price.to_string(),
        "amount": amount.to_string(),
        "side": side,
        "timestamp": timestamp * 1000, // Convert to milliseconds
    });

    let channel = format!("trades:{}", symbol);
    pubsub
        .publisher()
        .publish(&channel, &trade_data.to_string())
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

async fn update_kline(
    db: &crate::db::Database,
    symbol: &str,
    price: f64,
    volume: f64,
    timestamp: i64,
) -> Result<(), String> {
    // Update 1-minute kline
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
