//! K-Line API Handlers
//!
//! REST API endpoints for K-line/candlestick data:
//! - GET /markets/{symbol}/candles - Get historical candles
//! - GET /markets/{symbol}/candles/latest - Get latest candle
//! - GET /klines - Get klines directly from Binance
//! - POST /internal/klines/import - Batch import historical K-lines

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::str::FromStr;
use std::sync::Arc;

use crate::services::kline::{Candle, HistoricalKline, KlinePeriod};
use crate::AppState;

/// Normalize symbol format to backend format (BTCUSDT)
/// Supports multiple input formats:
/// - "BTCUSDT" -> "BTCUSDT" (already correct)
/// - "BTC-USD" -> "BTCUSDT" (frontend TradingView format)
/// - "BTC-USDT" -> "BTCUSDT"
/// - "btcusdt" -> "BTCUSDT" (lowercase)
fn normalize_symbol(symbol: &str) -> String {
    let upper = symbol.to_uppercase();
    
    // If already in BTCUSDT format (no separators), return as is
    if !upper.contains('-') && !upper.contains('/') && !upper.contains('_') {
        return upper;
    }
    
    // Handle BTC-USD format (convert to BTCUSDT)
    if upper.ends_with("-USD") {
        let base = upper.strip_suffix("-USD").unwrap_or(&upper);
        return format!("{}USDT", base);
    }
    
    // Handle BTC-USDT format (convert to BTCUSDT)
    if upper.contains("-USDT") {
        return upper.replace("-", "");
    }
    
    // Handle BTC/USD or BTC_USD formats
    if upper.contains("/") || upper.contains("_") {
        let cleaned = upper.replace("/", "").replace("_", "");
        if !cleaned.ends_with("USDT") && cleaned.ends_with("USD") {
            let base = cleaned.strip_suffix("USD").unwrap_or(&cleaned);
            return format!("{}USDT", base);
        }
        return cleaned;
    }
    
    // Default: return uppercase version
    upper
}

/// Query parameters for historical candles
#[derive(Debug, Deserialize)]
pub struct CandlesQuery {
    /// Time period: 1m, 5m, 15m, 1h, 4h, 1d, 1w, 1M
    pub period: String,
    /// Maximum number of candles to return (default: 300, max: 10000)
    #[serde(default = "default_limit")]
    pub limit: usize,
    /// Start time (Unix seconds)
    #[serde(alias = "start")]
    pub from: Option<i64>,
    /// End time (Unix seconds)
    #[serde(alias = "end")]
    pub to: Option<i64>,
}

fn default_limit() -> usize {
    300
}

/// Response for historical candles
#[derive(Debug, Serialize)]
pub struct CandlesResponse {
    pub symbol: String,
    pub period: String,
    pub candles: Vec<CandleDto>,
}

/// Response for latest candle
#[derive(Debug, Serialize)]
pub struct LatestCandleResponse {
    pub symbol: String,
    pub period: String,
    pub candle: CandleDto,
    pub is_final: bool,
}

/// DTO for candle data
#[derive(Debug, Clone, Serialize)]
pub struct CandleDto {
    pub time: i64,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub volume: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quote_volume: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trade_count: Option<u32>,
}

impl From<Candle> for CandleDto {
    fn from(c: Candle) -> Self {
        Self {
            time: c.time, // Return as seconds (Unix timestamp)
            open: c.open.to_string(),
            high: c.high.to_string(),
            low: c.low.to_string(),
            close: c.close.to_string(),
            volume: c.volume.to_string(),
            quote_volume: c.quote_volume.map(|v| v.to_string()),
            trade_count: c.trade_count,
        }
    }
}

/// Error response
#[derive(Debug, Serialize)]
pub struct KlineError {
    pub error: String,
    pub message: String,
    pub code: String,
}

/// Get historical candles
///
/// GET /api/v1/markets/{symbol}/candles?period=5m&limit=100
pub async fn get_candles(
    State(state): State<Arc<AppState>>,
    Path(symbol): Path<String>,
    Query(query): Query<CandlesQuery>,
) -> impl IntoResponse {
    // Normalize symbol format (supports BTC-USD, BTCUSDT, etc.)
    let normalized_symbol = normalize_symbol(&symbol);
    
    // Validate period
    let period = match KlinePeriod::from_str(&query.period) {
        Some(p) => p,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(KlineError {
                    error: "invalid_period".to_string(),
                    message: "Period must be one of: 1m, 5m, 15m, 1h, 4h, 1d, 1w, 1M".to_string(),
                    code: "ERR_INVALID_PERIOD".to_string(),
                }),
            )
                .into_response();
        }
    };

    // Validate limit
    let limit = query.limit.min(10000).max(1);

    // Handle milliseconds to seconds conversion for time range
    // If timestamp > 10,000,000,000, it's likely in milliseconds
    let from = query.from.map(|t| if t > 10_000_000_000 { t / 1000 } else { t });
    let to = query.to.map(|t| if t > 10_000_000_000 { t / 1000 } else { t });

    // Validate time range
    if let (Some(f), Some(t)) = (from, to) {
        if f > t {
            return (
                StatusCode::BAD_REQUEST,
                Json(KlineError {
                    error: "invalid_time_range".to_string(),
                    message: "from must be less than or equal to to".to_string(),
                    code: "ERR_INVALID_TIME_RANGE".to_string(),
                }),
            )
                .into_response();
        }
    }

        // Validate symbol visibility
    if !state.market_config_service.is_visible(&normalized_symbol).await {
        return (
            StatusCode::NOT_FOUND,
            Json(KlineError {
                error: "not_found".to_string(),
                message: format!("Trading pair not found or not listed yet: {}", normalized_symbol),
                code: "ERR_MARKET_NOT_FOUND".to_string(),
            }),
        ).into_response();
    }

    // Get candles from internal service first
    let candles = state
        .kline_service
        .get_candles(&normalized_symbol, period, limit, from, to)
        .await;

    let candle_dtos: Vec<CandleDto> = candles.into_iter().map(|c| c.into()).collect();

    // If internal data is empty, try Hyperliquid for RWA/external symbols
    if candle_dtos.is_empty() {
        if let Some(hl_coin) = crate::services::rwa::registry::get_hl_candle_coin(&normalized_symbol) {
            tracing::info!(
                "No internal candles for {}, fetching from Hyperliquid (coin={})",
                normalized_symbol, hl_coin
            );
            let client = crate::services::external_data::HyperliquidClient::new();
            match client.get_candles_by_coin(&hl_coin, &query.period, limit, from, to).await {
                Ok(external_candles) => {
                    return Json(CandlesResponse {
                        symbol: normalized_symbol,
                        period: query.period,
                        candles: external_candles,
                    })
                    .into_response();
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to fetch external candles for {} (coin={}): {}",
                        normalized_symbol, hl_coin, e
                    );
                }
            }
        }
    }

    Json(CandlesResponse {
        symbol: normalized_symbol,
        period: query.period,
        candles: candle_dtos,
    })
    .into_response()
}

/// Get latest candle
///
/// GET /api/v1/markets/{symbol}/candles/latest?period=5m
pub async fn get_latest_candle(
    State(state): State<Arc<AppState>>,
    Path(symbol): Path<String>,
    Query(query): Query<CandlesQuery>,
) -> impl IntoResponse {
    // Normalize symbol format (supports BTC-USD, BTCUSDT, etc.)
    let normalized_symbol = normalize_symbol(&symbol);
    
    // Validate period
    let period = match KlinePeriod::from_str(&query.period) {
        Some(p) => p,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(KlineError {
                    error: "invalid_period".to_string(),
                    message: "Period must be one of: 1m, 5m, 15m, 1h, 4h, 1d, 1w, 1M".to_string(),
                    code: "ERR_INVALID_PERIOD".to_string(),
                }),
            )
                .into_response();
        }
    };

    // Validate symbol visibility
    if !state.market_config_service.is_visible(&normalized_symbol).await {
        return (
            StatusCode::NOT_FOUND,
            Json(KlineError {
                error: "not_found".to_string(),
                message: format!("Trading pair not found or not listed yet: {}", normalized_symbol),
                code: "ERR_MARKET_NOT_FOUND".to_string(),
            }),
        ).into_response();
    }

    // Get latest candle from service
    match state.kline_service.get_latest_candle(&normalized_symbol, period).await {
        Some((candle, is_final)) => Json(LatestCandleResponse {
            symbol: normalized_symbol,
            period: query.period,
            candle: candle.into(),
            is_final,
        })
        .into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(KlineError {
                error: "no_data".to_string(),
                message: "No candle data available for this symbol and period".to_string(),
                code: "ERR_NO_DATA".to_string(),
            }),
        )
            .into_response(),
    }
}

/// Query parameters for Binance klines
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct BinanceKlinesQuery {
    /// Kline period: 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M
    pub period: String,
    /// Number of klines to return (default: 500, max: 1500)
    #[serde(default = "default_binance_limit")]
    pub limit: usize,
    /// Start time in milliseconds
    pub start: Option<i64>,
    /// End time in milliseconds
    pub end: Option<i64>,
}

#[allow(dead_code)]
fn default_binance_limit() -> usize {
    500
}

/// Response for Binance klines (matching candles format)
#[allow(dead_code)]
#[derive(Debug, Serialize)]
pub struct BinanceKlinesResponse {
    pub symbol: String,
    pub period: String,
    pub candles: Vec<CandleDto>,
}

/// Get klines directly from Binance
///
/// GET /api/v1/klines/:symbol/candles?period=5m&limit=300&start=1765521170972&end=1765539170972
#[allow(dead_code)]
pub async fn get_binance_klines(
    Path(symbol): Path<String>,
    Query(query): Query<BinanceKlinesQuery>,
) -> impl IntoResponse {
    // Validate limit
    let limit = query.limit.min(1500).max(1);

    // Build Binance API URL
    let mut url = format!(
        "https://fapi.binance.com/fapi/v1/klines?symbol={}&interval={}&limit={}",
        symbol, query.period, limit
    );

    // Add optional time parameters
    if let Some(start) = query.start {
        url.push_str(&format!("&startTime={}", start));
    }
    if let Some(end) = query.end {
        url.push_str(&format!("&endTime={}", end));
    }

    // Create HTTP client
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .unwrap_or_default();

    // Fetch data from Binance
    match client.get(&url).send().await {
        Ok(response) => {
            match response.json::<Vec<Value>>().await {
                Ok(data) => {
                    // Convert Binance klines format to our candles format
                    // Binance klines format:
                    // [
                    //   [
                    //     1499040000000,      // 0: Open time
                    //     "0.01634000",       // 1: Open
                    //     "0.80000000",       // 2: High
                    //     "0.01575800",       // 3: Low
                    //     "0.01577100",       // 4: Close
                    //     "148976.11427815",  // 5: Volume
                    //     1499644799999,      // 6: Close time
                    //     "2434.19055334",    // 7: Quote asset volume
                    //     308,                // 8: Number of trades
                    //     "1756.87402397",    // 9: Taker buy base asset volume
                    //     "28.46694368",      // 10: Taker buy quote asset volume
                    //     "0"                 // 11: Ignore
                    //   ]
                    // ]
                    let candles: Vec<CandleDto> = data
                        .into_iter()
                        .filter_map(|kline| {
                            if let Value::Array(arr) = kline {
                                if arr.len() >= 9 {
                                    // Extract values from Binance kline array
                                    let time = arr[0].as_i64().unwrap_or(0) / 1000; // Convert ms to seconds
                                    let open = arr[1].as_str().unwrap_or("0").to_string();
                                    let high = arr[2].as_str().unwrap_or("0").to_string();
                                    let low = arr[3].as_str().unwrap_or("0").to_string();
                                    let close = arr[4].as_str().unwrap_or("0").to_string();
                                    let volume = arr[5].as_str().unwrap_or("0").to_string();
                                    let quote_volume = arr[7].as_str().map(|s| s.to_string());
                                    let trade_count = arr[8].as_u64().map(|n| n as u32);

                                    return Some(CandleDto {
                                        time,
                                        open,
                                        high,
                                        low,
                                        close,
                                        volume,
                                        quote_volume,
                                        trade_count,
                                    });
                                }
                            }
                            None
                        })
                        .collect();

                    Json(BinanceKlinesResponse {
                        symbol: symbol.clone(),
                        period: query.period,
                        candles,
                    })
                    .into_response()
                }
                Err(e) => {
                    tracing::error!("Failed to parse Binance response: {}", e);
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(KlineError {
                            error: "parse_error".to_string(),
                            message: format!("Failed to parse Binance response: {}", e),
                            code: "ERR_PARSE_ERROR".to_string(),
                        }),
                    )
                        .into_response()
                }
            }
        }
        Err(e) => {
            tracing::error!("Failed to fetch klines from Binance: {}", e);
            (
                StatusCode::BAD_GATEWAY,
                Json(KlineError {
                    error: "binance_error".to_string(),
                    message: format!("Failed to fetch data from Binance: {}", e),
                    code: "ERR_BINANCE_ERROR".to_string(),
                }),
            )
                .into_response()
        }
    }
}

// ============================================================================
// Batch K-line Import API
// ============================================================================

/// Request body for batch K-line import
#[derive(Debug, Deserialize)]
pub struct BatchImportRequest {
    /// Array of K-lines to import
    pub klines: Vec<ImportKlineDto>,
}

/// DTO for K-line import (matches Python script format)
#[derive(Debug, Deserialize)]
pub struct ImportKlineDto {
    pub symbol: String,
    pub period: String,
    /// Unix timestamp in milliseconds
    pub open_time: i64,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub volume: String,
    #[serde(default)]
    pub quote_volume: Option<String>,
    #[serde(default)]
    pub trade_count: Option<i32>,
}

/// Response for batch import
#[derive(Debug, Serialize)]
pub struct BatchImportResponse {
    /// Total number of K-lines in the request
    pub total: usize,
    /// Number successfully imported
    pub imported: usize,
    /// Number of errors
    pub errors: usize,
    /// Success status
    pub success: bool,
    /// Optional error details
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_details: Option<Vec<String>>,
}

/// Batch import historical K-lines
///
/// POST /api/v1/internal/klines/import
///
/// This endpoint is designed for internal use to import historical K-line data.
/// It accepts batches of K-lines and stores them in the database.
///
/// Example request body:
/// ```json
/// {
///   "klines": [
///     {
///       "symbol": "BTCUSDT",
///       "period": "1m",
///       "open_time": 1704067200,
///       "open": "42000.50",
///       "high": "42100.00",
///       "low": "41900.00",
///       "close": "42050.00",
///       "volume": "123.45",
///       "quote_volume": "5185125.0",
///       "trade_count": 1234
///     }
///   ]
/// }
/// ```
pub async fn batch_import_klines(
    State(state): State<Arc<AppState>>,
    Json(request): Json<BatchImportRequest>,
) -> impl IntoResponse {
    let total = request.klines.len();
    
    if total == 0 {
        return (
            StatusCode::BAD_REQUEST,
            Json(BatchImportResponse {
                total: 0,
                imported: 0,
                errors: 0,
                success: false,
                error_details: Some(vec!["No K-lines provided".to_string()]),
            }),
        )
            .into_response();
    }

    tracing::info!("📥 Batch import request received: {} K-lines", total);

    // Convert DTOs to HistoricalKline
    let mut historical_klines = Vec::new();
    let mut conversion_errors = Vec::new();

    for (idx, dto) in request.klines.iter().enumerate() {
        // Validate period
        if KlinePeriod::from_str(&dto.period).is_none() {
            conversion_errors.push(format!(
                "K-line #{}: Invalid period '{}'", 
                idx + 1, 
                dto.period
            ));
            continue;
        }

        // Parse decimal values
        let open = match Decimal::from_str(&dto.open) {
            Ok(v) => v,
            Err(e) => {
                conversion_errors.push(format!(
                    "K-line #{}: Invalid open price '{}': {}", 
                    idx + 1, 
                    dto.open, 
                    e
                ));
                continue;
            }
        };

        let high = match Decimal::from_str(&dto.high) {
            Ok(v) => v,
            Err(e) => {
                conversion_errors.push(format!(
                    "K-line #{}: Invalid high price '{}': {}", 
                    idx + 1, 
                    dto.high, 
                    e
                ));
                continue;
            }
        };

        let low = match Decimal::from_str(&dto.low) {
            Ok(v) => v,
            Err(e) => {
                conversion_errors.push(format!(
                    "K-line #{}: Invalid low price '{}': {}", 
                    idx + 1, 
                    dto.low, 
                    e
                ));
                continue;
            }
        };

        let close = match Decimal::from_str(&dto.close) {
            Ok(v) => v,
            Err(e) => {
                conversion_errors.push(format!(
                    "K-line #{}: Invalid close price '{}': {}", 
                    idx + 1, 
                    dto.close, 
                    e
                ));
                continue;
            }
        };

        let volume = match Decimal::from_str(&dto.volume) {
            Ok(v) => v,
            Err(e) => {
                conversion_errors.push(format!(
                    "K-line #{}: Invalid volume '{}': {}", 
                    idx + 1, 
                    dto.volume, 
                    e
                ));
                continue;
            }
        };

        let quote_volume = if let Some(ref qv_str) = dto.quote_volume {
            match Decimal::from_str(qv_str) {
                Ok(v) => Some(v),
                Err(e) => {
                    conversion_errors.push(format!(
                        "K-line #{}: Invalid quote_volume '{}': {}", 
                        idx + 1, 
                        qv_str, 
                        e
                    ));
                    continue;
                }
            }
        } else {
            None
        };

        // Validate basic constraints
        if high < low {
            conversion_errors.push(format!(
                "K-line #{}: High ({}) cannot be less than Low ({})", 
                idx + 1, 
                high, 
                low
            ));
            continue;
        }

        if open < low || open > high {
            conversion_errors.push(format!(
                "K-line #{}: Open ({}) must be between Low ({}) and High ({})", 
                idx + 1, 
                open, 
                low, 
                high
            ));
            continue;
        }

        if close < low || close > high {
            conversion_errors.push(format!(
                "K-line #{}: Close ({}) must be between Low ({}) and High ({})", 
                idx + 1, 
                close, 
                low, 
                high
            ));
            continue;
        }

        // Create HistoricalKline
        historical_klines.push(HistoricalKline {
            symbol: dto.symbol.to_uppercase(), // Normalize symbol to uppercase
            period: dto.period.clone(),
            open_time: dto.open_time,
            open,
            high,
            low,
            close,
            volume,
            quote_volume,
            trade_count: dto.trade_count,
        });
    }

    // Log conversion errors if any
    if !conversion_errors.is_empty() {
        tracing::warn!(
            "⚠️  {} K-lines failed validation: {:?}", 
            conversion_errors.len(),
            conversion_errors.iter().take(5).collect::<Vec<_>>()
        );
    }

    // Save to database
    let imported = match state.kline_service.save_klines_batch(&historical_klines).await {
        Ok(count) => {
            tracing::info!("✅ Successfully imported {} K-lines to database", count);
            count
        }
        Err(e) => {
            tracing::error!("❌ Database error during batch import: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(BatchImportResponse {
                    total,
                    imported: 0,
                    errors: total,
                    success: false,
                    error_details: Some(vec![format!("Database error: {}", e)]),
                }),
            )
                .into_response();
        }
    };

    let errors = total - imported;
    let success = errors == 0;

    let response = BatchImportResponse {
        total,
        imported,
        errors,
        success,
        error_details: if conversion_errors.is_empty() {
            None
        } else {
            Some(conversion_errors)
        },
    };

    if success {
        tracing::info!("✅ Batch import completed successfully: {}/{} K-lines", imported, total);
        (StatusCode::OK, Json(response)).into_response()
    } else {
        tracing::warn!("⚠️  Batch import completed with errors: {}/{} K-lines imported, {} errors", 
            imported, total, errors);
        (StatusCode::PARTIAL_CONTENT, Json(response)).into_response()
    }
}

/// Response for repair operation
#[derive(Debug, Serialize)]
pub struct RepairKlinesResponse {
    pub success: bool,
    pub message: String,
    pub deleted_count: u64,
    pub imported_count: usize,
    pub symbols: Vec<String>,
    pub periods: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Query parameters for repair operation
#[derive(Debug, Deserialize)]
pub struct RepairKlinesQuery {
    /// Specific symbol to repair (optional, if not provided repairs all configured symbols)
    pub symbol: Option<String>,
    /// Specific period to repair (optional, if not provided repairs all periods)
    pub period: Option<String>,
    /// Number of days to fetch historical data (default: 30, max: 365)
    #[serde(default = "default_repair_days")]
    pub days: u32,
}

fn default_repair_days() -> u32 {
    30
}

/// Repair K-lines by deleting existing data and fetching from Binance
///
/// GET /api/v1/internal/klines/repair
///
/// This endpoint:
/// 1. Deletes existing K-line data for the specified symbol/period
/// 2. Fetches fresh data from Binance Futures API (fapi.binance.com)
/// 3. Imports the fetched data into the database
///
/// Query parameters:
/// - symbol: Optional specific symbol (e.g., "BTCUSDT"). If not provided, repairs all configured symbols.
/// - period: Optional specific period (e.g., "1m", "5m", "1h"). If not provided, repairs all periods.
/// - days: Number of days of historical data to fetch (default: 30, max: 365)
///
/// Example:
/// - GET /api/v1/internal/klines/repair - Repair all symbols and periods (last 30 days)
/// - GET /api/v1/internal/klines/repair?symbol=BTCUSDT - Repair BTCUSDT all periods
/// - GET /api/v1/internal/klines/repair?symbol=BTCUSDT&period=1h&days=7 - Repair BTCUSDT 1h for last 7 days
pub async fn repair_klines(
    State(state): State<Arc<AppState>>,
    Query(query): Query<RepairKlinesQuery>,
) -> impl IntoResponse {
    tracing::info!(
        "🔧 K-line repair initiated: symbol={:?}, period={:?}, days={}",
        query.symbol,
        query.period,
        query.days
    );

    // Limit days to prevent excessive API calls
    let days = query.days.min(365);

    // Determine symbols to repair
    let symbols_to_repair: Vec<String> = if let Some(ref sym) = query.symbol {
        vec![sym.to_uppercase()]
    } else {
        // Use configured trading pairs from DB
        state.market_config_service.get_trading_symbols().await
    };

    // Determine periods to repair
    let periods_to_repair: Vec<String> = if let Some(ref per) = query.period {
        vec![per.clone()]
    } else {
        vec!["1m".to_string(), "5m".to_string(), "15m".to_string(), "1h".to_string(), "4h".to_string(), "1d".to_string()]
    };

    tracing::info!(
        "📋 Repair plan: {} symbols × {} periods = {} combinations",
        symbols_to_repair.len(),
        periods_to_repair.len(),
        symbols_to_repair.len() * periods_to_repair.len()
    );

    // Step 1: Delete existing K-lines
    let deleted_count = match state.kline_service.delete_klines(
        query.symbol.as_deref(),
        query.period.as_deref(),
    ).await {
        Ok(count) => {
            tracing::info!("✅ Deleted {} existing K-line records", count);
            count
        }
        Err(e) => {
            tracing::error!("❌ Failed to delete existing K-lines: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(RepairKlinesResponse {
                    success: false,
                    message: "Failed to delete existing K-lines".to_string(),
                    deleted_count: 0,
                    imported_count: 0,
                    symbols: symbols_to_repair,
                    periods: periods_to_repair,
                    error: Some(format!("Database error: {}", e)),
                }),
            )
                .into_response();
        }
    };

    // Step 2: Fetch and import from Binance
    let client = reqwest::Client::new();
    let mut total_imported = 0;
    let mut all_klines: Vec<crate::services::kline::HistoricalKline> = Vec::new();

    let end_time = chrono::Utc::now().timestamp() * 1000; // milliseconds
    let start_time = end_time - (days as i64 * 24 * 60 * 60 * 1000);

    for symbol in &symbols_to_repair {
        for period in &periods_to_repair {
            tracing::info!("📥 Fetching {} {} from Binance...", symbol, period);

            // Binance API URL
            let url = format!(
                "https://fapi.binance.com/fapi/v1/klines?symbol={}&interval={}&startTime={}&endTime={}&limit=1500",
                symbol, period, start_time, end_time
            );

            match client.get(&url).send().await {
                Ok(response) => {
                    if response.status().is_success() {
                        match response.json::<Vec<Value>>().await {
                            Ok(klines_data) => {
                                tracing::info!("✅ Fetched {} K-lines for {} {}", klines_data.len(), symbol, period);

                                // Convert Binance format to our format
                                for kline_array in klines_data {
                                    if let Value::Array(arr) = kline_array {
                                        if arr.len() >= 11 {
                                            // Binance kline format:
                                            // [0] open_time, [1] open, [2] high, [3] low, [4] close,
                                            // [5] volume, [6] close_time, [7] quote_volume, [8] trade_count, ...
                                            let open_time = arr[0].as_i64().unwrap_or(0); // Keep milliseconds
                                            let open = arr[1].as_str().unwrap_or("0");
                                            let high = arr[2].as_str().unwrap_or("0");
                                            let low = arr[3].as_str().unwrap_or("0");
                                            let close = arr[4].as_str().unwrap_or("0");
                                            let volume = arr[5].as_str().unwrap_or("0");
                                            let quote_volume = arr[7].as_str().unwrap_or("0");
                                            let trade_count = arr[8].as_i64().unwrap_or(0) as i32;

                                            all_klines.push(HistoricalKline {
                                                symbol: symbol.clone(),
                                                period: period.clone(),
                                                open_time,
                                                open: Decimal::from_str(open).unwrap_or_default(),
                                                high: Decimal::from_str(high).unwrap_or_default(),
                                                low: Decimal::from_str(low).unwrap_or_default(),
                                                close: Decimal::from_str(close).unwrap_or_default(),
                                                volume: Decimal::from_str(volume).unwrap_or_default(),
                                                quote_volume: Some(Decimal::from_str(quote_volume).unwrap_or_default()),
                                                trade_count: Some(trade_count),
                                            });
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::error!("❌ Failed to parse Binance response for {} {}: {}", symbol, period, e);
                            }
                        }
                    } else {
                        tracing::error!("❌ Binance API returned error for {} {}: {}", symbol, period, response.status());
                    }
                }
                Err(e) => {
                    tracing::error!("❌ Failed to fetch from Binance for {} {}: {}", symbol, period, e);
                }
            }

            // Add small delay to avoid rate limiting
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    }

    // Step 3: Batch import all fetched K-lines
    if !all_klines.is_empty() {
        match state.kline_service.save_klines_batch(&all_klines).await {
            Ok(count) => {
                total_imported = count;
                tracing::info!("✅ Successfully imported {} K-lines from Binance", count);
            }
            Err(e) => {
                tracing::error!("❌ Failed to import K-lines: {}", e);
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(RepairKlinesResponse {
                        success: false,
                        message: "Failed to import K-lines after fetching".to_string(),
                        deleted_count,
                        imported_count: 0,
                        symbols: symbols_to_repair,
                        periods: periods_to_repair,
                        error: Some(format!("Import error: {}", e)),
                    }),
                )
                    .into_response();
            }
        }
    }

    let message = format!(
        "K-line repair completed: deleted {} records, imported {} new records",
        deleted_count, total_imported
    );

    tracing::info!("🎉 {}", message);

    (
        StatusCode::OK,
        Json(RepairKlinesResponse {
            success: true,
            message,
            deleted_count,
            imported_count: total_imported,
            symbols: symbols_to_repair,
            periods: periods_to_repair,
            error: None,
        }),
    )
        .into_response()
}
