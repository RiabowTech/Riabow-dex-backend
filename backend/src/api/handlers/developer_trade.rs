//! Developer API — Trade Endpoints (Binance-compatible)
//!
//! Authenticated endpoints under `/fapi/v1/` for trading operations.
//! Uses HMAC-SHA256 via X-MBX-APIKEY (handled by auth_middleware).


use axum::{
    extract::{Query, State},
    http::StatusCode,
    Extension, Json,
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::sync::Arc;
use uuid::Uuid;

use crate::auth::middleware::AuthUser;
use crate::models::order::{OrderSide, OrderStatus, OrderType};
use crate::services::matching::{
    OrderStatus as MatchingOrderStatus, OrderType as MatchingOrderType, Side as MatchingSide,
    TimeInForce as MatchingTimeInForce,
};
use crate::AppState;

/// Margin buffer rate (0.5%) added on top of required margin for fees/slippage
const MARGIN_BUFFER_RATE: Decimal = Decimal::from_parts(5, 0, 0, false, 3); // 0.005

// ─── Helpers ──────────────────────────────────────────────────────

/// Accept boolean payloads as either real JSON booleans (`true`/`false`)
/// or Binance-style strings (`"true"`/`"false"`). Missing/null defaults
/// to `false` via `#[serde(default)]` on the field.
fn deserialize_bool_or_string<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::{self, Unexpected};
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum BoolOrString {
        Bool(bool),
        Str(String),
    }
    match BoolOrString::deserialize(deserializer)? {
        BoolOrString::Bool(b) => Ok(b),
        BoolOrString::Str(s) => match s.to_ascii_lowercase().as_str() {
            "true" | "1" => Ok(true),
            "false" | "0" | "" => Ok(false),
            other => Err(de::Error::invalid_value(
                Unexpected::Str(other),
                &"\"true\" or \"false\"",
            )),
        },
    }
}

/// Accept integers as either real JSON numbers or numeric strings.
/// Binance Futures sends leverage as a number; some clients send it as
/// a string. Reject empty strings and non-digits with a descriptive error.
fn deserialize_i32_or_string<'de, D>(deserializer: D) -> Result<i32, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::{self, Unexpected};
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum IntOrString {
        Int(i64),
        Str(String),
    }
    match IntOrString::deserialize(deserializer)? {
        IntOrString::Int(i) => i32::try_from(i)
            .map_err(|_| de::Error::invalid_value(Unexpected::Signed(i), &"i32 in range")),
        IntOrString::Str(s) => s
            .trim()
            .parse::<i32>()
            .map_err(|_| de::Error::invalid_value(Unexpected::Str(&s), &"a base-10 integer")),
    }
}

#[derive(Debug, Serialize)]
pub struct BinanceError {
    pub code: i32,
    pub msg: String,
}

type ApiResult<T> = Result<Json<T>, (StatusCode, Json<BinanceError>)>;

fn bad_request(code: i32, msg: &str) -> (StatusCode, Json<BinanceError>) {
    (
        StatusCode::BAD_REQUEST,
        Json(BinanceError {
            code,
            msg: msg.to_string(),
        }),
    )
}

fn internal_error(msg: &str) -> (StatusCode, Json<BinanceError>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(BinanceError {
            code: -1001,
            msg: msg.to_string(),
        }),
    )
}

fn not_found(msg: &str) -> (StatusCode, Json<BinanceError>) {
    (
        StatusCode::NOT_FOUND,
        Json(BinanceError {
            code: -2013,
            msg: msg.to_string(),
        }),
    )
}

fn normalize_symbol(s: &str) -> String {
    let upper = s.to_uppercase();
    if !upper.contains('-') && !upper.contains('/') && !upper.contains('_') {
        return upper;
    }
    if upper.ends_with("-USD") {
        return format!("{}USDT", upper.strip_suffix("-USD").unwrap());
    }
    if upper.contains("-USDT") {
        return upper.replace("-", "");
    }
    upper.replace("/", "").replace("_", "")
}

fn map_order_status(s: &OrderStatus) -> &'static str {
    match s {
        OrderStatus::Pending => "NEW",
        OrderStatus::Open => "NEW",
        OrderStatus::PartiallyFilled => "PARTIALLY_FILLED",
        OrderStatus::Filled => "FILLED",
        OrderStatus::Cancelled => "CANCELED",
        OrderStatus::Rejected => "REJECTED",
    }
}

/// Validate user-supplied timeInForce and convert to the lowercase form
/// the `time_in_force` enum stores. Defaults to GTC when absent. The
/// matching engine honors GTC / IOC / FOK semantics; GTD requires a
/// goodTillDate that we don't yet support.
///
/// GTX (post-only) is accepted at this layer: it is enforced by the
/// caller via `must_be_post_only` (see `would_cross_book`) and then
/// stored as `gtc` because:
///   1. the Postgres `time_in_force` enum doesn't have a `gtx` variant
///      (would need a migration), and
///   2. once the post-only check passes, the order is functionally a
///      resting GTC — the GTX semantics live entirely in the placement
///      pre-check.
fn normalize_tif(input: Option<&str>) -> Result<&'static str, (StatusCode, Json<BinanceError>)> {
    match input.map(|s| s.to_uppercase()).as_deref() {
        Some("GTC") | None => Ok("gtc"),
        Some("IOC") => Ok("ioc"),
        Some("FOK") => Ok("fok"),
        Some("GTX") => Ok("gtc"), // see doc above
        Some("GTD") => Err(bad_request(
            -1102,
            "timeInForce GTD requires goodTillDate (not yet supported).",
        )),
        Some(other) => Err(bad_request(
            -1102,
            &format!("Invalid timeInForce: {}", other),
        )),
    }
}

/// True when the caller asked for GTX/post-only. Kept as a separate
/// signal because `normalize_tif` collapses GTX → gtc for storage.
fn is_post_only_tif(input: Option<&str>) -> bool {
    matches!(
        input.map(|s| s.to_uppercase()).as_deref(),
        Some("GTX")
    )
}

fn db_tif_to_binance(s: &str) -> &'static str {
    match s {
        "ioc" => "IOC",
        "fok" => "FOK",
        "gtd" => "GTD",
        _ => "GTC",
    }
}

fn validate_client_order_id(s: &str) -> Result<(), (StatusCode, Json<BinanceError>)> {
    if s.is_empty() || s.len() > 36 {
        return Err(bad_request(-1100, "newClientOrderId length must be 1..=36."));
    }
    if !s.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.')) {
        return Err(bad_request(
            -1100,
            "newClientOrderId may only contain [A-Za-z0-9_.-].",
        ));
    }
    Ok(())
}

/// Resolve a (orderId, origClientOrderId) pair to the canonical UUID.
/// Binance contract: exactly one of the two is required; if both are sent,
/// `orderId` wins (Binance ignores `origClientOrderId` when `orderId` is set).
async fn resolve_order_uuid(
    pool: &PgPool,
    user_addr: &str,
    order_id: Option<&str>,
    orig_client_order_id: Option<&str>,
) -> Result<Uuid, (StatusCode, Json<BinanceError>)> {
    if let Some(s) = order_id {
        return s.parse().map_err(|_| bad_request(-1100, "Invalid orderId."));
    }
    let coid = orig_client_order_id.ok_or_else(|| {
        bad_request(-1102, "Either orderId or origClientOrderId must be sent.")
    })?;
    // Most recent order with this coid for this user. The unique-active
    // index ensures at most one *active* match; for terminal-status orders
    // we pick the latest so cancel/query against a recently filled coid
    // resolves to that fill rather than 4xx.
    let row: Option<(Uuid,)> = sqlx::query_as(
        r#"
        SELECT id FROM orders
        WHERE user_address = $1 AND client_order_id = $2
        ORDER BY created_at DESC
        LIMIT 1
        "#,
    )
    .bind(user_addr)
    .bind(coid)
    .fetch_optional(pool)
    .await
    .map_err(|e| internal_error(&e.to_string()))?;

    row.map(|(id,)| id)
        .ok_or_else(|| not_found("Order does not exist."))
}


// ─── Symbol-shard routing helpers ─────────────────────────────────
//
// One helper per write handler. Each one:
//   1. Asks ShardingConfig where the symbol's owner is.
//   2. If owner == us, returns Ok(None) — caller proceeds with local
//      execution.
//   3. If owner != us AND the request hasn't already been forwarded
//      once, proxies to the owner pod and returns the typed response.
//   4. If owner != us AND the request HAS been forwarded, falls
//      through to local execution with a loud warn — better degraded
//      than a forwarding loop. This handles hash drift / pod scaling
//      mid-flight without crashing.
//
// Returns ApiResult<Option<R>> so the caller pattern stays:
//     if let Some(resp) = maybe_forward_X(...).await? { return Ok(resp); }

async fn maybe_forward_new_order(
    state: &Arc<AppState>,
    headers: &axum::http::HeaderMap,
    original_uri: &axum::http::Uri,
    symbol: &str,
    req: &NewOrderRequest,
) -> Result<Option<Json<BinanceOrderResponse>>, (StatusCode, Json<BinanceError>)> {
    use crate::services::sharding::{proxy, RouteDecision, FORWARDED_HEADER};

    let already_forwarded = headers.contains_key(FORWARDED_HEADER);
    match state.sharding.route_for(symbol) {
        RouteDecision::Local => Ok(None),
        RouteDecision::Forward { target_host, owner_ordinal } => {
            if already_forwarded {
                tracing::warn!(
                    "shard-routing: hash drift — forwarded request for {} arrived at non-owner pod {} (owner thinks: {}). Handling locally as degraded fallback.",
                    symbol, state.sharding.my_ordinal, owner_ordinal
                );
                return Ok(None);
            }
            let path = original_uri.path();
            let resp: BinanceOrderResponse = proxy::forward_json(
                &target_host,
                axum::http::Method::POST,
                path,
                headers,
                &[],
                Some(req),
            )
            .await
            .map_err(|e| internal_error(&format!("shard-proxy new_order: {}", e)))?;
            Ok(Some(Json(resp)))
        }
    }
}

async fn maybe_forward_cancel_order(
    state: &Arc<AppState>,
    headers: &axum::http::HeaderMap,
    original_uri: &axum::http::Uri,
    q: &CancelOrderParams,
) -> Result<Option<Json<BinanceOrderResponse>>, (StatusCode, Json<BinanceError>)> {
    use crate::services::sharding::{proxy, RouteDecision, FORWARDED_HEADER};

    let symbol = normalize_symbol(&q.symbol);
    let already_forwarded = headers.contains_key(FORWARDED_HEADER);
    match state.sharding.route_for(&symbol) {
        RouteDecision::Local => Ok(None),
        RouteDecision::Forward { target_host, owner_ordinal } => {
            if already_forwarded {
                tracing::warn!(
                    "shard-routing: hash drift on cancel for {} (owner thinks: {}); handling locally as fallback.",
                    symbol, owner_ordinal
                );
                return Ok(None);
            }
            // DELETE /fapi/v1/order is query-string driven. Reconstruct
            // the params we need to forward; the receiving handler will
            // re-verify the HMAC over qs+body so we can't lose anything.
            let mut qs: Vec<(String, String)> = Vec::with_capacity(4);
            qs.push(("symbol".into(), q.symbol.clone()));
            if let Some(oid) = &q.order_id { qs.push(("orderId".into(), oid.clone())); }
            if let Some(coid) = &q.orig_client_order_id {
                qs.push(("origClientOrderId".into(), coid.clone()));
            }
            if let Some(ts) = q.timestamp { qs.push(("timestamp".into(), ts.to_string())); }
            // The signature was computed over the original query string;
            // re-encode here will produce the same canonical form for the
            // standard Binance fields. signature itself is in `headers`
            // as `X-MBX-APIKEY` is the API key — wait, `signature` is in
            // the query string, not headers. Pull it from the URI.
            if let Some(orig_q) = original_uri.query() {
                for pair in orig_q.split('&') {
                    if let Some(eq) = pair.find('=') {
                        let (k, v) = (&pair[..eq], &pair[eq+1..]);
                        if k == "signature" {
                            qs.push(("signature".into(), v.to_string()));
                        }
                    }
                }
            }

            let resp: BinanceOrderResponse = proxy::forward_json::<(), _>(
                &target_host,
                axum::http::Method::DELETE,
                original_uri.path(),
                headers,
                &qs,
                None,
            )
            .await
            .map_err(|e| internal_error(&format!("shard-proxy cancel_order: {}", e)))?;
            Ok(Some(Json(resp)))
        }
    }
}

async fn maybe_forward_modify_order(
    state: &Arc<AppState>,
    headers: &axum::http::HeaderMap,
    original_uri: &axum::http::Uri,
    symbol: &str,
    req: &ModifyOrderRequest,
) -> Result<Option<Json<BinanceOrderResponse>>, (StatusCode, Json<BinanceError>)> {
    use crate::services::sharding::{proxy, RouteDecision, FORWARDED_HEADER};

    let already_forwarded = headers.contains_key(FORWARDED_HEADER);
    match state.sharding.route_for(symbol) {
        RouteDecision::Local => Ok(None),
        RouteDecision::Forward { target_host, owner_ordinal } => {
            if already_forwarded {
                tracing::warn!(
                    "shard-routing: hash drift on modify for {} (owner thinks: {}); handling locally as fallback.",
                    symbol, owner_ordinal
                );
                return Ok(None);
            }
            let resp: BinanceOrderResponse = proxy::forward_json(
                &target_host,
                axum::http::Method::PUT,
                original_uri.path(),
                headers,
                &[],
                Some(req),
            )
            .await
            .map_err(|e| internal_error(&format!("shard-proxy modify_order: {}", e)))?;
            Ok(Some(Json(resp)))
        }
    }
}

async fn maybe_forward_cancel_all(
    state: &Arc<AppState>,
    headers: &axum::http::HeaderMap,
    original_uri: &axum::http::Uri,
    raw_symbol: &str,
) -> Result<Option<Json<serde_json::Value>>, (StatusCode, Json<BinanceError>)> {
    use crate::services::sharding::{proxy, RouteDecision, FORWARDED_HEADER};

    let symbol = normalize_symbol(raw_symbol);
    let already_forwarded = headers.contains_key(FORWARDED_HEADER);
    match state.sharding.route_for(&symbol) {
        RouteDecision::Local => Ok(None),
        RouteDecision::Forward { target_host, owner_ordinal } => {
            if already_forwarded {
                tracing::warn!(
                    "shard-routing: hash drift on cancel_all for {} (owner thinks: {}); handling locally as fallback.",
                    symbol, owner_ordinal
                );
                return Ok(None);
            }
            // DELETE /fapi/v1/allOpenOrders is query-string driven. Pass
            // through the original query string verbatim so the HMAC
            // signature stays valid.
            let qs = preserve_signed_query(original_uri);
            let (_status, val) = proxy::forward_json_raw::<()>(
                &target_host,
                axum::http::Method::DELETE,
                original_uri.path(),
                headers,
                &qs,
                None,
            )
            .await
            .map_err(|e| internal_error(&format!("shard-proxy cancel_all: {}", e)))?;
            Ok(Some(Json(val)))
        }
    }
}

async fn maybe_forward_batch_cancel(
    state: &Arc<AppState>,
    headers: &axum::http::HeaderMap,
    original_uri: &axum::http::Uri,
    raw_symbol: &str,
) -> Result<Option<Json<Vec<BinanceOrderResponse>>>, (StatusCode, Json<BinanceError>)> {
    use crate::services::sharding::{proxy, RouteDecision, FORWARDED_HEADER};

    let symbol = normalize_symbol(raw_symbol);
    let already_forwarded = headers.contains_key(FORWARDED_HEADER);
    match state.sharding.route_for(&symbol) {
        RouteDecision::Local => Ok(None),
        RouteDecision::Forward { target_host, owner_ordinal } => {
            if already_forwarded {
                tracing::warn!(
                    "shard-routing: hash drift on batch_cancel for {} (owner thinks: {}); handling locally as fallback.",
                    symbol, owner_ordinal
                );
                return Ok(None);
            }
            // Batch cancel is symbol-scoped: BatchCancelQuery requires
            // `symbol` and the orderIdList all belong to that symbol. Pass
            // the original signed query through unchanged.
            let qs = preserve_signed_query(original_uri);
            let resp: Vec<BinanceOrderResponse> = proxy::forward_json::<(), _>(
                &target_host,
                axum::http::Method::DELETE,
                original_uri.path(),
                headers,
                &qs,
                None,
            )
            .await
            .map_err(|e| internal_error(&format!("shard-proxy batch_cancel: {}", e)))?;
            Ok(Some(Json(resp)))
        }
    }
}

/// Re-encode the original URI's query string so the forwarded hop sees
/// the exact same `(k, v)` pairs the client sent. Critical for HMAC: the
/// receiving pod re-computes the signature over the canonical form, so
/// dropping or reordering pairs would break the signature check.
fn preserve_signed_query(original_uri: &axum::http::Uri) -> Vec<(String, String)> {
    let mut out = Vec::new();
    if let Some(q) = original_uri.query() {
        for pair in q.split('&') {
            if pair.is_empty() {
                continue;
            }
            if let Some(eq) = pair.find('=') {
                out.push((pair[..eq].to_string(), pair[eq + 1..].to_string()));
            } else {
                out.push((pair.to_string(), String::new()));
            }
        }
    }
    out
}

// ─── Binance-compatible Order Response ────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
pub struct BinanceOrderResponse {
    #[serde(rename = "orderId")]
    pub order_id: String,
    pub symbol: String,
    pub status: String,
    #[serde(rename = "clientOrderId")]
    pub client_order_id: String,
    pub price: String,
    #[serde(rename = "avgPrice")]
    pub avg_price: String,
    #[serde(rename = "origQty")]
    pub orig_qty: String,
    #[serde(rename = "executedQty")]
    pub executed_qty: String,
    #[serde(rename = "cumQuote")]
    pub cum_quote: String,
    #[serde(rename = "timeInForce")]
    pub time_in_force: String,
    #[serde(rename = "type")]
    pub order_type: String,
    #[serde(rename = "reduceOnly")]
    pub reduce_only: bool,
    pub side: String,
    #[serde(rename = "positionSide")]
    pub position_side: String,
    #[serde(rename = "stopPrice")]
    pub stop_price: String,
    #[serde(rename = "workingType")]
    pub working_type: String,
    #[serde(rename = "priceProtect")]
    pub price_protect: bool,
    #[serde(rename = "origType")]
    pub orig_type: String,
    #[serde(rename = "updateTime")]
    pub update_time: i64,
    pub time: i64,
}

// ─── 1. Test Order ────────────────────────────────────────────────
// POST /fapi/v1/order/test

#[derive(Debug, Deserialize, Serialize)]
#[allow(dead_code)]
pub struct NewOrderRequest {
    pub symbol: String,
    pub side: String,
    #[serde(rename = "positionSide")]
    pub position_side: Option<String>,
    #[serde(rename = "type")]
    pub order_type: String,
    #[serde(
        rename = "reduceOnly",
        default,
        deserialize_with = "deserialize_bool_or_string"
    )]
    pub reduce_only: bool,
    pub quantity: Option<String>,
    pub price: Option<String>,
    #[serde(rename = "stopPrice")]
    pub stop_price: Option<String>,
    #[serde(rename = "timeInForce")]
    pub time_in_force: Option<String>,
    #[serde(rename = "workingType")]
    pub working_type: Option<String>,
    /// User-supplied client-side ID, used for idempotency / dedup. Stored
    /// in `orders.client_order_id` and echoed back on subsequent reads.
    /// Allowed: [A-Za-z0-9_.-], length 1..=36. Uniqueness enforced per
    /// (user_address) for active orders only — terminal coids may be reused.
    #[serde(rename = "newClientOrderId")]
    pub new_client_order_id: Option<String>,
    pub timestamp: Option<i64>,
}

pub async fn test_order(
    State(state): State<Arc<AppState>>,
    Extension(_auth_user): Extension<AuthUser>,
    Json(req): Json<NewOrderRequest>,
) -> ApiResult<serde_json::Value> {
    let symbol = normalize_symbol(&req.symbol);
    if !state.market_config_service.is_tradeable(&symbol).await {
        return Err(bad_request(-1121, &format!("Invalid symbol: {}", symbol)));
    }
    Ok(Json(serde_json::json!({})))
}

// ─── 2. New Order ─────────────────────────────────────────────────
// POST /fapi/v1/order

pub async fn new_order(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    headers: axum::http::HeaderMap,
    axum::extract::OriginalUri(original_uri): axum::extract::OriginalUri,
    Json(req): Json<NewOrderRequest>,
) -> ApiResult<BinanceOrderResponse> {
    let symbol = normalize_symbol(&req.symbol);

    if !state.market_config_service.is_tradeable(&symbol).await {
        return Err(bad_request(-1121, &format!("Invalid symbol: {}", symbol)));
    }

    // Symbol-shard routing. Off by default; once a StatefulSet rollout
    // turns it on, requests for symbols not owned by this pod are
    // proxied to the owner. Pre-existing single-replica deployments are
    // unaffected. See `services/sharding/mod.rs`.
    if let Some(resp) = maybe_forward_new_order(
        &state, &headers, &original_uri, &symbol, &req,
    ).await? {
        return Ok(resp);
    }

    // Parse side
    let side = match req.side.to_uppercase().as_str() {
        "BUY" => OrderSide::Buy,
        "SELL" => OrderSide::Sell,
        _ => return Err(bad_request(-1130, "Invalid side.")),
    };

    // Parse order type. Binance Futures canonical names: LIMIT, MARKET,
    // STOP, STOP_MARKET, TAKE_PROFIT, TAKE_PROFIT_MARKET. Binance Spot
    // and many SDKs send STOP_LIMIT / TAKE_PROFIT_LIMIT for the same
    // intent — accept both as aliases.
    let order_type = match req.order_type.to_uppercase().as_str() {
        "LIMIT" => OrderType::Limit,
        "MARKET" => OrderType::Market,
        "STOP" | "STOP_LIMIT" => OrderType::StopLossLimit,
        "STOP_MARKET" => OrderType::StopLossMarket,
        "TAKE_PROFIT" | "TAKE_PROFIT_LIMIT" => OrderType::TakeProfitLimit,
        "TAKE_PROFIT_MARKET" => OrderType::TakeProfitMarket,
        _ => return Err(bad_request(-1130, "Invalid orderType.")),
    };

    // Parse quantity
    let mut quantity: Decimal = req
        .quantity
        .as_deref()
        .ok_or_else(|| bad_request(-1102, "Mandatory parameter 'quantity' was not sent."))?
        .parse()
        .map_err(|_| bad_request(-1100, "Invalid quantity."))?;

    if quantity <= Decimal::ZERO {
        return Err(bad_request(-1100, "Quantity must be positive."));
    }

    // Parse price (required for LIMIT)
    let price: Option<Decimal> = if let Some(ref p) = req.price {
        let parsed: Decimal =
            p.parse().map_err(|_| bad_request(-1100, "Invalid price."))?;
        // Reject zero / negative prices. Previously only price=0 was
        // rejected (commit a4c7a01 from 2026-04-20 QA); price<0 would
        // INSERT into orders with a negative price and rest in the book.
        // (P1: 2026-04-25 QA finding.)
        if parsed <= Decimal::ZERO {
            return Err(bad_request(-1100, "Price must be positive."));
        }
        Some(parsed)
    } else {
        None
    };

    // Parse stopPrice (required for STOP/TAKE_PROFIT types)
    let stop_price: Option<Decimal> = if let Some(ref sp) = req.stop_price {
        let parsed: Decimal =
            sp.parse().map_err(|_| bad_request(-1100, "Invalid stopPrice."))?;
        if parsed <= Decimal::ZERO {
            return Err(bad_request(-1100, "stopPrice must be positive."));
        }
        Some(parsed)
    } else {
        None
    };

    let is_trigger_type = matches!(
        order_type,
        OrderType::TakeProfitLimit | OrderType::StopLossLimit | OrderType::TakeProfitMarket | OrderType::StopLossMarket
    );
    if is_trigger_type && stop_price.is_none() {
        return Err(bad_request(
            -1102,
            "Mandatory parameter 'stopPrice' was not sent for trigger order.",
        ));
    }

    // Reject trigger orders whose stopPrice already satisfies the trigger
    // condition at placement (Binance -2021 "Order would immediately
    // trigger"). Without this check, R7 D5 saw a SELL STOP_MARKET with
    // stopPrice 81670 placed while the bid was 77781 sit as `NEW`, and
    // the order would fire on the next tick — almost never what the
    // caller wanted.
    //
    //   STOP (stop-loss): triggers as price moves *against* the side.
    //     SELL STOP   triggers when price ≤ stopPrice → reject if
    //                 stopPrice ≥ ref_price.
    //     BUY  STOP   triggers when price ≥ stopPrice → reject if
    //                 stopPrice ≤ ref_price.
    //   TAKE_PROFIT: triggers as price moves *in favor*.
    //     SELL TP     triggers when price ≥ stopPrice → reject if
    //                 stopPrice ≤ ref_price.
    //     BUY  TP     triggers when price ≤ stopPrice → reject if
    //                 stopPrice ≥ ref_price.
    if is_trigger_type {
        if let Some(sp) = stop_price {
            let ref_price = state
                .price_feed_service
                .get_mark_price(&symbol)
                .await
                .unwrap_or(Decimal::ZERO);
            if ref_price > Decimal::ZERO {
                let is_stop = matches!(
                    order_type,
                    OrderType::StopLossLimit | OrderType::StopLossMarket
                );
                let immediate = match (is_stop, side) {
                    (true,  OrderSide::Sell) => sp >= ref_price,
                    (true,  OrderSide::Buy)  => sp <= ref_price,
                    (false, OrderSide::Sell) => sp <= ref_price,
                    (false, OrderSide::Buy)  => sp >= ref_price,
                };
                if immediate {
                    return Err(bad_request(
                        -2021,
                        &format!(
                            "Order would immediately trigger. \
                             stopPrice={} ref_price={} side={:?} type={:?}",
                            sp, ref_price, side, order_type
                        ),
                    ));
                }
            }
        }
    }

    let tif_db = normalize_tif(req.time_in_force.as_deref())?;
    let post_only = is_post_only_tif(req.time_in_force.as_deref());

    // GTX (post-only) — reject at placement if the order would cross
    // the book. Binance returns -2010 "New order rejected: post only
    // order would have matched immediately." GTX must carry a price
    // (it's only meaningful for LIMIT orders).
    if post_only {
        if !matches!(order_type, OrderType::Limit) {
            return Err(bad_request(
                -1102,
                "timeInForce GTX requires a LIMIT order with a price.",
            ));
        }
        let limit_px = price.ok_or_else(|| {
            bad_request(-1102, "timeInForce GTX requires `price`.")
        })?;
        // Best top-of-book; depth=1 is enough.
        if let Ok(snap) = state.matching_engine.get_orderbook(&symbol, 1) {
            let best_ask: Option<Decimal> = snap
                .asks
                .first()
                .and_then(|lvl| lvl[0].parse().ok());
            let best_bid: Option<Decimal> = snap
                .bids
                .first()
                .and_then(|lvl| lvl[0].parse().ok());
            let crosses = match side {
                OrderSide::Buy => best_ask.map(|a| limit_px >= a).unwrap_or(false),
                OrderSide::Sell => best_bid.map(|b| limit_px <= b).unwrap_or(false),
            };
            if crosses {
                return Err(bad_request(
                    -2010,
                    "New order rejected: post only order would have matched immediately.",
                ));
            }
        }
        // If the orderbook isn't initialized yet (cold-start) the check
        // is skipped — there's nothing to cross, so passive admit is safe.
    }

    let collateral_symbol = state.config.collateral_symbol();
    let user_addr = auth_user.address.to_lowercase();

    // Resolve effective leverage. Binance fapi clients can't pass leverage
    // on each /fapi/v1/order — they call /fapi/v1/leverage once per symbol
    // and expect every subsequent order to use it. Look up the persisted
    // value first; clamp to the market max in case the market lowered its
    // cap after the user wrote a value; fall back to the market max when
    // the user hasn't set anything yet (preserving the prior implicit
    // behavior). See migration 20260426060000_user_market_leverage.sql.
    let market_cfg = state.market_config_service.get_config(&symbol).await;

    // Lot-size and min-notional validation. Until 2026-04-27 R4 the handler
    // accepted any positive quantity, so a sub-satoshi dust order
    // (`quantity = 0.000000001 BTC`) sailed through and an order whose
    // notional was below the symbol's `min_order_size_usd` threshold was
    // also placed. Both are rejected here with Binance-shaped error codes.
    if let Some(cfg) = market_cfg.as_ref() {
        let lot = cfg.lot_size;
        if lot > Decimal::ZERO {
            if quantity < lot {
                return Err(bad_request(
                    -1013,
                    &format!(
                        "Filter failure: LOT_SIZE. Quantity {} is below the minimum lot size {} for {}.",
                        quantity, lot, symbol
                    ),
                ));
            }
            // Floor-to-lot. PR #77 originally rejected non-aligned
            // quantities with a strict modulo check, which froze the MM
            // bots whose computed sizes carry float-style residues
            // (e.g. 0.1831325 ETHUSDT vs lot 0.001). Hotfix #85 dropped
            // the modulo check entirely. Re-introduce alignment here
            // without rejecting: silently floor `quantity` to the lot
            // grid, mutate the parsed value so downstream notional
            // checks, persistence, and the response all reflect the
            // post-floor amount. Effective rule: tradeable quantity =
            // ⌊requested / lot⌋ × lot, ≥ lot.
            let floored = (quantity / lot).floor() * lot;
            if floored < lot {
                return Err(bad_request(
                    -1013,
                    &format!(
                        "Filter failure: LOT_SIZE. Quantity {} rounds below the minimum lot size {} for {}.",
                        quantity, lot, symbol
                    ),
                ));
            }
            quantity = floored;
        }

        // Notional check uses the limit price when the user supplied one
        // (the worst-case spend a LIMIT order can incur is bounded by its
        // own price); otherwise fall back to the mark price.
        let ref_price = match price {
            Some(p) => p,
            None => state
                .price_feed_service
                .get_mark_price(&symbol)
                .await
                .unwrap_or(Decimal::ZERO),
        };
        if ref_price > Decimal::ZERO {
            let notional = quantity * ref_price;
            // reduceOnly bypasses MIN_NOTIONAL. The check exists to prevent
            // dust opens (sub-10 USD orders the engine can't economically
            // settle); reduceOnly orders only ever shrink an existing
            // position, so they can't create new dust. Without this bypass a
            // user whose position has bled down to e.g. 8 USD (collateral
            // burned by funding/fees) cannot place a closing order at all
            // — the close is the only way to recover the residual collateral
            // and is always strictly safer than leaving it open.
            // Bot feedback 2026-05-07: SEIUSDT pos at ~8 USD got stuck.
            // The MAX_NOTIONAL check still applies — reduceOnly capped to
            // pos_size below means it can't realistically hit the cap, but
            // we keep the guard for defense in depth.
            if !req.reduce_only
                && cfg.min_order_size_usd > Decimal::ZERO
                && notional < cfg.min_order_size_usd
            {
                return Err(bad_request(
                    -1013,
                    &format!(
                        "Filter failure: MIN_NOTIONAL. Order notional {} is below the minimum {} USD for {}.",
                        notional, cfg.min_order_size_usd, symbol
                    ),
                ));
            }
            if cfg.max_order_size_usd > Decimal::ZERO
                && notional > cfg.max_order_size_usd
            {
                return Err(bad_request(
                    -1013,
                    &format!(
                        "Filter failure: MAX_NOTIONAL. Order notional {} exceeds the maximum {} USD for {}.",
                        notional, cfg.max_order_size_usd, symbol
                    ),
                ));
            }
        }
    }

    let market_max_lev = market_cfg.as_ref().map(|c| c.max_leverage).unwrap_or(50);
    let user_lev: Option<i32> = sqlx::query_scalar(
        "SELECT leverage FROM user_market_leverage WHERE user_address = $1 AND symbol = $2",
    )
    .bind(&user_addr)
    .bind(&symbol)
    .fetch_optional(&state.db.pool)
    .await
    .ok()
    .flatten();
    let leverage = user_lev
        .map(|l| l.min(market_max_lev).max(1))
        .unwrap_or(market_max_lev);

    // Lookup opposite-side open position once; used both for the
    // reduceOnly cap below and for the legacy is_closing check.
    let opposite_side_str = match side {
        OrderSide::Buy => "short",
        OrderSide::Sell => "long",
    };
    let opposite_pos_size: Decimal = sqlx::query_as::<_, (Decimal,)>(
        "SELECT size_in_tokens FROM positions \
         WHERE user_address = $1 AND symbol = $2 AND side::text = $3 AND status = 'open'",
    )
    .bind(&user_addr)
    .bind(&symbol)
    .bind(opposite_side_str)
    .fetch_optional(&state.db.pool)
    .await
    .ok()
    .flatten()
    .map(|(sz,)| sz)
    .unwrap_or(Decimal::ZERO);

    // reduceOnly handling (P0 N2: 2026-04-25 QA round 2).
    //
    // Round 2 found that `req.reduce_only` was being echoed back to clients
    // but never actually constrained the order. With the matching engine
    // having no concept of reduceOnly, a `BUY 0.001 reduceOnly=true` against
    // a SHORT 0.000275 position would fully fill 0.001 (consuming 0.000275
    // worth of opposite-side liquidity to flatten, then OPENING a new LONG
    // 0.000725) — the exact opposite of reduceOnly's purpose. Same shape
    // hit the round-2 cleanup loop and was the source of N6's $0.81/cycle
    // collateral leak (the buggy second order triggered a fresh full freeze
    // because `is_closing` is qty-vs-position based and qty > pos_size).
    //
    // Fix: at handler entry, if reduceOnly=true:
    //   - reject (-2022) if no opposite-side position
    //   - cap quantity to position size if requested qty exceeds it
    // This handles both N2 (cap) and N6 (no spurious freeze on the leftover)
    // for the single-order case. Cumulative-over-fill across multiple
    // concurrent reduceOnly orders still requires engine-level support
    // (out of scope for this PR — flagged as follow-up).
    let is_reduce_only = req.reduce_only;
    if is_reduce_only {
        if opposite_pos_size <= Decimal::ZERO {
            return Err(bad_request(
                -2022,
                "ReduceOnly Order is rejected: no opposite-side position to reduce.",
            ));
        }
        if quantity > opposite_pos_size {
            tracing::info!(
                "reduceOnly cap: user={} symbol={} requested_qty={} pos_size={} → capping to pos_size",
                user_addr, symbol, quantity, opposite_pos_size
            );
            quantity = opposite_pos_size;
        }
    }

    // Per-market per-side OI cap. Mirrors the JWT-path enforcement in
    // backend/src/api/handlers/order.rs (the spec §1.4 hard reject) — the
    // fapi/api-key path was previously not running this check, which let
    // MM bots concentrate OI past the configured per-side ceilings.
    //
    // reduceOnly orders cannot grow OI (they're capped to the opposite
    // position above), so they bypass. Cap is a soft target — no
    // per-symbol mutex; brief overshoot under concurrent placement is
    // acceptable. opposite_pos_size_usd comes from the positions row's
    // size_in_usd column (USD); opposite_pos_size at line 904 is in
    // tokens and is not interchangeable.
    if !is_reduce_only {
        use crate::models::PositionSide;

        let est_price = match price {
            Some(p) if p > Decimal::ZERO => p,
            _ => state
                .price_feed_service
                .get_mark_price(&symbol)
                .await
                .unwrap_or(Decimal::ZERO),
        };

        if est_price > Decimal::ZERO {
            let order_size_usd = quantity * est_price;

            let opposite_size_usd: Decimal = sqlx::query_scalar(
                "SELECT COALESCE(size_in_usd, 0) FROM positions \
                 WHERE user_address = $1 AND symbol = $2 AND side::text = $3 AND status = 'open'",
            )
            .bind(&user_addr)
            .bind(&symbol)
            .bind(opposite_side_str)
            .fetch_optional(&state.db.pool)
            .await
            .ok()
            .flatten()
            .unwrap_or(Decimal::ZERO);

            let cap_side = match side {
                OrderSide::Buy => PositionSide::Long,
                OrderSide::Sell => PositionSide::Short,
            };
            let delta_usd = (order_size_usd - opposite_size_usd).max(Decimal::ZERO);

            if delta_usd > Decimal::ZERO {
                if let Some(cfg) = state.market_config_service.get_config(&symbol).await {
                    let cap_usd = match cap_side {
                        PositionSide::Long => cfg.max_long_oi_usd,
                        PositionSide::Short => cfg.max_short_oi_usd,
                    };
                    if let Err(e) = state
                        .funding_rate_service
                        .check_oi_cap_with_cap(&symbol, cap_side, delta_usd, cap_usd)
                        .await
                    {
                        tracing::warn!(
                            "OI cap rejection (fapi): {} {:?} delta={} current={} cap={}",
                            e.symbol, e.side, e.requested_delta_usd, e.current_oi_usd, e.cap_usd
                        );
                        return Err(bad_request(
                            -2010,
                            &format!(
                                "{} OI cap exceeded on {:?} side: current ${}, cap ${}, requested delta ${}",
                                e.symbol, e.side, e.current_oi_usd, e.cap_usd, e.requested_delta_usd
                            ),
                        ));
                    }
                }
            }
        }
    }

    // Check if this is a closing order — quantity here may have just been
    // capped down by the reduceOnly branch above, so re-evaluate against
    // the (already-fetched) opposite position size.
    let is_closing = quantity <= opposite_pos_size && opposite_pos_size > Decimal::ZERO;

    // Persisted on the orders row so cancel paths can refund the exact
    // amount we froze, instead of falling back to the
    // `remaining * price / leverage * 1.005` recompute (which over-refunds
    // any order whose price has been modified). None means "no margin was
    // frozen for this order" — closing / reduce-only orders take that path.
    let mut frozen_margin_for_insert: Option<Decimal> = None;

    if !is_closing {
        // Calculate required margin
        let actual_price = if let Some(p) = price {
            p
        } else {
            state
                .price_feed_service
                .get_mark_price(&symbol)
                .await
                .unwrap_or(Decimal::ZERO)
        };
        let notional = quantity * actual_price;
        let required_margin = notional / Decimal::from(leverage);
        let required_with_buffer = required_margin + required_margin * MARGIN_BUFFER_RATE;

        let balance: Option<(Decimal,)> = sqlx::query_as(
            "SELECT available FROM balances WHERE user_address = $1 AND token = $2",
        )
        .bind(&user_addr)
        .bind(collateral_symbol)
        .fetch_optional(&state.db.pool)
        .await
        .ok()
        .flatten();

        let available = balance.map(|(a,)| a).unwrap_or(Decimal::ZERO);
        if available < required_with_buffer {
            return Err(bad_request(
                -2019,
                &format!(
                    "Margin is insufficient. Required: {}, Available: {}",
                    required_with_buffer, available
                ),
            ));
        }

        // Freeze margin
        sqlx::query(
            "UPDATE balances SET available = available - $1, frozen = frozen + $1 WHERE user_address = $2 AND token = $3",
        )
        .bind(required_with_buffer)
        .bind(&user_addr)
        .bind(collateral_symbol)
        .execute(&state.db.pool)
        .await
        .map_err(|e| internal_error(&format!("Failed to freeze margin: {}", e)))?;

        frozen_margin_for_insert = Some(required_with_buffer);
    }

    // Create order
    let order_id = Uuid::new_v4();
    let now = Utc::now();

    let mark_price_at_creation = state
        .price_feed_service
        .get_mark_price(&symbol)
        .await
        .unwrap_or_else(|| price.unwrap_or(Decimal::ZERO));

    // Validate optional newClientOrderId before inserting so a violation
    // surfaces as a clean 4xx instead of a generic INSERT failure.
    let coid: Option<&str> = req.new_client_order_id.as_deref();
    if let Some(c) = coid {
        validate_client_order_id(c)?;
    }

    let insert_result = sqlx::query(
        r#"
        INSERT INTO orders (id, user_address, symbol, side, order_type, price, amount, filled_amount, leverage, status, signature, created_at, updated_at, mark_price_at_creation, time_in_force, client_order_id, reduce_only, trigger_price, frozen_margin)
        VALUES ($1, $2, $3, $4, $5, $6, $7, 0, $8, 'pending', 'api_key', $9, $9, $10, $11::time_in_force, $12, $13, $14, $15)
        "#,
    )
    .bind(order_id)
    .bind(&user_addr)
    .bind(&symbol)
    .bind(side)
    .bind(order_type)
    .bind(price)
    .bind(quantity)
    .bind(leverage)
    .bind(now)
    .bind(mark_price_at_creation)
    .bind(tif_db)
    .bind(coid)
    .bind(is_reduce_only)
    .bind(stop_price)
    .bind(frozen_margin_for_insert)
    .execute(&state.db.pool)
    .await;

    if let Err(e) = insert_result {
        // Detect the partial-unique-index violation on (user, client_order_id).
        // Binance returns -2014 for duplicate client order id.
        let msg = e.to_string();
        if msg.contains("idx_orders_user_client_oid_active") || msg.contains("client_order_id") {
            return Err(bad_request(
                -2014,
                "Duplicate newClientOrderId — another active order already uses this id.",
            ));
        }
        return Err(internal_error(&format!("Failed to create order: {}", e)));
    }

    // Trigger types (STOP_*, TAKE_PROFIT_*) must NOT be submitted to the
    // matching engine here — the engine has no concept of `stopPrice` and
    // would treat them as IOC market orders, filling immediately at the
    // current book and silently dropping the trigger condition. Mirror the
    // JWT /api/v1/orders divert (api/handlers/order.rs:548) and route the
    // request to `trigger_orders_service`. The keeper picks up the entry
    // when the price condition is met and submits a fresh derived order
    // to matching at that point.
    if is_trigger_type {
        use crate::services::trigger_orders::{
            CreateTriggerOrderRequest,
            TriggerOrderType as ModelTriggerType,
            OrderSide as TriggerSide,
        };

        let trigger_type_model = match order_type {
            OrderType::TakeProfitLimit => ModelTriggerType::TakeProfitLimit,
            OrderType::StopLossLimit => ModelTriggerType::StopLimit,
            OrderType::TakeProfitMarket => ModelTriggerType::TakeProfit,
            OrderType::StopLossMarket => ModelTriggerType::StopLoss,
            _ => unreachable!("is_trigger_type guards this branch"),
        };
        let trigger_side_model = match side {
            OrderSide::Buy => TriggerSide::Buy,
            OrderSide::Sell => TriggerSide::Sell,
        };
        let stop_p = stop_price.expect("validated above when is_trigger_type");

        let trigger_req = CreateTriggerOrderRequest {
            position_id: None,
            market_symbol: symbol.clone(),
            trigger_type: trigger_type_model,
            side: trigger_side_model,
            // size is in USD (engine convention); use trigger_price as the
            // notional anchor — same convention as the JWT path.
            size: quantity * stop_p,
            trigger_price: stop_p,
            limit_price: price,
            trailing_delta: None,
            trailing_delta_type: None,
            reduce_only: Some(is_reduce_only),
            close_position: Some(false),
            expires_at: None,
            client_order_id: None,
        };

        match state
            .trigger_orders_service
            .create_trigger_order(&user_addr, trigger_req)
            .await
        {
            Ok(trigger_order) => {
                let _ = sqlx::query(
                    "UPDATE orders SET trigger_order_id = $1 WHERE id = $2",
                )
                .bind(trigger_order.id)
                .bind(order_id)
                .execute(&state.db.pool)
                .await;

                return Ok(Json(BinanceOrderResponse {
                    order_id: order_id.to_string(),
                    symbol: symbol.clone(),
                    status: "NEW".to_string(),
                    client_order_id: coid
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| order_id.to_string()),
                    price: price.unwrap_or(Decimal::ZERO).to_string(),
                    avg_price: "0".to_string(),
                    orig_qty: quantity.to_string(),
                    executed_qty: "0".to_string(),
                    cum_quote: "0".to_string(),
                    time_in_force: db_tif_to_binance(tif_db).to_string(),
                    order_type: req.order_type.to_uppercase(),
                    reduce_only: is_reduce_only,
                    side: req.side.to_uppercase(),
                    position_side: req.position_side.unwrap_or_else(|| "BOTH".into()),
                    stop_price: stop_p.to_string(),
                    working_type: req.working_type.unwrap_or_else(|| "CONTRACT_PRICE".into()),
                    price_protect: false,
                    orig_type: req.order_type.to_uppercase(),
                    update_time: now.timestamp_millis(),
                    time: now.timestamp_millis(),
                }));
            }
            Err(e) => {
                tracing::error!(
                    "Failed to create trigger order (parent order_id={}): {}",
                    order_id, e
                );
                // Same caveat as the JWT path (api/handlers/order.rs:600):
                // we do not roll back the parent orders row or unfreeze the
                // collateral here. Tracked separately as the trigger-cancel
                // refund gap; not addressing in this PR.
                return Err(internal_error(&format!(
                    "Failed to create trigger order: {}",
                    e
                )));
            }
        }
    }

    // Submit to matching engine
    let matching_side = match side {
        OrderSide::Buy => MatchingSide::Buy,
        OrderSide::Sell => MatchingSide::Sell,
    };
    let matching_type = match order_type {
        OrderType::Limit => MatchingOrderType::Limit,
        OrderType::Market => MatchingOrderType::Market,
        OrderType::StopLossLimit => MatchingOrderType::StopLossLimit,
        OrderType::StopLossMarket => MatchingOrderType::StopLossMarket,
        OrderType::TakeProfitLimit => MatchingOrderType::TakeProfitLimit,
        OrderType::TakeProfitMarket => MatchingOrderType::TakeProfitMarket,
    };

    // Resolve VIP-tier rates so HMAC API trades pay the same fee as JWT
    // ones. `resolve` is the interactive entry; mirrors create_order.
    let effective_tier = crate::services::vip_tier::resolve(
        &state.db.pool,
        &state.vip_tier_event_sender,
        &user_addr,
    ).await;
    let multiplier = crate::utils::fee_tiers::discount_multiplier(&user_addr, true);
    let taker_fee_rate =
        crate::utils::fee_tiers::round_fee(effective_tier.current.taker * multiplier);
    let maker_fee_rate =
        crate::utils::fee_tiers::round_fee(effective_tier.current.maker * multiplier);

    let matching_tif = match tif_db {
        "ioc" => MatchingTimeInForce::IOC,
        "fok" => MatchingTimeInForce::FOK,
        _ => MatchingTimeInForce::GTC,
    };

    let match_result = state
        .matching_engine
        .submit_order(
            order_id,
            &symbol,
            &user_addr,
            matching_side,
            matching_type,
            quantity,
            price,
            matching_tif,
            leverage as u32,
            taker_fee_rate,
            maker_fee_rate,
        )
        .map_err(|e| internal_error(&format!("Matching engine error: {}", e)))?;

    let order_status = match match_result.status {
        MatchingOrderStatus::Open => OrderStatus::Open,
        MatchingOrderStatus::PartiallyFilled => OrderStatus::PartiallyFilled,
        MatchingOrderStatus::Filled => OrderStatus::Filled,
        MatchingOrderStatus::Cancelled => OrderStatus::Cancelled,
        MatchingOrderStatus::Rejected => OrderStatus::Rejected,
    };

    let status_str = match order_status {
        OrderStatus::Open => "open",
        OrderStatus::PartiallyFilled => "partially_filled",
        OrderStatus::Filled => "filled",
        OrderStatus::Cancelled => "cancelled",
        OrderStatus::Rejected => "rejected",
        OrderStatus::Pending => "pending",
    };

    // Persist fill state: write avg fill price into its own column and
    // leave `orders.price` (the original limit price) untouched. Previously
    // the `price` column was clobbered with the avg, losing the original
    // limit price for analytics/risk (B5-7).
    sqlx::query(
        "UPDATE orders SET status = $1::order_status, filled_amount = $2, avg_price = $3 WHERE id = $4",
    )
    .bind(status_str)
    .bind(match_result.filled_amount)
    .bind(match_result.average_price)
    .bind(order_id)
    .execute(&state.db.pool)
    .await
    .ok();

    // Release the taker's frozen collateral that didn't end up backing a
    // position (the unused buffer + any unfilled remainder). Mirrors the
    // post-match settlement in `order.rs` (legacy /orders handler) which
    // was added to plug the same leak — without it, every fapi fill
    // accumulates phantom frozen on the account (P0: 2026-04-25 QA).
    //
    // Skipped when `is_closing` is true because we never froze in that
    // branch (see line ~393).
    if !is_closing
        && (match_result.filled_amount > Decimal::ZERO
            || order_status == OrderStatus::Cancelled
            || order_status == OrderStatus::Rejected)
    {
        // What we froze upfront: required_with_buffer (= notional/leverage * 1.005)
        let initial_freeze = {
            // Recompute to avoid threading a new local through the long
            // function; cheap (handful of decimal ops).
            let p_used = if let Some(p) = price {
                p
            } else {
                state
                    .price_feed_service
                    .get_mark_price(&symbol)
                    .await
                    .unwrap_or(Decimal::ZERO)
            };
            let notional = quantity * p_used;
            let req = notional / Decimal::from(leverage);
            req + req * MARGIN_BUFFER_RATE
        };

        // What actually moved into positions.collateral_amount on fill
        let avg = match_result
            .average_price
            .unwrap_or(price.unwrap_or(Decimal::ZERO));
        let collateral_to_position = if avg > Decimal::ZERO && leverage > 0 {
            match_result.filled_amount * avg / Decimal::from(leverage)
        } else {
            Decimal::ZERO
        };

        let (frozen_delta, available_delta) = match order_status {
            OrderStatus::Filled | OrderStatus::Cancelled | OrderStatus::Rejected => {
                let avail =
                    (initial_freeze - collateral_to_position).max(Decimal::ZERO);
                (initial_freeze, avail)
            }
            OrderStatus::PartiallyFilled if match_result.filled_amount > Decimal::ZERO => {
                // Pro-rata release for the filled slice
                let per_unit = if quantity > Decimal::ZERO {
                    initial_freeze / quantity
                } else {
                    Decimal::ZERO
                };
                let frozen = match_result.filled_amount * per_unit;
                let avail = (frozen - collateral_to_position).max(Decimal::ZERO);
                (frozen, avail)
            }
            _ => (Decimal::ZERO, Decimal::ZERO),
        };

        if frozen_delta > Decimal::ZERO {
            let _ = sqlx::query(
                "UPDATE balances SET frozen = GREATEST(frozen - $1, 0), available = available + $2, updated_at = NOW() WHERE user_address = $3 AND token = $4",
            )
            .bind(frozen_delta)
            .bind(available_delta)
            .bind(&user_addr)
            .bind(collateral_symbol)
            .execute(&state.db.pool)
            .await;

            let _ = sqlx::query(
                "UPDATE orders SET frozen_margin = GREATEST(frozen_margin - $1, 0) WHERE id = $2",
            )
            .bind(frozen_delta)
            .bind(order_id)
            .execute(&state.db.pool)
            .await;
        }
    }

    let avg_price = match_result
        .average_price
        .unwrap_or(price.unwrap_or(Decimal::ZERO));

    Ok(Json(BinanceOrderResponse {
        order_id: order_id.to_string(),
        symbol: symbol.clone(),
        status: map_order_status(&order_status).to_string(),
        client_order_id: coid.map(|s| s.to_string()).unwrap_or_else(|| order_id.to_string()),
        price: price.unwrap_or(Decimal::ZERO).to_string(),
        avg_price: avg_price.to_string(),
        orig_qty: quantity.to_string(),
        executed_qty: match_result.filled_amount.to_string(),
        cum_quote: (match_result.filled_amount * avg_price).to_string(),
        time_in_force: db_tif_to_binance(tif_db).to_string(),
        order_type: req.order_type.to_uppercase(),
        reduce_only: req.reduce_only,
        side: req.side.to_uppercase(),
        position_side: req.position_side.unwrap_or("BOTH".into()),
        stop_price: "0".to_string(),
        working_type: req.working_type.unwrap_or("CONTRACT_PRICE".into()),
        price_protect: false,
        orig_type: req.order_type.to_uppercase(),
        update_time: now.timestamp_millis(),
        time: now.timestamp_millis(),
    }))
}

// ─── 3. Query Order ───────────────────────────────────────────────
// GET /fapi/v1/order

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct QueryOrderParams {
    pub symbol: String,
    #[serde(rename = "orderId")]
    pub order_id: Option<String>,
    #[serde(rename = "origClientOrderId")]
    pub orig_client_order_id: Option<String>,
    pub timestamp: Option<i64>,
}

pub async fn query_order(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Query(q): Query<QueryOrderParams>,
) -> ApiResult<BinanceOrderResponse> {
    let user_addr = auth_user.address.to_lowercase();
    let oid = resolve_order_uuid(
        &state.db.pool,
        &user_addr,
        q.order_id.as_deref(),
        q.orig_client_order_id.as_deref(),
    )
    .await?;

    let row: Option<(
        Uuid, String, String, String,
        Option<Decimal>, Option<Decimal>, Decimal, Decimal, i32,
        String, String, DateTime<Utc>, DateTime<Utc>, Option<String>,
        bool, Option<Decimal>,
    )> = sqlx::query_as(
        r#"
        SELECT id, symbol, side::text, order_type::text,
               price, avg_price, amount, filled_amount, leverage,
               status::text, COALESCE(time_in_force::text, 'gtc'),
               created_at, updated_at, client_order_id,
               COALESCE(reduce_only, false), trigger_price
        FROM orders WHERE id = $1 AND user_address = $2
        "#,
    )
    .bind(oid)
    .bind(&user_addr)
    .fetch_optional(&state.db.pool)
    .await
    .map_err(|e| internal_error(&e.to_string()))?;

    let (id, symbol, side, otype, price, avg_price, amount, filled, _leverage,
         status, tif, created, updated, coid, reduce_only, trigger_price) =
        row.ok_or_else(|| not_found("Order does not exist."))?;

    let p = price.unwrap_or(Decimal::ZERO);
    let avg = avg_price.unwrap_or(Decimal::ZERO);
    let binance_status = db_status_to_binance(&status);
    let binance_type = db_type_to_binance(&otype);
    let stop_price_str = trigger_price
        .map(|d| d.to_string())
        .unwrap_or_else(|| "0".to_string());

    Ok(Json(BinanceOrderResponse {
        order_id: id.to_string(),
        symbol,
        status: binance_status.to_string(),
        client_order_id: coid.unwrap_or_else(|| id.to_string()),
        price: p.to_string(),
        avg_price: avg.to_string(),
        orig_qty: amount.to_string(),
        executed_qty: filled.to_string(),
        cum_quote: (filled * avg).to_string(),
        time_in_force: db_tif_to_binance(&tif).to_string(),
        order_type: binance_type.clone(),
        reduce_only,
        side: side.to_uppercase(),
        position_side: "BOTH".to_string(),
        stop_price: stop_price_str,
        working_type: "CONTRACT_PRICE".to_string(),
        price_protect: false,
        orig_type: binance_type,
        update_time: updated.timestamp_millis(),
        time: created.timestamp_millis(),
    }))
}

fn db_status_to_binance(s: &str) -> &str {
    match s {
        "pending" | "open" => "NEW",
        "partially_filled" => "PARTIALLY_FILLED",
        "filled" => "FILLED",
        "cancelled" => "CANCELED",
        "rejected" => "REJECTED",
        _ => "NEW",
    }
}

/// Map an `order_type` enum value (DB / internal) to the Binance fapi
/// canonical wire name. The DB stores the verbose internal name
/// (`stop_loss_market`, `take_profit_limit`, …) but the fapi response
/// must echo the same canonical strings Binance uses (`STOP_MARKET`,
/// `TAKE_PROFIT`, …) so external SDKs can round-trip type names safely.
fn db_type_to_binance(t: &str) -> String {
    match t {
        "limit" => "LIMIT".to_string(),
        "market" => "MARKET".to_string(),
        "stop_loss_limit" => "STOP".to_string(),
        "stop_loss_market" => "STOP_MARKET".to_string(),
        "take_profit_limit" => "TAKE_PROFIT".to_string(),
        "take_profit_market" => "TAKE_PROFIT_MARKET".to_string(),
        other => other.to_uppercase(),
    }
}

// ─── 4. Cancel Order ──────────────────────────────────────────────
// DELETE /fapi/v1/order

#[derive(Debug, Deserialize, Serialize)]
#[allow(dead_code)]
pub struct CancelOrderParams {
    pub symbol: String,
    #[serde(rename = "orderId")]
    pub order_id: Option<String>,
    #[serde(rename = "origClientOrderId")]
    pub orig_client_order_id: Option<String>,
    pub timestamp: Option<i64>,
}

pub async fn cancel_order(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    headers: axum::http::HeaderMap,
    axum::extract::OriginalUri(original_uri): axum::extract::OriginalUri,
    Query(q): Query<CancelOrderParams>,
) -> ApiResult<BinanceOrderResponse> {
    let user_addr = auth_user.address.to_lowercase();

    // Symbol-shard routing for cancel: the request carries `symbol` as
    // a query param. We forward BEFORE resolving the order id so a
    // stale id on this pod (e.g. order placed via owner pod, cancel
    // landed on us) doesn't surface as -2011.
    if let Some(resp) = maybe_forward_cancel_order(
        &state, &headers, &original_uri, &q,
    ).await? {
        return Ok(resp);
    }

    let oid = resolve_order_uuid(
        &state.db.pool,
        &user_addr,
        q.order_id.as_deref(),
        q.orig_client_order_id.as_deref(),
    )
    .await?;

    // Fetch the order
    let row: Option<(
        Uuid, String, String, String,
        Option<Decimal>, Option<Decimal>, Decimal, Decimal, i32,
        String, DateTime<Utc>, DateTime<Utc>, Option<String>, Option<Uuid>,
    )> = sqlx::query_as(
        r#"
        SELECT id, symbol, side::text, order_type::text,
               price, avg_price, amount, filled_amount, leverage,
               status::text, created_at, updated_at, client_order_id, trigger_order_id
        FROM orders WHERE id = $1 AND user_address = $2
        "#,
    )
    .bind(oid)
    .bind(&user_addr)
    .fetch_optional(&state.db.pool)
    .await
    .map_err(|e| internal_error(&e.to_string()))?;

    let (id, symbol, side, otype, price, avg_price, amount, filled, leverage, status, created, _updated, coid, trigger_oid) =
        row.ok_or_else(|| not_found("Order does not exist."))?;

    if status != "open" && status != "partially_filled" && status != "pending" {
        return Err(bad_request(
            -2011,
            &format!("Unknown order. Status: {}", status),
        ));
    }

    // Trigger-order branch (P0: 2026-04-26 prod test).
    //
    // STOP_* / TAKE_PROFIT_* orders are diverted to `trigger_orders` at
    // placement (PR #50, line 537 above) and never enter the matching
    // engine. The previous cancel implementation jumped straight to
    // `engine.cancel_order` for them, which returned Ok(false) and then
    // mapped to -2011 — leaving the trigger active and the up-front
    // freeze stuck. Detect via `orders.trigger_order_id` and route to
    // the trigger service. The freeze release uses the same recompute
    // formula as `batch_cancel` (developer_trade.rs:771) since
    // fapi-created orders carry NULL `frozen_margin` and the original
    // freeze was `quantity * place_price / leverage * 1.005`.
    if let Some(trig_id) = trigger_oid {
        match state
            .trigger_orders_service
            .cancel_trigger_order(&user_addr, trig_id)
            .await
        {
            Ok(_) => {}
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("already cancelled")
                    || msg.contains("already fired")
                    || msg.contains("already executed")
                    || msg.contains("already expired")
                    || msg.contains("just fired")
                {
                    return Err(bad_request(-2011, &msg));
                }
                return Err(internal_error(&format!("trigger cancel error: {}", e)));
            }
        }

        sqlx::query("UPDATE orders SET status = 'cancelled', updated_at = NOW() WHERE id = $1")
            .bind(oid)
            .execute(&state.db.pool)
            .await
            .ok();

        // Release the up-front freeze. Trigger orders use the mark price
        // at place time as the notional anchor (no limit price for
        // STOP_MARKET). We don't have the mark price snapshot, so use
        // the orders row's mark_price_at_creation, which the INSERT
        // populates regardless of order type.
        let mark_at_creation: Option<Decimal> = sqlx::query_scalar(
            "SELECT mark_price_at_creation FROM orders WHERE id = $1",
        )
        .bind(oid)
        .fetch_optional(&state.db.pool)
        .await
        .ok()
        .flatten();
        let p_used = price.or(mark_at_creation).unwrap_or(Decimal::ZERO);
        let remaining = amount - filled;
        if remaining > Decimal::ZERO && p_used > Decimal::ZERO {
            let release =
                (remaining * p_used) / Decimal::from(leverage) * Decimal::new(1005, 3);
            let collateral = state.config.collateral_symbol();
            sqlx::query(
                "UPDATE balances SET available = available + $1, frozen = GREATEST(frozen - $1, 0) WHERE user_address = $2 AND token = $3",
            )
            .bind(release)
            .bind(&user_addr)
            .bind(collateral)
            .execute(&state.db.pool)
            .await
            .ok();
        }

        let avg = avg_price.unwrap_or(Decimal::ZERO);
        return Ok(Json(BinanceOrderResponse {
            order_id: id.to_string(),
            symbol,
            status: "CANCELED".to_string(),
            client_order_id: coid.unwrap_or_else(|| id.to_string()),
            price: price.unwrap_or(Decimal::ZERO).to_string(),
            avg_price: avg.to_string(),
            orig_qty: amount.to_string(),
            executed_qty: filled.to_string(),
            cum_quote: (filled * avg).to_string(),
            time_in_force: "GTC".to_string(),
            order_type: otype.to_uppercase(),
            reduce_only: false,
            side: side.to_uppercase(),
            position_side: "BOTH".to_string(),
            stop_price: "0".to_string(),
            working_type: "CONTRACT_PRICE".to_string(),
            price_protect: false,
            orig_type: otype.to_uppercase(),
            update_time: Utc::now().timestamp_millis(),
            time: created.timestamp_millis(),
        }));
    }

    // Cancel in matching engine. The engine returns Ok(true) when the
    // order was actually removed from the in-memory book and Ok(false)
    // when the order id wasn't there. The previous implementation used
    // `let _ = ...` and unconditionally responded `CANCELED` + UPDATEd
    // the DB, which lied to the client when the engine couldn't find
    // the order — the order would later be matched by an opposite-side
    // marketable order and "fill" despite the API saying CANCELED.
    // (P0: 2026-04-25 QA found 4 such phantom fills.)
    match state.matching_engine.cancel_order(&symbol, oid, &user_addr) {
        Ok(true) => {
            sqlx::query("UPDATE orders SET status = 'cancelled', updated_at = NOW() WHERE id = $1")
                .bind(oid)
                .execute(&state.db.pool)
                .await
                .ok();
        }
        Ok(false) => {
            tracing::warn!(
                "cancel_order: engine reported order {} not in book (status in DB: {}). \
                Returning -2011 instead of fake CANCELED to surface the divergence.",
                oid, status
            );
            return Err(bad_request(
                -2011,
                "Order not in matching engine — refusing to fake CANCELED. \
                 Possible orderbook/DB divergence; check server logs.",
            ));
        }
        Err(e) => {
            tracing::error!("cancel_order engine error for {}: {}", oid, e);
            return Err(internal_error(&format!("matching engine cancel error: {}", e)));
        }
    }

    // Unfreeze remaining margin
    let p = price.unwrap_or(Decimal::ZERO);
    let remaining = amount - filled;
    if remaining > Decimal::ZERO && p > Decimal::ZERO {
        let margin_to_release =
            (remaining * p) / Decimal::from(leverage) * Decimal::new(1005, 3); // with buffer
        let collateral = state.config.collateral_symbol();
        sqlx::query(
            "UPDATE balances SET available = available + $1, frozen = GREATEST(frozen - $1, 0) WHERE user_address = $2 AND token = $3",
        )
        .bind(margin_to_release)
        .bind(&user_addr)
        .bind(collateral)
        .execute(&state.db.pool)
        .await
        .ok();
    }

    let avg = avg_price.unwrap_or(Decimal::ZERO);
    Ok(Json(BinanceOrderResponse {
        order_id: id.to_string(),
        symbol,
        status: "CANCELED".to_string(),
        client_order_id: coid.unwrap_or_else(|| id.to_string()),
        price: p.to_string(),
        avg_price: avg.to_string(),
        orig_qty: amount.to_string(),
        executed_qty: filled.to_string(),
        cum_quote: (filled * avg).to_string(),
        time_in_force: "GTC".to_string(),
        order_type: otype.to_uppercase(),
        reduce_only: false,
        side: side.to_uppercase(),
        position_side: "BOTH".to_string(),
        stop_price: "0".to_string(),
        working_type: "CONTRACT_PRICE".to_string(),
        price_protect: false,
        orig_type: otype.to_uppercase(),
        update_time: Utc::now().timestamp_millis(),
        time: created.timestamp_millis(),
    }))
}

// ─── 5. Cancel All Open Orders ────────────────────────────────────
// DELETE /fapi/v1/allOpenOrders

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct CancelAllQuery {
    pub symbol: String,
    pub timestamp: Option<i64>,
}

pub async fn cancel_all_open_orders(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    headers: axum::http::HeaderMap,
    axum::extract::OriginalUri(original_uri): axum::extract::OriginalUri,
    Query(q): Query<CancelAllQuery>,
) -> ApiResult<serde_json::Value> {
    if let Some(resp) =
        maybe_forward_cancel_all(&state, &headers, &original_uri, &q.symbol).await?
    {
        return Ok(resp);
    }
    let symbol = normalize_symbol(&q.symbol);
    let user_addr = auth_user.address.to_lowercase();

    // Get all open orders, including the trigger_order_id so we can route
    // STOP_* / TAKE_PROFIT_* entries to the trigger service instead of
    // calling engine.cancel_order on an order id the engine never had,
    // and the per-row freeze inputs so we can release the up-front
    // margin. The previous implementation flipped status='cancelled'
    // unconditionally without touching balances, leaking the entire
    // frozen margin (P0: 2026-04-26 prod test — cancel-all of three
    // 0.001 BTCUSDT @70-72k LIMIT BUYs left 4.28 USDT stuck in
    // balances.frozen).
    let rows: Vec<(
        Uuid,
        Option<Uuid>,
        Decimal,
        Decimal,
        i32,
        Option<Decimal>,
        Option<Decimal>,
    )> = sqlx::query_as(
        "SELECT id, trigger_order_id, amount, filled_amount, leverage, price, mark_price_at_creation \
         FROM orders WHERE user_address = $1 AND symbol = $2 AND status IN ('open', 'partially_filled', 'pending')",
    )
    .bind(&user_addr)
    .bind(&symbol)
    .fetch_all(&state.db.pool)
    .await
    .map_err(|e| internal_error(&e.to_string()))?;

    let buffer_mul = Decimal::new(1005, 3); // 1.005
    let mut total_release = Decimal::ZERO;

    for (oid, trig_id, amount, filled, lev, price, mark) in &rows {
        if let Some(tid) = trig_id {
            let _ = state
                .trigger_orders_service
                .cancel_trigger_order(&user_addr, *tid)
                .await;
        } else {
            let _ = state.matching_engine.cancel_order(&symbol, *oid, &user_addr);
        }

        // Recompute the still-frozen portion. Same shape as single
        // cancel_order (line ~1010) and the trigger branch (line ~990):
        // remaining * price / leverage * 1.005. For triggers and market
        // orders the row's `price` is NULL, fall back to
        // mark_price_at_creation which the INSERT populated.
        let p_used = price.or(*mark).unwrap_or(Decimal::ZERO);
        let remaining = amount - filled;
        if remaining > Decimal::ZERO && p_used > Decimal::ZERO && *lev > 0 {
            let release = remaining * p_used / Decimal::from(*lev) * buffer_mul;
            total_release += release;
        }
    }

    sqlx::query(
        "UPDATE orders SET status = 'cancelled', updated_at = NOW() WHERE user_address = $1 AND symbol = $2 AND status IN ('open', 'partially_filled', 'pending')",
    )
    .bind(&user_addr)
    .bind(&symbol)
    .execute(&state.db.pool)
    .await
    .ok();

    if total_release > Decimal::ZERO {
        let collateral = state.config.collateral_symbol();
        let _ = sqlx::query(
            "UPDATE balances SET available = available + $1, frozen = GREATEST(frozen - $1, 0), updated_at = NOW() \
             WHERE user_address = $2 AND token = $3",
        )
        .bind(total_release)
        .bind(&user_addr)
        .bind(collateral)
        .execute(&state.db.pool)
        .await;
    }

    Ok(Json(serde_json::json!({
        "code": 200,
        "msg": "The operation of cancel all open order is done."
    })))
}

// ─── 6. Current Open Orders ──────────────────────────────────────
// GET /fapi/v1/openOrders

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct OpenOrdersQuery {
    pub symbol: Option<String>,
    pub timestamp: Option<i64>,
}

pub async fn open_orders(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Query(q): Query<OpenOrdersQuery>,
) -> ApiResult<Vec<BinanceOrderResponse>> {
    let user_addr = auth_user.address.to_lowercase();

    let rows: Vec<(
        Uuid, String, String, String,
        Option<Decimal>, Option<Decimal>, Decimal, Decimal, i32,
        String, String, DateTime<Utc>, DateTime<Utc>, Option<String>,
        bool, Option<Decimal>,
    )> = if let Some(ref sym) = q.symbol {
        let symbol = normalize_symbol(sym);
        sqlx::query_as(
            r#"
            SELECT id, symbol, side::text, order_type::text,
                   price, avg_price, amount, filled_amount, leverage,
                   status::text, COALESCE(time_in_force::text, 'gtc'),
                   created_at, updated_at, client_order_id,
                   COALESCE(reduce_only, false), trigger_price
            FROM orders WHERE user_address = $1 AND symbol = $2 AND status IN ('open', 'partially_filled', 'pending')
            ORDER BY created_at DESC
            "#,
        )
        .bind(&user_addr)
        .bind(&symbol)
        .fetch_all(&state.db.pool)
        .await
    } else {
        sqlx::query_as(
            r#"
            SELECT id, symbol, side::text, order_type::text,
                   price, avg_price, amount, filled_amount, leverage,
                   status::text, COALESCE(time_in_force::text, 'gtc'),
                   created_at, updated_at, client_order_id,
                   COALESCE(reduce_only, false), trigger_price
            FROM orders WHERE user_address = $1 AND status IN ('open', 'partially_filled', 'pending')
            ORDER BY created_at DESC
            "#,
        )
        .bind(&user_addr)
        .fetch_all(&state.db.pool)
        .await
    }
    .map_err(|e| internal_error(&e.to_string()))?;

    let orders = rows_to_binance_orders(rows);
    Ok(Json(orders))
}

// ─── 7. All Orders (History) ─────────────────────────────────────
// GET /fapi/v1/allOrders

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct AllOrdersQuery {
    pub symbol: String,
    #[serde(rename = "orderId")]
    pub order_id: Option<String>,
    #[serde(rename = "startTime")]
    pub start_time: Option<i64>,
    #[serde(rename = "endTime")]
    pub end_time: Option<i64>,
    pub limit: Option<i64>,
    pub timestamp: Option<i64>,
}

pub async fn all_orders(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Query(q): Query<AllOrdersQuery>,
) -> ApiResult<Vec<BinanceOrderResponse>> {
    let symbol = normalize_symbol(&q.symbol);
    let limit = q.limit.unwrap_or(500).min(1000);
    let user_addr = auth_user.address.to_lowercase();

    let rows: Vec<(
        Uuid, String, String, String,
        Option<Decimal>, Option<Decimal>, Decimal, Decimal, i32,
        String, String, DateTime<Utc>, DateTime<Utc>, Option<String>,
        bool, Option<Decimal>,
    )> = sqlx::query_as(
        r#"
        SELECT id, symbol, side::text, order_type::text,
               price, avg_price, amount, filled_amount, leverage,
               status::text, COALESCE(time_in_force::text, 'gtc'),
               created_at, updated_at, client_order_id,
               COALESCE(reduce_only, false), trigger_price
        FROM orders WHERE user_address = $1 AND symbol = $2
        ORDER BY created_at DESC
        LIMIT $3
        "#,
    )
    .bind(&user_addr)
    .bind(&symbol)
    .bind(limit)
    .fetch_all(&state.db.pool)
    .await
    .map_err(|e| internal_error(&e.to_string()))?;

    Ok(Json(rows_to_binance_orders(rows)))
}

// ─── 8. User Trades ──────────────────────────────────────────────
// GET /fapi/v1/userTrades

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct UserTradesQuery {
    pub symbol: String,
    #[serde(rename = "orderId")]
    pub order_id: Option<String>,
    #[serde(rename = "startTime")]
    pub start_time: Option<i64>,
    #[serde(rename = "endTime")]
    pub end_time: Option<i64>,
    #[serde(rename = "fromId")]
    pub from_id: Option<i64>,
    pub limit: Option<i64>,
    pub timestamp: Option<i64>,
}

#[derive(Serialize)]
pub struct UserTradeItem {
    pub symbol: String,
    pub id: String,
    #[serde(rename = "orderId")]
    pub order_id: String,
    pub side: String,
    pub price: String,
    pub qty: String,
    #[serde(rename = "quoteQty")]
    pub quote_qty: String,
    #[serde(rename = "realizedPnl")]
    pub realized_pnl: String,
    pub commission: String,
    #[serde(rename = "commissionAsset")]
    pub commission_asset: String,
    pub time: i64,
    pub buyer: bool,
    pub maker: bool,
}

pub async fn user_trades(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Query(q): Query<UserTradesQuery>,
) -> ApiResult<Vec<UserTradeItem>> {
    let symbol = normalize_symbol(&q.symbol);
    let limit = q.limit.unwrap_or(500).min(1000);
    let user_addr = auth_user.address.to_lowercase();

    // Optional orderId filter (caller restricts to fills of one order).
    let order_id: Option<Uuid> = match q.order_id.as_deref() {
        Some(s) => Some(s.parse().map_err(|_| bad_request(-1100, "Invalid orderId."))?),
        None => None,
    };

    let rows: Vec<(Uuid, Uuid, String, Decimal, Decimal, Decimal, Decimal, DateTime<Utc>, bool)> = sqlx::query_as(
        r#"
        SELECT
            t.id,
            CASE WHEN t.maker_address = $1 THEN t.maker_order_id ELSE t.taker_order_id END,
            CASE WHEN t.taker_address = $1 THEN t.side::text ELSE
                CASE WHEN t.side::text = 'buy' THEN 'sell' ELSE 'buy' END
            END,
            t.price,
            t.amount,
            CASE WHEN t.maker_address = $1 THEN t.maker_fee ELSE t.taker_fee END,
            -- Prefer exact trade_id match; fall back to time-proximity for
            -- legacy events without trade_id. Keeps "0" for no match so the
            -- Binance-compatible shape is preserved for bots.
            -- IMPORTANT: must filter by user_address on BOTH branches —
            -- realized_pnl_events stores one row per side per trade, so
            -- the trade_id-only branch could otherwise return the
            -- counterparty's row (P0 leak: 2026-04-25 QA finding).
            COALESCE((
                SELECT realized_pnl FROM realized_pnl_events
                WHERE user_address = $1 AND (
                       trade_id = t.id
                    OR (trade_id IS NULL
                        AND symbol = t.symbol
                        AND created_at BETWEEN t.created_at - interval '5 seconds' AND t.created_at + interval '5 seconds')
                )
                ORDER BY (trade_id = t.id) DESC NULLS LAST,
                         ABS(EXTRACT(EPOCH FROM (created_at - t.created_at)))
                LIMIT 1
            ), 0),
            t.created_at,
            t.maker_address = $1
        FROM trades t
        WHERE (t.maker_address = $1 OR t.taker_address = $1) AND t.symbol = $2
          AND ($4::uuid       IS NULL OR t.maker_order_id = $4 OR t.taker_order_id = $4)
          AND ($5::bigint     IS NULL OR t.created_at >= to_timestamp($5::bigint / 1000.0))
          AND ($6::bigint     IS NULL OR t.created_at <= to_timestamp($6::bigint / 1000.0))
          AND ($7::bigint     IS NULL OR (extract(epoch from t.created_at) * 1000)::bigint >= $7)
        ORDER BY t.created_at DESC
        LIMIT $3
        "#,
    )
    .bind(&user_addr)
    .bind(&symbol)
    .bind(limit)
    .bind(order_id)
    .bind(q.start_time)
    .bind(q.end_time)
    .bind(q.from_id)
    .fetch_all(&state.db.pool)
    .await
    .map_err(|e| internal_error(&e.to_string()))?;

    let items: Vec<UserTradeItem> = rows
        .into_iter()
        .map(
            |(id, order_id, side, price, amount, fee, rpnl, time, is_maker)| {
                let is_buyer = side == "buy";
                UserTradeItem {
                    symbol: symbol.clone(),
                    id: id.to_string(),
                    order_id: order_id.to_string(),
                    side: side.to_uppercase(),
                    price: price.to_string(),
                    qty: amount.to_string(),
                    quote_qty: (price * amount).to_string(),
                    realized_pnl: rpnl.to_string(),
                    commission: fee.abs().to_string(),
                    commission_asset: state.config.collateral_symbol().to_string(),
                    time: time.timestamp_millis(),
                    buyer: is_buyer,
                    maker: is_maker,
                }
            },
        )
        .collect();

    Ok(Json(items))
}

// ─── 8.1 User Trades Slippage ─────────────────────────────────────
// GET /fapi/v1/userTrades/slippage
//
// Returns per-trade slippage for the authenticated user. Expected price is
// `orders.mark_price_at_creation` (captured at submission time for both LIMIT
// and MARKET orders). Slippage is signed so that positive = unfavorable to
// the user (paid more on BUY, received less on SELL).

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct SlippageQuery {
    pub symbol: Option<String>,
    #[serde(rename = "startTime")]
    pub start_time: Option<i64>,
    #[serde(rename = "endTime")]
    pub end_time: Option<i64>,
    pub limit: Option<i64>,
    pub timestamp: Option<i64>,
}

#[derive(Serialize)]
pub struct SlippageItem {
    pub symbol: String,
    #[serde(rename = "tradeId")]
    pub trade_id: String,
    #[serde(rename = "orderId")]
    pub order_id: String,
    pub side: String,
    #[serde(rename = "orderType")]
    pub order_type: String,
    #[serde(rename = "expectedPrice")]
    pub expected_price: String,
    #[serde(rename = "executedPrice")]
    pub executed_price: String,
    pub qty: String,
    #[serde(rename = "slippageAbs")]
    pub slippage_abs: String,
    #[serde(rename = "slippageBps")]
    pub slippage_bps: String,
    pub maker: bool,
    pub time: i64,
}

pub async fn user_trades_slippage(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Query(q): Query<SlippageQuery>,
) -> ApiResult<Vec<SlippageItem>> {
    let user_addr = auth_user.address.to_lowercase();
    let limit = q.limit.unwrap_or(500).clamp(1, 1000);
    let symbol = q.symbol.as_deref().map(normalize_symbol);
    let start = q.start_time.and_then(DateTime::<Utc>::from_timestamp_millis);
    let end = q.end_time.and_then(DateTime::<Utc>::from_timestamp_millis);

    let rows: Vec<(
        Uuid, Uuid, String, String, String,
        Option<Decimal>, Decimal, Decimal, Decimal,
        DateTime<Utc>, bool,
    )> = sqlx::query_as(
        r#"
        SELECT
            t.id,
            CASE WHEN t.maker_address = $1 THEN t.maker_order_id ELSE t.taker_order_id END AS order_id,
            t.symbol,
            CASE WHEN t.taker_address = $1 THEN t.side::text ELSE
                CASE WHEN t.side::text = 'buy' THEN 'sell' ELSE 'buy' END
            END AS user_side,
            o.order_type::text,
            o.price AS limit_price,
            COALESCE(o.mark_price_at_creation, o.price, t.price) AS expected_price,
            t.price AS fill_price,
            t.amount,
            t.created_at,
            (t.maker_address = $1) AS is_maker
        FROM trades t
        JOIN orders o
          ON o.id = CASE WHEN t.maker_address = $1 THEN t.maker_order_id ELSE t.taker_order_id END
        WHERE (t.maker_address = $1 OR t.taker_address = $1)
          AND ($2::text IS NULL OR t.symbol = $2)
          AND ($3::timestamptz IS NULL OR t.created_at >= $3)
          AND ($4::timestamptz IS NULL OR t.created_at <= $4)
        ORDER BY t.created_at DESC
        LIMIT $5
        "#,
    )
    .bind(&user_addr)
    .bind(symbol.as_deref())
    .bind(start)
    .bind(end)
    .bind(limit)
    .fetch_all(&state.db.pool)
    .await
    .map_err(|e| internal_error(&e.to_string()))?;

    let items: Vec<SlippageItem> = rows
        .into_iter()
        .map(|(tid, oid, sym, side, otype, _lp, expected, fill, qty, time, is_maker)| {
            let sign = if side == "buy" { Decimal::ONE } else { -Decimal::ONE };
            let slip_abs = (fill - expected) * sign;
            let slip_bps = if expected.is_zero() {
                Decimal::ZERO
            } else {
                slip_abs / expected * Decimal::from(10_000)
            };
            SlippageItem {
                symbol: sym,
                trade_id: tid.to_string(),
                order_id: oid.to_string(),
                side: side.to_uppercase(),
                order_type: otype.to_uppercase(),
                expected_price: expected.to_string(),
                executed_price: fill.to_string(),
                qty: qty.to_string(),
                slippage_abs: slip_abs.to_string(),
                slippage_bps: slip_bps.round_dp(4).to_string(),
                maker: is_maker,
                time: time.timestamp_millis(),
            }
        })
        .collect();

    Ok(Json(items))
}

// ─── 9. Batch Orders ─────────────────────────────────────────────
// POST /fapi/v1/batchOrders (simplified — returns array of results)

// ─── 10. Batch Cancel Orders ──────────────────────────────────────
// DELETE /fapi/v1/batchOrders

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct BatchCancelQuery {
    pub symbol: String,
    #[serde(rename = "orderIdList")]
    pub order_id_list: Option<String>, // JSON array string
    pub timestamp: Option<i64>,
}

pub async fn batch_cancel_orders(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    headers: axum::http::HeaderMap,
    axum::extract::OriginalUri(original_uri): axum::extract::OriginalUri,
    Query(q): Query<BatchCancelQuery>,
) -> ApiResult<Vec<BinanceOrderResponse>> {
    if let Some(resp) =
        maybe_forward_batch_cancel(&state, &headers, &original_uri, &q.symbol).await?
    {
        return Ok(resp);
    }
    let _symbol = normalize_symbol(&q.symbol);
    let user_addr = auth_user.address.to_lowercase();

    let order_ids: Vec<String> = q
        .order_id_list
        .as_deref()
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or_default();

    let mut results = Vec::new();

    for oid_str in order_ids {
        let oid: Uuid = match oid_str.parse() {
            Ok(id) => id,
            Err(_) => continue,
        };

        let row: Option<(Uuid, String, String, String, Option<Decimal>, Decimal, Decimal, i32, DateTime<Utc>, Option<Uuid>, Option<Decimal>)> = sqlx::query_as(
            r#"
            SELECT id, symbol, side::text, order_type::text, price, amount, filled_amount, leverage, created_at, trigger_order_id, mark_price_at_creation
            FROM orders WHERE id = $1 AND user_address = $2 AND status IN ('open', 'partially_filled', 'pending')
            "#,
        )
        .bind(oid)
        .bind(&user_addr)
        .fetch_optional(&state.db.pool)
        .await
        .ok()
        .flatten();

        if let Some((id, sym, side, otype, price, amount, filled, leverage, created, trig_id, mark)) = row {
            if let Some(tid) = trig_id {
                let _ = state
                    .trigger_orders_service
                    .cancel_trigger_order(&user_addr, tid)
                    .await;
            } else {
                let _ = state.matching_engine.cancel_order(&sym, id, &user_addr);
            }
            sqlx::query("UPDATE orders SET status = 'cancelled', updated_at = NOW() WHERE id = $1")
                .bind(id)
                .execute(&state.db.pool)
                .await
                .ok();

            // Release the still-frozen portion (same shape as single
            // cancel; previously this loop dropped the freeze on the
            // floor — same leak as cancel_all_open_orders before
            // 2026-04-26).
            let p_used = price.or(mark).unwrap_or(Decimal::ZERO);
            let remaining = amount - filled;
            if remaining > Decimal::ZERO && p_used > Decimal::ZERO && leverage > 0 {
                let release = remaining * p_used / Decimal::from(leverage)
                    * Decimal::new(1005, 3);
                let collateral = state.config.collateral_symbol();
                let _ = sqlx::query(
                    "UPDATE balances SET available = available + $1, frozen = GREATEST(frozen - $1, 0), updated_at = NOW() \
                     WHERE user_address = $2 AND token = $3",
                )
                .bind(release)
                .bind(&user_addr)
                .bind(collateral)
                .execute(&state.db.pool)
                .await;
            }

            let p = price.unwrap_or(Decimal::ZERO);
            results.push(BinanceOrderResponse {
                order_id: id.to_string(),
                symbol: sym,
                status: "CANCELED".to_string(),
                client_order_id: id.to_string(),
                price: p.to_string(),
                avg_price: "0".to_string(),
                orig_qty: amount.to_string(),
                executed_qty: filled.to_string(),
                cum_quote: (filled * p).to_string(),
                time_in_force: "GTC".to_string(),
                order_type: otype.to_uppercase(),
                reduce_only: false,
                side: side.to_uppercase(),
                position_side: "BOTH".to_string(),
                stop_price: "0".to_string(),
                working_type: "CONTRACT_PRICE".to_string(),
                price_protect: false,
                orig_type: otype.to_uppercase(),
                update_time: Utc::now().timestamp_millis(),
                time: created.timestamp_millis(),
            });
        }
    }

    Ok(Json(results))
}

// ─── 11. Modify Order ─────────────────────────────────────────────
// PUT /fapi/v1/order

#[derive(Debug, Deserialize, Serialize)]
#[allow(dead_code)]
pub struct ModifyOrderRequest {
    pub symbol: String,
    #[serde(rename = "orderId")]
    pub order_id: Option<String>,
    #[serde(rename = "origClientOrderId")]
    pub orig_client_order_id: Option<String>,
    /// Optional. The side of an order is immutable on modify; the field
    /// is accepted (and validated against the resting order if present)
    /// to keep Binance SDKs that always send it happy, but it is not
    /// required.
    pub side: Option<String>,
    pub quantity: String,
    pub price: String,
    pub timestamp: Option<i64>,
}

pub async fn modify_order(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    headers: axum::http::HeaderMap,
    axum::extract::OriginalUri(original_uri): axum::extract::OriginalUri,
    Json(req): Json<ModifyOrderRequest>,
) -> ApiResult<BinanceOrderResponse> {
    let symbol_for_route = normalize_symbol(&req.symbol);
    if let Some(resp) = maybe_forward_modify_order(
        &state,
        &headers,
        &original_uri,
        &symbol_for_route,
        &req,
    )
    .await?
    {
        return Ok(resp);
    }
    let user_addr = auth_user.address.to_lowercase();
    let oid = resolve_order_uuid(
        &state.db.pool,
        &user_addr,
        req.order_id.as_deref(),
        req.orig_client_order_id.as_deref(),
    )
    .await?;

    let new_qty: Decimal = req
        .quantity
        .parse()
        .map_err(|_| bad_request(-1100, "Invalid quantity."))?;
    let new_price: Decimal = req
        .price
        .parse()
        .map_err(|_| bad_request(-1100, "Invalid price."))?;
    if new_qty <= Decimal::ZERO {
        return Err(bad_request(-1100, "Quantity must be positive."));
    }
    if new_price <= Decimal::ZERO {
        return Err(bad_request(-1100, "Price must be positive."));
    }

    // Snapshot the existing order. Only resting LIMITs may be modified.
    let row: Option<(
        String, String, String,
        Decimal, Decimal, Option<Decimal>, Option<Decimal>,
        i32, String, DateTime<Utc>, Option<String>,
    )> = sqlx::query_as(
        r#"
        SELECT symbol, side::text, order_type::text,
               amount, filled_amount, price, frozen_margin,
               leverage, COALESCE(time_in_force::text, 'gtc'), created_at, client_order_id
        FROM orders
        WHERE id = $1 AND user_address = $2
          AND status IN ('open', 'partially_filled')
          AND order_type = 'limit'
        "#,
    )
    .bind(oid)
    .bind(&user_addr)
    .fetch_optional(&state.db.pool)
    .await
    .map_err(|e| internal_error(&e.to_string()))?;

    let (symbol, side_str, otype, old_amount, filled, old_price_opt, old_frozen,
         leverage, tif, created, coid) =
        row.ok_or_else(|| bad_request(-2013, "Order does not exist or cannot be modified."))?;

    // If the client supplied `side`, it must agree with the resting order
    // (sides are immutable on modify). This catches accidentally targeting
    // the wrong order via a clientOrderId/orderId mix-up.
    if let Some(ref s) = req.side {
        if !s.eq_ignore_ascii_case(&side_str) {
            return Err(bad_request(
                -1102,
                &format!(
                    "side {} does not match the resting order side {}",
                    s.to_uppercase(),
                    side_str.to_uppercase()
                ),
            ));
        }
    }

    if new_qty < filled {
        return Err(bad_request(
            -1102,
            &format!(
                "New quantity {} cannot be smaller than already-filled {}",
                new_qty, filled
            ),
        ));
    }
    if leverage <= 0 {
        return Err(internal_error("Order has invalid leverage."));
    }

    let collateral_symbol = state.config.collateral_symbol();
    let buffer_mul = Decimal::ONE + MARGIN_BUFFER_RATE; // 1.005

    // Margin currently locked for the *unfilled* slice of the original
    // order. Two cases:
    //   - JWT-created orders carry a real `frozen_margin` from INSERT;
    //     pro-rata it by remaining/amount.
    //   - fapi-created orders have NULL frozen_margin (the developer
    //     INSERT never populates it); fall back to recomputing from the
    //     row's pre-modify shape, which is what the cancel handler also
    //     does — keeping the two paths in agreement.
    let old_remaining = old_amount - filled;
    let old_freeze_active = match old_frozen {
        Some(f) if f > Decimal::ZERO && old_amount > Decimal::ZERO => f * old_remaining / old_amount,
        _ => {
            let p = old_price_opt.unwrap_or(Decimal::ZERO);
            if p <= Decimal::ZERO {
                Decimal::ZERO
            } else {
                old_remaining * p / Decimal::from(leverage) * buffer_mul
            }
        }
    };

    let new_remaining = new_qty - filled;
    let new_freeze = new_remaining * new_price / Decimal::from(leverage) * buffer_mul;
    let delta = new_freeze - old_freeze_active;

    // Pre-check available so we don't cancel in the engine and then
    // discover we can't fund the new shape (the cancel can't be cleanly
    // rolled back). Only matters when the modify needs *more* margin.
    if delta > Decimal::ZERO {
        let avail: Option<(Decimal,)> = sqlx::query_as(
            "SELECT available FROM balances WHERE user_address = $1 AND token = $2",
        )
        .bind(&user_addr)
        .bind(collateral_symbol)
        .fetch_optional(&state.db.pool)
        .await
        .ok()
        .flatten();
        let avail_v = avail.map(|(a,)| a).unwrap_or(Decimal::ZERO);
        if avail_v < delta {
            return Err(bad_request(
                -2019,
                &format!(
                    "Margin insufficient for modify. Need additional {}, have {}",
                    delta, avail_v
                ),
            ));
        }
    }

    // Pull the order out of the matching engine before changing its
    // shape on disk. If the engine reports it's already gone (raced with
    // a fill / cancel), refuse the modify rather than fabricate state.
    match state.matching_engine.cancel_order(&symbol, oid, &user_addr) {
        Ok(true) => {}
        Ok(false) => {
            return Err(bad_request(
                -2011,
                "Order is no longer in the matching engine; cannot modify.",
            ));
        }
        Err(e) => {
            return Err(internal_error(&format!("matching engine cancel error: {}", e)));
        }
    }

    // Apply the freeze delta and the orders-row shape change atomically.
    // The engine cancel above is already committed (in-memory op, not
    // rollback-able), so a tx failure here leaves the engine without the
    // order while the DB still claims it's open — the client gets an
    // error and an operator/recovery sweep is needed to reconcile. That
    // is strictly worse than current state without this guard, which is
    // partial DB writes plus engine state drift.
    let mut tx = state
        .db
        .pool
        .begin()
        .await
        .map_err(|e| internal_error(&format!("Failed to begin modify tx: {}", e)))?;

    if delta > Decimal::ZERO {
        sqlx::query(
            "UPDATE balances SET available = available - $1, frozen = frozen + $1, updated_at = NOW() \
             WHERE user_address = $2 AND token = $3",
        )
        .bind(delta)
        .bind(&user_addr)
        .bind(collateral_symbol)
        .execute(&mut *tx)
        .await
        .map_err(|e| internal_error(&format!("Failed to debit balance on modify: {}", e)))?;
    } else if delta < Decimal::ZERO {
        let release = -delta;
        sqlx::query(
            "UPDATE balances SET available = available + $1, frozen = GREATEST(frozen - $1, 0), updated_at = NOW() \
             WHERE user_address = $2 AND token = $3",
        )
        .bind(release)
        .bind(&user_addr)
        .bind(collateral_symbol)
        .execute(&mut *tx)
        .await
        .map_err(|e| internal_error(&format!("Failed to release balance on modify: {}", e)))?;
    }

    // Persist the new shape AND the new frozen_margin so future cancels
    // stop relying on the cancel-fallback formula. Status flips back to
    // 'open' (engine reports the post-resubmit state below; we set it
    // there).
    sqlx::query(
        "UPDATE orders SET amount = $1, price = $2, frozen_margin = $3, updated_at = NOW() \
         WHERE id = $4",
    )
    .bind(new_qty)
    .bind(new_price)
    .bind(new_freeze)
    .bind(oid)
    .execute(&mut *tx)
    .await
    .map_err(|e| internal_error(&format!("Failed to update order shape on modify: {}", e)))?;

    tx.commit()
        .await
        .map_err(|e| internal_error(&format!("Failed to commit modify tx: {}", e)))?;

    // Resubmit to the engine with the same order_id and the new shape.
    // Only the unfilled slice goes back into the book; fees come from
    // the user's current VIP tier (mirrors new_order).
    let matching_side = match side_str.as_str() {
        "buy" => MatchingSide::Buy,
        "sell" => MatchingSide::Sell,
        _ => return Err(internal_error("Order has invalid side.")),
    };
    let matching_tif = match tif.as_str() {
        "ioc" => MatchingTimeInForce::IOC,
        "fok" => MatchingTimeInForce::FOK,
        _ => MatchingTimeInForce::GTC,
    };

    let effective_tier = crate::services::vip_tier::resolve(
        &state.db.pool,
        &state.vip_tier_event_sender,
        &user_addr,
    )
    .await;
    let multiplier = crate::utils::fee_tiers::discount_multiplier(&user_addr, true);
    let taker_fee_rate =
        crate::utils::fee_tiers::round_fee(effective_tier.current.taker * multiplier);
    let maker_fee_rate =
        crate::utils::fee_tiers::round_fee(effective_tier.current.maker * multiplier);

    let match_result = state
        .matching_engine
        .submit_order(
            oid,
            &symbol,
            &user_addr,
            matching_side,
            MatchingOrderType::Limit,
            new_remaining,
            Some(new_price),
            matching_tif,
            leverage as u32,
            taker_fee_rate,
            maker_fee_rate,
        )
        .map_err(|e| internal_error(&format!("Matching engine resubmit error: {}", e)))?;

    let post_status = match match_result.status {
        MatchingOrderStatus::Open => "open",
        MatchingOrderStatus::PartiallyFilled => "partially_filled",
        MatchingOrderStatus::Filled => "filled",
        MatchingOrderStatus::Cancelled => "cancelled",
        MatchingOrderStatus::Rejected => "rejected",
    };
    // filled_amount is cumulative — include the slice already filled
    // before the modify plus anything that filled on this resubmit.
    let total_filled = filled + match_result.filled_amount;
    sqlx::query(
        "UPDATE orders SET status = $1::order_status, filled_amount = $2, avg_price = $3 WHERE id = $4",
    )
    .bind(post_status)
    .bind(total_filled)
    .bind(match_result.average_price)
    .bind(oid)
    .execute(&state.db.pool)
    .await
    .ok();

    // If the resubmit produced fills, run the same post-match release
    // that new_order does so the buffer slice that didn't end up backing
    // a position flows back to `available` instead of staying frozen.
    let resubmit_filled = match_result.filled_amount;
    let order_status_enum = match match_result.status {
        MatchingOrderStatus::Open => OrderStatus::Open,
        MatchingOrderStatus::PartiallyFilled => OrderStatus::PartiallyFilled,
        MatchingOrderStatus::Filled => OrderStatus::Filled,
        MatchingOrderStatus::Cancelled => OrderStatus::Cancelled,
        MatchingOrderStatus::Rejected => OrderStatus::Rejected,
    };
    if resubmit_filled > Decimal::ZERO
        || matches!(order_status_enum, OrderStatus::Cancelled | OrderStatus::Rejected)
    {
        let initial_freeze = new_freeze; // freeze for the just-resubmitted slice
        let avg = match_result
            .average_price
            .unwrap_or(new_price);
        let collateral_to_position = if avg > Decimal::ZERO {
            resubmit_filled * avg / Decimal::from(leverage)
        } else {
            Decimal::ZERO
        };
        let (frozen_delta, available_delta) = match order_status_enum {
            OrderStatus::Filled | OrderStatus::Cancelled | OrderStatus::Rejected => {
                let avail = (initial_freeze - collateral_to_position).max(Decimal::ZERO);
                (initial_freeze, avail)
            }
            OrderStatus::PartiallyFilled if resubmit_filled > Decimal::ZERO => {
                let per_unit = if new_remaining > Decimal::ZERO {
                    initial_freeze / new_remaining
                } else {
                    Decimal::ZERO
                };
                let frozen = resubmit_filled * per_unit;
                let avail = (frozen - collateral_to_position).max(Decimal::ZERO);
                (frozen, avail)
            }
            _ => (Decimal::ZERO, Decimal::ZERO),
        };
        if frozen_delta > Decimal::ZERO {
            let _ = sqlx::query(
                "UPDATE balances SET frozen = GREATEST(frozen - $1, 0), available = available + $2, updated_at = NOW() \
                 WHERE user_address = $3 AND token = $4",
            )
            .bind(frozen_delta)
            .bind(available_delta)
            .bind(&user_addr)
            .bind(collateral_symbol)
            .execute(&state.db.pool)
            .await;
            let _ = sqlx::query(
                "UPDATE orders SET frozen_margin = GREATEST(frozen_margin - $1, 0) WHERE id = $2",
            )
            .bind(frozen_delta)
            .bind(oid)
            .execute(&state.db.pool)
            .await;
        }
    }

    let avg_price = match_result.average_price.unwrap_or(new_price);
    Ok(Json(BinanceOrderResponse {
        order_id: oid.to_string(),
        symbol,
        status: db_status_to_binance(post_status).to_string(),
        client_order_id: coid.unwrap_or_else(|| oid.to_string()),
        price: new_price.to_string(),
        avg_price: avg_price.to_string(),
        orig_qty: new_qty.to_string(),
        executed_qty: total_filled.to_string(),
        cum_quote: (total_filled * avg_price).to_string(),
        time_in_force: db_tif_to_binance(&tif).to_string(),
        order_type: otype.to_uppercase(),
        reduce_only: false,
        side: side_str.to_uppercase(),
        position_side: "BOTH".to_string(),
        stop_price: "0".to_string(),
        working_type: "CONTRACT_PRICE".to_string(),
        price_protect: false,
        orig_type: otype.to_uppercase(),
        update_time: Utc::now().timestamp_millis(),
        time: created.timestamp_millis(),
    }))
}

// ─── 12. Query Open Order ─────────────────────────────────────────
// GET /fapi/v1/openOrder (single)

pub async fn query_open_order(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Query(q): Query<QueryOrderParams>,
) -> ApiResult<BinanceOrderResponse> {
    // Binance contract: returns 200 only for active orders (NEW/PARTIALLY_FILLED).
    // Terminal-status orders (FILLED/CANCELED/REJECTED) and missing IDs → -2013.
    let user_addr = auth_user.address.to_lowercase();
    let oid = resolve_order_uuid(
        &state.db.pool,
        &user_addr,
        q.order_id.as_deref(),
        q.orig_client_order_id.as_deref(),
    )
    .await?;

    let row: Option<(
        Uuid, String, String, String,
        Option<Decimal>, Option<Decimal>, Decimal, Decimal, i32,
        String, String, DateTime<Utc>, DateTime<Utc>, Option<String>,
    )> = sqlx::query_as(
        r#"
        SELECT id, symbol, side::text, order_type::text,
               price, avg_price, amount, filled_amount, leverage,
               status::text, COALESCE(time_in_force::text, 'gtc'),
               created_at, updated_at, client_order_id
        FROM orders
        WHERE id = $1 AND user_address = $2
          AND status IN ('open', 'partially_filled', 'pending')
        "#,
    )
    .bind(oid)
    .bind(&user_addr)
    .fetch_optional(&state.db.pool)
    .await
    .map_err(|e| internal_error(&e.to_string()))?;

    let (id, symbol, side, otype, price, avg_price, amount, filled, _leverage, status, tif, created, updated, coid) =
        row.ok_or_else(|| not_found("Order does not exist."))?;

    let p = price.unwrap_or(Decimal::ZERO);
    let avg = avg_price.unwrap_or(Decimal::ZERO);
    Ok(Json(BinanceOrderResponse {
        order_id: id.to_string(),
        symbol,
        status: db_status_to_binance(&status).to_string(),
        client_order_id: coid.unwrap_or_else(|| id.to_string()),
        price: p.to_string(),
        avg_price: avg.to_string(),
        orig_qty: amount.to_string(),
        executed_qty: filled.to_string(),
        cum_quote: (filled * avg).to_string(),
        time_in_force: db_tif_to_binance(&tif).to_string(),
        order_type: otype.to_uppercase(),
        reduce_only: false,
        side: side.to_uppercase(),
        position_side: "BOTH".to_string(),
        stop_price: "0".to_string(),
        working_type: "CONTRACT_PRICE".to_string(),
        price_protect: false,
        orig_type: otype.to_uppercase(),
        update_time: updated.timestamp_millis(),
        time: created.timestamp_millis(),
    }))
}

// ─── 13. Force Orders (User Liquidations) ─────────────────────────
// GET /fapi/v1/forceOrders

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct ForceOrdersQuery {
    pub symbol: Option<String>,
    #[serde(rename = "startTime")]
    pub start_time: Option<i64>,
    #[serde(rename = "endTime")]
    pub end_time: Option<i64>,
    pub limit: Option<i64>,
    pub timestamp: Option<i64>,
}

pub async fn force_orders(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Query(q): Query<ForceOrdersQuery>,
) -> ApiResult<Vec<serde_json::Value>> {
    let user_addr = auth_user.address.to_lowercase();
    let limit = q.limit.unwrap_or(100).min(1000);

    let rows: Vec<(Uuid, String, String, Decimal, Decimal, DateTime<Utc>)> = if let Some(ref sym) = q.symbol {
        let symbol = normalize_symbol(sym);
        sqlx::query_as(
            r#"
            SELECT id, symbol, side::text, price, amount, created_at
            FROM liquidation_events
            WHERE user_address = $1 AND symbol = $2
            ORDER BY created_at DESC LIMIT $3
            "#,
        )
        .bind(&user_addr)
        .bind(&symbol)
        .bind(limit)
        .fetch_all(&state.db.pool)
        .await
    } else {
        sqlx::query_as(
            r#"
            SELECT id, symbol, side::text, price, amount, created_at
            FROM liquidation_events
            WHERE user_address = $1
            ORDER BY created_at DESC LIMIT $2
            "#,
        )
        .bind(&user_addr)
        .bind(limit)
        .fetch_all(&state.db.pool)
        .await
    }
    .unwrap_or_default();

    let items: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|(id, sym, side, price, amount, time)| {
            serde_json::json!({
                "orderId": id.to_string(),
                "symbol": sym,
                "status": "FILLED",
                "side": side.to_uppercase(),
                "type": "LIQUIDATION",
                "price": price.to_string(),
                "origQty": amount.to_string(),
                "executedQty": amount.to_string(),
                "time": time.timestamp_millis(),
                "updateTime": time.timestamp_millis()
            })
        })
        .collect();

    Ok(Json(items))
}

// ─── 14. Position Risk ────────────────────────────────────────────
// GET /fapi/v1/positionRisk

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct PositionRiskQuery {
    pub symbol: Option<String>,
    pub timestamp: Option<i64>,
}

#[derive(Serialize)]
pub struct PositionRiskItem {
    pub symbol: String,
    #[serde(rename = "positionAmt")]
    pub position_amt: String,
    #[serde(rename = "entryPrice")]
    pub entry_price: String,
    #[serde(rename = "breakEvenPrice")]
    pub break_even_price: String,
    #[serde(rename = "markPrice")]
    pub mark_price: String,
    #[serde(rename = "unRealizedProfit")]
    pub unrealized_profit: String,
    pub leverage: String,
    #[serde(rename = "marginType")]
    pub margin_type: String,
    #[serde(rename = "isolatedMargin")]
    pub isolated_margin: String,
    #[serde(rename = "positionSide")]
    pub position_side: String,
    pub notional: String,
    #[serde(rename = "isolatedWallet")]
    pub isolated_wallet: String,
    #[serde(rename = "updateTime")]
    pub update_time: i64,
}

pub async fn position_risk(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Query(q): Query<PositionRiskQuery>,
) -> ApiResult<Vec<PositionRiskItem>> {
    let user_addr = auth_user.address.to_lowercase();

    let rows: Vec<(Uuid, String, String, Decimal, Decimal, Decimal, i32, DateTime<Utc>)> = if let Some(ref sym) = q.symbol {
        let symbol = normalize_symbol(sym);
        sqlx::query_as(
            r#"
            SELECT id, symbol, side::text, size_in_usd, entry_price, collateral_amount, leverage, updated_at
            FROM positions WHERE user_address = $1 AND symbol = $2 AND status = 'open'
            "#,
        )
        .bind(&user_addr)
        .bind(&symbol)
        .fetch_all(&state.db.pool)
        .await
    } else {
        sqlx::query_as(
            r#"
            SELECT id, symbol, side::text, size_in_usd, entry_price, collateral_amount, leverage, updated_at
            FROM positions WHERE user_address = $1 AND status = 'open'
            "#,
        )
        .bind(&user_addr)
        .fetch_all(&state.db.pool)
        .await
    }
    .map_err(|e| internal_error(&e.to_string()))?;

    let symbols: Vec<String> = rows.iter().map(|(_, s, ..)| s.clone()).collect();
    let prices = state.price_feed_service.batch_get_mark_prices(&symbols).await;

    let items: Vec<PositionRiskItem> = rows
        .into_iter()
        .map(|(_id, symbol, side, size_usd, entry_price, collateral, leverage, updated)| {
            let mark = prices.get(&symbol).copied().unwrap_or(entry_price);
            let size_tokens = if entry_price > Decimal::ZERO {
                size_usd / entry_price
            } else {
                Decimal::ZERO
            };
            let is_long = side == "long";
            let signed_amt = if is_long { size_tokens } else { -size_tokens };
            let pnl = if is_long {
                (mark - entry_price) * size_tokens
            } else {
                (entry_price - mark) * size_tokens
            };
            let notional = size_tokens * mark;

            PositionRiskItem {
                symbol,
                position_amt: signed_amt.to_string(),
                entry_price: entry_price.to_string(),
                break_even_price: entry_price.to_string(),
                mark_price: mark.to_string(),
                unrealized_profit: pnl.round_dp(4).to_string(),
                leverage: leverage.to_string(),
                margin_type: "cross".to_string(),
                isolated_margin: collateral.to_string(),
                position_side: if is_long { "LONG" } else { "SHORT" }.to_string(),
                notional: notional.to_string(),
                isolated_wallet: collateral.to_string(),
                update_time: updated.timestamp_millis(),
            }
        })
        .collect();

    Ok(Json(items))
}

// ─── 15. ADL Quantile ─────────────────────────────────────────────
// GET /fapi/v1/adlQuantile

pub async fn adl_quantile(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Query(q): Query<PositionRiskQuery>,
) -> ApiResult<Vec<serde_json::Value>> {
    let user_addr = auth_user.address.to_lowercase();

    let rows: Vec<(String, String)> = if let Some(ref sym) = q.symbol {
        let symbol = normalize_symbol(sym);
        sqlx::query_as(
            "SELECT symbol, side::text FROM positions WHERE user_address = $1 AND symbol = $2 AND status = 'open'",
        )
        .bind(&user_addr)
        .bind(&symbol)
        .fetch_all(&state.db.pool)
        .await
    } else {
        sqlx::query_as(
            "SELECT symbol, side::text FROM positions WHERE user_address = $1 AND status = 'open'",
        )
        .bind(&user_addr)
        .fetch_all(&state.db.pool)
        .await
    }
    .unwrap_or_default();

    let items: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|(sym, side)| {
            let (long_q, short_q) = if side == "long" { (2, 0) } else { (0, 2) };
            serde_json::json!({
                "symbol": sym,
                "adlQuantile": {
                    "LONG": long_q,
                    "SHORT": short_q,
                    "BOTH": long_q.max(short_q)
                }
            })
        })
        .collect();

    Ok(Json(items))
}

// ─── 16. Change Leverage ──────────────────────────────────────────
// POST /fapi/v1/leverage

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct ChangeLeverageRequest {
    pub symbol: String,
    #[serde(deserialize_with = "deserialize_i32_or_string")]
    pub leverage: i32,
    pub timestamp: Option<i64>,
}

#[derive(Serialize)]
pub struct ChangeLeverageResponse {
    pub leverage: i32,
    #[serde(rename = "maxNotionalValue")]
    pub max_notional_value: String,
    pub symbol: String,
}

pub async fn change_leverage(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Json(req): Json<ChangeLeverageRequest>,
) -> ApiResult<ChangeLeverageResponse> {
    let symbol = normalize_symbol(&req.symbol);

    let max_leverage = state
        .market_config_service
        .get_config(&symbol)
        .await
        .map(|c| c.max_leverage)
        .unwrap_or(125);

    if req.leverage < 1 || req.leverage > max_leverage {
        return Err(bad_request(
            -4028,
            &format!("Leverage {} is not valid. Max: {}", req.leverage, max_leverage),
        ));
    }

    // Persist the per-user / per-symbol leverage. Until 2026-04-26 the
    // handler validated and returned the value but never wrote it
    // anywhere; new_order then defaulted to `market_cfg.max_leverage`
    // every time, so an MM client setting leverage=10 silently traded
    // at 100×. See migration 20260426060000_user_market_leverage.sql.
    let user_addr = auth_user.address.to_lowercase();
    sqlx::query(
        "INSERT INTO user_market_leverage (user_address, symbol, leverage) \
         VALUES ($1, $2, $3) \
         ON CONFLICT (user_address, symbol) DO UPDATE SET \
           leverage = EXCLUDED.leverage, updated_at = NOW()",
    )
    .bind(&user_addr)
    .bind(&symbol)
    .bind(req.leverage)
    .execute(&state.db.pool)
    .await
    .map_err(|e| internal_error(&format!("Failed to persist leverage: {}", e)))?;

    Ok(Json(ChangeLeverageResponse {
        leverage: req.leverage,
        max_notional_value: state
            .market_config_service
            .get_config(&symbol)
            .await
            .map(|c| c.max_position_size_usd.to_string())
            .unwrap_or_else(|| "10000000".to_string()),
        symbol,
    }))
}

// ─── Helper: Convert DB rows to Binance responses ─────────────────

fn rows_to_binance_orders(
    rows: Vec<(
        Uuid, String, String, String,
        Option<Decimal>, Option<Decimal>, Decimal, Decimal, i32,
        String, String, DateTime<Utc>, DateTime<Utc>, Option<String>,
        bool, Option<Decimal>,
    )>,
) -> Vec<BinanceOrderResponse> {
    rows.into_iter()
        .map(|(id, symbol, side, otype, price, avg_price, amount, filled, _leverage,
               status, tif, created, updated, coid, reduce_only, trigger_price)| {
            let p = price.unwrap_or(Decimal::ZERO);
            let avg = avg_price.unwrap_or(Decimal::ZERO);
            let binance_type = db_type_to_binance(&otype);
            let stop_price_str = trigger_price
                .map(|d| d.to_string())
                .unwrap_or_else(|| "0".to_string());
            BinanceOrderResponse {
                order_id: id.to_string(),
                symbol,
                status: db_status_to_binance(&status).to_string(),
                client_order_id: coid.unwrap_or_else(|| id.to_string()),
                price: p.to_string(),
                avg_price: avg.to_string(),
                orig_qty: amount.to_string(),
                executed_qty: filled.to_string(),
                cum_quote: (filled * avg).to_string(),
                time_in_force: db_tif_to_binance(&tif).to_string(),
                order_type: binance_type.clone(),
                reduce_only,
                side: side.to_uppercase(),
                position_side: "BOTH".to_string(),
                stop_price: stop_price_str,
                working_type: "CONTRACT_PRICE".to_string(),
                price_protect: false,
                orig_type: binance_type,
                update_time: updated.timestamp_millis(),
                time: created.timestamp_millis(),
            }
        })
        .collect()
}
