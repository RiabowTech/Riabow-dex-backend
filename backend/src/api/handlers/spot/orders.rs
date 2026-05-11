//! REST handlers for spot trading orders.
//!
//! All endpoints behind the existing auth middleware (JWT or API Key); place
//! and cancel work for both. The cancel-all endpoint takes optional ?symbol=
//! query param to scope to one market.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Extension, Json,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

use crate::auth::middleware::AuthUser;
use crate::models::spot::{Side, OrderType, Tif, SpotOrder, SpotTrade};
use crate::services::spot::matching::types::{
    EngineHandle, PlaceOrderRequest, PlaceOrderError, CancelError,
};
use crate::AppState;

#[derive(Serialize)]
pub struct ErrorBody { pub error: String }

fn err(code: StatusCode, msg: impl Into<String>) -> (StatusCode, Json<ErrorBody>) {
    (code, Json(ErrorBody { error: msg.into() }))
}

fn engine(state: &AppState) -> Result<Arc<EngineHandle>, (StatusCode, Json<ErrorBody>)> {
    state.spot_engine.clone()
        .ok_or_else(|| err(StatusCode::SERVICE_UNAVAILABLE, "spot trading disabled"))
}

// ---------- POST /spot/orders ----------

#[derive(Deserialize)]
pub struct PlaceBody {
    pub symbol: String,
    pub side: String,
    pub r#type: String,                // "limit" | "market"
    pub tif: Option<String>,           // default "gtc" for limit; "ioc" for market
    pub price: Option<Decimal>,
    pub quantity: Option<Decimal>,
    pub quote_quantity: Option<Decimal>,
}

#[derive(Serialize)]
pub struct FillView {
    pub trade_id: String,
    pub price: String,
    pub quantity: String,
    pub fee: String,
    pub fee_token: String,
}

#[derive(Serialize)]
pub struct OrderView {
    pub id: String,
    pub symbol: String,
    pub side: String,
    pub r#type: String,
    pub tif: String,
    pub price: Option<String>,
    pub quantity: Option<String>,
    pub quote_quantity: Option<String>,
    pub filled_qty: String,
    pub avg_fill_price: String,
    pub status: String,
    pub reject_reason: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

impl OrderView {
    fn from_order(o: SpotOrder) -> Self {
        Self {
            id: o.id.to_string(),
            symbol: o.market_id,
            side: o.side,
            r#type: o.r#type,
            tif: o.tif,
            price: o.price.map(|d| d.normalize().to_string()),
            quantity: o.quantity.map(|d| d.normalize().to_string()),
            quote_quantity: o.quote_quantity.map(|d| d.normalize().to_string()),
            filled_qty: o.filled_qty.normalize().to_string(),
            avg_fill_price: o.avg_fill_price.normalize().to_string(),
            status: o.status,
            reject_reason: o.reject_reason,
            created_at: o.created_at.timestamp(),
            updated_at: o.updated_at.timestamp(),
        }
    }
}

#[derive(Serialize)]
pub struct PlaceResponse {
    #[serde(flatten)]
    pub order: OrderView,
    pub fills: Vec<FillView>,
}

pub async fn place_order(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<AuthUser>,
    Json(body): Json<PlaceBody>,
) -> Result<Json<PlaceResponse>, (StatusCode, Json<ErrorBody>)> {
    let eng = engine(&state)?;

    let side = Side::parse(&body.side)
        .ok_or_else(|| err(StatusCode::BAD_REQUEST, "invalid side"))?;
    let typ = OrderType::parse(&body.r#type)
        .ok_or_else(|| err(StatusCode::BAD_REQUEST, "invalid type"))?;
    let tif = match (body.tif.as_deref(), typ) {
        (Some(t), _)               => Tif::parse(t)
            .ok_or_else(|| err(StatusCode::BAD_REQUEST, "invalid tif"))?,
        (None, OrderType::Limit)   => Tif::Gtc,
        (None, OrderType::Market)  => Tif::Ioc,
    };

    let req = PlaceOrderRequest {
        user_address: user.address.to_lowercase(),
        market_id: body.symbol,
        side, r#type: typ, tif,
        price: body.price, quantity: body.quantity, quote_quantity: body.quote_quantity,
    };

    let result = eng.place(req).await;
    match result {
        Ok(ok) => {
            let fee_token_quote = "USDT".to_string();   // MVP single market
            // For each fill, determine fee/fee_token based on whether the caller
            // was the maker or taker — simplest in MVP: report taker fee with
            // the appropriate token. Since we don't know maker/taker from
            // SpotTrade alone (it has both), and the caller IS the taker for
            // a freshly placed order, taker_fee + appropriate token works.
            let fills = ok.fills.into_iter().map(|t| FillView {
                trade_id: t.id.to_string(),
                price: t.price.normalize().to_string(),
                quantity: t.quantity.normalize().to_string(),
                fee: t.taker_fee.normalize().to_string(),
                // taker is the caller; their fee is in their RECEIVED token
                // (BUY taker receives base, SELL taker receives quote).
                fee_token: match t.side.as_str() {
                    "buy"  => "DF".to_string(),     // taker buys → received DF
                    "sell" => fee_token_quote.clone(),
                    _ => fee_token_quote.clone(),
                },
            }).collect();
            Ok(Json(PlaceResponse {
                order: OrderView::from_order(ok.order),
                fills,
            }))
        }
        Err(e) => Err(map_place_error(e)),
    }
}

fn map_place_error(e: PlaceOrderError) -> (StatusCode, Json<ErrorBody>) {
    match e {
        PlaceOrderError::MarketNotFound      => err(StatusCode::NOT_FOUND, "MARKET_NOT_FOUND"),
        PlaceOrderError::MarketHalted        => err(StatusCode::CONFLICT, "MARKET_HALTED"),
        PlaceOrderError::MarketDelisted      => err(StatusCode::GONE,     "MARKET_DELISTED"),
        PlaceOrderError::InvalidTick         => err(StatusCode::BAD_REQUEST, "INVALID_TICK"),
        PlaceOrderError::InvalidLot          => err(StatusCode::BAD_REQUEST, "INVALID_LOT"),
        PlaceOrderError::BelowMinNotional    => err(StatusCode::BAD_REQUEST, "BELOW_MIN_NOTIONAL"),
        PlaceOrderError::InsufficientBalance => err(StatusCode::BAD_REQUEST, "INSUFFICIENT_BALANCE"),
        PlaceOrderError::PostOnlyReject      => err(StatusCode::BAD_REQUEST, "POST_ONLY_REJECT"),
        PlaceOrderError::SelfTrade           => err(StatusCode::BAD_REQUEST, "SELF_TRADE"),
        PlaceOrderError::InvalidRequest(m)   => err(StatusCode::BAD_REQUEST, m),
        PlaceOrderError::DbError(_)          => err(StatusCode::INTERNAL_SERVER_ERROR, "DB_ERROR"),
        PlaceOrderError::EngineBusy          => err(StatusCode::SERVICE_UNAVAILABLE, "ENGINE_BUSY"),
        PlaceOrderError::EngineRestarting    => err(StatusCode::SERVICE_UNAVAILABLE, "ENGINE_RESTARTING"),
    }
}

// ---------- DELETE /spot/orders/:id ----------

pub async fn cancel_order(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<AuthUser>,
    Path(id): Path<String>,
) -> Result<Json<OrderView>, (StatusCode, Json<ErrorBody>)> {
    let eng = engine(&state)?;
    let id = Uuid::parse_str(&id)
        .map_err(|_| err(StatusCode::BAD_REQUEST, "invalid id"))?;
    match eng.cancel(id, user.address.to_lowercase()).await {
        Ok(o)  => Ok(Json(OrderView::from_order(o))),
        Err(CancelError::NotFound)  => Err(err(StatusCode::NOT_FOUND, "ORDER_NOT_FOUND")),
        Err(CancelError::Forbidden) => Err(err(StatusCode::FORBIDDEN, "FORBIDDEN")),
        Err(CancelError::Db(_))     => Err(err(StatusCode::INTERNAL_SERVER_ERROR, "DB_ERROR")),
    }
}

// ---------- DELETE /spot/orders ----------

#[derive(Deserialize)]
pub struct CancelAllQuery { pub symbol: Option<String> }

#[derive(Serialize)]
pub struct CancelAllResponse { pub canceled: Vec<OrderView> }

pub async fn cancel_all(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<AuthUser>,
    Query(q): Query<CancelAllQuery>,
) -> Result<Json<CancelAllResponse>, (StatusCode, Json<ErrorBody>)> {
    let eng = engine(&state)?;
    let canceled = eng.cancel_all(user.address.to_lowercase(), q.symbol).await;
    Ok(Json(CancelAllResponse {
        canceled: canceled.into_iter().map(OrderView::from_order).collect(),
    }))
}

// ---------- GET /spot/orders/:id ----------

pub async fn get_order(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<AuthUser>,
    Path(id): Path<String>,
) -> Result<Json<OrderView>, (StatusCode, Json<ErrorBody>)> {
    let id = Uuid::parse_str(&id)
        .map_err(|_| err(StatusCode::BAD_REQUEST, "invalid id"))?;
    let user_lc = user.address.to_lowercase();
    let row: Option<SpotOrder> = sqlx::query_as(
        "SELECT * FROM spot_orders WHERE id=$1 AND user_address=$2"
    ).bind(id).bind(&user_lc).fetch_optional(&state.db.pool).await
        .map_err(|_| err(StatusCode::INTERNAL_SERVER_ERROR, "DB_ERROR"))?;
    let row = row.ok_or_else(|| err(StatusCode::NOT_FOUND, "ORDER_NOT_FOUND"))?;
    Ok(Json(OrderView::from_order(row)))
}

// ---------- GET /spot/orders ----------

#[derive(Deserialize)]
pub struct ListOrdersQuery {
    pub symbol: Option<String>,
    pub status: Option<String>,
    pub start: Option<i64>,
    pub end: Option<i64>,
    pub limit: Option<i64>,
}

pub async fn list_orders(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<AuthUser>,
    Query(q): Query<ListOrdersQuery>,
) -> Result<Json<Vec<OrderView>>, (StatusCode, Json<ErrorBody>)> {
    use sqlx::QueryBuilder;
    let user_lc = user.address.to_lowercase();
    let limit = q.limit.unwrap_or(50).clamp(1, 200);

    let mut qb: QueryBuilder<sqlx::Postgres> = QueryBuilder::new(
        "SELECT * FROM spot_orders WHERE user_address = "
    );
    qb.push_bind(user_lc);
    if let Some(s) = &q.symbol { qb.push(" AND market_id = ").push_bind(s); }
    if let Some(s) = &q.status { qb.push(" AND status = ").push_bind(s); }
    if let Some(start) = q.start {
        if let Some(dt) = chrono::DateTime::<chrono::Utc>::from_timestamp(start, 0) {
            qb.push(" AND created_at >= ").push_bind(dt);
        }
    }
    if let Some(end) = q.end {
        if let Some(dt) = chrono::DateTime::<chrono::Utc>::from_timestamp(end, 0) {
            qb.push(" AND created_at <= ").push_bind(dt);
        }
    }
    qb.push(" ORDER BY created_at DESC LIMIT ").push_bind(limit);
    let rows: Vec<SpotOrder> = qb.build_query_as::<SpotOrder>()
        .fetch_all(&state.db.pool).await
        .map_err(|_| err(StatusCode::INTERNAL_SERVER_ERROR, "DB_ERROR"))?;
    Ok(Json(rows.into_iter().map(OrderView::from_order).collect()))
}

// ---------- GET /spot/trades/me ----------

#[derive(Deserialize)]
pub struct ListTradesQuery {
    pub symbol: Option<String>,
    pub start: Option<i64>,
    pub end: Option<i64>,
    pub limit: Option<i64>,
}

#[derive(Serialize)]
pub struct TradeView {
    pub trade_id: String,
    pub symbol: String,
    pub side: String,
    pub price: String,
    pub quantity: String,
    pub fee: String,
    pub fee_token: String,
    pub role: String,             // "maker" | "taker"
    pub order_id: String,
    pub ts: i64,
}

pub async fn trades_me(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<AuthUser>,
    Query(q): Query<ListTradesQuery>,
) -> Result<Json<Vec<TradeView>>, (StatusCode, Json<ErrorBody>)> {
    use sqlx::QueryBuilder;
    let user_lc = user.address.to_lowercase();
    let limit = q.limit.unwrap_or(50).clamp(1, 200);
    let mut qb: QueryBuilder<sqlx::Postgres> = QueryBuilder::new(
        "SELECT * FROM spot_trades WHERE (maker_user = "
    );
    qb.push_bind(&user_lc);
    qb.push(" OR taker_user = ").push_bind(&user_lc).push(")");
    if let Some(s) = &q.symbol { qb.push(" AND market_id = ").push_bind(s); }
    if let Some(start) = q.start {
        if let Some(dt) = chrono::DateTime::<chrono::Utc>::from_timestamp(start, 0) {
            qb.push(" AND created_at >= ").push_bind(dt);
        }
    }
    if let Some(end) = q.end {
        if let Some(dt) = chrono::DateTime::<chrono::Utc>::from_timestamp(end, 0) {
            qb.push(" AND created_at <= ").push_bind(dt);
        }
    }
    qb.push(" ORDER BY created_at DESC LIMIT ").push_bind(limit);
    let rows: Vec<SpotTrade> = qb.build_query_as::<SpotTrade>()
        .fetch_all(&state.db.pool).await
        .map_err(|_| err(StatusCode::INTERNAL_SERVER_ERROR, "DB_ERROR"))?;

    // Determine role + fee + fee_token per trade for the caller's perspective
    let views = rows.into_iter().map(|t| {
        let is_maker = t.maker_user == user_lc;
        let role = if is_maker { "maker" } else { "taker" };
        let order_id = if is_maker { t.maker_order_id } else { t.taker_order_id };
        // Fee in received token: BUY side receives base, SELL receives quote.
        // Taker side is t.side; maker side is opposite.
        let caller_side = if is_maker {
            match t.side.as_str() { "buy" => "sell", "sell" => "buy", _ => "?" }
        } else {
            t.side.as_str()
        };
        let (fee, fee_token) = if is_maker {
            (t.maker_fee, match caller_side { "buy" => "DF", "sell" => "USDT", _ => "USDT" })
        } else {
            (t.taker_fee, match caller_side { "buy" => "DF", "sell" => "USDT", _ => "USDT" })
        };
        TradeView {
            trade_id: t.id.to_string(),
            symbol: t.market_id,
            side: caller_side.to_string(),
            price: t.price.normalize().to_string(),
            quantity: t.quantity.normalize().to_string(),
            fee: fee.normalize().to_string(),
            fee_token: fee_token.to_string(),
            role: role.to_string(),
            order_id: order_id.to_string(),
            ts: t.created_at.timestamp(),
        }
    }).collect();
    Ok(Json(views))
}
