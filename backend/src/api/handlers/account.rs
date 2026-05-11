//! Account API Handlers
//!
//! Phase 9: Complete account data layer with real database queries

use axum::{
    extract::{Query, State},
    http::StatusCode,
    Extension, Json,
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use uuid::Uuid;

use crate::auth::middleware::AuthUser;
use crate::models::{BalanceResponse, UserProfile};
use crate::AppState;

static EMAIL_MIGRATION_DONE: AtomicBool = AtomicBool::new(false);

// Helper module to serialize DateTime as milliseconds timestamp
mod datetime_as_millis {
    use chrono::{DateTime, Utc};
    use serde::Serializer;

    pub fn serialize<S>(dt: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_i64(dt.timestamp_millis())
    }
}

#[derive(Debug, Serialize)]
pub struct BalancesResponse {
    pub balances: Vec<BalanceResponse>,
}

/// Simplified position response for API
#[derive(Debug, Serialize)]
pub struct PositionDetail {
    pub id: Uuid,
    /// Alias for id, for frontend compatibility
    #[serde(rename = "position_id")]
    pub position_id: Uuid,
    pub symbol: String,
    pub side: String,
    /// Position size in USDT
    pub size: Decimal,
    /// Position amount in token units (e.g., BTC quantity)
    pub amount: Decimal,
    pub entry_price: Decimal,
    pub mark_price: Decimal,
    pub liquidation_price: Decimal,
    pub collateral_amount: Decimal,
    pub leverage: i32,
    pub unrealized_pnl: Decimal,
    pub unrealized_pnl_percent: Decimal,
    pub realized_pnl: Decimal,
    pub margin_ratio: Decimal,
    pub accumulated_funding_fee: Decimal,
    /// Take-profit trigger price (null = no TP set)
    pub take_profit_price: Option<Decimal>,
    /// Stop-loss trigger price (null = no SL set)
    pub stop_loss_price: Option<Decimal>,
    #[serde(serialize_with = "datetime_as_millis::serialize")]
    pub created_at: DateTime<Utc>,
    #[serde(serialize_with = "datetime_as_millis::serialize")]
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
pub struct PositionsResponse {
    pub positions: Vec<PositionDetail>,
    pub total_unrealized_pnl: Decimal,
    pub total_collateral: Decimal,
}

#[derive(Debug, Serialize)]
pub struct OrdersResponse {
    pub orders: Vec<OrderDetail>,
    /// Legacy `offset + rows` approximation (±1 when there are more results).
    /// `-1` when a cursor is used — follow `next_cursor` instead.
    pub total: i64,
    /// Opaque pagination cursor (`<timestamp_millis>_<uuid>`). Present on
    /// every response that has a next page; `null` on the last page. Pass
    /// it back as `?cursor=…` to read the next page without OFFSET.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct TradesResponse {
    pub trades: Vec<TradeRecord>,
    /// See OrdersResponse.total — same semantics.
    pub total: i64,
    /// See OrdersResponse.next_cursor — same semantics.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct OrderDetail {
    pub id: Uuid,
    pub symbol: String,
    pub side: String,
    pub order_type: String,
    /// Order price (for limit orders) or execution price (for market orders)
    pub price: Decimal,
    /// Order size in USDT (= amount * price)
    pub size: Decimal,
    /// Order amount in tokens (e.g., BTC quantity)
    pub amount: Decimal,
    pub filled_amount: Decimal,
    pub leverage: i32,
    pub status: String,
    /// Mark price at order creation time
    pub mark_price: Decimal,
    pub reduce_only: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trigger_condition: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_in: Option<i64>,
    /// Take-profit price set at order creation (null if not set)
    pub tp_price: Option<Decimal>,
    /// Stop-loss price set at order creation (null if not set)
    pub sl_price: Option<Decimal>,
    #[serde(serialize_with = "datetime_as_millis::serialize")]
    pub created_at: DateTime<Utc>,
    #[serde(serialize_with = "datetime_as_millis::serialize")]
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TradeRole {
    Maker,
    Taker,
}

#[derive(Debug, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TradeType {
    /// 常规 P2P 撮合成交。
    Trade,
    /// 预留：清算触发的成交，后续可区分。
    Liquidation,
}

#[derive(Debug, Serialize)]
pub struct TradeRecord {
    pub id: Uuid,
    pub order_id: Uuid,
    pub symbol: String,
    pub side: String,
    pub price: Decimal,
    pub amount: Decimal,
    pub fee: Decimal,
    pub realized_pnl: Option<Decimal>,
    pub trade_value: Decimal,
    pub role: TradeRole,
    #[serde(rename = "type")]
    pub trade_type: TradeType,
    #[serde(serialize_with = "datetime_as_millis::serialize")]
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
pub struct OrdersQuery {
    pub symbol: Option<String>,
    pub status: Option<String>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    /// Opaque cursor from a previous response's `next_cursor`. When present,
    /// `offset` is ignored and the server does a keyset scan using
    /// `(created_at, id) < (cursor_ts, cursor_id)`. Required for deep
    /// pagination (OFFSET 5000+ is O(N) in this schema).
    pub cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TradesQuery {
    pub symbol: Option<String>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    /// See OrdersQuery.cursor — same semantics.
    pub cursor: Option<String>,
}

/// Opaque pagination cursor: `<timestamp_millis>_<uuid>`. Chosen because
/// both /account/trades and /account/orders order by `(created_at DESC, id)`
/// and this tuple is stable + unique.
fn parse_cursor(s: &str) -> Option<(DateTime<Utc>, Uuid)> {
    let (ts_str, id_str) = s.split_once('_')?;
    let ts_millis: i64 = ts_str.parse().ok()?;
    let ts = DateTime::<Utc>::from_timestamp_millis(ts_millis)?;
    let id = Uuid::parse_str(id_str).ok()?;
    Some((ts, id))
}

fn build_cursor(ts: DateTime<Utc>, id: Uuid) -> String {
    format!("{}_{}", ts.timestamp_millis(), id)
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub code: String,
}

/// Get user profile
/// GET /account/profile
pub async fn get_profile(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
) -> Result<Json<UserProfile>, (StatusCode, Json<ErrorResponse>)> {
    // One-time migration: add email columns if they don't exist
    if !EMAIL_MIGRATION_DONE.load(Ordering::Relaxed) {
        let _ = sqlx::query(
            r#"
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'users' AND column_name = 'email'
                ) THEN
                    ALTER TABLE users ADD COLUMN email VARCHAR(255);
                    ALTER TABLE users ADD COLUMN email_verified BOOLEAN DEFAULT false;
                END IF;
            END $$;
            "#,
        )
        .execute(&state.db.pool)
        .await;
        EMAIL_MIGRATION_DONE.store(true, Ordering::Relaxed);
    }

    // Try to fetch from database using tuple query
    let user: Option<(
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        bool,
        DateTime<Utc>,
    )> = sqlx::query_as(
        r#"
        SELECT
            address,
            referral_code,
            referrer_address,
            email,
            COALESCE(email_verified, false) as email_verified,
            created_at
        FROM users
        WHERE address = $1
        "#,
    )
    .bind(&auth_user.address.to_lowercase())
    .fetch_optional(&state.db.pool)
    .await
    .map_err(|e| {
        tracing::error!("Failed to fetch user profile: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "获取用户信息失败".to_string(),
                code: "PROFILE_FETCH_FAILED".to_string(),
            }),
        )
    })?;

    // If user doesn't exist, create a new one
    if let Some((address, referral_code, referrer_address, email, email_verified, created_at)) =
        user
    {
        Ok(Json(UserProfile {
            address,
            referral_code,
            referrer_address,
            email,
            email_verified,
            created_at,
        }))
    } else {
        // Auto-create user record
        let now = Utc::now();
        sqlx::query(
            "INSERT INTO users (address, created_at) VALUES ($1, $2) ON CONFLICT (address) DO NOTHING"
        )
        .bind(&auth_user.address.to_lowercase())
        .bind(now)
        .execute(&state.db.pool)
        .await
        .ok();

        Ok(Json(UserProfile {
            address: auth_user.address.to_lowercase(),
            referral_code: None,
            referrer_address: None,
            email: None,
            email_verified: false,
            created_at: now,
        }))
    }
}

/// Get user balances
/// GET /account/balances
pub async fn get_balances(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
) -> Result<Json<BalancesResponse>, (StatusCode, Json<ErrorResponse>)> {
    // Fetch balances from database
    let rows: Vec<(String, Decimal, Decimal)> =
        sqlx::query_as("SELECT token, available, frozen FROM balances WHERE user_address = $1")
            .bind(&auth_user.address.to_lowercase())
            .fetch_all(&state.db.pool)
            .await
            .map_err(|e| {
                tracing::error!("Failed to fetch balances: {}", e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: "获取余额失败".to_string(),
                        code: "BALANCE_FETCH_FAILED".to_string(),
                    }),
                )
            })?;

    let balances: Vec<BalanceResponse> = rows
        .into_iter()
        .map(|(token, available, frozen)| BalanceResponse {
            token,
            available,
            frozen,
            total: available + frozen,
        })
        .collect();

    Ok(Json(BalancesResponse { balances }))
}

/// Get user positions
/// GET /account/positions
pub async fn get_positions(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
) -> Result<Json<PositionsResponse>, (StatusCode, Json<ErrorResponse>)> {
    // Fetch open positions from database - use size_in_usd and collateral_amount for GMX-style schema
    // LEFT JOIN position_tp_sl to include TP/SL prices in the response (avoids N+1)
    let rows: Vec<(Uuid, String, String, Decimal, Decimal, Decimal, i32, Decimal, Decimal, DateTime<Utc>, DateTime<Utc>, Option<Decimal>, Option<Decimal>)> = sqlx::query_as(
        r#"
        SELECT
            p.id, p.symbol, p.side::text, p.size_in_usd, p.entry_price,
            p.collateral_amount, p.leverage, p.realized_pnl, COALESCE(p.accumulated_funding_fee, 0) as accumulated_funding_fee,
            p.created_at, p.updated_at,
            tp.take_profit_price, tp.stop_loss_price
        FROM positions p
        LEFT JOIN position_tp_sl tp ON tp.position_id = p.id
        WHERE p.user_address = $1 AND p.status = 'open'
        ORDER BY p.created_at DESC
        "#
    )
    .bind(&auth_user.address.to_lowercase())
    .fetch_all(&state.db.pool)
    .await
    .map_err(|e| {
        tracing::error!("Failed to fetch positions: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "获取仓位失败".to_string(),
                code: "POSITION_FETCH_FAILED".to_string(),
            }),
        )
    })?;

    // Batch fetch all mark prices in one read (avoids N+1 queries)
    let symbols: Vec<String> = rows.iter().map(|(_, symbol, ..)| symbol.clone()).collect();
    let prices = state
        .price_feed_service
        .batch_get_mark_prices(&symbols)
        .await;

    let mut positions = Vec::new();
    let mut total_unrealized_pnl = Decimal::ZERO;
    let mut total_collateral = Decimal::ZERO;

    for (
        id,
        symbol,
        side,
        size,
        entry_price,
        collateral,
        leverage,
        realized_pnl,
        accumulated_funding_fee,
        created_at,
        updated_at,
        take_profit_price,
        stop_loss_price,
    ) in rows
    {
        // Use pre-fetched price, fallback to entry price
        let mark_price = prices.get(&symbol).copied().unwrap_or(entry_price);

        // Calculate unrealized PnL
        let is_long = side.to_lowercase() == "long";
        let size_in_tokens = if entry_price > Decimal::ZERO {
            size / entry_price
        } else {
            Decimal::ZERO
        };

        let unrealized_pnl = if is_long {
            (mark_price - entry_price) * size_in_tokens
        } else {
            (entry_price - mark_price) * size_in_tokens
        };

        // Calculate liquidation price
        let position_value = size;
        let maintenance_margin = position_value * Decimal::new(5, 3); // 0.5%
        let liq_distance = if size_in_tokens > Decimal::ZERO {
            (collateral - maintenance_margin) / size_in_tokens
        } else {
            Decimal::ZERO
        };

        let liquidation_price = if is_long {
            entry_price - liq_distance
        } else {
            entry_price + liq_distance
        };

        // Calculate margin ratio (Binance-style: Maintenance Margin / Equity)
        // - Equity = Collateral + Unrealized PnL
        // - Maintenance Margin = Current Position Value × MMR (0.5%)
        // - Returns 0.0~1.0+, higher = more dangerous, >= 1.0 = liquidation
        let equity = collateral + unrealized_pnl;
        let current_position_value = size_in_tokens * mark_price;
        let maintenance_margin_rate = Decimal::new(5, 3); // 0.5% MMR
        let maintenance_margin = current_position_value * maintenance_margin_rate;
        let margin_ratio = if equity > Decimal::ZERO {
            maintenance_margin / equity
        } else {
            Decimal::ONE // 100% risk if no equity
        };

        let unrealized_pnl_percent = if collateral > Decimal::ZERO {
            unrealized_pnl / collateral
        } else {
            Decimal::ZERO
        };

        // Accumulate totals
        total_unrealized_pnl += unrealized_pnl;
        total_collateral += collateral;

        positions.push(PositionDetail {
            id,
            position_id: id,
            symbol,
            side,
            size,
            amount: size_in_tokens,
            entry_price,
            mark_price,
            liquidation_price: liquidation_price.max(Decimal::ZERO),
            collateral_amount: collateral,
            leverage,
            unrealized_pnl,
            unrealized_pnl_percent,
            realized_pnl,
            margin_ratio,
            accumulated_funding_fee,
            take_profit_price,
            stop_loss_price,
            created_at,
            updated_at,
        });
    }

    Ok(Json(PositionsResponse {
        positions,
        total_unrealized_pnl,
        total_collateral,
    }))
}

/// Get user orders with filtering
/// GET /account/orders
pub async fn get_orders(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Query(query): Query<OrdersQuery>,
) -> Result<Json<OrdersResponse>, (StatusCode, Json<ErrorResponse>)> {
    let limit = query.limit.unwrap_or(50).min(200);
    let offset = query.offset.unwrap_or(0);
    let user_address = auth_user.address.to_lowercase();

    // Parse opaque cursor if present. Invalid cursor → 400 (not silently
    // falling back to OFFSET, which would hide client bugs).
    let cursor_parsed = match query.cursor.as_deref() {
        Some(c) => match parse_cursor(c) {
            Some(p) => Some(p),
            None => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: "cursor 格式无效，应为 <timestamp_ms>_<uuid>".to_string(),
                        code: "INVALID_CURSOR".to_string(),
                    }),
                ));
            }
        },
        None => None,
    };

    // Fetch limit+1 rows to detect whether there's a next page (avoids slow COUNT).
    let fetch_limit = limit + 1;

    let mut sql = String::from(
        r#"
        SELECT
            id, symbol, side::TEXT, order_type::TEXT,
            COALESCE(price, 0) as price, amount,
            filled_amount, leverage, status::TEXT,
            COALESCE(mark_price_at_creation, price, 0) as mark_price,
            reduce_only, created_at, updated_at,
            tp_price, sl_price
        FROM orders
        WHERE user_address = $1
        "#,
    );
    let mut param_idx = 2;

    if query.symbol.is_some() {
        sql.push_str(&format!(" AND symbol = ${}", param_idx));
        param_idx += 1;
    }
    if query.status.is_some() {
        sql.push_str(&format!(" AND status = ${}::order_status", param_idx));
        param_idx += 1;
    }
    // Keyset seek: ORDER BY created_at DESC tuple comparison.
    if cursor_parsed.is_some() {
        sql.push_str(&format!(
            " AND (created_at, id) < (${}, ${})",
            param_idx,
            param_idx + 1
        ));
        param_idx += 2;
    }
    sql.push_str(" ORDER BY created_at DESC, id DESC");
    sql.push_str(&format!(" LIMIT ${}", param_idx));
    param_idx += 1;
    // OFFSET is skipped when a cursor is used: cursor + OFFSET combined
    // would be ambiguous and the keyset scan already starts past the cursor.
    if cursor_parsed.is_none() {
        sql.push_str(&format!(" OFFSET ${}", param_idx));
    }

    // Bind in positional order matching the SQL built above.
    let mut q = sqlx::query_as::<
        _,
        (
            Uuid,
            String,
            String,
            String,
            Decimal,
            Decimal,
            Decimal,
            i32,
            String,
            Decimal,
            bool,
            DateTime<Utc>,
            DateTime<Utc>,
            Option<Decimal>,
            Option<Decimal>,
        ),
    >(&sql)
    .bind(&user_address);
    if let Some(ref s) = query.symbol {
        q = q.bind(s);
    }
    if let Some(ref s) = query.status {
        q = q.bind(s);
    }
    if let Some((ts, id)) = cursor_parsed {
        q = q.bind(ts).bind(id);
    }
    q = q.bind(fetch_limit);
    if cursor_parsed.is_none() {
        q = q.bind(offset);
    }

    let mut rows = q.fetch_all(&state.db.pool).await.map_err(|e| {
        tracing::error!("Failed to fetch orders: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "获取订单失败".to_string(),
                code: "ORDER_FETCH_FAILED".to_string(),
            }),
        )
    })?;

    let has_more = rows.len() as i64 > limit;
    if has_more {
        rows.pop();
    }

    // In cursor mode we can't compute a meaningful total without extra work;
    // return -1 as the sentinel so clients know to use next_cursor.
    let total = if cursor_parsed.is_some() {
        -1
    } else if has_more {
        offset + limit + 1
    } else {
        offset + rows.len() as i64
    };

    // Build next_cursor from the last row of the current page if more exist.
    let next_cursor = if has_more {
        rows.last().map(|r| build_cursor(r.11, r.0))
    } else {
        None
    };

    // Collect unique symbols to batch fetch current prices for fallback
    let symbols: Vec<String> = rows.iter().map(|(_, symbol, ..)| symbol.clone()).collect();
    let current_prices = state
        .price_feed_service
        .batch_get_mark_prices(&symbols)
        .await;

    let orders: Vec<OrderDetail> = rows
        .into_iter()
        .map(
            |(
                id,
                symbol,
                side,
                order_type,
                price,
                amount,
                filled_amount,
                leverage,
                status,
                mark_price,
                reduce_only,
                created_at,
                updated_at,
                tp_price,
                sl_price,
            )| {
                // Use current price as fallback if price/mark_price is 0
                let current_price = current_prices
                    .get(&symbol)
                    .copied()
                    .unwrap_or(Decimal::ZERO);
                let effective_price = if price > Decimal::ZERO {
                    price
                } else {
                    current_price
                };
                let effective_mark_price = if mark_price > Decimal::ZERO {
                    mark_price
                } else {
                    current_price
                };

                // Calculate size in USDT
                let size = amount * effective_price;

                OrderDetail {
                    id,
                    symbol,
                    side,
                    order_type,
                    price: effective_price,
                    size,
                    amount,
                    filled_amount,
                    leverage,
                    status,
                    mark_price: effective_mark_price,
                    reduce_only,
                    trigger_condition: None,
                    expires_in: None,
                    tp_price,
                    sl_price,
                    created_at,
                    updated_at,
                }
            },
        )
        .collect();

    Ok(Json(OrdersResponse {
        orders,
        total,
        next_cursor,
    }))
}

/// Get user trades history
/// GET /account/trades
pub async fn get_trades(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Query(query): Query<TradesQuery>,
) -> Result<Json<TradesResponse>, (StatusCode, Json<ErrorResponse>)> {
    let limit = query.limit.unwrap_or(50).min(200);
    let offset = query.offset.unwrap_or(0);
    let user_address = auth_user.address.to_lowercase();

    let cursor_parsed = match query.cursor.as_deref() {
        Some(c) => match parse_cursor(c) {
            Some(p) => Some(p),
            None => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: "cursor 格式无效，应为 <timestamp_ms>_<uuid>".to_string(),
                        code: "INVALID_CURSOR".to_string(),
                    }),
                ));
            }
        },
        None => None,
    };

    // Outer page fetches limit+1 rows to detect has_more without COUNT on a
    // 28M-row hypertable.
    let fetch_limit = limit + 1;

    // Each branch (maker / taker) must contribute enough rows that after the
    // merge + outer OFFSET skip, we still satisfy fetch_limit. Worst case is
    // all user rows land in one branch. Cursor path offsets at the branch
    // level, so it needs only fetch_limit there.
    let branch_limit = if cursor_parsed.is_some() {
        fetch_limit
    } else {
        offset + fetch_limit
    };

    // Rewrite of the old `WHERE maker = $1 OR taker = $1` + LATERAL form.
    //
    // Why this shape: on TimescaleDB the OR-form lures the planner into
    // `Custom Scan (ChunkAppend)` over `idx_trades_created_at` with a heap
    // Filter for the address match — at 28M trade rows / 24GB this costs
    // ~17s per request on prod because each chunk returns 1–7M rows through
    // the Filter to pluck ≤ 29 user rows. A 2026-04-24 EXPLAIN measured
    // 28,573,081 shared buffer hits for one request.
    //
    // Splitting the OR into two UNION ALL branches lets the planner pick
    // `idx_trades_{maker,taker}_address_created_desc` per branch
    // (pre-sorted by created_at DESC), Merge Append them, and answer in
    // ~7ms / 262 buffers. Self-trades (maker = taker) would be double-
    // counted; we exclude them from the taker branch since the maker
    // branch already catches them.
    //
    // PnL resolution on the outer LATERAL is unchanged: exact trade_id hit
    // preferred, legacy ±5s time-proximity as fallback.
    let mut inner_filters = String::new();
    let mut param_idx = 2;
    if query.symbol.is_some() {
        inner_filters.push_str(&format!(" AND symbol = ${}", param_idx));
        param_idx += 1;
    }
    if cursor_parsed.is_some() {
        inner_filters.push_str(&format!(
            " AND (created_at, id) < (${}, ${})",
            param_idx,
            param_idx + 1
        ));
        param_idx += 2;
    }
    let branch_limit_idx = param_idx;
    param_idx += 1;
    let outer_limit_idx = param_idx;
    param_idx += 1;
    let outer_offset_idx = if cursor_parsed.is_none() {
        let idx = param_idx;
        param_idx += 1;
        Some(idx)
    } else {
        None
    };
    let _ = param_idx; // silence unused warning on the last increment path

    let maker_branch = format!(
        r#"
        SELECT id, symbol, side, price, amount,
               maker_address, taker_address,
               maker_order_id, taker_order_id,
               maker_fee, taker_fee, created_at
        FROM trades
        WHERE maker_address = $1{inner_filters}
        ORDER BY created_at DESC, id DESC
        LIMIT ${branch_limit_idx}
        "#,
        inner_filters = inner_filters,
        branch_limit_idx = branch_limit_idx,
    );
    let taker_branch = format!(
        r#"
        SELECT id, symbol, side, price, amount,
               maker_address, taker_address,
               maker_order_id, taker_order_id,
               maker_fee, taker_fee, created_at
        FROM trades
        WHERE taker_address = $1 AND maker_address <> $1{inner_filters}
        ORDER BY created_at DESC, id DESC
        LIMIT ${branch_limit_idx}
        "#,
        inner_filters = inner_filters,
        branch_limit_idx = branch_limit_idx,
    );

    let mut sql = format!(
        r#"
        WITH user_trades AS (
            ({maker_branch})
            UNION ALL
            ({taker_branch})
        )
        SELECT
            t.id,
            CASE WHEN t.maker_address = $1 THEN t.maker_order_id ELSE t.taker_order_id END as order_id,
            t.symbol,
            CASE WHEN t.taker_address = $1 THEN t.side::TEXT ELSE
                CASE WHEN t.side::TEXT = 'buy' THEN 'sell' ELSE 'buy' END
            END as side,
            t.price,
            t.amount,
            CASE WHEN t.maker_address = $1 THEN t.maker_fee ELSE t.taker_fee END as fee,
            rpe.realized_pnl as realized_pnl,
            (t.maker_address = $1) as is_maker,
            t.created_at
        FROM user_trades t
        LEFT JOIN LATERAL (
            -- realized_pnl_events stores one row per side per trade.
            -- Must filter by user_address on BOTH branches or this lookup
            -- can return the counterparty's row (P0 leak: 2026-04-25 QA).
            SELECT realized_pnl
            FROM realized_pnl_events
            WHERE user_address = $1 AND (
                  trade_id = t.id
                OR (trade_id IS NULL
                    AND symbol = t.symbol
                    AND created_at BETWEEN t.created_at - interval '5 seconds' AND t.created_at + interval '5 seconds')
            )
            ORDER BY (trade_id = t.id) DESC NULLS LAST,
                     ABS(EXTRACT(EPOCH FROM (created_at - t.created_at)))
            LIMIT 1
        ) rpe ON true
        ORDER BY t.created_at DESC, t.id DESC
        LIMIT ${outer_limit_idx}
        "#,
        maker_branch = maker_branch,
        taker_branch = taker_branch,
        outer_limit_idx = outer_limit_idx,
    );
    if let Some(idx) = outer_offset_idx {
        sql.push_str(&format!(" OFFSET ${}", idx));
    }

    let mut q = sqlx::query_as::<
        _,
        (
            Uuid,
            Uuid,
            String,
            String,
            Decimal,
            Decimal,
            Decimal,
            Option<Decimal>,
            bool,
            DateTime<Utc>,
        ),
    >(&sql)
    .bind(&user_address);
    if let Some(ref s) = query.symbol {
        q = q.bind(s);
    }
    if let Some((ts, id)) = cursor_parsed {
        q = q.bind(ts).bind(id);
    }
    q = q.bind(branch_limit);
    q = q.bind(fetch_limit);
    if cursor_parsed.is_none() {
        q = q.bind(offset);
    }

    let mut rows = q.fetch_all(&state.db.pool).await.map_err(|e| {
        tracing::error!("Failed to fetch trades: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "获取交易历史失败".to_string(),
                code: "TRADE_FETCH_FAILED".to_string(),
            }),
        )
    })?;

    let has_more = rows.len() as i64 > limit;
    if has_more {
        rows.pop();
    }
    let total = if cursor_parsed.is_some() {
        -1
    } else if has_more {
        offset + limit + 1
    } else {
        offset + rows.len() as i64
    };
    let next_cursor = if has_more {
        rows.last().map(|r| build_cursor(r.9, r.0))
    } else {
        None
    };

    let trades: Vec<TradeRecord> = rows
        .into_iter()
        .map(
            |(
                id,
                order_id,
                symbol,
                side,
                price,
                amount,
                fee,
                realized_pnl,
                is_maker,
                timestamp,
            )| {
                let trade_value = price * amount;
                let role = if is_maker {
                    TradeRole::Maker
                } else {
                    TradeRole::Taker
                };
                // 预留：之后可通过 liquidator system account / liquidation 表关联区分。
                let trade_type = TradeType::Trade;

                TradeRecord {
                    id,
                    order_id,
                    symbol,
                    side,
                    price,
                    amount,
                    fee,
                    realized_pnl,
                    trade_value,
                    role,
                    trade_type,
                    timestamp,
                }
            },
        )
        .collect();

    Ok(Json(TradesResponse {
        trades,
        total,
        next_cursor,
    }))
}

// ============================================================================
// Email verification
// ============================================================================

use rand::Rng;

static EMAIL_TABLES_CREATED: AtomicBool = AtomicBool::new(false);

async fn ensure_email_verification_table(pool: &sqlx::PgPool) {
    if EMAIL_TABLES_CREATED.load(Ordering::Relaxed) {
        return;
    }
    let _ = sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS email_verifications (
            id              BIGSERIAL PRIMARY KEY,
            user_address    VARCHAR(42) NOT NULL,
            email           VARCHAR(255) NOT NULL,
            code            VARCHAR(6) NOT NULL,
            used            BOOLEAN NOT NULL DEFAULT false,
            expires_at      TIMESTAMPTZ NOT NULL,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        "#,
    )
    .execute(pool)
    .await;
    let _ = sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_email_verif_user ON email_verifications(user_address, created_at DESC)"
    )
    .execute(pool)
    .await;
    EMAIL_TABLES_CREATED.store(true, Ordering::Relaxed);
}

fn generate_verification_code() -> String {
    let code: u32 = rand::thread_rng().gen_range(0..1_000_000);
    format!("{:06}", code)
}

async fn send_verification_email(to: &str, code: &str) -> Result<(), String> {
    use lettre::message::header::ContentType;
    use lettre::transport::smtp::authentication::Credentials;
    use lettre::{Message, SmtpTransport, Transport};

    let gmail_user = std::env::var("GMAIL_USER").map_err(|_| "GMAIL_USER not set".to_string())?;
    let gmail_pass =
        std::env::var("GMAIL_PASSWORD").map_err(|_| "GMAIL_PASSWORD not set".to_string())?;

    let html_body = format!(
        r#"<div style="font-family: sans-serif; max-width: 480px; margin: 0 auto; padding: 24px;">
        <h2>Email Verification</h2>
        <p>Your verification code is:</p>
        <div style="font-size: 32px; font-weight: bold; letter-spacing: 8px; text-align: center; padding: 16px; background: #f4f4f4; border-radius: 8px;">
          {}
        </div>
        <p style="margin-top: 16px; color: #666;">This code expires in 15 minutes.</p>
        </div>"#,
        code
    );

    let email = Message::builder()
        .from(
            gmail_user
                .parse()
                .map_err(|e| format!("Invalid from: {}", e))?,
        )
        .to(to.parse().map_err(|e| format!("Invalid to: {}", e))?)
        .subject("AXBlade - Email Verification")
        .header(ContentType::TEXT_HTML)
        .body(html_body)
        .map_err(|e| format!("Failed to build email: {}", e))?;

    let creds = Credentials::new(gmail_user, gmail_pass);
    let mailer = SmtpTransport::relay("smtp.gmail.com")
        .map_err(|e| format!("SMTP relay error: {}", e))?
        .credentials(creds)
        .build();

    mailer
        .send(&email)
        .map_err(|e| format!("Send error: {}", e))?;
    Ok(())
}

#[derive(Debug, Deserialize)]
pub struct SendVerificationRequest {
    pub email: String,
}

#[derive(Debug, Serialize)]
pub struct SendVerificationResponse {
    pub success: bool,
    pub message: String,
}

/// Send email verification code
/// POST /account/send-verification
pub async fn send_verification(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Json(req): Json<SendVerificationRequest>,
) -> Result<Json<SendVerificationResponse>, (StatusCode, Json<ErrorResponse>)> {
    let user_address = auth_user.address.to_lowercase();
    let email = req.email.trim().to_lowercase();

    if !email.contains('@') || !email.contains('.') {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "无效的邮箱格式".to_string(),
                code: "INVALID_EMAIL".to_string(),
            }),
        ));
    }

    ensure_email_verification_table(&state.db.pool).await;

    // Rate limit: max 3 per hour
    let recent_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM email_verifications WHERE user_address = $1 AND created_at > NOW() - INTERVAL '1 hour'"
    )
    .bind(&user_address)
    .fetch_one(&state.db.pool)
    .await
    .unwrap_or(0);

    if recent_count >= 3 {
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            Json(ErrorResponse {
                error: "请求过于频繁，请稍后再试".to_string(),
                code: "RATE_LIMITED".to_string(),
            }),
        ));
    }

    let code = generate_verification_code();

    // Store in DB
    sqlx::query(
        "INSERT INTO email_verifications (user_address, email, code, expires_at) VALUES ($1, $2, $3, NOW() + INTERVAL '15 minutes')"
    )
    .bind(&user_address)
    .bind(&email)
    .bind(&code)
    .execute(&state.db.pool)
    .await
    .map_err(|e| {
        tracing::error!("Failed to store verification: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "系统错误".to_string(),
                code: "DB_ERROR".to_string(),
            }),
        )
    })?;

    // Send email (async but don't block response)
    match send_verification_email(&email, &code).await {
        Ok(_) => tracing::info!("Verification email sent to {} for {}", email, user_address),
        Err(e) => tracing::error!("Failed to send verification email: {}", e),
    }

    Ok(Json(SendVerificationResponse {
        success: true,
        message: "验证码已发送".to_string(),
    }))
}

#[derive(Debug, Deserialize)]
pub struct VerifyEmailRequest {
    pub email: String,
    pub code: String,
}

#[derive(Debug, Serialize)]
pub struct VerifyEmailResponse {
    pub success: bool,
    pub email: String,
}

/// Verify email with code
/// POST /account/verify-email
pub async fn verify_email(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Json(req): Json<VerifyEmailRequest>,
) -> Result<Json<VerifyEmailResponse>, (StatusCode, Json<ErrorResponse>)> {
    let user_address = auth_user.address.to_lowercase();
    let code = req.code.trim().to_string();
    let _email = req.email.trim().to_lowercase();

    ensure_email_verification_table(&state.db.pool).await;

    // Find valid verification
    let verification: Option<(i64, String)> = sqlx::query_as(
        r#"
        SELECT id, email FROM email_verifications
        WHERE user_address = $1 AND code = $2 AND used = false AND expires_at > NOW()
        ORDER BY created_at DESC
        LIMIT 1
        "#,
    )
    .bind(&user_address)
    .bind(&code)
    .fetch_optional(&state.db.pool)
    .await
    .map_err(|e| {
        tracing::error!("Failed to query verification: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "系统错误".to_string(),
                code: "DB_ERROR".to_string(),
            }),
        )
    })?;

    let (verif_id, verified_email) = match verification {
        Some(v) => v,
        None => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "验证码无效或已过期".to_string(),
                    code: "INVALID_CODE".to_string(),
                }),
            ));
        }
    };

    // Mark verification as used
    let _ = sqlx::query("UPDATE email_verifications SET used = true WHERE id = $1")
        .bind(verif_id)
        .execute(&state.db.pool)
        .await;

    // Update user email + verified status
    sqlx::query(
        "UPDATE users SET email = $1, email_verified = true, updated_at = NOW() WHERE address = $2",
    )
    .bind(&verified_email)
    .bind(&user_address)
    .execute(&state.db.pool)
    .await
    .map_err(|e| {
        tracing::error!("Failed to update user email: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "更新邮箱失败".to_string(),
                code: "UPDATE_FAILED".to_string(),
            }),
        )
    })?;

    tracing::info!("Email verified: {} for {}", verified_email, user_address);

    Ok(Json(VerifyEmailResponse {
        success: true,
        email: verified_email,
    }))
}

#[cfg(test)]
mod cursor_tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_cursor_roundtrip() {
        let ts = Utc.timestamp_millis_opt(1_714_000_000_000).unwrap();
        let id = Uuid::parse_str("225cf830-2044-4994-994e-5a51fb5f579b").unwrap();
        let s = build_cursor(ts, id);
        let (ts2, id2) = parse_cursor(&s).unwrap();
        assert_eq!(ts.timestamp_millis(), ts2.timestamp_millis());
        assert_eq!(id, id2);
    }

    #[test]
    fn test_cursor_rejects_garbage() {
        assert!(parse_cursor("").is_none());
        assert!(parse_cursor("no-underscore").is_none());
        assert!(parse_cursor("notanumber_225cf830-2044-4994-994e-5a51fb5f579b").is_none());
        // valid timestamp, bad uuid
        assert!(parse_cursor("1714000000000_not-a-uuid").is_none());
        // timestamp beyond i64 ms range would overflow parse::<i64> → None
        assert!(
            parse_cursor("99999999999999999999999_225cf830-2044-4994-994e-5a51fb5f579b").is_none()
        );
    }

    #[test]
    fn test_cursor_format_is_sortable_by_created_at() {
        // Cursors ordered lexicographically by the timestamp prefix give the
        // same DESC order as created_at DESC for cursors within the same
        // second-range. Not a hard correctness requirement (DB uses tuple
        // comparison, not cursor strings), but a sanity check that the
        // format isn't pathological.
        let id = Uuid::nil();
        let a = build_cursor(Utc.timestamp_millis_opt(1_000_000_000_000).unwrap(), id);
        let b = build_cursor(Utc.timestamp_millis_opt(2_000_000_000_000).unwrap(), id);
        // Both 13-digit millis — same length, so lexical cmp == numeric cmp.
        assert!(a < b);
    }
}
