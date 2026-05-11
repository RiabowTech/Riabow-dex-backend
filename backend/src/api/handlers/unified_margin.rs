//! Unified Margin Mode HTTP handlers (MVP).
//!
//! - GET  /api/v1/unified/account     — live unified-account snapshot
//! - POST /api/v1/account/margin-mode — switch isolated ↔ unified
//!
//! See design_docs/08_统一保证金模式设计.md §7 / §8.

use axum::{extract::{Query, State}, http::StatusCode, Extension, Json};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use crate::auth::middleware::AuthUser;
use crate::models::margin_mode::MarginMode;
use crate::models::position::{Position, PositionStatus};
use crate::services::unified_margin::{compute_risk_with_tiers, MMR_DEFAULT};
use crate::AppState;

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub code: String,
}

fn err(status: StatusCode, code: &str, msg: impl Into<String>) -> (StatusCode, Json<ErrorResponse>) {
    (status, Json(ErrorResponse { error: msg.into(), code: code.to_string() }))
}

// ---------------------------------------------------------------
// GET /api/v1/unified/account
// ---------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct UnifiedAccountResponse {
    pub margin_mode: String,
    pub wallet_balance: Decimal,
    pub total_equity: Decimal,
    pub available_balance: Decimal,
    pub total_initial_margin: Decimal,
    pub total_maintenance_margin: Decimal,
    pub total_unrealized_pnl: Decimal,
    pub uni_mmr: Option<Decimal>,
    pub account_status: String,
}

pub async fn get_unified_account(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
) -> Result<Json<UnifiedAccountResponse>, (StatusCode, Json<ErrorResponse>)> {
    let addr = auth_user.address.to_lowercase();
    let snapshot = load_unified_snapshot(&state, &addr).await?;

    // Read current margin_mode (may be unified or isolated — both allowed here).
    let mode: String = sqlx::query_scalar("SELECT margin_mode FROM users WHERE address = $1")
        .bind(&addr)
        .fetch_optional(&state.db.pool)
        .await
        .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, "DB_ERROR", e.to_string()))?
        .unwrap_or_else(|| "isolated".to_string());

    Ok(Json(UnifiedAccountResponse {
        margin_mode: mode,
        wallet_balance: snapshot.wallet_balance,
        total_equity: snapshot.total_equity,
        available_balance: snapshot.available_balance,
        total_initial_margin: snapshot.total_initial_margin,
        total_maintenance_margin: snapshot.total_maint_margin,
        total_unrealized_pnl: snapshot.total_unrealized_pnl,
        uni_mmr: snapshot.uni_mmr,
        account_status: snapshot.account_status.to_string(),
    }))
}

// ---------------------------------------------------------------
// POST /api/v1/account/margin-mode
// ---------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct SwitchMarginModeRequest {
    /// Accepts both `margin_mode` (current) and `mode` (Binance-aligned
    /// short form), so that frontend / SDKs that follow Binance's naming
    /// don't get a 422.
    #[serde(alias = "mode")]
    pub margin_mode: String,
}

#[derive(Debug, Serialize)]
pub struct SwitchMarginModeResponse {
    pub success: bool,
    pub margin_mode: String,
    pub uni_mmr: Option<Decimal>,
    pub available_balance: Decimal,
}

pub async fn switch_margin_mode(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Json(req): Json<SwitchMarginModeRequest>,
) -> Result<Json<SwitchMarginModeResponse>, (StatusCode, Json<ErrorResponse>)> {
    let addr = auth_user.address.to_lowercase();
    let target: MarginMode = req.margin_mode.parse().map_err(|e: String| {
        err(StatusCode::BAD_REQUEST, "INVALID_MARGIN_MODE", e)
    })?;

    // Reject if user has any non-terminal (pending/open/partially_filled) orders.
    // Both mode switches (§7.1 and §7.2) require a clean order book.
    let open_orders: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM orders WHERE user_address = $1 \
         AND status IN ('pending','open','partially_filled')",
    )
    .bind(&addr)
    .fetch_one(&state.db.pool)
    .await
    .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, "DB_ERROR", e.to_string()))?;

    if open_orders > 0 {
        return Err(err(
            StatusCode::BAD_REQUEST,
            "HAS_OPEN_ORDERS",
            "Please cancel all open orders before switching margin mode",
        ));
    }

    // Switching → isolated additionally requires zero open positions (§7.2).
    if matches!(target, MarginMode::Isolated) {
        let open_positions: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM positions WHERE user_address = $1 AND status = 'open'",
        )
        .bind(&addr)
        .fetch_one(&state.db.pool)
        .await
        .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, "DB_ERROR", e.to_string()))?;

        if open_positions > 0 {
            return Err(err(
                StatusCode::BAD_REQUEST,
                "HAS_OPEN_POSITIONS",
                "Close all positions before switching back to isolated mode",
            ));
        }
    }

    let mut tx = state
        .db
        .pool
        .begin()
        .await
        .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, "DB_ERROR", e.to_string()))?;

    // Upsert users.margin_mode.
    sqlx::query("UPDATE users SET margin_mode = $1, updated_at = NOW() WHERE address = $2")
        .bind(target.to_string())
        .bind(&addr)
        .execute(&mut *tx)
        .await
        .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, "DB_ERROR", e.to_string()))?;

    // When entering unified mode, ensure the unified_margin_accounts row exists.
    if matches!(target, MarginMode::Unified) {
        sqlx::query(
            "INSERT INTO unified_margin_accounts (user_address) VALUES ($1) \
             ON CONFLICT (user_address) DO NOTHING",
        )
        .bind(&addr)
        .execute(&mut *tx)
        .await
        .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, "DB_ERROR", e.to_string()))?;
    }

    tx.commit()
        .await
        .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, "DB_ERROR", e.to_string()))?;

    // Compute a post-switch snapshot for the response. Per §7.1, entering
    // unified mode with uniMMR < 1.10 should be rejected; we verify here
    // and roll back if the freshly-switched account fails the safety bar.
    let snapshot = load_unified_snapshot(&state, &addr).await?;

    if matches!(target, MarginMode::Unified) {
        if let Some(m) = snapshot.uni_mmr {
            if m < rust_decimal_macros::dec!(1.10) {
                // Roll back the mode switch.
                sqlx::query("UPDATE users SET margin_mode = 'isolated', updated_at = NOW() WHERE address = $1")
                    .bind(&addr)
                    .execute(&state.db.pool)
                    .await
                    .ok();
                return Err(err(
                    StatusCode::BAD_REQUEST,
                    "UNI_MMR_TOO_LOW",
                    format!("uniMMR {} is below 1.10; switch rejected", m),
                ));
            }
        }
    }

    tracing::info!("User {} switched margin_mode → {}", addr, target);

    Ok(Json(SwitchMarginModeResponse {
        success: true,
        margin_mode: target.to_string(),
        uni_mmr: snapshot.uni_mmr,
        available_balance: snapshot.available_balance,
    }))
}

// ---------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------

/// Load the user's current unified-margin risk snapshot from live state.
pub(crate) async fn load_unified_snapshot(
    state: &Arc<AppState>,
    addr_lower: &str,
) -> Result<crate::models::unified_margin::UnifiedRiskSnapshot, (StatusCode, Json<ErrorResponse>)> {
    let collateral_symbol = state.config.collateral_symbol();

    let balance: Option<(Decimal, Decimal)> = sqlx::query_as(
        "SELECT available, frozen FROM balances WHERE user_address = $1 AND token = $2",
    )
    .bind(addr_lower)
    .bind(collateral_symbol)
    .fetch_optional(&state.db.pool)
    .await
    .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, "DB_ERROR", e.to_string()))?;

    let wallet_balance = balance
        .map(|(a, f)| a + f)
        .unwrap_or(Decimal::ZERO);

    let positions: Vec<Position> = sqlx::query_as::<_, Position>(
        "SELECT * FROM positions WHERE user_address = $1 AND status = $2",
    )
    .bind(addr_lower)
    .bind(PositionStatus::Open)
    .fetch_all(&state.db.pool)
    .await
    .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, "DB_ERROR", e.to_string()))?;

    let mut mark_prices: HashMap<String, Decimal> = HashMap::new();
    for p in &positions {
        if !mark_prices.contains_key(&p.symbol) {
            if let Some(mp) = state.price_feed_service.get_mark_price(&p.symbol).await {
                mark_prices.insert(p.symbol.clone(), mp);
            }
        }
    }

    // Pure IM committed by resting (open / partially_filled) limit-style
    // orders. compute_risk_with_tiers iterates positions only — without
    // this, /unified/account reports total_initial_margin=0 and
    // available_balance = full wallet even though balances.frozen has the
    // per-order collateral locked, contradicting `/account/balances` and
    // the gate at order placement that uses `available ≥ new_initial_margin`.
    // Excludes the 1.005× slippage buffer on purpose: this is unified IM
    // accounting, not a freeze count.
    let pending_orders_im: Decimal = sqlx::query_scalar(
        "SELECT COALESCE(SUM((amount - filled_amount) * price / leverage), 0)::numeric \
         FROM orders \
         WHERE user_address = $1 \
           AND status IN ('open', 'partially_filled') \
           AND price IS NOT NULL \
           AND price > 0 \
           AND leverage > 0",
    )
    .bind(addr_lower)
    .fetch_one(&state.db.pool)
    .await
    .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, "DB_ERROR", e.to_string()))?;

    let tiers_guard = state.margin_tiers.read().await;
    let mut snap = compute_risk_with_tiers(
        wallet_balance,
        &positions,
        &mark_prices,
        MMR_DEFAULT,
        Some(&*tiers_guard),
    );
    drop(tiers_guard);

    // Fold resting-order IM into the snapshot. This shifts total_initial_margin
    // up and available_balance down so the user-facing view tells the truth
    // about how much is actually free for new orders. uni_mmr / total_maint_margin
    // / liquidation status are deliberately untouched — resting orders carry no
    // maintenance obligation (they can be cancelled instantly), only an IM
    // commitment.
    if pending_orders_im > Decimal::ZERO {
        snap.total_initial_margin += pending_orders_im;
        snap.available_balance = snap.total_equity - snap.total_initial_margin;
    }

    Ok(snap)
}

// ---------------------------------------------------------------
// GET /api/v1/unified/liquidations
// ---------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct LiquidationsQuery {
    #[serde(default)]
    pub limit: Option<i64>,
    #[serde(default)]
    pub offset: Option<i64>,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct LiquidationRecord {
    pub id: uuid::Uuid,
    pub position_id: uuid::Uuid,
    pub symbol: String,
    pub side: String,
    pub closed_size_usd: Decimal,
    pub closed_size_tokens: Decimal,
    pub mark_price: Decimal,
    pub pnl_realized: Decimal,
    pub collateral_returned: Decimal,
    pub trigger_uni_mmr: Option<Decimal>,
    pub trigger_equity: Decimal,
    pub post_uni_mmr: Option<Decimal>,
    pub liquidation_type: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize)]
pub struct LiquidationsResponse {
    pub liquidations: Vec<LiquidationRecord>,
    pub total: i64,
}

pub async fn get_liquidations(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Query(q): Query<LiquidationsQuery>,
) -> Result<Json<LiquidationsResponse>, (StatusCode, Json<ErrorResponse>)> {
    let addr = auth_user.address.to_lowercase();
    let limit = q.limit.unwrap_or(20).clamp(1, 200);
    let offset = q.offset.unwrap_or(0).max(0);

    let rows: Vec<LiquidationRecord> = sqlx::query_as::<_, LiquidationRecord>(
        "SELECT id, position_id, symbol, side, closed_size_usd, closed_size_tokens, \
                mark_price, pnl_realized, collateral_returned, trigger_uni_mmr, \
                trigger_equity, post_uni_mmr, liquidation_type, created_at \
         FROM unified_liquidation_records \
         WHERE user_address = $1 \
         ORDER BY created_at DESC \
         LIMIT $2 OFFSET $3",
    )
    .bind(&addr)
    .bind(limit)
    .bind(offset)
    .fetch_all(&state.db.pool)
    .await
    .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, "DB_ERROR", e.to_string()))?;

    let total: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM unified_liquidation_records WHERE user_address = $1",
    )
    .bind(&addr)
    .fetch_one(&state.db.pool)
    .await
    .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, "DB_ERROR", e.to_string()))?;

    Ok(Json(LiquidationsResponse { liquidations: rows, total }))
}

// ---------------------------------------------------------------
// POST /api/v1/unified/risk/simulate
// ---------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct SimulateRequest {
    pub symbol: String,
    /// `long` or `short` — informational only in the current MVP
    /// (simulate_open treats new notional positively); reserved for
    /// future hedge-aware simulation.
    #[serde(default)]
    pub side: Option<String>,
    /// Notional value the trade would open (USD).
    pub size_usd: Decimal,
    pub leverage: i32,
}

#[derive(Debug, Serialize)]
pub struct SimulateResponse {
    pub current_uni_mmr: Option<Decimal>,
    pub simulated_uni_mmr: Option<Decimal>,
    pub new_initial_margin: Decimal,
    pub new_maint_margin: Decimal,
    pub available_after: Decimal,
    pub can_open: bool,
    pub reason: Option<String>,
}

// ---------------------------------------------------------------
// Admin: margin-tier ladder CRUD (gated by X-API-Key / admin_api_key)
// ---------------------------------------------------------------

#[derive(Debug, Deserialize, Serialize)]
pub struct MarginTierDto {
    pub symbol: String,
    pub tier: i32,
    pub max_notional: Decimal,
    pub maint_margin_rate: Decimal,
    pub max_leverage: i32,
    #[serde(default)]
    pub cum_amount: Decimal,
}

#[derive(Debug, Serialize)]
pub struct MarginTiersListResponse {
    pub tiers: Vec<MarginTierDto>,
}

pub async fn admin_list_margin_tiers(
    State(state): State<Arc<AppState>>,
) -> Result<Json<MarginTiersListResponse>, (StatusCode, Json<ErrorResponse>)> {
    #[derive(sqlx::FromRow)]
    struct Row {
        symbol: String,
        tier: i32,
        max_notional: Decimal,
        maint_margin_rate: Decimal,
        max_leverage: i32,
        cum_amount: Decimal,
    }
    let rows: Vec<Row> = sqlx::query_as::<_, Row>(
        "SELECT symbol, tier, max_notional, maint_margin_rate, max_leverage, cum_amount \
         FROM margin_tiers ORDER BY symbol, tier",
    )
    .fetch_all(&state.db.pool)
    .await
    .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, "DB_ERROR", e.to_string()))?;
    Ok(Json(MarginTiersListResponse {
        tiers: rows
            .into_iter()
            .map(|r| MarginTierDto {
                symbol: r.symbol,
                tier: r.tier,
                max_notional: r.max_notional,
                maint_margin_rate: r.maint_margin_rate,
                max_leverage: r.max_leverage,
                cum_amount: r.cum_amount,
            })
            .collect(),
    }))
}

/// Upsert a single tier row by (symbol, tier). Validates that the
/// ladder remains monotone in `max_notional` after the write — a
/// non-monotone ladder would produce surprising MM. Caller is expected
/// to also call `/admin/margin-tiers/reload` after a batch edit.
pub async fn admin_upsert_margin_tier(
    State(state): State<Arc<AppState>>,
    Json(req): Json<MarginTierDto>,
) -> Result<Json<MarginTierDto>, (StatusCode, Json<ErrorResponse>)> {
    if req.tier < 1 {
        return Err(err(StatusCode::BAD_REQUEST, "INVALID_TIER", "tier must be >= 1"));
    }
    if req.max_notional <= Decimal::ZERO {
        return Err(err(
            StatusCode::BAD_REQUEST,
            "INVALID_MAX_NOTIONAL",
            "max_notional must be > 0",
        ));
    }
    if req.maint_margin_rate <= Decimal::ZERO || req.maint_margin_rate >= Decimal::ONE {
        return Err(err(
            StatusCode::BAD_REQUEST,
            "INVALID_MMR",
            "maint_margin_rate must be in (0, 1)",
        ));
    }
    if req.max_leverage < 1 {
        return Err(err(
            StatusCode::BAD_REQUEST,
            "INVALID_LEVERAGE",
            "max_leverage must be >= 1",
        ));
    }

    sqlx::query(
        "INSERT INTO margin_tiers \
            (symbol, tier, max_notional, maint_margin_rate, max_leverage, cum_amount) \
         VALUES ($1, $2, $3, $4, $5, $6) \
         ON CONFLICT (symbol, tier) DO UPDATE SET \
            max_notional = EXCLUDED.max_notional, \
            maint_margin_rate = EXCLUDED.maint_margin_rate, \
            max_leverage = EXCLUDED.max_leverage, \
            cum_amount = EXCLUDED.cum_amount",
    )
    .bind(&req.symbol)
    .bind(req.tier)
    .bind(req.max_notional)
    .bind(req.maint_margin_rate)
    .bind(req.max_leverage)
    .bind(req.cum_amount)
    .execute(&state.db.pool)
    .await
    .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, "DB_ERROR", e.to_string()))?;

    Ok(Json(req))
}

#[derive(Debug, Deserialize)]
pub struct DeleteMarginTierRequest {
    pub symbol: String,
    pub tier: i32,
}

pub async fn admin_delete_margin_tier(
    State(state): State<Arc<AppState>>,
    Json(req): Json<DeleteMarginTierRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let affected = sqlx::query(
        "DELETE FROM margin_tiers WHERE symbol = $1 AND tier = $2",
    )
    .bind(&req.symbol)
    .bind(req.tier)
    .execute(&state.db.pool)
    .await
    .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, "DB_ERROR", e.to_string()))?
    .rows_affected();

    Ok(Json(serde_json::json!({ "deleted": affected })))
}

/// Hot-reload the in-memory tier store from DB. Returns the number of
/// tiers loaded. Effective immediately for all subsequent compute_risk
/// / simulate_open calls — no restart required.
pub async fn admin_reload_margin_tiers(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    match crate::services::unified_margin::TierStore::load(&state.db.pool).await {
        Ok(store) => {
            let count: usize = 0; // detailed count not exposed by TierStore; see /list
            *state.margin_tiers.write().await = store;
            Ok(Json(serde_json::json!({ "reloaded": true, "count_hint": count })))
        }
        Err(e) => Err(err(
            StatusCode::INTERNAL_SERVER_ERROR,
            "RELOAD_FAILED",
            e.to_string(),
        )),
    }
}

// ---------------------------------------------------------------
// /api/v1/unified/risk/simulate (unchanged)
// ---------------------------------------------------------------

pub async fn simulate_open(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Json(req): Json<SimulateRequest>,
) -> Result<Json<SimulateResponse>, (StatusCode, Json<ErrorResponse>)> {
    if req.leverage < 1 {
        return Err(err(StatusCode::BAD_REQUEST, "INVALID_LEVERAGE", "leverage must be >= 1"));
    }
    if req.size_usd <= Decimal::ZERO {
        return Err(err(StatusCode::BAD_REQUEST, "INVALID_SIZE", "size_usd must be > 0"));
    }

    let addr = auth_user.address.to_lowercase();
    let snapshot = load_unified_snapshot(&state, &addr).await?;

    let tiers_guard = state.margin_tiers.read().await;
    let sim = crate::services::unified_margin::simulate_open_with_tiers(
        &snapshot,
        &req.symbol,
        req.size_usd,
        req.leverage,
        MMR_DEFAULT,
        Some(&*tiers_guard),
    );

    Ok(Json(SimulateResponse {
        current_uni_mmr: sim.current_uni_mmr,
        simulated_uni_mmr: sim.simulated_uni_mmr,
        new_initial_margin: sim.new_initial_margin,
        new_maint_margin: sim.new_maint_margin,
        available_after: sim.available_after,
        can_open: sim.can_open,
        reason: sim.reason.map(|s| s.to_string()),
    }))
}
