//! Admin endpoints for spot trading.
//!
//! Mounted under /admin/* in api/routes/mod.rs alongside the perp admin
//! endpoints; the existing api_key_middleware on the admin_routes Router
//! provides auth (API key with admin permission).
//!
//! Endpoints:
//!   POST   /admin/spot/markets              create_market
//!   PATCH  /admin/spot/markets/:id          patch_market (fee/tick/lot/min_notional)
//!   PATCH  /admin/spot/markets/:id/status   patch_status (listed/halted/delisted)
//!   POST   /admin/spot/balances/credit      credit_balance (testnet only)
//!
//! Each market mutation pings the engine with `ReloadMarket` so the in-
//! memory MarketCache picks up the change without a restart. The delist
//! transition additionally drains the engine's open book via
//! `cancel_market` before flipping status, which unfreezes balances.

use axum::{extract::{Path, State}, http::StatusCode, Json};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::services::spot::matching::types::OrderCommand;
use crate::AppState;

#[derive(Serialize)]
pub struct ErrorBody { pub error: String }

fn err(code: StatusCode, msg: impl Into<String>) -> (StatusCode, Json<ErrorBody>) {
    (code, Json(ErrorBody { error: msg.into() }))
}

// ---------- POST /admin/spot/markets ----------

#[derive(Deserialize)]
pub struct CreateMarketBody {
    pub id: String,
    pub base_token: String,
    pub quote_token: String,
    pub tick_size: Decimal,
    pub lot_size: Decimal,
    pub min_notional: Decimal,
    pub maker_fee_bps: i32,
    pub taker_fee_bps: i32,
}

pub async fn create_market(
    State(state): State<Arc<AppState>>,
    Json(b): Json<CreateMarketBody>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorBody>)> {
    sqlx::query(
        "INSERT INTO spot_markets
            (id, base_token, quote_token, tick_size, lot_size, min_notional,
             maker_fee_bps, taker_fee_bps, status)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'listed')"
    )
    .bind(&b.id).bind(&b.base_token).bind(&b.quote_token)
    .bind(b.tick_size).bind(b.lot_size).bind(b.min_notional)
    .bind(b.maker_fee_bps).bind(b.taker_fee_bps)
    .execute(&state.db.pool).await
    .map_err(|e| match e {
        sqlx::Error::Database(db) if db.code().as_deref() == Some("23505") =>
            err(StatusCode::CONFLICT, "MARKET_EXISTS"),
        _ => err(StatusCode::INTERNAL_SERVER_ERROR, "DB_ERROR"),
    })?;
    if let Some(eng) = &state.spot_engine {
        let _ = eng.cmd_tx.try_send(OrderCommand::ReloadMarket { market_id: b.id.clone() });
    }
    Ok(Json(serde_json::json!({ "ok": true, "id": b.id })))
}

// ---------- PATCH /admin/spot/markets/:id ----------

#[derive(Deserialize)]
pub struct PatchMarketBody {
    pub tick_size: Option<Decimal>,
    pub lot_size: Option<Decimal>,
    pub min_notional: Option<Decimal>,
    pub maker_fee_bps: Option<i32>,
    pub taker_fee_bps: Option<i32>,
    /// Curated label rendered on the market card (e.g. "Diffie / Tether").
    pub display_name: Option<String>,
    /// Free-form description for the market detail page.
    pub description: Option<String>,
}

pub async fn patch_market(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(b): Json<PatchMarketBody>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorBody>)> {
    sqlx::query(
        "UPDATE spot_markets SET
            tick_size     = COALESCE($1, tick_size),
            lot_size      = COALESCE($2, lot_size),
            min_notional  = COALESCE($3, min_notional),
            maker_fee_bps = COALESCE($4, maker_fee_bps),
            taker_fee_bps = COALESCE($5, taker_fee_bps),
            display_name  = COALESCE($6, display_name),
            description   = COALESCE($7, description),
            updated_at    = NOW()
          WHERE id = $8"
    )
    .bind(b.tick_size).bind(b.lot_size).bind(b.min_notional)
    .bind(b.maker_fee_bps).bind(b.taker_fee_bps)
    .bind(b.display_name).bind(b.description).bind(&id)
    .execute(&state.db.pool).await
    .map_err(|_| err(StatusCode::INTERNAL_SERVER_ERROR, "DB_ERROR"))?;
    if let Some(eng) = &state.spot_engine {
        let _ = eng.cmd_tx.try_send(OrderCommand::ReloadMarket { market_id: id.clone() });
    }
    Ok(Json(serde_json::json!({ "ok": true, "id": id })))
}

// ---------- PATCH /admin/spot/markets/:id/status ----------

#[derive(Deserialize)]
pub struct StatusBody { pub status: String }

pub async fn patch_status(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(b): Json<StatusBody>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorBody>)> {
    if !["listed", "halted", "delisted"].contains(&b.status.as_str()) {
        return Err(err(StatusCode::BAD_REQUEST, "INVALID_STATUS"));
    }
    // Drain the book BEFORE flipping status to delisted so balances unfreeze
    // through the normal cancel path. After delist, place_order rejects with
    // MARKET_DELISTED so no new resters can appear.
    if b.status == "delisted" {
        if let Some(eng) = &state.spot_engine {
            let _ = eng.cancel_market(id.clone()).await;
        }
    }
    sqlx::query("UPDATE spot_markets SET status=$1, updated_at=NOW() WHERE id=$2")
        .bind(&b.status).bind(&id)
        .execute(&state.db.pool).await
        .map_err(|_| err(StatusCode::INTERNAL_SERVER_ERROR, "DB_ERROR"))?;
    if let Some(eng) = &state.spot_engine {
        let _ = eng.cmd_tx.try_send(OrderCommand::ReloadMarket { market_id: id.clone() });
    }
    Ok(Json(serde_json::json!({ "ok": true, "id": id, "status": b.status })))
}

// ---------- POST /admin/spot/balances/credit (testnet only) ----------

#[derive(Deserialize)]
pub struct CreditBody {
    pub user_address: String,
    pub token: String,
    pub amount: Decimal,
    pub reason: Option<String>,
}

pub async fn credit_balance(
    State(state): State<Arc<AppState>>,
    Json(b): Json<CreditBody>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorBody>)> {
    // Hard gate: only available when TESTNET_ONLY=true. On mainnet the
    // route still mounts but answers 404 to keep the surface uniform.
    if std::env::var("TESTNET_ONLY").as_deref() != Ok("true") {
        return Err(err(StatusCode::NOT_FOUND, "DISABLED"));
    }
    if b.amount <= Decimal::ZERO {
        return Err(err(StatusCode::BAD_REQUEST, "AMOUNT_NON_POSITIVE"));
    }
    let user_lc = b.user_address.to_lowercase();
    let mut tx = state.db.pool.begin().await
        .map_err(|_| err(StatusCode::INTERNAL_SERVER_ERROR, "DB_ERROR"))?;
    sqlx::query(
        "INSERT INTO spot_balances (user_address, token, available, frozen)
         VALUES ($1, $2, $3, 0)
         ON CONFLICT (user_address, token)
         DO UPDATE SET available  = spot_balances.available + EXCLUDED.available,
                       updated_at = NOW()"
    )
    .bind(&user_lc).bind(&b.token).bind(b.amount)
    .execute(&mut *tx).await
    .map_err(|_| err(StatusCode::INTERNAL_SERVER_ERROR, "DB_ERROR"))?;
    sqlx::query(
        "INSERT INTO spot_admin_credits (user_address, token, amount, admin_actor, reason)
         VALUES ($1, $2, $3, $4, $5)"
    )
    // admin_actor: api_key_middleware doesn't insert AuthUser; record the
    // generic actor since admin auth is a single shared API key, not per-user.
    .bind(&user_lc).bind(&b.token).bind(b.amount).bind("admin_api_key").bind(&b.reason)
    .execute(&mut *tx).await
    .map_err(|_| err(StatusCode::INTERNAL_SERVER_ERROR, "DB_ERROR"))?;
    tx.commit().await
        .map_err(|_| err(StatusCode::INTERNAL_SERVER_ERROR, "DB_ERROR"))?;
    // Push to WS subscribers so the UI updates immediately.
    crate::services::spot::ws_publisher::push_balance_now(&state, &user_lc, &b.token).await;
    Ok(Json(serde_json::json!({ "ok": true })))
}
