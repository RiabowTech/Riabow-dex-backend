use axum::{extract::{Path, Query, State}, http::StatusCode, Extension, Json};
use ethers::types::Address;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;

use crate::auth::middleware::AuthUser;
use crate::models::spot::SpotWithdrawal;
use crate::services::spot::withdraw_signer::SpotWithdrawSigner;
use crate::AppState;

#[derive(Deserialize)]
pub struct WithdrawRequest {
    pub token: String,             // MVP: "DF"
    pub amount: Decimal,
}

#[derive(Serialize)]
pub struct WithdrawResponse {
    pub id: String,
    pub nonce: i64,
    pub signature: String,
    pub deadline: i64,
    pub vault_address: String,
    pub chain_id: i64,
    /// Human-readable decimal amount (e.g. "5", "12.5"). Matches the
    /// list/get views and the FE's legacy `parseUnits(amount, decimals)`
    /// fallback.
    pub amount: String,
    /// Wei-scaled amount that was placed in the EIP-712 `value` field and
    /// which the FE feeds into `vault.withdraw`. Preferred over `amount`
    /// because it removes any decimal/precision ambiguity.
    pub amount_in_wei: String,
}

#[cfg(test)]
mod response_shape_tests {
    use super::*;
    use serde_json::json;

    /// Locks in the FE↔BE contract: spot WithdrawalView reads
    ///   amount_in_wei  → BigInt(amount_in_wei)
    /// and falls back to
    ///   parseUnits(amount, decimals)
    /// We must therefore serialize both fields, with `amount` as a decimal
    /// string and `amount_in_wei` as a base-units string.
    #[test]
    fn response_serializes_both_amount_and_amount_in_wei() {
        let resp = WithdrawResponse {
            id: "00000000-0000-0000-0000-000000000000".into(),
            nonce: 0,
            signature: "0xdeadbeef".into(),
            deadline: 1700000000,
            vault_address: "0x4Fe0b354c5865ee9deb979a99030d757ae47664a".into(),
            chain_id: 97,
            amount: "5".into(),
            amount_in_wei: "5000000000000000000".into(),
        };
        let v = serde_json::to_value(&resp).unwrap();
        assert_eq!(v["amount"], json!("5"));
        assert_eq!(v["amount_in_wei"], json!("5000000000000000000"));
    }

    /// The FE's history list (TradingFundingHistoryItem) reads:
    ///   item.created_at, item.expiry, item.backend_signature
    /// without any field-name fallback. The view must serialize those
    /// keys verbatim (we keep the Rust field names matching the DB column
    /// names via #[serde(rename = ...)] aliases). amount_in_wei is also
    /// surfaced so FE can submit a stuck `signed` row from history.
    #[test]
    fn withdrawal_view_serializes_with_fe_field_names() {
        let view = WithdrawalView {
            id: "00000000-0000-0000-0000-000000000000".into(),
            token: "DF".into(),
            amount: "4".into(),
            amount_in_wei: "4000000000000000000".into(),
            fee: "0".into(),
            chain_id: 97,
            nonce: 0,
            status: "signed".into(),
            backend_signature: Some("0xabc".into()),
            deadline: 1778511461,
            tx_hash: None,
            block_number: None,
            requested_at: 1778425061,
            confirmed_at: None,
        };
        let v = serde_json::to_value(&view).unwrap();
        assert_eq!(v["created_at"], json!(1778425061), "must rename requested_at -> created_at");
        assert_eq!(v["expiry"], json!(1778511461), "must rename deadline -> expiry");
        assert_eq!(v["backend_signature"], json!("0xabc"), "must surface signature");
        assert_eq!(v["amount_in_wei"], json!("4000000000000000000"));
        assert_eq!(v["amount"], json!("4"));
    }
}

#[derive(Serialize)]
pub struct ErrorResponse { pub error: String }

#[derive(Serialize)]
pub struct WithdrawalView {
    pub id: String,
    pub token: String,
    /// Decimal amount string. Mirrors WithdrawResponse.amount.
    pub amount: String,
    /// Wei-scaled amount that was placed in the EIP-712 `value` field. The
    /// FE re-uses this when submitting a stuck `signed` row from history,
    /// so we must surface it on the list/get views, not just on the
    /// signing response.
    pub amount_in_wei: String,
    pub fee: String,
    pub chain_id: i64,
    pub nonce: i64,
    pub status: String,
    /// 0x-prefixed 65-byte signature. Empty string serializes as
    /// `backend_signature: null` to keep the FE's `!withdrawal.backend_signature`
    /// guard sensible — the FE history map reads this name directly with no
    /// fallback to `signature`.
    #[serde(rename = "backend_signature")]
    pub backend_signature: Option<String>,
    /// Unix-seconds. Renamed for the FE's WithdrawRecord type.
    #[serde(rename = "expiry")]
    pub deadline: i64,
    pub tx_hash: Option<String>,
    pub block_number: Option<i64>,
    /// Unix-seconds. Renamed for the FE's WithdrawRecord type.
    #[serde(rename = "created_at")]
    pub requested_at: i64,
    pub confirmed_at: Option<i64>,
}

#[derive(Deserialize)]
pub struct ListQuery {
    pub status: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: i64,
}
fn default_limit() -> i64 { 50 }

pub async fn request_withdraw(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Json(req): Json<WithdrawRequest>,
) -> Result<Json<WithdrawResponse>, (StatusCode, Json<ErrorResponse>)> {
    if auth_user.is_api_key {
        return Err((StatusCode::FORBIDDEN, Json(ErrorResponse { error: "API Key permission denied: withdraws not allowed".into() })));
    }

    let cfg = state.config.spot.as_ref()
        .ok_or((StatusCode::SERVICE_UNAVAILABLE, Json(ErrorResponse { error: "spot subsystem disabled".into() })))?;

    if req.token != "DF" {
        return Err((StatusCode::BAD_REQUEST, Json(ErrorResponse { error: format!("unsupported token: {}", req.token) })));
    }
    if req.amount < cfg.withdraw_min_amount_df {
        return Err((StatusCode::BAD_REQUEST, Json(ErrorResponse {
            error: format!("amount below minimum {}", cfg.withdraw_min_amount_df)
        })));
    }

    let user = auth_user.address.to_lowercase();
    let user_addr = Address::from_str(&user)
        .map_err(|_| (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: "invalid user address".into() })))?;

    // The vault validates against its own `releaseNonces[user]` counter, so
    // sign with that — not `MAX(db.nonce)+1`. Fetch before opening the tx so
    // a slow RPC doesn't pin a row lock.
    let bc = state.spot_blockchain.as_ref()
        .ok_or((StatusCode::SERVICE_UNAVAILABLE, Json(ErrorResponse { error: "spot blockchain not initialized".into() })))?;
    let on_chain_nonce: i64 = bc.release_nonce(user_addr).await
        .map_err(|e| { tracing::error!("spot withdraw read on-chain nonce: {e:?}"); (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: "chain query failed".into() })) })?
        as i64;

    let mut tx = state.db.pool.begin().await
        .map_err(|e| { tracing::error!("spot withdraw begin tx: {e}"); (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: "internal".into() })) })?;

    let available: Decimal = sqlx::query_scalar(
        "SELECT available FROM spot_balances WHERE user_address=$1 AND token='DF' FOR UPDATE"
    )
    .bind(&user)
    .fetch_optional(&mut *tx).await
    .map_err(|e| { tracing::error!("spot withdraw read balance: {e}"); (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: "internal".into() })) })?
    .unwrap_or(Decimal::ZERO);

    if available < req.amount {
        return Err((StatusCode::BAD_REQUEST, Json(ErrorResponse { error: "insufficient balance".into() })));
    }

    // Pending-withdraw guard: while the user has any `signed` row at or after
    // the contract's current nonce, every new signature would race with those
    // for the same on-chain slot. Force redeem (or wait for the reaper) before
    // issuing a new one.
    let pending: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM spot_withdrawals
          WHERE user_address=$1 AND chain_id=$2 AND status='signed' AND nonce >= $3"
    )
    .bind(&user).bind(cfg.bsc_chain_id as i64).bind(on_chain_nonce)
    .fetch_one(&mut *tx).await
    .map_err(|e| { tracing::error!("spot withdraw pending count: {e}"); (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: "internal".into() })) })?;
    if pending > 0 {
        return Err((StatusCode::CONFLICT, Json(ErrorResponse {
            error: "you have a pending withdrawal — submit it on-chain or wait for it to expire before signing a new one".into()
        })));
    }

    // Compose with confirmed-row history so we never reuse a nonce that has
    // already shown up in `confirmed`/`broadcast` rows (defence against a
    // brief re-org window where on-chain nonce regressed).
    let db_max_nonce: Option<i64> = sqlx::query_scalar(
        "SELECT MAX(nonce) FROM spot_withdrawals
          WHERE user_address=$1 AND chain_id=$2 AND status IN ('confirmed','broadcast')"
    )
    .bind(&user).bind(cfg.bsc_chain_id as i64)
    .fetch_one(&mut *tx).await
    .map_err(|e| { tracing::error!("spot withdraw db_max_nonce: {e}"); (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: "internal".into() })) })?;
    let next_nonce = std::cmp::max(on_chain_nonce, db_max_nonce.map(|n| n + 1).unwrap_or(0));

    let signer = SpotWithdrawSigner::from_config(cfg)
        .map_err(|e| { tracing::error!("spot withdraw signer init: {e}"); (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: "signer unavailable".into() })) })?;
    let signed = signer.sign(user_addr, req.amount, next_nonce, cfg.withdraw_nonce_ttl_secs).await
        .map_err(|e| { tracing::error!("spot withdraw sign: {e}"); (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: "sign failed".into() })) })?;

    // Reuse an `expired` slot at the same nonce if one exists (the reaper
    // turns stale `signed` rows into `expired`; without this the unique
    // (user_address, chain_id, nonce) constraint blocks every retry that
    // lands on the same on-chain slot). The WHERE clause on the UPDATE
    // branch only re-arms expired rows, never overwrites confirmed/signed.
    let row: Option<(uuid::Uuid,)> = sqlx::query_as(
        "INSERT INTO spot_withdrawals (user_address, token, amount, fee, chain_id, nonce,
                                       signature, deadline, status)
         VALUES ($1, 'DF', $2, 0, $3, $4, $5, $6, 'signed')
         ON CONFLICT (user_address, chain_id, nonce) DO UPDATE SET
            amount       = EXCLUDED.amount,
            fee          = EXCLUDED.fee,
            signature    = EXCLUDED.signature,
            deadline     = EXCLUDED.deadline,
            status       = 'signed',
            requested_at = NOW(),
            tx_hash      = NULL,
            block_number = NULL,
            confirmed_at = NULL
          WHERE spot_withdrawals.status = 'expired'
         RETURNING id"
    )
    .bind(&user).bind(req.amount).bind(cfg.bsc_chain_id as i64)
    .bind(next_nonce).bind(&signed.signature).bind(signed.deadline)
    .fetch_optional(&mut *tx).await
    .map_err(|e| { tracing::error!("spot withdraw insert: {e}"); (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: "internal".into() })) })?;
    let row = row.ok_or_else(|| {
        // ON CONFLICT WHERE expired filtered out — slot is held by a
        // confirmed/broadcast/signed row. Pending guard above should make
        // this unreachable, but surface a clear error if it ever fires.
        tracing::error!(user = %user, nonce = %next_nonce, "spot withdraw: nonce slot occupied by non-expired row");
        (StatusCode::CONFLICT, Json(ErrorResponse {
            error: "withdrawal slot conflict — try again in a moment".into()
        }))
    })?;

    sqlx::query(
        "UPDATE spot_balances
            SET available = available - $1, frozen = frozen + $1, updated_at = NOW()
          WHERE user_address = $2 AND token = 'DF'"
    )
    .bind(req.amount).bind(&user)
    .execute(&mut *tx).await
    .map_err(|e| { tracing::error!("spot withdraw freeze: {e}"); (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: "internal".into() })) })?;

    tx.commit().await
        .map_err(|e| { tracing::error!("spot withdraw commit: {e}"); (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: "internal".into() })) })?;

    Ok(Json(WithdrawResponse {
        id: row.0.to_string(),
        nonce: next_nonce,
        signature: signed.signature,
        deadline: signed.deadline.timestamp(),
        vault_address: format!("{:?}", cfg.bsc_vault_address),
        chain_id: cfg.bsc_chain_id as i64,
        amount: req.amount.normalize().to_string(),
        amount_in_wei: signed.amount_wei.to_string(),
    }))
}

pub async fn list_withdrawals(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Query(q): Query<ListQuery>,
) -> Result<Json<Vec<WithdrawalView>>, (StatusCode, Json<ErrorResponse>)> {
    let cfg = state.config.spot.as_ref()
        .ok_or((StatusCode::SERVICE_UNAVAILABLE, Json(ErrorResponse { error: "spot subsystem disabled".into() })))?;
    let user = auth_user.address.to_lowercase();
    let limit = q.limit.clamp(1, 200);

    let rows: Vec<SpotWithdrawal> = if let Some(status) = q.status {
        sqlx::query_as::<_, SpotWithdrawal>(
            "SELECT * FROM spot_withdrawals WHERE user_address=$1 AND status=$2
             ORDER BY requested_at DESC LIMIT $3"
        ).bind(&user).bind(status).bind(limit).fetch_all(&state.db.pool).await
    } else {
        sqlx::query_as::<_, SpotWithdrawal>(
            "SELECT * FROM spot_withdrawals WHERE user_address=$1
             ORDER BY requested_at DESC LIMIT $2"
        ).bind(&user).bind(limit).fetch_all(&state.db.pool).await
    }.map_err(|e| { tracing::error!("spot list_withdrawals: {e}"); (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: "internal".into() })) })?;

    let df_decimals = cfg.df_token_decimals;
    Ok(Json(rows.into_iter().map(|w| to_view(w, df_decimals)).collect()))
}

pub async fn get_withdrawal(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path(id): Path<String>,
) -> Result<Json<WithdrawalView>, (StatusCode, Json<ErrorResponse>)> {
    let cfg = state.config.spot.as_ref()
        .ok_or((StatusCode::SERVICE_UNAVAILABLE, Json(ErrorResponse { error: "spot subsystem disabled".into() })))?;
    let user = auth_user.address.to_lowercase();
    let id = uuid::Uuid::parse_str(&id)
        .map_err(|_| (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: "invalid id".into() })))?;

    let row: Option<SpotWithdrawal> = sqlx::query_as::<_, SpotWithdrawal>(
        "SELECT * FROM spot_withdrawals WHERE id=$1 AND user_address=$2"
    ).bind(id).bind(&user).fetch_optional(&state.db.pool).await
        .map_err(|e| { tracing::error!("spot get_withdrawal: {e}"); (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: "internal".into() })) })?;

    let row = row.ok_or((StatusCode::NOT_FOUND, Json(ErrorResponse { error: "not found".into() })))?;
    Ok(Json(to_view(row, cfg.df_token_decimals)))
}

fn to_view(w: SpotWithdrawal, df_decimals: u8) -> WithdrawalView {
    let amount_in_wei = crate::services::spot::withdraw_signer::decimal_to_wei(w.amount, df_decimals)
        .map(|u| u.to_string())
        .unwrap_or_else(|_| "0".to_string());
    // The DB stores `signature` as NOT NULL TEXT; expose Option to keep room
    // for future statuses where a signature might be cleared.
    let backend_signature = if w.signature.is_empty() { None } else { Some(w.signature) };
    WithdrawalView {
        id: w.id.to_string(),
        token: w.token,
        amount: w.amount.normalize().to_string(),
        amount_in_wei,
        fee: w.fee.normalize().to_string(),
        chain_id: w.chain_id,
        nonce: w.nonce,
        status: w.status,
        backend_signature,
        deadline: w.deadline.timestamp(),
        tx_hash: w.tx_hash,
        block_number: w.block_number,
        requested_at: w.requested_at.timestamp(),
        confirmed_at: w.confirmed_at.map(|t| t.timestamp()),
    }
}
