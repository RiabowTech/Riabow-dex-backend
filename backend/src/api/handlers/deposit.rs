use axum::{extract::State, http::StatusCode, Extension, Json};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

use crate::auth::middleware::AuthUser;
use crate::AppState;

#[derive(Debug, Deserialize)]
pub struct PrepareDepositRequest {
    pub token: String,
    pub amount: Decimal,
}

#[derive(Debug, Serialize)]
pub struct PrepareDepositResponse {
    pub contract_address: String,
    pub token_address: String,
    pub amount: String,
    pub estimated_gas: u64,
}

#[derive(Debug, Serialize)]
pub struct DepositHistoryResponse {
    pub deposits: Vec<DepositRecord>,
}

#[derive(Debug, Serialize)]
pub struct DepositRecord {
    pub id: String,
    pub token: String,
    pub amount: Decimal,
    pub tx_hash: String,
    pub status: String,
    pub created_at: i64,
}

/// Prepare deposit - returns contract call parameters
pub async fn prepare_deposit(
    State(state): State<Arc<AppState>>,
    Extension(_auth_user): Extension<AuthUser>,
    Json(req): Json<PrepareDepositRequest>,
) -> Result<Json<PrepareDepositResponse>, StatusCode> {
    // Get token address from config
    let token_address = state.config.get_token_address(&req.token)
        .ok_or(StatusCode::BAD_REQUEST)?;

    Ok(Json(PrepareDepositResponse {
        contract_address: state.config.vault_address.clone(),
        token_address: token_address.to_string(),
        amount: req.amount.to_string(),
        estimated_gas: 100000,
    }))
}

/// Get deposit history
pub async fn get_history(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
) -> Result<Json<DepositHistoryResponse>, StatusCode> {
    // Fetch deposit history from database
    let rows: Vec<(Uuid, String, Decimal, String, String, DateTime<Utc>)> = sqlx::query_as(
        r#"
        SELECT id, token, amount, tx_hash, status, created_at
        FROM deposits
        WHERE user_address = $1
        ORDER BY created_at DESC
        LIMIT 100
        "#
    )
    .bind(&auth_user.address.to_lowercase())
    .fetch_all(&state.db.pool)
    .await
    .map_err(|e| {
        tracing::error!("Failed to fetch deposit history: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let deposits: Vec<DepositRecord> = rows
        .into_iter()
        .map(|(id, token, amount, tx_hash, status, created_at)| {
            DepositRecord {
                id: id.to_string(),
                token,
                amount,
                tx_hash,
                status,
                created_at: created_at.timestamp(),
            }
        })
        .collect();

    Ok(Json(DepositHistoryResponse { deposits }))
}
