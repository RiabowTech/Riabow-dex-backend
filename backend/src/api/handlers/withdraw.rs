use axum::{extract::State, http::StatusCode, Extension, Json};
use rust_decimal::Decimal;
use serde::Serialize;
use std::sync::Arc;

use crate::auth::middleware::AuthUser;
use crate::models::{WithdrawRequest, ConfirmWithdrawRequest};
use crate::AppState;
use axum::extract::Path;

#[derive(Debug, Serialize)]
pub struct WithdrawResponse {
    pub withdraw_id: String,
    pub token: String,
    pub amount: String,
    pub backend_signature: String,
    pub nonce: i64,
    pub expiry: i64,
    pub vault_address: String,
}

#[derive(Debug, Serialize)]
pub struct WithdrawHistoryResponse {
    pub withdrawals: Vec<WithdrawHistoryRecord>,
}

#[derive(Debug, Serialize)]
pub struct WithdrawHistoryRecord {
    pub id: String,
    pub token: String,
    pub amount: Decimal,
    pub nonce: i64,
    pub expiry: i64,
    pub backend_signature: Option<String>,
    pub tx_hash: Option<String>,
    pub status: String,
    pub created_at: i64,
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

/// Incident kill-switch: when set (`WITHDRAWALS_PAUSED=1`), every new withdrawal
/// request is rejected at the edge. Cancellation, history, and on-chain confirm
/// (which is permissionless once a backend signature is issued) are left alone.
///
/// Context: 2026-04-22 phantom-credit audit. Until per-user reconciliation is
/// complete, we don't want to mint new backend signatures. Flip the env var off
/// on the ztdx-backend host via `/home/ubuntu/backend/.env` + container restart
/// once the ledger is clean.
fn withdrawals_paused() -> bool {
    matches!(
        std::env::var("WITHDRAWALS_PAUSED").ok().as_deref(),
        Some("1") | Some("true") | Some("TRUE")
    )
}

/// 请求提款 - 返回后端签名用于合约调用
pub async fn request_withdraw(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Json(req): Json<WithdrawRequest>,
) -> Result<Json<WithdrawResponse>, (StatusCode, Json<ErrorResponse>)> {
    if withdrawals_paused() {
        tracing::warn!(
            "withdraw blocked by kill switch: user={}, token={}, amount={}",
            auth_user.address, req.token, req.amount
        );
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "提现服务临时暂停：账本对账中，稍后恢复。详情请联系支持。".to_string(),
            }),
        ));
    }

    // Block API Key withdrawals
    if auth_user.is_api_key {
        return Err((
            StatusCode::FORBIDDEN,
            Json(ErrorResponse {
                error: "API Key permission denied: Withdrawals not allowed".to_string(),
            }),
        ));
    }

    // 调用提款服务
    match state.withdraw_service.request_withdrawal(
        &auth_user.address,
        &req.token,
        req.amount,
    ).await {
        Ok(result) => {
            Ok(Json(WithdrawResponse {
                withdraw_id: result.withdrawal_id,
                token: result.token,
                amount: result.amount,
                backend_signature: result.signature,
                nonce: result.nonce,
                expiry: result.expiry,
                vault_address: result.vault_address,
            }))
        }
        Err(e) => {
            tracing::error!("提款请求失败: {}", e);
            Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: e.to_string(),
                }),
            ))
        }
    }
}

/// 获取提款历史
pub async fn get_history(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
) -> Result<Json<WithdrawHistoryResponse>, (StatusCode, Json<ErrorResponse>)> {
    tracing::info!(
        "收到提现历史查询请求 - 用户地址: {}",
        auth_user.address
    );
    
    match state.withdraw_service.get_history(&auth_user.address).await {
        Ok(records) => {
            let withdrawals: Vec<WithdrawHistoryRecord> = records
                .into_iter()
                .map(|r| WithdrawHistoryRecord {
                    id: r.id,
                    token: r.token,
                    amount: r.amount,
                    nonce: r.nonce,
                    expiry: r.expiry,
                    backend_signature: r.backend_signature,
                    tx_hash: r.tx_hash,
                    status: r.status,
                    created_at: r.created_at,
                })
                .collect();
            
            tracing::info!(
                "成功返回提现历史 - 用户: {}, 记录数: {}",
                auth_user.address,
                withdrawals.len()
            );

            Ok(Json(WithdrawHistoryResponse { withdrawals }))
        }
        Err(e) => {
            tracing::error!(
                "获取提款历史失败 - 用户: {}, 错误: {}",
                auth_user.address,
                e
            );
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "获取提款历史失败".to_string(),
                }),
            ))
        }
    }
}

/// 获取单个提现详情
pub async fn get_withdrawal(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path(withdrawal_id): Path<String>,
) -> Result<Json<WithdrawHistoryRecord>, (StatusCode, Json<ErrorResponse>)> {
    tracing::info!(
        "获取提现详情 - 用户: {}, ID: {}",
        auth_user.address,
        withdrawal_id
    );

    match state.withdraw_service.get_withdrawal(&auth_user.address, &withdrawal_id).await {
        Ok(record) => {
            Ok(Json(WithdrawHistoryRecord {
                id: record.id,
                token: record.token,
                amount: record.amount,
                nonce: record.nonce,
                expiry: record.expiry,
                backend_signature: record.backend_signature,
                tx_hash: record.tx_hash,
                status: record.status,
                created_at: record.created_at,
            }))
        }
        Err(e) => {
            tracing::error!(
                "获取提现详情失败 - 用户: {}, ID: {}, 错误: {}",
                auth_user.address,
                withdrawal_id,
                e
            );
            Err((
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: "提现记录不存在".to_string(),
                }),
            ))
        }
    }
}

/// 取消提现
pub async fn cancel_withdraw(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path(withdrawal_id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    tracing::info!(
        "取消提现 - 用户: {}, ID: {}",
        auth_user.address,
        withdrawal_id
    );

    match state.withdraw_service.cancel_withdrawal(&auth_user.address, &withdrawal_id).await {
        Ok(_) => {
            tracing::info!(
                "提现已取消 - 用户: {}, ID: {}",
                auth_user.address,
                withdrawal_id
            );
            Ok(Json(serde_json::json!({
                "success": true,
                "message": "提现已取消"
            })))
        }
        Err(e) => {
            tracing::error!(
                "取消提现失败 - 用户: {}, ID: {}, 错误: {}",
                auth_user.address,
                withdrawal_id,
                e
            );
            Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: e.to_string(),
                }),
            ))
        }
    }
}

/// 确认提现交易
pub async fn confirm_withdraw(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path(withdrawal_id): Path<String>,
    Json(req): Json<ConfirmWithdrawRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    tracing::info!(
        "确认提现 - 用户: {}, ID: {}, TX: {}",
        auth_user.address,
        withdrawal_id,
        req.tx_hash
    );

    match state.withdraw_service.confirm_withdrawal(
        &auth_user.address,
        &withdrawal_id,
        &req.tx_hash,
    ).await {
        Ok(_) => {
            tracing::info!(
                "提现已确认 - 用户: {}, ID: {}",
                auth_user.address,
                withdrawal_id
            );
            Ok(Json(serde_json::json!({
                "success": true,
                "message": "提现已确认"
            })))
        }
        Err(e) => {
            tracing::error!(
                "确认提现失败 - 用户: {}, ID: {}, 错误: {}",
                auth_user.address,
                withdrawal_id,
                e
            );
            Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: e.to_string(),
                }),
            ))
        }
    }
}
