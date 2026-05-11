use axum::{extract::State, http::StatusCode, Extension, Json};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::auth::middleware::AuthUser;
use crate::models::spot::TransferDirection;
use crate::services::spot::wallet::{self, WalletError};
use crate::AppState;

#[derive(Deserialize)]
pub struct TransferRequest {
    pub direction: String, // "perp_to_spot" | "spot_to_perp"
    pub token: String,     // MVP: "USDT"
    pub amount: Decimal,
}

#[derive(Serialize)]
pub struct TransferResponse {
    pub direction: String,
    pub token: String,
    pub amount: String,
    pub perp_balance_after: String,
    pub spot_balance_after: String,
}

#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

pub async fn transfer(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Json(req): Json<TransferRequest>,
) -> Result<Json<TransferResponse>, (StatusCode, Json<ErrorResponse>)> {
    if auth_user.is_api_key {
        return Err((
            StatusCode::FORBIDDEN,
            Json(ErrorResponse {
                error: "API Key permission denied: transfers not allowed".into(),
            }),
        ));
    }

    let direction = match req.direction.as_str() {
        "perp_to_spot" => TransferDirection::PerpToSpot,
        "spot_to_perp" => TransferDirection::SpotToPerp,
        other => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: format!("invalid direction: {other}"),
                }),
            ))
        }
    };

    let result =
        wallet::transfer(&state.db.pool, &auth_user.address, direction, &req.token, req.amount)
            .await;

    match result {
        Ok(r) => {
            // SPOT-side push for the caller's USDT balance (perp side has its own channel).
            // MVP: req.token is always "USDT", but routing through it keeps this
            // forward-compatible when more tokens become transferable.
            crate::services::spot::ws_publisher::push_balance_now(
                &state,
                &auth_user.address.to_lowercase(),
                &req.token,
            ).await;
            Ok(Json(TransferResponse {
                direction: req.direction,
                token: req.token,
                amount: req.amount.normalize().to_string(),
                perp_balance_after: r.perp_balance_after.normalize().to_string(),
                spot_balance_after: r.spot_balance_after.normalize().to_string(),
            }))
        }
        Err(WalletError::InsufficientBalance) => Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "insufficient balance".into(),
            }),
        )),
        Err(WalletError::UnsupportedToken(t)) => Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: format!("unsupported token: {t}"),
            }),
        )),
        Err(WalletError::NonPositiveAmount) => Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "amount must be positive".into(),
            }),
        )),
        Err(WalletError::Db(e)) => {
            tracing::error!("spot transfer db error: {e}");
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "internal error".into(),
                }),
            ))
        }
    }
}
