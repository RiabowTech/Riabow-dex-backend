use axum::{extract::State, http::StatusCode, Extension, Json};
use rust_decimal::Decimal;
use serde::Serialize;
use std::sync::Arc;

use crate::auth::middleware::AuthUser;
use crate::services::spot::wallet;
use crate::AppState;

#[derive(Serialize)]
pub struct BalanceItem {
    pub token: String,
    pub available: String,
    pub frozen: String,
}

#[derive(Serialize)]
pub struct BalancesResponse {
    pub balances: Vec<BalanceItem>,
}

#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

pub async fn list_balances(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
) -> Result<Json<BalancesResponse>, (StatusCode, Json<ErrorResponse>)> {
    let rows = wallet::list_balances(&state.db.pool, &auth_user.address)
        .await
        .map_err(|e| {
            tracing::error!("spot list_balances db error: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "internal error".into(),
                }),
            )
        })?;

    let balances = rows
        .into_iter()
        .map(|r| BalanceItem {
            token: r.token,
            available: format_decimal(r.available),
            frozen: format_decimal(r.frozen),
        })
        .collect();

    Ok(Json(BalancesResponse { balances }))
}

fn format_decimal(d: Decimal) -> String {
    d.normalize().to_string()
}
