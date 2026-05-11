use axum::{extract::{Query, State}, http::StatusCode, Extension, Json};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::auth::middleware::AuthUser;
use crate::models::spot::SpotDeposit;
use crate::AppState;

#[derive(Serialize)]
pub struct DepositView {
    pub id: String,
    pub token: String,
    pub amount: String,
    pub chain_id: i64,
    pub tx_hash: String,
    pub block_number: i64,
    pub status: String,
    pub created_at: i64,
    pub confirmed_at: Option<i64>,
}

#[derive(Serialize)]
pub struct ErrorResponse { pub error: String }

#[derive(Deserialize)]
pub struct ListQuery {
    #[serde(default = "default_limit")]
    pub limit: i64,
}
fn default_limit() -> i64 { 50 }

pub async fn list_deposits(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Query(q): Query<ListQuery>,
) -> Result<Json<Vec<DepositView>>, (StatusCode, Json<ErrorResponse>)> {
    let user = auth_user.address.to_lowercase();
    let limit = q.limit.clamp(1, 200);

    let rows: Vec<SpotDeposit> = sqlx::query_as::<_, SpotDeposit>(
        "SELECT * FROM spot_deposits WHERE user_address=$1
         ORDER BY created_at DESC LIMIT $2"
    ).bind(&user).bind(limit).fetch_all(&state.db.pool).await
        .map_err(|e| { tracing::error!("spot list_deposits: {e}"); (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: "internal".into() })) })?;

    let views = rows.into_iter().map(|d| DepositView {
        id: d.id.to_string(),
        token: d.token,
        amount: d.amount.normalize().to_string(),
        chain_id: d.chain_id,
        tx_hash: d.tx_hash,
        block_number: d.block_number,
        status: d.status,
        created_at: d.created_at.timestamp(),
        confirmed_at: d.confirmed_at.map(|t| t.timestamp()),
    }).collect();

    Ok(Json(views))
}
