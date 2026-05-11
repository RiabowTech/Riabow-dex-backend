//! MM pool HTTP handlers (PRD §7.4 + admin whitelist CRUD).
//!
//! - `GET  /api/v1/mm/dashboard`  — caller's MM dashboard (auth)
//! - `GET  /api/v1/mm/snapshots`  — recent quality snapshots (auth)
//! - `GET  /api/v1/admin/mm/members`        — list whitelist
//! - `POST /api/v1/admin/mm/members`        — upsert MM
//! - `DELETE /api/v1/admin/mm/members/:addr` — soft-deactivate

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Extension, Json,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::auth::middleware::AuthUser;
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
// GET /api/v1/mm/dashboard
// ---------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct MmDashboardResponse {
    pub mm_address: String,
    pub is_whitelisted: bool,
    pub epoch_number: Option<i32>,
    pub quality_score_sum: Decimal,
    pub snapshot_count: i32,
    pub estimated_token_share: Option<Decimal>,
    pub actual_tokens: Option<i64>,
    /// Aggregated 4-dim breakdown over the current epoch. Each value
    /// is the AVERAGE of the per-snapshot raw measurement.
    pub dimensions: MmDimensionAvg,
    /// Top symbols by maker volume contribution this epoch.
    pub top_symbols: Vec<MmSymbolStat>,
}

#[derive(Debug, Serialize, Default)]
pub struct MmDimensionAvg {
    pub avg_maker_volume_usd: Decimal,
    pub avg_spread_bps: Option<Decimal>,
    pub avg_depth_usd: Decimal,
    pub uptime_pct: Decimal,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct MmSymbolStat {
    pub symbol: String,
    pub total_maker_volume_usd: Decimal,
    pub avg_quality_score: Decimal,
    pub snapshot_count: i64,
}

pub async fn get_mm_dashboard(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
) -> Result<Json<MmDashboardResponse>, (StatusCode, Json<ErrorResponse>)> {
    let addr = auth_user.address.to_lowercase();

    let is_whitelisted: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM mm_program_members WHERE address = $1 AND is_active = true)",
    )
    .bind(&addr)
    .fetch_one(&state.db.pool)
    .await
    .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, "DB_ERROR", e.to_string()))?;

    let epoch = state
        .points_service
        .get_active_epoch()
        .await
        .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, "DB_ERROR", e.to_string()))?
        .map(|e| e.epoch_number);

    let (quality_score_sum, snapshot_count, estimated_token_share, actual_tokens): (
        Decimal,
        i32,
        Option<Decimal>,
        Option<i64>,
    ) = if let Some(ep) = epoch {
        sqlx::query_as(
            "SELECT quality_score_sum, snapshot_count, estimated_token_share, actual_tokens
             FROM mm_points_balance WHERE mm_address = $1 AND epoch_number = $2",
        )
        .bind(&addr)
        .bind(ep)
        .fetch_optional(&state.db.pool)
        .await
        .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, "DB_ERROR", e.to_string()))?
        .unwrap_or((Decimal::ZERO, 0, None, None))
    } else {
        (Decimal::ZERO, 0, None, None)
    };

    // Dimension averages and uptime pct over the current epoch.
    let dims: MmDimensionAvg = if let Some(ep) = epoch {
        let row: Option<(Decimal, Option<Decimal>, Decimal, i64, i64)> = sqlx::query_as(
            "SELECT COALESCE(AVG(maker_volume_usd), 0),
                    AVG(spread_bps),
                    COALESCE(AVG(depth_usd), 0),
                    COALESCE(SUM(CASE WHEN is_online THEN 1 ELSE 0 END), 0),
                    COUNT(*)
             FROM mm_quality_snapshots
             WHERE mm_address = $1 AND epoch_number = $2",
        )
        .bind(&addr)
        .bind(ep)
        .fetch_optional(&state.db.pool)
        .await
        .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, "DB_ERROR", e.to_string()))?;

        match row {
            Some((vol, spread, depth, online, total)) if total > 0 => MmDimensionAvg {
                avg_maker_volume_usd: vol,
                avg_spread_bps: spread,
                avg_depth_usd: depth,
                uptime_pct: Decimal::from(online) * Decimal::from(100) / Decimal::from(total),
            },
            _ => MmDimensionAvg::default(),
        }
    } else {
        MmDimensionAvg::default()
    };

    let top_symbols: Vec<MmSymbolStat> = if let Some(ep) = epoch {
        sqlx::query_as::<_, MmSymbolStat>(
            "SELECT symbol,
                    COALESCE(SUM(maker_volume_usd), 0) AS total_maker_volume_usd,
                    COALESCE(AVG(quality_score), 0)    AS avg_quality_score,
                    COUNT(*)                           AS snapshot_count
             FROM mm_quality_snapshots
             WHERE mm_address = $1 AND epoch_number = $2
             GROUP BY symbol
             ORDER BY total_maker_volume_usd DESC
             LIMIT 10",
        )
        .bind(&addr)
        .bind(ep)
        .fetch_all(&state.db.pool)
        .await
        .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, "DB_ERROR", e.to_string()))?
    } else {
        Vec::new()
    };

    Ok(Json(MmDashboardResponse {
        mm_address: addr,
        is_whitelisted,
        epoch_number: epoch,
        quality_score_sum,
        snapshot_count,
        estimated_token_share,
        actual_tokens,
        dimensions: dims,
        top_symbols,
    }))
}

// ---------------------------------------------------------------
// GET /api/v1/mm/snapshots
// ---------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct SnapshotsQuery {
    #[serde(default)]
    pub limit: Option<i64>,
    #[serde(default)]
    pub symbol: Option<String>,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct SnapshotRow {
    pub mm_address: String,
    pub symbol: String,
    pub snapshot_at: chrono::DateTime<chrono::Utc>,
    pub maker_volume_usd: Decimal,
    pub spread_bps: Option<Decimal>,
    pub depth_usd: Decimal,
    pub is_online: bool,
    pub quality_score: Decimal,
    pub epoch_number: i32,
}

#[derive(Debug, Serialize)]
pub struct SnapshotsResponse {
    pub snapshots: Vec<SnapshotRow>,
}

pub async fn get_mm_snapshots(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Query(q): Query<SnapshotsQuery>,
) -> Result<Json<SnapshotsResponse>, (StatusCode, Json<ErrorResponse>)> {
    let addr = auth_user.address.to_lowercase();
    let limit = q.limit.unwrap_or(50).clamp(1, 500);

    let snapshots: Vec<SnapshotRow> = if let Some(sym) = q.symbol {
        sqlx::query_as::<_, SnapshotRow>(
            "SELECT mm_address, symbol, snapshot_at, maker_volume_usd, spread_bps, depth_usd,
                    is_online, quality_score, epoch_number
             FROM mm_quality_snapshots
             WHERE mm_address = $1 AND symbol = $2
             ORDER BY snapshot_at DESC LIMIT $3",
        )
        .bind(&addr)
        .bind(sym)
        .bind(limit)
        .fetch_all(&state.db.pool)
        .await
    } else {
        sqlx::query_as::<_, SnapshotRow>(
            "SELECT mm_address, symbol, snapshot_at, maker_volume_usd, spread_bps, depth_usd,
                    is_online, quality_score, epoch_number
             FROM mm_quality_snapshots
             WHERE mm_address = $1
             ORDER BY snapshot_at DESC LIMIT $2",
        )
        .bind(&addr)
        .bind(limit)
        .fetch_all(&state.db.pool)
        .await
    }
    .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, "DB_ERROR", e.to_string()))?;

    Ok(Json(SnapshotsResponse { snapshots }))
}

// ---------------------------------------------------------------
// Admin: MM whitelist CRUD
// ---------------------------------------------------------------

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct MmMember {
    pub address: String,
    pub label: Option<String>,
    pub is_active: bool,
    pub activated_at: chrono::DateTime<chrono::Utc>,
    pub deactivated_at: Option<chrono::DateTime<chrono::Utc>>,
    pub notes: Option<String>,
    pub updated_by: Option<String>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize)]
pub struct MembersResponse {
    pub members: Vec<MmMember>,
}

pub async fn admin_list_mm_members(
    State(state): State<Arc<AppState>>,
) -> Result<Json<MembersResponse>, (StatusCode, Json<ErrorResponse>)> {
    let members: Vec<MmMember> = sqlx::query_as::<_, MmMember>(
        "SELECT address, label, is_active, activated_at, deactivated_at, notes,
                updated_by, updated_at
         FROM mm_program_members
         ORDER BY activated_at DESC",
    )
    .fetch_all(&state.db.pool)
    .await
    .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, "DB_ERROR", e.to_string()))?;
    Ok(Json(MembersResponse { members }))
}

#[derive(Debug, Deserialize)]
pub struct UpsertMmRequest {
    pub address: String,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub notes: Option<String>,
    #[serde(default = "default_true")]
    pub is_active: bool,
    #[serde(default)]
    pub updated_by: Option<String>,
}
fn default_true() -> bool { true }

pub async fn admin_upsert_mm_member(
    State(state): State<Arc<AppState>>,
    Json(req): Json<UpsertMmRequest>,
) -> Result<Json<MmMember>, (StatusCode, Json<ErrorResponse>)> {
    let addr = req.address.to_lowercase();
    if !addr.starts_with("0x") || addr.len() != 42 {
        return Err(err(StatusCode::BAD_REQUEST, "INVALID_ADDRESS", "address must be 0x-prefixed 40 hex"));
    }

    sqlx::query(
        "INSERT INTO mm_program_members (address, label, is_active, notes, updated_by, updated_at)
         VALUES ($1, $2, $3, $4, $5, NOW())
         ON CONFLICT (address) DO UPDATE SET
            label          = EXCLUDED.label,
            is_active      = EXCLUDED.is_active,
            notes          = EXCLUDED.notes,
            updated_by     = EXCLUDED.updated_by,
            updated_at     = NOW(),
            deactivated_at = CASE WHEN EXCLUDED.is_active = false
                                  THEN COALESCE(mm_program_members.deactivated_at, NOW())
                                  ELSE NULL END",
    )
    .bind(&addr)
    .bind(&req.label)
    .bind(req.is_active)
    .bind(&req.notes)
    .bind(&req.updated_by)
    .execute(&state.db.pool)
    .await
    .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, "DB_ERROR", e.to_string()))?;

    let row: MmMember = sqlx::query_as::<_, MmMember>(
        "SELECT address, label, is_active, activated_at, deactivated_at, notes,
                updated_by, updated_at
         FROM mm_program_members WHERE address = $1",
    )
    .bind(&addr)
    .fetch_one(&state.db.pool)
    .await
    .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, "DB_ERROR", e.to_string()))?;
    Ok(Json(row))
}

pub async fn admin_deactivate_mm_member(
    State(state): State<Arc<AppState>>,
    Path(address): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let addr = address.to_lowercase();
    let affected = sqlx::query(
        "UPDATE mm_program_members
         SET is_active = false,
             deactivated_at = COALESCE(deactivated_at, NOW()),
             updated_at = NOW()
         WHERE address = $1",
    )
    .bind(&addr)
    .execute(&state.db.pool)
    .await
    .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, "DB_ERROR", e.to_string()))?
    .rows_affected();
    Ok(Json(serde_json::json!({ "deactivated": affected })))
}
