use axum::{
    extract::{Path, State},
    http::StatusCode,
    Extension, Json,
};
use serde::Serialize;
use std::sync::Arc;
use uuid::Uuid;
use chrono::Utc;
use std::net::IpAddr;
use rand::RngCore;

fn validate_ip_whitelist(whitelist: &str) -> bool {
    if whitelist.trim().is_empty() {
        return true;
    }
    for ip in whitelist.split(',') {
        if ip.trim().parse::<IpAddr>().is_err() {
            return false;
        }
    }
    true
}

use crate::auth::middleware::AuthUser;
use crate::models::user_api_key::{
    CreateUserApiKeyRequest, CreatedUserApiKeyResponse, UpdateUserApiKeyRequest, UserApiKey,
    UserApiKeyResponse,
};
use crate::AppState;

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub code: String,
}

/// Helper to get user ID from address, creating if not exists
async fn get_or_create_user_id(
    pool: &sqlx::PgPool,
    address: &str,
) -> Result<Uuid, (StatusCode, Json<ErrorResponse>)> {
    let address_lower = address.to_lowercase();
    
    // First try to find existing user
    let user_id = sqlx::query_scalar::<_, Uuid>("SELECT id FROM users WHERE address = $1")
        .bind(&address_lower)
        .fetch_optional(pool)
        .await
        .map_err(|e| {
            tracing::error!("Failed to fetch user: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Internal server error".to_string(),
                    code: "DB_ERROR".to_string(),
                }),
            )
        })?;

    if let Some(id) = user_id {
        Ok(id)
    } else {
        // Create new user
        let now = Utc::now();
        let new_id = sqlx::query_scalar::<_, Uuid>(
            "INSERT INTO users (address, created_at) VALUES ($1, $2) RETURNING id"
        )
        .bind(&address_lower)
        .bind(now)
        .fetch_one(pool)
        .await
        .map_err(|e| {
            tracing::error!("Failed to create user: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Failed to create user".to_string(),
                    code: "USER_CREATE_FAILED".to_string(),
                }),
            )
        })?;
        Ok(new_id)
    }
}

fn generate_random_hex(bytes: usize) -> String {
    let mut data = vec![0u8; bytes];
    rand::thread_rng().fill_bytes(&mut data);
    hex::encode(data)
}

/// Create a new API Key
pub async fn create_api_key(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Json(payload): Json<CreateUserApiKeyRequest>,
) -> Result<Json<CreatedUserApiKeyResponse>, (StatusCode, Json<ErrorResponse>)> {
    let user_id = get_or_create_user_id(&state.db.pool, &auth_user.address).await?;

    // Check limit (e.g. 30 keys)
    let count = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM user_api_keys WHERE user_id = $1")
        .bind(user_id)
        .fetch_one(&state.db.pool)
        .await
        .unwrap_or(0);

    if count >= 30 {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "API Key limit reached (max 30)".to_string(),
                code: "LIMIT_REACHED".to_string(),
            }),
        ));
    }
    
    if let Some(ref whitelist) = payload.ip_whitelist {
        if !validate_ip_whitelist(whitelist) {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "Invalid IP whitelist format".to_string(),
                    code: "INVALID_IP".to_string(),
                }),
            ));
        }
    }

    // Generate keys
    // Binance: 64 chars hex (32 bytes)
    let api_key = generate_random_hex(32);
    let secret = generate_random_hex(32);

    let permissions = "trading,deposit";
    let now = Utc::now();

    let created_key = sqlx::query_as::<_, UserApiKey>(
        r#"
        INSERT INTO user_api_keys (user_id, api_key, secret_key, label, ip_whitelist, permissions, created_at, updated_at, status)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'active')
        RETURNING *
        "#
    )
    .bind(user_id)
    .bind(&api_key)
    .bind(&secret) // Storing plain secret as discussed (or encrypted if added later)
    .bind(payload.label)
    .bind(payload.ip_whitelist)
    .bind(permissions)
    .bind(now)
    .bind(now)
    .fetch_one(&state.db.pool)
    .await
    .map_err(|e| {
        tracing::error!("Failed to create API key: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "Failed to create API Key".to_string(),
                code: "CREATE_FAILED".to_string(),
            }),
        )
    })?;

    Ok(Json(CreatedUserApiKeyResponse {
        id: created_key.id,
        api_key: created_key.api_key,
        secret_key: secret, // Return secret ONLY here
        label: created_key.label,
        ip_whitelist: created_key.ip_whitelist,
        permissions: created_key.permissions,
        created_at: created_key.created_at,
        status: created_key.status,
    }))
}

/// List API Keys
pub async fn list_api_keys(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
) -> Result<Json<Vec<UserApiKeyResponse>>, (StatusCode, Json<ErrorResponse>)> {
    let user_id = get_or_create_user_id(&state.db.pool, &auth_user.address).await?;

    let keys = sqlx::query_as::<_, UserApiKey>(
        "SELECT * FROM user_api_keys WHERE user_id = $1 ORDER BY created_at DESC"
    )
    .bind(user_id)
    .fetch_all(&state.db.pool)
    .await
    .map_err(|e| {
        tracing::error!("Failed to list API keys: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "Failed to list API Keys".to_string(),
                code: "LIST_FAILED".to_string(),
            }),
        )
    })?;

    let response = keys.into_iter().map(|k| UserApiKeyResponse {
        id: k.id,
        api_key: k.api_key,
        label: k.label,
        ip_whitelist: k.ip_whitelist,
        permissions: k.permissions,
        created_at: k.created_at,
        last_used_at: k.last_used_at,
        status: k.status,
    }).collect();

    Ok(Json(response))
}

/// Delete API Key
pub async fn delete_api_key(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let user_id = get_or_create_user_id(&state.db.pool, &auth_user.address).await?;

    let result = sqlx::query("DELETE FROM user_api_keys WHERE id = $1 AND user_id = $2")
        .bind(id)
        .bind(user_id)
        .execute(&state.db.pool)
        .await
        .map_err(|e| {
            tracing::error!("Failed to delete API key: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Failed to delete API Key".to_string(),
                    code: "DELETE_FAILED".to_string(),
                }),
            )
        })?;

    if result.rows_affected() == 0 {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: "API Key not found".to_string(),
                code: "NOT_FOUND".to_string(),
            }),
        ));
    }

    Ok(StatusCode::NO_CONTENT)
}

/// Update API Key
pub async fn update_api_key(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path(id): Path<Uuid>,
    Json(payload): Json<UpdateUserApiKeyRequest>,
) -> Result<Json<UserApiKeyResponse>, (StatusCode, Json<ErrorResponse>)> {
    let user_id = get_or_create_user_id(&state.db.pool, &auth_user.address).await?;

    // Build dynamic update query
    let mut query = String::from("UPDATE user_api_keys SET updated_at = NOW()");
    let mut params = Vec::new();
    let mut param_idx = 1;

    // We can't use dynamic binding easily with sqlx macros in loop, simplified approach:
    // Actually standard approach is simple if fields are known.
    
    if let Some(ref label) = payload.label {
        query.push_str(&format!(", label = ${}", param_idx));
        params.push(label.clone()); // Assuming bind(val) works
        param_idx += 1;
    }
    
    if let Some(ref whitelist) = payload.ip_whitelist {
        if !validate_ip_whitelist(whitelist) {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "Invalid IP whitelist format".to_string(),
                    code: "INVALID_IP".to_string(),
                }),
            ));
        }
        query.push_str(&format!(", ip_whitelist = ${}", param_idx));
        params.push(whitelist.clone());
        param_idx += 1;
    }
    
    if let Some(ref status) = payload.status {
        query.push_str(&format!(", status = ${}", param_idx));
        params.push(status.clone());
        param_idx += 1;
    }

    query.push_str(&format!(" WHERE id = ${} AND user_id = ${} RETURNING *", param_idx, param_idx + 1));

    // Execution is tricky with variable args in sqlx without query builder or manual binding
    // I'll use a simpler Update approach: fetch, modify, save.
    
    let mut key = sqlx::query_as::<_, UserApiKey>("SELECT * FROM user_api_keys WHERE id = $1 AND user_id = $2")
        .bind(id)
        .bind(user_id)
        .fetch_optional(&state.db.pool)
        .await
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse{error:"DB Error".into(), code:"DB_ERR".into()})))?
        .ok_or((StatusCode::NOT_FOUND, Json(ErrorResponse{error:"Not Found".into(), code:"NOT_FOUND".into()})))?;

    if let Some(label) = payload.label {
        key.label = Some(label);
    }
    if let Some(whitelist) = payload.ip_whitelist {
        key.ip_whitelist = Some(whitelist);
    }
    if let Some(status) = payload.status {
        key.status = status;
    }

    let updated_key = sqlx::query_as::<_, UserApiKey>(
        r#"
        UPDATE user_api_keys 
        SET label = $1, ip_whitelist = $2, status = $3, updated_at = NOW()
        WHERE id = $4
        RETURNING *
        "#
    )
    .bind(key.label)
    .bind(key.ip_whitelist)
    .bind(key.status)
    .bind(key.id)
    .fetch_one(&state.db.pool)
    .await
    .map_err(|e| {
         tracing::error!("Update failed: {}", e);
         (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse{error:"Update Failed".into(), code:"UPDATE_FAILED".into()}))
    })?;

    Ok(Json(UserApiKeyResponse {
        id: updated_key.id,
        api_key: updated_key.api_key,
        label: updated_key.label,
        ip_whitelist: updated_key.ip_whitelist,
        permissions: updated_key.permissions,
        created_at: updated_key.created_at,
        last_used_at: updated_key.last_used_at,
        status: updated_key.status,
    }))
}
