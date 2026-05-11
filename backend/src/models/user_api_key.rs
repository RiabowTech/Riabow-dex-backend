use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct UserApiKey {
    pub id: Uuid,
    pub user_id: Uuid,
    pub api_key: String,
    #[serde(skip_serializing)] // Never serialize secret by default
    pub secret_key: String,
    pub label: Option<String>,
    pub ip_whitelist: Option<String>,
    pub permissions: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub last_used_at: Option<DateTime<Utc>>,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateUserApiKeyRequest {
    pub label: Option<String>,
    pub ip_whitelist: Option<String>, // comma separated
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateUserApiKeyRequest {
    pub label: Option<String>,
    pub ip_whitelist: Option<String>, // comma separated
    pub status: Option<String>, // active / disabled
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserApiKeyResponse {
    pub id: Uuid,
    pub api_key: String,
    pub label: Option<String>,
    pub ip_whitelist: Option<String>,
    pub permissions: String,
    pub created_at: DateTime<Utc>,
    pub last_used_at: Option<DateTime<Utc>>,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreatedUserApiKeyResponse {
    pub id: Uuid,
    pub api_key: String,
    pub secret_key: String, // Only returned once upon creation
    pub label: Option<String>,
    pub ip_whitelist: Option<String>,
    pub permissions: String,
    pub created_at: DateTime<Utc>,
    pub status: String,
}
