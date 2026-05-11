#![allow(dead_code)]
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct User {
    pub id: Uuid,
    pub address: String,
    pub nonce: i64,
    pub referral_code: Option<String>,
    pub referrer_address: Option<String>,
    pub email: Option<String>,
    pub email_verified: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateUser {
    pub address: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UserProfile {
    pub address: String,
    pub referral_code: Option<String>,
    pub referrer_address: Option<String>,
    pub email: Option<String>,
    pub email_verified: bool,
    pub created_at: DateTime<Utc>,
}

impl From<User> for UserProfile {
    fn from(user: User) -> Self {
        Self {
            address: user.address,
            referral_code: user.referral_code,
            referrer_address: user.referrer_address,
            email: user.email,
            email_verified: user.email_verified,
            created_at: user.created_at,
        }
    }
}
