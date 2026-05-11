//! Referral System API Handlers
//!
//! Phase 10: Complete referral code generation, binding, and commission distribution

use axum::{extract::State, http::StatusCode, Extension, Json};
use rust_decimal::Decimal;
use serde::{Serialize, Serializer};
// use sqlx::PgPool;
use std::sync::Arc;
// use tokio::sync::RwLock;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::auth::middleware::AuthUser;
use crate::auth::eip712::{
    verify_create_referral_signature, verify_bind_referral_signature,
    CreateReferralMessage, BindReferralMessage,
};
use crate::models::{BindReferralRequest, CreateReferralCodeRequest};
use crate::AppState;

// Helper module to serialize DateTime as milliseconds timestamp
mod datetime_as_millis {
    use chrono::{DateTime, Utc};
    use serde::Serializer;

    pub fn serialize<S>(dt: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_i64(dt.timestamp_millis())
    }
}

#[derive(Debug, Serialize)]
pub struct CreateCodeResponse {
    pub success: bool,
    pub code: String,
    #[serde(serialize_with = "datetime_as_millis::serialize")]
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
pub struct BindCodeResponse {
    pub success: bool,
    pub referrer_address: String,
    pub referrer_code: String,
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub code: String,
}

#[derive(Debug, Serialize)]
pub struct ReferralActivity {
    pub referral_address: String,
    pub event_type: String,
    pub volume: Decimal,
    pub commission: Decimal,
    #[serde(serialize_with = "datetime_as_millis::serialize")]
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
pub struct DashboardResponse {
    pub code: Option<String>,
    pub total_referrals: i64,
    pub active_referrals: i64,
    pub total_earnings: Decimal,
    pub pending_earnings: Decimal,
    pub claimed_earnings: Decimal,
    pub total_referred_volume: Decimal,
    /// `null` when user has not yet reached the Starter threshold.
    pub tier: Option<ReferralTier>,
    /// Set when `tier` is null — explains the minimum requirement to the user.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tier_note: Option<String>,
    pub recent_activity: Vec<ReferralActivity>,
    /// 用户已绑定的邀请码信息（使用的上级邀请码）
    pub bound_referral: Option<BoundReferralInfo>,
}

#[derive(Debug, Serialize)]
pub struct BoundReferralInfo {
    /// 绑定的邀请码
    pub code: String,
    /// 推荐人地址
    pub referrer_address: String,
    /// 绑定时间
    pub bound_at: DateTime<Utc>,
}

/// Tier info returned in dashboard responses.
///
/// Level values match the DB `referral_codes.tier` column (0–4).
#[derive(Debug, Serialize)]
pub struct ReferralTier {
    pub level: i32,
    pub name: String,
    pub commission_rate: Decimal,
    pub rate_bps: i32,
    /// Referral count needed to reach the next tier (None at Diamond).
    pub next_tier_referrals: Option<i64>,
    /// Referred trading volume (USD) needed to reach the next tier.
    pub next_tier_volume: Option<Decimal>,
}

/// Validate timestamp (within 5 minutes)
fn validate_timestamp(timestamp: u64) -> bool {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    now.abs_diff(timestamp) <= 300
}

/// Generate a unique referral code from address
fn generate_referral_code(address: &str) -> String {
    use sha3::{Digest, Keccak256};
    let input = format!("{}{}", address, chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0));
    let hash = Keccak256::digest(input.as_bytes());
    format!("{:x}", hash)[..8].to_uppercase()
}

/// Compute effective tier from dual AND criteria (referral count + referred volume).
///
/// Returns `None` if neither condition for Starter is satisfied — the caller
/// should surface an error message to the user.
fn get_tier(referral_count: i64, total_referred_volume: Decimal) -> Option<ReferralTier> {
    if referral_count >= 100 && total_referred_volume >= Decimal::new(2_000_000, 0) {
        Some(ReferralTier {
            level: 4,
            name: "Diamond".to_string(),
            commission_rate: Decimal::new(25, 2),
            rate_bps: 2500,
            next_tier_referrals: None,
            next_tier_volume: None,
        })
    } else if referral_count >= 50 && total_referred_volume >= Decimal::new(500_000, 0) {
        Some(ReferralTier {
            level: 3,
            name: "Gold".to_string(),
            commission_rate: Decimal::new(22, 2),
            rate_bps: 2200,
            next_tier_referrals: Some(100),
            next_tier_volume: Some(Decimal::new(2_000_000, 0)),
        })
    } else if referral_count >= 20 && total_referred_volume >= Decimal::new(100_000, 0) {
        Some(ReferralTier {
            level: 2,
            name: "Silver".to_string(),
            commission_rate: Decimal::new(17, 2),
            rate_bps: 1700,
            next_tier_referrals: Some(50),
            next_tier_volume: Some(Decimal::new(500_000, 0)),
        })
    } else if referral_count >= 5 && total_referred_volume >= Decimal::new(10_000, 0) {
        Some(ReferralTier {
            level: 1,
            name: "Bronze".to_string(),
            commission_rate: Decimal::new(12, 2),
            rate_bps: 1200,
            next_tier_referrals: Some(20),
            next_tier_volume: Some(Decimal::new(100_000, 0)),
        })
    } else if referral_count >= 1 && total_referred_volume >= Decimal::new(1_000, 0) {
        Some(ReferralTier {
            level: 0,
            name: "Starter".to_string(),
            commission_rate: Decimal::new(10, 2),
            rate_bps: 1000,
            next_tier_referrals: Some(5),
            next_tier_volume: Some(Decimal::new(10_000, 0)),
        })
    } else {
        None
    }
}

/// Create a new referral code
/// POST /referral/code
pub async fn create_code(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Json(req): Json<CreateReferralCodeRequest>,
) -> Result<Json<CreateCodeResponse>, (StatusCode, Json<ErrorResponse>)> {
    // Validate timestamp
    if !validate_timestamp(req.timestamp) {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "时间戳已过期".to_string(),
                code: "TIMESTAMP_EXPIRED".to_string(),
            }),
        ));
    }

    // EIP-712 签名验证
    let create_msg = CreateReferralMessage {
        wallet: auth_user.address.to_lowercase(),
        timestamp: req.timestamp,
    };

    let valid = match verify_create_referral_signature(&create_msg, &req.signature, &auth_user.address) {
        Ok(v) => v,
        Err(e) => {
            tracing::error!("Create referral code signature verification error: {}", e);
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "签名格式无效".to_string(),
                    code: "INVALID_SIGNATURE_FORMAT".to_string(),
                }),
            ));
        }
    };

    if !valid {
        tracing::warn!("Create referral code signature verification failed for address: {}", auth_user.address);
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse {
                error: "创建推荐码签名验证失败".to_string(),
                code: "SIGNATURE_INVALID".to_string(),
            }),
        ));
    }

    tracing::info!("EIP-712 create referral code signature verified for address: {}", auth_user.address);

    // Check if user already has a referral code
    let existing: Option<String> = sqlx::query_scalar(
        "SELECT code FROM referral_codes WHERE owner_address = $1"
    )
    .bind(&auth_user.address.to_lowercase())
    .fetch_optional(&state.db.pool)
    .await
    .map_err(|e| {
        tracing::error!("Failed to check existing referral code: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "数据库查询失败".to_string(),
                code: "DB_ERROR".to_string(),
            }),
        )
    })?;

    if let Some(existing_code) = existing {
        return Err((
            StatusCode::CONFLICT,
            Json(ErrorResponse {
                error: format!("您已经有推荐码: {}", existing_code),
                code: "CODE_ALREADY_EXISTS".to_string(),
            }),
        ));
    }

    // Auto-generate referral code
    let code = generate_referral_code(&auth_user.address);

    let now = Utc::now();

    // Insert referral code (tier is calculated dynamically based on total_referrals)
    sqlx::query(
        r#"
        INSERT INTO referral_codes (id, code, owner_address, total_referrals, total_earnings, created_at)
        VALUES ($1, $2, $3, 0, 0, $4)
        "#
    )
    .bind(Uuid::new_v4())
    .bind(&code)
    .bind(&auth_user.address.to_lowercase())
    .bind(now)
    .execute(&state.db.pool)
    .await
    .map_err(|e| {
        tracing::error!("Failed to create referral code: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "创建推荐码失败".to_string(),
                code: "CREATE_FAILED".to_string(),
            }),
        )
    })?;

    // Update user record
    sqlx::query("UPDATE users SET referral_code = $1 WHERE address = $2")
        .bind(&code)
        .bind(&auth_user.address.to_lowercase())
        .execute(&state.db.pool)
        .await
        .ok();

    tracing::info!("Referral code created: {} for {}", code, auth_user.address);

    // Record log
    record_referral_log(
        &state.db.pool,
        &auth_user.address,
        "create_code",
        None,
        Some(&code),
        None,
        None,
        Some(&code),
        None,
    ).await;

    Ok(Json(CreateCodeResponse {
        success: true,
        code,
        created_at: now,
    }))
}

/// Bind to a referral code
/// POST /referral/bind
pub async fn bind_code(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Json(req): Json<BindReferralRequest>,
) -> Result<Json<BindCodeResponse>, (StatusCode, Json<ErrorResponse>)> {
    // Validate timestamp
    if !validate_timestamp(req.timestamp) {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "时间戳已过期".to_string(),
                code: "TIMESTAMP_EXPIRED".to_string(),
            }),
        ));
    }

    // EIP-712 签名验证
    let bind_msg = BindReferralMessage {
        wallet: auth_user.address.to_lowercase(),
        code: req.code.clone(),
        timestamp: req.timestamp,
    };

    let valid = match verify_bind_referral_signature(&bind_msg, &req.signature, &auth_user.address) {
        Ok(v) => v,
        Err(e) => {
            tracing::error!("Bind referral code signature verification error: {}", e);
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "签名格式无效".to_string(),
                    code: "INVALID_SIGNATURE_FORMAT".to_string(),
                }),
            ));
        }
    };

    if !valid {
        tracing::warn!("Bind referral code signature verification failed for address: {}", auth_user.address);
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse {
                error: "绑定推荐码签名验证失败".to_string(),
                code: "SIGNATURE_INVALID".to_string(),
            }),
        ));
    }

    tracing::info!("EIP-712 bind referral code signature verified for address: {}", auth_user.address);

    let user_addr = auth_user.address.to_lowercase();

    // Check if user already bound to a referrer (for rebind support)
    let existing_binding: Option<(String, String)> = sqlx::query_as(
        "SELECT referrer_address, code FROM referral_relations WHERE referee_address = $1"
    )
    .bind(&user_addr)
    .fetch_optional(&state.db.pool)
    .await
    .map_err(|e| {
        tracing::error!("Failed to check existing binding: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "数据库查询失败".to_string(),
                code: "DB_ERROR".to_string(),
            }),
        )
    })?;

    // Find referral code
    let referrer: Option<String> = sqlx::query_scalar(
        "SELECT owner_address FROM referral_codes WHERE UPPER(code) = UPPER($1)"
    )
    .bind(&req.code)
    .fetch_optional(&state.db.pool)
    .await
    .map_err(|e| {
        tracing::error!("Failed to find referral code: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "数据库查询失败".to_string(),
                code: "DB_ERROR".to_string(),
            }),
        )
    })?;

    let referrer_address = referrer.ok_or((
        StatusCode::NOT_FOUND,
        Json(ErrorResponse {
            error: "推荐码不存在".to_string(),
            code: "CODE_NOT_FOUND".to_string(),
        }),
    ))?;

    // Can't refer yourself
    if referrer_address.to_lowercase() == user_addr {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "不能使用自己的推荐码".to_string(),
                code: "SELF_REFERRAL".to_string(),
            }),
        ));
    }

    // Invited user must have registered AFTER the inviter
    let reg_times: Option<(chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)> =
        sqlx::query_as(
            r#"
            SELECT
                (SELECT created_at FROM users WHERE address = $1) AS inviter_reg,
                (SELECT created_at FROM users WHERE address = $2) AS invitee_reg
            "#,
        )
        .bind(&referrer_address.to_lowercase())
        .bind(&user_addr)
        .fetch_optional(&state.db.pool)
        .await
        .map_err(|e| {
            tracing::error!("Failed to query registration times: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "数据库查询失败".to_string(),
                    code: "DB_ERROR".to_string(),
                }),
            )
        })?;

    if let Some((inviter_reg, invitee_reg)) = reg_times {
        if invitee_reg <= inviter_reg {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "您的注册时间早于邀请人，无法绑定".to_string(),
                    code: "INVALID_REGISTRATION_ORDER".to_string(),
                }),
            ));
        }
    }

    // Check if trying to bind to the same referrer
    if let Some((existing_referrer, _)) = &existing_binding {
        if existing_referrer.to_lowercase() == referrer_address.to_lowercase() {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "您已绑定此推荐人".to_string(),
                    code: "SAME_REFERRER".to_string(),
                }),
            ));
        }
    }

    let now = Utc::now();
    let is_rebind = existing_binding.is_some();
    let old_referrer = existing_binding.as_ref().map(|(r, _)| r.clone());
    let old_code = existing_binding.as_ref().map(|(_, c)| c.clone());

    // Begin transaction
    let mut tx = state.db.pool.begin().await.map_err(|e| {
        tracing::error!("Failed to begin transaction: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "数据库事务失败".to_string(),
                code: "DB_ERROR".to_string(),
            }),
        )
    })?;

    // If rebinding, first remove old binding
    if is_rebind {
        // Delete old referral relation
        sqlx::query("DELETE FROM referral_relations WHERE referee_address = $1")
            .bind(&user_addr)
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                tracing::error!("Failed to delete old referral relation: {}", e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: "解除原绑定失败".to_string(),
                        code: "UNBIND_FAILED".to_string(),
                    }),
                )
            })?;

        // Decrement old referrer's count
        if let Some(ref old_c) = old_code {
            sqlx::query("UPDATE referral_codes SET total_referrals = GREATEST(0, total_referrals - 1) WHERE UPPER(code) = UPPER($1)")
                .bind(old_c)
                .execute(&mut *tx)
                .await
                .ok();
        }
    }

    // Create new referral relationship
    sqlx::query(
        r#"
        INSERT INTO referral_relations (id, referrer_address, referee_address, code, created_at)
        VALUES ($1, $2, $3, $4, $5)
        "#
    )
    .bind(Uuid::new_v4())
    .bind(&referrer_address)
    .bind(&user_addr)
    .bind(&req.code.to_uppercase())
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(|e| {
        tracing::error!("Failed to create referral relationship: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "绑定失败".to_string(),
                code: "BIND_FAILED".to_string(),
            }),
        )
    })?;

    // Update user record
    sqlx::query("UPDATE users SET referrer_address = $1 WHERE address = $2")
        .bind(&referrer_address)
        .bind(&user_addr)
        .execute(&mut *tx)
        .await
        .ok();

    // Update new referral code stats
    sqlx::query("UPDATE referral_codes SET total_referrals = total_referrals + 1 WHERE UPPER(code) = UPPER($1)")
        .bind(&req.code)
        .execute(&mut *tx)
        .await
        .ok();

    tx.commit().await.map_err(|e| {
        tracing::error!("Failed to commit transaction: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "事务提交失败".to_string(),
                code: "TX_FAILED".to_string(),
            }),
        )
    })?;

    let action = if is_rebind { "rebind" } else { "bind" };
    tracing::info!("Referral {}: {} bound to {} via code {}", action, user_addr, referrer_address, req.code);

    // Record log
    record_referral_log(
        &state.db.pool,
        &user_addr,
        action,
        Some(&referrer_address),
        Some(&req.code.to_uppercase()),
        None,
        old_referrer.as_deref(),
        Some(&referrer_address),
        None,
    ).await;

    Ok(Json(BindCodeResponse {
        success: true,
        referrer_address,
        referrer_code: req.code.to_uppercase(),
    }))
}

/// Get referral dashboard
/// GET /referral/dashboard
pub async fn get_dashboard(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
) -> Result<Json<DashboardResponse>, (StatusCode, Json<ErrorResponse>)> {
    // Get user's referral code
    let code: Option<String> = sqlx::query_scalar(
        "SELECT code FROM referral_codes WHERE owner_address = $1"
    )
    .bind(&auth_user.address.to_lowercase())
    .fetch_optional(&state.db.pool)
    .await
    .map_err(|e| {
        tracing::error!("Failed to fetch referral code: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "数据库查询失败".to_string(),
                code: "DB_ERROR".to_string(),
            }),
        )
    })?;

    // Get total and active referrals count
    let total_referrals: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM referral_relations WHERE referrer_address = $1"
    )
    .bind(&auth_user.address.to_lowercase())
    .fetch_one(&state.db.pool)
    .await
    .unwrap_or(0);

    // Active referrals (users who traded in last 30 days)
    let active_referrals: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(DISTINCT rr.referee_address)
        FROM referral_relations rr
        JOIN trades t ON (t.maker_address = rr.referee_address OR t.taker_address = rr.referee_address)
        WHERE rr.referrer_address = $1
        AND t.created_at > NOW() - INTERVAL '30 days'
        "#
    )
    .bind(&auth_user.address.to_lowercase())
    .fetch_one(&state.db.pool)
    .await
    .unwrap_or(0);

    // Get earnings summary
    let earnings: Option<(Decimal, Decimal)> = sqlx::query_as(
        r#"
        SELECT
            COALESCE(SUM(commission), 0) as total,
            COALESCE(SUM(CASE WHEN status = 'pending' THEN commission ELSE 0 END), 0) as pending
        FROM referral_earnings
        WHERE referrer_address = $1
        "#
    )
    .bind(&auth_user.address.to_lowercase())
    .fetch_optional(&state.db.pool)
    .await
    .map_err(|e| {
        tracing::error!("Failed to fetch earnings: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "数据库查询失败".to_string(),
                code: "DB_ERROR".to_string(),
            }),
        )
    })?;

    let (total_earnings, pending_earnings) = earnings.unwrap_or((Decimal::ZERO, Decimal::ZERO));
    let claimed_earnings = total_earnings - pending_earnings;

    // Total trading volume generated by all referred users
    let total_referred_volume: Decimal = sqlx::query_scalar(
        "SELECT COALESCE(SUM(volume), 0) FROM referral_earnings WHERE referrer_address = $1",
    )
    .bind(&auth_user.address.to_lowercase())
    .fetch_one(&state.db.pool)
    .await
    .unwrap_or(Decimal::ZERO);

    // Get recent activity
    let activity_rows: Vec<(String, String, Decimal, Decimal, DateTime<Utc>)> = sqlx::query_as(
        r#"
        SELECT
            re.referee_address,
            re.event_type,
            re.volume,
            re.commission,
            re.created_at
        FROM referral_earnings re
        WHERE re.referrer_address = $1
        ORDER BY re.created_at DESC
        LIMIT 20
        "#
    )
    .bind(&auth_user.address.to_lowercase())
    .fetch_all(&state.db.pool)
    .await
    .unwrap_or_default();

    let recent_activity: Vec<ReferralActivity> = activity_rows
        .into_iter()
        .map(|(addr, event_type, volume, commission, timestamp)| {
            ReferralActivity {
                referral_address: addr,
                event_type,
                volume,
                commission,
                timestamp,
            }
        })
        .collect();

    let tier = get_tier(total_referrals, total_referred_volume);
    let tier_note = if tier.is_none() {
        Some("未达到最低返佣门槛，需满足：≥1 位被推荐人 且 被推荐人累计交易量 ≥ $1,000".to_string())
    } else {
        None
    };

    // 查询用户使用的邀请码（自己绑定的上级邀请码）
    let bound_referral: Option<(String, String, DateTime<Utc>)> = sqlx::query_as(
        r#"
        SELECT code, referrer_address, created_at
        FROM referral_relations
        WHERE referee_address = $1
        "#
    )
    .bind(&auth_user.address.to_lowercase())
    .fetch_optional(&state.db.pool)
    .await
    .map_err(|e| {
        tracing::error!("Failed to fetch bound referral: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "数据库查询失败".to_string(),
                code: "DB_ERROR".to_string(),
            }),
        )
    })?;

    let bound_referral_info = bound_referral.map(|(code, referrer_address, bound_at)| {
        BoundReferralInfo {
            code,
            referrer_address,
            bound_at,
        }
    });

    Ok(Json(DashboardResponse {
        code,
        total_referrals,
        active_referrals,
        total_earnings,
        pending_earnings,
        claimed_earnings,
        total_referred_volume,
        tier,
        tier_note,
        recent_activity,
        bound_referral: bound_referral_info,
    }))
}

// ============================================
// On-Chain Referral Query Endpoints (Public)
// ============================================

/// Response for on-chain user rebate info query (from ReferralRebate.getUserRebateInfo)
#[derive(Debug, Serialize)]
pub struct OnChainUserRebateResponse {
    pub address: String,
    pub claimed_usd: String,
    pub nonce: u64,
    pub referral_code: String,
    pub referrer: String,
    pub tier_level: u8,
    pub tier_name: String,
}

/// Response for on-chain referral info query (from ReferralRebate.getReferralInfo)
#[derive(Debug, Serialize)]
pub struct OnChainReferralInfoResponse {
    pub address: String,
    pub code: String,
    pub referrer: String,
    pub total_rebate_bps: u16,
    pub trader_discount_bps: u16,
    pub affiliate_reward_bps: u16,
}

/// Response for claimed amount query
#[derive(Debug, Serialize)]
pub struct ClaimedAmountResponse {
    pub address: String,
    pub claimed_usd: String,
}

/// Response for claim signature request
#[derive(Debug, Serialize)]
pub struct ClaimSignatureResponse {
    pub amount: String,
    pub nonce: u64,
    pub deadline: u64,
    pub signature: String,
    pub contract_address: String,
}

/// Response for tier info query
#[derive(Debug, Serialize)]
#[allow(dead_code)]
pub struct TierInfoResponse {
    pub address: String,
    pub tier_index: u8,
    pub tier_name: String,
    pub referrer_rate_bps: u16,
    pub referee_discount_bps: u16,
}

/// Map on-chain tier index to display name.
fn get_tier_name_from_index(tier: u8) -> &'static str {
    match tier {
        0 => "Starter",
        1 => "Bronze",
        2 => "Silver",
        3 => "Gold",
        _ => "Diamond",
    }
}

/// Get on-chain user rebate info
/// GET /referral/on-chain/user-rebate/:address
pub async fn get_on_chain_user_rebate(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(address): axum::extract::Path<String>,
) -> Result<Json<OnChainUserRebateResponse>, (StatusCode, Json<ErrorResponse>)> {
    // Validate address format
    if !address.starts_with("0x") || address.len() != 42 {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "Invalid address format".to_string(),
                code: "INVALID_ADDRESS".to_string(),
            }),
        ));
    }

    let rebate_info = state.referral_service.get_user_rebate_info(&address)
        .await
        .map_err(|e| {
            tracing::error!("Failed to fetch on-chain user rebate for {}: {}", address, e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Failed to fetch on-chain data".to_string(),
                    code: "CHAIN_ERROR".to_string(),
                }),
            )
        })?;

    Ok(Json(OnChainUserRebateResponse {
        address: address.to_lowercase(),
        claimed_usd: rebate_info.claimed.to_string(),
        nonce: rebate_info.nonce,
        referral_code: rebate_info.referral_code,
        referrer: rebate_info.referrer,
        tier_level: rebate_info.tier_level,
        tier_name: get_tier_name_from_index(rebate_info.tier_level).to_string(),
    }))
}

/// Get on-chain referral info for a trader
/// GET /referral/on-chain/referral-info/:address
pub async fn get_on_chain_referral_info(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(address): axum::extract::Path<String>,
) -> Result<Json<OnChainReferralInfoResponse>, (StatusCode, Json<ErrorResponse>)> {
    // Validate address format
    if !address.starts_with("0x") || address.len() != 42 {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "Invalid address format".to_string(),
                code: "INVALID_ADDRESS".to_string(),
            }),
        ));
    }

    let referral_info = state.referral_service.get_referral_info(&address)
        .await
        .map_err(|e| {
            tracing::error!("Failed to fetch on-chain referral info for {}: {}", address, e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Failed to fetch on-chain data".to_string(),
                    code: "CHAIN_ERROR".to_string(),
                }),
            )
        })?;

    Ok(Json(OnChainReferralInfoResponse {
        address: address.to_lowercase(),
        code: referral_info.code,
        referrer: referral_info.referrer,
        total_rebate_bps: referral_info.total_rebate_bps,
        trader_discount_bps: referral_info.trader_discount_bps,
        affiliate_reward_bps: referral_info.affiliate_reward_bps,
    }))
}

/// Get on-chain claimed rebate amount for a user
/// GET /referral/on-chain/claimed/:address
pub async fn get_on_chain_claimed(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(address): axum::extract::Path<String>,
) -> Result<Json<ClaimedAmountResponse>, (StatusCode, Json<ErrorResponse>)> {
    // Validate address format
    if !address.starts_with("0x") || address.len() != 42 {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "Invalid address format".to_string(),
                code: "INVALID_ADDRESS".to_string(),
            }),
        ));
    }

    let claimed = state.referral_service.get_claimed_rebates(&address)
        .await
        .map_err(|e| {
            tracing::error!("Failed to fetch on-chain claimed for {}: {}", address, e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Failed to fetch on-chain data".to_string(),
                    code: "CHAIN_ERROR".to_string(),
                }),
            )
        })?;

    Ok(Json(ClaimedAmountResponse {
        address: address.to_lowercase(),
        claimed_usd: claimed.to_string(),
    }))
}

/// Check operator status for the backend signer
/// GET /referral/on-chain/operator-status
pub async fn get_operator_status(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    // Get the backend signer address from config
    let backend_signer = &state.config.backend_signer_private_key;

    // Parse private key to get address
    use ethers::signers::{LocalWallet, Signer};
    let wallet: LocalWallet = backend_signer.parse().map_err(|e| {
        tracing::error!("Failed to parse backend signer private key: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "Configuration error".to_string(),
                code: "CONFIG_ERROR".to_string(),
            }),
        )
    })?;

    let signer_address = format!("{:#x}", wallet.address());

    let is_operator = state.referral_service.check_operator_status(&signer_address)
        .await
        .map_err(|e| {
            tracing::error!("Failed to check operator status: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Failed to check operator status".to_string(),
                    code: "CHAIN_ERROR".to_string(),
                }),
            )
        })?;

    Ok(Json(serde_json::json!({
        "operator_address": signer_address,
        "is_operator": is_operator,
        "contract_address": state.config.referral_rebate_address,
    })))
}

/// Request for on-chain claim signature
#[derive(Debug, serde::Deserialize)]
pub struct OnChainClaimRequest {
    pub amount: String,  // Amount in USDT (e.g., "100.50")
}

/// Get signature for on-chain rebate claim
/// POST /referral/on-chain/claim-signature
pub async fn get_claim_signature(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Json(req): Json<OnChainClaimRequest>,
) -> Result<Json<ClaimSignatureResponse>, (StatusCode, Json<ErrorResponse>)> {
    // Parse amount
    let amount: Decimal = req.amount.parse().map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "Invalid amount format".to_string(),
                code: "INVALID_AMOUNT".to_string(),
            }),
        )
    })?;

    if amount <= Decimal::ZERO {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "Amount must be positive".to_string(),
                code: "INVALID_AMOUNT".to_string(),
            }),
        ));
    }

    // Generate EIP-712 signature (1 hour deadline)
    let result = state.referral_service
        .generate_claim_signature(&auth_user.address, amount, 3600)
        .await
        .map_err(|e| {
            tracing::error!("Failed to generate claim signature: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Failed to generate signature".to_string(),
                    code: "SIGNATURE_ERROR".to_string(),
                }),
            )
        })?;

    tracing::info!(
        "Generated claim signature for user={}, amount={}",
        auth_user.address,
        amount
    );

    Ok(Json(ClaimSignatureResponse {
        amount: result.amount,
        nonce: result.nonce,
        deadline: result.deadline,
        signature: result.signature,
        contract_address: result.contract_address,
    }))
}

/// Claim referral earnings (on-chain signature mode)
/// POST /referral/claim
pub async fn claim_earnings(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let address = auth_user.address.to_lowercase();

    // Query claimable amount: only synced AND unclaimed earnings
    let claimable: Decimal = sqlx::query_scalar(
        "SELECT COALESCE(SUM(commission), 0) FROM referral_earnings WHERE referrer_address = $1 AND chain_sync_status = 'synced' AND status = 'pending'"
    )
    .bind(&address)
    .fetch_one(&state.db.pool)
    .await
    .unwrap_or(Decimal::ZERO);

    if claimable <= Decimal::ZERO {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "没有可领取的佣金（请等待链上同步完成）".to_string(),
                code: "NO_CLAIMABLE".to_string(),
            }),
        ));
    }

    // Minimum claim amount
    let collateral_symbol = state.config.collateral_symbol();
    let min_claim = Decimal::new(10, 0); // 10 minimum
    if claimable < min_claim {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: format!("最低领取金额为 {} {}", min_claim, collateral_symbol),
                code: "BELOW_MINIMUM".to_string(),
            }),
        ));
    }

    // Generate EIP-712 signature (1 hour deadline)
    let result = state.referral_service
        .generate_claim_signature(&address, claimable, 3600)
        .await
        .map_err(|e| {
            tracing::error!("Failed to generate claim signature: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "生成签名失败".to_string(),
                    code: "SIGNATURE_ERROR".to_string(),
                }),
            )
        })?;

    tracing::info!(
        "Generated claim signature for user={}, amount={}",
        address,
        claimable
    );

    // Record log
    record_referral_log(
        &state.db.pool,
        &address,
        "claim",
        None,
        None,
        Some(claimable),
        None,
        None,
        None,
    ).await;

    Ok(Json(serde_json::json!({
        "success": true,
        "amount": result.amount,
        "nonce": result.nonce,
        "deadline": result.deadline,
        "signature": result.signature,
        "contract_address": result.contract_address,
        "message": "请使用返回的签名调用合约 redeemReward() 方法完成链上领取"
    })))
}

// ============================================
// User Referral Status Endpoint
// ============================================

/// Referee info in status response
#[derive(Debug, Serialize)]
pub struct RefereeInfo {
    pub address: String,
    #[serde(serialize_with = "datetime_as_millis::serialize")]
    pub bound_at: DateTime<Utc>,
}

/// As referrer status (I invited others)
#[derive(Debug, Serialize)]
pub struct AsReferrerStatus {
    pub has_code: bool,
    pub code: Option<String>,
    #[serde(serialize_with = "serialize_option_datetime")]
    pub code_created_at: Option<DateTime<Utc>>,
    pub total_referrals: i64,
    pub referees: Vec<RefereeInfo>,
}

/// As referee status (I was invited by someone)
#[derive(Debug, Serialize)]
pub struct AsRefereeStatus {
    pub is_bound: bool,
    pub referrer_address: Option<String>,
    pub referrer_code: Option<String>,
    #[serde(serialize_with = "serialize_option_datetime")]
    pub bound_at: Option<DateTime<Utc>>,
}

/// Complete referral status response
#[derive(Debug, Serialize)]
pub struct ReferralStatusResponse {
    pub as_referrer: AsReferrerStatus,
    pub as_referee: AsRefereeStatus,
}

/// Helper to serialize Option<DateTime> as milliseconds
fn serialize_option_datetime<S>(dt: &Option<DateTime<Utc>>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match dt {
        Some(d) => serializer.serialize_some(&d.timestamp_millis()),
        None => serializer.serialize_none(),
    }
}

/// Get user's complete referral status
/// GET /referral/status
pub async fn get_status(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
) -> Result<Json<ReferralStatusResponse>, (StatusCode, Json<ErrorResponse>)> {
    let user_addr = auth_user.address.to_lowercase();

    // Get user's own referral code info
    let code_info: Option<(String, DateTime<Utc>)> = sqlx::query_as(
        "SELECT code, created_at FROM referral_codes WHERE owner_address = $1"
    )
    .bind(&user_addr)
    .fetch_optional(&state.db.pool)
    .await
    .map_err(|e| {
        tracing::error!("Failed to fetch referral code: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "Database error".to_string(),
                code: "DB_ERROR".to_string(),
            }),
        )
    })?;

    // Get total referrals count
    let total_referrals: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM referral_relations WHERE referrer_address = $1"
    )
    .bind(&user_addr)
    .fetch_one(&state.db.pool)
    .await
    .unwrap_or(0);

    // Get recent referees (limit 50)
    let referees_rows: Vec<(String, DateTime<Utc>)> = sqlx::query_as(
        r#"
        SELECT referee_address, created_at
        FROM referral_relations
        WHERE referrer_address = $1
        ORDER BY created_at DESC
        LIMIT 50
        "#
    )
    .bind(&user_addr)
    .fetch_all(&state.db.pool)
    .await
    .unwrap_or_default();

    let referees: Vec<RefereeInfo> = referees_rows
        .into_iter()
        .map(|(address, bound_at)| RefereeInfo { address, bound_at })
        .collect();

    // Get user's referrer info (who invited me)
    let referrer_info: Option<(String, String, DateTime<Utc>)> = sqlx::query_as(
        "SELECT referrer_address, code, created_at FROM referral_relations WHERE referee_address = $1"
    )
    .bind(&user_addr)
    .fetch_optional(&state.db.pool)
    .await
    .map_err(|e| {
        tracing::error!("Failed to fetch referrer info: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "Database error".to_string(),
                code: "DB_ERROR".to_string(),
            }),
        )
    })?;

    let as_referrer = AsReferrerStatus {
        has_code: code_info.is_some(),
        code: code_info.as_ref().map(|(c, _)| c.clone()),
        code_created_at: code_info.map(|(_, t)| t),
        total_referrals,
        referees,
    };

    let as_referee = match referrer_info {
        Some((referrer_address, referrer_code, bound_at)) => AsRefereeStatus {
            is_bound: true,
            referrer_address: Some(referrer_address),
            referrer_code: Some(referrer_code),
            bound_at: Some(bound_at),
        },
        None => AsRefereeStatus {
            is_bound: false,
            referrer_address: None,
            referrer_code: None,
            bound_at: None,
        },
    };

    Ok(Json(ReferralStatusResponse {
        as_referrer,
        as_referee,
    }))
}

// ============================================
// Unbind Referral Code Endpoint
// ============================================

/// Request for unbinding referral code
#[derive(Debug, serde::Deserialize)]
pub struct UnbindReferralRequest {
    pub signature: String,
    pub timestamp: u64,
}

/// Response for unbind operation
#[derive(Debug, Serialize)]
pub struct UnbindResponse {
    pub success: bool,
    pub previous_referrer: String,
    pub previous_code: String,
}

/// Unbind from current referrer
/// POST /referral/unbind
pub async fn unbind_code(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Json(req): Json<UnbindReferralRequest>,
) -> Result<Json<UnbindResponse>, (StatusCode, Json<ErrorResponse>)> {
    // Validate timestamp
    if !validate_timestamp(req.timestamp) {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "Timestamp expired".to_string(),
                code: "TIMESTAMP_EXPIRED".to_string(),
            }),
        ));
    }

    // Use the same EIP-712 signature verification as create_code (wallet + timestamp)
    let unbind_msg = CreateReferralMessage {
        wallet: auth_user.address.to_lowercase(),
        timestamp: req.timestamp,
    };

    let valid = match verify_create_referral_signature(&unbind_msg, &req.signature, &auth_user.address) {
        Ok(v) => v,
        Err(e) => {
            tracing::error!("Unbind signature verification error: {}", e);
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "Invalid signature format".to_string(),
                    code: "INVALID_SIGNATURE_FORMAT".to_string(),
                }),
            ));
        }
    };

    if !valid {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse {
                error: "Signature verification failed".to_string(),
                code: "SIGNATURE_INVALID".to_string(),
            }),
        ));
    }

    let user_addr = auth_user.address.to_lowercase();

    // Check if user is bound to someone
    let current_binding: Option<(String, String)> = sqlx::query_as(
        "SELECT referrer_address, code FROM referral_relations WHERE referee_address = $1"
    )
    .bind(&user_addr)
    .fetch_optional(&state.db.pool)
    .await
    .map_err(|e| {
        tracing::error!("Failed to check binding: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "Database error".to_string(),
                code: "DB_ERROR".to_string(),
            }),
        )
    })?;

    let (previous_referrer, previous_code) = current_binding.ok_or((
        StatusCode::BAD_REQUEST,
        Json(ErrorResponse {
            error: "You are not bound to any referrer".to_string(),
            code: "NOT_BOUND".to_string(),
        }),
    ))?;

    // Begin transaction
    let mut tx = state.db.pool.begin().await.map_err(|e| {
        tracing::error!("Failed to begin transaction: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "Database error".to_string(),
                code: "DB_ERROR".to_string(),
            }),
        )
    })?;

    // Delete referral relation
    sqlx::query("DELETE FROM referral_relations WHERE referee_address = $1")
        .bind(&user_addr)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            tracing::error!("Failed to delete referral relation: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Failed to unbind".to_string(),
                    code: "UNBIND_FAILED".to_string(),
                }),
            )
        })?;

    // Update user record
    sqlx::query("UPDATE users SET referrer_address = NULL WHERE address = $1")
        .bind(&user_addr)
        .execute(&mut *tx)
        .await
        .ok();

    // Decrement referral code total_referrals
    sqlx::query("UPDATE referral_codes SET total_referrals = GREATEST(0, total_referrals - 1) WHERE UPPER(code) = UPPER($1)")
        .bind(&previous_code)
        .execute(&mut *tx)
        .await
        .ok();

    tx.commit().await.map_err(|e| {
        tracing::error!("Failed to commit transaction: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "Transaction failed".to_string(),
                code: "TX_FAILED".to_string(),
            }),
        )
    })?;

    tracing::info!(
        "Referral unbind: {} unbound from {} (code: {})",
        user_addr,
        previous_referrer,
        previous_code
    );

    // Record log
    record_referral_log(
        &state.db.pool,
        &user_addr,
        "unbind",
        Some(&previous_referrer),
        Some(&previous_code),
        None,
        Some(&previous_referrer),
        None,
        None,
    ).await;

    Ok(Json(UnbindResponse {
        success: true,
        previous_referrer,
        previous_code,
    }))
}

// ============================================
// Referral Logs Query Endpoint
// ============================================

/// Referral log entry
#[derive(Debug, Serialize)]
pub struct ReferralLogEntry {
    pub id: String,
    pub action: String,
    pub target_address: Option<String>,
    pub referral_code: Option<String>,
    pub amount: Option<Decimal>,
    pub old_value: Option<String>,
    pub new_value: Option<String>,
    #[serde(serialize_with = "datetime_as_millis::serialize")]
    pub created_at: DateTime<Utc>,
}

/// Referral logs response
#[derive(Debug, Serialize)]
pub struct ReferralLogsResponse {
    pub logs: Vec<ReferralLogEntry>,
    pub total: i64,
    pub page: i32,
    pub page_size: i32,
}

/// Query parameters for logs
#[derive(Debug, serde::Deserialize)]
pub struct LogsQueryParams {
    #[serde(default = "default_page")]
    pub page: i32,
    #[serde(default = "default_page_size")]
    pub page_size: i32,
    pub action: Option<String>,
}

fn default_page() -> i32 { 1 }
fn default_page_size() -> i32 { 20 }

/// Get user's referral operation logs
/// GET /referral/logs
pub async fn get_logs(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    axum::extract::Query(params): axum::extract::Query<LogsQueryParams>,
) -> Result<Json<ReferralLogsResponse>, (StatusCode, Json<ErrorResponse>)> {
    let user_addr = auth_user.address.to_lowercase();
    let page = params.page.max(1);
    let page_size = params.page_size.clamp(1, 100);
    let offset = (page - 1) * page_size;

    // Build query based on action filter
    let (logs_query, count_query) = if let Some(ref _action) = params.action {
        (
            format!(
                r#"
                SELECT id, action, target_address, referral_code, amount, old_value, new_value, created_at
                FROM referral_logs
                WHERE user_address = $1 AND action = $2
                ORDER BY created_at DESC
                LIMIT $3 OFFSET $4
                "#
            ),
            "SELECT COUNT(*) FROM referral_logs WHERE user_address = $1 AND action = $2".to_string(),
        )
    } else {
        (
            r#"
            SELECT id, action, target_address, referral_code, amount, old_value, new_value, created_at
            FROM referral_logs
            WHERE user_address = $1
            ORDER BY created_at DESC
            LIMIT $2 OFFSET $3
            "#.to_string(),
            "SELECT COUNT(*) FROM referral_logs WHERE user_address = $1".to_string(),
        )
    };

    // Fetch logs
    let logs_rows: Vec<(Uuid, String, Option<String>, Option<String>, Option<Decimal>, Option<String>, Option<String>, DateTime<Utc>)> =
        if let Some(ref action) = params.action {
            sqlx::query_as(&logs_query)
                .bind(&user_addr)
                .bind(action)
                .bind(page_size)
                .bind(offset)
                .fetch_all(&state.db.pool)
                .await
        } else {
            sqlx::query_as(&logs_query)
                .bind(&user_addr)
                .bind(page_size)
                .bind(offset)
                .fetch_all(&state.db.pool)
                .await
        }
        .map_err(|e| {
            tracing::error!("Failed to fetch logs: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Database error".to_string(),
                    code: "DB_ERROR".to_string(),
                }),
            )
        })?;

    // Fetch total count
    let total: i64 = if let Some(ref action) = params.action {
        sqlx::query_scalar(&count_query)
            .bind(&user_addr)
            .bind(action)
            .fetch_one(&state.db.pool)
            .await
    } else {
        sqlx::query_scalar(&count_query)
            .bind(&user_addr)
            .fetch_one(&state.db.pool)
            .await
    }
    .unwrap_or(0);

    let logs: Vec<ReferralLogEntry> = logs_rows
        .into_iter()
        .map(|(id, action, target_address, referral_code, amount, old_value, new_value, created_at)| {
            ReferralLogEntry {
                id: id.to_string(),
                action,
                target_address,
                referral_code,
                amount,
                old_value,
                new_value,
                created_at,
            }
        })
        .collect();

    Ok(Json(ReferralLogsResponse {
        logs,
        total,
        page,
        page_size,
    }))
}

// ============================================
// Helper: Record Referral Log
// ============================================

/// Record a referral operation log
#[allow(clippy::too_many_arguments)]
async fn record_referral_log(
    pool: &sqlx::PgPool,
    user_address: &str,
    action: &str,
    target_address: Option<&str>,
    referral_code: Option<&str>,
    amount: Option<Decimal>,
    old_value: Option<&str>,
    new_value: Option<&str>,
    metadata: Option<serde_json::Value>,
) {
    let result = sqlx::query(
        r#"
        INSERT INTO referral_logs (user_address, action, target_address, referral_code, amount, old_value, new_value, metadata)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        "#
    )
    .bind(user_address.to_lowercase())
    .bind(action)
    .bind(target_address)
    .bind(referral_code)
    .bind(amount)
    .bind(old_value)
    .bind(new_value)
    .bind(metadata)
    .execute(pool)
    .await;

    if let Err(e) = result {
        tracing::warn!("Failed to record referral log: {}", e);
    }
}

// ============================================
// Referral Commission Leaderboard
// ============================================

/// One entry in the leaderboard response.
#[derive(Debug, Serialize)]
pub struct CommissionLeaderboardEntry {
    pub rank: i32,
    pub referrer_address: String,
    pub total_commission: Decimal,
    #[serde(serialize_with = "datetime_as_millis::serialize")]
    pub computed_at: DateTime<Utc>,
}

#[derive(Debug, serde::Deserialize)]
pub struct LeaderboardQuery {
    /// Number of top entries to return (1–50, default 10)
    #[serde(default = "default_lb_n")]
    pub n: i32,
}
fn default_lb_n() -> i32 { 10 }

/// Get top-N referral commission leaderboard
/// GET /referral/leaderboard?n=<1-50>
pub async fn get_commission_leaderboard(
    State(state): State<Arc<AppState>>,
    axum::extract::Query(params): axum::extract::Query<LeaderboardQuery>,
) -> Result<Json<Vec<CommissionLeaderboardEntry>>, (StatusCode, Json<ErrorResponse>)> {
    if params.n < 1 || params.n > 50 {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "n 必须在 1-50 之间".to_string(),
                code: "INVALID_PARAM".to_string(),
            }),
        ));
    }
    let n = params.n;

    let rows: Vec<(String, Decimal, DateTime<Utc>)> = sqlx::query_as(
        r#"
        SELECT referrer_address, total_commission, computed_at
        FROM referral_commission_leaderboard
        ORDER BY total_commission DESC
        LIMIT $1
        "#,
    )
    .bind(n)
    .fetch_all(&state.db.pool)
    .await
    .map_err(|e| {
        tracing::error!("Failed to fetch referral leaderboard: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "数据库查询失败".to_string(),
                code: "DB_ERROR".to_string(),
            }),
        )
    })?;

    let entries = rows
        .into_iter()
        .enumerate()
        .map(|(i, (referrer_address, total_commission, computed_at))| {
            CommissionLeaderboardEntry {
                rank: i as i32 + 1,
                referrer_address,
                total_commission,
                computed_at,
            }
        })
        .collect();

    Ok(Json(entries))
}

/// POST /referral/on-chain/update-backend-signer
/// Admin endpoint to update contract authorization signer.
/// Requires X-API-Key header (same as admin API) and CONTRACT_OWNER_KEY env var.
pub async fn update_backend_signer(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    let new_signer = payload.get("new_signer")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "Missing new_signer field".to_string(),
                    code: "INVALID_REQUEST".to_string(),
                }),
            )
        })?;

    // Read owner key from environment — never hardcode private keys
    let owner_key = std::env::var("REFERRAL_REBATE_CONTRACT_OWNER_KEY").map_err(|_| {
        tracing::error!("CONTRACT_OWNER_KEY not set in environment");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "Server not configured for this operation".to_string(),
                code: "NOT_CONFIGURED".to_string(),
            }),
        )
    })?;

    match state.referral_service.update_authorization_signer(&state.config.rpc_url, state.config.chain_id, &owner_key, new_signer).await {
        Ok(tx_hash) => {
            Ok(Json(serde_json::json!({
                "success": true,
                "tx_hash": tx_hash,
                "new_backend_signer": new_signer
            })))
        }
        Err(e) => {
            tracing::error!("Failed to update authorization signer: {}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("Failed to update: {}", e),
                    code: "UPDATE_FAILED".to_string(),
                }),
            ))
        }
    }
}

