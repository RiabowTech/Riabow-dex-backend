//! Earn Service Data Models
//!
//! Data structures for the ZtdxTermYield earn plans (理财服务).

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

// ============================================
// ENUMS
// ============================================

/// Product lifecycle status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "earn_product_status", rename_all = "lowercase")]
#[serde(rename_all = "lowercase")]
pub enum EarnProductStatus {
    Created,      // 产品已创建，等待开放申购
    Subscribing,  // 申购期，用户可以申购
    Active,       // 申购结束，产品运行中
    Settled,      // 已结算，用户可领取本息
    Ended,        // 用户申领完成
    Cancelled,    // 已取消（紧急情况）
}

impl EarnProductStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Subscribing => "subscribing",
            Self::Active => "active",
            Self::Settled => "settled",
            Self::Ended => "ended",
            Self::Cancelled => "cancelled",
        }
    }
}

/// NFT lifecycle status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "earn_nft_status", rename_all = "lowercase")]
#[serde(rename_all = "lowercase")]
pub enum EarnNftStatus {
    Created,   // NFT record created (pending on-chain)
    Active,    // NFT minted, product active
    Matured,   // Product matured, ready for claim
    Redeemed,  // NFT burned, funds claimed
}

impl EarnNftStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Active => "active",
            Self::Matured => "matured",
            Self::Redeemed => "redeemed",
        }
    }
}

// ============================================
// DATABASE MODELS
// ============================================

/// Earn Plan (ZtdxTermYield plan record)
#[derive(Debug, Clone, FromRow, Serialize)]
pub struct EarnProduct {
    pub id: Uuid,
    pub chain_product_id: i64,
    pub contract_address: String,
    pub name: String,
    pub description: Option<String>,
    pub annual_rate_bps: i32,
    pub duration_seconds: i64,
    pub period_rate_bps: i32,
    pub total_quota: Decimal,
    pub min_amount: Decimal,
    pub max_amount_per_user: Decimal,
    pub subscribed_amount: Decimal,
    pub subscribe_start_time: DateTime<Utc>,
    pub subscribe_end_time: DateTime<Utc>,
    pub settle_time: DateTime<Utc>,
    pub status: EarnProductStatus,
    pub subscriber_count: i32,
    pub total_interest_paid: Decimal,
    pub creator_address: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Earn Subscription (申购记录)
#[derive(Debug, Clone, FromRow, Serialize)]
pub struct EarnSubscription {
    pub id: Uuid,
    pub product_id: Uuid,
    pub chain_product_id: i64,
    pub user_address: String,
    pub amount: Decimal,
    pub nft_amount: Decimal,
    pub expected_return: Decimal,
    pub actual_return: Option<Decimal>,
    pub nft_status: EarnNftStatus,
    pub subscribed_at: DateTime<Utc>,
    pub settled_at: Option<DateTime<Utc>>,
    pub claimed_at: Option<DateTime<Utc>>,
    pub subscribe_tx_hash: Option<String>,
    pub claim_tx_hash: Option<String>,
    pub claimed: bool,
}

/// Earn Settlement (结算记录)
#[derive(Debug, Clone, FromRow, Serialize)]
pub struct EarnSettlement {
    pub id: Uuid,
    pub product_id: Uuid,
    pub chain_product_id: i64,
    pub total_principal: Decimal,
    pub total_interest: Decimal,
    pub settled_count: i32,
    pub tx_hash: Option<String>,
    pub block_number: Option<i64>,
    pub settled_at: DateTime<Utc>,
}

/// Earn Subscribe Signature (签名记录)
#[derive(Debug, Clone, FromRow, Serialize)]
pub struct EarnSubscribeSignature {
    pub id: Uuid,
    pub user_address: String,
    pub product_id: Uuid,
    pub chain_product_id: i64,
    pub amount: Decimal,
    pub deadline: i64,
    pub signature: String,
    pub used: bool,
    pub used_at: Option<DateTime<Utc>>,
    pub used_tx_hash: Option<String>,
    pub created_at: DateTime<Utc>,
}

// ============================================
// API REQUEST/RESPONSE MODELS
// ============================================

/// Product list query parameters
#[derive(Debug, Deserialize)]
pub struct ProductListQuery {
    pub status: Option<String>,
    pub page: Option<i32>,
    pub page_size: Option<i32>,
}

/// Product list response
#[derive(Debug, Serialize)]
pub struct ProductListResponse {
    pub products: Vec<ProductDetail>,
    pub total: i64,
    pub page: i32,
    pub page_size: i32,
}

/// Product detail for API response (with computed fields)
#[derive(Debug, Serialize)]
pub struct ProductDetail {
    pub id: String,
    pub chain_product_id: i64,
    pub name: String,
    pub description: Option<String>,

    // Rate info (formatted)
    pub annual_rate: String,         // e.g., "190.36%"
    pub period_rate: String,         // e.g., "3.65%"
    pub annual_rate_bps: i32,
    pub period_rate_bps: i32,
    pub duration_seconds: i64,

    // Quota info
    pub total_quota: String,
    pub subscribed_amount: String,
    pub available_quota: String,
    pub subscription_rate: String,   // e.g., "75.00%"
    pub min_amount: String,
    pub max_amount_per_user: String,

    // Status
    pub status: EarnProductStatus,
    pub subscriber_count: i32,

    // Time info
    pub subscribe_start_time: DateTime<Utc>,
    pub subscribe_end_time: DateTime<Utc>,
    pub settle_time: DateTime<Utc>,

    // Computed status flags
    pub is_subscribing: bool,
    pub is_sold_out: bool,
    pub time_remaining_seconds: Option<i64>,
}

impl ProductDetail {
    pub fn from_product(product: EarnProduct) -> Self {
        let now = Utc::now();
        let available_quota = product.total_quota - product.subscribed_amount;
        let subscription_rate = if product.total_quota > Decimal::ZERO {
            (product.subscribed_amount / product.total_quota) * Decimal::from(100)
        } else {
            Decimal::ZERO
        };

        let is_subscribing = product.status == EarnProductStatus::Subscribing
            && now >= product.subscribe_start_time
            && now < product.subscribe_end_time;

        let is_sold_out = available_quota <= Decimal::ZERO;

        let time_remaining_seconds = if is_subscribing {
            Some((product.subscribe_end_time - now).num_seconds().max(0))
        } else {
            None
        };

        Self {
            id: product.id.to_string(),
            chain_product_id: product.chain_product_id,
            name: product.name,
            description: product.description,
            annual_rate: format!("{:.2}%", Decimal::from(product.annual_rate_bps) / Decimal::from(100)),
            period_rate: format!("{:.2}%", Decimal::from(product.period_rate_bps) / Decimal::from(100)),
            annual_rate_bps: product.annual_rate_bps,
            period_rate_bps: product.period_rate_bps,
            duration_seconds: product.duration_seconds,
            total_quota: format!("{:.2}", product.total_quota),
            subscribed_amount: format!("{:.2}", product.subscribed_amount),
            available_quota: format!("{:.2}", available_quota),
            subscription_rate: format!("{:.2}%", subscription_rate),
            min_amount: format!("{:.2}", product.min_amount),
            max_amount_per_user: format!("{:.2}", product.max_amount_per_user),
            status: product.status,
            subscriber_count: product.subscriber_count,
            subscribe_start_time: product.subscribe_start_time,
            subscribe_end_time: product.subscribe_end_time,
            settle_time: product.settle_time,
            is_subscribing,
            is_sold_out,
            time_remaining_seconds,
        }
    }
}

/// User subscription detail
#[derive(Debug, Serialize)]
pub struct UserSubscriptionDetail {
    pub id: String,
    pub product_id: String,
    pub product_name: String,
    pub chain_product_id: i64,

    // Amount info
    pub amount: String,
    pub nft_amount: String,
    pub expected_return: String,
    pub actual_return: Option<String>,
    pub total_return: String,         // principal + interest

    // Status
    pub nft_status: EarnNftStatus,
    pub claimed: bool,
    pub product_status: EarnProductStatus,

    // Rates
    pub annual_rate: String,
    pub period_rate: String,

    // Time
    pub subscribed_at: DateTime<Utc>,
    pub settle_time: DateTime<Utc>,
    pub settled_at: Option<DateTime<Utc>>,
    pub claimed_at: Option<DateTime<Utc>>,

    // TX
    pub subscribe_tx_hash: Option<String>,
    pub claim_tx_hash: Option<String>,
}

/// Prepare join-plan request
#[derive(Debug, Deserialize)]
pub struct PrepareJoinPlanRequest {
    pub product_id: String,
    pub amount: String,
}

/// Prepare join-plan response (signature for on-chain tx)
#[derive(Debug, Serialize)]
pub struct PrepareJoinPlanResponse {
    pub chain_product_id: i64,
    pub amount: String,           // Wei amount (6 decimals for USDT)
    pub deadline: u64,            // Unix timestamp
    pub signature: String,        // EIP-712 signature
    pub contract_address: String,
    pub user_address: String,
}

/// Historical performance record
#[derive(Debug, Serialize, Deserialize)]
pub struct HistoricalPerformance {
    pub product_name: String,
    pub duration_seconds: i64,
    pub annual_rate: String,
    pub period_rate: String,
    pub total_subscribed: String,
    pub total_interest_paid: String,
    pub subscriber_count: i32,
    pub settled_at: DateTime<Utc>,
}

// ============================================
// ADMIN MODELS
// ============================================

/// Create product request (admin)
#[derive(Debug, Deserialize)]
pub struct CreateProductRequest {
    pub name: String,
    pub description: Option<String>,
    pub annual_rate_bps: i32,
    pub duration_seconds: i64,
    pub total_quota: String,
    pub min_amount: String,
    pub max_amount_per_user: String,
    pub subscribe_start_time: DateTime<Utc>,
    pub subscribe_end_time: DateTime<Utc>,
}

/// Update product status request (admin)
#[derive(Debug, Deserialize)]
pub struct UpdateProductStatusRequest {
    pub status: String,
}

/// Admin subscription query
#[derive(Debug, Deserialize)]
pub struct AdminSubscriptionQuery {
    pub page: Option<i32>,
    pub page_size: Option<i32>,
    pub user_address: Option<String>,
    pub claimed: Option<bool>,
}

/// Admin subscription list response
#[derive(Debug, Serialize)]
pub struct AdminSubscriptionListResponse {
    pub subscriptions: Vec<EarnSubscription>,
    pub total: i64,
    pub page: i32,
    pub page_size: i32,
    pub total_amount: String,
    pub total_expected_return: String,
}

// ============================================
// CONTRACT EVENT MODELS
// ============================================

/// PlanJoined event from ZtdxTermYield contract
#[derive(Debug, Clone)]
pub struct PlanJoinedEvent {
    pub product_id: u64,
    pub user: String,
    pub amount: Decimal,
    pub nft_amount: Decimal,
    pub expected_return: Decimal,
    pub tx_hash: String,
    pub block_number: u64,
    pub log_index: u64,
}

/// PlanClosed event from ZtdxTermYield contract
#[derive(Debug, Clone)]
pub struct PlanClosedEvent {
    pub product_id: u64,
    pub total_principal: Decimal,
    pub total_interest: Decimal,
    pub tx_hash: String,
    pub block_number: u64,
    pub log_index: u64,
}

/// PlanRedeemed event from ZtdxTermYield contract
#[derive(Debug, Clone)]
pub struct PlanRedeemedEvent {
    pub product_id: u64,
    pub user: String,
    pub principal: Decimal,
    pub interest: Decimal,
    pub tx_hash: String,
    pub block_number: u64,
}

/// PlanCreated event from ZtdxTermYield contract
#[derive(Debug, Clone)]
pub struct PlanCreatedEvent {
    pub product_id: u64,
    pub name: String,
    pub annual_rate_bps: u64,
    pub duration_seconds: u64,
    pub total_quota: Decimal,
    pub tx_hash: String,
    pub block_number: u64,
}
