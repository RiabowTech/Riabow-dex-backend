#![allow(dead_code)]
//! Earn Service (理财服务)
//!
//! Manages fixed-term earn plans with ERC-1155 NFT proof of ownership.
//! Users join plans on-chain via ZtdxTermYield, receive NFT tokens representing their stake,
//! and redeem principal + interest upon maturity.
//!
//! Key features:
//! - ERC-1155 Soulbound NFT (non-transferable)
//! - EIP-712 JoinPlan signature verification for plan subscriptions
//! - Automatic settlement on plan maturity
//! - Event-driven synchronization with ZtdxTermYield contract
//!
//! Contract address is configured via EARN_CONTRACT_ADDRESS environment variable.

pub mod models;
pub mod settlement;

use ethers::prelude::*;
use ethers::abi::{encode, Token};
use ethers::providers::{Http, Provider};
use ethers::signers::{LocalWallet, Signer};
use ethers::types::{Address, U256, H256, Filter, Log};
use ethers::utils::keccak256;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use sqlx::PgPool;
use std::str::FromStr;
use std::sync::Arc;
use tracing::Instrument;
use crate::services::points::PointsService;
use chrono::{Utc, Duration};
use uuid::Uuid;

pub use models::*;

// ============================================
// CONTRACT ABI BINDINGS
// ============================================

abigen!(
    EarnContract,
    r#"[
        function planCount() external view returns (uint256)
        function createPlan(uint256 planId, string name, uint256 maxAnnualRateBps, uint256 durationSeconds, uint256 totalQuota, uint256 minAmount, uint256 maxAmountPerUser, uint256 subscribeStartTime, uint256 subscribeEndTime) external
        function openPlan(uint256 planId) external
        function activatePlan(uint256 planId) external
        function closePlan(uint256 planId) external
        function joinPlan(uint256 planId, uint256 amount, uint256 deadline, bytes signature) external
        function redeemPlan(uint256 planId) external
        function getPlanConfig(uint256 planId) external view returns (string name, uint256 maxAnnualRateBps, uint256 durationSeconds, uint256 totalQuota, uint256 minAmount, uint256 maxAmountPerUser)
        function getPlanSchedule(uint256 planId) external view returns (uint256 subscribeStartTime, uint256 subscribeEndTime, uint256 settleTime, uint256 actualSettleTime)
        function getPlanLedger(uint256 planId) external view returns (uint256 subscribedAmount, uint256 totalInterestPaid, uint256 participantCount, uint8 status, address creator)
        function getPlanPosition(uint256 planId, address user) external view returns (uint256 amount, uint256 expectedReturn, uint256 actualReturn, uint256 subscribedAt, bool claimed)
        function balanceOf(address account, uint256 id) external view returns (uint256)
        function domainSeparator() external view returns (bytes32)
        function principalSentToTreasury(uint256 planId) external view returns (bool)
        function returnsFunded(uint256 planId) external view returns (bool)
        event PlanCreated(uint256 indexed planId, string name, uint256 maxAnnualRateBps, uint256 durationSeconds, uint256 totalQuota)
        event PlanStatusChanged(uint256 indexed planId, uint8 oldStatus, uint8 newStatus)
        event PlanJoined(uint256 indexed planId, address indexed user, uint256 amount, uint256 expectedReturn)
        event PlanClosed(uint256 indexed planId, uint256 totalPrincipal, uint256 totalInterest)
        event PlanRedeemed(uint256 indexed planId, address indexed user, uint256 principal, uint256 interest)
    ]"#
);

// ============================================
// EARN SERVICE
// ============================================

pub struct EarnService {
    pool: PgPool,
    contract: Option<EarnContract<SignerMiddleware<Provider<Http>, LocalWallet>>>,
    provider: Option<Arc<Provider<Http>>>,
    signer: Option<LocalWallet>,
    contract_address: String,
    chain_id: u64,
    points_service: Option<Arc<PointsService>>,
}

impl EarnService {
    /// Create new EarnService without contract connection (for testing/dev)
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            contract: None,
            provider: None,
            signer: None,
            contract_address: String::new(),
            chain_id: 0,
            points_service: None,
        }
    }

    /// Initialize with contract connection
    pub async fn with_contract(
        pool: PgPool,
        rpc_url: &str,
        contract_address: &str,
        private_key: &str,
        chain_id: u64,
    ) -> anyhow::Result<Self> {
        let provider = Provider::<Http>::try_from(rpc_url)?;
        let wallet = private_key.parse::<LocalWallet>()?.with_chain_id(chain_id);
        let signer_client = SignerMiddleware::new(provider.clone(), wallet.clone());
        let signer_client = Arc::new(signer_client);
        let provider = Arc::new(provider);

        let contract_addr = Address::from_str(contract_address)?;
        let contract = EarnContract::new(contract_addr, signer_client);

        tracing::info!(
            "EarnService initialized with contract: {}",
            contract_address
        );

        Ok(Self {
            pool,
            contract: Some(contract),
            provider: Some(provider),
            signer: Some(wallet),
            contract_address: contract_address.to_string(),
            chain_id,
            points_service: None,
        })
    }

    /// Set points service dependency
    pub fn set_points_service(&mut self, points_service: Arc<PointsService>) {
        self.points_service = Some(points_service);
    }

    // ============================================
    // PUBLIC API: Product Queries
    // ============================================

    /// List plans with optional status filter
    pub async fn list_plans(
        &self,
        status: Option<EarnProductStatus>,
        page: i32,
        page_size: i32,
    ) -> anyhow::Result<ProductListResponse> {
        let offset = (page - 1) * page_size;

        let db_timeout = std::time::Duration::from_secs(5);

        let (products, total): (Vec<EarnProduct>, i64) = match status {
            Some(s) => {
                let products = tokio::time::timeout(
                    db_timeout,
                    sqlx::query_as::<_, EarnProduct>(
                        r#"
                        SELECT * FROM earn_products
                        WHERE status = $1
                        ORDER BY created_at DESC
                        LIMIT $2 OFFSET $3
                        "#
                    )
                    .bind(s)
                    .bind(page_size)
                    .bind(offset)
                    .fetch_all(&self.pool),
                )
                .await
                .map_err(|_| anyhow::anyhow!("Database query timed out"))??;

                let total: (i64,) = tokio::time::timeout(
                    db_timeout,
                    sqlx::query_as(
                        "SELECT COUNT(*) FROM earn_products WHERE status = $1"
                    )
                    .bind(s)
                    .fetch_one(&self.pool),
                )
                .await
                .map_err(|_| anyhow::anyhow!("Database query timed out"))??;

                (products, total.0)
            }
            None => {
                let products = tokio::time::timeout(
                    db_timeout,
                    sqlx::query_as::<_, EarnProduct>(
                        r#"
                        SELECT * FROM earn_products
                        ORDER BY created_at DESC
                        LIMIT $1 OFFSET $2
                        "#
                    )
                    .bind(page_size)
                    .bind(offset)
                    .fetch_all(&self.pool),
                )
                .await
                .map_err(|_| anyhow::anyhow!("Database query timed out"))??;

                let total: (i64,) = tokio::time::timeout(
                    db_timeout,
                    sqlx::query_as("SELECT COUNT(*) FROM earn_products")
                        .fetch_one(&self.pool),
                )
                .await
                .map_err(|_| anyhow::anyhow!("Database query timed out"))??;

                (products, total.0)
            }
        };

        let product_details: Vec<ProductDetail> = products
            .into_iter()
            .map(ProductDetail::from_product)
            .collect();

        Ok(ProductListResponse {
            products: product_details,
            total,
            page,
            page_size,
        })
    }

    /// Get plan by ID (UUID or chain_product_id)
    pub async fn get_plan(&self, product_id: &str) -> anyhow::Result<ProductDetail> {
        let db_timeout = std::time::Duration::from_secs(5);

        let product = if let Ok(uuid) = Uuid::parse_str(product_id) {
            tokio::time::timeout(
                db_timeout,
                sqlx::query_as::<_, EarnProduct>(
                    "SELECT * FROM earn_products WHERE id = $1"
                )
                .bind(uuid)
                .fetch_optional(&self.pool),
            )
            .await
            .map_err(|_| anyhow::anyhow!("Database query timed out"))??
        } else if let Ok(chain_id) = product_id.parse::<i64>() {
            tokio::time::timeout(
                db_timeout,
                sqlx::query_as::<_, EarnProduct>(
                    "SELECT * FROM earn_products WHERE chain_product_id = $1"
                )
                .bind(chain_id)
                .fetch_optional(&self.pool),
            )
            .await
            .map_err(|_| anyhow::anyhow!("Database query timed out"))??
        } else {
            None
        };

        match product {
            Some(p) => Ok(ProductDetail::from_product(p)),
            None => anyhow::bail!("Product not found: {}", product_id),
        }
    }

    /// Get historical performance of ended products
    pub async fn get_historical_performance(&self, limit: i32) -> anyhow::Result<Vec<HistoricalPerformance>> {
        let products = sqlx::query_as::<_, EarnProduct>(
            r#"
            SELECT * FROM earn_products
            WHERE status = 'ended'
            ORDER BY settle_time DESC
            LIMIT $1
            "#
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        let history: Vec<HistoricalPerformance> = products
            .into_iter()
            .map(|p| HistoricalPerformance {
                product_name: p.name,
                duration_seconds: p.duration_seconds,
                annual_rate: format!("{:.2}%", Decimal::from(p.annual_rate_bps) / Decimal::from(100)),
                period_rate: format!("{:.2}%", Decimal::from(p.period_rate_bps) / Decimal::from(100)),
                total_subscribed: format!("{:.2}", p.subscribed_amount),
                total_interest_paid: format!("{:.2}", p.total_interest_paid),
                subscriber_count: p.subscriber_count,
                settled_at: p.settle_time,
            })
            .collect();

        Ok(history)
    }

    // ============================================
    // PUBLIC API: User Subscriptions
    // ============================================

    /// Get user's positions
    pub async fn get_user_positions(&self, user_address: &str) -> anyhow::Result<Vec<UserSubscriptionDetail>> {
        let user_addr = user_address.to_lowercase();

        let subscriptions = sqlx::query_as::<_, EarnSubscription>(
            r#"
            SELECT * FROM earn_subscriptions
            WHERE LOWER(user_address) = $1
            ORDER BY subscribed_at DESC
            "#
        )
        .bind(&user_addr)
        .fetch_all(&self.pool)
        .await?;

        let mut details = Vec::new();
        for sub in subscriptions {
            let product = sqlx::query_as::<_, EarnProduct>(
                "SELECT * FROM earn_products WHERE id = $1"
            )
            .bind(sub.product_id)
            .fetch_optional(&self.pool)
            .await?;

            if let Some(product) = product {
                let total_return = sub.amount + sub.actual_return.unwrap_or(sub.expected_return);

                details.push(UserSubscriptionDetail {
                    id: sub.id.to_string(),
                    product_id: sub.product_id.to_string(),
                    product_name: product.name.clone(),
                    chain_product_id: sub.chain_product_id,
                    amount: format!("{:.2}", sub.amount),
                    nft_amount: format!("{:.2}", sub.nft_amount),
                    expected_return: format!("{:.2}", sub.expected_return),
                    actual_return: sub.actual_return.map(|r| format!("{:.2}", r)),
                    total_return: format!("{:.2}", total_return),
                    nft_status: sub.nft_status,
                    claimed: sub.claimed,
                    product_status: product.status,
                    annual_rate: format!("{:.2}%", Decimal::from(product.annual_rate_bps) / Decimal::from(100)),
                    period_rate: format!("{:.2}%", Decimal::from(product.period_rate_bps) / Decimal::from(100)),
                    subscribed_at: sub.subscribed_at,
                    settle_time: product.settle_time,
                    settled_at: sub.settled_at,
                    claimed_at: sub.claimed_at,
                    subscribe_tx_hash: sub.subscribe_tx_hash,
                    claim_tx_hash: sub.claim_tx_hash,
                });
            }
        }

        Ok(details)
    }

    /// Prepare join plan - generate EIP-712 signature for on-chain joinPlan
    pub async fn prepare_join_plan(
        &self,
        user_address: &str,
        product_id: &str,
        amount: Decimal,
    ) -> anyhow::Result<PrepareJoinPlanResponse> {
        let _signer = match &self.signer {
            Some(s) => s,
            None => anyhow::bail!("Signer not configured"),
        };

        // Get product
        let product = self.get_product_internal(product_id).await?;

        // Convert USDT amount to Wei format (6 decimals for USDT)
        // Frontend passes amount in USDT (e.g., "100" for 100 USDT), we convert to Wei (100 * 10^6 = 100000000)
        // All database values (min_amount, max_amount_per_user, total_quota) are stored in Wei
        let amount_wei = amount * Decimal::from(1_000_000);

        // Validate
        if product.status != EarnProductStatus::Subscribing {
            anyhow::bail!("Product is not open for subscription");
        }

        let now = Utc::now();
        if now < product.subscribe_start_time || now >= product.subscribe_end_time {
            anyhow::bail!("Subscription window is not active");
        }

        if amount_wei < product.min_amount {
            anyhow::bail!("Amount below minimum: {} USDT < {} USDT", amount, product.min_amount / Decimal::from(1_000_000));
        }

        let user_addr = user_address.to_lowercase();

        // Check user's existing subscription (stored in Wei)
        let existing: Option<(Decimal,)> = sqlx::query_as(
            "SELECT amount FROM earn_subscriptions WHERE product_id = $1 AND LOWER(user_address) = $2"
        )
        .bind(product.id)
        .bind(&user_addr)
        .fetch_optional(&self.pool)
        .await?;

        let current_amount_wei = existing.map(|e| e.0).unwrap_or(Decimal::ZERO);
        let new_total_wei = current_amount_wei + amount_wei;

        if new_total_wei > product.max_amount_per_user {
            anyhow::bail!(
                "Exceeds per-user limit: {} + {} > {} USDT",
                current_amount_wei / Decimal::from(1_000_000),
                amount,
                product.max_amount_per_user / Decimal::from(1_000_000)
            );
        }

        // Check remaining quota (in Wei)
        let available_wei = product.total_quota - product.subscribed_amount;
        if amount_wei > available_wei {
            anyhow::bail!("Insufficient quota: {} > {} USDT", amount, available_wei / Decimal::from(1_000_000));
        }

        // Generate signature
        let deadline = (Utc::now() + Duration::minutes(30)).timestamp() as u64;
        let amount_raw = U256::from(amount_wei.to_u128().unwrap_or(0));

        let signature = self.sign_join_plan(
            &user_addr,
            product.chain_product_id as u64,
            amount_raw,
            deadline,
        )?;

        // Store signature for anti-replay
        sqlx::query(
            r#"
            INSERT INTO earn_subscribe_signatures
            (id, user_address, product_id, chain_product_id, amount, deadline, signature)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#
        )
        .bind(Uuid::new_v4())
        .bind(&user_addr)
        .bind(product.id)
        .bind(product.chain_product_id)
        .bind(amount_wei)
        .bind(deadline as i64)
        .bind(&signature)
        .execute(&self.pool)
        .await?;

        tracing::info!(
            "Prepared JoinPlan signature for user={}, plan={}, amount={} Wei ({} USDT)",
            user_addr, product.chain_product_id, amount_wei, amount
        );

        Ok(PrepareJoinPlanResponse {
            chain_product_id: product.chain_product_id,
            amount: amount_raw.to_string(),
            deadline,
            signature,
            contract_address: self.contract_address.clone(),
            user_address: user_addr,
        })
    }

    // ============================================
    // EIP-712 SIGNATURE
    // ============================================

    /// Generate EIP-712 signature for JoinPlan (ZtdxTermYield)
    fn sign_join_plan(
        &self,
        user: &str,
        plan_id: u64,
        amount: U256,
        deadline: u64,
    ) -> anyhow::Result<String> {
        let signer = match &self.signer {
            Some(s) => s,
            None => anyhow::bail!("Signer not configured"),
        };

        // EIP-712 Domain Separator
        let domain_type_hash = keccak256(
            b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"
        );

        // Domain name pinned to the deployed ZtdxTermYield contract.
        // See constants/eip712_domains.rs — DO NOT RENAME.
        use crate::constants::eip712_domains::{earn_domain_name, domain_version};
        let name_hash = keccak256(earn_domain_name().as_bytes());
        let version_hash = keccak256(domain_version().as_bytes());
        let chain_id = U256::from(self.chain_id);
        let verifying_contract = Address::from_str(&self.contract_address)?;

        let domain_separator = keccak256(encode(&[
            Token::FixedBytes(domain_type_hash.to_vec()),
            Token::FixedBytes(name_hash.to_vec()),
            Token::FixedBytes(version_hash.to_vec()),
            Token::Uint(chain_id),
            Token::Address(verifying_contract),
        ]));

        // JoinPlan struct type hash — exact string from ZtdxTermYield JOIN_TYPEHASH
        let join_plan_type_hash = keccak256(
            b"JoinPlan(address account,uint256 planId,uint256 principalAmount,uint256 deadline)"
        );

        let user_addr = Address::from_str(user)?;
        let struct_hash = keccak256(encode(&[
            Token::FixedBytes(join_plan_type_hash.to_vec()),
            Token::Address(user_addr),
            Token::Uint(U256::from(plan_id)),
            Token::Uint(amount),
            Token::Uint(U256::from(deadline)),
        ]));

        // EIP-712 message hash
        let mut message = Vec::with_capacity(66);
        message.extend_from_slice(b"\x19\x01");
        message.extend_from_slice(&domain_separator);
        message.extend_from_slice(&struct_hash);
        let message_hash = keccak256(&message);

        // Sign
        let signature = signer.sign_hash(H256::from(message_hash))?;
        let sig_bytes = signature.to_vec();
        let signature_hex = format!("0x{}", hex::encode(&sig_bytes));

        Ok(signature_hex)
    }

    // ============================================
    // ADMIN OPERATIONS
    // ============================================

    /// Create a new earn plan (admin)
    pub async fn create_plan(
        &self,
        req: CreateProductRequest,
        creator_address: &str,
        chain_product_id: i64,
    ) -> anyhow::Result<EarnProduct> {
        let total_quota = Decimal::from_str(&req.total_quota)?;
        let min_amount = Decimal::from_str(&req.min_amount)?;
        let max_amount_per_user = Decimal::from_str(&req.max_amount_per_user)?;

        // Calculate period rate from annual rate and duration
        // period_rate = annual_rate * (duration_seconds / seconds_per_year)
        const SECONDS_PER_YEAR: i64 = 365 * 24 * 60 * 60; // 31536000
        let period_rate_bps = ((req.annual_rate_bps as i64) * req.duration_seconds / SECONDS_PER_YEAR) as i32;

        // Calculate settle time (subscribe_end + duration)
        let settle_time = req.subscribe_end_time + Duration::seconds(req.duration_seconds);

        let id = Uuid::new_v4();

        sqlx::query(
            r#"
            INSERT INTO earn_products
            (id, chain_product_id, contract_address, name, description,
             annual_rate_bps, duration_seconds, period_rate_bps,
             total_quota, min_amount, max_amount_per_user,
             subscribe_start_time, subscribe_end_time, settle_time,
             status, creator_address)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
            ON CONFLICT (chain_product_id) DO UPDATE SET
                name = EXCLUDED.name,
                description = EXCLUDED.description,
                annual_rate_bps = EXCLUDED.annual_rate_bps,
                duration_seconds = EXCLUDED.duration_seconds,
                period_rate_bps = EXCLUDED.period_rate_bps,
                total_quota = EXCLUDED.total_quota,
                min_amount = EXCLUDED.min_amount,
                max_amount_per_user = EXCLUDED.max_amount_per_user,
                subscribe_start_time = EXCLUDED.subscribe_start_time,
                subscribe_end_time = EXCLUDED.subscribe_end_time,
                settle_time = EXCLUDED.settle_time,
                creator_address = EXCLUDED.creator_address,
                updated_at = NOW()
            "#
        )
        .bind(id)
        .bind(chain_product_id)
        .bind(&self.contract_address)
        .bind(&req.name)
        .bind(&req.description)
        .bind(req.annual_rate_bps)
        .bind(req.duration_seconds)
        .bind(period_rate_bps)
        .bind(total_quota)
        .bind(min_amount)
        .bind(max_amount_per_user)
        .bind(req.subscribe_start_time)
        .bind(req.subscribe_end_time)
        .bind(settle_time)
        .bind(EarnProductStatus::Created)
        .bind(creator_address.to_lowercase())
        .execute(&self.pool)
        .await?;

        let product = sqlx::query_as::<_, EarnProduct>(
            "SELECT * FROM earn_products WHERE chain_product_id = $1"
        )
        .bind(chain_product_id)
        .fetch_one(&self.pool)
        .await?;

        tracing::info!(
            "Created product: {} (chain_id={})",
            req.name, chain_product_id
        );

        Ok(product)
    }

    /// Update plan status (admin)
    pub async fn update_plan_status(
        &self,
        product_id: &str,
        new_status: EarnProductStatus,
    ) -> anyhow::Result<EarnProduct> {
        let product = self.get_product_internal(product_id).await?;

        // Validate status transition
        // Flow: created → subscribing → active → settled → ended
        // Cancelled can be set from created, subscribing, or active
        let valid_transition = match (product.status, new_status) {
            (EarnProductStatus::Created, EarnProductStatus::Subscribing) => true,
            (EarnProductStatus::Subscribing, EarnProductStatus::Active) => true,
            (EarnProductStatus::Active, EarnProductStatus::Settled) => true,
            (EarnProductStatus::Settled, EarnProductStatus::Ended) => true,
            // Cancelled transitions
            (EarnProductStatus::Created, EarnProductStatus::Cancelled) => true,
            (EarnProductStatus::Subscribing, EarnProductStatus::Cancelled) => true,
            (EarnProductStatus::Active, EarnProductStatus::Cancelled) => true,
            _ => false,
        };

        if !valid_transition {
            anyhow::bail!(
                "Invalid status transition: {:?} -> {:?}",
                product.status, new_status
            );
        }

        sqlx::query("UPDATE earn_products SET status = $1, updated_at = NOW() WHERE id = $2")
            .bind(new_status)
            .bind(product.id)
            .execute(&self.pool)
            .await?;

        let updated = sqlx::query_as::<_, EarnProduct>(
            "SELECT * FROM earn_products WHERE id = $1"
        )
        .bind(product.id)
        .fetch_one(&self.pool)
        .await?;

        tracing::info!(
            "Updated product {} status: {:?} -> {:?}",
            product_id, product.status, new_status
        );

        Ok(updated)
    }

    /// Get positions for a plan (admin)
    pub async fn get_plan_positions(
        &self,
        product_id: &str,
        query: AdminSubscriptionQuery,
    ) -> anyhow::Result<AdminSubscriptionListResponse> {
        let product = self.get_product_internal(product_id).await?;
        let page = query.page.unwrap_or(1);
        let page_size = query.page_size.unwrap_or(50);
        let offset = (page - 1) * page_size;

        let subscriptions = sqlx::query_as::<_, EarnSubscription>(
            r#"
            SELECT * FROM earn_subscriptions
            WHERE product_id = $1
            ORDER BY subscribed_at DESC
            LIMIT $2 OFFSET $3
            "#
        )
        .bind(product.id)
        .bind(page_size)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;

        let stats: (i64, Decimal, Decimal) = sqlx::query_as(
            r#"
            SELECT
                COUNT(*),
                COALESCE(SUM(amount), 0),
                COALESCE(SUM(expected_return), 0)
            FROM earn_subscriptions
            WHERE product_id = $1
            "#
        )
        .bind(product.id)
        .fetch_one(&self.pool)
        .await?;

        Ok(AdminSubscriptionListResponse {
            subscriptions,
            total: stats.0,
            page,
            page_size,
            total_amount: format!("{:.2}", stats.1),
            total_expected_return: format!("{:.2}", stats.2),
        })
    }

    // ============================================
    // EVENT HANDLING
    // ============================================

    /// Handle PlanJoined event from ZtdxTermYield contract
    ///
    /// Idempotent with respect to (tx_hash, log_index): the listener is
    /// at-least-once, and the subscription upsert accumulates `amount`, so
    /// replaying the same chain log would double-count the user's position.
    /// We gate the entire mutation in a DB transaction that first claims
    /// the event via INSERT … ON CONFLICT DO NOTHING into
    /// `earn_processed_events`; if the claim fails (already processed),
    /// the tx is rolled back and nothing changes.
    pub async fn handle_plan_joined_event(&self, event: PlanJoinedEvent) -> anyhow::Result<()> {
        let product = sqlx::query_as::<_, EarnProduct>(
            "SELECT * FROM earn_products WHERE chain_product_id = $1"
        )
        .bind(event.product_id as i64)
        .fetch_optional(&self.pool)
        .await?;

        let product = match product {
            Some(p) => p,
            None => {
                tracing::warn!("Product not found for chain_product_id={}", event.product_id);
                return Ok(());
            }
        };

        // Use expected_return from event (already calculated by contract)
        let expected_return = event.expected_return;

        let mut db_tx = self.pool.begin().await?;

        let claim = sqlx::query(
            r#"
            INSERT INTO earn_processed_events (tx_hash, log_index, event_type, block_number)
            VALUES ($1, $2, 'Subscribed', $3)
            ON CONFLICT (tx_hash, log_index) DO NOTHING
            "#
        )
        .bind(&event.tx_hash)
        .bind(event.log_index as i32)
        .bind(event.block_number as i64)
        .execute(&mut *db_tx)
        .await?;

        if claim.rows_affected() == 0 {
            tracing::info!(
                "Skipping already-processed PlanJoined event: tx={} log_index={} plan={} user={}",
                event.tx_hash, event.log_index, event.product_id, event.user
            );
            return Ok(());
        }

        // Upsert subscription
        sqlx::query(
            r#"
            INSERT INTO earn_subscriptions
            (id, product_id, chain_product_id, user_address, amount, nft_amount,
             expected_return, nft_status, subscribe_tx_hash)
            VALUES ($1, $2, $3, $4, $5, $6, $7, 'active', $8)
            ON CONFLICT (product_id, user_address)
            DO UPDATE SET
                amount = earn_subscriptions.amount + $5,
                nft_amount = earn_subscriptions.nft_amount + $6,
                expected_return = earn_subscriptions.expected_return + $7,
                subscribe_tx_hash = $8
            "#
        )
        .bind(Uuid::new_v4())
        .bind(product.id)
        .bind(event.product_id as i64)
        .bind(event.user.to_lowercase())
        .bind(event.amount)
        .bind(event.nft_amount)
        .bind(expected_return)
        .bind(&event.tx_hash)
        .execute(&mut *db_tx)
        .await?;

        // Update product stats
        sqlx::query(
            r#"
            UPDATE earn_products SET
                subscribed_amount = subscribed_amount + $1,
                subscriber_count = (SELECT COUNT(DISTINCT user_address) FROM earn_subscriptions WHERE product_id = $2),
                updated_at = NOW()
            WHERE id = $2
            "#
        )
        .bind(event.amount)
        .bind(product.id)
        .execute(&mut *db_tx)
        .await?;

        // Mark signature as used
        sqlx::query(
            r#"
            UPDATE earn_subscribe_signatures
            SET used = true, used_at = NOW(), used_tx_hash = $1
            WHERE chain_product_id = $2 AND LOWER(user_address) = $3 AND NOT used
            "#
        )
        .bind(&event.tx_hash)
        .bind(event.product_id as i64)
        .bind(event.user.to_lowercase())
        .execute(&mut *db_tx)
        .await?;

        db_tx.commit().await?;

        tracing::info!(
            "Processed PlanJoined event: plan={}, user={}, amount={}",
            event.product_id, event.user, event.amount
        );

        // Record staking in points system
        if let Some(points_service) = &self.points_service {
            // Note: Token address is hardcoded to USDT for now as product contract implies USDT
            match points_service.record_staking(
                &event.user,
                event.amount,
                "USDT",
                Some(event.tx_hash.clone()),
            ).await {
                Ok(_) => tracing::info!("Recorded staking points for user={}", event.user),
                Err(e) => tracing::error!("Failed to record staking points: {}", e),
            }
        }

        Ok(())
    }

    /// Handle PlanClosed event from ZtdxTermYield contract
    ///
    /// Idempotent via `earn_processed_events`: the settlement INSERT has no
    /// unique key on tx_hash, so without this gate a replay of the same
    /// PlanClosed log would create duplicate settlement rows and
    /// re-bump `total_interest_paid`.
    pub async fn handle_plan_closed_event(&self, event: PlanClosedEvent) -> anyhow::Result<()> {
        let product = sqlx::query_as::<_, EarnProduct>(
            "SELECT * FROM earn_products WHERE chain_product_id = $1"
        )
        .bind(event.product_id as i64)
        .fetch_optional(&self.pool)
        .await?;

        let product = match product {
            Some(p) => p,
            None => {
                tracing::warn!("Product not found for chain_product_id={}", event.product_id);
                return Ok(());
            }
        };

        let mut db_tx = self.pool.begin().await?;

        let claim = sqlx::query(
            r#"
            INSERT INTO earn_processed_events (tx_hash, log_index, event_type, block_number)
            VALUES ($1, $2, 'Settled', $3)
            ON CONFLICT (tx_hash, log_index) DO NOTHING
            "#
        )
        .bind(&event.tx_hash)
        .bind(event.log_index as i32)
        .bind(event.block_number as i64)
        .execute(&mut *db_tx)
        .await?;

        if claim.rows_affected() == 0 {
            tracing::info!(
                "Skipping already-processed PlanClosed event: tx={} log_index={} plan={}",
                event.tx_hash, event.log_index, event.product_id
            );
            return Ok(());
        }

        // Create settlement record
        let settled_count: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM earn_subscriptions WHERE product_id = $1"
        )
        .bind(product.id)
        .fetch_one(&mut *db_tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO earn_settlements
            (id, product_id, chain_product_id, total_principal, total_interest, settled_count, tx_hash, block_number)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#
        )
        .bind(Uuid::new_v4())
        .bind(product.id)
        .bind(event.product_id as i64)
        .bind(event.total_principal)
        .bind(event.total_interest)
        .bind(settled_count.0 as i32)
        .bind(&event.tx_hash)
        .bind(event.block_number as i64)
        .execute(&mut *db_tx)
        .await?;

        // Update product status to 'settled' (users can now claim)
        // Status will change to 'ended' after all users have claimed
        sqlx::query(
            r#"
            UPDATE earn_products SET
                status = 'settled',
                total_interest_paid = $1,
                updated_at = NOW()
            WHERE id = $2
            "#
        )
        .bind(event.total_interest)
        .bind(product.id)
        .execute(&mut *db_tx)
        .await?;

        // Update all subscriptions to matured
        sqlx::query(
            r#"
            UPDATE earn_subscriptions SET
                nft_status = 'matured',
                settled_at = NOW(),
                actual_return = expected_return
            WHERE product_id = $1
            "#
        )
        .bind(product.id)
        .execute(&mut *db_tx)
        .await?;

        db_tx.commit().await?;

        tracing::info!(
            "Processed PlanClosed event: plan={}, principal={}, interest={}",
            event.product_id, event.total_principal, event.total_interest
        );

        Ok(())
    }

    /// Handle PlanRedeemed event from ZtdxTermYield contract
    pub async fn handle_plan_redeemed_event(&self, event: PlanRedeemedEvent) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            UPDATE earn_subscriptions SET
                nft_status = 'redeemed',
                claimed = true,
                claimed_at = NOW(),
                actual_return = $1,
                claim_tx_hash = $2
            WHERE chain_product_id = $3 AND LOWER(user_address) = $4
            "#
        )
        .bind(event.interest)
        .bind(&event.tx_hash)
        .bind(event.product_id as i64)
        .bind(event.user.to_lowercase())
        .execute(&self.pool)
        .await?;

        tracing::info!(
            "Processed PlanRedeemed event: plan={}, user={}, principal={}, interest={}",
            event.product_id, event.user, event.principal, event.interest
        );

        Ok(())
    }

    /// Handle PlanCreated event from ZtdxTermYield contract
    pub async fn handle_plan_created_event(&self, event: PlanCreatedEvent) -> anyhow::Result<()> {
        // Check if product already exists
        let existing: Option<(i64,)> = sqlx::query_as(
            "SELECT chain_product_id FROM earn_products WHERE chain_product_id = $1"
        )
        .bind(event.product_id as i64)
        .fetch_optional(&self.pool)
        .await?;

        if existing.is_some() {
            tracing::debug!("Product {} already exists, skipping", event.product_id);
            return Ok(());
        }

        // Get full product details from contract
        let contract = match &self.contract {
            Some(c) => c,
            None => {
                tracing::warn!("No contract configured, cannot fetch product details");
                return Ok(());
            }
        };

        // Get plan config
        let config = contract
            .get_plan_config(U256::from(event.product_id))
            .call()
            .await?;
        // Contract returns: (name, maxAnnualRateBps, durationSeconds, totalQuota, minAmount, maxAmountPerUser)
        let (_contract_name, annual_rate_bps, duration_seconds_u256, total_quota, min_amount, max_amount_per_user) = config;
        // Compute period_rate_bps locally: annual_rate * duration / seconds_per_year
        let period_rate_bps = U256::from(annual_rate_bps.as_u64() * duration_seconds_u256.as_u64() / (365 * 24 * 3600));

        // Use name from event log (correctly parsed) instead of contract call (may have ABI parsing issues)
        let name = &event.name;

        // Get plan timing
        let timing = contract
            .get_plan_schedule(U256::from(event.product_id))
            .call()
            .await?;
        let (subscribe_start_time, subscribe_end_time, settle_time, _actual_settle_time) = timing;

        // Get plan ledger for creator
        let stats = contract
            .get_plan_ledger(U256::from(event.product_id))
            .call()
            .await?;
        let (_subscribed_amount, _total_interest_paid, _participant_count, _status, creator) = stats;

        // Convert values
        let total_quota_dec = u256_to_decimal(total_quota);
        let min_amount_dec = u256_to_decimal(min_amount);
        let max_amount_per_user_dec = u256_to_decimal(max_amount_per_user);
        // Contract returns duration in seconds
        let duration_seconds = duration_seconds_u256.as_u64() as i64;

        // Convert timestamps
        let subscribe_start = chrono::DateTime::from_timestamp(subscribe_start_time.as_u64() as i64, 0)
            .unwrap_or_else(|| Utc::now());
        let subscribe_end = chrono::DateTime::from_timestamp(subscribe_end_time.as_u64() as i64, 0)
            .unwrap_or_else(|| Utc::now());
        let settle = chrono::DateTime::from_timestamp(settle_time.as_u64() as i64, 0)
            .unwrap_or_else(|| Utc::now());

        // Insert new product
        sqlx::query(
            r#"
            INSERT INTO earn_products (
                chain_product_id, contract_address, name, description,
                annual_rate_bps, duration_seconds, period_rate_bps,
                total_quota, min_amount, max_amount_per_user,
                subscribe_start_time, subscribe_end_time, settle_time,
                status, creator_address
            ) VALUES (
                $1, $2, $3, $4,
                $5, $6, $7,
                $8, $9, $10,
                $11, $12, $13,
                'created', $14
            )
            "#
        )
        .bind(event.product_id as i64)
        .bind(&self.contract_address)
        .bind(name)
        .bind::<Option<String>>(None) // description
        .bind(annual_rate_bps.as_u32() as i32)
        .bind(duration_seconds)
        .bind(period_rate_bps.as_u32() as i32)
        .bind(total_quota_dec)
        .bind(min_amount_dec)
        .bind(max_amount_per_user_dec)
        .bind(subscribe_start)
        .bind(subscribe_end)
        .bind(settle)
        .bind(format!("{:?}", creator).to_lowercase())
        .execute(&self.pool)
        .await?;

        tracing::info!(
            "Processed PlanCreated event: plan_id={}, name={}, max_annual_rate_bps={}, duration_seconds={}, total_quota={}",
            event.product_id, name, annual_rate_bps, duration_seconds, total_quota_dec
        );

        Ok(())
    }

    /// Process a raw PlanCreated log
    async fn process_plan_created_log(&self, log: &Log) -> anyhow::Result<()> {
        // Extract plan_id from topics[1] (indexed parameter)
        let product_id = if log.topics.len() > 1 {
            U256::from_big_endian(log.topics[1].as_bytes()).as_u64()
        } else {
            return Err(anyhow::anyhow!("Missing plan_id in log topics"));
        };

        tracing::info!(
            "Processing PlanCreated log: plan_id={}, block={:?}, tx={:?}",
            product_id, log.block_number, log.transaction_hash
        );

        // Decode the non-indexed parameters from log.data
        // Layout: offset_to_name (32 bytes), maxAnnualRateBps (32), durationSeconds (32), totalQuota (32), name_length (32), name_data
        let data = &log.data.0;
        if data.len() < 128 {
            return Err(anyhow::anyhow!("Log data too short: {} bytes", data.len()));
        }

        // Parse parameters (each 32 bytes)
        let _name_offset = U256::from_big_endian(&data[0..32]).as_usize();
        let annual_rate_bps = U256::from_big_endian(&data[32..64]).as_u64();
        let duration_seconds = U256::from_big_endian(&data[64..96]).as_u64();
        let total_quota = U256::from_big_endian(&data[96..128]);

        // Parse name string (at offset, first 32 bytes is length, then data)
        // Note: Solidity strings may contain null bytes (0x00) which PostgreSQL rejects
        // We strip all null bytes to ensure valid UTF-8 text for database storage
        let name = if data.len() > 160 {
            let name_len = U256::from_big_endian(&data[128..160]).as_usize();
            let name_end = std::cmp::min(160 + name_len, data.len());
            let raw_name = String::from_utf8_lossy(&data[160..name_end]).to_string();
            // Remove null bytes that PostgreSQL cannot store
            raw_name.replace('\0', "").trim().to_string()
        } else {
            format!("Product {}", product_id)
        };

        let total_quota_dec = u256_to_decimal(total_quota);
        let tx_hash = log.transaction_hash.map(|h| format!("{:?}", h)).unwrap_or_default();
        let block_number = log.block_number.map(|b| b.as_u64()).unwrap_or(0);

        self.handle_plan_created_event(PlanCreatedEvent {
            product_id,
            name,
            annual_rate_bps,
            duration_seconds,
            total_quota: total_quota_dec,
            tx_hash,
            block_number,
        }).await
    }

    /// Process PlanJoined event from raw log
    /// Event: PlanJoined(uint256 indexed planId, address indexed user, uint256 amount, uint256 expectedReturn)
    async fn process_subscribed_log(&self, log: &Log) -> anyhow::Result<()> {
        // Extract plan_id from topics[1] (indexed parameter)
        let product_id = if log.topics.len() > 1 {
            U256::from_big_endian(log.topics[1].as_bytes()).as_u64()
        } else {
            return Err(anyhow::anyhow!("Missing plan_id in log topics"));
        };

        // Extract user address from topics[2] (indexed parameter)
        let user = if log.topics.len() > 2 {
            let addr_bytes = &log.topics[2].as_bytes()[12..32]; // Last 20 bytes
            format!("0x{}", hex::encode(addr_bytes))
        } else {
            return Err(anyhow::anyhow!("Missing user address in log topics"));
        };

        tracing::info!(
            "Processing PlanJoined log: plan_id={}, user={}, block={:?}, tx={:?}",
            product_id, user, log.block_number, log.transaction_hash
        );

        // Decode non-indexed parameters from log.data
        // Layout: amount (32 bytes), expectedReturn (32 bytes)
        // Note: nftAmount is not in the event, set to 0
        let data = &log.data.0;
        if data.len() < 64 {
            return Err(anyhow::anyhow!("Log data too short: {} bytes", data.len()));
        }

        let amount = U256::from_big_endian(&data[0..32]);
        let expected_return = U256::from_big_endian(&data[32..64]);

        let tx_hash = log.transaction_hash.map(|h| format!("{:?}", h)).unwrap_or_default();
        let block_number = log.block_number.map(|b| b.as_u64()).unwrap_or(0);
        let log_index = log.log_index.map(|i| i.as_u64()).unwrap_or(0);

        self.handle_plan_joined_event(PlanJoinedEvent {
            product_id,
            user,
            amount: u256_to_decimal(amount),
            nft_amount: Decimal::ZERO, // Not in event, set to 0
            expected_return: u256_to_decimal(expected_return),
            tx_hash,
            block_number,
            log_index,
        }).await
    }

    /// Process PlanClosed event from raw log
    /// Event: PlanClosed(uint256 indexed planId, uint256 totalPrincipal, uint256 totalInterest)
    async fn process_settled_log(&self, log: &Log) -> anyhow::Result<()> {
        // Extract plan_id from topics[1] (indexed parameter)
        let product_id = if log.topics.len() > 1 {
            U256::from_big_endian(log.topics[1].as_bytes()).as_u64()
        } else {
            return Err(anyhow::anyhow!("Missing plan_id in log topics"));
        };

        tracing::info!(
            "Processing PlanClosed log: plan_id={}, block={:?}, tx={:?}",
            product_id, log.block_number, log.transaction_hash
        );

        // Decode non-indexed parameters from log.data
        // Layout: totalPrincipal (32 bytes), totalInterest (32 bytes)
        let data = &log.data.0;
        if data.len() < 64 {
            return Err(anyhow::anyhow!("Log data too short: {} bytes", data.len()));
        }

        let total_principal = U256::from_big_endian(&data[0..32]);
        let total_interest = U256::from_big_endian(&data[32..64]);

        let tx_hash = log.transaction_hash.map(|h| format!("{:?}", h)).unwrap_or_default();
        let block_number = log.block_number.map(|b| b.as_u64()).unwrap_or(0);
        let log_index = log.log_index.map(|i| i.as_u64()).unwrap_or(0);

        self.handle_plan_closed_event(PlanClosedEvent {
            product_id,
            total_principal: u256_to_decimal(total_principal),
            total_interest: u256_to_decimal(total_interest),
            tx_hash,
            block_number,
            log_index,
        }).await
    }

    /// Process PlanRedeemed event from raw log
    /// Event: PlanRedeemed(uint256 indexed planId, address indexed user, uint256 principal, uint256 interest)
    async fn process_claimed_log(&self, log: &Log) -> anyhow::Result<()> {
        // Extract plan_id from topics[1] (indexed parameter)
        let product_id = if log.topics.len() > 1 {
            U256::from_big_endian(log.topics[1].as_bytes()).as_u64()
        } else {
            return Err(anyhow::anyhow!("Missing plan_id in log topics"));
        };

        // Extract user address from topics[2] (indexed parameter)
        let user = if log.topics.len() > 2 {
            let addr_bytes = &log.topics[2].as_bytes()[12..32]; // Last 20 bytes
            format!("0x{}", hex::encode(addr_bytes))
        } else {
            return Err(anyhow::anyhow!("Missing user address in log topics"));
        };

        tracing::info!(
            "Processing PlanRedeemed log: plan_id={}, user={}, block={:?}, tx={:?}",
            product_id, user, log.block_number, log.transaction_hash
        );

        // Decode non-indexed parameters from log.data
        // Layout: principal (32 bytes), interest (32 bytes)
        let data = &log.data.0;
        if data.len() < 64 {
            return Err(anyhow::anyhow!("Log data too short: {} bytes", data.len()));
        }

        let principal = U256::from_big_endian(&data[0..32]);
        let interest = U256::from_big_endian(&data[32..64]);

        let tx_hash = log.transaction_hash.map(|h| format!("{:?}", h)).unwrap_or_default();
        let block_number = log.block_number.map(|b| b.as_u64()).unwrap_or(0);

        self.handle_plan_redeemed_event(PlanRedeemedEvent {
            product_id,
            user,
            principal: u256_to_decimal(principal),
            interest: u256_to_decimal(interest),
            tx_hash,
            block_number,
        }).await
    }

    // ============================================
    // EVENT LISTENER
    // ============================================

    /// Start the event listener for contract events
    /// Polls for PlanJoined, PlanClosed, and PlanRedeemed events
    pub async fn start_event_listener(self: Arc<Self>) {
        if self.provider.is_none() || self.contract.is_none() {
            tracing::info!("Earn event listener skipped - no contract configured");
            return;
        }

        let service = self.clone();

        tokio::spawn(async move {
            tracing::info!("Earn event listener started");

            let poll_interval = std::time::Duration::from_secs(12);
            let mut consecutive_errors = 0u32;
            let mut last_block = service.get_last_synced_block().await.unwrap_or(0);

            loop {
                match service.poll_events(last_block).await {
                    Ok(new_block) => {
                        if new_block > last_block {
                            last_block = new_block;
                            service.update_last_synced_block(new_block).await.ok();
                        }
                        consecutive_errors = 0;
                    }
                    Err(e) => {
                        consecutive_errors += 1;
                        let backoff = std::cmp::min(consecutive_errors * 10, 300);
                        if consecutive_errors <= 3 {
                            tracing::error!("Earn event listener error: {}", e);
                        } else {
                            tracing::debug!("Earn event listener error (suppressed): {}", e);
                        }
                        tokio::time::sleep(std::time::Duration::from_secs(backoff as u64)).await;
                        continue;
                    }
                }

                tokio::time::sleep(poll_interval).await;
            }
        }.instrument(tracing::info_span!("earn-event-listener")));
    }

    /// Poll for contract events from a given block
    async fn poll_events(&self, from_block: u64) -> anyhow::Result<u64> {
        let provider = match &self.provider {
            Some(p) => p,
            None => return Ok(from_block),
        };

        let _contract = match &self.contract {
            Some(c) => c,
            None => return Ok(from_block),
        };

        let current_block = provider.get_block_number().await?.as_u64();
        if from_block >= current_block {
            return Ok(from_block);
        }

        let to_block = std::cmp::min(from_block + 1000, current_block);

        tracing::debug!(
            "Polling earn events from block {} to {} (current: {})",
            from_block + 1, to_block, current_block
        );

        // Query PlanCreated events using raw log filter for better reliability
        // Event signature: PlanCreated(uint256 indexed planId, string name, uint256 maxAnnualRateBps, uint256 durationSeconds, uint256 totalQuota)
        let plan_created_topic = H256::from(keccak256("PlanCreated(uint256,string,uint256,uint256,uint256)"));
        let contract_address = Address::from_str(&self.contract_address).ok();

        let plan_created_filter = Filter::new()
            .address(contract_address.unwrap_or_default())
            .topic0(plan_created_topic)
            .from_block(from_block + 1)
            .to_block(to_block);

        let plan_created_logs = provider.get_logs(&plan_created_filter).await?;

        if !plan_created_logs.is_empty() {
            tracing::info!(
                "Found {} PlanCreated events in blocks {}-{}",
                plan_created_logs.len(), from_block + 1, to_block
            );
        }

        for log in plan_created_logs {
            if let Err(e) = self.process_plan_created_log(&log).await {
                tracing::error!("Failed to process PlanCreated log: {}", e);
            }
        }

        // Query PlanJoined events using raw log filter
        // Event signature: PlanJoined(uint256 indexed planId, address indexed user, uint256 amount, uint256 expectedReturn)
        let plan_joined_topic = H256::from(keccak256("PlanJoined(uint256,address,uint256,uint256)"));
        let plan_joined_filter = Filter::new()
            .address(contract_address.unwrap_or_default())
            .topic0(plan_joined_topic)
            .from_block(from_block + 1)
            .to_block(to_block);

        let plan_joined_logs = provider.get_logs(&plan_joined_filter).await?;

        if !plan_joined_logs.is_empty() {
            tracing::info!(
                "Found {} PlanJoined events in blocks {}-{}",
                plan_joined_logs.len(), from_block + 1, to_block
            );
        }

        for log in plan_joined_logs {
            if let Err(e) = self.process_subscribed_log(&log).await {
                tracing::error!("Failed to process PlanJoined log: {}", e);
            }
        }

        // Query PlanClosed events using raw log filter
        // Event signature: PlanClosed(uint256 indexed planId, uint256 totalPrincipal, uint256 totalInterest)
        let plan_closed_topic = H256::from(keccak256("PlanClosed(uint256,uint256,uint256)"));
        let plan_closed_filter = Filter::new()
            .address(contract_address.unwrap_or_default())
            .topic0(plan_closed_topic)
            .from_block(from_block + 1)
            .to_block(to_block);

        let plan_closed_logs = provider.get_logs(&plan_closed_filter).await?;

        if !plan_closed_logs.is_empty() {
            tracing::info!(
                "Found {} PlanClosed events in blocks {}-{}",
                plan_closed_logs.len(), from_block + 1, to_block
            );
        }

        for log in plan_closed_logs {
            if let Err(e) = self.process_settled_log(&log).await {
                tracing::error!("Failed to process PlanClosed log: {}", e);
            }
        }

        // Query PlanRedeemed events using raw log filter
        // Event signature: PlanRedeemed(uint256 indexed planId, address indexed user, uint256 principal, uint256 interest)
        let plan_redeemed_topic = H256::from(keccak256("PlanRedeemed(uint256,address,uint256,uint256)"));
        let plan_redeemed_filter = Filter::new()
            .address(contract_address.unwrap_or_default())
            .topic0(plan_redeemed_topic)
            .from_block(from_block + 1)
            .to_block(to_block);

        let plan_redeemed_logs = provider.get_logs(&plan_redeemed_filter).await?;

        if !plan_redeemed_logs.is_empty() {
            tracing::info!(
                "Found {} PlanRedeemed events in blocks {}-{}",
                plan_redeemed_logs.len(), from_block + 1, to_block
            );
        }

        for log in plan_redeemed_logs {
            if let Err(e) = self.process_claimed_log(&log).await {
                tracing::error!("Failed to process PlanRedeemed log: {}", e);
            }
        }

        tracing::debug!(
            "Earn poll completed: blocks {}-{}, events processed",
            from_block + 1, to_block
        );

        Ok(to_block)
    }

    /// Get the last synced block from database
    async fn get_last_synced_block(&self) -> anyhow::Result<u64> {
        let result: Option<(i64,)> = sqlx::query_as(
            "SELECT last_block FROM block_sync_state WHERE event_type = 'earn_events'"
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(result.map(|r| r.0 as u64).unwrap_or(0))
    }

    /// Update the last synced block in database
    async fn update_last_synced_block(&self, block: u64) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            INSERT INTO block_sync_state (event_type, last_block, updated_at)
            VALUES ('earn_events', $1, NOW())
            ON CONFLICT (event_type) DO UPDATE SET last_block = $1, updated_at = NOW()
            "#
        )
        .bind(block as i64)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    // ============================================
    // INTERNAL HELPERS
    // ============================================

    async fn get_product_internal(&self, product_id: &str) -> anyhow::Result<EarnProduct> {
        let db_timeout = std::time::Duration::from_secs(5);

        let product = if let Ok(uuid) = Uuid::parse_str(product_id) {
            tokio::time::timeout(
                db_timeout,
                sqlx::query_as::<_, EarnProduct>(
                    "SELECT * FROM earn_products WHERE id = $1"
                )
                .bind(uuid)
                .fetch_optional(&self.pool),
            )
            .await
            .map_err(|_| anyhow::anyhow!("Database query timed out"))??
        } else if let Ok(chain_id) = product_id.parse::<i64>() {
            tokio::time::timeout(
                db_timeout,
                sqlx::query_as::<_, EarnProduct>(
                    "SELECT * FROM earn_products WHERE chain_product_id = $1"
                )
                .bind(chain_id)
                .fetch_optional(&self.pool),
            )
            .await
            .map_err(|_| anyhow::anyhow!("Database query timed out"))??
        } else {
            None
        };

        product.ok_or_else(|| anyhow::anyhow!("Product not found: {}", product_id))
    }

    /// Get contract address
    pub fn get_contract_address(&self) -> &str {
        &self.contract_address
    }

    /// Check if contract is configured
    pub fn has_contract(&self) -> bool {
        self.contract.is_some()
    }

    // ============================================
    // ON-CHAIN CONTRACT CALLS
    // ============================================

    /// Call createPlan on-chain to register the earn plan in ZtdxTermYield
    pub async fn call_create_plan(
        &self,
        chain_product_id: i64,
        name: &str,
        annual_rate_bps: i32,
        duration_seconds: i64,
        total_quota: Decimal,
        min_amount: Decimal,
        max_amount_per_user: Decimal,
        subscribe_start_time: chrono::DateTime<Utc>,
        subscribe_end_time: chrono::DateTime<Utc>,
    ) -> anyhow::Result<H256> {
        let contract = self.contract.as_ref()
            .ok_or_else(|| anyhow::anyhow!("Contract not configured"))?;

        let plan_id_u256 = U256::from(chain_product_id as u64);
        let duration_secs = U256::from(duration_seconds.max(1) as u64);
        // Convert quota/amounts from DB wei format (6 decimals already applied) to U256
        let total_quota_u256 = decimal_to_wei(total_quota, 0);
        let min_amount_u256 = decimal_to_wei(min_amount, 0);
        let max_amount_u256 = decimal_to_wei(max_amount_per_user, 0);
        let start_time = U256::from(subscribe_start_time.timestamp() as u64);
        let end_time = U256::from(subscribe_end_time.timestamp() as u64);

        tracing::info!(
            "Calling createPlan on-chain: id={}, name={}, rate_bps={}, duration_seconds={}, quota={}, start={}, end={}",
            chain_product_id, name, annual_rate_bps, duration_secs, total_quota_u256, start_time, end_time
        );

        let tx = contract
            .create_plan(
                plan_id_u256,
                name.to_string(),
                U256::from(annual_rate_bps as u64),
                duration_secs,
                total_quota_u256,
                min_amount_u256,
                max_amount_u256,
                start_time,
                end_time,
            )
            .send()
            .await?
            .await?;

        let tx_hash = tx.ok_or_else(|| anyhow::anyhow!("createPlan transaction failed"))?.transaction_hash;
        tracing::info!("createPlan tx: {:?}", tx_hash);

        Ok(tx_hash)
    }

    /// Call openPlan on-chain (Created -> Subscribing)
    pub async fn call_open_plan(&self, product_id: i64) -> anyhow::Result<H256> {
        let contract = self.contract.as_ref()
            .ok_or_else(|| anyhow::anyhow!("Contract not configured"))?;

        tracing::info!("Calling openPlan for plan {}", product_id);

        let tx = contract
            .open_plan(U256::from(product_id as u64))
            .send()
            .await?
            .await?;

        let tx_hash = tx.ok_or_else(|| anyhow::anyhow!("Transaction failed"))?.transaction_hash;
        tracing::info!("openPlan tx: {:?}", tx_hash);

        Ok(tx_hash)
    }

    /// Call activatePlan on-chain (Subscribing -> Active)
    pub async fn call_activate_plan(&self, product_id: i64) -> anyhow::Result<H256> {
        let contract = self.contract.as_ref()
            .ok_or_else(|| anyhow::anyhow!("Contract not configured"))?;

        tracing::info!("Calling activatePlan for plan {}", product_id);

        let tx = contract
            .activate_plan(U256::from(product_id as u64))
            .send()
            .await?
            .await?;

        let tx_hash = tx.ok_or_else(|| anyhow::anyhow!("Transaction failed"))?.transaction_hash;
        tracing::info!("activatePlan tx: {:?}", tx_hash);

        Ok(tx_hash)
    }

    /// Call closePlan on-chain (Active -> Settled)
    ///
    /// Before calling closePlan, checks if:
    /// - principalSentToTreasury is true -> returnsFunded must also be true
    /// - Otherwise returns an error indicating fundPlanReturns needs to be called first
    pub async fn call_close_plan(&self, product_id: i64) -> anyhow::Result<H256> {
        let contract = self.contract.as_ref()
            .ok_or_else(|| anyhow::anyhow!("Contract not configured"))?;

        let plan_id_u256 = U256::from(product_id as u64);

        // Check if principal was sent to treasury
        let principal_sent = contract
            .principal_sent_to_treasury(plan_id_u256)
            .call()
            .await?;

        if principal_sent {
            // If principal was sent, check if returns were funded
            let returns_funded = contract
                .returns_funded(plan_id_u256)
                .call()
                .await?;

            if !returns_funded {
                tracing::warn!(
                    "Plan {} has principalSentToTreasury=true but returnsFunded=false. Waiting for fundPlanReturns.",
                    product_id
                );
                return Err(anyhow::anyhow!(
                    "Plan {} requires fundPlanReturns before settlement (principal was sent to treasury)",
                    product_id
                ));
            }
        }

        tracing::info!("Calling closePlan for plan {}", product_id);

        let tx = contract
            .close_plan(plan_id_u256)
            .send()
            .await?
            .await?;

        let tx_hash = tx.ok_or_else(|| anyhow::anyhow!("Transaction failed"))?.transaction_hash;
        tracing::info!("closePlan tx: {:?}", tx_hash);

        Ok(tx_hash)
    }


    /// Check if a plan can be auto-settled by the scheduler
    /// Returns (can_settle, reason)
    pub async fn can_auto_settle(&self, product_id: i64) -> anyhow::Result<(bool, String)> {
        let contract = self.contract.as_ref()
            .ok_or_else(|| anyhow::anyhow!("Contract not configured"))?;

        let plan_id_u256 = U256::from(product_id as u64);

        let principal_sent = contract
            .principal_sent_to_treasury(plan_id_u256)
            .call()
            .await?;

        if principal_sent {
            let returns_funded = contract
                .returns_funded(plan_id_u256)
                .call()
                .await?;

            if !returns_funded {
                return Ok((false, "Waiting for fundPlanReturns (principal was sent to treasury)".to_string()));
            }
        }

        Ok((true, "Ready for settlement".to_string()))
    }
}

// ============================================
// UTILITY FUNCTIONS
// ============================================

/// Convert Decimal to wei (U256)
fn decimal_to_wei(value: Decimal, decimals: u8) -> U256 {
    let multiplier = Decimal::new(10i64.pow(decimals as u32), 0);
    let wei_value = value * multiplier;
    let wei_u128 = wei_value.to_u128().unwrap_or(0);
    U256::from(wei_u128)
}

/// Convert wei (U256) to Decimal (with division by decimals)
#[allow(dead_code)]
fn wei_to_decimal(wei: U256, decimals: u8) -> Decimal {
    let wei_str = wei.to_string();
    let divisor = Decimal::new(10i64.pow(decimals as u32), 0);

    match Decimal::from_str(&wei_str) {
        Ok(wei_decimal) => wei_decimal / divisor,
        Err(_) => Decimal::ZERO,
    }
}

/// Convert U256 to Decimal without any conversion (keep raw precision)
/// Frontend is responsible for display formatting
fn u256_to_decimal(value: U256) -> Decimal {
    let value_str = value.to_string();
    Decimal::from_str(&value_str).unwrap_or(Decimal::ZERO)
}
