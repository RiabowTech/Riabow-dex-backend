#![allow(dead_code)]
//! Referral Service
//!
//! Manages referral codes, relationships, commission calculations,
//! and on-chain synchronization with ZtdxRewardRouter contract.
//!
//! Contract addresses are configured via environment variables:
//! - REFERRAL_STORAGE_ADDRESS: AffiliateRegistry contract address
//! - REFERRAL_REBATE_ADDRESS: ZtdxRewardRouter contract address
//!
//! See .env.mainnet and .env.sepolia for environment-specific addresses.

use ethers::prelude::*;
use ethers::abi::{encode, Token};
use ethers::providers::{Http, Provider};
use ethers::signers::{LocalWallet, Signer};
use ethers::types::{Address, U256, TransactionReceipt};
use ethers::utils::keccak256;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use sqlx::PgPool;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::Instrument;

// Generate contract bindings for ZtdxRewardRouter
abigen!(
    ReferralRebateContract,
    r#"[
        function redeemReward(uint256 amount, uint256 deadline, bytes signature) external
        function batchSettleRewards(address[] users, uint256[] amounts, uint256 batchId) external
        function rewardAccountInfo(address user) external view returns (uint256 redeemed, uint256 nonce, bytes32 affiliateCode, address affiliate, uint256 tierLevel)
        function affiliateRewardInfo(address trader) external view returns (bytes32 code, address affiliate, uint256 totalRebateBps, uint256 traderDiscountBps, uint256 affiliateRewardBps)
        function redeemedRewards(address user) external view returns (uint256)
        function rewardNonces(address user) external view returns (uint256)
        function authorizationSigner() external view returns (address)
        event RewardRedeemed(address indexed user, uint256 amount, uint256 nonce)
        event RewardBatchSettled(uint256 indexed batchId, uint256 totalAmount, uint256 userCount)
    ]"#
);

// Generate contract bindings for AffiliateRegistry
abigen!(
    ReferralStorageContract,
    r#"[
        function createAffiliateCode(bytes32 code) external
        function attachTraderCode(address account, bytes32 code) external
        function configureTier(uint256 tierId, uint256 totalRebate, uint256 discountShare) external
        function assignAffiliateTier(address referrer, uint256 tierId) external
        function codeOwnerOf(bytes32 code) external view returns (address)
        function traderCodeOf(address account) external view returns (bytes32)
        function affiliateTiers(address account) external view returns (uint256)
        function tierSettings(uint256 tierLevel) external view returns (uint256 totalRebate, uint256 discountShare)
        function resolveTraderAffiliate(address account) external view returns (bytes32 code, address affiliate)
        event AffiliateCodeCreated(bytes32 indexed code, address indexed owner)
        event TraderAffiliateAttached(address indexed trader, bytes32 indexed code, address indexed referrer)
        event AffiliateTierConfigured(uint256 indexed tierId, uint256 totalRebate, uint256 discountShare)
        event AffiliateTierAssigned(address indexed referrer, uint256 indexed tierId)
    ]"#
);

/// Pending trade record for batch submission
#[derive(Debug, Clone)]
pub struct PendingTradeRecord {
    pub trader: String,
    pub volume_usd: Decimal,
    pub fee_usd: Decimal,
    pub trade_id: String,
}

/// On-chain referrer dashboard data
#[derive(Debug, Clone, serde::Serialize)]
pub struct OnChainDashboard {
    pub code: String,
    pub total_referees: u64,
    pub total_volume: Decimal,
    pub total_earnings: Decimal,
    pub claimed_earnings: Decimal,
    pub claimable_earnings: Decimal,
    pub current_tier: u8,
    pub current_rate_bps: u16,
}

/// User rebate info from on-chain
#[derive(Debug, Clone, serde::Serialize)]
pub struct UserRebateInfo {
    pub claimed: Decimal,
    pub nonce: u64,
    pub referral_code: String,
    pub referrer: String,
    pub tier_level: u8,
}

/// Referral info from on-chain
#[derive(Debug, Clone, serde::Serialize)]
pub struct ReferralInfo {
    pub code: String,
    pub referrer: String,
    pub total_rebate_bps: u16,
    pub trader_discount_bps: u16,
    pub affiliate_reward_bps: u16,
}

/// Claim signature result
#[derive(Debug, Clone, serde::Serialize)]
pub struct ClaimSignatureResult {
    pub amount: String,
    pub nonce: u64,
    pub deadline: u64,
    pub signature: String,
    pub contract_address: String,
}

pub struct ReferralService {
    pool: PgPool,
    rebate_contract: Option<ReferralRebateContract<SignerMiddleware<Provider<Http>, LocalWallet>>>,
    storage_contract: Option<ReferralStorageContract<Provider<Http>>>,
    signer: Option<LocalWallet>,
    pending_trades: Arc<RwLock<Vec<PendingTradeRecord>>>,
    rebate_contract_address: String,
    storage_contract_address: String,
    rpc_url: String,
    chain_id: u64,
}

impl ReferralService {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            rebate_contract: None,
            storage_contract: None,
            signer: None,
            pending_trades: Arc::new(RwLock::new(Vec::new())),
            rebate_contract_address: String::new(),
            storage_contract_address: String::new(),
            rpc_url: String::new(),
            chain_id: 0,
        }
    }

    /// Initialize with contract connections
    pub async fn with_contracts(
        pool: PgPool,
        rpc_url: &str,
        rebate_contract_address: &str,
        storage_contract_address: &str,
        private_key: &str,
        chain_id: u64,
    ) -> anyhow::Result<Self> {
        let provider = Provider::<Http>::try_from(rpc_url)?;
        let wallet = private_key.parse::<LocalWallet>()?.with_chain_id(chain_id);
        let signer_client = SignerMiddleware::new(provider.clone(), wallet.clone());
        let signer_client = Arc::new(signer_client);
        let provider = Arc::new(provider);

        let rebate_addr = Address::from_str(rebate_contract_address)?;
        let storage_addr = Address::from_str(storage_contract_address)?;

        let rebate_contract = ReferralRebateContract::new(rebate_addr, signer_client);
        let storage_contract = ReferralStorageContract::new(storage_addr, provider);

        tracing::info!(
            "ReferralService initialized with contracts: rebate={}, storage={}",
            rebate_contract_address,
            storage_contract_address
        );

        Ok(Self {
            pool,
            rebate_contract: Some(rebate_contract),
            storage_contract: Some(storage_contract),
            signer: Some(wallet),
            pending_trades: Arc::new(RwLock::new(Vec::new())),
            rebate_contract_address: rebate_contract_address.to_string(),
            storage_contract_address: storage_contract_address.to_string(),
            rpc_url: rpc_url.to_string(),
            chain_id,
        })
    }

    /// Ensure chain_sync columns exist on referral_earnings table
    pub async fn ensure_schema(&self) -> anyhow::Result<()> {
        sqlx::query(r#"
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'referral_earnings' AND column_name = 'chain_sync_status'
                ) THEN
                    ALTER TABLE referral_earnings ADD COLUMN chain_sync_status VARCHAR(10) NOT NULL DEFAULT 'pending';
                    ALTER TABLE referral_earnings ADD COLUMN chain_sync_tx TEXT;
                    ALTER TABLE referral_earnings ADD COLUMN chain_sync_error TEXT;
                    ALTER TABLE referral_earnings ADD COLUMN chain_sync_attempts INTEGER NOT NULL DEFAULT 0;
                    CREATE INDEX IF NOT EXISTS idx_referral_earnings_sync ON referral_earnings(chain_sync_status);
                END IF;
            END $$;
        "#)
        .execute(&self.pool)
        .await?;
        tracing::info!("referral_earnings chain_sync schema ensured");
        Ok(())
    }

    /// For backwards compatibility - DEPRECATED
    /// Use with_contracts() instead and provide both contract addresses from environment config
    #[deprecated(note = "Use with_contracts() instead with both addresses from environment config")]
    pub async fn with_contract(
        pool: PgPool,
        rpc_url: &str,
        rebate_contract_address: &str,
        private_key: &str,
        chain_id: u64,
    ) -> anyhow::Result<Self> {
        // Storage address must be provided via REFERRAL_STORAGE_ADDRESS env var
        let storage_address = std::env::var("REFERRAL_STORAGE_ADDRESS")
            .map_err(|_| anyhow::anyhow!("REFERRAL_STORAGE_ADDRESS environment variable is required"))?;

        Self::with_contracts(
            pool,
            rpc_url,
            rebate_contract_address,
            &storage_address,
            private_key,
            chain_id,
        ).await
    }

    /// Calculate referral commission from a trade
    pub fn calculate_commission(trade_fee: Decimal, tier: ReferralTier) -> Decimal {
        trade_fee * Decimal::new(tier.commission_rate_bps() as i64, 4)
    }

    /// Get referral tier based on dual AND criteria (referral count + referred volume).
    ///
    /// Returns `None` when neither the Starter threshold is met — callers should
    /// surface an error to the user in that case.
    pub fn get_tier(referral_count: i64, total_referred_volume: Decimal) -> Option<ReferralTier> {
        if referral_count >= 100 && total_referred_volume >= Decimal::new(2_000_000, 0) {
            Some(ReferralTier::Diamond)
        } else if referral_count >= 50 && total_referred_volume >= Decimal::new(500_000, 0) {
            Some(ReferralTier::Gold)
        } else if referral_count >= 20 && total_referred_volume >= Decimal::new(100_000, 0) {
            Some(ReferralTier::Silver)
        } else if referral_count >= 5 && total_referred_volume >= Decimal::new(10_000, 0) {
            Some(ReferralTier::Bronze)
        } else if referral_count >= 1 && total_referred_volume >= Decimal::new(1_000, 0) {
            Some(ReferralTier::Starter)
        } else {
            None
        }
    }

    /// Queue a trade for batch submission to on-chain contract
    pub async fn queue_trade(&self, trade: PendingTradeRecord) {
        let mut pending = self.pending_trades.write().await;
        pending.push(trade);
        tracing::debug!("Trade queued for on-chain sync. Queue size: {}", pending.len());
    }

    /// Submit pending rebates to on-chain contract using batchSyncRebates.
    ///
    /// Sources: in-memory queue AND DB rows with chain_sync_status = 'pending'.
    pub async fn submit_pending_trades(&self, batch_id: u64) -> anyhow::Result<Option<TransactionReceipt>> {
        let contract = match &self.rebate_contract {
            Some(c) => c,
            None => {
                tracing::warn!("ZtdxRewardRouter contract not configured, skipping on-chain sync");
                return Ok(None);
            }
        };

        // 1. Drain in-memory queue
        let mem_trades: Vec<PendingTradeRecord> = {
            let mut pending = self.pending_trades.write().await;
            std::mem::take(&mut *pending)
        };

        // 2. Query DB for pending earnings not yet synced (max 3 attempts)
        use std::collections::HashMap;
        let mut user_rebates: HashMap<Address, U256> = HashMap::new();

        // Aggregate memory queue
        for trade in &mem_trades {
            if let Ok(addr) = Address::from_str(&trade.trader) {
                let fee_wei = decimal_to_wei(trade.fee_usd, 6);
                *user_rebates.entry(addr).or_insert(U256::zero()) += fee_wei;
            }
        }

        // Aggregate from DB
        let db_rows: Vec<(String, Decimal)> = sqlx::query_as(
            "SELECT referrer_address, SUM(commission) as total FROM referral_earnings WHERE chain_sync_status = 'pending' AND chain_sync_attempts < 3 GROUP BY referrer_address"
        )
        .fetch_all(&self.pool)
        .await
        .unwrap_or_default();

        for (addr_str, total) in &db_rows {
            if let Ok(addr) = Address::from_str(addr_str) {
                let fee_wei = decimal_to_wei(*total, 6);
                *user_rebates.entry(addr).or_insert(U256::zero()) += fee_wei;
            }
        }

        if user_rebates.is_empty() {
            tracing::debug!("No pending trades to submit");
            return Ok(None);
        }

        let users: Vec<Address> = user_rebates.keys().cloned().collect();
        let amounts: Vec<U256> = user_rebates.values().cloned().collect();

        tracing::info!(
            "Submitting {} user rebates to ZtdxRewardRouter contract (batch_id={}, mem={}, db_groups={})",
            users.len(), batch_id, mem_trades.len(), db_rows.len()
        );

        // Submit batch transaction
        let call = contract.batch_settle_rewards(users.clone(), amounts.clone(), U256::from(batch_id));
        let pending_tx = match call.send().await {
            Ok(tx) => tx,
            Err(e) => {
                tracing::error!("Failed to submit batchSettleRewards tx: {}", e);
                // Re-queue memory trades
                let mut pending = self.pending_trades.write().await;
                pending.extend(mem_trades);
                // Increment DB attempts
                let _ = sqlx::query(
                    "UPDATE referral_earnings SET chain_sync_attempts = chain_sync_attempts + 1, chain_sync_error = $1 WHERE chain_sync_status = 'pending' AND chain_sync_attempts < 3"
                )
                .bind(format!("send failed: {}", e))
                .execute(&self.pool)
                .await;
                // Mark as failed if attempts >= 3
                let _ = sqlx::query(
                    "UPDATE referral_earnings SET chain_sync_status = 'failed' WHERE chain_sync_status = 'pending' AND chain_sync_attempts >= 3"
                )
                .execute(&self.pool)
                .await;
                return Err(e.into());
            }
        };

        let tx_hash = pending_tx.tx_hash();
        tracing::info!("batchSettleRewards tx submitted: {:?}", tx_hash);

        // Wait for confirmation
        match pending_tx.await {
            Ok(Some(receipt)) => {
                tracing::info!(
                    "batchSettleRewards tx confirmed in block {:?}, gas used: {:?}",
                    receipt.block_number,
                    receipt.gas_used
                );

                let tx_hash_str = format!("{:?}", receipt.transaction_hash);

                // Mark DB rows as synced
                let _ = sqlx::query(
                    "UPDATE referral_earnings SET chain_sync_status = 'synced', chain_sync_tx = $1 WHERE chain_sync_status = 'pending'"
                )
                .bind(&tx_hash_str)
                .execute(&self.pool)
                .await;

                Ok(Some(receipt))
            }
            Ok(None) => {
                tracing::warn!("Transaction dropped from mempool");
                // Re-queue memory trades
                let mut pending = self.pending_trades.write().await;
                pending.extend(mem_trades);
                // Increment DB attempts
                let _ = sqlx::query(
                    "UPDATE referral_earnings SET chain_sync_attempts = chain_sync_attempts + 1, chain_sync_error = 'tx dropped from mempool' WHERE chain_sync_status = 'pending' AND chain_sync_attempts < 3"
                )
                .execute(&self.pool)
                .await;
                let _ = sqlx::query(
                    "UPDATE referral_earnings SET chain_sync_status = 'failed' WHERE chain_sync_status = 'pending' AND chain_sync_attempts >= 3"
                )
                .execute(&self.pool)
                .await;
                Ok(None)
            }
            Err(e) => {
                tracing::error!("Transaction failed: {}", e);
                // Re-queue memory trades
                let mut pending = self.pending_trades.write().await;
                pending.extend(mem_trades);
                // Increment DB attempts + mark failed
                let _ = sqlx::query(
                    "UPDATE referral_earnings SET chain_sync_attempts = chain_sync_attempts + 1, chain_sync_error = $1 WHERE chain_sync_status = 'pending' AND chain_sync_attempts < 3"
                )
                .bind(format!("tx failed: {}", e))
                .execute(&self.pool)
                .await;
                let _ = sqlx::query(
                    "UPDATE referral_earnings SET chain_sync_status = 'failed' WHERE chain_sync_status = 'pending' AND chain_sync_attempts >= 3"
                )
                .execute(&self.pool)
                .await;
                Err(e.into())
            }
        }
    }

    /// Get user rebate info from on-chain contract
    pub async fn get_user_rebate_info(&self, user: &str) -> anyhow::Result<UserRebateInfo> {
        let contract = match &self.rebate_contract {
            Some(c) => c,
            None => anyhow::bail!("ZtdxRewardRouter contract not configured"),
        };

        let user_addr = Address::from_str(user)?;
        let (claimed, nonce, referral_code, referrer, tier_level) =
            contract.reward_account_info(user_addr).call().await?;

        Ok(UserRebateInfo {
            claimed: wei_to_decimal(claimed, 6),
            nonce: nonce.as_u64(),
            referral_code: bytes32_to_string(referral_code),
            referrer: format!("{:?}", referrer),
            tier_level: tier_level.as_u64() as u8,
        })
    }

    /// Get referral info for a trader from on-chain contract
    pub async fn get_referral_info(&self, trader: &str) -> anyhow::Result<ReferralInfo> {
        let contract = match &self.rebate_contract {
            Some(c) => c,
            None => anyhow::bail!("ZtdxRewardRouter contract not configured"),
        };

        let trader_addr = Address::from_str(trader)?;
        let (code, referrer, total_rebate_bps, trader_discount_bps, affiliate_reward_bps) =
            contract.affiliate_reward_info(trader_addr).call().await?;

        Ok(ReferralInfo {
            code: bytes32_to_string(code),
            referrer: format!("{:?}", referrer),
            total_rebate_bps: total_rebate_bps.as_u64() as u16,
            trader_discount_bps: trader_discount_bps.as_u64() as u16,
            affiliate_reward_bps: affiliate_reward_bps.as_u64() as u16,
        })
    }

    /// Get claimed rebate amount for a user
    pub async fn get_claimed_rebates(&self, user: &str) -> anyhow::Result<Decimal> {
        let contract = match &self.rebate_contract {
            Some(c) => c,
            None => return Ok(Decimal::ZERO),
        };

        let user_addr = Address::from_str(user)?;
        let claimed: U256 = contract.redeemed_rewards(user_addr).call().await?;

        // Convert from 6 decimals (USDT) to Decimal
        Ok(wei_to_decimal(claimed, 6))
    }

    /// Get reward nonce for a user (used for redemption signature)
    pub async fn get_reward_nonce(&self, user: &str) -> anyhow::Result<u64> {
        let contract = match &self.rebate_contract {
            Some(c) => c,
            None => return Ok(0),
        };

        let user_addr = Address::from_str(user)?;
        let nonce: U256 = contract.reward_nonces(user_addr).call().await?;

        Ok(nonce.as_u64())
    }

    /// Generate EIP-712 signature for reward redemption (ZtdxRewardRouter)
    ///
    /// EIP-712 Domain:
    /// - name: "ZTDX Reward Router" (configurable via EIP712_REFERRAL_DOMAIN_NAME)
    /// - version: "1"
    /// - chainId: from config
    /// - verifyingContract: ZtdxRewardRouter contract address
    pub async fn generate_claim_signature(
        &self,
        user: &str,
        amount: Decimal,
        deadline_secs: u64,
    ) -> anyhow::Result<ClaimSignatureResult> {
        let signer = match &self.signer {
            Some(s) => s,
            None => anyhow::bail!("Signer not configured"),
        };

        // Get current nonce from on-chain
        let nonce = self.get_reward_nonce(user).await?;

        // Convert amount to 6 decimals (USDT)
        let amount_wei = decimal_to_wei(amount, 6);
        let amount_str = amount_wei.to_string();

        // Calculate deadline
        let deadline = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs()
            + deadline_secs;

        // EIP-712 Domain Separator
        // keccak256("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)")
        let domain_type_hash = keccak256(
            b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"
        );

        // Domain name pinned to the deployed ZtdxRewardRouter contract.
        // See constants/eip712_domains.rs — DO NOT RENAME without a
        // coordinated contract redeploy.
        use crate::constants::eip712_domains::{referral_rebate_domain_name, domain_version};
        let name_hash = keccak256(referral_rebate_domain_name().as_bytes());
        let version_hash = keccak256(domain_version().as_bytes());
        let chain_id = U256::from(self.chain_id);
        let verifying_contract = Address::from_str(&self.rebate_contract_address)?;

        let domain_separator = keccak256(encode(&[
            Token::FixedBytes(domain_type_hash.to_vec()),
            Token::FixedBytes(name_hash.to_vec()),
            Token::FixedBytes(version_hash.to_vec()),
            Token::Uint(chain_id),
            Token::Address(verifying_contract),
        ]));

        // RedeemReward struct type hash
        // keccak256("RedeemReward(address account,uint256 value,uint256 nonce,uint256 deadline)")
        let redeem_type_hash = keccak256(
            b"RedeemReward(address account,uint256 value,uint256 nonce,uint256 deadline)"
        );

        let user_addr = Address::from_str(user)?;
        let struct_hash = keccak256(encode(&[
            Token::FixedBytes(redeem_type_hash.to_vec()),
            Token::Address(user_addr),
            Token::Uint(amount_wei),
            Token::Uint(U256::from(nonce)),
            Token::Uint(U256::from(deadline)),
        ]));

        // EIP-712 message hash: keccak256("\x19\x01" || domainSeparator || structHash)
        let mut message = Vec::with_capacity(66);
        message.extend_from_slice(b"\x19\x01");
        message.extend_from_slice(&domain_separator);
        message.extend_from_slice(&struct_hash);
        let message_hash = keccak256(&message);

        // Sign the message hash
        let signature = signer.sign_hash(H256::from(message_hash))?;
        let sig_bytes = signature.to_vec();
        let signature_hex = format!("0x{}", hex::encode(&sig_bytes));

        tracing::info!(
            "Generated claim signature for user={}, amount={}, nonce={}, deadline={}",
            user, amount, nonce, deadline
        );

        Ok(ClaimSignatureResult {
            amount: amount_str,
            nonce,
            deadline,
            signature: signature_hex,
            contract_address: self.rebate_contract_address.clone(),
        })
    }

    /// Start the batch submission loop (runs every 5 minutes)
    pub async fn start_batch_sync_loop(self: Arc<Self>) {
        let service = self.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(300)); // 5 minutes
            let mut batch_id: u64 = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);

            loop {
                interval.tick().await;

                tracing::info!("Running scheduled referral trade sync (batch_id={})...", batch_id);

                match service.submit_pending_trades(batch_id).await {
                    Ok(Some(receipt)) => {
                        tracing::info!(
                            "Scheduled sync completed. TX: {:?}",
                            receipt.transaction_hash
                        );
                        batch_id += 1;
                    }
                    Ok(None) => {
                        tracing::debug!("No trades to sync");
                    }
                    Err(e) => {
                        tracing::error!("Scheduled sync failed: {}", e);
                    }
                }
            }
        }.instrument(tracing::info_span!("referral-batch-sync")));

        // Spawn RewardRedeemed event listener (polls every 30 seconds)
        {
            let pool = self.pool.clone();
            let rebate_addr = self.rebate_contract_address.clone();
            let rpc_url = self.rpc_url.clone();

            tokio::spawn(async move {
                if rebate_addr.is_empty() || rpc_url.is_empty() {
                    tracing::warn!("RewardRedeemed event listener not started: missing rpc_url or rebate_contract_address");
                    return;
                }

                let provider = match Provider::<Http>::try_from(&rpc_url) {
                    Ok(p) => Arc::new(p),
                    Err(e) => {
                        tracing::error!("Failed to create provider for event listener: {}", e);
                        return;
                    }
                };

                let contract = ReferralRebateContract::new(
                    rebate_addr.parse::<Address>().unwrap(),
                    provider.clone(),
                );

                let mut last_block = provider.get_block_number().await.unwrap_or_default();
                tracing::info!("RewardRedeemed event listener started from block {}", last_block);

                loop {
                    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;

                    let current_block = match provider.get_block_number().await {
                        Ok(b) => b,
                        Err(_) => continue,
                    };

                    if current_block <= last_block {
                        continue;
                    }

                    let events = contract
                        .reward_redeemed_filter()
                        .from_block(last_block + 1)
                        .to_block(current_block)
                        .query()
                        .await;

                    match events {
                        Ok(logs) => {
                            for log in logs {
                                let user = format!("{:?}", log.user).to_lowercase();
                                tracing::info!("RewardRedeemed event: user={}, amount={}, nonce={}", user, log.amount, log.nonce);

                                let _ = sqlx::query(
                                    "UPDATE referral_earnings SET status = 'claimed', claimed_at = NOW() WHERE referrer_address = $1 AND status = 'pending' AND chain_sync_status = 'synced'"
                                )
                                .bind(&user)
                                .execute(&pool)
                                .await;
                            }
                        }
                        Err(e) => {
                            tracing::warn!("Failed to query RewardRedeemed events: {}", e);
                        }
                    }

                    last_block = current_block;
                }
            }.instrument(tracing::info_span!("referral-event-listener")));
        }

        tracing::info!("Referral batch sync loop started (interval: 5 minutes)");
    }

    /// Check if operator is set for the backend address
    pub async fn check_operator_status(&self, operator: &str) -> anyhow::Result<bool> {
        let contract = match &self.rebate_contract {
            Some(c) => c,
            None => return Ok(false),
        };

        // Get the authorization signer address from contract
        let backend_signer: Address = contract.authorization_signer().call().await?;

        // Compare with the provided operator address (case-insensitive)
        let operator_addr = Address::from_str(operator)?;
        Ok(backend_signer == operator_addr)
    }

    /// Get authorization signer address from on-chain contract
    pub async fn get_authorization_signer(&self) -> anyhow::Result<String> {
        let contract = match &self.rebate_contract {
            Some(c) => c,
            None => anyhow::bail!("ZtdxRewardRouter contract not configured"),
        };

        let signer_addr: Address = contract.authorization_signer().call().await?;
        Ok(format!("{:#x}", signer_addr))
    }

    /// Get storage contract address
    pub fn get_storage_contract_address(&self) -> &str {
        &self.storage_contract_address
    }

    /// Get rebate contract address
    pub fn get_rebate_contract_address(&self) -> &str {
        &self.rebate_contract_address
    }

    /// Update authorization signer address in contract (requires owner permission)
    pub async fn update_authorization_signer(
        &self,
        rpc_url: &str,
        chain_id: u64,
        owner_private_key: &str,
        new_signer: &str
    ) -> anyhow::Result<String> {
        use ethers::providers::{Http, Provider};
        use ethers::signers::{LocalWallet, Signer};
        use ethers::middleware::SignerMiddleware;

        // Parse addresses
        let new_signer_addr = Address::from_str(new_signer)?;

        // Create wallet from owner private key
        let wallet: LocalWallet = owner_private_key.parse()?;
        tracing::info!("Using wallet address: {:?} to update authorization signer", wallet.address());

        // Create provider and client
        let provider = Provider::<Http>::try_from(rpc_url)?;
        let client = Arc::new(SignerMiddleware::new(
            provider,
            wallet.with_chain_id(chain_id),
        ));

        // Create contract instance with signer
        abigen!(
            ZtdxRewardRouterWithSigner,
            r#"[
                function setAuthorizationSigner(address newSigner) external
                function owner() external view returns (address)
            ]"#
        );

        let contract_addr = Address::from_str(&self.rebate_contract_address)?;
        let contract = ZtdxRewardRouterWithSigner::new(contract_addr, client.clone());

        // First check if we have owner permission
        let owner_addr = contract.owner().call().await?;
        tracing::info!("Contract owner: {:?}, Our address: {:?}", owner_addr, client.address());

        if owner_addr != client.address() {
            anyhow::bail!("Wallet does not have owner permission. Owner is {:?}, we are {:?}", owner_addr, client.address());
        }

        // Call setAuthorizationSigner
        tracing::info!("Calling setAuthorizationSigner with new signer: {:?}", new_signer_addr);
        let call_builder = contract.set_authorization_signer(new_signer_addr);
        let pending_tx = call_builder.send().await?;
        let receipt = pending_tx.await?;

        match receipt {
            Some(r) => {
                tracing::info!("Authorization signer updated successfully. Tx hash: {:?}", r.transaction_hash);
                Ok(format!("{:?}", r.transaction_hash))
            }
            None => {
                anyhow::bail!("Transaction receipt not available")
            }
        }
    }
}

/// Referral tier levels (0 = Starter … 4 = Diamond).
///
/// Upgrade condition: referral count AND total referred trading volume must
/// both meet the threshold (AND logic).
///
/// | Level | Name    | Rate | BPS  | ≥ Referrals | ≥ Volume   |
/// |-------|---------|------|------|-------------|------------|
/// |   0   | Starter | 10%  | 1000 |      1      |   $1,000   |
/// |   1   | Bronze  | 12%  | 1200 |      5      |  $10,000   |
/// |   2   | Silver  | 17%  | 1700 |     20      | $100,000   |
/// |   3   | Gold    | 22%  | 2200 |     50      | $500,000   |
/// |   4   | Diamond | 25%  | 2500 |    100      | $2,000,000 |
#[derive(Debug, Clone, Copy)]
pub enum ReferralTier {
    Starter,
    Bronze,
    Silver,
    Gold,
    Diamond,
}

impl ReferralTier {
    pub fn from_index(index: u8) -> Self {
        match index {
            0 => ReferralTier::Starter,
            1 => ReferralTier::Bronze,
            2 => ReferralTier::Silver,
            3 => ReferralTier::Gold,
            _ => ReferralTier::Diamond,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            ReferralTier::Starter => "Starter",
            ReferralTier::Bronze  => "Bronze",
            ReferralTier::Silver  => "Silver",
            ReferralTier::Gold    => "Gold",
            ReferralTier::Diamond => "Diamond",
        }
    }

    pub fn commission_rate_bps(&self) -> u16 {
        match self {
            ReferralTier::Starter => 1000,
            ReferralTier::Bronze  => 1200,
            ReferralTier::Silver  => 1700,
            ReferralTier::Gold    => 2200,
            ReferralTier::Diamond => 2500,
        }
    }
}

/// Convert Decimal to wei (U256)
fn decimal_to_wei(value: Decimal, decimals: u8) -> U256 {
    let multiplier = Decimal::new(10i64.pow(decimals as u32), 0);
    let wei_value = value * multiplier;

    // Convert to u128 first, then to U256
    let wei_u128 = wei_value.to_u128().unwrap_or(0);
    U256::from(wei_u128)
}

/// Convert wei (U256) to Decimal
fn wei_to_decimal(wei: U256, decimals: u8) -> Decimal {
    let wei_str = wei.to_string();
    let divisor = Decimal::new(10i64.pow(decimals as u32), 0);

    match Decimal::from_str(&wei_str) {
        Ok(wei_decimal) => wei_decimal / divisor,
        Err(_) => Decimal::ZERO,
    }
}

/// Convert bytes32 to string (trim null bytes)
fn bytes32_to_string(bytes: [u8; 32]) -> String {
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(32);
    String::from_utf8_lossy(&bytes[..end]).to_string()
}
