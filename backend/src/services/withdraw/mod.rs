//! Withdrawal Service
//!
//! Handles withdrawal requests and backend signature generation for ZtdxReserveVault.releaseFunds.

use ethers::abi::{encode, Token};
use ethers::contract::abigen;
use ethers::providers::{Http, Provider};
use ethers::signers::LocalWallet;
use ethers::types::{Address, U256};
use ethers::utils::keccak256;
use rust_decimal::Decimal;
use sqlx::PgPool;
use std::str::FromStr;
use std::sync::Arc;

// Note: User sends amounts in raw wei format, but database stores human-readable format
// We need to convert wei to decimal for balance comparison

// Generate contract bindings for ZtdxReserveVault
abigen!(
    VaultContract,
    r#"[
        function releaseNonces(address user) external view returns (uint256)
        function accountLiquidity(address user) external view returns (uint256)
        event AccountFunded(address indexed user, uint256 amount, bytes32 referralCode)
        event FundsReleased(address indexed user, uint256 amount, uint256 nonce)
        event AffiliateCodeBound(address indexed user, bytes32 indexed code, address indexed referrer)
    ]"#
);

pub struct WithdrawService {
    signer: LocalWallet,
    vault_address: Address,
    chain_id: u64,
    pool: PgPool,
    collateral_token_symbol: String,
    collateral_token_address: String,
    #[allow(dead_code)]
    collateral_token_decimals: u8,
    provider: Arc<Provider<Http>>,
    eip712_domain_name: String,
}

impl WithdrawService {
    pub fn new(
        private_key: &str,
        vault_address: &str,
        chain_id: u64,
        pool: PgPool,
        collateral_token_symbol: &str,
        collateral_token_address: &str,
        collateral_token_decimals: u8,
        rpc_url: &str,
        eip712_domain_name: &str,
    ) -> anyhow::Result<Self> {
        let signer = private_key.parse::<LocalWallet>()?;
        let vault_address = Address::from_str(vault_address)?;
        let provider = Provider::<Http>::try_from(rpc_url)?;

        Ok(Self {
            signer,
            vault_address,
            chain_id,
            pool,
            collateral_token_symbol: collateral_token_symbol.to_string(),
            collateral_token_address: collateral_token_address.to_lowercase(),
            collateral_token_decimals,
            provider: Arc::new(provider),
            eip712_domain_name: eip712_domain_name.to_string(),
        })
    }

    /// Get the current release nonce from the ZtdxReserveVault contract
    async fn get_contract_nonce(&self, user_address: &str) -> anyhow::Result<u64> {
        let user = Address::from_str(user_address)?;
        let contract = VaultContract::new(self.vault_address, self.provider.clone());
        let nonce = contract.release_nonces(user).call().await?;
        Ok(nonce.as_u64())
    }

    /// Convert wei to decimal (human-readable format)
    /// e.g., 10000000000 wei with 6 decimals -> 10000 USDT
    #[allow(dead_code)]
    fn wei_to_decimal(&self, amount_wei: Decimal) -> Decimal {
        let divisor = Decimal::new(10i64.pow(self.collateral_token_decimals as u32), 0);
        amount_wei / divisor
    }

    /// Convert token address to symbol, or return the input if it's already a symbol
    fn resolve_token_symbol(&self, token: &str) -> String {
        let token_lower = token.to_lowercase();
        // If it looks like an address (starts with 0x), try to map it
        if token_lower.starts_with("0x") {
            if token_lower == self.collateral_token_address {
                return self.collateral_token_symbol.clone();
            }
        }
        // Return as-is (could be a symbol already)
        token.to_uppercase()
    }

    /// Request a withdrawal - validates and creates signature
    pub async fn request_withdrawal(
        &self,
        user_address: &str,
        token: &str,
        amount: Decimal,
    ) -> anyhow::Result<WithdrawResult> {
        let user_addr = user_address.to_lowercase();
        // Convert token address to symbol if needed (e.g., 0x572E... -> USDT)
        let token_symbol = self.resolve_token_symbol(token);
        let token_input = token.to_lowercase(); // Keep original for logging

        // Frontend sends human-readable USDT amount (e.g., "1" for 1 USDT)
        // Database stores human-readable format (e.g., 1 for 1 USDT)
        // We need to convert to Wei for on-chain signature (1 USDT = 1,000,000 Wei)
        let amount_usdt = amount; // Human-readable USDT amount
        let amount_wei = amount * Decimal::from(1_000_000); // Convert to Wei for signature

        tracing::info!(
            "处理提现请求 - 用户: {}, 代币输入: {}, 解析为: {}, 金额(USDT): {}, 金额(Wei): {}",
            user_addr,
            token_input,
            token_symbol,
            amount_usdt,
            amount_wei
        );

        if amount_usdt <= Decimal::ZERO {
            anyhow::bail!("Invalid withdrawal amount");
        }

        // Check available and frozen balance (use token symbol for database query)
        let balance: Option<(Decimal, Decimal)> = sqlx::query_as(
            "SELECT available, frozen FROM balances WHERE user_address = $1 AND token = $2"
        )
        .bind(&user_addr)
        .bind(&token_symbol)
        .fetch_optional(&self.pool)
        .await?;

        let (available, frozen) = balance.unwrap_or((Decimal::ZERO, Decimal::ZERO));

        tracing::info!(
            "用户余额 - 可用: {}, 冻结: {}, 总额: {}",
            available,
            frozen,
            available + frozen
        );

        // Calculate total unrealized PnL from open positions
        // If user has losing positions, the loss should reduce withdrawable amount
        let unrealized_pnl: Decimal = sqlx::query_scalar(
            r#"
            SELECT COALESCE(SUM(unrealized_pnl), 0)
            FROM positions
            WHERE user_address = $1 AND status = 'open'
            "#
        )
        .bind(&user_addr)
        .fetch_one(&self.pool)
        .await
        .unwrap_or(Decimal::ZERO);

        // Calculate actual withdrawable amount
        // If unrealized_pnl is negative (loss), reduce available
        // If unrealized_pnl is positive (profit), don't add it (can only withdraw after closing)
        let withdrawable = if unrealized_pnl < Decimal::ZERO {
            available + unrealized_pnl // unrealized_pnl is negative, so this reduces available
        } else {
            available
        };

        tracing::info!(
            "提现验证 - 可用: {}, 未实现盈亏: {}, 实际可提: {}, 请求提现(USDT): {}",
            available, unrealized_pnl, withdrawable, amount_usdt
        );

        if amount_usdt > withdrawable {
            // 如果余额不足，查询详细信息以提供更有用的错误消息
            let pending_withdrawals: Option<Decimal> = sqlx::query_scalar(
                "SELECT COALESCE(SUM(amount), 0) FROM withdrawals WHERE user_address = $1 AND token = $2 AND status IN ('pending', 'signed', 'submitted')"
            )
            .bind(&user_addr)
            .bind(&token_symbol)
            .fetch_optional(&self.pool)
            .await?;

            let open_orders_count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM orders WHERE user_address = $1 AND status IN ('open', 'partially_filled', 'pending')"
            )
            .bind(&user_addr)
            .fetch_optional(&self.pool)
            .await?
            .unwrap_or(0);

            let open_positions_count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM positions WHERE user_address = $1 AND status = 'open'"
            )
            .bind(&user_addr)
            .fetch_optional(&self.pool)
            .await?
            .unwrap_or(0);

            let pending_amount = pending_withdrawals.unwrap_or(Decimal::ZERO);

            tracing::warn!(
                "提现余额不足 - 用户: {}, 请求(USDT): {}, 请求(Wei): {}, 可用: {}, 冻结: {}, 未实现盈亏: {}, 实际可提: {}, 待处理提现: {}, 未成交订单: {}, 持仓: {}",
                user_addr,
                amount_usdt,
                amount_wei,
                available,
                frozen,
                unrealized_pnl,
                withdrawable,
                pending_amount,
                open_orders_count,
                open_positions_count
            );

            let mut error_msg = format!(
                "余额不足。请求提现: {} USDT, 实际可提: {} USDT (可用: {}, 未实现盈亏: {})",
                amount_usdt, withdrawable, available, unrealized_pnl
            );

            if frozen > Decimal::ZERO || unrealized_pnl < Decimal::ZERO {
                error_msg.push_str("。原因: ");
                let mut reasons = Vec::new();
                if pending_amount > Decimal::ZERO {
                    reasons.push(format!("待处理提现 {}", pending_amount));
                }
                if open_orders_count > 0 {
                    reasons.push(format!("{} 个未成交订单", open_orders_count));
                }
                if open_positions_count > 0 {
                    reasons.push(format!("{} 个持仓", open_positions_count));
                }
                if unrealized_pnl < Decimal::ZERO {
                    reasons.push(format!("未实现亏损 {}", unrealized_pnl.abs()));
                }
                error_msg.push_str(&reasons.join(", "));
            }

            anyhow::bail!(error_msg);
        }

        // Get current nonce from the contract (not from database)
        let nonce = self.get_contract_nonce(&user_addr).await? as i64;

        tracing::info!(
            "从 ZtdxReserveVault 合约获取 release nonce - 用户: {}, nonce: {}",
            user_addr,
            nonce
        );

        // Set expiry to 1 hour from now
        let expiry = chrono::Utc::now().timestamp() + 3600;

        // Get the actual token contract address for signature
        // If input was an address, use it; otherwise use collateral token address
        let token_address_for_sig = if token_input.starts_with("0x") {
            token_input.clone()
        } else {
            self.collateral_token_address.clone()
        };

        // Convert Wei amount to U256 for signature
        // amount_wei is already calculated above (amount_usdt * 1_000_000)
        let amount_wei_u256 = U256::from_dec_str(&amount_wei.trunc().to_string()).unwrap_or_default();

        // Generate signature using token contract address and Wei amount
        let signature = self.sign_withdrawal_raw(
            &user_addr,
            &token_address_for_sig,
            amount_wei_u256,
            nonce,
            expiry,
        ).await?;

        // Start transaction
        let mut tx = self.pool.begin().await?;

        // Freeze the withdrawal amount (use human-readable USDT format for database)
        sqlx::query(
            r#"
            UPDATE balances
            SET available = available - $1, frozen = frozen + $1
            WHERE user_address = $2 AND token = $3
            "#
        )
        .bind(amount_usdt)
        .bind(&user_addr)
        .bind(&token_symbol)
        .execute(&mut *tx)
        .await?;

        // Insert withdrawal record (store token symbol and human-readable USDT amount in database)
        let withdrawal_id: uuid::Uuid = sqlx::query_scalar(
            r#"
            INSERT INTO withdrawals (user_address, token, amount, to_address, nonce, expiry, backend_signature, status)
            VALUES ($1, $2, $3, $1, $4, $5, $6, 'signed')
            RETURNING id
            "#
        )
        .bind(&user_addr)
        .bind(&token_symbol)
        .bind(amount_usdt)
        .bind(nonce)
        .bind(expiry)
        .bind(&signature)
        .fetch_one(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(WithdrawResult {
            withdrawal_id: withdrawal_id.to_string(),
            token: token_address_for_sig, // Return the contract address for on-chain use
            amount: amount_wei.trunc().to_string(), // Return Wei amount for on-chain use
            nonce,
            expiry,
            signature,
            vault_address: format!("{:?}", self.vault_address),
        })
    }

    /// Generate backend signature for ZtdxReserveVault.releaseFunds using EIP-712 (raw wei amount).
    ///
    /// Typehash: `ReleaseFunds(address account,uint256 value,uint256 nonce,uint256 deadline)`
    async fn sign_withdrawal_raw(
        &self,
        user_address: &str,
        token: &str,
        amount_wei: U256,
        nonce: i64,
        expiry: i64,
    ) -> anyhow::Result<String> {
        let user = Address::from_str(user_address)?;
        // Note: token is not used in the signature (contract uses single collateral token)
        let _ = token;

        // EIP-712 Domain Separator
        // keccak256("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)")
        let domain_type_hash = keccak256(
            b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"
        );

        let domain_separator = keccak256(&encode(&[
            Token::FixedBytes(domain_type_hash.to_vec()),
            Token::FixedBytes(keccak256(self.eip712_domain_name.as_bytes()).to_vec()), // Must match contract DOMAIN_SEPARATOR
            Token::FixedBytes(keccak256(b"1").to_vec()),
            Token::Uint(U256::from(self.chain_id)),
            Token::Address(self.vault_address),
        ]));

        // EIP-712 ReleaseFunds Struct Hash
        // keccak256("ReleaseFunds(address account,uint256 value,uint256 nonce,uint256 deadline)")
        // Note: contract uses `account`/`value` field names (not `user`/`amount`), and does NOT include token
        let release_type_hash = keccak256(
            b"ReleaseFunds(address account,uint256 value,uint256 nonce,uint256 deadline)"
        );

        let struct_hash = keccak256(&encode(&[
            Token::FixedBytes(release_type_hash.to_vec()),
            Token::Address(user),
            Token::Uint(amount_wei),
            Token::Uint(U256::from(nonce as u64)),
            Token::Uint(U256::from(expiry as u64)),
        ]));

        // EIP-712 Final Hash: keccak256("\x19\x01" + domainSeparator + structHash)
        let mut digest_input = Vec::with_capacity(66);
        digest_input.push(0x19);
        digest_input.push(0x01);
        digest_input.extend_from_slice(&domain_separator);
        digest_input.extend_from_slice(&struct_hash);
        let digest = keccak256(&digest_input);

        // Sign the digest directly (no eth_sign prefix for EIP-712)
        let signature = self.signer.sign_hash(digest.into())?;

        tracing::debug!(
            "EIP-712 ReleaseFunds signature: user={}, amount={}, nonce={}, expiry={}, domain_separator={}, struct_hash={}, digest={}",
            user_address,
            amount_wei,
            nonce,
            expiry,
            hex::encode(domain_separator),
            hex::encode(struct_hash),
            hex::encode(digest)
        );

        Ok(format!("0x{}", hex::encode(signature.to_vec())))
    }

    /// Cancel a pending withdrawal (return frozen funds)
    pub async fn cancel_withdrawal(&self, user_address: &str, withdrawal_id: &str) -> anyhow::Result<()> {
        let user_addr = user_address.to_lowercase();

        // Parse withdrawal_id as UUID
        let withdrawal_uuid = uuid::Uuid::parse_str(withdrawal_id)
            .map_err(|_| anyhow::anyhow!("Invalid withdrawal ID format"))?;

        let mut tx = self.pool.begin().await?;

        // Get withdrawal details
        let withdrawal: Option<(String, Decimal)> = sqlx::query_as(
            "SELECT token, amount FROM withdrawals WHERE id = $1 AND user_address = $2 AND status = 'signed'"
        )
        .bind(withdrawal_uuid)
        .bind(&user_addr)
        .fetch_optional(&mut *tx)
        .await?;

        let (token, amount) = withdrawal.ok_or_else(|| anyhow::anyhow!("Withdrawal not found or already processed"))?;

        // Unfreeze the amount
        sqlx::query(
            r#"
            UPDATE balances
            SET available = available + $1, frozen = frozen - $1
            WHERE user_address = $2 AND token = $3
            "#
        )
        .bind(amount)
        .bind(&user_addr)
        .bind(&token)
        .execute(&mut *tx)
        .await?;

        // Update withdrawal status
        sqlx::query(
            "UPDATE withdrawals SET status = 'cancelled' WHERE id = $1"
        )
        .bind(withdrawal_uuid)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(())
    }

    /// Get withdrawal history for a user
    pub async fn get_history(&self, user_address: &str) -> anyhow::Result<Vec<WithdrawRecord>> {
        let user_addr = user_address.to_lowercase();
        
        tracing::info!(
            "查询提现历史 - 原始地址: {}, 小写地址: {}",
            user_address,
            user_addr
        );

        // 先查询总数，用于调试
        let total_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM withdrawals WHERE user_address = $1"
        )
        .bind(&user_addr)
        .fetch_one(&self.pool)
        .await?;
        
        tracing::info!(
            "用户 {} 的提现记录数: {}",
            user_addr,
            total_count
        );
        
        // 如果没有记录，检查是否有地址格式问题
        if total_count == 0 {
            let total_any: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM withdrawals"
            )
            .fetch_one(&self.pool)
            .await?;
            
            tracing::warn!(
                "用户 {} 没有提现记录，但数据库中总共有 {} 条提现记录",
                user_addr,
                total_any
            );
            
            // 检查是否存在大小写不同的地址
            let similar_count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM withdrawals WHERE LOWER(user_address) = $1"
            )
            .bind(&user_addr)
            .fetch_one(&self.pool)
            .await?;
            
            if similar_count > 0 {
                tracing::error!(
                    "发现地址格式问题！LOWER(user_address) 匹配到 {} 条记录，但直接匹配为 0",
                    similar_count
                );
            }
        }

        let records = sqlx::query_as::<_, WithdrawRecord>(
            r#"
            SELECT id::text, token, amount, nonce, expiry, backend_signature, tx_hash, status::text,
                   EXTRACT(EPOCH FROM created_at)::bigint as created_at
            FROM withdrawals
            WHERE user_address = $1
            ORDER BY created_at DESC
            LIMIT 100
            "#
        )
        .bind(&user_addr)
        .fetch_all(&self.pool)
        .await?;
        
        tracing::info!("成功返回 {} 条提现记录", records.len());

        Ok(records)
    }

    /// Get single withdrawal details
    pub async fn get_withdrawal(
        &self,
        user_address: &str,
        withdrawal_id: &str,
    ) -> anyhow::Result<WithdrawRecord> {
        let user_addr = user_address.to_lowercase();

        // Parse withdrawal_id as UUID
        let withdrawal_uuid = uuid::Uuid::parse_str(withdrawal_id)
            .map_err(|_| anyhow::anyhow!("Invalid withdrawal ID format"))?;

        tracing::info!(
            "查询提现详情 - 用户: {}, 提现ID: {}",
            user_addr,
            withdrawal_id
        );

        let record = sqlx::query_as::<_, WithdrawRecord>(
            r#"
            SELECT id::text, token, amount, nonce, expiry, backend_signature, tx_hash, status::text,
                   EXTRACT(EPOCH FROM created_at)::bigint as created_at
            FROM withdrawals
            WHERE id = $1 AND user_address = $2
            "#
        )
        .bind(withdrawal_uuid)
        .bind(&user_addr)
        .fetch_optional(&self.pool)
        .await?;

        record.ok_or_else(|| anyhow::anyhow!("Withdrawal not found"))
    }

    /// Confirm withdrawal with transaction hash
    pub async fn confirm_withdrawal(
        &self,
        user_address: &str,
        withdrawal_id: &str,
        tx_hash: &str,
    ) -> anyhow::Result<()> {
        let user_addr = user_address.to_lowercase();

        // Parse withdrawal_id as UUID
        let withdrawal_uuid = uuid::Uuid::parse_str(withdrawal_id)
            .map_err(|_| anyhow::anyhow!("Invalid withdrawal ID format"))?;

        tracing::info!(
            "确认提现 - 用户: {}, 提现ID: {}, 交易哈希: {}",
            user_addr,
            withdrawal_id,
            tx_hash
        );

        // Verify withdrawal exists and is in signed status
        let existing: Option<String> = sqlx::query_scalar(
            "SELECT status::text FROM withdrawals WHERE id = $1 AND user_address = $2"
        )
        .bind(withdrawal_uuid)
        .bind(&user_addr)
        .fetch_optional(&self.pool)
        .await?;

        match existing.as_deref() {
            Some("signed") => {
                // Update withdrawal status and tx_hash
                let mut tx = self.pool.begin().await?;

                sqlx::query(
                    r#"
                    UPDATE withdrawals
                    SET tx_hash = $1, status = 'submitted', updated_at = NOW()
                    WHERE id = $2 AND user_address = $3
                    "#
                )
                .bind(tx_hash)
                .bind(withdrawal_uuid)
                .bind(&user_addr)
                .execute(&mut *tx)
                .await?;

                tx.commit().await?;

                tracing::info!(
                    "提现已确认 - 提现ID: {}, 状态: signed -> submitted",
                    withdrawal_id
                );

                Ok(())
            }
            Some(status) => {
                tracing::warn!(
                    "无法确认提现 - 提现ID: {}, 当前状态: {}",
                    withdrawal_id,
                    status
                );
                anyhow::bail!("Withdrawal is not in signed status (current: {})", status)
            }
            None => {
                tracing::error!(
                    "提现不存在 - 用户: {}, 提现ID: {}",
                    user_addr,
                    withdrawal_id
                );
                anyhow::bail!("Withdrawal not found")
            }
        }
    }

    /// Update withdrawal status (internal/admin use)
    pub async fn update_withdrawal_status(
        &self,
        withdrawal_id: &str,
        new_status: &str,
    ) -> anyhow::Result<()> {
        // Parse withdrawal_id as UUID
        let withdrawal_uuid = uuid::Uuid::parse_str(withdrawal_id)
            .map_err(|_| anyhow::anyhow!("Invalid withdrawal ID format"))?;

        tracing::info!(
            "更新提现状态 - 提现ID: {}, 新状态: {}",
            withdrawal_id,
            new_status
        );

        // Validate status
        let valid_statuses = ["pending", "signed", "submitted", "confirmed", "failed", "cancelled"];
        if !valid_statuses.contains(&new_status) {
            anyhow::bail!("Invalid withdrawal status: {}", new_status);
        }

        sqlx::query(
            r#"
            UPDATE withdrawals
            SET status = $1::withdrawal_status, updated_at = NOW()
            WHERE id = $2
            "#
        )
        .bind(new_status)
        .bind(withdrawal_uuid)
        .execute(&self.pool)
        .await?;

        tracing::info!("提现状态已更新 - 提现ID: {}", withdrawal_id);

        Ok(())
    }

    /// Process expired withdrawals - unfreeze funds for withdrawals that have expired
    /// This should be called periodically by a background task
    pub async fn process_expired_withdrawals(&self) -> anyhow::Result<u32> {
        let now = chrono::Utc::now().timestamp();

        // Find all expired 'signed' withdrawals (not yet submitted on-chain)
        let expired_withdrawals: Vec<(uuid::Uuid, String, String, Decimal)> = sqlx::query_as(
            r#"
            SELECT id, user_address, token, amount
            FROM withdrawals
            WHERE status = 'signed' AND expiry < $1
            "#
        )
        .bind(now)
        .fetch_all(&self.pool)
        .await?;

        let count = expired_withdrawals.len() as u32;

        if count == 0 {
            return Ok(0);
        }

        tracing::info!("处理过期提现 - 发现 {} 个过期提现", count);

        for (id, user_address, token, amount) in expired_withdrawals {
            let mut tx = self.pool.begin().await?;

            // Unfreeze the amount
            let result = sqlx::query(
                r#"
                UPDATE balances
                SET available = available + $1, frozen = frozen - $1
                WHERE user_address = $2 AND token = $3
                "#
            )
            .bind(amount)
            .bind(&user_address)
            .bind(&token)
            .execute(&mut *tx)
            .await;

            if let Err(e) = result {
                tracing::error!("解冻失败 - 提现ID: {}, 错误: {}", id, e);
                continue;
            }

            // Update withdrawal status to 'expired'
            let result = sqlx::query(
                "UPDATE withdrawals SET status = 'expired', updated_at = NOW() WHERE id = $1"
            )
            .bind(id)
            .execute(&mut *tx)
            .await;

            if let Err(e) = result {
                tracing::error!("更新状态失败 - 提现ID: {}, 错误: {}", id, e);
                continue;
            }

            tx.commit().await?;

            tracing::info!(
                "过期提现已处理 - 提现ID: {}, 用户: {}, 金额: {} {}",
                id, user_address, amount, token
            );
        }

        Ok(count)
    }
}

#[derive(Debug, serde::Serialize)]
pub struct WithdrawResult {
    pub withdrawal_id: String,
    pub token: String,
    pub amount: String,
    pub nonce: i64,
    pub expiry: i64,
    pub signature: String,
    pub vault_address: String,
}

#[derive(Debug, serde::Serialize, sqlx::FromRow)]
pub struct WithdrawRecord {
    pub id: String,
    pub token: String,
    pub amount: Decimal,
    pub nonce: i64,
    pub expiry: i64,
    pub backend_signature: Option<String>,
    pub tx_hash: Option<String>,
    pub status: String,
    pub created_at: i64,
}
