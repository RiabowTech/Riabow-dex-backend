//! Blockchain Service
//!
//! Handles blockchain event listening and contract interactions.

use ethers::prelude::*;
use ethers::providers::{Provider, Http};
use rust_decimal::Decimal;
use sqlx::PgPool;
use std::str::FromStr;
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tracing::Instrument;

// Vault contract ABI for events (新版 Vault 合约)
abigen!(
    VaultContract,
    r#"[
        event AccountFunded(address indexed user, uint256 amount, bytes32 referralCode)
        event FundsReleased(address indexed user, uint256 amount, uint256 nonce)
        function accountLiquidity(address user) external view returns (uint256)
    ]"#
);

pub struct BlockchainService {
    provider: Arc<Provider<Http>>,
    vault_address: Address,
    pool: PgPool,
    #[allow(dead_code)]
    chain_id: u64,
    /// Token decimals for amount conversion (e.g., 6 for USDT, 18 for ETH)
    token_decimals: u8,
    /// Number of blocks to look back when first starting (no saved state)
    block_sync_lookback: u64,
}

impl BlockchainService {
    pub async fn new(
        rpc_url: &str,
        vault_address: &str,
        pool: PgPool,
        chain_id: u64,
        token_decimals: u8,
        block_sync_lookback: u64,
    ) -> anyhow::Result<Self> {
        // Create request client with Origin header to satisfy RPC allowlists
        // Origin can be configured via RPC_ORIGIN env var (default: http://localhost)
        let origin = std::env::var("RPC_ORIGIN").unwrap_or_else(|_| "http://localhost".to_string());
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::ORIGIN,
            reqwest::header::HeaderValue::from_str(&origin).unwrap_or_else(|_| {
                reqwest::header::HeaderValue::from_static("http://localhost")
            })
        );

        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()?;

        let url = reqwest::Url::parse(rpc_url)?;
        let provider = Provider::new(Http::new_with_client(url, client));

        let vault_address = vault_address.parse()?;

        tracing::info!(
            "BlockchainService: Using {} decimals, lookback {} blocks",
            token_decimals, block_sync_lookback
        );

        Ok(Self {
            provider: Arc::new(provider),
            vault_address,
            pool,
            chain_id,
            token_decimals,
            block_sync_lookback,
        })
    }

    /// Start the event listener (polls for new events)
    pub async fn start_event_listener(self: Arc<Self>) {
        let deposit_handle = {
            let service = self.clone();
            tokio::spawn(async move {
                if let Err(e) = service.poll_deposits().await {
                    tracing::error!("Deposit listener error: {}", e);
                }
            }.instrument(tracing::info_span!("blockchain-deposit")))
        };

        let withdraw_handle = {
            let service = self.clone();
            tokio::spawn(async move {
                if let Err(e) = service.poll_withdrawals().await {
                    tracing::error!("Withdrawal listener error: {}", e);
                }
            }.instrument(tracing::info_span!("blockchain-withdrawal")))
        };

        tokio::select! {
            _ = deposit_handle => {},
            _ = withdraw_handle => {},
        }
    }

    /// Poll for deposit events
    async fn poll_deposits(&self) -> anyhow::Result<()> {
        let mut last_block = self.get_last_processed_block("deposit").await?;
        let mut consecutive_errors = 0u32;

        loop {
            match self.try_poll_deposits(&mut last_block).await {
                Ok(_) => {
                    consecutive_errors = 0;
                    sleep(Duration::from_secs(12)).await; // ~1 block time
                }
                Err(e) => {
                    consecutive_errors += 1;
                    let backoff_secs = std::cmp::min(60 * consecutive_errors, 300); // Max 5 min
                    
                    if consecutive_errors <= 3 {
                        tracing::error!("Deposit listener error: {}", e);
                    } else {
                        tracing::debug!("Deposit listener error (suppressed): {}", e);
                    }
                    
                    tracing::debug!("Retrying deposits in {} seconds...", backoff_secs);
                    sleep(Duration::from_secs(backoff_secs as u64)).await;
                }
            }
        }
    }

    /// Try to poll for deposit events (extracted for error handling)
    async fn try_poll_deposits(&self, last_block: &mut u64) -> anyhow::Result<()> {
        let current_block = self.provider.get_block_number().await?.as_u64();

        if current_block > *last_block {
            let from_block = *last_block + 1;
            // Use larger block range for faster catch-up (1000 blocks max)
            let to_block = std::cmp::min(from_block + 999, current_block);

            tracing::debug!("Scanning deposits from block {} to {}", from_block, to_block);

            // 新版合约事件签名: AccountFunded(address indexed user, uint256 amount, bytes32 referralCode)
            let deposit_topic = H256::from(ethers::utils::keccak256(
                "AccountFunded(address,uint256,bytes32)",
            ));

            let filter = Filter::new()
                .address(self.vault_address)
                .topic0(deposit_topic)
                .from_block(from_block)
                .to_block(to_block);

            let logs = self.provider.get_logs(&filter).await?;

            for log in logs {
                if let Err(e) = self.handle_deposit_event(&log).await {
                    tracing::error!("Error handling deposit: {}", e);
                }
            }

            self.save_last_processed_block("deposit", to_block).await?;
            *last_block = to_block;
        }

        Ok(())
    }

    /// Poll for withdrawal events
    async fn poll_withdrawals(&self) -> anyhow::Result<()> {
        let mut last_block = self.get_last_processed_block("withdraw").await?;
        let mut consecutive_errors = 0u32;

        loop {
            match self.try_poll_withdrawals(&mut last_block).await {
                Ok(_) => {
                    consecutive_errors = 0;
                    sleep(Duration::from_secs(12)).await;
                }
                Err(e) => {
                    consecutive_errors += 1;
                    let backoff_secs = std::cmp::min(60 * consecutive_errors, 300); // Max 5 min
                    
                    if consecutive_errors <= 3 {
                        tracing::error!("Withdrawal listener error: {}", e);
                    } else {
                        tracing::debug!("Withdrawal listener error (suppressed): {}", e);
                    }
                    
                    tracing::debug!("Retrying withdrawals in {} seconds...", backoff_secs);
                    sleep(Duration::from_secs(backoff_secs as u64)).await;
                }
            }
        }
    }

    /// Try to poll for withdrawal events (extracted for error handling)
    async fn try_poll_withdrawals(&self, last_block: &mut u64) -> anyhow::Result<()> {
        let current_block = self.provider.get_block_number().await?.as_u64();

        if current_block > *last_block {
            let from_block = *last_block + 1;
            // Use larger block range for faster catch-up (1000 blocks max)
            let to_block = std::cmp::min(from_block + 999, current_block);

            // 新版合约事件签名: FundsReleased(address indexed user, uint256 amount, uint256 nonce)
            let withdraw_topic = H256::from(ethers::utils::keccak256(
                "FundsReleased(address,uint256,uint256)",
            ));

            let filter = Filter::new()
                .address(self.vault_address)
                .topic0(withdraw_topic)
                .from_block(from_block)
                .to_block(to_block);

            let logs = self.provider.get_logs(&filter).await?;

            for log in logs {
                if let Err(e) = self.handle_withdraw_event(&log).await {
                    tracing::error!("Error handling withdrawal: {}", e);
                }
            }

            self.save_last_processed_block("withdraw", to_block).await?;
            *last_block = to_block;
        }

        Ok(())
    }

    /// Handle a deposit event (新版合约: AccountFunded(address indexed user, uint256 amount, bytes32 referralCode))
    async fn handle_deposit_event(&self, log: &Log) -> anyhow::Result<()> {
        let tx_hash = log.transaction_hash.unwrap_or_default();
        let block_number = log.block_number.unwrap_or_default().as_u64() as i64;

        // Check if already processed
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM deposits WHERE tx_hash = $1)"
        )
        .bind(format!("{:?}", tx_hash))
        .fetch_one(&self.pool)
        .await?;

        if exists {
            return Ok(());
        }

        // Parse event data (新版合约格式)
        // topics[1]: user (indexed)
        // data: amount (uint256, 32 bytes) + referralCode (bytes32, 32 bytes)
        let user = Address::from(log.topics.get(1).copied().unwrap_or_default());

        // Amount is in the first 32 bytes of data
        let amount = if log.data.len() >= 32 {
            U256::from_big_endian(&log.data[0..32])
        } else {
            U256::zero()
        };

        let user_address = format!("{:?}", user).to_lowercase();
        // 新版合约只支持 USDT，使用固定的 token 标识（大写，与交易系统统一）
        let token_address = "USDT".to_string();
        let amount_decimal = wei_to_decimal(amount, self.token_decimals)?;

        tracing::info!(
            "Processing deposit: user={}, token={}, amount={} (decimals={}), tx={}",
            user_address, token_address, amount_decimal, self.token_decimals, tx_hash
        );

        // Start transaction
        let mut tx = self.pool.begin().await?;

        // Insert deposit record
        sqlx::query(
            r#"
            INSERT INTO deposits (user_address, token, amount, tx_hash, block_number, status)
            VALUES ($1, $2, $3, $4, $5, 'confirmed')
            "#
        )
        .bind(&user_address)
        .bind(&token_address)
        .bind(amount_decimal)
        .bind(format!("{:?}", tx_hash))
        .bind(block_number)
        .execute(&mut *tx)
        .await?;

        // Update user balance
        sqlx::query(
            r#"
            INSERT INTO balances (user_address, token, available, frozen)
            VALUES ($1, $2, $3, 0)
            ON CONFLICT (user_address, token)
            DO UPDATE SET available = balances.available + $3
            "#
        )
        .bind(&user_address)
        .bind(&token_address)
        .bind(amount_decimal)
        .execute(&mut *tx)
        .await?;

        // Ensure user exists
        sqlx::query(
            r#"
            INSERT INTO users (address, nonce)
            VALUES ($1, 1)
            ON CONFLICT (address) DO NOTHING
            "#
        )
        .bind(&user_address)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        tracing::info!("Deposit processed successfully: {}", tx_hash);

        Ok(())
    }

    /// Handle a withdrawal event (新版合约: FundsReleased(address indexed user, uint256 amount, uint256 nonce))
    async fn handle_withdraw_event(&self, log: &Log) -> anyhow::Result<()> {
        let tx_hash = log.transaction_hash.unwrap_or_default();

        // 新版合约格式:
        // topics[1]: user (indexed)
        // data: amount (uint256, 32 bytes) + nonce (uint256, 32 bytes)
        let user = Address::from(log.topics.get(1).copied().unwrap_or_default());

        // Parse amount and nonce from data
        let amount = if log.data.len() >= 32 {
            U256::from_big_endian(&log.data[0..32])
        } else {
            U256::zero()
        };

        let nonce = if log.data.len() >= 64 {
            U256::from_big_endian(&log.data[32..64]).as_u64() as i64
        } else {
            0
        };

        let user_address = format!("{:?}", user).to_lowercase();
        // 新版合约只支持 USDT（大写，与交易系统统一）
        let token_address = "USDT".to_string();
        let amount_decimal = wei_to_decimal(amount, self.token_decimals)?;

        tracing::info!(
            "Processing withdrawal: user={}, token={}, amount={} (decimals={}), nonce={}, tx={}",
            user_address, token_address, amount_decimal, self.token_decimals, nonce, tx_hash
        );

        // Start transaction
        let mut tx = self.pool.begin().await?;

        // Update withdrawal status
        sqlx::query(
            r#"
            UPDATE withdrawals
            SET status = 'confirmed', tx_hash = $1
            WHERE user_address = $2 AND token = $3 AND nonce = $4 AND status IN ('signed', 'submitted')
            "#
        )
        .bind(format!("{:?}", tx_hash))
        .bind(&user_address)
        .bind(&token_address)
        .bind(nonce)
        .execute(&mut *tx)
        .await?;

        // Clear frozen balance (withdrawal completed on-chain)
        // Flow: request_withdraw: available -= amount, frozen += amount
        //       on-chain confirm: frozen -= amount
        sqlx::query(
            "UPDATE balances SET frozen = frozen - $1, updated_at = NOW() WHERE user_address = $2 AND token = $3 AND frozen >= $1"
        )
        .bind(&amount_decimal)
        .bind(&user_address)
        .bind(&token_address)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        tracing::info!(
            "Withdrawal confirmed and frozen cleared: user={}, amount={}, tx={}",
            user_address, amount_decimal, tx_hash
        );

        Ok(())
    }

    async fn get_last_processed_block(&self, event_type: &str) -> anyhow::Result<u64> {
        // Try to get from database first
        let result: Option<i64> = sqlx::query_scalar(
            "SELECT last_block FROM block_sync_state WHERE event_type = $1"
        )
        .bind(event_type)
        .fetch_optional(&self.pool)
        .await?;

        if let Some(block) = result {
            tracing::info!("Resuming {} listener from block {}", event_type, block);
            return Ok(block as u64);
        }

        // If no record, start from configured lookback
        let current = self.provider.get_block_number().await?.as_u64();
        let start_block = current.saturating_sub(self.block_sync_lookback);

        tracing::info!(
            "No saved state for {} listener, starting from block {} (lookback: {}, current: {})",
            event_type, start_block, self.block_sync_lookback, current
        );

        Ok(start_block)
    }

    async fn save_last_processed_block(&self, event_type: &str, block: u64) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            INSERT INTO block_sync_state (event_type, last_block, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (event_type)
            DO UPDATE SET last_block = $2, updated_at = NOW()
            "#
        )
        .bind(event_type)
        .bind(block as i64)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Get on-chain balance for a user (新版合约只支持 USDT，不需要 token 参数)
    #[allow(dead_code)]
    pub async fn get_onchain_balance(&self, user: &str, _token: &str) -> anyhow::Result<U256> {
        let contract = VaultContract::new(self.vault_address, self.provider.clone());
        let user_addr: Address = user.parse()?;

        let balance = contract.account_liquidity(user_addr).call().await?;
        Ok(balance)
    }
}

/// Convert wei to Decimal with specified decimals
fn wei_to_decimal(wei: U256, decimals: u8) -> anyhow::Result<Decimal> {
    let divisor = U256::from(10u64).pow(U256::from(decimals));
    let whole = wei / divisor;
    let frac = wei % divisor;

    let whole_str = whole.to_string();
    let frac_str = format!("{:0>width$}", frac.to_string(), width = decimals as usize);

    let decimal_str = format!("{}.{}", whole_str, frac_str);
    Decimal::from_str(&decimal_str).map_err(|e| anyhow::anyhow!("Decimal parse error: {}", e))
}

/// Convert Decimal to wei
#[allow(dead_code)]
pub fn decimal_to_wei(amount: Decimal, decimals: u8) -> U256 {
    let multiplier = Decimal::from(10u64.pow(decimals as u32));
    let wei_decimal = amount * multiplier;
    let wei_str = wei_decimal.trunc().to_string();
    U256::from_dec_str(&wei_str).unwrap_or_default()
}
