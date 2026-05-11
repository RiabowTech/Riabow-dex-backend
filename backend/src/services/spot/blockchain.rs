//! BSC blockchain integration for the spot subsystem.
//!
//! Owns one provider, one Vault contract binding, and two polling tasks
//! (one for deposits, one for withdrawals — implemented in subsequent tasks).

use anyhow::{Context, Result};
use ethers::prelude::*;
use sqlx::PgPool;
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tracing::Instrument;

use crate::services::spot::config::SpotConfig;

abigen!(
    SpotVaultContract,
    r#"[
        event SpotDeposit(address indexed account, address indexed token, uint256 amount)
        event SpotWithdrawal(address indexed account, address indexed token, uint256 amount, uint256 nonce)
        function releaseNonces(address account) external view returns (uint256)
    ]"#
);

pub struct SpotBlockchainService {
    pub provider: Arc<Provider<Http>>,
    pub vault: SpotVaultContract<Provider<Http>>,
    pub pool: PgPool,
    pub chain_id: u64,
    pub df_decimals: u8,
    pub df_token_address: String,
    pub confirmation_depth: u64,
    pub poll_interval_ms: u64,
    pub start_block_hint: Option<u64>,
}

impl SpotBlockchainService {
    pub async fn new(cfg: &SpotConfig, pool: PgPool) -> Result<Arc<Self>> {
        // Reuse the same RPC_ORIGIN trick as the existing perp blockchain
        // service so allowlisted endpoints accept us.
        let origin = std::env::var("RPC_ORIGIN").unwrap_or_else(|_| "http://localhost".to_string());
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::ORIGIN,
            reqwest::header::HeaderValue::from_str(&origin)
                .unwrap_or_else(|_| reqwest::header::HeaderValue::from_static("http://localhost"))
        );
        let client = reqwest::Client::builder().default_headers(headers).build()?;
        let url = reqwest::Url::parse(&cfg.bsc_rpc_url)?;
        let http = Http::new_with_client(url, client);
        let provider = Arc::new(Provider::new(http));

        let vault: SpotVaultContract<Provider<Http>> = SpotVaultContract::new(
            cfg.bsc_vault_address,
            provider.clone(),
        );

        Ok(Arc::new(Self {
            provider,
            vault,
            pool,
            chain_id: cfg.bsc_chain_id,
            df_decimals: cfg.df_token_decimals,
            df_token_address: format!("{:?}", cfg.df_token_address).to_lowercase(),
            confirmation_depth: cfg.bsc_confirmation_depth,
            poll_interval_ms: cfg.bsc_poll_interval_ms,
            start_block_hint: cfg.bsc_start_block,
        }))
    }

    /// Read or initialize the last_processed_block for one event type.
    /// On first run with an empty row, seeds from `start_block_hint` (if set)
    /// or from the current head minus a 1000-block lookback.
    pub async fn resume_from(&self, event_type: &str) -> Result<u64> {
        let existing: Option<i64> = sqlx::query_scalar(
            "SELECT last_processed_block FROM spot_block_sync_state
              WHERE chain_id=$1 AND event_type=$2"
        )
        .bind(self.chain_id as i64).bind(event_type)
        .fetch_optional(&self.pool).await?;

        if let Some(b) = existing {
            return Ok(b as u64);
        }

        let head = self.provider.get_block_number().await
            .context("get_block_number")?.as_u64();
        let initial = self.start_block_hint.unwrap_or_else(|| head.saturating_sub(1000));
        sqlx::query(
            "INSERT INTO spot_block_sync_state (chain_id, event_type, last_processed_block)
             VALUES ($1, $2, $3)
             ON CONFLICT (chain_id, event_type) DO NOTHING"
        )
        .bind(self.chain_id as i64).bind(event_type).bind(initial as i64)
        .execute(&self.pool).await?;
        Ok(initial)
    }

    /// Convenience: convert wei (U256) → Decimal at the configured decimals.
    pub fn wei_to_decimal(&self, wei: U256) -> rust_decimal::Decimal {
        wei_to_decimal_inner(wei, self.df_decimals)
    }

    /// Read the contract's per-user release nonce. Used by the withdraw
    /// handler so the EIP-712 `nonce` field matches what the vault will
    /// validate against. Without this, an unredeemed signed record drifts
    /// the DB-side counter and every later signature reverts on chain.
    pub async fn release_nonce(&self, user: Address) -> Result<u64> {
        let n: U256 = self.vault.release_nonces(user).call().await
            .context("vault.releaseNonces call")?;
        Ok(n.as_u64())
    }
}

pub(crate) fn wei_to_decimal_inner(wei: U256, decimals: u8) -> rust_decimal::Decimal {
    use rust_decimal::Decimal;
    use std::str::FromStr;
    // Approximation good enough for amounts that fit in i128.
    // For larger amounts, switch to a U256→string conversion.
    let s = wei.to_string();
    let d = Decimal::from_str(&s).unwrap_or(Decimal::ZERO);
    // Build scale = 10^decimals via repeated multiplication (avoids
    // requiring rust_decimal's `maths` feature, which isn't enabled here).
    let mut scale = Decimal::from(1u64);
    let ten = Decimal::from(10u64);
    for _ in 0..decimals {
        scale *= ten;
    }
    d / scale
}

impl SpotBlockchainService {
    /// Spawn-once entrypoint. Loops forever, polling for AccountFunded events
    /// and atomically inserting them into spot_deposits + spot_balances.
    /// Errors are logged but do not break the loop.
    pub async fn run_deposit_poller(self: Arc<Self>) {
        let span = tracing::info_span!("spot_deposit_poller", chain_id = self.chain_id);
        async move {
            tracing::info!("spot deposit poller starting");
            loop {
                if let Err(e) = self.poll_deposits_once().await {
                    tracing::error!("spot deposit poll error: {e:?}");
                }
                sleep(Duration::from_millis(self.poll_interval_ms)).await;
            }
        }
        .instrument(span)
        .await
    }

    async fn poll_deposits_once(&self) -> Result<()> {
        let last = self.resume_from("deposit").await?;
        let head = self.provider.get_block_number().await?.as_u64();
        let target = head.saturating_sub(self.confirmation_depth);
        if target <= last {
            return Ok(());
        }
        // Moralis caps eth_getLogs at 100 blocks per call; smaller windows
        // also keep response sizes bounded across providers.
        let to = std::cmp::min(target, last + 100);
        let from = last + 1;

        let filter = self.vault.spot_deposit_filter()
            .from_block(from)
            .to_block(to);
        let events = filter.query_with_meta().await?;

        for (event, meta) in events {
            // Vault is multi-token; only credit DF deposits.
            if format!("{:?}", event.token).to_lowercase() != self.df_token_address {
                continue;
            }
            let user_addr = format!("{:?}", event.account).to_lowercase();
            let amount = self.wei_to_decimal(event.amount);
            let tx_hash = format!("{:?}", meta.transaction_hash);
            let block_number = meta.block_number.as_u64() as i64;
            let log_index = meta.log_index.as_u64() as i32;

            let mut tx = self.pool.begin().await?;
            let inserted = sqlx::query(
                "INSERT INTO spot_deposits
                   (user_address, token, amount, chain_id, tx_hash, block_number, log_index, status, confirmed_at)
                 VALUES ($1, 'DF', $2, $3, $4, $5, $6, 'confirmed', NOW())
                 ON CONFLICT (chain_id, tx_hash, log_index) DO NOTHING"
            )
            .bind(&user_addr).bind(amount).bind(self.chain_id as i64)
            .bind(&tx_hash).bind(block_number).bind(log_index)
            .execute(&mut *tx).await?
            .rows_affected();

            if inserted > 0 {
                sqlx::query(
                    "INSERT INTO spot_balances (user_address, token, available, frozen)
                     VALUES ($1, 'DF', $2, 0)
                     ON CONFLICT (user_address, token)
                     DO UPDATE SET available = spot_balances.available + EXCLUDED.available,
                                   updated_at = NOW()"
                )
                .bind(&user_addr).bind(amount)
                .execute(&mut *tx).await?;

                tracing::info!(
                    user = %user_addr, amount = %amount, tx = %tx_hash,
                    "spot deposit credited"
                );
            }
            tx.commit().await?;
        }

        // Update the cursor *after* the batch commits.
        sqlx::query(
            "UPDATE spot_block_sync_state SET last_processed_block=$1, updated_at=NOW()
              WHERE chain_id=$2 AND event_type='deposit'"
        )
        .bind(to as i64).bind(self.chain_id as i64)
        .execute(&self.pool).await?;

        Ok(())
    }
}

impl SpotBlockchainService {
    pub async fn run_withdrawal_poller(self: Arc<Self>) {
        let span = tracing::info_span!("spot_withdrawal_poller", chain_id = self.chain_id);
        async move {
            tracing::info!("spot withdrawal poller starting");
            loop {
                if let Err(e) = self.poll_withdrawals_once().await {
                    tracing::error!("spot withdrawal poll error: {e:?}");
                }
                sleep(Duration::from_millis(self.poll_interval_ms)).await;
            }
        }
        .instrument(span)
        .await
    }

    async fn poll_withdrawals_once(&self) -> Result<()> {
        let last = self.resume_from("withdrawal").await?;
        let head = self.provider.get_block_number().await?.as_u64();
        let target = head.saturating_sub(self.confirmation_depth);
        if target <= last {
            return Ok(());
        }
        let to = std::cmp::min(target, last + 100);
        let from = last + 1;

        let filter = self.vault.spot_withdrawal_filter()
            .from_block(from)
            .to_block(to);
        let events = filter.query_with_meta().await?;

        for (event, meta) in events {
            // Vault is multi-token; only process DF withdrawals.
            if format!("{:?}", event.token).to_lowercase() != self.df_token_address {
                continue;
            }
            let user_addr = format!("{:?}", event.account).to_lowercase();
            let amount = self.wei_to_decimal(event.amount);
            let nonce = event.nonce.as_u64() as i64;
            let tx_hash = format!("{:?}", meta.transaction_hash);
            let block_number = meta.block_number.as_u64() as i64;

            let mut tx = self.pool.begin().await?;

            // Mark the withdrawal confirmed; idempotent via WHERE status='signed'
            // (a second pass over the same event simply finds 0 rows).
            let updated = sqlx::query(
                "UPDATE spot_withdrawals
                    SET status='confirmed', tx_hash=$1, block_number=$2, confirmed_at=NOW()
                  WHERE user_address=$3 AND chain_id=$4 AND nonce=$5
                    AND status='signed'"
            )
            .bind(&tx_hash).bind(block_number)
            .bind(&user_addr).bind(self.chain_id as i64).bind(nonce)
            .execute(&mut *tx).await?
            .rows_affected();

            if updated > 0 {
                // Money already left the vault; release the frozen lock without
                // touching available (available was decremented at sign time).
                sqlx::query(
                    "UPDATE spot_balances
                        SET frozen = frozen - $1, updated_at=NOW()
                      WHERE user_address=$2 AND token='DF'"
                )
                .bind(amount).bind(&user_addr)
                .execute(&mut *tx).await?;

                tracing::info!(
                    user=%user_addr, amount=%amount, nonce=nonce, tx=%tx_hash,
                    "spot withdrawal confirmed"
                );
            } else {
                tracing::warn!(
                    user=%user_addr, nonce=nonce, tx=%tx_hash,
                    "SpotWithdrawal seen but no matching signed withdrawal — possibly already processed or expired by reaper"
                );
            }
            tx.commit().await?;
        }

        sqlx::query(
            "UPDATE spot_block_sync_state SET last_processed_block=$1, updated_at=NOW()
              WHERE chain_id=$2 AND event_type='withdrawal'"
        )
        .bind(to as i64).bind(self.chain_id as i64)
        .execute(&self.pool).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn wei_to_decimal_18_decimals() {
        assert_eq!(wei_to_decimal_inner(U256::from_dec_str("1000000000000000000").unwrap(), 18), dec!(1));
        assert_eq!(wei_to_decimal_inner(U256::from_dec_str("500000000000000000").unwrap(), 18), dec!(0.5));
        assert_eq!(wei_to_decimal_inner(U256::zero(), 18), dec!(0));
    }
}
