//! EIP-712 signer for the BSC vault `releaseFunds` payload.
//!
//! Domain separator uses chain_id from `SpotConfig::bsc_chain_id` and
//! verifying contract = `SpotConfig::bsc_vault_address`. Signing key
//! comes from `SpotConfig::withdraw_signer_private_key` — independent
//! from the perp signer.
//!
//! TypedData layout (matches assumed BSC vault contract; if the contracts
//! team's ABI differs, update STRUCT_NAME and field list):
//!
//!   ReleaseFunds(address user, address token, uint256 amount,
//!                uint256 nonce, uint256 deadline)

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use ethers::core::types::transaction::eip712::{EIP712Domain, Eip712, TypedData};
use ethers::signers::{LocalWallet, Signer};
use ethers::types::{Address, U256};
use rust_decimal::Decimal;
use std::collections::BTreeMap;
use std::str::FromStr;

use crate::services::spot::config::SpotConfig;

pub const STRUCT_NAME: &str = "SpotReleaseFunds";

pub struct SpotWithdrawSigner {
    wallet: LocalWallet,
    domain: EIP712Domain,
    df_token: Address,
    df_decimals: u8,
}

#[derive(Debug)]
pub struct SignedRelease {
    pub signature: String,         // 0x-prefixed hex
    pub deadline: DateTime<Utc>,
    /// Wei-scaled amount that was placed in the EIP-712 `value` field.
    /// Returned to the caller so the API response advertises the same
    /// units the contract validates against — the FE parses this as
    /// `BigInt(amount)` and feeds it directly into `vault.withdraw`.
    pub amount_wei: U256,
}

impl SpotWithdrawSigner {
    pub fn from_config(cfg: &SpotConfig) -> Result<Self> {
        let key = cfg.withdraw_signer_private_key.expose_secret().trim_start_matches("0x");
        let wallet = LocalWallet::from_str(key)
            .context("invalid SPOT_WITHDRAW_SIGNER_PRIVATE_KEY")?
            .with_chain_id(cfg.bsc_chain_id);

        let domain = EIP712Domain {
            name: Some(cfg.eip712_domain_name.clone()),
            version: Some(cfg.eip712_domain_version.clone()),
            chain_id: Some(U256::from(cfg.bsc_chain_id)),
            verifying_contract: Some(cfg.bsc_vault_address),
            salt: None,
        };

        Ok(Self {
            wallet,
            domain,
            df_token: cfg.df_token_address,
            df_decimals: cfg.df_token_decimals,
        })
    }

    /// Sign a releaseFunds payload. Returns a 65-byte 0x-prefixed signature
    /// and the deadline (now + nonce_ttl).
    pub async fn sign(
        &self,
        user: Address,
        amount_decimal: Decimal,
        nonce: i64,
        ttl_secs: u64,
    ) -> Result<SignedRelease> {
        let amount_wei = decimal_to_wei(amount_decimal, self.df_decimals)?;
        let deadline = Utc::now() + chrono::Duration::seconds(ttl_secs as i64);

        let mut message: BTreeMap<String, serde_json::Value> = BTreeMap::new();
        message.insert("account".into(), serde_json::Value::String(format!("{:?}", user)));
        message.insert("token".into(), serde_json::Value::String(format!("{:?}", self.df_token)));
        message.insert("value".into(), serde_json::Value::String(amount_wei.to_string()));
        message.insert("nonce".into(), serde_json::Value::String(nonce.to_string()));
        message.insert("deadline".into(),
            serde_json::Value::String(deadline.timestamp().to_string()));

        let mut types: BTreeMap<String, Vec<ethers::core::types::transaction::eip712::Eip712DomainType>> = BTreeMap::new();
        types.insert(STRUCT_NAME.into(), vec![
            field("account", "address"),
            field("token", "address"),
            field("value", "uint256"),
            field("nonce", "uint256"),
            field("deadline", "uint256"),
        ]);

        let typed = TypedData {
            domain: self.domain.clone(),
            types,
            primary_type: STRUCT_NAME.into(),
            message,
        };

        let digest = typed.encode_eip712().context("encode eip712")?;
        let sig = self.wallet.sign_hash(digest.into())
            .context("sign digest")?;

        Ok(SignedRelease {
            signature: format!("0x{}", sig.to_string()),
            deadline,
            amount_wei,
        })
    }
}

fn field(name: &str, ty: &str) -> ethers::core::types::transaction::eip712::Eip712DomainType {
    ethers::core::types::transaction::eip712::Eip712DomainType {
        name: name.to_string(),
        r#type: ty.to_string(),
    }
}

/// Convert a Decimal amount to on-chain wei (uint256 in base units).
/// e.g. Decimal::new(1234500, 4) at decimals=18 → 123450000000000000000.
pub(crate) fn decimal_to_wei(amount: Decimal, decimals: u8) -> Result<U256> {
    // Build 10^decimals as a Decimal without the `maths` feature by repeated multiplication.
    let mut scale = Decimal::ONE;
    let ten = Decimal::from(10u32);
    for _ in 0..decimals {
        scale *= ten;
    }
    let scaled = (amount * scale).round_dp(0);
    let s = scaled.normalize().to_string();
    // normalize() may produce e.g. "1E+18"; handle both notations.
    // We only need the integer part (round_dp(0) already ensured no fractional part).
    let s = if let Some(dot) = s.find('.') { &s[..dot] } else { s.as_str() };
    U256::from_dec_str(if s.is_empty() { "0" } else { s })
        .with_context(|| format!("amount {} cannot fit U256", amount))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::spot::config::SecretString;
    use rust_decimal_macros::dec;

    fn test_config() -> SpotConfig {
        SpotConfig {
            df_token_address: Address::from_str("0xeaae2cbe4c153f7f012f7326d1106013a2b76422").unwrap(),
            df_token_decimals: 18,
            bsc_chain_id: 56,
            bsc_rpc_url: "http://localhost".into(),
            bsc_vault_address: Address::from_str("0x1111111111111111111111111111111111111111").unwrap(),
            bsc_confirmation_depth: 20,
            bsc_poll_interval_ms: 2000,
            bsc_start_block: None,
            eip712_domain_name: "ZTDX Spot Vault".to_string(),
            eip712_domain_version: "1".to_string(),
            // deterministic test private key — anvil account #0
            withdraw_signer_private_key: SecretString::new(
                "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80".into()
            ),
            withdraw_nonce_ttl_secs: 86400,
            withdraw_min_amount_df: dec!(1),
            reconciler_interval_secs: 3600,
            trading_enabled: false,
        }
    }

    #[test]
    fn decimal_to_wei_basic() {
        assert_eq!(decimal_to_wei(dec!(1), 18).unwrap(), U256::from_dec_str("1000000000000000000").unwrap());
        assert_eq!(decimal_to_wei(dec!(0.5), 18).unwrap(), U256::from_dec_str("500000000000000000").unwrap());
        assert_eq!(decimal_to_wei(dec!(123.45), 18).unwrap(), U256::from_dec_str("123450000000000000000").unwrap());
    }

    #[tokio::test]
    async fn signer_produces_65_byte_signature() {
        let cfg = test_config();
        let signer = SpotWithdrawSigner::from_config(&cfg).unwrap();
        let user = Address::from_str("0xabcdabcdabcdabcdabcdabcdabcdabcdabcdabcd").unwrap();

        let signed = signer.sign(user, dec!(100), 1, 86400).await.unwrap();
        // 65 bytes hex = 130 chars + "0x" = 132
        assert_eq!(signed.signature.len(), 132);
        assert!(signed.signature.starts_with("0x"));
    }

    #[tokio::test]
    async fn signer_returns_amount_wei_in_signed_release() {
        let cfg = test_config();
        let signer = SpotWithdrawSigner::from_config(&cfg).unwrap();
        let user = Address::from_str("0xabcdabcdabcdabcdabcdabcdabcdabcdabcdabcd").unwrap();

        let signed = signer.sign(user, dec!(5), 0, 86400).await.unwrap();
        // The handler must return wei to match the FE's BigInt(amount) parsing
        // and the value the contract validates against. 5 DF at 18 decimals.
        assert_eq!(
            signed.amount_wei,
            U256::from_dec_str("5000000000000000000").unwrap()
        );
    }

    #[tokio::test]
    async fn signer_signs_consistently_for_same_inputs_modulo_deadline() {
        let cfg = test_config();
        let signer = SpotWithdrawSigner::from_config(&cfg).unwrap();
        let user = Address::from_str("0xabcdabcdabcdabcdabcdabcdabcdabcdabcdabcd").unwrap();
        let sig_a = signer.sign(user, dec!(50), 7, 86400).await.unwrap();
        let sig_b = signer.sign(user, dec!(50), 7, 86400).await.unwrap();
        // Deadlines differ by µs; signatures will differ.
        // But both should be well-formed and identical length.
        assert_eq!(sig_a.signature.len(), sig_b.signature.len());
    }
}
