//! EIP-712 signer for `ClaimPointsRewards`.
//!
//! The on-chain distribution contract doesn't exist pre-TGE; this
//! signer produces a forward-compatible signature using the same
//! signing key as ReferralRebate (`BACKEND_SIGNER_PRIVATE_KEY`). The
//! verifying contract address is configurable via env (defaults to the
//! Vault contract so the signature has a non-zero domain) and can be
//! re-pointed once the distribution contract ships.
//!
//! Domain: name comes from `POINTS_CLAIM_DOMAIN_NAME` env
//!         (default: `AXBlade Points Distribution`), version="1".
//!         Pre-TGE this may be overridden; once the on-chain
//!         distribution contract is pinned the env override should be
//!         removed and the constant frozen (see
//!         `constants/eip712_domains.rs`).
//! Struct:  ClaimPointsRewards(address user, uint256 distributionId,
//!                             uint256 amount, uint256 nonce, uint256 deadline)

use anyhow::{Context, Result};
use ethers::abi::{encode, Token};
use ethers::core::types::{Address, Signature, U256};
use ethers::core::utils::keccak256;
use ethers::signers::{LocalWallet, Signer};
use serde::Serialize;
use std::str::FromStr;

#[derive(Debug, Clone, Serialize)]
pub struct ClaimSignaturePayload {
    /// Recipient — the user who can submit the claim on-chain.
    pub user: String,
    /// distribution_id is the points_distribution.id (UUID → uint256
    /// big-endian truncation; on-chain side will treat as opaque id).
    pub distribution_id: String,
    /// 18-decimal token amount, as decimal string.
    pub amount: String,
    /// Per-user monotonic nonce, copied from points_distribution.claim_nonce.
    pub nonce: u64,
    /// Unix-seconds deadline (matches points_distribution.claim_deadline).
    pub deadline: u64,
    /// hex (0x-prefixed) — submit alongside the message to a future
    /// distribution contract.
    pub signature: String,
    /// Domain meta exposed for client-side display + replay verification.
    pub domain: ClaimDomain,
}

#[derive(Debug, Clone, Serialize)]
pub struct ClaimDomain {
    pub name: String,
    pub version: String,
    pub chain_id: u64,
    pub verifying_contract: String,
}

pub struct ClaimSigner {
    wallet: LocalWallet,
    chain_id: u64,
    verifying_contract: String,
    domain_name: String,
}

impl ClaimSigner {
    pub fn from_env(chain_id: u64) -> Result<Self> {
        let pk = std::env::var("BACKEND_SIGNER_PRIVATE_KEY")
            .context("BACKEND_SIGNER_PRIVATE_KEY not set")?;
        let pk = pk.trim_start_matches("0x");
        let wallet = LocalWallet::from_str(pk)
            .context("invalid BACKEND_SIGNER_PRIVATE_KEY")?
            .with_chain_id(chain_id);
        let verifying_contract = std::env::var("POINTS_CLAIM_VERIFYING_CONTRACT")
            .or_else(|_| std::env::var("VAULT_ADDRESS"))
            .unwrap_or_else(|_| "0x0000000000000000000000000000000000000000".to_string());
        // Pre-TGE: env may override the domain name. Once the
        // distribution contract ships, this override is removed and
        // the pin in `constants/eip712_domains.rs` is frozen.
        let domain_name = crate::constants::eip712_domains::points_claim_domain_name_default().to_string();
        Ok(Self { wallet, chain_id, verifying_contract, domain_name })
    }

    pub async fn sign_claim(
        &self,
        user: &str,
        distribution_id_uuid: uuid::Uuid,
        amount_decimal_string: &str,
        nonce: u64,
        deadline_secs: u64,
    ) -> Result<ClaimSignaturePayload> {
        // Convert UUID → uint256 (big-endian).
        let dist_id_u256 = U256::from_big_endian(distribution_id_uuid.as_bytes());

        // Convert amount Decimal → wei (18 decimals).
        let amount_wei = decimal_to_wei18(amount_decimal_string)?;

        // Domain separator.
        let domain_type_hash = keccak256(
            b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
        );
        let name_hash = keccak256(self.domain_name.as_bytes());
        let version_hash = keccak256(
            crate::constants::eip712_domains::domain_version().as_bytes()
        );
        let verifying_addr = Address::from_str(&self.verifying_contract)
            .context("invalid verifying_contract address")?;
        let domain_separator = keccak256(encode(&[
            Token::FixedBytes(domain_type_hash.to_vec()),
            Token::FixedBytes(name_hash.to_vec()),
            Token::FixedBytes(version_hash.to_vec()),
            Token::Uint(U256::from(self.chain_id)),
            Token::Address(verifying_addr),
        ]));

        // Struct hash.
        let claim_type_hash = keccak256(
            b"ClaimPointsRewards(address user,uint256 distributionId,uint256 amount,uint256 nonce,uint256 deadline)",
        );
        let user_addr = Address::from_str(user).context("invalid user address")?;
        let struct_hash = keccak256(encode(&[
            Token::FixedBytes(claim_type_hash.to_vec()),
            Token::Address(user_addr),
            Token::Uint(dist_id_u256),
            Token::Uint(amount_wei),
            Token::Uint(U256::from(nonce)),
            Token::Uint(U256::from(deadline_secs)),
        ]));

        // Final EIP-712 digest: keccak256("\x19\x01" || domainSeparator || structHash)
        let mut prefix = vec![0x19u8, 0x01];
        prefix.extend_from_slice(&domain_separator);
        prefix.extend_from_slice(&struct_hash);
        let digest = keccak256(&prefix);

        let signature: Signature = self
            .wallet
            .sign_hash(digest.into())
            .context("sign_hash failed")?;
        let sig_hex = format!("0x{}", hex::encode(signature.to_vec()));

        Ok(ClaimSignaturePayload {
            user: user.to_string(),
            distribution_id: distribution_id_uuid.to_string(),
            amount: amount_decimal_string.to_string(),
            nonce,
            deadline: deadline_secs,
            signature: sig_hex,
            domain: ClaimDomain {
                name: self.domain_name.clone(),
                version: crate::constants::eip712_domains::domain_version().to_string(),
                chain_id: self.chain_id,
                verifying_contract: self.verifying_contract.clone(),
            },
        })
    }
}

/// Multiply a base-10 decimal string by 10^18 → U256.
/// Truncates anything past 18 fractional digits.
fn decimal_to_wei18(s: &str) -> Result<U256> {
    let s = s.trim();
    let (int_part, frac_part) = match s.split_once('.') {
        Some((i, f)) => (i, f),
        None => (s, ""),
    };
    // Pad / truncate fractional part to 18 digits.
    let mut frac = frac_part.chars().take(18).collect::<String>();
    while frac.len() < 18 {
        frac.push('0');
    }
    let combined = format!("{}{}", int_part, frac);
    U256::from_dec_str(&combined).context("amount parse failure")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wei_conversion() {
        // Helper to shorten the expects below; U256::from_dec_str only
        // fails on malformed literals which are all hard-coded here.
        let dec = |s: &str| U256::from_dec_str(s).expect("static decimal literal");

        // 1 → 1e18
        assert_eq!(decimal_to_wei18("1").expect("valid"), dec("1000000000000000000"));
        // 1.5 → 1.5e18
        assert_eq!(decimal_to_wei18("1.5").expect("valid"), dec("1500000000000000000"));
        // 0.0000001 → 1e11
        assert_eq!(decimal_to_wei18("0.0000001").expect("valid"), dec("100000000000"));
    }
}
