//! Spot subsystem configuration. Loaded from SPOT_* env vars.

use anyhow::{anyhow, Context, Result};
use ethers::types::Address;
use rust_decimal::Decimal;
use std::str::FromStr;

/// A wrapper that prevents the inner string from being printed via `Debug`,
/// `Display`, or any standard formatting trait. Use `expose_secret()` only
/// at the call site that genuinely needs the raw value (e.g., signer init).
#[derive(Clone)]
pub struct SecretString(String);

impl SecretString {
    pub fn new(s: String) -> Self { Self(s) }
    /// Returns the inner string. Call sites using this MUST NOT log the result.
    pub fn expose_secret(&self) -> &str { &self.0 }
}

impl std::fmt::Debug for SecretString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SecretString(<redacted>)")
    }
}

#[derive(Clone)]
pub struct SpotConfig {
    pub df_token_address: Address,
    pub df_token_decimals: u8,
    pub bsc_chain_id: u64,
    pub bsc_rpc_url: String,
    pub bsc_vault_address: Address,
    pub bsc_confirmation_depth: u64,
    pub bsc_poll_interval_ms: u64,
    pub bsc_start_block: Option<u64>,
    pub eip712_domain_name: String,
    pub eip712_domain_version: String,
    /// Hex-encoded private key for signing BSC vault releaseFunds messages.
    /// NEVER log this. NEVER include in Debug output (Debug derive uses a
    /// redacted format below).
    pub withdraw_signer_private_key: SecretString,
    pub withdraw_nonce_ttl_secs: u64,
    pub withdraw_min_amount_df: Decimal,
    pub reconciler_interval_secs: u64,
    /// Master gate for the spot CLOB trading subsystem (engine, REST/WS order
    /// endpoints, match-loop). When false the wallet/deposit/withdraw paths
    /// remain available but order placement is rejected. Driven by
    /// `SPOT_TRADING_ENABLED` (truthy: "1"/"true"/"TRUE"). Defaults to false.
    pub trading_enabled: bool,
}

impl SpotConfig {
    /// Returns Ok(None) when SPOT_ENABLED is unset / "false" / "0".
    /// Returns Ok(Some(cfg)) when SPOT_ENABLED=true AND all required vars are present.
    /// Returns Err if SPOT_ENABLED=true but a required var is missing/malformed.
    pub fn from_env() -> Result<Option<Self>> {
        let enabled = matches!(
            std::env::var("SPOT_ENABLED").ok().as_deref(),
            Some("1") | Some("true") | Some("TRUE")
        );
        if !enabled {
            return Ok(None);
        }

        let get = |k: &str| std::env::var(k).map_err(|_| anyhow!("{} not set", k));

        let df_token_address = Address::from_str(&get("SPOT_DF_TOKEN_ADDRESS")?)
            .context("SPOT_DF_TOKEN_ADDRESS not a valid hex address")?;
        let df_token_decimals: u8 = get("SPOT_DF_TOKEN_DECIMALS")?
            .parse().context("SPOT_DF_TOKEN_DECIMALS not a valid u8")?;
        let bsc_chain_id: u64 = get("SPOT_BSC_CHAIN_ID")?
            .parse().context("SPOT_BSC_CHAIN_ID not a valid u64")?;
        let bsc_rpc_url = get("SPOT_BSC_RPC_URL")?;
        let bsc_vault_address = Address::from_str(&get("SPOT_BSC_VAULT_ADDRESS")?)
            .context("SPOT_BSC_VAULT_ADDRESS not a valid hex address")?;
        let bsc_confirmation_depth: u64 = get("SPOT_BSC_CONFIRMATION_DEPTH")?
            .parse().context("SPOT_BSC_CONFIRMATION_DEPTH not a valid u64")?;
        let bsc_poll_interval_ms: u64 = get("SPOT_BSC_POLL_INTERVAL_MS")?
            .parse().context("SPOT_BSC_POLL_INTERVAL_MS not a valid u64")?;
        let bsc_start_block = std::env::var("SPOT_BSC_START_BLOCK").ok()
            .filter(|s| !s.is_empty())
            .map(|s| s.parse::<u64>().context("SPOT_BSC_START_BLOCK not a valid u64"))
            .transpose()?;
        let eip712_domain_name = get("SPOT_EIP712_DOMAIN_NAME")?;
        let eip712_domain_version = get("SPOT_EIP712_DOMAIN_VERSION")?;
        let withdraw_signer_private_key = SecretString::new(get("SPOT_WITHDRAW_SIGNER_PRIVATE_KEY")?);
        let withdraw_nonce_ttl_secs: u64 = get("SPOT_WITHDRAW_NONCE_TTL_SECS")?
            .parse().context("SPOT_WITHDRAW_NONCE_TTL_SECS not a valid u64")?;
        let withdraw_min_amount_df = Decimal::from_str(&get("SPOT_WITHDRAW_MIN_AMOUNT_DF")?)
            .context("SPOT_WITHDRAW_MIN_AMOUNT_DF not a valid decimal")?;
        let reconciler_interval_secs: u64 = get("SPOT_RECONCILER_INTERVAL_SECS")?
            .parse().context("SPOT_RECONCILER_INTERVAL_SECS not a valid u64")?;
        let trading_enabled = matches!(
            std::env::var("SPOT_TRADING_ENABLED").ok().as_deref(),
            Some("1") | Some("true") | Some("TRUE")
        );

        Ok(Some(SpotConfig {
            df_token_address,
            df_token_decimals,
            bsc_chain_id,
            bsc_rpc_url,
            bsc_vault_address,
            bsc_confirmation_depth,
            bsc_poll_interval_ms,
            bsc_start_block,
            eip712_domain_name,
            eip712_domain_version,
            withdraw_signer_private_key,
            withdraw_nonce_ttl_secs,
            withdraw_min_amount_df,
            reconciler_interval_secs,
            trading_enabled,
        }))
    }
}

// Manual Debug to redact the signer key.
impl std::fmt::Debug for SpotConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpotConfig")
            .field("df_token_address", &self.df_token_address)
            .field("df_token_decimals", &self.df_token_decimals)
            .field("bsc_chain_id", &self.bsc_chain_id)
            .field("bsc_rpc_url", &self.bsc_rpc_url)
            .field("bsc_vault_address", &self.bsc_vault_address)
            .field("bsc_confirmation_depth", &self.bsc_confirmation_depth)
            .field("bsc_poll_interval_ms", &self.bsc_poll_interval_ms)
            .field("bsc_start_block", &self.bsc_start_block)
            .field("eip712_domain_name", &self.eip712_domain_name)
            .field("eip712_domain_version", &self.eip712_domain_version)
            .field("withdraw_signer_private_key", &self.withdraw_signer_private_key)
            .field("withdraw_nonce_ttl_secs", &self.withdraw_nonce_ttl_secs)
            .field("withdraw_min_amount_df", &self.withdraw_min_amount_df)
            .field("reconciler_interval_secs", &self.reconciler_interval_secs)
            .field("trading_enabled", &self.trading_enabled)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Env-mutating tests serialize on this Mutex so cargo test's parallel
    // runner doesn't race them against each other (no new dep needed).
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn from_env_returns_none_when_disabled() {
        let _g = ENV_LOCK.lock().unwrap();
        std::env::remove_var("SPOT_ENABLED");
        assert!(SpotConfig::from_env().unwrap().is_none());

        std::env::set_var("SPOT_ENABLED", "false");
        assert!(SpotConfig::from_env().unwrap().is_none());
        std::env::remove_var("SPOT_ENABLED");
    }

    #[test]
    fn from_env_errors_when_enabled_but_missing_required() {
        let _g = ENV_LOCK.lock().unwrap();
        std::env::set_var("SPOT_ENABLED", "true");
        std::env::remove_var("SPOT_DF_TOKEN_ADDRESS");
        let result = SpotConfig::from_env();
        assert!(result.is_err());
        std::env::remove_var("SPOT_ENABLED"); // cleanup
    }

    #[test]
    fn secret_string_redacts_in_debug() {
        let s = SecretString::new("super-secret".to_string());
        let formatted = format!("{:?}", s);
        assert!(!formatted.contains("super-secret"));
        assert!(formatted.contains("redacted"));
    }

    #[test]
    fn trading_enabled_is_moot_when_spot_disabled() {
        // SPOT_TRADING_ENABLED has no effect when the wallet subsystem is off:
        // from_env still returns Ok(None) without ever reading the trading flag.
        let _g = ENV_LOCK.lock().unwrap();
        std::env::remove_var("SPOT_ENABLED");
        std::env::set_var("SPOT_TRADING_ENABLED", "true");
        assert!(SpotConfig::from_env().unwrap().is_none());
        std::env::remove_var("SPOT_TRADING_ENABLED");
    }
}
