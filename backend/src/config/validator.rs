//! Configuration validator for startup checks
//!
//! Validates that the configuration is correct and safe for the current environment.
//! Prevents common misconfigurations like using test private keys in production.

use super::AppConfig;
use anyhow::{bail, Result};

/// Known Hardhat/development test private keys that should NEVER be used in production
const HARDHAT_TEST_KEYS: &[&str] = &[
    // Hardhat Account #0
    "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
    // Hardhat Account #1
    "59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d",
    // Hardhat Account #2
    "5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a",
    // Common test key
    "df57089febbacf7ba0bc227dafbffa9fc08a93fdc68e1e42411a14efcf23656e",
];

/// Configuration validator that runs on startup
pub struct ConfigValidator;

impl ConfigValidator {
    /// Validate the configuration for the current environment
    pub fn validate(config: &AppConfig) -> Result<()> {
        let mut errors: Vec<&str> = Vec::new();
        let mut warnings: Vec<String> = Vec::new();

        // =======================================================================
        // Required Fields
        // =======================================================================

        if config.database_url.is_empty() {
            errors.push("DATABASE_URL is required");
        }

        if config.jwt_secret.is_empty() {
            errors.push("JWT_SECRET is required");
        } else if config.jwt_secret.len() < 32 {
            warnings.push("JWT_SECRET should be at least 32 characters for security".to_string());
        }

        if config.backend_signer_private_key.is_empty() {
            errors.push("BACKEND_SIGNER_PRIVATE_KEY is required");
        }

        if config.rpc_url.is_empty() {
            errors.push("RPC_URL is required");
        }

        if config.vault_address.is_empty() {
            errors.push("VAULT_ADDRESS is required");
        }

        if config.collateral_token_address.is_empty() {
            errors.push("COLLATERAL_TOKEN_ADDRESS is required");
        }

        // =======================================================================
        // Format Validation
        // =======================================================================

        // Validate Ethereum addresses format
        if !Self::is_valid_eth_address(&config.vault_address) {
            errors.push("VAULT_ADDRESS must be a valid Ethereum address (0x + 40 hex chars)");
        }

        if !Self::is_valid_eth_address(&config.collateral_token_address) {
            errors.push("COLLATERAL_TOKEN_ADDRESS must be a valid Ethereum address");
        }

        if !Self::is_valid_eth_address(&config.referral_storage_address) {
            errors.push("REFERRAL_STORAGE_ADDRESS must be a valid Ethereum address");
        }

        if !Self::is_valid_eth_address(&config.referral_rebate_address) {
            errors.push("REFERRAL_REBATE_ADDRESS must be a valid Ethereum address");
        }

        // =======================================================================
        // Production-Specific Checks
        // =======================================================================

        if config.is_production() {
            // AUTH_DISABLED must not be true in production
            if config.auth_disabled {
                errors.push("AUTH_DISABLED cannot be true in production environment");
            }

            // Check for test private keys in production
            let pk_lower = config.backend_signer_private_key.to_lowercase();
            let pk_normalized = pk_lower.trim_start_matches("0x");

            for test_key in HARDHAT_TEST_KEYS {
                if pk_normalized == *test_key {
                    errors.push("CRITICAL: Production cannot use Hardhat/test private key!");
                    break;
                }
            }

            // Admin API key should be set in production
            if config.admin_api_key.is_none() {
                warnings.push("ADMIN_API_KEY is recommended in production".to_string());
            }

            // Check chain ID matches expected production chain
            if config.chain_id != 42161 {
                warnings.push(format!(
                    "Production typically uses Arbitrum One (chain_id=42161), got {}",
                    config.chain_id
                ));
            }

            // Verbose errors should be off in production
            if config.verbose_errors {
                warnings.push("VERBOSE_ERRORS should be false in production".to_string());
            }
        }

        // =======================================================================
        // Staging-Specific Checks
        // =======================================================================

        if config.is_staging() {
            // Check chain ID matches expected staging chain
            if config.chain_id != 421614 {
                warnings.push(format!(
                    "Staging typically uses Arbitrum Sepolia (chain_id=421614), got {}",
                    config.chain_id
                ));
            }
        }

        // =======================================================================
        // Development Warnings
        // =======================================================================

        if config.is_development() {
            if config.auth_disabled {
                tracing::warn!("⚠️  AUTH_DISABLED=true in development mode");
            }
        }

        // =======================================================================
        // Log Warnings
        // =======================================================================

        for warning in &warnings {
            tracing::warn!("⚠️  Config warning: {}", warning);
        }

        // =======================================================================
        // Return Result
        // =======================================================================

        if !errors.is_empty() {
            let error_list = errors.join("\n  - ");
            bail!("Configuration validation failed:\n  - {}", error_list);
        }

        Ok(())
    }

    /// Check if a string is a valid Ethereum address
    fn is_valid_eth_address(address: &str) -> bool {
        if !address.starts_with("0x") {
            return false;
        }
        if address.len() != 42 {
            return false;
        }
        // Check that all characters after 0x are valid hex
        address[2..].chars().all(|c| c.is_ascii_hexdigit())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_valid_eth_address() {
        assert!(ConfigValidator::is_valid_eth_address(
            "0x5d2efcbdC2dD4b9Ff06Ea396F62878Ef982377c2"
        ));
        assert!(ConfigValidator::is_valid_eth_address(
            "0x0000000000000000000000000000000000000000"
        ));
        assert!(!ConfigValidator::is_valid_eth_address("0x123")); // too short
        assert!(!ConfigValidator::is_valid_eth_address("123456789012345678901234567890123456789012")); // no 0x
        assert!(!ConfigValidator::is_valid_eth_address(
            "0x5d2efcbdC2dD4b9Ff06Ea396F62878Ef982377c2gg"
        )); // invalid hex
    }
}
