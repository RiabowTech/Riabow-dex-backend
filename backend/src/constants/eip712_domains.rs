//! EIP-712 domain-name runtime configuration.
//!
//! All domain names are read from environment variables at startup.
//! No hardcoded values — each deployment configures its own domain
//! names via .env to match the deployed contracts.

use std::sync::OnceLock;

static REFERRAL_REBATE_DOMAIN: OnceLock<String> = OnceLock::new();
static EARN_DOMAIN: OnceLock<String> = OnceLock::new();
static POINTS_CLAIM_DOMAIN: OnceLock<String> = OnceLock::new();
static DOMAIN_VER: OnceLock<String> = OnceLock::new();

/// Initialize all EIP-712 domain names from environment variables.
/// Must be called once at startup (e.g. in bootstrap).
pub fn init_from_env() {
    REFERRAL_REBATE_DOMAIN.get_or_init(|| {
        std::env::var("EIP712_REFERRAL_DOMAIN_NAME")
            .unwrap_or_else(|_| "ZTDX Reward Router".to_string())
    });
    EARN_DOMAIN.get_or_init(|| {
        std::env::var("EIP712_EARN_DOMAIN_NAME")
            .unwrap_or_else(|_| "ZTDX Term Yield".to_string())
    });
    POINTS_CLAIM_DOMAIN.get_or_init(|| {
        std::env::var("POINTS_CLAIM_DOMAIN_NAME")
            .unwrap_or_else(|_| "ZTDX Points Distribution".to_string())
    });
    DOMAIN_VER.get_or_init(|| {
        std::env::var("EIP712_DOMAIN_VERSION")
            .unwrap_or_else(|_| "1".to_string())
    });
}

pub fn referral_rebate_domain_name() -> &'static str {
    REFERRAL_REBATE_DOMAIN.get().map(|s| s.as_str()).unwrap_or("ZTDX Reward Router")
}

pub fn earn_domain_name() -> &'static str {
    EARN_DOMAIN.get().map(|s| s.as_str()).unwrap_or("ZTDX Term Yield")
}

pub fn points_claim_domain_name_default() -> &'static str {
    POINTS_CLAIM_DOMAIN.get().map(|s| s.as_str()).unwrap_or("ZTDX Points Distribution")
}

pub fn domain_version() -> &'static str {
    DOMAIN_VER.get().map(|s| s.as_str()).unwrap_or("1")
}
