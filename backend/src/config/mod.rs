use serde::Deserialize;

use crate::constants::{api_urls, cache_ttl, jwt, pool, ports, price_feed, trading, block_sync};

pub mod validator;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    #[serde(default = "default_environment")]
    pub environment: String,

    #[serde(default = "default_port")]
    pub port: u16,

    pub database_url: String,

    #[serde(default)]
    pub redis_url: Option<String>,

    pub jwt_secret: String,

    #[serde(default = "default_jwt_expiry")]
    pub jwt_expiry_seconds: u64,

    // Auth settings - set to true to disable JWT/EIP verification
    #[serde(default)]
    pub auth_disabled: bool,

    // Blockchain settings
    pub rpc_url: String,
    pub chain_id: u64,
    pub vault_address: String,
    pub referral_storage_address: String,
    pub referral_rebate_address: String,

    // Collateral token settings (REQUIRED - no defaults, must be set per environment)
    #[serde(default = "default_collateral_token_symbol")]
    pub collateral_token_symbol: String,

    // REQUIRED: Must be set in environment config (.env.mainnet or .env.sepolia)
    pub collateral_token_address: String,

    #[serde(default = "default_collateral_token_decimals")]
    pub collateral_token_decimals: u8,

    // Legacy token addresses (for backwards compatibility)
    #[serde(default = "default_usdc_address")]
    pub usdc_address: String,

    #[serde(default = "default_weth_address")]
    pub weth_address: String,

    // Backend signer for withdrawals
    pub backend_signer_private_key: String,

    // EIP-712 Domain Configuration
    #[serde(default = "default_eip712_domain_name")]
    pub eip712_domain_name: String,

    #[serde(default = "default_eip712_domain_version")]
    pub eip712_domain_version: String,

    #[serde(default = "default_eip712_referral_domain_name")]
    pub eip712_referral_domain_name: String,

    // Price feed settings
    #[serde(default = "default_price_feed_url")]
    pub price_feed_url: String,

    #[serde(default = "default_hyperliquid_api_url")]
    pub hyperliquid_api_url: String,

    #[serde(default = "default_price_feed_top_markets")]
    pub price_feed_top_markets: usize,

    #[serde(default = "default_price_feed_update_interval")]
    pub price_feed_update_interval_secs: u64,

    #[serde(default = "default_price_feed_market_refresh")]
    pub price_feed_market_refresh_secs: u64,

    // Position service settings
    #[serde(default = "default_min_collateral_usd")]
    pub min_collateral_usd: String,

    #[serde(default = "default_min_position_size_usd")]
    pub min_position_size_usd: String,

    #[serde(default = "default_max_leverage")]
    pub max_leverage: i32,

    #[serde(default = "default_maintenance_margin_rate")]
    pub maintenance_margin_rate: String,

    #[serde(default = "default_position_fee_rate")]
    pub position_fee_rate: String,

    // Block sync settings
    #[serde(default = "default_block_sync_lookback")]
    pub block_sync_lookback: u64,

    // Cache TTL settings (seconds)
    #[serde(default = "default_cache_ttl_price")]
    pub cache_ttl_price: u64,

    #[serde(default = "default_cache_ttl_ticker")]
    pub cache_ttl_ticker: u64,

    #[serde(default = "default_cache_ttl_balance")]
    pub cache_ttl_balance: u64,

    #[serde(default = "default_cache_ttl_positions")]
    pub cache_ttl_positions: u64,

    #[serde(default = "default_cache_ttl_funding")]
    pub cache_ttl_funding: u64,

    #[serde(default = "default_cache_ttl_session")]
    pub cache_ttl_session: u64,

    #[serde(default = "default_cache_ttl_nonce")]
    pub cache_ttl_nonce: u64,

    #[serde(default = "default_cache_ttl_rate_limit")]
    pub cache_ttl_rate_limit: u64,

    #[serde(default = "default_cache_ttl_kline")]
    pub cache_ttl_kline: u64,

    // Database pool settings
    #[serde(default = "default_db_max_connections")]
    pub db_max_connections: u32,

    #[serde(default = "default_db_min_connections")]
    pub db_min_connections: u32,

    #[serde(default = "default_db_acquire_timeout_secs")]
    pub db_acquire_timeout_secs: u64,

    #[serde(default = "default_db_idle_timeout_secs")]
    pub db_idle_timeout_secs: u64,

    #[serde(default = "default_db_max_lifetime_secs")]
    pub db_max_lifetime_secs: u64,

    // Admin API key for protected internal endpoints
    #[serde(default)]
    pub admin_api_key: Option<String>,

    // ==========================================================================
    // V2 Security Configuration
    // ==========================================================================

    /// CORS allowed origins (comma-separated). If not set, uses environment-based defaults.
    /// Development: allows all origins
    /// Staging: allows test domains + localhost
    /// Production: allows only production domains
    #[serde(default)]
    pub cors_allowed_origins: Option<String>,

    /// Whether to show verbose error messages to clients.
    /// Development: true (show detailed errors)
    /// Staging/Production: false (generic error messages)
    #[serde(default)]
    pub verbose_errors: bool,

    /// Validate configuration on startup. Recommended to keep enabled.
    #[serde(default = "default_validate_config")]
    pub validate_config_on_start: bool,

    // ==========================================================================

    // Points System Configuration (Phase 3)
    // ==========================================================================

    /// Enable/disable points system globally
    #[serde(default = "default_points_enabled")]
    pub points_enabled: bool,

    /// Enable/disable individual point types
    #[serde(default = "default_points_trading_enabled")]
    pub points_trading_enabled: bool,

    #[serde(default = "default_points_pnl_enabled")]
    pub points_pnl_enabled: bool,

    #[serde(default = "default_points_holding_enabled")]
    pub points_holding_enabled: bool,

    #[serde(default = "default_points_referral_enabled")]
    pub points_referral_enabled: bool,

    #[serde(default = "default_points_staking_enabled")]
    pub points_staking_enabled: bool,

    /// Points cache TTL in seconds
    #[serde(default = "default_points_cache_ttl")]
    pub points_cache_ttl: u64,

    /// Leaderboard size limit
    #[serde(default = "default_points_leaderboard_limit")]
    pub points_leaderboard_limit: usize,

    /// Scheduler intervals (in seconds)
    #[serde(default = "default_points_holding_interval")]
    pub points_holding_interval_secs: u64,

    #[serde(default = "default_points_staking_interval")]
    pub points_staking_interval_secs: u64,

    #[serde(default = "default_points_leaderboard_refresh_interval")]
    pub points_leaderboard_refresh_interval_secs: u64,

    #[serde(default = "default_points_epoch_transition_interval")]
    pub points_epoch_transition_interval_secs: u64,

    // ==========================================================================
    // Spot subsystem configuration (sub-project 1: DF wallet on BSC)
    // ==========================================================================
    /// Populated from SPOT_* env vars only when SPOT_ENABLED=true.
    /// None when the subsystem is disabled (default for all environments
    /// until BSC vault is deployed).
    #[serde(skip)]
    pub spot: Option<crate::services::spot::config::SpotConfig>,
}

fn default_weth_address() -> String {
    // Legacy - not supported for deposits (only USDT supported)
    "0x0000000000000000000000000000000000000000".to_string()
}

fn default_usdc_address() -> String {
    // Legacy - not supported for deposits (only USDT supported)
    "0x0000000000000000000000000000000000000000".to_string()
}

fn default_collateral_token_symbol() -> String {
    "USDT".to_string()
}

fn default_collateral_token_decimals() -> u8 {
    6 // Default for most stablecoins, override via COLLATERAL_TOKEN_DECIMALS env var
}

// EIP-712 Domain defaults — all values should be overridden via .env
fn default_eip712_domain_name() -> String {
    "ZTDX Vault".to_string()
}

fn default_eip712_domain_version() -> String {
    "1".to_string()
}

fn default_eip712_referral_domain_name() -> String {
    "ZTDX Reward Router".to_string()
}

// Price feed URL defaults
fn default_price_feed_url() -> String {
    api_urls::BINANCE_FUTURES_API.to_string()
}

fn default_hyperliquid_api_url() -> String {
    api_urls::HYPERLIQUID_INFO_API.to_string()
}

fn default_environment() -> String {
    "development".to_string()
}

fn default_port() -> u16 {
    ports::DEFAULT_HTTP_PORT
}

fn default_jwt_expiry() -> u64 {
    jwt::EXPIRY_SECONDS
}

fn default_price_feed_top_markets() -> usize {
    price_feed::TOP_MARKETS
}

fn default_price_feed_update_interval() -> u64 {
    price_feed::UPDATE_INTERVAL_SECS
}

fn default_price_feed_market_refresh() -> u64 {
    price_feed::MARKET_REFRESH_SECS
}

fn default_min_collateral_usd() -> String {
    trading::MIN_COLLATERAL_USD.to_string()
}

fn default_min_position_size_usd() -> String {
    trading::MIN_POSITION_SIZE_USD.to_string()
}

fn default_max_leverage() -> i32 {
    trading::MAX_LEVERAGE
}

fn default_maintenance_margin_rate() -> String {
    trading::MAINTENANCE_MARGIN_RATE.to_string()
}

fn default_position_fee_rate() -> String {
    trading::POSITION_FEE_RATE.to_string()
}

fn default_block_sync_lookback() -> u64 {
    block_sync::LOOKBACK_BLOCKS
}

// Cache TTL defaults (seconds) - values from constants::cache_ttl
fn default_cache_ttl_price() -> u64 {
    cache_ttl::PRICE
}

fn default_cache_ttl_ticker() -> u64 {
    cache_ttl::TICKER
}

fn default_cache_ttl_balance() -> u64 {
    cache_ttl::BALANCE
}

fn default_cache_ttl_positions() -> u64 {
    cache_ttl::POSITIONS
}

fn default_cache_ttl_funding() -> u64 {
    cache_ttl::FUNDING
}

fn default_cache_ttl_session() -> u64 {
    cache_ttl::SESSION
}

fn default_cache_ttl_nonce() -> u64 {
    cache_ttl::NONCE
}

fn default_cache_ttl_rate_limit() -> u64 {
    cache_ttl::RATE_LIMIT
}

fn default_cache_ttl_kline() -> u64 {
    cache_ttl::KLINE
}

// V2 Security defaults
fn default_validate_config() -> bool {
    true
}

// Points system defaults
fn default_points_enabled() -> bool {
    true
}

fn default_points_trading_enabled() -> bool {
    true
}

fn default_points_pnl_enabled() -> bool {
    true
}

fn default_points_holding_enabled() -> bool {
    true
}

fn default_points_referral_enabled() -> bool {
    true
}

fn default_points_staking_enabled() -> bool {
    true
}

fn default_points_cache_ttl() -> u64 {
    60 // 60 seconds
}

fn default_points_leaderboard_limit() -> usize {
    100
}

fn default_points_holding_interval() -> u64 {
    3600 // 1 hour
}

fn default_points_staking_interval() -> u64 {
    86400 // 1 day
}

fn default_points_leaderboard_refresh_interval() -> u64 {
    3600 // 1 hour
}

fn default_points_epoch_transition_interval() -> u64 {
    3600 // 1 hour
}

// Database pool defaults - values from constants::pool
fn default_db_max_connections() -> u32 {
    pool::MAX_CONNECTIONS
}

fn default_db_min_connections() -> u32 {
    pool::MIN_CONNECTIONS
}

fn default_db_acquire_timeout_secs() -> u64 {
    pool::ACQUIRE_TIMEOUT_SECS
}

fn default_db_idle_timeout_secs() -> u64 {
    pool::IDLE_TIMEOUT_SECS
}

fn default_db_max_lifetime_secs() -> u64 {
    pool::MAX_LIFETIME_SECS
}

impl AppConfig {
    pub fn load() -> anyhow::Result<Self> {
        use anyhow::Context;

        let config = config::Config::builder()
            .add_source(config::Environment::default())
            .build()?;

        let mut app_config: AppConfig = config.try_deserialize()?;

        // Load subsystem configs that require custom env parsing.
        app_config.spot = crate::services::spot::config::SpotConfig::from_env()
            .context("loading SpotConfig from env")?;

        Ok(app_config)
    }

    /// Get token address by symbol (only USDT supported)
    pub fn get_token_address(&self, symbol: &str) -> Option<&str> {
        let upper = symbol.to_uppercase();
        // Only USDT is supported for deposits
        if upper == self.collateral_token_symbol.to_uppercase() || upper == "USDT" {
            Some(&self.collateral_token_address)
        } else {
            None
        }
    }

    /// Get token symbol by address (only USDT supported)
    pub fn get_token_symbol(&self, address: &str) -> Option<&str> {
        let addr_lower = address.to_lowercase();
        // Only USDT is supported for deposits
        if addr_lower == self.collateral_token_address.to_lowercase() {
            Some(&self.collateral_token_symbol)
        } else {
            None
        }
    }

    /// Get collateral token address
    pub fn collateral_token(&self) -> &str {
        &self.collateral_token_address
    }

    /// Get collateral token symbol (e.g., "USDT")
    pub fn collateral_symbol(&self) -> &str {
        &self.collateral_token_symbol
    }

    /// Get collateral token decimals
    pub fn collateral_decimals(&self) -> u8 {
        self.collateral_token_decimals
    }

    /// Check if auth is disabled (for development)
    pub fn is_auth_disabled(&self) -> bool {
        self.auth_disabled
    }

    // ==========================================================================
    // V2 Security Methods
    // ==========================================================================

    /// Check if running in development environment
    pub fn is_development(&self) -> bool {
        self.environment == "development"
    }

    /// Check if running in staging environment
    pub fn is_staging(&self) -> bool {
        self.environment == "staging"
    }

    /// Check if running in production environment
    pub fn is_production(&self) -> bool {
        self.environment == "production"
    }

    /// Get CORS allowed origins based on configuration or environment defaults
    pub fn get_cors_origins(&self) -> Vec<String> {
        // If explicitly configured, use that
        if let Some(ref origins) = self.cors_allowed_origins {
            return origins
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
        }

        // Otherwise use environment-based defaults
        match self.environment.as_str() {
            "development" => vec![], // Empty means allow all
            "staging" => vec![
                "https://8a27.xyz".to_string(),
                "https://www.8a27.xyz".to_string(),
                "https://api.ztdx.io".to_string(),
                "http://localhost:3000".to_string(),
                "http://localhost:5173".to_string(),
            ],
            _ => vec![
                "https://renance.xyz".to_string(),
                "https://www.renance.xyz".to_string(),
                "https://app.renance.xyz".to_string(),
            ],
        }
    }

    /// Check if verbose errors should be shown to clients
    pub fn should_show_verbose_errors(&self) -> bool {
        self.verbose_errors || self.is_development()
    }

    /// Check if auth bypass is actually allowed (only in development)
    pub fn is_auth_bypass_allowed(&self) -> bool {
        self.auth_disabled && self.is_development()
    }

    // ==========================================================================
    // Points System Methods
    // ==========================================================================

    /// Get points system configuration from app config
    pub fn get_points_config(&self) -> crate::services::points::PointsConfig {
        crate::services::points::PointsConfig {
            enabled: self.points_enabled,
            trading_enabled: self.points_trading_enabled,
            pnl_enabled: self.points_pnl_enabled,
            holding_enabled: self.points_holding_enabled,
            referral_enabled: self.points_referral_enabled,
            staking_enabled: self.points_staking_enabled,
            cache_ttl: self.points_cache_ttl,
            leaderboard_limit: self.points_leaderboard_limit,
        }
    }

}
