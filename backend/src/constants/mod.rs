//! Global constants for ZTDX Backend
//!
//! This module centralizes all hardcoded values for better maintainability.
//! All magic numbers and URLs should be defined here.

pub mod eip712_domains;

/// External API URLs
pub mod api_urls {
    /// Binance Futures API base URL
    pub const BINANCE_FUTURES_API: &str = "https://fapi.binance.com";

    /// HyperLiquid Info API URL
    pub const HYPERLIQUID_INFO_API: &str = "https://api.hyperliquid.xyz/info";

    /// HyperLiquid Exchange API URL
    #[allow(dead_code)]
    pub const HYPERLIQUID_EXCHANGE_API: &str = "https://api.hyperliquid.xyz/exchange";

    /// HyperLiquid WebSocket URL
    #[allow(dead_code)]
    pub const HYPERLIQUID_WS_URL: &str = "wss://api.hyperliquid.xyz/ws";
}

/// Default ports
pub mod ports {
    /// Default HTTP server port
    pub const DEFAULT_HTTP_PORT: u16 = 8080;

    /// Default Redis port
    #[allow(dead_code)]
    pub const DEFAULT_REDIS_PORT: u16 = 6379;

    /// Default PostgreSQL port
    #[allow(dead_code)]
    pub const DEFAULT_POSTGRES_PORT: u16 = 5432;
}

/// Cache TTL values in seconds
pub mod cache_ttl {
    /// Price data TTL
    pub const PRICE: u64 = 5;

    /// Ticker data TTL
    pub const TICKER: u64 = 5;

    /// User balance TTL
    pub const BALANCE: u64 = 30;

    /// Position data TTL
    pub const POSITIONS: u64 = 10;

    /// Funding rate TTL
    pub const FUNDING: u64 = 60;

    /// User session TTL (24 hours)
    pub const SESSION: u64 = 86400;

    /// Nonce TTL (5 minutes)
    pub const NONCE: u64 = 300;

    /// Rate limit window TTL
    pub const RATE_LIMIT: u64 = 60;

    /// K-line data TTL
    pub const KLINE: u64 = 60;
}

/// Database connection pool configuration
pub mod pool {
    /// Maximum number of connections in pool
    pub const MAX_CONNECTIONS: u32 = 200;

    /// Minimum number of connections in pool
    pub const MIN_CONNECTIONS: u32 = 50;

    /// Connection acquire timeout in seconds
    pub const ACQUIRE_TIMEOUT_SECS: u64 = 10;

    /// Idle connection timeout in seconds (10 minutes)
    pub const IDLE_TIMEOUT_SECS: u64 = 600;

    /// Maximum connection lifetime in seconds (1 hour)
    pub const MAX_LIFETIME_SECS: u64 = 3600;
}

/// Broadcast channel capacities
pub mod channels {
    /// Trade event channel capacity
    pub const TRADE_CHANNEL_CAPACITY: usize = 10000;

    /// Orderbook update channel capacity
    pub const ORDERBOOK_CHANNEL_CAPACITY: usize = 10000;

    /// Order update channel capacity
    pub const ORDER_UPDATE_CHANNEL_CAPACITY: usize = 1000;

    /// K-line update channel capacity
    pub const KLINE_CHANNEL_CAPACITY: usize = 10000;
}

/// USDT precision constants
pub mod precision {
    /// USDT decimal places
    #[allow(dead_code)]
    pub const USDT_DECIMALS: u8 = 6;

    /// USDT multiplier (10^6)
    #[allow(dead_code)]
    pub const USDT_MULTIPLIER: u64 = 1_000_000;
}

/// Retry configuration defaults
pub mod retry {
    /// Maximum number of retry attempts
    pub const MAX_ATTEMPTS: u32 = 3;

    /// Initial delay before first retry (milliseconds)
    pub const INITIAL_DELAY_MS: u64 = 100;

    /// Maximum delay between retries (milliseconds)
    pub const MAX_DELAY_MS: u64 = 5000;

    /// Backoff multiplier for exponential backoff
    pub const BACKOFF_MULTIPLIER: f64 = 2.0;
}

/// Price feed configuration defaults
pub mod price_feed {
    /// Number of top markets to track
    pub const TOP_MARKETS: usize = 200;

    /// Price update interval in seconds
    pub const UPDATE_INTERVAL_SECS: u64 = 5;

    /// Market list refresh interval in seconds
    pub const MARKET_REFRESH_SECS: u64 = 300;
}

/// Trading configuration defaults
pub mod trading {
    /// Maximum leverage allowed
    pub const MAX_LEVERAGE: i32 = 100;

    /// Minimum collateral in USD
    pub const MIN_COLLATERAL_USD: &str = "10";

    /// Minimum position size in USD
    pub const MIN_POSITION_SIZE_USD: &str = "10";

    /// Maintenance margin rate (0.5%)
    pub const MAINTENANCE_MARGIN_RATE: &str = "0.005";

    /// Position fee rate (0.1%)
    pub const POSITION_FEE_RATE: &str = "0.001";
}

/// Dynamic fee adjustment configuration
pub mod dynamic_fee {
    /// Fee adjustment worker interval in seconds
    pub const ADJUSTMENT_INTERVAL_SECS: u64 = 60;

    /// Default fee sensitivity coefficient (k)
    #[allow(dead_code)]
    pub const DEFAULT_SENSITIVITY: &str = "1.5";

    /// Default fee floor (0.01%)
    #[allow(dead_code)]
    pub const DEFAULT_FEE_FLOOR: &str = "0.0001";

    /// Default fee ceiling (0.3%)
    #[allow(dead_code)]
    pub const DEFAULT_FEE_CEILING: &str = "0.003";

    /// Default base maker fee rate (0.02%)
    #[allow(dead_code)]
    pub const DEFAULT_MAKER_FEE: &str = "0.0002";

    /// Default base taker fee rate (0.05%)
    #[allow(dead_code)]
    pub const DEFAULT_TAKER_FEE: &str = "0.0005";
}

/// JWT configuration defaults
pub mod jwt {
    /// JWT expiry time in seconds (24 hours)
    pub const EXPIRY_SECONDS: u64 = 86400;
}

/// Block sync configuration
pub mod block_sync {
    /// Number of blocks to look back when syncing (~7 hours on Arbitrum)
    pub const LOOKBACK_BLOCKS: u64 = 100_000;
}
