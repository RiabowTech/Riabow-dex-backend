//! Cache Key Naming Conventions
//!
//! Provides consistent key generation for Redis cache operations.
//! Format: {service}:{entity}:{identifier}:{field}

/// Cache key prefixes
pub mod prefix {
    pub const PRICE: &str = "price";
    pub const ORDERBOOK: &str = "orderbook";
    pub const USER: &str = "user";
    pub const SESSION: &str = "session";
    pub const NONCE: &str = "nonce";
    pub const RATE: &str = "rate";
    pub const TICKER: &str = "ticker";
    pub const FUNDING: &str = "funding";
    #[allow(dead_code)]
    pub const KLINE: &str = "kline";
    pub const CHANNEL: &str = "channel";
    pub const POSITION: &str = "position";
    pub const EARN: &str = "earn";
    pub const FEE: &str = "fee";
    #[allow(dead_code)]
    pub const OI: &str = "oi";
    #[allow(dead_code)]
    pub const MARKET_CONFIG: &str = "market_config";
}

/// Cache TTL values in seconds
pub mod ttl {
    /// Price data TTL (5 seconds)
    pub const PRICE: u64 = 5;
    /// Ticker data TTL (5 seconds)
    pub const TICKER: u64 = 5;
    /// User balance TTL (30 seconds)
    pub const BALANCE: u64 = 30;
    /// User positions TTL (10 seconds)
    pub const POSITIONS: u64 = 10;
    /// Funding rate TTL (60 seconds)
    pub const FUNDING: u64 = 60;
    /// Session TTL (24 hours)
    pub const SESSION: u64 = 86400;
    /// Nonce TTL (5 minutes)
    pub const NONCE: u64 = 300;
    /// Rate limit window (60 seconds)
    pub const RATE_LIMIT: u64 = 60;
    /// K-line data TTL (60 seconds)
    #[allow(dead_code)]
    pub const KLINE: u64 = 60;
    /// Earn performance history TTL (60 seconds)
    pub const EARN_PERFORMANCE: u64 = 60;
    /// Dynamic fee TTL (90 seconds - slightly longer than 60s worker interval)
    pub const DYNAMIC_FEE: u64 = 90;
    /// Market config TTL (30 seconds)
    #[allow(dead_code)]
    pub const MARKET_CONFIG: u64 = 30;
}

/// Cache key builders
pub struct CacheKey;

#[allow(dead_code)]
impl CacheKey {
    // ==================== Price Keys ====================

    /// Key for mark price: price:mark:{symbol}
    pub fn mark_price(symbol: &str) -> String {
        format!("{}:mark:{}", prefix::PRICE, symbol.to_uppercase())
    }

    /// Key for index price: price:index:{symbol}
    pub fn index_price(symbol: &str) -> String {
        format!("{}:index:{}", prefix::PRICE, symbol.to_uppercase())
    }

    /// Key for last price: price:last:{symbol}
    pub fn last_price(symbol: &str) -> String {
        format!("{}:last:{}", prefix::PRICE, symbol.to_uppercase())
    }

    // ==================== Orderbook Keys ====================

    /// Key for orderbook bids: orderbook:{symbol}:bids
    pub fn orderbook_bids(symbol: &str) -> String {
        format!("{}:{}:bids", prefix::ORDERBOOK, symbol.to_uppercase())
    }

    /// Key for orderbook asks: orderbook:{symbol}:asks
    pub fn orderbook_asks(symbol: &str) -> String {
        format!("{}:{}:asks", prefix::ORDERBOOK, symbol.to_uppercase())
    }

    /// Key for orderbook snapshot: orderbook:{symbol}:snapshot
    pub fn orderbook_snapshot(symbol: &str) -> String {
        format!("{}:{}:snapshot", prefix::ORDERBOOK, symbol.to_uppercase())
    }

    // ==================== User Keys ====================

    /// Key for user balance: user:balance:{address}
    /// Hash structure: field = token, value = amount
    pub fn user_balance(address: &str) -> String {
        format!("{}:balance:{}", prefix::USER, address.to_lowercase())
    }

    /// Key for user positions: user:positions:{address}
    pub fn user_positions(address: &str) -> String {
        format!("{}:positions:{}", prefix::USER, address.to_lowercase())
    }

    // ==================== Position Keys ====================

    /// Key for single position: position:{id}
    pub fn position(position_id: &str) -> String {
        format!("{}:{}", prefix::POSITION, position_id)
    }

    /// Key for user's position by symbol and side: position:user:{address}:{symbol}:{side}
    pub fn position_by_key(address: &str, symbol: &str, side: &str) -> String {
        format!(
            "{}:user:{}:{}:{}",
            prefix::POSITION,
            address.to_lowercase(),
            symbol.to_uppercase(),
            side.to_lowercase()
        )
    }

    /// Key for all positions of a user: position:user:{address}:*
    #[allow(dead_code)]
    pub fn position_user_pattern(address: &str) -> String {
        format!("{}:user:{}:*", prefix::POSITION, address.to_lowercase())
    }

    /// Key for user orders: user:orders:{address}
    pub fn user_orders(address: &str) -> String {
        format!("{}:orders:{}", prefix::USER, address.to_lowercase())
    }

    /// Key for user profile: user:profile:{address}
    pub fn user_profile(address: &str) -> String {
        format!("{}:profile:{}", prefix::USER, address.to_lowercase())
    }

    // ==================== Session Keys ====================

    /// Key for user session: session:{address}
    pub fn session(address: &str) -> String {
        format!("{}:{}", prefix::SESSION, address.to_lowercase())
    }

    /// Key for nonce: nonce:{address}
    pub fn nonce(address: &str) -> String {
        format!("{}:{}", prefix::NONCE, address.to_lowercase())
    }

    // ==================== Rate Limit Keys ====================

    /// Key for IP rate limit: rate:ip:{ip}
    pub fn rate_limit_ip(ip: &str) -> String {
        format!("{}:ip:{}", prefix::RATE, ip)
    }

    /// Key for user rate limit: rate:user:{address}
    pub fn rate_limit_user(address: &str) -> String {
        format!("{}:user:{}", prefix::RATE, address.to_lowercase())
    }

    /// Key for endpoint rate limit: rate:endpoint:{method}:{path}:{identifier}
    pub fn rate_limit_endpoint(method: &str, path: &str, identifier: &str) -> String {
        format!("{}:endpoint:{}:{}:{}", prefix::RATE, method, path, identifier)
    }

    // ==================== Ticker Keys ====================

    /// Key for ticker: ticker:{symbol}
    pub fn ticker(symbol: &str) -> String {
        format!("{}:{}", prefix::TICKER, symbol.to_uppercase())
    }

    // ==================== Funding Rate Keys ====================

    /// Key for funding rate: funding:{symbol}
    pub fn funding_rate(symbol: &str) -> String {
        format!("{}:{}", prefix::FUNDING, symbol.to_uppercase())
    }

    /// Key for funding rate info: funding:info:{symbol}
    pub fn funding_info(symbol: &str) -> String {
        format!("{}:info:{}", prefix::FUNDING, symbol.to_uppercase())
    }

    // ==================== K-line Keys ====================

    /// Key for K-line data: kline:{symbol}:{period}
    pub fn kline(symbol: &str, period: &str) -> String {
        format!("{}:{}:{}", prefix::KLINE, symbol.to_uppercase(), period)
    }

    /// Key for latest K-line: kline:{symbol}:{period}:latest
    pub fn kline_latest(symbol: &str, period: &str) -> String {
        format!("{}:{}:{}:latest", prefix::KLINE, symbol.to_uppercase(), period)
    }

    // ==================== Pub/Sub Channel Keys ====================

    /// Channel for trades: channel:trades:{symbol}
    pub fn channel_trades(symbol: &str) -> String {
        format!("{}:trades:{}", prefix::CHANNEL, symbol.to_uppercase())
    }

    /// Channel for orderbook: channel:orderbook:{symbol}
    pub fn channel_orderbook(symbol: &str) -> String {
        format!("{}:orderbook:{}", prefix::CHANNEL, symbol.to_uppercase())
    }

    /// Channel for ticker: channel:ticker:{symbol}
    pub fn channel_ticker(symbol: &str) -> String {
        format!("{}:ticker:{}", prefix::CHANNEL, symbol.to_uppercase())
    }

    /// Channel for K-line: channel:kline:{symbol}:{period}
    pub fn channel_kline(symbol: &str, period: &str) -> String {
        format!("{}:kline:{}:{}", prefix::CHANNEL, symbol.to_uppercase(), period)
    }

    /// Channel for user orders: channel:orders:{address}
    pub fn channel_user_orders(address: &str) -> String {
        format!("{}:orders:{}", prefix::CHANNEL, address.to_lowercase())
    }

    /// Channel for user positions: channel:positions:{address}
    pub fn channel_user_positions(address: &str) -> String {
        format!("{}:positions:{}", prefix::CHANNEL, address.to_lowercase())
    }

    // ==================== Earn Keys ====================

    /// Key for earn performance history: earn:performance:{limit}
    pub fn earn_performance(limit: i32) -> String {
        format!("{}:performance:{}", prefix::EARN, limit)
    }

    // ==================== Dynamic Fee Keys ====================

    /// Key for dynamic long taker fee: fee:dynamic:{symbol}:long_taker
    pub fn dynamic_long_taker_fee(symbol: &str) -> String {
        format!("{}:dynamic:{}:long_taker", prefix::FEE, symbol.to_uppercase())
    }

    /// Key for dynamic short taker fee: fee:dynamic:{symbol}:short_taker
    pub fn dynamic_short_taker_fee(symbol: &str) -> String {
        format!("{}:dynamic:{}:short_taker", prefix::FEE, symbol.to_uppercase())
    }

    /// Key for dynamic maker fee: fee:dynamic:{symbol}:maker
    pub fn dynamic_maker_fee(symbol: &str) -> String {
        format!("{}:dynamic:{}:maker", prefix::FEE, symbol.to_uppercase())
    }

    /// Key for dynamic imbalance ratio: fee:dynamic:{symbol}:imbalance
    pub fn dynamic_imbalance(symbol: &str) -> String {
        format!("{}:dynamic:{}:imbalance", prefix::FEE, symbol.to_uppercase())
    }

    // ==================== Open Interest Keys ====================

    /// Key for open interest: oi:{symbol}:{side}
    pub fn open_interest(symbol: &str, side: &str) -> String {
        format!("{}:{}:{}", prefix::OI, symbol.to_uppercase(), side.to_lowercase())
    }

    // ==================== Pattern Keys for Scanning ====================

    /// Pattern for all price keys: price:*
    pub fn pattern_all_prices() -> String {
        format!("{}:*", prefix::PRICE)
    }

    /// Pattern for all orderbook keys: orderbook:*
    pub fn pattern_all_orderbooks() -> String {
        format!("{}:*", prefix::ORDERBOOK)
    }

    /// Pattern for user's all keys: user:*:{address}
    pub fn pattern_user_all(address: &str) -> String {
        format!("{}:*:{}", prefix::USER, address.to_lowercase())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_price_keys() {
        assert_eq!(CacheKey::mark_price("btcusdt"), "price:mark:BTCUSDT");
        assert_eq!(CacheKey::index_price("ethusdt"), "price:index:ETHUSDT");
    }

    #[test]
    fn test_orderbook_keys() {
        assert_eq!(CacheKey::orderbook_bids("BTCUSDT"), "orderbook:BTCUSDT:bids");
        assert_eq!(CacheKey::orderbook_asks("btcusdt"), "orderbook:BTCUSDT:asks");
    }

    #[test]
    fn test_user_keys() {
        let addr = "0x1234ABCD";
        assert_eq!(CacheKey::user_balance(addr), "user:balance:0x1234abcd");
        assert_eq!(CacheKey::user_positions(addr), "user:positions:0x1234abcd");
    }

    #[test]
    fn test_channel_keys() {
        assert_eq!(CacheKey::channel_trades("BTCUSDT"), "channel:trades:BTCUSDT");
        assert_eq!(CacheKey::channel_kline("ethusdt", "1m"), "channel:kline:ETHUSDT:1m");
    }

    #[test]
    fn test_ttl_values() {
        assert_eq!(ttl::PRICE, 5);
        assert_eq!(ttl::SESSION, 86400);
        assert_eq!(ttl::BALANCE, 30);
    }
}
