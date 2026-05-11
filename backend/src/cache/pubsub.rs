//! Redis Pub/Sub Module
//!
//! Provides real-time data broadcasting capabilities using Redis Pub/Sub.
//! Used for broadcasting price updates, orderbook changes, and user notifications.

use redis::RedisError;
use serde::Serialize;
use std::sync::Arc;

use super::keys::CacheKey;
use super::redis_client::RedisClient;

/// Pub/Sub publisher for broadcasting messages
pub struct Publisher {
    redis: Arc<RedisClient>,
}

impl Publisher {
    /// Create new publisher
    pub fn new(redis: Arc<RedisClient>) -> Self {
        Self { redis }
    }

    /// Publish a message to a channel
    pub async fn publish(&self, channel: &str, message: &str) -> Result<i32, RedisError> {
        self.redis.publish(channel, message.to_string()).await
    }

    /// Publish JSON-serializable message
    pub async fn publish_json<T: Serialize>(
        &self,
        channel: &str,
        message: &T,
    ) -> Result<i32, RedisError> {
        let json = serde_json::to_string(message).map_err(|e| {
            RedisError::from((
                redis::ErrorKind::IoError,
                "Serialization error",
                e.to_string(),
            ))
        })?;
        self.publish(channel, &json).await
    }

    // ==================== Market Data Channels ====================

    /// Publish trade to symbol channel
    pub async fn publish_trade<T: Serialize>(
        &self,
        symbol: &str,
        trade: &T,
    ) -> Result<i32, RedisError> {
        let channel = CacheKey::channel_trades(symbol);
        self.publish_json(&channel, trade).await
    }

    /// Publish orderbook update to symbol channel
    pub async fn publish_orderbook<T: Serialize>(
        &self,
        symbol: &str,
        orderbook: &T,
    ) -> Result<i32, RedisError> {
        let channel = CacheKey::channel_orderbook(symbol);
        self.publish_json(&channel, orderbook).await
    }

    /// Publish ticker update to symbol channel
    pub async fn publish_ticker<T: Serialize>(
        &self,
        symbol: &str,
        ticker: &T,
    ) -> Result<i32, RedisError> {
        let channel = CacheKey::channel_ticker(symbol);
        self.publish_json(&channel, ticker).await
    }

    /// Publish K-line update to symbol/period channel
    pub async fn publish_kline<T: Serialize>(
        &self,
        symbol: &str,
        period: &str,
        kline: &T,
    ) -> Result<i32, RedisError> {
        let channel = CacheKey::channel_kline(symbol, period);
        self.publish_json(&channel, kline).await
    }

    // ==================== User Data Channels ====================

    /// Publish user order update
    pub async fn publish_user_order<T: Serialize>(
        &self,
        address: &str,
        order: &T,
    ) -> Result<i32, RedisError> {
        let channel = CacheKey::channel_user_orders(address);
        self.publish_json(&channel, order).await
    }

    /// Publish user position update
    pub async fn publish_user_position<T: Serialize>(
        &self,
        address: &str,
        position: &T,
    ) -> Result<i32, RedisError> {
        let channel = CacheKey::channel_user_positions(address);
        self.publish_json(&channel, position).await
    }
}

/// Subscriber configuration
#[derive(Debug, Clone)]
pub struct SubscriberConfig {
    /// Buffer size for broadcast channel
    pub buffer_size: usize,
    /// Whether to auto-reconnect on connection loss
    pub auto_reconnect: bool,
    /// Reconnect delay in milliseconds
    pub reconnect_delay_ms: u64,
}

impl Default for SubscriberConfig {
    fn default() -> Self {
        Self {
            buffer_size: 1024,
            auto_reconnect: true,
            reconnect_delay_ms: 1000,
        }
    }
}

/// Subscription handle for receiving messages
/// Note: Full subscription implementation requires redis pub/sub client
/// which is more complex. This is a placeholder for the interface.
#[derive(Debug)]
pub struct Subscription {
    pub channel: String,
}

/// Pub/Sub subscriber (placeholder implementation)
/// Full implementation requires dedicated pub/sub connection
pub struct Subscriber {
    redis_url: String,
    config: SubscriberConfig,
}

impl Subscriber {
    /// Create new subscriber
    pub fn new(redis_url: &str, config: SubscriberConfig) -> Self {
        Self {
            redis_url: redis_url.to_string(),
            config,
        }
    }

    /// Get the Redis URL
    pub fn redis_url(&self) -> &str {
        &self.redis_url
    }

    /// Get the config
    pub fn config(&self) -> &SubscriberConfig {
        &self.config
    }

    /// Subscribe to a channel (returns channel name)
    /// Full implementation would spawn a task to listen for messages
    pub fn subscribe(&self, channel: &str) -> Subscription {
        tracing::debug!("Creating subscription for channel: {}", channel);
        Subscription {
            channel: channel.to_string(),
        }
    }

    /// Get list of channels for market data
    pub fn get_market_channels(symbol: &str) -> Vec<String> {
        vec![
            CacheKey::channel_trades(symbol),
            CacheKey::channel_orderbook(symbol),
            CacheKey::channel_ticker(symbol),
        ]
    }

    /// Get channel for K-line
    pub fn get_kline_channel(symbol: &str, period: &str) -> String {
        CacheKey::channel_kline(symbol, period)
    }

    /// Get channels for user data
    pub fn get_user_channels(address: &str) -> Vec<String> {
        vec![
            CacheKey::channel_user_orders(address),
            CacheKey::channel_user_positions(address),
        ]
    }
}

/// Convenience struct for pub/sub operations
pub struct PubSubManager {
    publisher: Publisher,
    redis_url: String,
    subscriber_config: SubscriberConfig,
}

impl PubSubManager {
    /// Create new pub/sub manager
    pub fn new(redis: Arc<RedisClient>, redis_url: &str) -> Self {
        Self {
            publisher: Publisher::new(redis),
            redis_url: redis_url.to_string(),
            subscriber_config: SubscriberConfig::default(),
        }
    }

    /// Create with custom subscriber config
    pub fn with_config(
        redis: Arc<RedisClient>,
        redis_url: &str,
        subscriber_config: SubscriberConfig,
    ) -> Self {
        Self {
            publisher: Publisher::new(redis),
            redis_url: redis_url.to_string(),
            subscriber_config,
        }
    }

    /// Get publisher reference
    pub fn publisher(&self) -> &Publisher {
        &self.publisher
    }

    /// Create a new subscriber
    pub fn create_subscriber(&self) -> Subscriber {
        Subscriber::new(&self.redis_url, self.subscriber_config.clone())
    }

    /// Get Redis URL
    pub fn redis_url(&self) -> &str {
        &self.redis_url
    }

    // ==================== Convenience Methods for Channels ====================

    /// Get trade channel for a symbol
    pub fn trade_channel(&self, symbol: &str) -> String {
        CacheKey::channel_trades(symbol)
    }

    /// Get orderbook channel for a symbol
    pub fn orderbook_channel(&self, symbol: &str) -> String {
        CacheKey::channel_orderbook(symbol)
    }

    /// Get ticker channel for a symbol
    pub fn ticker_channel(&self, symbol: &str) -> String {
        CacheKey::channel_ticker(symbol)
    }

    /// Get K-line channel for a symbol/period
    pub fn kline_channel(&self, symbol: &str, period: &str) -> String {
        CacheKey::channel_kline(symbol, period)
    }

    /// Get user orders channel
    pub fn user_orders_channel(&self, address: &str) -> String {
        CacheKey::channel_user_orders(address)
    }

    /// Get user positions channel
    pub fn user_positions_channel(&self, address: &str) -> String {
        CacheKey::channel_user_positions(address)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subscriber_config_default() {
        let config = SubscriberConfig::default();
        assert_eq!(config.buffer_size, 1024);
        assert!(config.auto_reconnect);
        assert_eq!(config.reconnect_delay_ms, 1000);
    }

    #[test]
    fn test_channel_names() {
        assert_eq!(
            Subscriber::get_kline_channel("BTCUSDT", "1m"),
            "channel:kline:BTCUSDT:1m"
        );
    }
}
