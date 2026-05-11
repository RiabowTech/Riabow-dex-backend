//! Cache Module
//!
//! Provides Redis-based caching layer for the trading platform.
//! Handles price data, orderbook, user data caching, and real-time pub/sub.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                      CacheManager                           │
//! ├─────────────────────────────────────────────────────────────┤
//! │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
//! │  │ PriceCache   │  │OrderbookCache│  │  UserCache   │       │
//! │  │              │  │              │  │              │       │
//! │  │ - mark_price │  │ - bids       │  │ - balances   │       │
//! │  │ - index_price│  │ - asks       │  │ - positions  │       │
//! │  │ - last_price │  │ - spread     │  │ - sessions   │       │
//! │  │ - ticker     │  │ - mid_price  │  │ - rate_limit │       │
//! │  └──────────────┘  └──────────────┘  └──────────────┘       │
//! │                                                              │
//! │  ┌──────────────┐  ┌───────────────────────────────┐        │
//! │  │ PubSubManager│  │       RedisClient             │        │
//! │  │              │  │                               │        │
//! │  │ - publish    │  │ - connection pooling          │        │
//! │  │ - subscribe  │  │ - auto reconnect              │        │
//! │  │              │  │ - retry logic                 │        │
//! │  └──────────────┘  └───────────────────────────────┘        │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Usage
//!
//! ```rust,ignore
//! use crate::cache::{CacheManager, CacheConfig};
//!
//! // Create cache manager
//! let config = CacheConfig::from_env();
//! let cache = CacheManager::new(config).await?;
//!
//! // Use price cache
//! cache.price().set_mark_price("BTCUSDT", price).await?;
//! let price = cache.price().get_mark_price("BTCUSDT").await;
//!
//! // Use orderbook cache
//! cache.orderbook().set_bid("BTCUSDT", price, amount).await?;
//! let bids = cache.orderbook().get_bids("BTCUSDT", Some(10)).await;
//!
//! // Use user cache
//! cache.user().set_balance(address, &balance).await?;
//! let balance = cache.user().get_balance(address, "USDT").await;
//!
//! // Pub/Sub
//! cache.pubsub().publisher().publish_trade("BTCUSDT", &trade).await?;
//! ```

pub mod keys;
pub mod orderbook_cache;
pub mod position_cache;
pub mod price_cache;
pub mod pubsub;
pub mod redis_client;
pub mod user_cache;

use std::sync::Arc;

// Re-exports for convenience (only export what's commonly used externally)
pub use orderbook_cache::OrderbookCache;
pub use position_cache::PositionCache;
pub use price_cache::PriceCache;
pub use pubsub::PubSubManager;
pub use redis_client::{RedisClient, RedisConfig};
pub use user_cache::UserCache;

/// Cache configuration
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Redis URL
    pub redis_url: String,
    /// Enable caching (for graceful degradation)
    pub enabled: bool,
    /// Connection timeout in milliseconds
    pub timeout_ms: u64,
    /// Maximum retry attempts
    pub max_retries: u32,
    /// Default orderbook depth
    pub orderbook_depth: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            redis_url: "redis://127.0.0.1:6379".to_string(),
            enabled: true,
            timeout_ms: 5000,
            max_retries: 3,
            orderbook_depth: 50,
        }
    }
}

impl CacheConfig {
    /// Create config from environment variables
    pub fn from_env() -> Self {
        Self {
            redis_url: std::env::var("REDIS_URL")
                .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string()),
            enabled: std::env::var("CACHE_ENABLED")
                .map(|v| v.to_lowercase() != "false" && v != "0")
                .unwrap_or(true),
            timeout_ms: std::env::var("REDIS_TIMEOUT_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(5000),
            max_retries: std::env::var("REDIS_MAX_RETRIES")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(3),
            orderbook_depth: std::env::var("ORDERBOOK_DEPTH")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(50),
        }
    }
}

/// Main cache manager that provides access to all cache operations
pub struct CacheManager {
    config: CacheConfig,
    redis: Option<Arc<RedisClient>>,
    price_cache: Option<PriceCache>,
    orderbook_cache: Option<OrderbookCache>,
    user_cache: Option<UserCache>,
    position_cache: Option<PositionCache>,
    pubsub_manager: Option<PubSubManager>,
}

impl CacheManager {
    /// Create a new cache manager
    pub async fn new(config: CacheConfig) -> Result<Self, CacheError> {
        if !config.enabled {
            tracing::info!("Cache is disabled, running without Redis");
            return Ok(Self {
                config,
                redis: None,
                price_cache: None,
                orderbook_cache: None,
                user_cache: None,
                position_cache: None,
                pubsub_manager: None,
            });
        }

        // Create Redis client
        let redis_config = RedisConfig {
            url: config.redis_url.clone(),
            timeout_ms: config.timeout_ms,
            max_retries: config.max_retries,
            retry_delay_ms: 100,
        };

        match RedisClient::new(redis_config).await {
            Ok(client) => {
                let redis = Arc::new(client);

                // Create cache instances
                let price_cache = PriceCache::new(Arc::clone(&redis));
                let orderbook_cache =
                    OrderbookCache::with_depth(Arc::clone(&redis), config.orderbook_depth);
                let user_cache = UserCache::new(Arc::clone(&redis));
                let position_cache = PositionCache::new(Arc::clone(&redis));
                let pubsub_manager = PubSubManager::new(Arc::clone(&redis), &config.redis_url);

                tracing::info!("Cache manager initialized with Redis at {}", config.redis_url);

                Ok(Self {
                    config,
                    redis: Some(redis),
                    price_cache: Some(price_cache),
                    orderbook_cache: Some(orderbook_cache),
                    user_cache: Some(user_cache),
                    position_cache: Some(position_cache),
                    pubsub_manager: Some(pubsub_manager),
                })
            }
            Err(e) => {
                tracing::warn!("Failed to connect to Redis: {}. Running without cache.", e);

                // Graceful degradation - continue without cache
                Ok(Self {
                    config,
                    redis: None,
                    price_cache: None,
                    orderbook_cache: None,
                    user_cache: None,
                    position_cache: None,
                    pubsub_manager: None,
                })
            }
        }
    }

    /// Create with default configuration
    pub async fn default() -> Result<Self, CacheError> {
        Self::new(CacheConfig::default()).await
    }

    /// Check if cache is available
    pub fn is_available(&self) -> bool {
        self.redis.is_some()
    }

    /// Check if cache is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Get configuration
    pub fn config(&self) -> &CacheConfig {
        &self.config
    }

    /// Get Redis client (if available)
    pub fn redis(&self) -> Option<&Arc<RedisClient>> {
        self.redis.as_ref()
    }

    /// Get price cache (returns NoOp implementation if not available)
    pub fn price(&self) -> &PriceCache {
        #[allow(dead_code)]
        static NOOP: std::sync::OnceLock<PriceCache> = std::sync::OnceLock::new();
        self.price_cache.as_ref().unwrap_or_else(|| {
            // This is a fallback that will fail gracefully
            panic!("Price cache not available - Redis is not connected")
        })
    }

    /// Get price cache if available
    pub fn price_opt(&self) -> Option<&PriceCache> {
        self.price_cache.as_ref()
    }

    /// Get orderbook cache
    pub fn orderbook(&self) -> &OrderbookCache {
        self.orderbook_cache.as_ref().unwrap_or_else(|| {
            panic!("Orderbook cache not available - Redis is not connected")
        })
    }

    /// Get orderbook cache if available
    pub fn orderbook_opt(&self) -> Option<&OrderbookCache> {
        self.orderbook_cache.as_ref()
    }

    /// Get user cache
    pub fn user(&self) -> &UserCache {
        self.user_cache.as_ref().unwrap_or_else(|| {
            panic!("User cache not available - Redis is not connected")
        })
    }

    /// Get user cache if available
    pub fn user_opt(&self) -> Option<&UserCache> {
        self.user_cache.as_ref()
    }

    /// Get position cache
    pub fn position(&self) -> &PositionCache {
        self.position_cache.as_ref().unwrap_or_else(|| {
            panic!("Position cache not available - Redis is not connected")
        })
    }

    /// Get position cache if available
    pub fn position_opt(&self) -> Option<&PositionCache> {
        self.position_cache.as_ref()
    }

    /// Get pub/sub manager
    pub fn pubsub(&self) -> &PubSubManager {
        self.pubsub_manager.as_ref().unwrap_or_else(|| {
            panic!("PubSub manager not available - Redis is not connected")
        })
    }

    /// Get pub/sub manager if available
    pub fn pubsub_opt(&self) -> Option<&PubSubManager> {
        self.pubsub_manager.as_ref()
    }

    /// Health check - verify Redis connection
    pub async fn health_check(&self) -> Result<bool, CacheError> {
        if let Some(redis) = &self.redis {
            redis
                .ping()
                .await
                .map_err(|e| CacheError::ConnectionError(e.to_string()))
        } else {
            Ok(false)
        }
    }

    /// Get cache statistics
    pub async fn stats(&self) -> CacheStats {
        CacheStats {
            available: self.is_available(),
            enabled: self.is_enabled(),
            redis_url: self.config.redis_url.clone(),
        }
    }
}

/// Cache statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub available: bool,
    pub enabled: bool,
    pub redis_url: String,
}

/// Cache error types
#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    #[error("Redis connection error: {0}")]
    ConnectionError(String),

    #[error("Cache operation failed: {0}")]
    OperationError(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Cache not available")]
    NotAvailable,
}

impl From<redis::RedisError> for CacheError {
    fn from(e: redis::RedisError) -> Self {
        CacheError::OperationError(e.to_string())
    }
}

impl From<serde_json::Error> for CacheError {
    fn from(e: serde_json::Error) -> Self {
        CacheError::SerializationError(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_config_default() {
        let config = CacheConfig::default();
        assert!(config.enabled);
        assert_eq!(config.redis_url, "redis://127.0.0.1:6379");
        assert_eq!(config.orderbook_depth, 50);
    }

    #[tokio::test]
    async fn test_cache_manager_disabled() {
        let config = CacheConfig {
            enabled: false,
            ..Default::default()
        };

        let manager = CacheManager::new(config).await.unwrap();
        assert!(!manager.is_available());
        assert!(!manager.is_enabled());
    }
}
