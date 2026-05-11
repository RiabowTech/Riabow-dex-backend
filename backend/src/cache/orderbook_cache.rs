//! Orderbook Cache Module
//!
//! Handles caching of orderbook data using Redis Sorted Sets for efficient
//! price-level queries and updates.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use super::keys::CacheKey;
use super::redis_client::RedisClient;

/// A single price level in the orderbook
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceLevel {
    pub price: Decimal,
    pub amount: Decimal,
}

/// Orderbook snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedOrderbook {
    pub symbol: String,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub timestamp: i64,
}

/// Orderbook cache operations
pub struct OrderbookCache {
    redis: Arc<RedisClient>,
    default_depth: usize,
}

impl OrderbookCache {
    /// Create new orderbook cache
    pub fn new(redis: Arc<RedisClient>) -> Self {
        Self {
            redis,
            default_depth: 50,
        }
    }

    /// Create with custom default depth
    pub fn with_depth(redis: Arc<RedisClient>, default_depth: usize) -> Self {
        Self { redis, default_depth }
    }

    // ==================== Bid Operations ====================

    /// Add or update a bid level
    pub async fn set_bid(
        &self,
        symbol: &str,
        price: Decimal,
        amount: Decimal,
    ) -> Result<(), redis::RedisError> {
        let key = CacheKey::orderbook_bids(symbol);

        if amount.is_zero() {
            // Remove level if amount is zero
            self.redis.zrem(&key, price.to_string()).await?;
        } else {
            // Use negative score so highest price (best bid) comes first in ZRANGE
            let level = PriceLevel { price, amount };
            let member = serde_json::to_string(&level).map_err(|e| {
                redis::RedisError::from((
                    redis::ErrorKind::IoError,
                    "Serialization error",
                    e.to_string(),
                ))
            })?;
            // Score is negative price for descending order
            let score = -price.to_string().parse::<f64>().unwrap_or(0.0);
            self.redis.zadd(&key, score, member).await?;
        }

        Ok(())
    }

    /// Get top N bid levels
    pub async fn get_bids(&self, symbol: &str, depth: Option<usize>) -> Vec<PriceLevel> {
        let key = CacheKey::orderbook_bids(symbol);
        let limit = depth.unwrap_or(self.default_depth);

        match self.redis.zrange::<String>(&key, 0, (limit - 1) as isize).await {
            Ok(members) => {
                members
                    .iter()
                    .filter_map(|m| serde_json::from_str(m).ok())
                    .collect()
            }
            Err(e) => {
                tracing::warn!("Failed to get bids from cache: {}", e);
                Vec::new()
            }
        }
    }

    /// Remove a bid level
    pub async fn remove_bid(&self, symbol: &str, price: Decimal) -> Result<(), redis::RedisError> {
        let key = CacheKey::orderbook_bids(symbol);
        // Need to find and remove the member with this price
        // Since we store JSON, we need to reconstruct the pattern
        let score = -price.to_string().parse::<f64>().unwrap_or(0.0);
        self.redis.zremrangebyscore(&key, score, score).await?;
        Ok(())
    }

    // ==================== Ask Operations ====================

    /// Add or update an ask level
    pub async fn set_ask(
        &self,
        symbol: &str,
        price: Decimal,
        amount: Decimal,
    ) -> Result<(), redis::RedisError> {
        let key = CacheKey::orderbook_asks(symbol);

        if amount.is_zero() {
            // Remove level if amount is zero
            self.redis.zrem(&key, price.to_string()).await?;
        } else {
            // Use positive score so lowest price (best ask) comes first in ZRANGE
            let level = PriceLevel { price, amount };
            let member = serde_json::to_string(&level).map_err(|e| {
                redis::RedisError::from((
                    redis::ErrorKind::IoError,
                    "Serialization error",
                    e.to_string(),
                ))
            })?;
            let score = price.to_string().parse::<f64>().unwrap_or(0.0);
            self.redis.zadd(&key, score, member).await?;
        }

        Ok(())
    }

    /// Get top N ask levels
    pub async fn get_asks(&self, symbol: &str, depth: Option<usize>) -> Vec<PriceLevel> {
        let key = CacheKey::orderbook_asks(symbol);
        let limit = depth.unwrap_or(self.default_depth);

        match self.redis.zrange::<String>(&key, 0, (limit - 1) as isize).await {
            Ok(members) => {
                members
                    .iter()
                    .filter_map(|m| serde_json::from_str(m).ok())
                    .collect()
            }
            Err(e) => {
                tracing::warn!("Failed to get asks from cache: {}", e);
                Vec::new()
            }
        }
    }

    /// Remove an ask level
    pub async fn remove_ask(&self, symbol: &str, price: Decimal) -> Result<(), redis::RedisError> {
        let key = CacheKey::orderbook_asks(symbol);
        let score = price.to_string().parse::<f64>().unwrap_or(0.0);
        self.redis.zremrangebyscore(&key, score, score).await?;
        Ok(())
    }

    // ==================== Full Orderbook Operations ====================

    /// Get full orderbook snapshot
    pub async fn get_orderbook(&self, symbol: &str, depth: Option<usize>) -> CachedOrderbook {
        let bids = self.get_bids(symbol, depth).await;
        let asks = self.get_asks(symbol, depth).await;

        CachedOrderbook {
            symbol: symbol.to_uppercase(),
            bids,
            asks,
            timestamp: chrono::Utc::now().timestamp_millis(),
        }
    }

    /// Set full orderbook (replaces existing data)
    pub async fn set_orderbook(
        &self,
        symbol: &str,
        bids: &[PriceLevel],
        asks: &[PriceLevel],
    ) -> Result<(), redis::RedisError> {
        // Clear existing data
        self.clear_orderbook(symbol).await?;

        // Set bids
        for level in bids {
            self.set_bid(symbol, level.price, level.amount).await?;
        }

        // Set asks
        for level in asks {
            self.set_ask(symbol, level.price, level.amount).await?;
        }

        Ok(())
    }

    /// Clear orderbook for a symbol
    pub async fn clear_orderbook(&self, symbol: &str) -> Result<(), redis::RedisError> {
        let bids_key = CacheKey::orderbook_bids(symbol);
        let asks_key = CacheKey::orderbook_asks(symbol);

        self.redis.del(&bids_key).await?;
        self.redis.del(&asks_key).await?;

        Ok(())
    }

    /// Get best bid price
    pub async fn get_best_bid(&self, symbol: &str) -> Option<PriceLevel> {
        let bids = self.get_bids(symbol, Some(1)).await;
        bids.into_iter().next()
    }

    /// Get best ask price
    pub async fn get_best_ask(&self, symbol: &str) -> Option<PriceLevel> {
        let asks = self.get_asks(symbol, Some(1)).await;
        asks.into_iter().next()
    }

    /// Get spread
    pub async fn get_spread(&self, symbol: &str) -> Option<Decimal> {
        let best_bid = self.get_best_bid(symbol).await?;
        let best_ask = self.get_best_ask(symbol).await?;
        Some(best_ask.price - best_bid.price)
    }

    /// Get mid price
    pub async fn get_mid_price(&self, symbol: &str) -> Option<Decimal> {
        let best_bid = self.get_best_bid(symbol).await?;
        let best_ask = self.get_best_ask(symbol).await?;
        Some((best_bid.price + best_ask.price) / Decimal::from(2))
    }

    // ==================== Snapshot Operations ====================

    /// Store orderbook snapshot as JSON (alternative to sorted sets)
    pub async fn set_snapshot(
        &self,
        symbol: &str,
        orderbook: &CachedOrderbook,
    ) -> Result<(), redis::RedisError> {
        let key = CacheKey::orderbook_snapshot(symbol);
        let value = serde_json::to_string(orderbook).map_err(|e| {
            redis::RedisError::from((
                redis::ErrorKind::IoError,
                "Serialization error",
                e.to_string(),
            ))
        })?;
        // Snapshot has short TTL as it's for quick reads
        self.redis.set_ex(&key, value, 5).await
    }

    /// Get orderbook snapshot from JSON
    pub async fn get_snapshot(&self, symbol: &str) -> Option<CachedOrderbook> {
        let key = CacheKey::orderbook_snapshot(symbol);
        match self.redis.get::<String>(&key).await {
            Ok(Some(value)) => serde_json::from_str(&value).ok(),
            Ok(None) => None,
            Err(e) => {
                tracing::warn!("Failed to get orderbook snapshot from cache: {}", e);
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_price_level_serialization() {
        let level = PriceLevel {
            price: Decimal::from(104000),
            amount: Decimal::new(15, 1), // 1.5
        };

        let json = serde_json::to_string(&level).unwrap();
        let parsed: PriceLevel = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.price, Decimal::from(104000));
        assert_eq!(parsed.amount, Decimal::new(15, 1));
    }

    #[test]
    fn test_cached_orderbook_serialization() {
        let orderbook = CachedOrderbook {
            symbol: "BTCUSDT".to_string(),
            bids: vec![
                PriceLevel { price: Decimal::from(104000), amount: Decimal::from(1) },
                PriceLevel { price: Decimal::from(103900), amount: Decimal::from(2) },
            ],
            asks: vec![
                PriceLevel { price: Decimal::from(104100), amount: Decimal::from(1) },
                PriceLevel { price: Decimal::from(104200), amount: Decimal::from(2) },
            ],
            timestamp: 1702654321000,
        };

        let json = serde_json::to_string(&orderbook).unwrap();
        let parsed: CachedOrderbook = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.symbol, "BTCUSDT");
        assert_eq!(parsed.bids.len(), 2);
        assert_eq!(parsed.asks.len(), 2);
    }
}
