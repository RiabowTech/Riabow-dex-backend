//! Price Cache Module
//!
//! Handles caching of market price data including mark price, index price,
//! last price, and ticker information.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use super::keys::{ttl, CacheKey};
use super::redis_client::RedisClient;

/// Cached price data for a symbol
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedPriceData {
    pub symbol: String,
    pub mark_price: Decimal,
    pub index_price: Decimal,
    pub last_price: Decimal,
    pub updated_at: i64,
}

/// Cached ticker data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedTickerData {
    pub symbol: String,
    pub last_price: Decimal,
    pub mark_price: Decimal,
    pub index_price: Decimal,
    pub open_24h: Decimal,
    pub high_24h: Decimal,
    pub low_24h: Decimal,
    pub volume_24h: Decimal,
    pub quote_volume_24h: Decimal,
    pub price_change_24h: Decimal,
    pub price_change_percent_24h: Decimal,
    pub funding_rate: Decimal,
    pub next_funding_time: i64,
    pub updated_at: i64,
}

/// Price cache operations
pub struct PriceCache {
    redis: Arc<RedisClient>,
}

impl PriceCache {
    /// Create new price cache
    pub fn new(redis: Arc<RedisClient>) -> Self {
        Self { redis }
    }

    // ==================== Mark Price ====================

    /// Get mark price for a symbol
    pub async fn get_mark_price(&self, symbol: &str) -> Option<Decimal> {
        let key = CacheKey::mark_price(symbol);
        match self.redis.get::<String>(&key).await {
            Ok(Some(value)) => value.parse().ok(),
            Ok(None) => None,
            Err(e) => {
                tracing::warn!("Failed to get mark price from cache: {}", e);
                None
            }
        }
    }

    /// Set mark price for a symbol
    pub async fn set_mark_price(&self, symbol: &str, price: Decimal) -> Result<(), redis::RedisError> {
        let key = CacheKey::mark_price(symbol);
        self.redis.set_ex(&key, price.to_string(), ttl::PRICE).await
    }

    // ==================== Index Price ====================

    /// Get index price for a symbol
    pub async fn get_index_price(&self, symbol: &str) -> Option<Decimal> {
        let key = CacheKey::index_price(symbol);
        match self.redis.get::<String>(&key).await {
            Ok(Some(value)) => value.parse().ok(),
            Ok(None) => None,
            Err(e) => {
                tracing::warn!("Failed to get index price from cache: {}", e);
                None
            }
        }
    }

    /// Set index price for a symbol
    pub async fn set_index_price(&self, symbol: &str, price: Decimal) -> Result<(), redis::RedisError> {
        let key = CacheKey::index_price(symbol);
        self.redis.set_ex(&key, price.to_string(), ttl::PRICE).await
    }

    // ==================== Last Price ====================

    /// Get last traded price for a symbol
    pub async fn get_last_price(&self, symbol: &str) -> Option<Decimal> {
        let key = CacheKey::last_price(symbol);
        match self.redis.get::<String>(&key).await {
            Ok(Some(value)) => value.parse().ok(),
            Ok(None) => None,
            Err(e) => {
                tracing::warn!("Failed to get last price from cache: {}", e);
                None
            }
        }
    }

    /// Set last traded price for a symbol
    pub async fn set_last_price(&self, symbol: &str, price: Decimal) -> Result<(), redis::RedisError> {
        let key = CacheKey::last_price(symbol);
        self.redis.set_ex(&key, price.to_string(), ttl::PRICE).await
    }

    // ==================== All Prices ====================

    /// Get all price data for a symbol
    pub async fn get_price_data(&self, symbol: &str) -> Option<CachedPriceData> {
        let mark = self.get_mark_price(symbol).await;
        let index = self.get_index_price(symbol).await;
        let last = self.get_last_price(symbol).await;

        // Return None if no prices are cached
        if mark.is_none() && index.is_none() && last.is_none() {
            return None;
        }

        Some(CachedPriceData {
            symbol: symbol.to_uppercase(),
            mark_price: mark.unwrap_or(Decimal::ZERO),
            index_price: index.unwrap_or(Decimal::ZERO),
            last_price: last.unwrap_or(Decimal::ZERO),
            updated_at: chrono::Utc::now().timestamp_millis(),
        })
    }

    /// Set all price data for a symbol
    pub async fn set_price_data(
        &self,
        symbol: &str,
        mark_price: Decimal,
        index_price: Decimal,
        last_price: Decimal,
    ) -> Result<(), redis::RedisError> {
        // Set all prices (failures are logged but don't stop other sets)
        let mut errors = Vec::new();

        if let Err(e) = self.set_mark_price(symbol, mark_price).await {
            errors.push(format!("mark_price: {}", e));
        }
        if let Err(e) = self.set_index_price(symbol, index_price).await {
            errors.push(format!("index_price: {}", e));
        }
        if let Err(e) = self.set_last_price(symbol, last_price).await {
            errors.push(format!("last_price: {}", e));
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(redis::RedisError::from((
                redis::ErrorKind::IoError,
                "Partial failure setting price data",
                errors.join(", "),
            )))
        }
    }

    // ==================== Ticker ====================

    /// Get ticker data for a symbol
    pub async fn get_ticker(&self, symbol: &str) -> Option<CachedTickerData> {
        let key = CacheKey::ticker(symbol);
        match self.redis.get::<String>(&key).await {
            Ok(Some(value)) => serde_json::from_str(&value).ok(),
            Ok(None) => None,
            Err(e) => {
                tracing::warn!("Failed to get ticker from cache: {}", e);
                None
            }
        }
    }

    /// Set ticker data for a symbol
    pub async fn set_ticker(&self, symbol: &str, ticker: &CachedTickerData) -> Result<(), redis::RedisError> {
        let key = CacheKey::ticker(symbol);
        let value = serde_json::to_string(ticker).map_err(|e| {
            redis::RedisError::from((
                redis::ErrorKind::IoError,
                "Serialization error",
                e.to_string(),
            ))
        })?;
        self.redis.set_ex(&key, value, ttl::TICKER).await
    }

    // ==================== Funding Rate ====================

    /// Get funding rate for a symbol
    pub async fn get_funding_rate(&self, symbol: &str) -> Option<Decimal> {
        let key = CacheKey::funding_rate(symbol);
        match self.redis.get::<String>(&key).await {
            Ok(Some(value)) => value.parse().ok(),
            Ok(None) => None,
            Err(e) => {
                tracing::warn!("Failed to get funding rate from cache: {}", e);
                None
            }
        }
    }

    /// Set funding rate for a symbol
    pub async fn set_funding_rate(&self, symbol: &str, rate: Decimal) -> Result<(), redis::RedisError> {
        let key = CacheKey::funding_rate(symbol);
        self.redis.set_ex(&key, rate.to_string(), ttl::FUNDING).await
    }

    // ==================== Bulk Operations ====================

    /// Set prices for multiple symbols
    pub async fn set_prices_bulk(
        &self,
        prices: &[(String, Decimal, Decimal, Decimal)], // (symbol, mark, index, last)
    ) -> Result<usize, redis::RedisError> {
        let mut success_count = 0;

        for (symbol, mark, index, last) in prices {
            if self.set_price_data(symbol, *mark, *index, *last).await.is_ok() {
                success_count += 1;
            }
        }

        Ok(success_count)
    }

    /// Invalidate all price caches for a symbol
    pub async fn invalidate_symbol(&self, symbol: &str) -> Result<(), redis::RedisError> {
        let keys = [
            CacheKey::mark_price(symbol),
            CacheKey::index_price(symbol),
            CacheKey::last_price(symbol),
            CacheKey::ticker(symbol),
            CacheKey::funding_rate(symbol),
        ];

        for key in &keys {
            if let Err(e) = self.redis.del(key).await {
                tracing::warn!("Failed to delete cache key {}: {}", key, e);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cached_price_data_serialization() {
        let data = CachedPriceData {
            symbol: "BTCUSDT".to_string(),
            mark_price: Decimal::from(104000),
            index_price: Decimal::from(103990),
            last_price: Decimal::from(104010),
            updated_at: 1702654321000,
        };

        let json = serde_json::to_string(&data).unwrap();
        let parsed: CachedPriceData = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.symbol, "BTCUSDT");
        assert_eq!(parsed.mark_price, Decimal::from(104000));
    }

    #[test]
    fn test_cached_ticker_data_serialization() {
        let ticker = CachedTickerData {
            symbol: "BTCUSDT".to_string(),
            last_price: Decimal::from(104000),
            mark_price: Decimal::from(104000),
            index_price: Decimal::from(103990),
            open_24h: Decimal::from(102000),
            high_24h: Decimal::from(105000),
            low_24h: Decimal::from(101000),
            volume_24h: Decimal::from(50000),
            quote_volume_24h: Decimal::from(5200000000u64),
            price_change_24h: Decimal::from(2000),
            price_change_percent_24h: Decimal::new(196, 2), // 1.96%
            funding_rate: Decimal::new(1, 4), // 0.0001
            next_funding_time: 1702656000000,
            updated_at: 1702654321000,
        };

        let json = serde_json::to_string(&ticker).unwrap();
        let parsed: CachedTickerData = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.symbol, "BTCUSDT");
        assert_eq!(parsed.high_24h, Decimal::from(105000));
    }
}
