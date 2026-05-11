//! User Data Cache Module
//!
//! Handles caching of user-related data including balances, positions,
//! sessions, and nonces.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use super::keys::{ttl, CacheKey};
use super::redis_client::RedisClient;

/// Cached user balance
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedBalance {
    pub token: String,
    pub available: Decimal,
    pub frozen: Decimal,
}

/// Cached position summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedPositionSummary {
    pub id: String,
    pub symbol: String,
    pub side: String,
    pub size_in_usd: Decimal,
    pub size_in_tokens: Decimal,
    pub entry_price: Decimal,
    pub mark_price: Decimal,
    pub unrealized_pnl: Decimal,
    pub leverage: i32,
    pub liquidation_price: Decimal,
}

/// Cached user session
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedSession {
    pub address: String,
    pub token: String,
    pub expires_at: i64,
    pub created_at: i64,
}

/// User cache operations
pub struct UserCache {
    redis: Arc<RedisClient>,
}

impl UserCache {
    /// Create new user cache
    pub fn new(redis: Arc<RedisClient>) -> Self {
        Self { redis }
    }

    // ==================== Balance Operations ====================

    /// Get user balance for a specific token
    pub async fn get_balance(&self, address: &str, token: &str) -> Option<CachedBalance> {
        let key = CacheKey::user_balance(address);
        match self.redis.hget::<String>(&key, token).await {
            Ok(Some(value)) => serde_json::from_str(&value).ok(),
            Ok(None) => None,
            Err(e) => {
                tracing::warn!("Failed to get balance from cache: {}", e);
                None
            }
        }
    }

    /// Get all balances for a user
    pub async fn get_all_balances(&self, address: &str) -> HashMap<String, CachedBalance> {
        let key = CacheKey::user_balance(address);
        match self.redis.hgetall::<HashMap<String, String>>(&key).await {
            Ok(map) => {
                map.into_iter()
                    .filter_map(|(token, value)| {
                        serde_json::from_str::<CachedBalance>(&value)
                            .ok()
                            .map(|b| (token, b))
                    })
                    .collect()
            }
            Err(e) => {
                tracing::warn!("Failed to get all balances from cache: {}", e);
                HashMap::new()
            }
        }
    }

    /// Set user balance for a specific token
    pub async fn set_balance(
        &self,
        address: &str,
        balance: &CachedBalance,
    ) -> Result<(), redis::RedisError> {
        let key = CacheKey::user_balance(address);
        let value = serde_json::to_string(balance).map_err(|e| {
            redis::RedisError::from((
                redis::ErrorKind::IoError,
                "Serialization error",
                e.to_string(),
            ))
        })?;
        self.redis.hset(&key, &balance.token, value).await?;
        self.redis.expire(&key, ttl::BALANCE).await?;
        Ok(())
    }

    /// Set multiple balances for a user
    pub async fn set_balances(
        &self,
        address: &str,
        balances: &[CachedBalance],
    ) -> Result<(), redis::RedisError> {
        for balance in balances {
            self.set_balance(address, balance).await?;
        }
        Ok(())
    }

    /// Invalidate user balance cache
    pub async fn invalidate_balance(&self, address: &str) -> Result<(), redis::RedisError> {
        let key = CacheKey::user_balance(address);
        self.redis.del(&key).await?;
        Ok(())
    }

    /// Invalidate specific token balance
    pub async fn invalidate_token_balance(
        &self,
        address: &str,
        token: &str,
    ) -> Result<(), redis::RedisError> {
        let key = CacheKey::user_balance(address);
        self.redis.hdel(&key, token).await?;
        Ok(())
    }

    // ==================== Position Operations ====================

    /// Get user positions
    pub async fn get_positions(&self, address: &str) -> Vec<CachedPositionSummary> {
        let key = CacheKey::user_positions(address);
        match self.redis.get::<String>(&key).await {
            Ok(Some(value)) => serde_json::from_str(&value).unwrap_or_default(),
            Ok(None) => Vec::new(),
            Err(e) => {
                tracing::warn!("Failed to get positions from cache: {}", e);
                Vec::new()
            }
        }
    }

    /// Set user positions
    pub async fn set_positions(
        &self,
        address: &str,
        positions: &[CachedPositionSummary],
    ) -> Result<(), redis::RedisError> {
        let key = CacheKey::user_positions(address);
        let value = serde_json::to_string(positions).map_err(|e| {
            redis::RedisError::from((
                redis::ErrorKind::IoError,
                "Serialization error",
                e.to_string(),
            ))
        })?;
        self.redis.set_ex(&key, value, ttl::POSITIONS).await
    }

    /// Invalidate user positions cache
    pub async fn invalidate_positions(&self, address: &str) -> Result<(), redis::RedisError> {
        let key = CacheKey::user_positions(address);
        self.redis.del(&key).await?;
        Ok(())
    }

    // ==================== Session Operations ====================

    /// Get user session
    pub async fn get_session(&self, address: &str) -> Option<CachedSession> {
        let key = CacheKey::session(address);
        match self.redis.get::<String>(&key).await {
            Ok(Some(value)) => serde_json::from_str(&value).ok(),
            Ok(None) => None,
            Err(e) => {
                tracing::warn!("Failed to get session from cache: {}", e);
                None
            }
        }
    }

    /// Set user session
    pub async fn set_session(&self, session: &CachedSession) -> Result<(), redis::RedisError> {
        let key = CacheKey::session(&session.address);
        let value = serde_json::to_string(session).map_err(|e| {
            redis::RedisError::from((
                redis::ErrorKind::IoError,
                "Serialization error",
                e.to_string(),
            ))
        })?;
        self.redis.set_ex(&key, value, ttl::SESSION).await
    }

    /// Delete user session (logout)
    pub async fn delete_session(&self, address: &str) -> Result<(), redis::RedisError> {
        let key = CacheKey::session(address);
        self.redis.del(&key).await?;
        Ok(())
    }

    /// Check if session exists
    pub async fn has_session(&self, address: &str) -> bool {
        let key = CacheKey::session(address);
        self.redis.exists(&key).await.unwrap_or(false)
    }

    // ==================== Nonce Operations ====================

    /// Get nonce for address
    pub async fn get_nonce(&self, address: &str) -> Option<i64> {
        let key = CacheKey::nonce(address);
        match self.redis.get::<String>(&key).await {
            Ok(Some(value)) => value.parse().ok(),
            Ok(None) => None,
            Err(e) => {
                tracing::warn!("Failed to get nonce from cache: {}", e);
                None
            }
        }
    }

    /// Set nonce for address
    pub async fn set_nonce(&self, address: &str, nonce: i64) -> Result<(), redis::RedisError> {
        let key = CacheKey::nonce(address);
        self.redis.set_ex(&key, nonce.to_string(), ttl::NONCE).await
    }

    /// Delete nonce (after successful verification)
    pub async fn delete_nonce(&self, address: &str) -> Result<(), redis::RedisError> {
        let key = CacheKey::nonce(address);
        self.redis.del(&key).await?;
        Ok(())
    }

    // ==================== Rate Limiting ====================

    /// Check and increment rate limit for IP
    pub async fn check_rate_limit_ip(
        &self,
        ip: &str,
        max_requests: i64,
    ) -> Result<(bool, i64), redis::RedisError> {
        let key = CacheKey::rate_limit_ip(ip);

        // Check if key exists and get current count
        let exists = self.redis.exists(&key).await?;
        let count = self.redis.incr(&key).await?;

        if !exists {
            // Set expiry on first request
            self.redis.expire(&key, ttl::RATE_LIMIT).await?;
        }

        let allowed = count <= max_requests;
        let remaining = (max_requests - count).max(0);

        Ok((allowed, remaining))
    }

    /// Check and increment rate limit for user
    pub async fn check_rate_limit_user(
        &self,
        address: &str,
        max_requests: i64,
    ) -> Result<(bool, i64), redis::RedisError> {
        let key = CacheKey::rate_limit_user(address);

        let exists = self.redis.exists(&key).await?;
        let count = self.redis.incr(&key).await?;

        if !exists {
            self.redis.expire(&key, ttl::RATE_LIMIT).await?;
        }

        let allowed = count <= max_requests;
        let remaining = (max_requests - count).max(0);

        Ok((allowed, remaining))
    }

    // ==================== Bulk Operations ====================

    /// Invalidate all cache for a user
    pub async fn invalidate_user(&self, address: &str) -> Result<(), redis::RedisError> {
        self.invalidate_balance(address).await?;
        self.invalidate_positions(address).await?;
        // Don't invalidate session - that requires explicit logout
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cached_balance_serialization() {
        let balance = CachedBalance {
            token: "USDT".to_string(),
            available: Decimal::from(10000),
            frozen: Decimal::from(500),
        };

        let json = serde_json::to_string(&balance).unwrap();
        let parsed: CachedBalance = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.token, "USDT");
        assert_eq!(parsed.available, Decimal::from(10000));
        assert_eq!(parsed.frozen, Decimal::from(500));
    }

    #[test]
    fn test_cached_position_serialization() {
        let position = CachedPositionSummary {
            id: "pos-123".to_string(),
            symbol: "BTCUSDT".to_string(),
            side: "long".to_string(),
            size_in_usd: Decimal::from(10000),
            size_in_tokens: Decimal::new(96, 3), // 0.096
            entry_price: Decimal::from(104000),
            mark_price: Decimal::from(105000),
            unrealized_pnl: Decimal::from(960),
            leverage: 10,
            liquidation_price: Decimal::from(95000),
        };

        let json = serde_json::to_string(&position).unwrap();
        let parsed: CachedPositionSummary = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.symbol, "BTCUSDT");
        assert_eq!(parsed.leverage, 10);
    }

    #[test]
    fn test_cached_session_serialization() {
        let session = CachedSession {
            address: "0x1234abcd".to_string(),
            token: "eyJ...".to_string(),
            expires_at: 1702740721000,
            created_at: 1702654321000,
        };

        let json = serde_json::to_string(&session).unwrap();
        let parsed: CachedSession = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.address, "0x1234abcd");
    }
}
