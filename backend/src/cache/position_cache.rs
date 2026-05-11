//! Position Cache Module
//!
//! Provides Redis-based caching for positions to improve high-frequency query performance.
//! Positions are cached in Redis with automatic sync to PostgreSQL.

use std::sync::Arc;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::keys::{ttl, CacheKey};
use super::redis_client::RedisClient;
use crate::models::{Position, PositionSide, PositionStatus};

/// Cached position data (serializable subset of Position)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedPosition {
    pub id: Uuid,
    pub user_address: String,
    pub symbol: String,
    pub side: String,
    pub size_in_usd: String,
    pub size_in_tokens: String,
    pub collateral_amount: String,
    pub entry_price: String,
    pub leverage: i32,
    pub liquidation_price: String,
    pub accumulated_funding_fee: String,
    pub accumulated_borrowing_fee: String,
    #[serde(default)]
    pub accumulated_trading_fee: String,
    pub realized_pnl: String,
    pub status: String,
    pub created_at: i64,
    pub updated_at: i64,
}

impl From<&Position> for CachedPosition {
    fn from(p: &Position) -> Self {
        Self {
            id: p.id,
            user_address: p.user_address.clone(),
            symbol: p.symbol.clone(),
            side: format!("{:?}", p.side).to_lowercase(),
            size_in_usd: p.size_in_usd.to_string(),
            size_in_tokens: p.size_in_tokens.to_string(),
            collateral_amount: p.collateral_amount.to_string(),
            entry_price: p.entry_price.to_string(),
            leverage: p.leverage,
            liquidation_price: p.liquidation_price.to_string(),
            accumulated_funding_fee: p.accumulated_funding_fee.to_string(),
            accumulated_borrowing_fee: p.accumulated_borrowing_fee.to_string(),
            accumulated_trading_fee: p.accumulated_trading_fee.to_string(),
            realized_pnl: p.realized_pnl.to_string(),
            status: format!("{:?}", p.status).to_lowercase(),
            created_at: p.created_at.timestamp_millis(),
            updated_at: p.updated_at.timestamp_millis(),
        }
    }
}

impl CachedPosition {
    /// Convert back to Position model
    pub fn to_position(&self) -> Option<Position> {
        use chrono::{TimeZone, Utc};

        let side = match self.side.as_str() {
            "long" => PositionSide::Long,
            "short" => PositionSide::Short,
            _ => return None,
        };

        let status = match self.status.as_str() {
            "open" => PositionStatus::Open,
            "closed" => PositionStatus::Closed,
            "liquidated" => PositionStatus::Liquidated,
            _ => return None,
        };

        Some(Position {
            id: self.id,
            user_address: self.user_address.clone(),
            symbol: self.symbol.clone(),
            side,
            size_in_usd: self.size_in_usd.parse().ok()?,
            size_in_tokens: self.size_in_tokens.parse().ok()?,
            collateral_amount: self.collateral_amount.parse().ok()?,
            entry_price: self.entry_price.parse().ok()?,
            leverage: self.leverage,
            liquidation_price: self.liquidation_price.parse().ok()?,
            borrowing_factor: Decimal::ZERO,
            funding_fee_amount_per_size: Decimal::ZERO,
            accumulated_funding_fee: self.accumulated_funding_fee.parse().ok()?,
            accumulated_borrowing_fee: self.accumulated_borrowing_fee.parse().ok()?,
            // Cached entries serialized before this column existed parse to "" → ZERO.
            accumulated_trading_fee: self.accumulated_trading_fee.parse().unwrap_or(Decimal::ZERO),
            unrealized_pnl: Decimal::ZERO,
            realized_pnl: self.realized_pnl.parse().ok()?,
            status,
            created_at: Utc.timestamp_millis_opt(self.created_at).single()?,
            updated_at: Utc.timestamp_millis_opt(self.updated_at).single()?,
            increased_at: None,
            decreased_at: None,
        })
    }
}

/// Position Cache service
pub struct PositionCache {
    redis: Arc<RedisClient>,
}

impl PositionCache {
    /// Create a new position cache
    pub fn new(redis: Arc<RedisClient>) -> Self {
        Self { redis }
    }

    /// Cache a position
    pub async fn set_position(&self, position: &Position) -> Result<(), redis::RedisError> {
        let cached = CachedPosition::from(position);
        let json = serde_json::to_string(&cached)
            .map_err(|e| redis::RedisError::from((redis::ErrorKind::IoError, "Serialization error", e.to_string())))?;

        // Cache by position ID
        let key = CacheKey::position(&position.id.to_string());
        self.redis.set_ex(&key, &json, ttl::POSITIONS).await?;

        // Cache by user/symbol/side key for quick lookup
        let side_str = match position.side {
            PositionSide::Long => "long",
            PositionSide::Short => "short",
        };
        let key = CacheKey::position_by_key(&position.user_address, &position.symbol, side_str);
        self.redis.set_ex(&key, &json, ttl::POSITIONS).await?;

        // Also store position ID in user's position set
        let user_positions_key = CacheKey::user_positions(&position.user_address);
        self.redis.hset(&user_positions_key, &position.id.to_string(), &json).await?;
        self.redis.expire(&user_positions_key, ttl::POSITIONS).await?;

        tracing::debug!("Cached position {} for user {}", position.id, position.user_address);
        Ok(())
    }

    /// Get position by ID from cache
    pub async fn get_position(&self, position_id: Uuid) -> Result<Option<Position>, redis::RedisError> {
        let key = CacheKey::position(&position_id.to_string());
        let json: Option<String> = self.redis.get(&key).await?;

        if let Some(json) = json {
            if let Ok(cached) = serde_json::from_str::<CachedPosition>(&json) {
                return Ok(cached.to_position());
            }
        }
        Ok(None)
    }

    /// Get position by user, symbol, and side from cache
    pub async fn get_position_by_key(
        &self,
        user_address: &str,
        symbol: &str,
        side: PositionSide,
    ) -> Result<Option<Position>, redis::RedisError> {
        let side_str = match side {
            PositionSide::Long => "long",
            PositionSide::Short => "short",
        };
        let key = CacheKey::position_by_key(user_address, symbol, side_str);
        let json: Option<String> = self.redis.get(&key).await?;

        if let Some(json) = json {
            if let Ok(cached) = serde_json::from_str::<CachedPosition>(&json) {
                if let Some(position) = cached.to_position() {
                    // Only return open positions
                    if position.status == PositionStatus::Open {
                        return Ok(Some(position));
                    }
                }
            }
        }
        Ok(None)
    }

    /// Get all open positions for a user from cache
    pub async fn get_user_positions(&self, user_address: &str) -> Result<Vec<Position>, redis::RedisError> {
        let key = CacheKey::user_positions(user_address);
        let all_positions: std::collections::HashMap<String, String> = self.redis.hgetall(&key).await?;

        let mut positions = Vec::new();
        for (_id, json) in all_positions {
            if let Ok(cached) = serde_json::from_str::<CachedPosition>(&json) {
                if let Some(position) = cached.to_position() {
                    if position.status == PositionStatus::Open {
                        positions.push(position);
                    }
                }
            }
        }

        Ok(positions)
    }

    /// Remove position from cache
    pub async fn remove_position(&self, position: &Position) -> Result<(), redis::RedisError> {
        // Remove by ID
        let key = CacheKey::position(&position.id.to_string());
        self.redis.del(&key).await?;

        // Remove by key
        let side_str = match position.side {
            PositionSide::Long => "long",
            PositionSide::Short => "short",
        };
        let key = CacheKey::position_by_key(&position.user_address, &position.symbol, side_str);
        self.redis.del(&key).await?;

        // Remove from user's position hash
        let user_positions_key = CacheKey::user_positions(&position.user_address);
        self.redis.hdel(&user_positions_key, &position.id.to_string()).await?;

        tracing::debug!("Removed position {} from cache", position.id);
        Ok(())
    }

    /// Remove position from cache by ID only
    /// Note: This only removes the position by ID key, other keys will expire naturally
    pub async fn remove_position_by_id(&self, position_id: Uuid) -> Result<(), redis::RedisError> {
        let key = CacheKey::position(&position_id.to_string());
        self.redis.del(&key).await?;
        tracing::debug!("Removed position {} from cache by ID", position_id);
        Ok(())
    }

    /// Invalidate all positions for a user
    pub async fn invalidate_user_positions(&self, user_address: &str) -> Result<(), redis::RedisError> {
        let key = CacheKey::user_positions(user_address);
        self.redis.del(&key).await?;
        tracing::debug!("Invalidated all positions cache for user {}", user_address);
        Ok(())
    }

    /// Check if Redis is available
    pub async fn is_available(&self) -> bool {
        self.redis.is_available().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cached_position_serialization() {
        use chrono::Utc;
        use rust_decimal_macros::dec;

        let position = Position {
            id: Uuid::new_v4(),
            user_address: "0x123".to_string(),
            symbol: "BTCUSDT".to_string(),
            side: PositionSide::Long,
            size_in_usd: dec!(1000),
            size_in_tokens: dec!(0.01),
            collateral_amount: dec!(100),
            entry_price: dec!(100000),
            leverage: 10,
            liquidation_price: dec!(90500),
            borrowing_factor: Decimal::ZERO,
            funding_fee_amount_per_size: Decimal::ZERO,
            accumulated_funding_fee: Decimal::ZERO,
            accumulated_borrowing_fee: Decimal::ZERO,
            accumulated_trading_fee: Decimal::ZERO,
            unrealized_pnl: Decimal::ZERO,
            realized_pnl: Decimal::ZERO,
            status: PositionStatus::Open,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            increased_at: None,
            decreased_at: None,
        };

        let cached = CachedPosition::from(&position);
        let json = serde_json::to_string(&cached).unwrap();
        let restored: CachedPosition = serde_json::from_str(&json).unwrap();
        let restored_position = restored.to_position().unwrap();

        assert_eq!(position.id, restored_position.id);
        assert_eq!(position.symbol, restored_position.symbol);
        assert_eq!(position.side, restored_position.side);
    }
}
