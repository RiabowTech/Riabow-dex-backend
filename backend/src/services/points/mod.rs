#![allow(dead_code)]
//! Points System Service
//!
//! Manages user points calculation, epoch management, tier calculations,
//! and leaderboard operations for the AXBlade points rewards system.
//!
//! The service integrates with:
//! - PostgreSQL: Primary data storage
//! - Redis: Points cache and leaderboard cache
//! - TimescaleDB: Time-series points events storage
//!
//! Key features:
//! - 5 point types: Trading, PnL, Holding, Referral, Staking
//! - Tier-based multipliers (T1-T4)
//! - Epoch-based accumulation periods
//! - Real-time leaderboards with caching
//! - Admin operations with audit logging
//!

// Submodules
mod epoch;
mod tier;
mod calculation;
mod query;
mod admin;
mod earn_level;
pub mod integration;
pub mod monitoring;
pub mod season_snapshot;
pub mod claim_signer;

// Re-export Earn Level types for external use
#[allow(unused_imports)]
pub use earn_level::EarnQuotaInfo;

// Re-export integration types for external use
pub use integration::{handle_trade_event_async, handle_position_close_async};


use crate::models::points::*;
use anyhow::{Context, Result};
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use rust_decimal::Decimal;
use serde_json::json;
use sqlx::{PgPool, Row};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

// ============================================================================
// Service Configuration
// ============================================================================

/// Points service configuration
#[derive(Debug, Clone)]
pub struct PointsConfig {
    /// Enable/disable points system
    pub enabled: bool,
    /// Enable/disable individual point types
    pub trading_enabled: bool,
    pub pnl_enabled: bool,
    pub holding_enabled: bool,
    pub referral_enabled: bool,
    pub staking_enabled: bool,
    /// Cache TTL in seconds
    pub cache_ttl: u64,
    /// Leaderboard size limit
    pub leaderboard_limit: usize,
}

impl Default for PointsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            trading_enabled: true,
            pnl_enabled: true,
            holding_enabled: true,
            referral_enabled: true,
            staking_enabled: true,
            cache_ttl: 60, // 60 seconds
            leaderboard_limit: 100,
        }
    }
}

// ============================================================================
// Service Struct
// ============================================================================

/// Points system service
pub struct PointsService {
    /// Database connection pool (isolated for points system)
    pool: PgPool,
    /// Redis connection manager for caching
    redis: Option<ConnectionManager>,
    /// Service configuration
    config: Arc<RwLock<PointsConfig>>,
    /// Optional WS push channel — wired in by bootstrap so points
    /// calculators can fan-out events to subscribers of the `points`
    /// private channel (PRD §9.4). Wrapped in RwLock so it can be set
    /// after the service is already wrapped in Arc by bootstrap.
    ws_sender: Arc<RwLock<Option<tokio::sync::broadcast::Sender<crate::app::state::PointsEventPush>>>>,
}

impl PointsService {
    /// Create a new PointsService with database connection
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            redis: None,
            config: Arc::new(RwLock::new(PointsConfig::default())),
            ws_sender: Arc::new(RwLock::new(None)),
        }
    }

    /// Create a new PointsService with database and Redis
    pub fn with_redis(pool: PgPool, redis: ConnectionManager) -> Self {
        Self {
            pool,
            redis: Some(redis),
            config: Arc::new(RwLock::new(PointsConfig::default())),
            ws_sender: Arc::new(RwLock::new(None)),
        }
    }

    /// Create a new PointsService with custom configuration
    pub fn with_config(pool: PgPool, redis: Option<ConnectionManager>, config: PointsConfig) -> Self {
        Self {
            pool,
            redis,
            config: Arc::new(RwLock::new(config)),
            ws_sender: Arc::new(RwLock::new(None)),
        }
    }

    /// Inject the WS push sender (bootstrap-time wiring). Safe to call
    /// after the service is wrapped in Arc.
    pub async fn set_ws_sender(
        &self,
        sender: tokio::sync::broadcast::Sender<crate::app::state::PointsEventPush>,
    ) {
        *self.ws_sender.write().await = Some(sender);
    }

    /// Internal helper: best-effort fan-out (no error if no subscribers).
    pub(crate) async fn ws_emit(
        &self,
        user_address: &str,
        event: &str,
        point_type: Option<&str>,
        amount: Option<rust_decimal::Decimal>,
        reason: Option<String>,
    ) {
        if let Some(sender) = self.ws_sender.read().await.as_ref() {
            let _ = sender.send(crate::app::state::PointsEventPush {
                user_address: user_address.to_string(),
                event: event.to_string(),
                point_type: point_type.map(|s| s.to_string()),
                amount,
                cap_kind: None,
                reason,
                timestamp: chrono::Utc::now().timestamp_millis(),
            });
        }
    }

    /// Fan-out cap-reached event with the `cap_kind` label.
    pub(crate) async fn ws_emit_cap(&self, user_address: &str, cap_kind: &str) {
        if let Some(sender) = self.ws_sender.read().await.as_ref() {
            let _ = sender.send(crate::app::state::PointsEventPush {
                user_address: user_address.to_string(),
                event: "cap_reached".to_string(),
                point_type: None,
                amount: None,
                cap_kind: Some(cap_kind.to_string()),
                reason: None,
                timestamp: chrono::Utc::now().timestamp_millis(),
            });
        }
    }

    /// Get a reference to the database pool
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Check if points system is enabled
    pub async fn is_enabled(&self) -> bool {
        self.config.read().await.enabled
    }

    /// Check if a specific point type is enabled
    pub async fn is_point_type_enabled(&self, point_type: &PointType) -> bool {
        let config = self.config.read().await;
        if !config.enabled {
            return false;
        }
        match point_type {
            PointType::Trading => config.trading_enabled,
            PointType::Pnl => config.pnl_enabled,
            PointType::Holding => config.holding_enabled,
            PointType::Referral => config.referral_enabled,
            PointType::Staking => config.staking_enabled,
        }
    }

    /// Update service configuration
    pub async fn update_config(&self, config: PointsConfig) {
        let mut current_config = self.config.write().await;
        *current_config = config;
        info!("Points service configuration updated");
    }

    /// Get current configuration
    pub async fn get_config(&self) -> PointsConfig {
        self.config.read().await.clone()
    }

    /// Check and grant referral activation rewards (from dev branch)
    pub async fn check_referral_activation(&self, user_address: &str) -> Result<()> {
        // 1. Check referral relation
        let relation_opt = sqlx::query(
            "SELECT referrer_address, created_at, activation_reward_claimed FROM referral_relations WHERE referee_address = $1"
        )
        .bind(user_address)
        .fetch_optional(&self.pool)
        .await?;

        if let Some(row) = relation_opt {
            let claimed: bool = row.try_get("activation_reward_claimed")?;
            if claimed {
                return Ok(());
            }

            let created_at: chrono::DateTime<chrono::Utc> = row.try_get("created_at")?;
            let now = chrono::Utc::now();
            let age = now - created_at;
            if age.num_days() > 7 {
                return Ok(());
            }

            // Grant rewards
            let referrer_address: String = row.try_get("referrer_address")?;
            let reward_points = Decimal::from(2);

            // Get active epoch
            let epoch_num = match self.get_active_epoch().await? {
                Some(epoch) => epoch.epoch_number,
                None => {
                    tracing::warn!("No active epoch found, skipping referral activation reward");
                    return Ok(());
                }
            };

            // Add points to Referrer (with cap)
            self.add_referral_points_with_cap(&referrer_address, reward_points, epoch_num).await?;
            
            // Add points to Referee (with cap)
            self.add_referral_points_with_cap(user_address, reward_points, epoch_num).await?;

            // Mark as claimed
            sqlx::query("UPDATE referral_relations SET activation_reward_claimed = TRUE WHERE referee_address = $1")
                .bind(user_address)
                .execute(&self.pool)
                .await?;
        }
        Ok(())
    }

    async fn add_referral_points_with_cap(&self, user_address: &str, amount: Decimal, epoch_number: i32) -> Result<()> {
        // cap check (100 points per day)
        let daily_total: Decimal = sqlx::query_scalar(
            "SELECT COALESCE(SUM(points), 0) FROM points_events WHERE user_address = $1 AND point_type = 'referral' AND created_at >= DATE_TRUNC('day', NOW())"
        )
        .bind(user_address)
        .fetch_one(&self.pool)
        .await
        .context("Failed to fetch daily referral points total")?;

        if daily_total >= Decimal::from(100) {
            return Ok(());
        }

        // Record points event
        self.save_points_event(
            user_address,
            epoch_number,
            PointType::Referral,
            amount,
            None,
            None,
            None,
            None,
            json!({"action": "activation_reward"}),
        ).await?;

        // Update summary
        self.update_referral_points_summary(user_address, epoch_number, Decimal::ZERO, amount).await?;

        Ok(())
    }

    // ============================================================================
    // Epoch Management (Phase 1.3) - Implemented in epoch.rs
    // ============================================================================

    // ============================================================================
    // Tier Calculation (Phase 1.4) - Implemented in tier.rs
    // ============================================================================

    // ============================================================================
    // Points Calculation (Phase 2.1-2.4) - Implemented in calculation.rs
    // ============================================================================

    // ============================================================================
    // User Points Queries (Phase 2.5-2.7) - Implemented in query.rs
    // ============================================================================

    // ============================================================================
    // Admin Operations (Phase 3.1) - Implemented in admin.rs
    // ============================================================================

    // ============================================================================
    // Internal Helpers
    // ============================================================================

    /// Get or create Redis connection
    fn get_redis(&self) -> Option<&ConnectionManager> {
        self.redis.as_ref()
    }

    /// Cache key for user points
    fn cache_key_user_points(user_address: &str, epoch_number: i32) -> String {
        format!("points:user:{}:epoch:{}", user_address, epoch_number)
    }

    /// Cache key for leaderboard
    fn cache_key_leaderboard(epoch_number: i32, rank_type: &LeaderboardType) -> String {
        format!("points:leaderboard:{}:{}", epoch_number, rank_type)
    }

    /// Cache key for tier info
    fn cache_key_tier_info(user_address: &str, epoch_number: i32) -> String {
        format!("points:tier:{}:epoch:{}", user_address, epoch_number)
    }

    /// Invalidate user points cache
    async fn invalidate_user_cache(&self, user_address: &str, epoch_number: i32) -> Result<()> {
        if let Some(redis) = self.get_redis() {
            let mut conn = redis.clone();
            let key = Self::cache_key_user_points(user_address, epoch_number);
            let _: () = conn.del(&key).await.context("Failed to delete cache")?;
        }
        Ok(())
    }

    /// Invalidate leaderboard cache
    async fn invalidate_leaderboard_cache(&self, epoch_number: i32) -> Result<()> {
        if let Some(redis) = self.get_redis() {
            let mut conn = redis.clone();
            for rank_type in [
                LeaderboardType::Total,
                LeaderboardType::Trading,
                LeaderboardType::Pnl,
                LeaderboardType::Holding,
                LeaderboardType::Referral,
                LeaderboardType::Staking,
            ] {
                let key = Self::cache_key_leaderboard(epoch_number, &rank_type);
                let _: () = conn.del(&key).await.context("Failed to delete cache")?;
            }
        }
        Ok(())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_key_generation() {
        let user_key = PointsService::cache_key_user_points("0x123", 1);
        assert_eq!(user_key, "points:user:0x123:epoch:1");

        let lb_key = PointsService::cache_key_leaderboard(1, &LeaderboardType::Total);
        assert_eq!(lb_key, "points:leaderboard:1:total");

        let tier_key = PointsService::cache_key_tier_info("0x123", 1);
        assert_eq!(tier_key, "points:tier:0x123:epoch:1");
    }

    #[test]
    fn test_default_config() {
        let config = PointsConfig::default();
        assert!(config.enabled);
        assert!(config.trading_enabled);
        assert_eq!(config.cache_ttl, 60);
        assert_eq!(config.leaderboard_limit, 100);
    }
}
