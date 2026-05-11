//! Points System Monitoring and Metrics
//!
//! Provides metrics collection and health check functionality for the points system.
//! Useful for monitoring system health, performance, and data integrity.

use anyhow::Result;
use rust_decimal::Decimal;
use serde::Serialize;
use tracing::info;

use super::PointsService;

// ============================================================================
// Monitoring Data Structures
// ============================================================================

/// System health status
#[derive(Debug, Clone, Serialize)]
pub struct HealthStatus {
    pub healthy: bool,
    pub database_connected: bool,
    pub redis_connected: bool,
    pub active_epochs: i32,
    pub total_users: i64,
    pub issues: Vec<String>,
}

/// System metrics snapshot
#[derive(Debug, Clone, Serialize)]
pub struct SystemMetrics {
    // Epoch metrics
    pub active_epochs_count: i32,
    pub scheduled_epochs_count: i32,
    pub settled_epochs_count: i32,

    // User metrics
    pub total_users_with_points: i64,
    pub users_with_tier: i64,

    // Points metrics
    pub total_points_issued: Decimal,
    pub total_trading_points: Decimal,
    pub total_pnl_points: Decimal,
    pub total_holding_points: Decimal,
    pub total_referral_points: Decimal,
    pub total_staking_points: Decimal,

    // Activity metrics
    pub total_points_events: i64,
    pub events_last_24h: i64,
    pub events_last_hour: i64,

    // Timestamp
    pub collected_at: chrono::DateTime<chrono::Utc>,
}

/// Per-epoch metrics
#[derive(Debug, Clone, Serialize)]
pub struct EpochMetrics {
    pub epoch_number: i32,
    pub status: String,
    pub participant_count: i64,
    pub total_points: Decimal,
    pub total_trading_volume: Decimal,
    pub total_trades: i64,
    pub total_pnl: Decimal,
    pub leaderboard_entries: i64,
    pub start_time: chrono::DateTime<chrono::Utc>,
    pub end_time: chrono::DateTime<chrono::Utc>,
}

// ============================================================================
// Health Check Implementation
// ============================================================================

impl PointsService {
    /// Perform health check on points system
    ///
    /// Checks database connectivity, Redis availability, and system state.
    pub async fn health_check(&self) -> Result<HealthStatus> {
        let mut issues = Vec::new();
        let mut healthy = true;

        // Check database connection
        let db_connected = match sqlx::query("SELECT 1")
            .fetch_one(&self.pool)
            .await
        {
            Ok(_) => true,
            Err(e) => {
                issues.push(format!("Database connection failed: {}", e));
                healthy = false;
                false
            }
        };

        // Check Redis connection
        let redis_connected = if let Some(redis) = self.get_redis() {
            match redis::Cmd::new()
                .arg("PING")
                .query_async::<_, String>(&mut redis.clone())
                .await
            {
                Ok(response) if response == "PONG" => true,
                Ok(_) => {
                    issues.push("Redis PING returned unexpected response".to_string());
                    false
                }
                Err(e) => {
                    issues.push(format!("Redis connection failed: {}", e));
                    false
                }
            }
        } else {
            // Redis is optional
            true
        };

        // Check for active epochs
        let active_epochs = match self.get_all_active_epochs().await {
            Ok(epochs) => epochs.len() as i32,
            Err(e) => {
                issues.push(format!("Failed to fetch active epochs: {}", e));
                healthy = false;
                0
            }
        };

        // Get total users with points
        let total_users = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(DISTINCT user_address) FROM user_points_summary",
        )
        .fetch_one(&self.pool)
        .await
        .unwrap_or(0);

        Ok(HealthStatus {
            healthy,
            database_connected: db_connected,
            redis_connected,
            active_epochs,
            total_users,
            issues,
        })
    }

    /// Collect comprehensive system metrics
    pub async fn collect_metrics(&self) -> Result<SystemMetrics> {
        // Epoch counts
        let epoch_counts: (i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT
                COUNT(CASE WHEN status = 'active' THEN 1 END) as active,
                COUNT(CASE WHEN status = 'scheduled' THEN 1 END) as scheduled,
                COUNT(CASE WHEN status = 'settled' THEN 1 END) as settled
            FROM points_epochs
            "#,
        )
        .fetch_one(&self.pool)
        .await?;

        // User counts
        let total_users: i64 = sqlx::query_scalar(
            "SELECT COUNT(DISTINCT user_address) FROM user_points_summary",
        )
        .fetch_one(&self.pool)
        .await?;

        let users_with_tier: i64 = sqlx::query_scalar(
            "SELECT COUNT(DISTINCT user_address) FROM user_points_summary WHERE tier IS NOT NULL",
        )
        .fetch_one(&self.pool)
        .await?;

        // Points totals
        let points_totals: (Decimal, Decimal, Decimal, Decimal, Decimal, Decimal) = sqlx::query_as(
            r#"
            SELECT
                COALESCE(SUM(total_points), 0) as total,
                COALESCE(SUM(trading_points), 0) as trading,
                COALESCE(SUM(pnl_points), 0) as pnl,
                COALESCE(SUM(holding_points), 0) as holding,
                COALESCE(SUM(referral_points), 0) as referral,
                COALESCE(SUM(staking_points), 0) as staking
            FROM user_points_summary
            "#,
        )
        .fetch_one(&self.pool)
        .await?;

        // Activity metrics
        let total_events: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM points_events")
            .fetch_one(&self.pool)
            .await?;

        let events_24h: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM points_events WHERE created_at > NOW() - INTERVAL '24 hours'",
        )
        .fetch_one(&self.pool)
        .await?;

        let events_1h: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM points_events WHERE created_at > NOW() - INTERVAL '1 hour'",
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(SystemMetrics {
            active_epochs_count: epoch_counts.0 as i32,
            scheduled_epochs_count: epoch_counts.1 as i32,
            settled_epochs_count: epoch_counts.2 as i32,
            total_users_with_points: total_users,
            users_with_tier,
            total_points_issued: points_totals.0,
            total_trading_points: points_totals.1,
            total_pnl_points: points_totals.2,
            total_holding_points: points_totals.3,
            total_referral_points: points_totals.4,
            total_staking_points: points_totals.5,
            total_points_events: total_events,
            events_last_24h: events_24h,
            events_last_hour: events_1h,
            collected_at: chrono::Utc::now(),
        })
    }

    /// Get metrics for a specific epoch
    pub async fn get_epoch_metrics(&self, epoch_number: i32) -> Result<EpochMetrics> {
        let epoch = self
            .get_epoch(epoch_number)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Epoch {} not found", epoch_number))?;

        // Get participant count
        let participant_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(DISTINCT user_address) FROM user_points_summary WHERE epoch_number = $1",
        )
        .bind(epoch_number)
        .fetch_one(&self.pool)
        .await?;

        // Get totals
        let totals: (Decimal, Decimal, i64, Decimal) = sqlx::query_as(
            r#"
            SELECT
                COALESCE(SUM(total_points), 0) as total_points,
                COALESCE(SUM(trading_volume), 0) as total_volume,
                COALESCE(SUM(trade_count), 0) as total_trades,
                COALESCE(SUM(realized_pnl), 0) as total_pnl
            FROM user_points_summary
            WHERE epoch_number = $1
            "#,
        )
        .bind(epoch_number)
        .fetch_one(&self.pool)
        .await?;

        // Get leaderboard entry count
        let leaderboard_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM points_leaderboard WHERE epoch_number = $1",
        )
        .bind(epoch_number)
        .fetch_one(&self.pool)
        .await?;

        Ok(EpochMetrics {
            epoch_number,
            status: epoch.status.to_string(),
            participant_count,
            total_points: totals.0,
            total_trading_volume: totals.1,
            total_trades: totals.2,
            total_pnl: totals.3,
            leaderboard_entries: leaderboard_count,
            start_time: epoch.start_time,
            end_time: epoch.end_time,
        })
    }

    /// Log system metrics to tracing
    pub async fn log_metrics(&self) -> Result<()> {
        let metrics = self.collect_metrics().await?;

        info!(
            target: "points_metrics",
            active_epochs = metrics.active_epochs_count,
            total_users = metrics.total_users_with_points,
            total_points = %metrics.total_points_issued,
            events_24h = metrics.events_last_24h,
            events_1h = metrics.events_last_hour,
            "Points system metrics snapshot"
        );

        Ok(())
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Legacy helper — the real Prometheus scrape endpoint is
/// `GET /metrics` served by `services::metrics::gather_metrics()`
/// (registered in `app::routes`). Kept as a stub only to avoid breaking
/// older test code that imports it; callers should migrate to the
/// shared metrics registry.
pub fn format_metrics_prometheus(_metrics: &SystemMetrics) -> String {
    "# Use GET /metrics for the live Prometheus scrape.\n".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_status_serialization() {
        let status = HealthStatus {
            healthy: true,
            database_connected: true,
            redis_connected: true,
            active_epochs: 1,
            total_users: 100,
            issues: vec![],
        };

        let json = serde_json::to_string(&status)
            .expect("HealthStatus is Serialize so this cannot fail");
        assert!(json.contains("\"healthy\":true"));
    }

    #[test]
    fn test_prometheus_format() {
        let metrics = SystemMetrics {
            active_epochs_count: 1,
            scheduled_epochs_count: 0,
            settled_epochs_count: 5,
            total_users_with_points: 1000,
            users_with_tier: 800,
            total_points_issued: Decimal::new(1000000, 0),
            total_trading_points: Decimal::new(500000, 0),
            total_pnl_points: Decimal::new(200000, 0),
            total_holding_points: Decimal::new(150000, 0),
            total_referral_points: Decimal::new(100000, 0),
            total_staking_points: Decimal::new(50000, 0),
            total_points_events: 10000,
            events_last_24h: 500,
            events_last_hour: 25,
            collected_at: chrono::Utc::now(),
        };

        let output = format_metrics_prometheus(&metrics);
        assert!(!output.is_empty());
    }
}
