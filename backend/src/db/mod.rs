//! Database Module
//!
//! Provides PostgreSQL connection pool management with optimized settings
//! for high-frequency trading workloads.

#[allow(dead_code)]
pub mod timescale;

use sqlx::postgres::{PgPool, PgPoolOptions};
use std::time::Duration;


/// Database configuration
#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    /// Database connection URL
    pub url: String,
    /// Maximum number of connections in the pool
    pub max_connections: u32,
    /// Minimum number of connections to maintain
    pub min_connections: u32,
    /// Connection acquisition timeout
    pub acquire_timeout_secs: u64,
    /// Idle connection timeout
    pub idle_timeout_secs: u64,
    /// Maximum connection lifetime
    pub max_lifetime_secs: u64,
    /// Enable statement caching
    pub statement_cache_capacity: usize,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            // Optimized for high-frequency trading workloads:
            // - Much higher max for concurrent virtual trade processing
            // - Higher minimum pool to handle baseline load without acquisition delays
            max_connections: 200,  // Increased from 50 to handle wash trading
            min_connections: 50,   // Increased from 10 for better baseline performance
            // Longer timeout for high-load scenarios
            acquire_timeout_secs: 10,  // Increased from 5
            // Keep connections warm but release idle ones
            idle_timeout_secs: 600, // 10 minutes (increased from 5)
            // Recycle connections periodically to prevent stale connections
            max_lifetime_secs: 3600, // 1 hour (increased from 30 minutes)
            // Cache more prepared statements for performance
            statement_cache_capacity: 200,  // Increased from 100
        }
    }
}

impl DatabaseConfig {
    /// Create config from environment variables
    pub fn from_env(database_url: &str) -> Self {
        Self {
            url: database_url.to_string(),
            max_connections: std::env::var("DB_MAX_CONNECTIONS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(50),
            min_connections: std::env::var("DB_MIN_CONNECTIONS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(10),
            acquire_timeout_secs: std::env::var("DB_ACQUIRE_TIMEOUT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(5),
            idle_timeout_secs: std::env::var("DB_IDLE_TIMEOUT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(300),
            max_lifetime_secs: std::env::var("DB_MAX_LIFETIME")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1800),
            statement_cache_capacity: std::env::var("DB_STATEMENT_CACHE")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(100),
        }
    }
}

/// Database connection wrapper
pub struct Database {
    pub pool: PgPool,
    config: DatabaseConfig,
}

impl Database {
    /// Connect to database with default settings
    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let config = DatabaseConfig::from_env(database_url);
        Self::connect_with_config(config).await
    }

    /// Connect to database with custom configuration
    pub async fn connect_with_config(config: DatabaseConfig) -> anyhow::Result<Self> {
        tracing::info!(
            "Connecting to database with pool config: max={}, min={}, acquire_timeout={}s",
            config.max_connections,
            config.min_connections,
            config.acquire_timeout_secs
        );

        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .min_connections(config.min_connections)
            .acquire_timeout(Duration::from_secs(config.acquire_timeout_secs))
            .idle_timeout(Duration::from_secs(config.idle_timeout_secs))
            .max_lifetime(Duration::from_secs(config.max_lifetime_secs))
            .test_before_acquire(true)
            .connect(&config.url)
            .await?;

        // Migrations are NOT run on startup. Apply them out of band with
        //   `sqlx migrate run --source backend/migrations`
        // (or an equivalent CI/pre-deploy step). Keeping DDL out of the
        // startup path eliminates AccessExclusiveLock contention on hot
        // tables like `orders` and makes restarts instant regardless of
        // schema history.

        // Log pool statistics
        tracing::info!(
            "Database pool established: size={}, idle={}",
            pool.size(),
            pool.num_idle()
        );

        Ok(Self { pool, config })
    }

    /// Get pool reference
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Get current pool statistics
    pub fn stats(&self) -> PoolStats {
        PoolStats {
            size: self.pool.size(),
            idle: self.pool.num_idle(),
            max_connections: self.config.max_connections,
            min_connections: self.config.min_connections,
        }
    }

    /// Check if database is healthy
    pub async fn health_check(&self) -> bool {
        sqlx::query("SELECT 1")
            .fetch_one(&self.pool)
            .await
            .is_ok()
    }
}

/// Pool statistics
#[derive(Debug, Clone)]
pub struct PoolStats {
    pub size: u32,
    pub idle: usize,
    pub max_connections: u32,
    pub min_connections: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_database_config_default() {
        let config = DatabaseConfig::default();
        // Updated defaults for high-frequency trading workloads
        assert_eq!(config.max_connections, 200);
        assert_eq!(config.min_connections, 50);
        assert_eq!(config.acquire_timeout_secs, 10);
    }
}
