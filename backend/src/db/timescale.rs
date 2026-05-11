//! TimescaleDB Operations Module
//!
//! Provides efficient access to time-series data including K-line queries,
//! continuous aggregate management, and compression operations.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

/// K-line (Candlestick) data structure
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct Kline {
    pub symbol: String,
    #[sqlx(rename = "bucket")]
    pub open_time: DateTime<Utc>,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
    pub quote_volume: Decimal,
    pub trade_count: i64,
}

/// K-line period/interval
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KlinePeriod {
    OneMinute,
    FiveMinutes,
    FifteenMinutes,
    OneHour,
    FourHours,
    OneDay,
    OneWeek,
}

impl KlinePeriod {
    /// Get the table/view name for this period
    pub fn table_name(&self) -> &'static str {
        match self {
            KlinePeriod::OneMinute => "klines_1m",
            KlinePeriod::FiveMinutes => "klines_5m",
            KlinePeriod::FifteenMinutes => "klines_15m",
            KlinePeriod::OneHour => "klines_1h",
            KlinePeriod::FourHours => "klines_4h",
            KlinePeriod::OneDay => "klines_1d",
            KlinePeriod::OneWeek => "klines_1w",
        }
    }

    /// Get the interval duration in seconds
    pub fn interval_seconds(&self) -> i64 {
        match self {
            KlinePeriod::OneMinute => 60,
            KlinePeriod::FiveMinutes => 300,
            KlinePeriod::FifteenMinutes => 900,
            KlinePeriod::OneHour => 3600,
            KlinePeriod::FourHours => 14400,
            KlinePeriod::OneDay => 86400,
            KlinePeriod::OneWeek => 604800,
        }
    }

    /// Parse period from string (e.g., "1m", "5m", "1h", "1d")
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "1m" | "1min" => Some(KlinePeriod::OneMinute),
            "5m" | "5min" => Some(KlinePeriod::FiveMinutes),
            "15m" | "15min" => Some(KlinePeriod::FifteenMinutes),
            "1h" | "60m" => Some(KlinePeriod::OneHour),
            "4h" | "240m" => Some(KlinePeriod::FourHours),
            "1d" | "1day" => Some(KlinePeriod::OneDay),
            "1w" | "1week" => Some(KlinePeriod::OneWeek),
            _ => None,
        }
    }

    /// Convert to string representation
    pub fn to_str(&self) -> &'static str {
        match self {
            KlinePeriod::OneMinute => "1m",
            KlinePeriod::FiveMinutes => "5m",
            KlinePeriod::FifteenMinutes => "15m",
            KlinePeriod::OneHour => "1h",
            KlinePeriod::FourHours => "4h",
            KlinePeriod::OneDay => "1d",
            KlinePeriod::OneWeek => "1w",
        }
    }
}

/// TimescaleDB operations
pub struct TimescaleOps {
    pool: PgPool,
}

impl TimescaleOps {
    /// Create new TimescaleDB operations instance
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Check if TimescaleDB extension is installed
    pub async fn is_timescaledb_installed(&self) -> Result<bool, sqlx::Error> {
        let result: Option<(String,)> = sqlx::query_as(
            "SELECT extname FROM pg_extension WHERE extname = 'timescaledb'"
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(result.is_some())
    }

    /// Get K-lines for a symbol and period
    pub async fn get_klines(
        &self,
        symbol: &str,
        period: KlinePeriod,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        limit: i32,
    ) -> Result<Vec<Kline>, sqlx::Error> {
        let table = period.table_name();

        // Use dynamic SQL since table name varies
        let query = format!(
            r#"
            SELECT
                symbol,
                bucket,
                open,
                high,
                low,
                close,
                volume,
                quote_volume,
                trade_count
            FROM {}
            WHERE symbol = $1
              AND bucket >= $2
              AND bucket < $3
            ORDER BY bucket DESC
            LIMIT $4
            "#,
            table
        );

        sqlx::query_as::<_, Kline>(&query)
            .bind(symbol.to_uppercase())
            .bind(start_time)
            .bind(end_time)
            .bind(limit)
            .fetch_all(&self.pool)
            .await
    }

    /// Get latest K-line for a symbol and period
    pub async fn get_latest_kline(
        &self,
        symbol: &str,
        period: KlinePeriod,
    ) -> Result<Option<Kline>, sqlx::Error> {
        let table = period.table_name();

        let query = format!(
            r#"
            SELECT
                symbol,
                bucket,
                open,
                high,
                low,
                close,
                volume,
                quote_volume,
                trade_count
            FROM {}
            WHERE symbol = $1
            ORDER BY bucket DESC
            LIMIT 1
            "#,
            table
        );

        sqlx::query_as::<_, Kline>(&query)
            .bind(symbol.to_uppercase())
            .fetch_optional(&self.pool)
            .await
    }

    /// Get K-lines with a limit starting from the most recent
    pub async fn get_recent_klines(
        &self,
        symbol: &str,
        period: KlinePeriod,
        limit: i32,
    ) -> Result<Vec<Kline>, sqlx::Error> {
        let table = period.table_name();

        let query = format!(
            r#"
            SELECT
                symbol,
                bucket,
                open,
                high,
                low,
                close,
                volume,
                quote_volume,
                trade_count
            FROM {}
            WHERE symbol = $1
            ORDER BY bucket DESC
            LIMIT $2
            "#,
            table
        );

        sqlx::query_as::<_, Kline>(&query)
            .bind(symbol.to_uppercase())
            .bind(limit)
            .fetch_all(&self.pool)
            .await
    }

    /// Manually refresh a continuous aggregate for a time range
    pub async fn refresh_continuous_aggregate(
        &self,
        period: KlinePeriod,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        let view_name = period.table_name();

        let query = format!(
            "CALL refresh_continuous_aggregate('{}', $1, $2)",
            view_name
        );

        sqlx::query(&query)
            .bind(start_time)
            .bind(end_time)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    /// Get compression statistics for the trades table
    pub async fn get_compression_stats(&self) -> Result<CompressionStats, sqlx::Error> {
        let result = sqlx::query_as::<_, CompressionStats>(
            r#"
            SELECT
                COALESCE(SUM(before_compression_total_bytes), 0) as uncompressed_bytes,
                COALESCE(SUM(after_compression_total_bytes), 0) as compressed_bytes,
                COUNT(*) as chunk_count
            FROM timescaledb_information.compressed_chunk_stats
            WHERE hypertable_name = 'trades'
            "#
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(result)
    }

    /// Get chunk information for the trades hypertable
    pub async fn get_chunk_info(&self) -> Result<Vec<ChunkInfo>, sqlx::Error> {
        sqlx::query_as::<_, ChunkInfo>(
            r#"
            SELECT
                chunk_name,
                range_start,
                range_end,
                is_compressed
            FROM timescaledb_information.chunks
            WHERE hypertable_name = 'trades'
            ORDER BY range_start DESC
            LIMIT 30
            "#
        )
        .fetch_all(&self.pool)
        .await
    }

    /// Manually compress old chunks
    pub async fn compress_chunks_older_than(&self, days: i32) -> Result<i64, sqlx::Error> {
        let result = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT compress_chunk(c.chunk_schema || '.' || c.chunk_name)
            FROM timescaledb_information.chunks c
            WHERE c.hypertable_name = 'trades'
              AND c.range_end < NOW() - ($1 || ' days')::INTERVAL
              AND NOT c.is_compressed
            RETURNING 1
            "#
        )
        .bind(days.to_string())
        .fetch_all(&self.pool)
        .await?;

        Ok(result.len() as i64)
    }
}

/// Compression statistics
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct CompressionStats {
    pub uncompressed_bytes: i64,
    pub compressed_bytes: i64,
    pub chunk_count: i64,
}

impl CompressionStats {
    /// Calculate compression ratio
    pub fn compression_ratio(&self) -> f64 {
        if self.uncompressed_bytes == 0 {
            return 0.0;
        }
        1.0 - (self.compressed_bytes as f64 / self.uncompressed_bytes as f64)
    }
}

/// Chunk information
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct ChunkInfo {
    pub chunk_name: String,
    pub range_start: DateTime<Utc>,
    pub range_end: DateTime<Utc>,
    pub is_compressed: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kline_period_from_str() {
        assert_eq!(KlinePeriod::from_str("1m"), Some(KlinePeriod::OneMinute));
        assert_eq!(KlinePeriod::from_str("5m"), Some(KlinePeriod::FiveMinutes));
        assert_eq!(KlinePeriod::from_str("1h"), Some(KlinePeriod::OneHour));
        assert_eq!(KlinePeriod::from_str("1d"), Some(KlinePeriod::OneDay));
        assert_eq!(KlinePeriod::from_str("invalid"), None);
    }

    #[test]
    fn test_kline_period_table_name() {
        assert_eq!(KlinePeriod::OneMinute.table_name(), "klines_1m");
        assert_eq!(KlinePeriod::OneHour.table_name(), "klines_1h");
        assert_eq!(KlinePeriod::OneDay.table_name(), "klines_1d");
    }

    #[test]
    fn test_compression_ratio() {
        let stats = CompressionStats {
            uncompressed_bytes: 1000,
            compressed_bytes: 200,
            chunk_count: 5,
        };
        assert!((stats.compression_ratio() - 0.8).abs() < 0.001);
    }
}
