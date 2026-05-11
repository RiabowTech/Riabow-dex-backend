//! Epoch Management
//!
//! Handles epoch lifecycle operations including creation, status updates,
//! and queries.

use crate::models::points::*;
use anyhow::{Context, Result};
use chrono::{Duration, Utc};
use tracing::info;

impl super::PointsService {
    /// Get current active epoch
    pub async fn get_active_epoch(&self) -> Result<Option<EpochInfo>> {
        let epoch = sqlx::query_as::<_, EpochInfo>(
            r#"
            SELECT id, epoch_number, start_time, end_time, duration_days,
                   status, config, created_at, updated_at
            FROM points_epochs
            WHERE status = 'active'
            ORDER BY epoch_number DESC
            LIMIT 1
            "#,
        )
        .fetch_optional(&self.pool)
        .await
        .context("Failed to fetch active epoch")?;

        Ok(epoch)
    }

    /// Get epoch by number
    pub async fn get_epoch(&self, epoch_number: i32) -> Result<Option<EpochInfo>> {
        let epoch = sqlx::query_as::<_, EpochInfo>(
            r#"
            SELECT id, epoch_number, start_time, end_time, duration_days,
                   status, config, created_at, updated_at
            FROM points_epochs
            WHERE epoch_number = $1
            "#,
        )
        .bind(epoch_number)
        .fetch_optional(&self.pool)
        .await
        .context("Failed to fetch epoch")?;

        Ok(epoch)
    }

    /// Create a new epoch
    pub async fn create_epoch(&self, request: CreateEpochRequest) -> Result<EpochInfo> {
        // Calculate end time based on duration
        let end_time = request.start_time + Duration::days(request.duration_days as i64);

        // Determine initial status based on start time
        let now = Utc::now();
        let initial_status = if request.start_time > now {
            EpochStatus::Pending
        } else if request.start_time <= now && end_time > now {
            EpochStatus::Active
        } else {
            EpochStatus::Ended
        };

        // Insert new epoch
        let epoch = sqlx::query_as::<_, EpochInfo>(
            r#"
            INSERT INTO points_epochs (
                epoch_number, start_time, end_time, duration_days, status, config
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id, epoch_number, start_time, end_time, duration_days,
                      status, config, created_at, updated_at
            "#,
        )
        .bind(request.epoch_number)
        .bind(request.start_time)
        .bind(end_time)
        .bind(request.duration_days)
        .bind(initial_status.to_string())
        .bind(&request.config)
        .fetch_one(&self.pool)
        .await
        .context("Failed to create epoch")?;

        info!(
            "Created new epoch: number={}, start={}, end={}, status={}",
            epoch.epoch_number,
            epoch.start_time,
            epoch.end_time,
            epoch.status
        );

        Ok(epoch)
    }

    /// Update epoch status
    pub async fn update_epoch_status(&self, epoch_number: i32, status: EpochStatus) -> Result<()> {
        let rows_affected = sqlx::query(
            r#"
            UPDATE points_epochs
            SET status = $1, updated_at = NOW()
            WHERE epoch_number = $2
            "#,
        )
        .bind(status.to_string())
        .bind(epoch_number)
        .execute(&self.pool)
        .await
        .context("Failed to update epoch status")?
        .rows_affected();

        if rows_affected == 0 {
            anyhow::bail!("Epoch {} not found", epoch_number);
        }

        info!(
            "Updated epoch {} status to {}",
            epoch_number, status
        );

        Ok(())
    }

    /// List all epochs with pagination
    pub async fn list_epochs(&self, limit: i64, offset: i64) -> Result<Vec<EpochInfo>> {
        let epochs = sqlx::query_as::<_, EpochInfo>(
            r#"
            SELECT id, epoch_number, start_time, end_time, duration_days,
                   status, config, created_at, updated_at
            FROM points_epochs
            ORDER BY epoch_number DESC
            LIMIT $1 OFFSET $2
            "#,
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await
        .context("Failed to list epochs")?;

        Ok(epochs)
    }

    /// Check if an epoch exists
    pub async fn epoch_exists(&self, epoch_number: i32) -> Result<bool> {
        let exists: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS(
                SELECT 1 FROM points_epochs WHERE epoch_number = $1
            )
            "#,
        )
        .bind(epoch_number)
        .fetch_one(&self.pool)
        .await
        .context("Failed to check epoch existence")?;

        Ok(exists)
    }

    /// Get the latest epoch number
    pub async fn get_latest_epoch_number(&self) -> Result<Option<i32>> {
        let latest: Option<i32> = sqlx::query_scalar(
            r#"
            SELECT epoch_number
            FROM points_epochs
            ORDER BY epoch_number DESC
            LIMIT 1
            "#,
        )
        .fetch_optional(&self.pool)
        .await
        .context("Failed to get latest epoch number")?;

        Ok(latest)
    }

    /// Update epoch configuration
    pub async fn update_epoch_config(
        &self,
        epoch_number: i32,
        config: serde_json::Value,
    ) -> Result<()> {
        let rows_affected = sqlx::query(
            r#"
            UPDATE points_epochs
            SET config = $1, updated_at = NOW()
            WHERE epoch_number = $2
            "#,
        )
        .bind(&config)
        .bind(epoch_number)
        .execute(&self.pool)
        .await
        .context("Failed to update epoch config")?
        .rows_affected();

        if rows_affected == 0 {
            anyhow::bail!("Epoch {} not found", epoch_number);
        }

        info!(
            "Updated epoch {} configuration",
            epoch_number
        );

        Ok(())
    }

    /// Get all active epochs (typically should be only one)
    pub async fn get_all_active_epochs(&self) -> Result<Vec<EpochInfo>> {
        let epochs = sqlx::query_as::<_, EpochInfo>(
            r#"
            SELECT id, epoch_number, start_time, end_time, duration_days,
                   status, config, created_at, updated_at
            FROM points_epochs
            WHERE status = 'active'
            ORDER BY epoch_number DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .context("Failed to fetch active epochs")?;

        Ok(epochs)
    }

    /// Transition epochs based on time (called by background task)
    pub async fn transition_epochs(&self) -> Result<u64> {
        let now = Utc::now();
        let mut transitions = 0u64;

        // Start pending epochs that have reached their start time
        let started = sqlx::query(
            r#"
            UPDATE points_epochs
            SET status = 'active', updated_at = NOW()
            WHERE status = 'pending' AND start_time <= $1
            "#,
        )
        .bind(now)
        .execute(&self.pool)
        .await
        .context("Failed to start pending epochs")?
        .rows_affected();

        if started > 0 {
            info!("Started {} pending epoch(s)", started);
            transitions += started;
        }

        // End active epochs that have reached their end time
        let ended = sqlx::query(
            r#"
            UPDATE points_epochs
            SET status = 'ended', updated_at = NOW()
            WHERE status = 'active' AND end_time <= $1
            "#,
        )
        .bind(now)
        .execute(&self.pool)
        .await
        .context("Failed to end active epochs")?
        .rows_affected();

        if ended > 0 {
            info!("Ended {} active epoch(s)", ended);
            transitions += ended;
        }

        Ok(transitions)
    }

    /// Manually settle an epoch (mark as settled after calculations complete)
    pub async fn settle_epoch(&self, epoch_number: i32) -> Result<()> {
        let rows_affected = sqlx::query(
            r#"
            UPDATE points_epochs
            SET status = 'settled', updated_at = NOW()
            WHERE epoch_number = $1 AND status = 'ended'
            "#,
        )
        .bind(epoch_number)
        .execute(&self.pool)
        .await
        .context("Failed to settle epoch")?
        .rows_affected();

        if rows_affected == 0 {
            anyhow::bail!(
                "Epoch {} not found or not in 'ended' status",
                epoch_number
            );
        }

        info!("Settled epoch {}", epoch_number);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_epoch_time_calculation() {
        let start_time = Utc::now();
        let duration_days = 30;
        let end_time = start_time + Duration::days(duration_days as i64);

        assert!(end_time > start_time);
        let difference = end_time - start_time;
        assert_eq!(difference.num_days(), 30);
    }
}
