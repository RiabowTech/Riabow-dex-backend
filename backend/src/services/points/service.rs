use crate::config::AppConfig;
use crate::services::points::models::{
    PointType, PointsEvent, PointsStaking, TradingTierConfig, UserPointsSummary,
};
use bigdecimal::{BigDecimal, FromPrimitive, ToPrimitive, Zero};
use chrono::Utc;
use dashmap::DashMap;
use sqlx::{PgPool, Postgres, Transaction};
use std::str::FromStr;
use std::sync::Arc;
use tracing::{error, info, warn};
use uuid::Uuid;

// Internal Constants (Fallback)
const DEFAULT_TRADING_POINT_RATE: f64 = 0.0001;
const DEFAULT_PNL_POINT_RATE: f64 = 0.001;
const DEFAULT_HOLDING_POINT_RATE: f64 = 0.00001;
const DEFAULT_REFERRAL_POINT_RATE: f64 = 0.00005;
const DEFAULT_STAKING_POINT_RATE: f64 = 0.0002;

#[derive(Clone)]
pub struct PointsService {
    pool: PgPool,
    config: Arc<AppConfig>,
    // Cache for tiers: Key = (TierName, EpochNumber or 0)
    tier_cache: Arc<DashMap<(String, i32), TradingTierConfig>>,
    // Cache for current epoch number
    current_epoch_cache: Arc<DashMap<String, i32>>,
}

impl PointsService {
    pub fn new(pool: PgPool, config: Arc<AppConfig>) -> Self {
        Self {
            pool,
            config,
            tier_cache: Arc::new(DashMap::new()),
            current_epoch_cache: Arc::new(DashMap::new()),
        }
    }

    /// Helper to get current active epoch
    pub async fn get_current_epoch(&self) -> Result<Option<i32>, sqlx::Error> {
        // Try cache first
        if let Some(epoch) = self.current_epoch_cache.get("current") {
            return Ok(Some(*epoch));
        }

        let now = Utc::now();
        let epoch: Option<i32> = sqlx::query_scalar!(
            r#"
            SELECT epoch_number FROM points_epochs 
            WHERE status = 'active' 
            AND start_time <= $1 
            AND end_time >= $1
            LIMIT 1
            "#,
            now
        )
        .fetch_optional(&self.pool)
        .await?;

        if let Some(e) = epoch {
            self.current_epoch_cache.insert("current".to_string(), e);
        }

        Ok(epoch)
    }

    /// Calculate Trading Points
    pub async fn calculate_trading_points(
        &self,
        user_address: &str,
        volume: BigDecimal,
        trade_id: Uuid,
    ) -> Result<(), anyhow::Error> {
        if !self.config.points_system_enabled || !self.config.points_trading_enabled {
            return Ok(());
        }

        let epoch_number = match self.get_current_epoch().await? {
            Some(e) => e,
            None => {
                warn!("No active epoch found for points calculation");
                return Ok(());
            }
        };

        // 1. Get User's current Tier Multiplier
        let (tier_name, multiplier) = self.get_user_tier_info(user_address, epoch_number).await?;

        // 2. Calculate Points: Volume * Rate * Multiplier
        let base_rate = BigDecimal::from_f64(DEFAULT_TRADING_POINT_RATE).expect("DEFAULT_TRADING_POINT_RATE is a finite literal");
        let points = &volume * &base_rate * &multiplier;

        // 3. Record Event and Update Summary in Transaction
        let mut tx = self.pool.begin().await?;

        // Insert Event
        let metadata = serde_json::json!({
            "volume": volume.to_string(),
            "tier": tier_name,
            "multiplier": multiplier.to_string()
        });

        sqlx::query!(
            r#"
            INSERT INTO points_events 
            (user_address, epoch_number, point_type, points, related_trade_id, metadata)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
            user_address,
            epoch_number,
            PointType::Trading.to_string(),
            points,
            trade_id,
            metadata
        )
        .execute(&mut *tx)
        .await?;

        // Update Summary (Upsert)
        self.update_user_summary_atomic(
            &mut tx,
            user_address,
            epoch_number,
            points,
            PointType::Trading,
            Some(volume), // Update trading volume
            None,
        )
        .await?;

        tx.commit().await?;

        info!(
            "Recorded trading points for user {}: {} points (Tier: {})",
            user_address, points, tier_name
        );
        
        // Check for Tier Upgrade asynchronously (fire and forget logic could be here, or inline)
        // For simplicity, we re-check tier on next trade effectively. 
        // Real-time upgrade logic is implicitly handled because get_user_tier_info queries current volume.

        Ok(())
    }

    /// Calculate PnL Points (Only for positive PnL)
    pub async fn calculate_pnl_points(
        &self,
        user_address: &str,
        pnl: BigDecimal,
        position_id: Uuid,
    ) -> Result<(), anyhow::Error> {
        if !self.config.points_system_enabled || !self.config.points_pnl_enabled {
            return Ok(());
        }

        // Only positive PnL awards points
        if pnl <= BigDecimal::zero() {
            return Ok(());
        }

        let epoch_number = match self.get_current_epoch().await? {
            Some(e) => e,
            None => return Ok(()),
        };

        let rate = BigDecimal::from_f64(DEFAULT_PNL_POINT_RATE).expect("DEFAULT_PNL_POINT_RATE is a finite literal");
        let points = &pnl * &rate;

        let mut tx = self.pool.begin().await?;

        let metadata = serde_json::json!({
            "realized_pnl": pnl.to_string()
        });

        sqlx::query!(
            r#"
            INSERT INTO points_events 
            (user_address, epoch_number, point_type, points, related_position_id, metadata)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
            user_address,
            epoch_number,
            PointType::Pnl.to_string(),
            points,
            position_id,
            metadata
        )
        .execute(&mut *tx)
        .await?;

        self.update_user_summary_atomic(
            &mut tx,
            user_address,
            epoch_number,
            points,
            PointType::Pnl,
            None,
            Some(pnl), // Update realized PnL
        )
        .await?;

        tx.commit().await?;

        Ok(())
    }

    /// Calculate Referral Points
    pub async fn calculate_referral_points(
        &self,
        referrer_address: &str,
        referee_volume: BigDecimal,
        referee_trade_id: Uuid,
    ) -> Result<(), anyhow::Error> {
        if !self.config.points_system_enabled || !self.config.points_referral_enabled {
            return Ok(());
        }

        let epoch_number = match self.get_current_epoch().await? {
            Some(e) => e,
            None => return Ok(()),
        };

        let rate = BigDecimal::from_f64(DEFAULT_REFERRAL_POINT_RATE).expect("DEFAULT_REFERRAL_POINT_RATE is a finite literal");
        let points = &referee_volume * &rate;

        let mut tx = self.pool.begin().await?;

        let metadata = serde_json::json!({
            "referee_volume": referee_volume.to_string(),
            "referee_trade_id": referee_trade_id.to_string()
        });

        sqlx::query!(
            r#"
            INSERT INTO points_events 
            (user_address, epoch_number, point_type, points, related_trade_id, metadata)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
            referrer_address,
            epoch_number,
            PointType::Referral.to_string(),
            points,
            referee_trade_id, // Link to referee's trade
            metadata
        )
        .execute(&mut *tx)
        .await?;

        // Update Summary: Add points and update referral volume stats
        sqlx::query!(
            r#"
            INSERT INTO user_points_summary 
            (user_address, epoch_number, referral_points, total_points, referral_volume, updated_at)
            VALUES ($1, $2, $3, $3, $4, NOW())
            ON CONFLICT (user_address, epoch_number) 
            DO UPDATE SET 
                referral_points = user_points_summary.referral_points + EXCLUDED.referral_points,
                total_points = user_points_summary.total_points + EXCLUDED.total_points,
                referral_volume = user_points_summary.referral_volume + EXCLUDED.referral_volume,
                updated_at = NOW()
            "#,
            referrer_address,
            epoch_number,
            points,
            referee_volume
        )
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(())
    }

    // Helper: Determine User Tier
    async fn get_user_tier_info(&self, user_address: &str, epoch_number: i32) -> Result<(String, BigDecimal), sqlx::Error> {
        // 1. Get current trading volume
        // Note: We read uncommitted volume if inside transaction, but here we likely want "latest committed"
        let volume: Option<BigDecimal> = sqlx::query_scalar!(
            r#"
            SELECT trading_volume FROM user_points_summary 
            WHERE user_address = $1 AND epoch_number = $2
            "#,
            user_address,
            epoch_number
        )
        .fetch_optional(&self.pool)
        .await?;
        
        let current_volume = volume.unwrap_or(BigDecimal::zero());

        // 2. Query Tiers Config
        // Ideally cached. For now, we query DB or hardcode fallback if DB empty.
        // Simplified: Query specific tier for this volume
        
        let tier_config = sqlx::query!(
            r#"
            SELECT tier_name, multiplier FROM trading_tier_config 
            WHERE is_active = true 
            AND (epoch_number = $1 OR epoch_number IS NULL)
            AND min_volume <= $2 
            AND (max_volume >= $2 OR max_volume IS NULL)
            ORDER BY epoch_number NULLS LAST, min_volume DESC
            LIMIT 1
            "#,
            epoch_number,
            current_volume
        )
        .fetch_optional(&self.pool)
        .await?;

        match tier_config {
            Some(t) => Ok((t.tier_name, t.multiplier)),
            None => Ok(("T1".to_string(), BigDecimal::from(1))), // Default T1
        }
    }

    // Helper: Atomic Update Summary
    async fn update_user_summary_atomic(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        user_address: &str,
        epoch_number: i32,
        points: BigDecimal,
        point_type: PointType,
        trading_volume_delta: Option<BigDecimal>,
        pnl_delta: Option<BigDecimal>,
    ) -> Result<(), sqlx::Error> {
        let volume_delta = trading_volume_delta.unwrap_or(BigDecimal::zero());
        let count_delta = if volume_delta > BigDecimal::zero() { 1 } else { 0 };
        let pnl_delta = pnl_delta.unwrap_or(BigDecimal::zero());

        
        // Dynamic column update based on type
        let col_name = match point_type {
            PointType::Trading => "trading_points",
            PointType::Pnl => "pnl_points",
            PointType::Holding => "holding_points",
            PointType::Referral => "referral_points",
            PointType::Staking => "staking_points",
        };

        let query = format!(
            r#"
            INSERT INTO user_points_summary 
            (user_address, epoch_number, {col}, total_points, trading_volume, trade_count, realized_pnl, updated_at)
            VALUES ($1, $2, $3, $3, $4, $5, $6, NOW())
            ON CONFLICT (user_address, epoch_number) 
            DO UPDATE SET 
                {col} = user_points_summary.{col} + EXCLUDED.{col},
                total_points = user_points_summary.total_points + EXCLUDED.total_points,
                trading_volume = user_points_summary.trading_volume + EXCLUDED.trading_volume,
                trade_count = user_points_summary.trade_count + EXCLUDED.trade_count,
                realized_pnl = user_points_summary.realized_pnl + EXCLUDED.realized_pnl,
                updated_at = NOW()
            "#,
            col = col_name
        );

        sqlx::query(&query)
            .bind(user_address)
            .bind(epoch_number)
            .bind(points) // maps to {col} and total_points initial value
            .bind(volume_delta)
            .bind(count_delta)
            .bind(pnl_delta)
            .execute(&mut **tx)
            .await?;
        
        // Also update the Tier Name stored in summary based on new volume (if changed)
        // This is a minimal optimization to avoid querying tier config on every read
        // Proper way requires re-evaluating tier after update. 
        // We'll skip complex "next tier" logic here for performance, assuming reads will join or calc on fly.
        
        Ok(())
    }
    
    // API Read Methods
    
    pub async fn get_user_summary(&self, user_address: &str, epoch_number: Option<i32>) -> Result<Option<UserPointsSummary>, anyhow::Error> {
         let epoch = match epoch_number {
            Some(e) => e,
            None => match self.get_current_epoch().await? {
                Some(e) => e,
                None => return Ok(None)
            }
        };

        let summary = sqlx::query_as!(
            UserPointsSummary,
            r#"
            SELECT * FROM user_points_summary 
            WHERE user_address = $1 AND epoch_number = $2
            "#,
            user_address,
            epoch
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(summary)
    }
}
