//! User Points Query APIs
//!
//! Implements query endpoints for users to check their points,
//! history, rankings, and tier information.

use crate::models::points::*;
use anyhow::{Context, Result};
use redis::AsyncCommands;
use rust_decimal::Decimal;
use tracing::info;

impl super::PointsService {
    // ============================================================================
    // User Points Queries (Phase 2.5)
    // ============================================================================

    /// Get user points summary for an epoch
    ///
    /// Returns complete points breakdown with tier info and ranking.
    /// Uses Redis cache with 60s TTL.
    pub async fn get_user_points(
        &self,
        user_address: &str,
        epoch_number: Option<i32>,
    ) -> Result<Option<UserPointsResponse>> {
        // Get epoch number (use active if not specified)
        let epoch_num = if let Some(num) = epoch_number {
            num
        } else {
            match self.get_active_epoch().await? {
                Some(epoch) => epoch.epoch_number,
                None => return Ok(None),
            }
        };

        // Try cache first
        if let Some(redis) = self.get_redis() {
            let cache_key = Self::cache_key_user_points(user_address, epoch_num);
            let mut conn = redis.clone();

            if let Ok(cached) = conn.get::<_, String>(&cache_key).await {
                if let Ok(response) = serde_json::from_str::<UserPointsResponse>(&cached) {
                    return Ok(Some(response));
                }
            }
        }

        // Get from database
        let summary = sqlx::query_as::<_, UserPointsSummary>(
            r#"
            SELECT id, user_address, epoch_number,
                   trading_points, pnl_points, holding_points, referral_points, referral_code, staking_points,
                   total_points, trading_volume, trade_count, realized_pnl,
                   tier, tier_multiplier, referral_count, referral_volume,
                   COALESCE(earn_level, 0)        AS earn_level,
                   COALESCE(earn_level_weight, 4) AS earn_level_weight,
                   COALESCE(tp_daily_used, 0)     AS tp_daily_used,
                   COALESCE(tp_weekly_used, 0)    AS tp_weekly_used,
                   COALESCE(rp_daily_used, 0)     AS rp_daily_used,
                   tp_daily_reset_at,
                   tp_weekly_reset_at,
                   COALESCE(pp_daily_used, 0)     AS pp_daily_used,
                   pp_daily_reset_at,
                   COALESCE(hp_daily_used, 0)     AS hp_daily_used,
                   hp_daily_reset_at,
                   updated_at
            FROM user_points_summary
            WHERE user_address = $1 AND epoch_number = $2
            "#,
        )
        .bind(user_address)
        .bind(epoch_num)
        .fetch_optional(&self.pool)
        .await
        .context("Failed to fetch user points")?;

        let summary = match summary {
            Some(s) => s,
            None => return Ok(None),
        };

        // Get epoch info for status
        let epoch = self.get_epoch(epoch_num).await?
            .ok_or_else(|| anyhow::anyhow!("Epoch {} not found", epoch_num))?;

        // Get user rank
        let rank = self.get_user_rank(user_address, epoch_num, LeaderboardType::Total).await?;

        // EarnLevel: 计算距下一等级差额
        let earn_level_enum = EarnLevel::from_points(summary.total_points);
        let points_to_next = earn_level_enum.points_to_next(summary.total_points);

        let response = UserPointsResponse {
            user_address: summary.user_address.clone(),
            epoch_number: summary.epoch_number,
            epoch_status: epoch.status,
            trading_points: summary.trading_points,
            pnl_points: summary.pnl_points,
            holding_points: summary.holding_points,
            referral_points: summary.referral_points,
            referral_code: summary.referral_code,
            staking_points: summary.staking_points,
            total_points: summary.total_points,
            tier: summary.tier.clone(),
            tier_multiplier: summary.tier_multiplier,
            earn_level: summary.earn_level,
            earn_level_weight: summary.earn_level_weight,
            earn_level_points_to_next: points_to_next,
            rank,
            trading_volume: summary.trading_volume,
            trade_count: summary.trade_count,
            referral_count: summary.referral_count,
            updated_at: summary.updated_at,
        };

        // Cache the result
        if let Some(redis) = self.get_redis() {
            let cache_key = Self::cache_key_user_points(user_address, epoch_num);
            let cache_value = serde_json::to_string(&response)?;
            let mut conn = redis.clone();
            let ttl = self.config.read().await.cache_ttl;
            let _: () = conn.set_ex(&cache_key, cache_value, ttl).await?;
        }

        Ok(Some(response))
    }

    // ============================================================================
    // Points History (Phase 2.6)
    // ============================================================================

    /// Get user points history with pagination and filtering
    pub async fn get_user_points_history(
        &self,
        user_address: &str,
        epoch_number: Option<i32>,
        point_type: Option<PointType>,
        page: i32,
        page_size: i32,
    ) -> Result<PointsHistoryResponse> {
        let offset = (page - 1) * page_size;
        let limit = page_size;

        // Build query based on filters
        let mut query = String::from(
            r#"
            SELECT id, user_address, epoch_number, point_type, points,
                   related_trade_id, related_order_id, related_position_id,
                   referrer_address, metadata, created_at
            FROM points_events
            WHERE user_address = $1
            "#,
        );

        let mut param_count = 1;

        // Add epoch filter if provided
        if epoch_number.is_some() {
            param_count += 1;
            query.push_str(&format!(" AND epoch_number = ${}", param_count));
        }

        // Add point type filter if provided
        if point_type.is_some() {
            param_count += 1;
            query.push_str(&format!(" AND point_type = ${}", param_count));
        }

        query.push_str(" ORDER BY created_at DESC");
        query.push_str(&format!(" LIMIT ${} OFFSET ${}", param_count + 1, param_count + 2));

        // Build and execute query
        let mut db_query = sqlx::query_as::<_, PointsEvent>(&query)
            .bind(user_address);

        if let Some(epoch) = epoch_number {
            db_query = db_query.bind(epoch);
        }

        if let Some(pt) = &point_type {
            db_query = db_query.bind(pt.to_string());
        }

        db_query = db_query.bind(limit as i64).bind(offset as i64);

        let events = db_query
            .fetch_all(&self.pool)
            .await
            .context("Failed to fetch points history")?;

        // Get total count
        let mut count_query = String::from(
            "SELECT COUNT(*) FROM points_events WHERE user_address = $1"
        );

        let mut param_count = 1;
        if epoch_number.is_some() {
            param_count += 1;
            count_query.push_str(&format!(" AND epoch_number = ${}", param_count));
        }

        if point_type.is_some() {
            param_count += 1;
            count_query.push_str(&format!(" AND point_type = ${}", param_count));
        }

        let mut count_db_query = sqlx::query_scalar::<_, i64>(&count_query)
            .bind(user_address);

        if let Some(epoch) = epoch_number {
            count_db_query = count_db_query.bind(epoch);
        }

        if let Some(pt) = &point_type {
            count_db_query = count_db_query.bind(pt.to_string());
        }

        let total = count_db_query
            .fetch_one(&self.pool)
            .await
            .context("Failed to count points history")?;

        let event_details: Vec<PointsEventDetail> = events
            .into_iter()
            .map(PointsEventDetail::from)
            .collect();

        Ok(PointsHistoryResponse {
            events: event_details,
            total,
            page,
            page_size,
        })
    }

    // ============================================================================
    // Leaderboard (Phase 2.7)
    // ============================================================================

    /// Get leaderboard for a specific type
    ///
    /// Returns top N users sorted by points. Uses Redis cache.
    pub async fn get_leaderboard(
        &self,
        epoch_number: i32,
        rank_type: LeaderboardType,
        limit: i64,
    ) -> Result<LeaderboardResponse> {
        // Try cache first
        if let Some(redis) = self.get_redis() {
            let cache_key = Self::cache_key_leaderboard(epoch_number, &rank_type);
            let mut conn = redis.clone();

            if let Ok(cached) = conn.get::<_, String>(&cache_key).await {
                if let Ok(mut response) = serde_json::from_str::<LeaderboardResponse>(&cached) {
                    // Limit entries if cached list is longer
                    if response.entries.len() > limit as usize {
                        response.entries.truncate(limit as usize);
                    }
                    return Ok(response);
                }
            }
        }

        // Check if leaderboard cache table has fresh data
        let cached_entries: Vec<LeaderboardEntry> = sqlx::query_as(
            r#"
            SELECT id, epoch_number, rank_type, user_address, rank, points,
                   username, tier, updated_at
            FROM points_leaderboard
            WHERE epoch_number = $1 AND rank_type = $2
            ORDER BY rank ASC
            LIMIT $3
            "#,
        )
        .bind(epoch_number)
        .bind(rank_type.to_string())
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .context("Failed to fetch leaderboard")?;

        if !cached_entries.is_empty() {
            let total = cached_entries.len() as i64;
            let updated_at = cached_entries[0].updated_at;

            let entries: Vec<LeaderboardEntryDetail> = cached_entries
                .into_iter()
                .map(LeaderboardEntryDetail::from)
                .collect();

            let response = LeaderboardResponse {
                epoch_number,
                rank_type: rank_type.clone(),
                entries,
                total,
                updated_at,
            };

            // Cache in Redis
            if let Some(redis) = self.get_redis() {
                let cache_key = Self::cache_key_leaderboard(epoch_number, &rank_type);
                let cache_value = serde_json::to_string(&response)?;
                let mut conn = redis.clone();
                let ttl = self.config.read().await.cache_ttl;
                let _: () = conn.set_ex(&cache_key, cache_value, ttl).await?;
            }

            return Ok(response);
        }

        // If no cached data, generate from user_points_summary
        let (points_column, order_column) = match rank_type {
            LeaderboardType::Total => ("total_points", "total_points"),
            LeaderboardType::Trading => ("trading_points", "trading_points"),
            LeaderboardType::Pnl => ("pnl_points", "pnl_points"),
            LeaderboardType::Holding => ("holding_points", "holding_points"),
            LeaderboardType::Referral => ("referral_points", "referral_points"),
            LeaderboardType::Staking => ("staking_points", "staking_points"),
        };

        let query = format!(
            r#"
            SELECT user_address, {}, tier, updated_at,
                   ROW_NUMBER() OVER (ORDER BY {} DESC) as rank
            FROM user_points_summary
            WHERE epoch_number = $1 AND {} > 0
            ORDER BY {} DESC
            LIMIT $2
            "#,
            points_column, order_column, points_column, order_column
        );

        #[derive(sqlx::FromRow)]
        struct LeaderboardRow {
            user_address: String,
            #[sqlx(rename = "total_points")]
            points: Decimal,
            tier: Option<String>,
            updated_at: chrono::DateTime<chrono::Utc>,
            rank: i64,
        }

        let rows: Vec<LeaderboardRow> = sqlx::query_as(&query)
            .bind(epoch_number)
            .bind(limit)
            .fetch_all(&self.pool)
            .await
            .context("Failed to generate leaderboard")?;

        let total = rows.len() as i64;
        let updated_at = if !rows.is_empty() {
            rows[0].updated_at
        } else {
            chrono::Utc::now()
        };

        let entries: Vec<LeaderboardEntryDetail> = rows
            .into_iter()
            .map(|row| LeaderboardEntryDetail {
                rank: row.rank as i32,
                user_address: row.user_address,
                username: None,
                points: row.points,
                tier: row.tier,
            })
            .collect();

        let response = LeaderboardResponse {
            epoch_number,
            rank_type,
            entries,
            total,
            updated_at,
        };

        Ok(response)
    }

    /// Get user's rank in leaderboard
    pub async fn get_user_rank(
        &self,
        user_address: &str,
        epoch_number: i32,
        rank_type: LeaderboardType,
    ) -> Result<Option<i32>> {
        // Try from leaderboard cache first
        let cached_rank: Option<i32> = sqlx::query_scalar(
            r#"
            SELECT rank
            FROM points_leaderboard
            WHERE epoch_number = $1 AND rank_type = $2 AND user_address = $3
            "#,
        )
        .bind(epoch_number)
        .bind(rank_type.to_string())
        .bind(user_address)
        .fetch_optional(&self.pool)
        .await
        .context("Failed to fetch user rank from cache")?;

        if cached_rank.is_some() {
            return Ok(cached_rank);
        }

        // Calculate rank from user_points_summary
        let points_column = match rank_type {
            LeaderboardType::Total => "total_points",
            LeaderboardType::Trading => "trading_points",
            LeaderboardType::Pnl => "pnl_points",
            LeaderboardType::Holding => "holding_points",
            LeaderboardType::Referral => "referral_points",
            LeaderboardType::Staking => "staking_points",
        };

        let query = format!(
            r#"
            SELECT COUNT(*) + 1
            FROM user_points_summary
            WHERE epoch_number = $1
              AND {} > (
                  SELECT {}
                  FROM user_points_summary
                  WHERE user_address = $2 AND epoch_number = $1
              )
            "#,
            points_column, points_column
        );

        let rank: Option<i64> = sqlx::query_scalar(&query)
            .bind(epoch_number)
            .bind(user_address)
            .fetch_optional(&self.pool)
            .await
            .context("Failed to calculate user rank")?;

        Ok(rank.map(|r| r as i32))
    }

    /// Refresh leaderboard cache (background task)
    ///
    /// This should be called periodically to update the leaderboard cache table.
    pub async fn refresh_leaderboard(&self, epoch_number: i32) -> Result<()> {
        let config = self.config.read().await;
        let limit = config.leaderboard_limit as i64;

        for rank_type in [
            LeaderboardType::Total,
            LeaderboardType::Trading,
            LeaderboardType::Pnl,
            LeaderboardType::Holding,
            LeaderboardType::Referral,
            LeaderboardType::Staking,
        ] {
            let points_column = match rank_type {
                LeaderboardType::Total => "total_points",
                LeaderboardType::Trading => "trading_points",
                LeaderboardType::Pnl => "pnl_points",
                LeaderboardType::Holding => "holding_points",
                LeaderboardType::Referral => "referral_points",
                LeaderboardType::Staking => "staking_points",
            };

            // Delete old entries for this rank type
            sqlx::query(
                r#"
                DELETE FROM points_leaderboard
                WHERE epoch_number = $1 AND rank_type = $2
                "#,
            )
            .bind(epoch_number)
            .bind(rank_type.to_string())
            .execute(&self.pool)
            .await
            .context("Failed to delete old leaderboard entries")?;

            // Insert new leaderboard entries
            let query = format!(
                r#"
                INSERT INTO points_leaderboard (epoch_number, rank_type, user_address, rank, points, tier)
                SELECT
                    $1,
                    $2,
                    user_address,
                    ROW_NUMBER() OVER (ORDER BY {} DESC) as rank,
                    {},
                    tier
                FROM user_points_summary
                WHERE epoch_number = $1 AND {} > 0
                ORDER BY {} DESC
                LIMIT $3
                "#,
                points_column, points_column, points_column, points_column
            );

            sqlx::query(&query)
                .bind(epoch_number)
                .bind(rank_type.to_string())
                .bind(limit)
                .execute(&self.pool)
                .await
                .context("Failed to refresh leaderboard")?;
        }

        // Invalidate Redis cache for all leaderboard types
        self.invalidate_leaderboard_cache(epoch_number).await?;

        info!("Leaderboard refreshed for epoch {}", epoch_number);

        Ok(())
    }

    // ============================================================================
    // Daily Increment Leaderboard
    // ============================================================================

    /// Refresh the daily points increment leaderboard — incremental mode.
    ///
    /// On the **first run of a UTC day** (or when Redis has no watermark) this does
    /// a full re-aggregation from midnight.  On every subsequent call it only
    /// aggregates events that arrived **since the last watermark**, adds them to the
    /// existing per-user totals, and then re-ranks the whole table in one UPDATE.
    ///
    /// This means the expensive `points_events` scan is bounded by the 5-minute
    /// window instead of growing throughout the day.
    pub async fn refresh_daily_leaderboard(&self, epoch_number: i32) -> Result<usize> {
        use chrono::Utc;

        let scan_end = Utc::now();
        let today = scan_end.date_naive();
        let day_start = today.and_hms_opt(0, 0, 0).unwrap().and_utc();

        // Decide: full refresh (first run of day) or incremental (subsequent runs).
        let watermark = self.get_daily_leaderboard_watermark(epoch_number).await;
        let (scan_from, is_full) = match watermark {
            Some(wm) if wm.date_naive() == today => (wm, false),
            _ => (day_start, true),
        };

        // On a new day, purge yesterday's rows before rebuilding.
        if is_full {
            sqlx::query(
                "DELETE FROM daily_points_leaderboard WHERE epoch_number = $1 AND date < $2",
            )
            .bind(epoch_number)
            .bind(today)
            .execute(&self.pool)
            .await
            .context("Failed to purge stale daily leaderboard rows")?;
        }

        // Count new events in [scan_from, scan_end).  Skip everything else when
        // there is nothing to process (common during quiet periods).
        let new_events: i64 = sqlx::query_scalar(
            r#"SELECT COUNT(*) FROM points_events
               WHERE epoch_number = $1
                 AND created_at >= $2 AND created_at < $3
                 AND points > 0"#,
        )
        .bind(epoch_number)
        .bind(scan_from)
        .bind(scan_end)
        .fetch_one(&self.pool)
        .await
        .unwrap_or(0);

        if new_events == 0 && !is_full {
            // Nothing new — advance the watermark and return the current row count.
            self.set_daily_leaderboard_watermark(epoch_number, scan_end).await;
            let n: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM daily_points_leaderboard WHERE epoch_number = $1 AND date = $2",
            )
            .bind(epoch_number)
            .bind(today)
            .fetch_one(&self.pool)
            .await
            .unwrap_or(0);
            return Ok(n as usize);
        }

        // --- Step 1: UPSERT incremental deltas -----------------------------------
        // Full refresh replaces points_today; incremental adds to it.
        if is_full {
            sqlx::query(
                r#"INSERT INTO daily_points_leaderboard
                       (epoch_number, date, rank, user_address, points_today, tier, refreshed_at)
                   SELECT $1, CURRENT_DATE, 0, pe.user_address,
                          SUM(pe.points), ups.tier, $3
                   FROM points_events pe
                   LEFT JOIN user_points_summary ups
                          ON ups.user_address = pe.user_address AND ups.epoch_number = $1
                   WHERE pe.epoch_number = $1
                     AND pe.created_at >= $2 AND pe.created_at < $3
                     AND pe.points > 0
                   GROUP BY pe.user_address, ups.tier
                   ON CONFLICT (epoch_number, date, user_address) DO UPDATE SET
                       points_today = EXCLUDED.points_today,
                       tier         = COALESCE(EXCLUDED.tier, daily_points_leaderboard.tier),
                       refreshed_at = EXCLUDED.refreshed_at"#,
            )
            .bind(epoch_number)
            .bind(scan_from)
            .bind(scan_end)
            .execute(&self.pool)
            .await
            .context("Failed to upsert full daily leaderboard")?;
        } else {
            sqlx::query(
                r#"INSERT INTO daily_points_leaderboard
                       (epoch_number, date, rank, user_address, points_today, tier, refreshed_at)
                   SELECT $1, CURRENT_DATE, 0, pe.user_address,
                          SUM(pe.points), ups.tier, $3
                   FROM points_events pe
                   LEFT JOIN user_points_summary ups
                          ON ups.user_address = pe.user_address AND ups.epoch_number = $1
                   WHERE pe.epoch_number = $1
                     AND pe.created_at >= $2 AND pe.created_at < $3
                     AND pe.points > 0
                   GROUP BY pe.user_address, ups.tier
                   ON CONFLICT (epoch_number, date, user_address) DO UPDATE SET
                       points_today = daily_points_leaderboard.points_today + EXCLUDED.points_today,
                       tier         = COALESCE(EXCLUDED.tier, daily_points_leaderboard.tier),
                       refreshed_at = EXCLUDED.refreshed_at"#,
            )
            .bind(epoch_number)
            .bind(scan_from)
            .bind(scan_end)
            .execute(&self.pool)
            .await
            .context("Failed to upsert incremental daily leaderboard")?;
        }

        // --- Step 2: Re-rank all rows for today in one pass ----------------------
        sqlx::query(
            r#"UPDATE daily_points_leaderboard AS lb
               SET rank = sub.new_rank
               FROM (
                   SELECT id,
                          ROW_NUMBER() OVER (ORDER BY points_today DESC, user_address ASC)
                              AS new_rank
                   FROM daily_points_leaderboard
                   WHERE epoch_number = $1 AND date = CURRENT_DATE
               ) AS sub
               WHERE lb.id = sub.id"#,
        )
        .bind(epoch_number)
        .execute(&self.pool)
        .await
        .context("Failed to re-rank daily leaderboard")?;

        // Advance the watermark so the next run only scans new events.
        self.set_daily_leaderboard_watermark(epoch_number, scan_end).await;

        // Invalidate the response cache.
        if let Some(redis) = self.get_redis() {
            let key = Self::cache_key_daily_leaderboard(epoch_number);
            let mut conn = redis.clone();
            let _: redis::RedisResult<()> = conn.del(&key).await;
        }

        let total: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM daily_points_leaderboard WHERE epoch_number = $1 AND date = $2",
        )
        .bind(epoch_number)
        .bind(today)
        .fetch_one(&self.pool)
        .await
        .unwrap_or(0);

        info!(
            "Daily leaderboard refreshed for epoch {} ({} mode): {} entries, scanned [{}, {})",
            epoch_number,
            if is_full { "full" } else { "incremental" },
            total,
            scan_from,
            scan_end,
        );
        Ok(total as usize)
    }

    /// Query the daily points increment leaderboard.
    ///
    /// Reads directly from the `daily_points_leaderboard` cache table which is
    /// kept fresh by the 5-minute background worker.
    pub async fn get_daily_leaderboard(
        &self,
        epoch_number: i32,
        limit: i64,
        offset: i64,
    ) -> Result<DailyLeaderboardResponse> {
        use chrono::Utc;

        let today = Utc::now().date_naive();

        #[derive(sqlx::FromRow)]
        struct Row {
            rank: i32,
            user_address: String,
            points_today: Decimal,
            tier: Option<String>,
            refreshed_at: chrono::DateTime<chrono::Utc>,
        }

        let (rows, total) = tokio::try_join!(
            sqlx::query_as::<_, Row>(
                r#"SELECT rank, user_address, points_today, tier, refreshed_at
                   FROM daily_points_leaderboard
                   WHERE epoch_number = $1 AND date = $2
                   ORDER BY rank ASC
                   LIMIT $3 OFFSET $4"#,
            )
            .bind(epoch_number)
            .bind(today)
            .bind(limit)
            .bind(offset)
            .fetch_all(&self.pool),
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM daily_points_leaderboard WHERE epoch_number = $1 AND date = $2",
            )
            .bind(epoch_number)
            .bind(today)
            .fetch_one(&self.pool),
        )
        .context("Failed to fetch daily leaderboard")?;

        let refreshed_at = rows.first().map(|r| r.refreshed_at).unwrap_or_else(Utc::now);

        Ok(DailyLeaderboardResponse {
            date: today.to_string(),
            epoch_number,
            refreshed_at,
            total,
            entries: rows
                .into_iter()
                .map(|r| DailyLeaderboardEntry {
                    rank: r.rank,
                    user_address: r.user_address,
                    points_today: r.points_today,
                    tier: r.tier,
                })
                .collect(),
        })
    }

    fn cache_key_daily_leaderboard(epoch_number: i32) -> String {
        format!("points:daily_leaderboard:{}", epoch_number)
    }

    async fn get_daily_leaderboard_watermark(
        &self,
        epoch_number: i32,
    ) -> Option<chrono::DateTime<chrono::Utc>> {
        let redis = self.get_redis()?;
        let key = format!("points:daily_leaderboard:{}:watermark", epoch_number);
        let mut conn = redis.clone();
        let val: String = conn.get(&key).await.ok()?;
        val.parse::<chrono::DateTime<chrono::Utc>>().ok()
    }

    async fn set_daily_leaderboard_watermark(
        &self,
        epoch_number: i32,
        ts: chrono::DateTime<chrono::Utc>,
    ) {
        if let Some(redis) = self.get_redis() {
            let key = format!("points:daily_leaderboard:{}:watermark", epoch_number);
            let mut conn = redis.clone();
            // 48-hour TTL: survives short Redis restarts across day boundaries.
            let _: redis::RedisResult<()> = conn.set_ex(&key, ts.to_rfc3339(), 172800).await;
        }
    }

    // ============================================================================
    // Points Simulation (Phase1)
    // ============================================================================

    /// 预估单笔交易的积分收益（纯读，不写库）
    pub async fn simulate_points(
        &self,
        user_address: &str,
        epoch_number: i32,
        request: &SimulatePointsRequest,
    ) -> Result<SimulatePointsResponse> {
        use chrono::{Datelike, Utc};

        let cfg = self.get_points_config(epoch_number).await?;

        // 确定 Tier 费率：优先使用请求参数，否则按用户当前滚动量计算
        let (tier_str, role_rate) = if let Some(t) = request.tier {
            let (maker, taker) = match t {
                1 => (cfg.tp_t1_maker, cfg.tp_t1_taker),
                2 => (cfg.tp_t2_maker, cfg.tp_t2_taker),
                _ => (cfg.tp_t3_maker, cfg.tp_t3_taker),
            };
            let tier_str = format!("T{}", t.clamp(1, 3));
            let rate = if request.order_type.to_lowercase() == "maker" { maker } else { taker };
            (tier_str, rate)
        } else {
            // 根据用户 VIP 等级确定 Tier
            let tier_result = self.calculate_tier(user_address, epoch_number).await?;
            let rate = if request.order_type.to_lowercase() == "maker" {
                tier_result.maker_rate
            } else {
                tier_result.taker_rate
            };
            (tier_result.tier, rate)
        };

        // 读用户当前的日/周已用量
        let (tp_daily_used, tp_weekly_used, tp_daily_reset_at, tp_weekly_reset_at): (
            Decimal, Decimal,
            Option<chrono::DateTime<Utc>>,
            Option<chrono::DateTime<Utc>>,
        ) = sqlx::query_as(
            r#"
            SELECT COALESCE(tp_daily_used, 0),
                   COALESCE(tp_weekly_used, 0),
                   tp_daily_reset_at,
                   tp_weekly_reset_at
            FROM user_points_summary
            WHERE user_address = $1 AND epoch_number = $2
            "#,
        )
        .bind(user_address)
        .bind(epoch_number)
        .fetch_optional(&self.pool)
        .await
        .context("Failed to fetch cap usage for simulate")?
        .unwrap_or((Decimal::ZERO, Decimal::ZERO, None, None));

        let today = Utc::now().date_naive();

        let daily_used = if tp_daily_reset_at.map(|t| t.date_naive() == today).unwrap_or(false) {
            tp_daily_used
        } else {
            Decimal::ZERO
        };
        let weekly_used = if tp_weekly_reset_at
            .map(|t| {
                let d = t.date_naive();
                d.iso_week().week() == today.iso_week().week() && d.year() == today.year()
            })
            .unwrap_or(false)
        {
            tp_weekly_used
        } else {
            Decimal::ZERO
        };

        let daily_cap = Decimal::from(cfg.tp_daily_cap);
        let weekly_cap = Decimal::from(cfg.tp_weekly_cap);

        let tp_estimate = (request.trade_amount / Decimal::from(1000)) * role_rate;
        let daily_remaining = (daily_cap - daily_used).max(Decimal::ZERO);
        let weekly_remaining = (weekly_cap - weekly_used).max(Decimal::ZERO);
        let tp_effective = tp_estimate.min(daily_remaining).min(weekly_remaining).max(Decimal::ZERO);

        Ok(SimulatePointsResponse {
            tp_estimate,
            tp_effective,
            hp_estimate: Decimal::ZERO, // Phase 2
            total_estimate: tp_effective,
            tier: tier_str,
            role_rate,
            daily_cap_status: CapInfo {
                used: daily_used,
                cap: cfg.tp_daily_cap,
                remaining: daily_remaining,
            },
            weekly_cap_status: CapInfo {
                used: weekly_used,
                cap: cfg.tp_weekly_cap,
                remaining: weekly_remaining,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_leaderboard_type_to_column() {
        let test_cases = vec![
            (LeaderboardType::Total, "total_points"),
            (LeaderboardType::Trading, "trading_points"),
            (LeaderboardType::Pnl, "pnl_points"),
        ];

        for (rank_type, expected_column) in test_cases {
            let column = match rank_type {
                LeaderboardType::Total => "total_points",
                LeaderboardType::Trading => "trading_points",
                LeaderboardType::Pnl => "pnl_points",
                _ => "total_points",
            };
            assert_eq!(column, expected_column);
        }
    }
}
