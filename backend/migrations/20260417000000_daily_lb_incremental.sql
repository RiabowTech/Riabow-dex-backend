-- Swap unique constraint on daily_points_leaderboard to support
-- watermark-based incremental UPSERT (conflict on user_address, not rank).
-- Rank is now just an updatable column recomputed after each upsert batch.

ALTER TABLE daily_points_leaderboard
    DROP CONSTRAINT IF EXISTS daily_points_leaderboard_epoch_number_date_rank_key;

-- The old non-unique user index is superseded by the new unique constraint below.
DROP INDEX IF EXISTS idx_daily_lb_user;

ALTER TABLE daily_points_leaderboard
    ADD CONSTRAINT daily_points_leaderboard_epoch_date_user_key
    UNIQUE (epoch_number, date, user_address);

-- Fast ORDER BY rank for the GET query.
CREATE INDEX IF NOT EXISTS idx_daily_lb_rank
    ON daily_points_leaderboard (epoch_number, date, rank);
