-- Add per-market per-side aggregate Open Interest caps.
-- Hard-rejected at order placement when adding `delta_usd` to `side` OI
-- on `symbol` would push the in-memory OI total past the cap.
-- Existing positions can still be closed (cap is open-side only).
--
-- Default $5,000,000 per side covers the tail symbols safely; top-10
-- markets get higher caps via scripts/oi_cap_bootstrap.sql post-deploy.
--
-- Precision (28,2) mirrors max_position_size_usd on the same table.
-- ALTER TABLE … ADD COLUMN … DEFAULT N NOT NULL is metadata-only on
-- PG 11+ for constant defaults — instant on the current row count.

ALTER TABLE market_configs
    ADD COLUMN IF NOT EXISTS max_long_oi_usd  NUMERIC(28,2) NOT NULL DEFAULT 5000000,
    ADD COLUMN IF NOT EXISTS max_short_oi_usd NUMERIC(28,2) NOT NULL DEFAULT 5000000;
