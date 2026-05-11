-- Add per-position accumulator for maker/taker fees that have been
-- recorded into trades.maker_fee/taker_fee at fill time but never
-- debited from collateral. After PR 2 lands, accumulated_trading_fee
-- will replace the legacy `position_fee = close_size_usd ×
-- position_fee_rate` path that today is the only fee actually moved
-- out of user balance on close.
--
-- Existing open positions start at 0 (Option I — zero-out, see spec
-- §3 PR 2 "Existing-position handling"). This means historical fills
-- on currently-open positions get a free pass on close — the migration
-- cost is bounded and accepted.
--
-- Precision: NUMERIC(36,18) — mirrors accumulated_funding_fee and
-- accumulated_borrowing_fee on this same table so all three sum without
-- implicit casts.
--
-- ALTER TABLE … ADD COLUMN … DEFAULT 0 NOT NULL with a constant default
-- is metadata-only on PostgreSQL 11+ (no rewrite, no row scan), safe at
-- the current 3.1 M rows / 983 MB.

ALTER TABLE positions
    ADD COLUMN IF NOT EXISTS accumulated_trading_fee NUMERIC(36,18) NOT NULL DEFAULT 0;
