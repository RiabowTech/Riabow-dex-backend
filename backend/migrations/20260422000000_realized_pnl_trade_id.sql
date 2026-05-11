-- Link realized_pnl_events to the specific trade that produced them.
--
-- `realized_pnl_events` used to be joined to `trades` via (user_address,
-- symbol, created_at ±5s) which is inherently fragile:
--   * One close order can fill against N makers, yielding N trades but only
--     one event (or vice versa), so proximity attribution double-counts or
--     misses.
--   * Manual closes via POST /positions/:id/close never created a trade
--     row at all, so those PnL events had nothing on the trade side to
--     attach to — users saw a flat $0 realized PnL column.
--
-- Adding `trade_id` lets writers record the exact producing trade (synthetic
-- self-matched trade for manual closes, liquidator-side trade for
-- liquidations, etc.) and lets `/account/trades` resolve PnL with a
-- deterministic LEFT JOIN. The column is nullable so old rows still work via
-- the legacy time-proximity fallback.

ALTER TABLE realized_pnl_events
    ADD COLUMN IF NOT EXISTS trade_id UUID;

CREATE INDEX IF NOT EXISTS idx_realized_pnl_events_trade_id
    ON realized_pnl_events (trade_id)
    WHERE trade_id IS NOT NULL;
