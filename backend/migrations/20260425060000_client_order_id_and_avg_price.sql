-- 2026-04-25 06:00 UTC — B5-7 fix
--
-- 1. Add `client_order_id` so user-supplied `newClientOrderId` can finally
--    be persisted (was silently dropped: response always echoed orderId).
-- 2. Add `avg_price` so executed-fill avg can live in its own column instead
--    of overwriting `price` (the original limit price). Old rows where
--    `price` was already overwritten are unrecoverable; we leave them and
--    only write the new column going forward.

ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS client_order_id VARCHAR(64),
    ADD COLUMN IF NOT EXISTS avg_price       NUMERIC(36, 18);

-- Per-user uniqueness on active orders only — once an order reaches a
-- terminal status (filled/cancelled/rejected) the same coid can be reused
-- for a new order. Mirrors Binance's reuse semantics without their 24h
-- post-terminal cooldown (we don't currently keep a coid history index).
CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_user_client_oid_active
    ON orders (user_address, client_order_id)
    WHERE status IN ('open', 'partially_filled', 'pending')
      AND client_order_id IS NOT NULL;

-- Lookup index for cancel/query by orig_client_order_id (any status)
CREATE INDEX IF NOT EXISTS idx_orders_user_client_oid
    ON orders (user_address, client_order_id)
    WHERE client_order_id IS NOT NULL;
