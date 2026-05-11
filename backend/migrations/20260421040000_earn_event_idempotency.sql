-- Idempotency guard for the earn on-chain event listener.
--
-- Incident: user subscribed 0.2 USDT but earn_subscriptions.amount showed
-- 0.4 USDT. Root cause — `handle_subscribed_event` upserts with
-- `amount = amount + $5` keyed on (product_id, user_address), so any
-- replay of the same Subscribed log double-counts. The poll loop is
-- at-least-once (restart between event apply and block-cursor write,
-- `update_last_synced_block().await.ok()` swallowing errors, or multiple
-- backend replicas all scanning the same range). The same class of bug
-- affects `handle_settled_event`, which INSERTs a settlement row with no
-- unique key.
--
-- This table exists solely to let each handler claim a (tx_hash,
-- log_index) pair exactly once inside its own DB transaction; if the
-- claim fails due to the UNIQUE constraint, the handler aborts and the
-- replay has no effect.

CREATE TABLE IF NOT EXISTS earn_processed_events (
    id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    tx_hash       VARCHAR(66) NOT NULL,
    log_index     INTEGER     NOT NULL,
    event_type    VARCHAR(32) NOT NULL,
    block_number  BIGINT      NOT NULL,
    processed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tx_hash, log_index)
);

CREATE INDEX IF NOT EXISTS idx_earn_processed_events_block
    ON earn_processed_events (block_number);
