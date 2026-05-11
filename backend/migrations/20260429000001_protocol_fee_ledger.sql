-- Append-only ledger of every USDT debit/credit out of user collateral
-- that is NOT a realized PnL. Captures protocol revenue events (and
-- protocol pay-outs as negative entries) for on-chain ↔ off-chain
-- reconciliation against the VAULT contract balance.
--
-- Rows are written inside the same transaction as the underlying
-- balance/position update — see backend/src/services/protocol_fee_ledger/mod.rs
-- (added in PR 2).
--
-- Field notes:
--   amount   — signed. Positive = debited from user (protocol receives).
--              Negative = credited to user (protocol pays out, e.g. a
--              negative funding settlement, or a liquidator_reward
--              physically leaving VAULT to the keeper wallet).
--   metadata — JSONB to avoid future ALTER TABLE for new context fields
--              (close_ratio, funding_rate, mark_price, etc.).
--   trade_id   is NULL for funding settlements and liquidations.
--   position_id is NULL for events not attributable to a single position
--              (currently only the bootstrap_pre_migration row).
--
-- Hypertable + columnar compression conversion lives in
-- scripts/protocol_fee_ledger_hypertable.sql (manual ops). The table is
-- empty at creation, so conversion has zero data-migration cost.

CREATE TABLE IF NOT EXISTS protocol_fee_ledger (
    id            UUID         NOT NULL DEFAULT gen_random_uuid(),
    user_address  VARCHAR(42)  NOT NULL,
    position_id   UUID,
    trade_id      UUID,
    fee_type      VARCHAR(24)  NOT NULL,
    -- trading_fee | funding_fee | borrowing_fee
    --   | liquidation_fee | insurance_contribution | liquidator_reward
    --   | bootstrap_pre_migration
    amount        NUMERIC(36,18) NOT NULL,
    asset         VARCHAR(10)  NOT NULL DEFAULT 'USDT',
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    metadata      JSONB,
    -- Composite PK so the table can be converted to a TimescaleDB
    -- hypertable later without a PK swap (hypertables require the time
    -- column to participate in every unique constraint, including the
    -- PK). Empty at creation so the conversion is metadata-only.
    PRIMARY KEY (id, created_at)
);

-- Per-user time-ordered index for reconciliation queries
-- ("what did this address pay/earn between t0 and t1?").
CREATE INDEX IF NOT EXISTS idx_pfl_user_time
    ON protocol_fee_ledger (user_address, created_at DESC);

-- Per-fee-type index for admin revenue dashboards
-- (SUM(amount) WHERE fee_type='trading_fee' AND created_at BETWEEN ...).
CREATE INDEX IF NOT EXISTS idx_pfl_type_time
    ON protocol_fee_ledger (fee_type, created_at DESC);
