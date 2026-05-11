-- Bootstrap migration for all DDL that used to live inline in
-- `backend/src/db/mod.rs::Database::connect_with_config`. Moving it into a
-- proper sqlx migration means restarts no longer re-issue AccessExclusiveLock
-- on hot tables (e.g. `orders`) — `_sqlx_migrations` tracks that this file has
-- been applied and sqlx::migrate!() will skip it.
--
-- Every statement is idempotent (IF NOT EXISTS / ON CONFLICT) so this file is
-- safe to run against an already-populated database.

-- ----------------------------------------------------------------------------
-- user_api_keys
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS user_api_keys (
    id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id       UUID        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    api_key       VARCHAR(64) NOT NULL UNIQUE,
    secret_key    VARCHAR(255) NOT NULL,
    label         VARCHAR(50),
    ip_whitelist  TEXT,
    permissions   TEXT        NOT NULL DEFAULT 'trading,deposit',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_used_at  TIMESTAMPTZ,
    status        VARCHAR(20) NOT NULL DEFAULT 'active'
                  CHECK (status IN ('active', 'disabled'))
);

CREATE INDEX IF NOT EXISTS idx_user_api_keys_user_id ON user_api_keys(user_id);
CREATE INDEX IF NOT EXISTS idx_user_api_keys_api_key ON user_api_keys(api_key);

-- ----------------------------------------------------------------------------
-- orders: deferred TP/SL columns (tp_price, sl_price, trigger_order_id)
-- ----------------------------------------------------------------------------
ALTER TABLE orders ADD COLUMN IF NOT EXISTS tp_price          NUMERIC;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS sl_price          NUMERIC;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS trigger_order_id  UUID;

-- ----------------------------------------------------------------------------
-- epochs + user_points (Points System core)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS epochs (
    id          SERIAL       PRIMARY KEY,
    label       VARCHAR(255) NOT NULL,
    start_date  TIMESTAMPTZ  NOT NULL,
    end_date    TIMESTAMPTZ  NOT NULL,
    status      VARCHAR(50)  NOT NULL CHECK (status IN ('upcoming', 'active', 'ended')),
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_epochs_status     ON epochs(status);
CREATE INDEX IF NOT EXISTS idx_epochs_date_range ON epochs(start_date, end_date);

CREATE TABLE IF NOT EXISTS user_points (
    id               UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    user_address     VARCHAR(42)     NOT NULL,
    epoch_id         INTEGER         REFERENCES epochs(id),
    total_points     DECIMAL(36, 18) NOT NULL DEFAULT 0,
    rank             INTEGER,
    trading_points   DECIMAL(36, 18) NOT NULL DEFAULT 0,
    holding_points   DECIMAL(36, 18) NOT NULL DEFAULT 0,
    pnl_points       DECIMAL(36, 18) NOT NULL DEFAULT 0,
    referral_points  DECIMAL(36, 18) NOT NULL DEFAULT 0,
    staking_points   DECIMAL(36, 18) NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    UNIQUE(user_address, epoch_id)
);

CREATE INDEX IF NOT EXISTS idx_user_points_address ON user_points(user_address);
CREATE INDEX IF NOT EXISTS idx_user_points_epoch   ON user_points(epoch_id);
CREATE INDEX IF NOT EXISTS idx_user_points_rank    ON user_points(epoch_id, total_points DESC);

-- Seed default epochs when empty. ON CONFLICT targets (label) — close enough
-- to idempotent; the inline version used `SELECT COUNT(*) = 0` as the gate.
INSERT INTO epochs (label, start_date, end_date, status) VALUES
    ('Epoch 1: 2026.1.1 - 2026.1.7',  '2026-01-01 00:00:00+00', '2026-01-07 23:59:59+00', 'ended'),
    ('Epoch 2: 2026.1.8 - 2026.1.15', '2026-01-08 00:00:00+00', '2026-01-15 23:59:59+00', 'ended'),
    ('Epoch 3: 2026.3.8 - 2026.3.15', '2026-03-08 00:00:00+00', '2026-03-15 23:59:59+00', 'active')
ON CONFLICT DO NOTHING;

-- ----------------------------------------------------------------------------
-- referral_relations: activation reward flag
-- point_logs: per-user point ledger
-- ----------------------------------------------------------------------------
ALTER TABLE referral_relations
    ADD COLUMN IF NOT EXISTS activation_reward_claimed BOOLEAN NOT NULL DEFAULT FALSE;

CREATE TABLE IF NOT EXISTS point_logs (
    id            UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    user_address  VARCHAR(42)     NOT NULL,
    point_type    VARCHAR(20)     NOT NULL,
    amount        DECIMAL(36, 18) NOT NULL,
    description   TEXT,
    created_at    TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_point_logs_user_type_date
    ON point_logs(user_address, point_type, created_at);

-- ----------------------------------------------------------------------------
-- Unified Margin Mode (MVP)
-- ----------------------------------------------------------------------------
ALTER TABLE users
    ADD COLUMN IF NOT EXISTS margin_mode VARCHAR(16) NOT NULL DEFAULT 'isolated'
        CHECK (margin_mode IN ('isolated', 'unified'));

CREATE TABLE IF NOT EXISTS unified_margin_accounts (
    id                    UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    user_address          VARCHAR(42)     NOT NULL UNIQUE,
    total_equity          DECIMAL(36, 18) NOT NULL DEFAULT 0,
    available_balance     DECIMAL(36, 18) NOT NULL DEFAULT 0,
    total_initial_margin  DECIMAL(36, 18) NOT NULL DEFAULT 0,
    total_maint_margin    DECIMAL(36, 18) NOT NULL DEFAULT 0,
    total_unrealized_pnl  DECIMAL(36, 18) NOT NULL DEFAULT 0,
    uni_mmr               DECIMAL(36, 18),
    account_status        VARCHAR(20)     NOT NULL DEFAULT 'normal'
        CHECK (account_status IN ('normal','warning_1','warning_2','reduce_only','liquidating')),
    is_reduce_only        BOOLEAN         NOT NULL DEFAULT false,
    created_at            TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_users_margin_mode
    ON users(margin_mode) WHERE margin_mode = 'unified';

CREATE TABLE IF NOT EXISTS unified_liquidation_records (
    id                   UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    user_address         VARCHAR(42)     NOT NULL,
    position_id          UUID            NOT NULL,
    symbol               VARCHAR(20)     NOT NULL,
    side                 VARCHAR(10)     NOT NULL,
    closed_size_usd      DECIMAL(36, 18) NOT NULL,
    closed_size_tokens   DECIMAL(36, 18) NOT NULL,
    mark_price           DECIMAL(36, 18) NOT NULL,
    pnl_realized         DECIMAL(36, 18) NOT NULL,
    collateral_returned  DECIMAL(36, 18) NOT NULL,
    trigger_uni_mmr      DECIMAL(36, 18),
    trigger_equity       DECIMAL(36, 18) NOT NULL,
    post_uni_mmr         DECIMAL(36, 18),
    liquidation_type     VARCHAR(20)     NOT NULL DEFAULT 'partial',
    created_at           TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_unified_liq_user ON unified_liquidation_records(user_address);
CREATE INDEX IF NOT EXISTS idx_unified_liq_time ON unified_liquidation_records(created_at DESC);

-- ----------------------------------------------------------------------------
-- Tiered margin ladder (design doc §12)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS margin_tiers (
    id                 UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    symbol             VARCHAR(20)     NOT NULL,
    tier               INT             NOT NULL,
    max_notional       DECIMAL(36, 18) NOT NULL,
    maint_margin_rate  DECIMAL(8, 6)   NOT NULL,
    max_leverage       INT             NOT NULL,
    cum_amount         DECIMAL(36, 18) NOT NULL DEFAULT 0,
    UNIQUE(symbol, tier)
);

INSERT INTO margin_tiers (symbol, tier, max_notional, maint_margin_rate, max_leverage, cum_amount) VALUES
    ('*', 1, 50000,     0.004, 125, 0),
    ('*', 2, 250000,    0.005, 100, 50),
    ('*', 3, 1000000,   0.01,   50, 1300),
    ('*', 4, 5000000,   0.025,  20, 16300),
    ('*', 5, 20000000,  0.05,   10, 141300),
    ('*', 6, 50000000,  0.10,    5, 1141300),
    ('*', 7, 100000000, 0.125,   4, 2391300),
    ('*', 8, 200000000, 0.15,    2, 4891300)
ON CONFLICT (symbol, tier) DO NOTHING;
