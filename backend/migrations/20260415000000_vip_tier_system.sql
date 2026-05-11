-- VIP 阶梯费率落地 —— 6 档 (VIP0–VIP5)，14 天滚动交易量。
-- 升级即时生效（immediate），降级有一日缓冲期（next UTC 00:00 才应用）。

CREATE TABLE IF NOT EXISTS user_vip_tiers (
    user_address          TEXT        PRIMARY KEY,
    current_tier          SMALLINT    NOT NULL DEFAULT 0,
    effective_since       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    pending_tier          SMALLINT,
    pending_effective_at  TIMESTAMPTZ,
    last_volume_14d       NUMERIC(30,10) NOT NULL DEFAULT 0,
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_user_vip_tiers_pending
    ON user_vip_tiers (pending_effective_at)
    WHERE pending_effective_at IS NOT NULL;

-- 事件审计：每次 tier 变更留痕，便于客服/风控排查。
CREATE TABLE IF NOT EXISTS vip_tier_events (
    id             UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_address   TEXT        NOT NULL,
    old_tier       SMALLINT    NOT NULL,
    new_tier       SMALLINT    NOT NULL,
    volume_14d     NUMERIC(30,10) NOT NULL,
    reason         TEXT        NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_vip_tier_events_user_time
    ON vip_tier_events (user_address, created_at DESC);
