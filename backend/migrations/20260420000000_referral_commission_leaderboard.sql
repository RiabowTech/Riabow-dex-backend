-- Referral commission leaderboard + incremental watermark
--
-- referral_commission_leaderboard holds the running total of commissions
-- per referrer and is refreshed every 5 minutes by the background worker.
-- The handler reads from this table with LIMIT n (n ∈ [1, 50]).

CREATE TABLE IF NOT EXISTS referral_commission_leaderboard (
    referrer_address  TEXT           NOT NULL PRIMARY KEY,
    total_commission  NUMERIC(36,18) NOT NULL DEFAULT 0,
    computed_at       TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

-- Fast top-N queries
CREATE INDEX IF NOT EXISTS idx_rcl_commission
    ON referral_commission_leaderboard (total_commission DESC);

-- Index on referral_earnings.created_at for the incremental worker window scan
CREATE INDEX IF NOT EXISTS idx_referral_earnings_created_at
    ON referral_earnings (created_at);

-- Single-row watermark: tracks the max created_at already aggregated
CREATE TABLE IF NOT EXISTS referral_lb_watermark (
    id                INT         PRIMARY KEY DEFAULT 1,
    last_processed_at TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01 00:00:00+00'
);

INSERT INTO referral_lb_watermark (id, last_processed_at)
VALUES (1, '1970-01-01 00:00:00+00')
ON CONFLICT (id) DO NOTHING;
