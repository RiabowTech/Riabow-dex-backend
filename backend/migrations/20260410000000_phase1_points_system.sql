-- =============================================================================
-- Phase 1 积分系统迁移
-- 生成时间: 2026-04-10
-- 覆盖范围:
--   1. 新增 points_config         —— TP/Tier/RP 参数（支持 epoch 级别覆盖）
--   2. 新增 earn_level_config     —— Earn Level 阈值与权重（L0-L5）
--   3. 新增 rp_trigger_events     —— RP 一次性触发记录
--   4. 新增 earn_weight_snapshot  —— 每日 Earn 权重聚合快照
--   5. ALTER user_points_summary  —— 新增 Phase1 所需字段
-- =============================================================================

-- =============================================================================
-- 1. points_config
--    每个 epoch 可独立配置 TP 系数、Tier 阈值、RP 参数。
--    epoch_number = 0 作为全局默认行（必须存在）。
-- =============================================================================
CREATE TABLE IF NOT EXISTS points_config (
    id                    UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    epoch_number          INT         NOT NULL UNIQUE,   -- 0 = 全局默认

    -- TP 系数（per 1000U 成交量）
    tp_t1_maker           DECIMAL(8,4) NOT NULL DEFAULT 1.2,
    tp_t1_taker           DECIMAL(8,4) NOT NULL DEFAULT 0.8,
    tp_t2_maker           DECIMAL(8,4) NOT NULL DEFAULT 1.5,
    tp_t2_taker           DECIMAL(8,4) NOT NULL DEFAULT 1.0,
    tp_t3_maker           DECIMAL(8,4) NOT NULL DEFAULT 2.0,
    tp_t3_taker           DECIMAL(8,4) NOT NULL DEFAULT 1.3,
    tp_daily_cap          INT          NOT NULL DEFAULT 5000,    -- 日上限（积分）
    tp_weekly_cap         INT          NOT NULL DEFAULT 25000,   -- 周上限（积分）

    -- Tier 阈值（14日滚动成交量，USD）
    tier_t2_min           DECIMAL(20,2) NOT NULL DEFAULT 5000000,    -- $5M -> T2
    tier_t3_min           DECIMAL(20,2) NOT NULL DEFAULT 100000000,  -- $100M -> T3

    -- RP 触发参数
    rp_trigger_min_volume DECIMAL(10,2) NOT NULL DEFAULT 1000.0, -- 触发所需最低成交额
    rp_trigger_days       INT           NOT NULL DEFAULT 7,      -- 绑定后有效触发天数
    rp_referrer_amount    INT           NOT NULL DEFAULT 10,     -- 推荐人获得 RP
    rp_referee_amount     INT           NOT NULL DEFAULT 10,     -- 被推荐人获得 RP
    rp_daily_cap_normal   INT           NOT NULL DEFAULT 100,    -- 每日 RP 发放上限

    -- 赛季末分配权重（JSONB，留给 Phase2/3 使用）
    season_weights        JSONB         NOT NULL DEFAULT '{}',

    updated_at            TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_by            TEXT
);

-- 插入全局默认配置（epoch_number = 0），应用启动时若查不到对应 epoch 则回退到此行
INSERT INTO points_config (epoch_number) VALUES (0)
ON CONFLICT (epoch_number) DO NOTHING;

-- =============================================================================
-- 2. earn_level_config
--    L0-L5 的积分阈值与权重，支持后台动态调整。
-- =============================================================================
CREATE TABLE IF NOT EXISTS earn_level_config (
    level       INT         PRIMARY KEY,          -- 0=L0 … 5=L5
    points_min  BIGINT      NOT NULL,             -- 达到此等级所需最低积分（含）
    points_max  BIGINT,                           -- 上一等级上限（NULL = 无上限）
    weight      INT         NOT NULL,             -- Earn 申购配额权重
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by  TEXT
);

-- 写入默认 L0-L5 配置（与 EarnLevel::from_points() 保持一致）
INSERT INTO earn_level_config (level, points_min, points_max, weight) VALUES
    (0,       0,      999,    4),
    (1,    1000,     9999,    8),
    (2,   10000,    49999,   12),
    (3,   50000,   199999,   25),
    (4,  200000,   499999,   60),
    (5,  500000,     NULL,  120)
ON CONFLICT (level) DO NOTHING;

-- =============================================================================
-- 3. rp_trigger_events
--    每个被推荐人（referee_address）只能触发一次 RP，由 UNIQUE 约束保证。
-- =============================================================================
CREATE TABLE IF NOT EXISTS rp_trigger_events (
    id                 UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    referrer_address   TEXT        NOT NULL,
    referee_address    TEXT        NOT NULL UNIQUE,   -- 核心约束：一人只触发一次
    trigger_trade_id   UUID,                          -- 触发该事件的成交 ID
    trigger_volume     DECIMAL(20,8) NOT NULL,        -- 该笔成交量
    referrer_rp        INT         NOT NULL,          -- 推荐人获得的 RP
    referee_rp         INT         NOT NULL,          -- 被推荐人获得的 RP
    status             TEXT        NOT NULL DEFAULT 'triggered',  -- 'triggered' | 'expired'
    epoch_number       INT         NOT NULL,
    triggered_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expired_at         TIMESTAMPTZ,                   -- 该推荐关系的过期时间
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_rp_trigger_events_referrer
    ON rp_trigger_events (referrer_address);
CREATE INDEX IF NOT EXISTS idx_rp_trigger_events_epoch
    ON rp_trigger_events (epoch_number);

-- =============================================================================
-- 4. earn_weight_snapshot
--    每日 UTC 00:00 批量刷新 Earn Level 后写入当日快照，
--    用于 Earn 申购配额计算（用户权重 / 全网总有效权重）。
-- =============================================================================
CREATE TABLE IF NOT EXISTS earn_weight_snapshot (
    snapshot_date          DATE        PRIMARY KEY,
    total_effective_weight BIGINT      NOT NULL DEFAULT 0,  -- 全网总有效权重（∑ count × weight）
    level_breakdown        JSONB       NOT NULL DEFAULT '{}', -- 各等级详情
    calculated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- =============================================================================
-- 5. ALTER user_points_summary — 新增 Phase1 字段
--    使用 ADD COLUMN IF NOT EXISTS（PostgreSQL 9.6+）
-- =============================================================================

-- Earn Level（0-5 映射 L0-L5，默认 L0）
ALTER TABLE user_points_summary
    ADD COLUMN IF NOT EXISTS earn_level        INT          NOT NULL DEFAULT 0;
ALTER TABLE user_points_summary
    ADD COLUMN IF NOT EXISTS earn_level_weight INT          NOT NULL DEFAULT 4;

-- TP 日/周累计用量（UTC 日期变化时由业务层清零）
ALTER TABLE user_points_summary
    ADD COLUMN IF NOT EXISTS tp_daily_used     DECIMAL(20,8) NOT NULL DEFAULT 0;
ALTER TABLE user_points_summary
    ADD COLUMN IF NOT EXISTS tp_weekly_used    DECIMAL(20,8) NOT NULL DEFAULT 0;

-- TP 重置时间戳（用于判断是否跨日/跨周）
ALTER TABLE user_points_summary
    ADD COLUMN IF NOT EXISTS tp_daily_reset_at  TIMESTAMPTZ;
ALTER TABLE user_points_summary
    ADD COLUMN IF NOT EXISTS tp_weekly_reset_at TIMESTAMPTZ;

-- RP 日累计用量（整数积分，日上限 rp_daily_cap_normal）
ALTER TABLE user_points_summary
    ADD COLUMN IF NOT EXISTS rp_daily_used     INT          NOT NULL DEFAULT 0;

-- =============================================================================
-- 6. ALTER points_config —— 补 PP / HP 参数（PRD §3.2 / §3.3）
-- =============================================================================
ALTER TABLE points_config
    ADD COLUMN IF NOT EXISTS pp_amount_rate    DECIMAL(8,4)  NOT NULL DEFAULT 2.5;
ALTER TABLE points_config
    ADD COLUMN IF NOT EXISTS pp_return_cap     DECIMAL(6,4)  NOT NULL DEFAULT 0.20;
ALTER TABLE points_config
    ADD COLUMN IF NOT EXISTS pp_return_coeff   DECIMAL(8,4)  NOT NULL DEFAULT 6.0;
ALTER TABLE points_config
    ADD COLUMN IF NOT EXISTS pp_daily_cap      INT           NOT NULL DEFAULT 20000;
ALTER TABLE points_config
    ADD COLUMN IF NOT EXISTS pp_decay_5min     DECIMAL(4,2)  NOT NULL DEFAULT 0.5;
ALTER TABLE points_config
    ADD COLUMN IF NOT EXISTS pp_decay_10min    DECIMAL(4,2)  NOT NULL DEFAULT 0.25;
ALTER TABLE points_config
    ADD COLUMN IF NOT EXISTS hp_rate_per_min   DECIMAL(12,8) NOT NULL DEFAULT 0.00003;
ALTER TABLE points_config
    ADD COLUMN IF NOT EXISTS hp_daily_cap      INT           NOT NULL DEFAULT 40000;

-- =============================================================================
-- 7. ALTER user_points_summary —— 补 PP / HP 日累计 + 重置时间
-- =============================================================================
ALTER TABLE user_points_summary
    ADD COLUMN IF NOT EXISTS pp_daily_used      DECIMAL(20,8) NOT NULL DEFAULT 0;
ALTER TABLE user_points_summary
    ADD COLUMN IF NOT EXISTS pp_daily_reset_at  TIMESTAMPTZ;
ALTER TABLE user_points_summary
    ADD COLUMN IF NOT EXISTS hp_daily_used      DECIMAL(20,8) NOT NULL DEFAULT 0;
ALTER TABLE user_points_summary
    ADD COLUMN IF NOT EXISTS hp_daily_reset_at  TIMESTAMPTZ;

-- =============================================================================
-- 8. earn_product_config —— Earn 产品容量 + 集中度上限（PRD §6.0）
-- =============================================================================
CREATE TABLE IF NOT EXISTS earn_product_config (
    product_id              UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    product_name            VARCHAR(100) NOT NULL,
    capacity                DECIMAL(20,2) NOT NULL,             -- 总申购容量 C (USDC)
    concentration_cap_pct   DECIMAL(6,4) NOT NULL DEFAULT 0.05, -- 单用户上限 % (5%)
    cross_product_multiplier INT         NOT NULL DEFAULT 3,    -- 跨产品持仓 ≤ 单上限 × 3
    is_active               BOOLEAN      NOT NULL DEFAULT true,
    updated_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_by              TEXT
);

-- 默认占位产品（运营可改）
INSERT INTO earn_product_config (product_name, capacity, concentration_cap_pct, cross_product_multiplier)
SELECT 'USDC Flexible (default)', 10000000, 0.05, 3
WHERE NOT EXISTS (SELECT 1 FROM earn_product_config WHERE product_name = 'USDC Flexible (default)');

-- =============================================================================
-- 9. ALTER trades —— STP 自成交标记（PRD §3.2）
-- =============================================================================
ALTER TABLE trades
    ADD COLUMN IF NOT EXISTS is_self_trade BOOLEAN NOT NULL DEFAULT false;
CREATE INDEX IF NOT EXISTS idx_trades_self_trade
    ON trades(is_self_trade) WHERE is_self_trade = true;

-- =============================================================================
-- 10. MM 积分池（Phase 2，PRD §2.5 / §5.3）
--     mm_program_members   —— 做市商白名单
--     mm_quality_snapshots —— 周期性 4 维度评分
--     mm_points_balance    —— 每 epoch 累计评分（独立于普通用户积分池）
-- =============================================================================
CREATE TABLE IF NOT EXISTS mm_program_members (
    address       VARCHAR(42) PRIMARY KEY,
    label         VARCHAR(100),
    is_active     BOOLEAN     NOT NULL DEFAULT true,
    activated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deactivated_at TIMESTAMPTZ,
    notes         TEXT,
    updated_by    TEXT,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS mm_quality_snapshots (
    id                 UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    mm_address         VARCHAR(42)  NOT NULL,
    symbol             VARCHAR(20)  NOT NULL,
    snapshot_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    -- 4 dimensions (raw measurements)
    maker_volume_usd   DECIMAL(36, 18) NOT NULL DEFAULT 0,  -- since previous snapshot
    spread_bps         DECIMAL(20, 6),                       -- (best_ask - best_bid) / mid * 1e4; NULL if no two-sided quote
    depth_usd          DECIMAL(36, 18) NOT NULL DEFAULT 0,   -- sum of MM open order amount × price
    is_online          BOOLEAN      NOT NULL DEFAULT false,  -- has any open order at snapshot time
    -- composite weighted score (volume 40% / spread 25% / depth 20% / uptime 15%)
    quality_score      DECIMAL(20, 6) NOT NULL DEFAULT 0,
    epoch_number       INT          NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_mm_snap_mm_epoch ON mm_quality_snapshots(mm_address, epoch_number);
CREATE INDEX IF NOT EXISTS idx_mm_snap_time     ON mm_quality_snapshots(snapshot_at DESC);

CREATE TABLE IF NOT EXISTS mm_points_balance (
    id                  UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    mm_address          VARCHAR(42)  NOT NULL,
    epoch_number        INT          NOT NULL,
    quality_score_sum   DECIMAL(36, 18) NOT NULL DEFAULT 0,
    snapshot_count      INT          NOT NULL DEFAULT 0,
    -- Phase 3: filled at season snapshot
    estimated_token_share DECIMAL(36, 18),
    actual_tokens         BIGINT,
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE(mm_address, epoch_number)
);
CREATE INDEX IF NOT EXISTS idx_mm_bal_epoch ON mm_points_balance(epoch_number);

-- =============================================================================
-- 11. Phase 3 — 赛季 + 代币分配
--     points_seasons       —— 赛季元数据（每 4 个 epoch 一个赛季，PRD §5.1）
--     points_distribution  —— 用户/MM 分配快照 + 30 天领取期 (PRD §6.1)
-- =============================================================================
CREATE TABLE IF NOT EXISTS points_seasons (
    season_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    season_no          INT  NOT NULL UNIQUE,
    label              VARCHAR(80) NOT NULL,
    start_epoch        INT  NOT NULL,
    end_epoch          INT  NOT NULL,
    user_pool_tokens   BIGINT NOT NULL,
    mm_pool_tokens     BIGINT NOT NULL,
    -- 'pending' | 'active' | 'snapshot' | 'distributing' | 'completed'
    status             VARCHAR(16) NOT NULL DEFAULT 'pending',
    snapshot_at        TIMESTAMPTZ,         -- snapshot policy: end_at of last epoch
    snapshot_taken_at  TIMESTAMPTZ,
    distribution_at    TIMESTAMPTZ,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Seed 6 seasons per PRD §5.2 (only if table empty).
INSERT INTO points_seasons (season_no, label, start_epoch, end_epoch, user_pool_tokens, mm_pool_tokens, status)
SELECT * FROM (VALUES
    (1, 'Season 1: Epoch 1-4',   1,  4, 35000000::BIGINT, 20000000::BIGINT, 'active'),
    (2, 'Season 2: Epoch 5-8',   5,  8, 25000000::BIGINT, 16000000::BIGINT, 'pending'),
    (3, 'Season 3: Epoch 9-12',  9, 12, 20000000::BIGINT, 13000000::BIGINT, 'pending'),
    (4, 'Season 4: Epoch 13-16',13, 16, 20000000::BIGINT, 13000000::BIGINT, 'pending'),
    (5, 'Season 5: Epoch 17-20',17, 20, 20000000::BIGINT, 13000000::BIGINT, 'pending'),
    (6, 'Season 6: Epoch 21-24',21, 24, 30000000::BIGINT, 25000000::BIGINT, 'pending')
) AS s(season_no, label, start_epoch, end_epoch, user_pool_tokens, mm_pool_tokens, status)
WHERE NOT EXISTS (SELECT 1 FROM points_seasons);

CREATE TABLE IF NOT EXISTS points_distribution (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    season_id       UUID NOT NULL REFERENCES points_seasons(season_id),
    user_address    VARCHAR(42) NOT NULL,
    pool_type       VARCHAR(8)  NOT NULL,                  -- 'user' | 'mm'
    weighted_points DECIMAL(36, 18) NOT NULL DEFAULT 0,
    share_pct       DECIMAL(20, 12) NOT NULL DEFAULT 0,    -- 0.000000000001 .. 1.0
    token_amount    DECIMAL(36, 18) NOT NULL DEFAULT 0,    -- whole tokens, fractional supported
    -- 'pending' | 'claimed' | 'expired'
    claim_status    VARCHAR(12) NOT NULL DEFAULT 'pending',
    claim_deadline  TIMESTAMPTZ NOT NULL,
    claim_nonce     BIGINT      NOT NULL DEFAULT 0,        -- monotonic per (user_address)
    claimed_at      TIMESTAMPTZ,
    claim_tx_hash   VARCHAR(80),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(season_id, user_address, pool_type)
);
CREATE INDEX IF NOT EXISTS idx_dist_user      ON points_distribution(user_address);
CREATE INDEX IF NOT EXISTS idx_dist_season    ON points_distribution(season_id);
CREATE INDEX IF NOT EXISTS idx_dist_status    ON points_distribution(claim_status, claim_deadline);
