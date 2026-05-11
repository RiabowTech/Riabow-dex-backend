#![allow(dead_code)]
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use std::fmt;
use uuid::Uuid;

// ============================================================================
// Enums
// ============================================================================

/// Epoch状态枚举
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::Type, PartialEq, Eq)]
#[sqlx(type_name = "varchar", rename_all = "lowercase")]
#[serde(rename_all = "lowercase")]
pub enum EpochStatus {
    Pending,  // 待开始
    Active,   // 进行中
    Ended,    // 已结束
    Settled,  // 已结算
}

impl fmt::Display for EpochStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            EpochStatus::Pending => write!(f, "pending"),
            EpochStatus::Active => write!(f, "active"),
            EpochStatus::Ended => write!(f, "ended"),
            EpochStatus::Settled => write!(f, "settled"),
        }
    }
}

/// 交易角色：Maker / Taker（Phase1 TP计算区分）
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TradeRole {
    Maker,
    Taker,
}

impl fmt::Display for TradeRole {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TradeRole::Maker => write!(f, "maker"),
            TradeRole::Taker => write!(f, "taker"),
        }
    }
}

/// Earn Level等级（Phase1，基于原始总积分）
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum EarnLevel {
    L0, // <1,000
    L1, // 1,000–9,999
    L2, // 10,000–49,999
    L3, // 50,000–199,999
    L4, // 200,000–499,999
    L5, // ≥500,000
}

impl EarnLevel {
    /// 根据原始总积分返回对应等级（内置默认阈值，DB可覆盖）
    pub fn from_points(total_points: Decimal) -> Self {
        if total_points >= dec!(500000) {
            EarnLevel::L5
        } else if total_points >= dec!(200000) {
            EarnLevel::L4
        } else if total_points >= dec!(50000) {
            EarnLevel::L3
        } else if total_points >= dec!(10000) {
            EarnLevel::L2
        } else if total_points >= dec!(1000) {
            EarnLevel::L1
        } else {
            EarnLevel::L0
        }
    }

    /// 返回分配权重系数
    pub fn weight(&self) -> u32 {
        match self {
            EarnLevel::L0 => 4,
            EarnLevel::L1 => 8,
            EarnLevel::L2 => 12,
            EarnLevel::L3 => 25,
            EarnLevel::L4 => 60,
            EarnLevel::L5 => 120,
        }
    }

    pub fn as_i32(&self) -> i32 {
        match self {
            EarnLevel::L0 => 0,
            EarnLevel::L1 => 1,
            EarnLevel::L2 => 2,
            EarnLevel::L3 => 3,
            EarnLevel::L4 => 4,
            EarnLevel::L5 => 5,
        }
    }

    /// 距下一等级还差多少积分（L5返回0）
    pub fn points_to_next(&self, current_points: Decimal) -> Decimal {
        let next_threshold = match self {
            EarnLevel::L0 => dec!(1000),
            EarnLevel::L1 => dec!(10000),
            EarnLevel::L2 => dec!(50000),
            EarnLevel::L3 => dec!(200000),
            EarnLevel::L4 => dec!(500000),
            EarnLevel::L5 => return Decimal::ZERO,
        };
        (next_threshold - current_points).max(Decimal::ZERO)
    }
}

/// 积分类型枚举
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::Type, PartialEq, Eq, Hash)]
#[sqlx(type_name = "varchar", rename_all = "lowercase")]
#[serde(rename_all = "lowercase")]
pub enum PointType {
    Trading,   // 交易积分
    Pnl,       // PnL积分
    Holding,   // 持仓积分
    Referral,  // 推荐积分
    Staking,   // 质押积分
}

impl fmt::Display for PointType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PointType::Trading => write!(f, "trading"),
            PointType::Pnl => write!(f, "pnl"),
            PointType::Holding => write!(f, "holding"),
            PointType::Referral => write!(f, "referral"),
            PointType::Staking => write!(f, "staking"),
        }
    }
}

/// 排行榜类型枚举
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::Type, PartialEq, Eq)]
#[sqlx(type_name = "varchar", rename_all = "lowercase")]
#[serde(rename_all = "lowercase")]
pub enum LeaderboardType {
    Total,     // 总积分
    Trading,   // 交易积分
    Pnl,       // PnL积分
    Holding,   // 持仓积分
    Referral,  // 推荐积分
    Staking,   // 质押积分
}

impl fmt::Display for LeaderboardType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            LeaderboardType::Total => write!(f, "total"),
            LeaderboardType::Trading => write!(f, "trading"),
            LeaderboardType::Pnl => write!(f, "pnl"),
            LeaderboardType::Holding => write!(f, "holding"),
            LeaderboardType::Referral => write!(f, "referral"),
            LeaderboardType::Staking => write!(f, "staking"),
        }
    }
}

/// 质押状态枚举
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::Type, PartialEq, Eq)]
#[sqlx(type_name = "varchar", rename_all = "lowercase")]
#[serde(rename_all = "lowercase")]
pub enum StakingStatus {
    Active,     // 质押中
    Withdrawn,  // 已提取
}

impl fmt::Display for StakingStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            StakingStatus::Active => write!(f, "active"),
            StakingStatus::Withdrawn => write!(f, "withdrawn"),
        }
    }
}

// ============================================================================
// Database Models
// ============================================================================

/// Epoch配置信息
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct EpochInfo {
    pub id: Uuid,
    pub epoch_number: i32,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub duration_days: i32,
    pub status: EpochStatus,
    pub config: Option<serde_json::Value>, // JSONB配置
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// 用户积分汇总
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct UserPointsSummary {
    pub id: Uuid,
    pub user_address: String,
    pub epoch_number: i32,

    // 5种积分类型
    pub trading_points: Decimal,
    pub pnl_points: Decimal,
    pub holding_points: Decimal,
    pub referral_points: Decimal,
    pub referral_code: Option<String>,
    pub staking_points: Decimal,

    // 汇总
    pub total_points: Decimal,

    // 交易统计
    pub trading_volume: Decimal,
    pub trade_count: i32,
    pub realized_pnl: Decimal,

    // Tier信息
    pub tier: Option<String>,
    pub tier_multiplier: Option<Decimal>,

    // 推荐统计
    pub referral_count: i32,
    pub referral_volume: Decimal,

    // Phase1: Earn Level
    pub earn_level: i32,
    pub earn_level_weight: i32,

    // Phase1: TP 日/周上限计数
    pub tp_daily_used: Decimal,
    pub tp_weekly_used: Decimal,
    pub rp_daily_used: i32,
    pub tp_daily_reset_at: Option<DateTime<Utc>>,
    pub tp_weekly_reset_at: Option<DateTime<Utc>>,

    // Phase1: PP / HP 日上限计数
    #[serde(default)]
    pub pp_daily_used: Decimal,
    #[serde(default)]
    pub pp_daily_reset_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub hp_daily_used: Decimal,
    #[serde(default)]
    pub hp_daily_reset_at: Option<DateTime<Utc>>,

    pub updated_at: DateTime<Utc>,
}

/// 积分事件明细
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct PointsEvent {
    pub id: Uuid,
    pub user_address: String,
    pub epoch_number: i32,
    pub point_type: PointType,
    pub points: Decimal,

    // 关联数据
    pub related_trade_id: Option<Uuid>,
    pub related_order_id: Option<Uuid>,
    pub related_position_id: Option<Uuid>,
    pub referrer_address: Option<String>,

    // 元数据
    pub metadata: Option<serde_json::Value>, // JSONB元数据

    pub created_at: DateTime<Utc>,
}

/// 交易量Tier配置
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct TierConfig {
    pub id: Uuid,
    pub tier_name: String,
    pub min_volume: Decimal,
    pub max_volume: Option<Decimal>,
    pub multiplier: Decimal,
    pub epoch_number: Option<i32>, // NULL表示全局默认配置
    pub is_active: bool,
    pub created_at: DateTime<Utc>,
}

/// 质押记录
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct StakingRecord {
    pub id: Uuid,
    pub user_address: String,
    pub amount: Decimal,
    pub token_address: String,

    pub start_time: DateTime<Utc>,
    pub end_time: Option<DateTime<Utc>>,
    pub status: StakingStatus,

    pub tx_hash: Option<String>,
    pub withdraw_tx_hash: Option<String>,

    pub last_calculated_at: DateTime<Utc>, // 上次计算积分时间

    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// 排行榜条目
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct LeaderboardEntry {
    pub id: Uuid,
    pub epoch_number: i32,
    pub rank_type: LeaderboardType,

    pub user_address: String,
    pub rank: i32,
    pub points: Decimal,

    // 额外信息
    pub username: Option<String>,
    pub tier: Option<String>,

    pub updated_at: DateTime<Utc>,
}

/// 管理员操作日志
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct AdminLog {
    pub id: Uuid,
    pub admin_address: String,
    pub action: String,

    pub target_user: Option<String>,
    pub target_epoch: Option<i32>,

    pub details: Option<serde_json::Value>, // JSONB详情

    pub created_at: DateTime<Utc>,
}

/// Phase1: 赛季积分参数配置（来自 points_config 表）
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct PointsConfigRow {
    pub id: Uuid,
    pub epoch_number: i32,

    // TP 系数（per 1000U）
    pub tp_t1_maker: Decimal,
    pub tp_t1_taker: Decimal,
    pub tp_t2_maker: Decimal,
    pub tp_t2_taker: Decimal,
    pub tp_t3_maker: Decimal,
    pub tp_t3_taker: Decimal,
    pub tp_daily_cap: i32,
    pub tp_weekly_cap: i32,

    // Tier 交易量阈值（14日滚动，USD）
    pub tier_t2_min: Decimal,
    pub tier_t3_min: Decimal,

    // RP 触发配置
    pub rp_trigger_min_volume: Decimal,
    pub rp_trigger_days: i32,
    pub rp_referrer_amount: i32,
    pub rp_referee_amount: i32,
    pub rp_daily_cap_normal: i32,

    // 赛季末分配权重（JSONB）
    pub season_weights: serde_json::Value,

    // PP / HP 参数（PRD §3.2 / §3.3）
    #[serde(default = "PointsConfigRow::default_pp_amount_rate")]
    pub pp_amount_rate: Decimal,
    #[serde(default = "PointsConfigRow::default_pp_return_cap")]
    pub pp_return_cap: Decimal,
    #[serde(default = "PointsConfigRow::default_pp_return_coeff")]
    pub pp_return_coeff: Decimal,
    #[serde(default = "PointsConfigRow::default_pp_daily_cap")]
    pub pp_daily_cap: i32,
    #[serde(default = "PointsConfigRow::default_pp_decay_5min")]
    pub pp_decay_5min: Decimal,
    #[serde(default = "PointsConfigRow::default_pp_decay_10min")]
    pub pp_decay_10min: Decimal,
    #[serde(default = "PointsConfigRow::default_hp_rate_per_min")]
    pub hp_rate_per_min: Decimal,
    #[serde(default = "PointsConfigRow::default_hp_daily_cap")]
    pub hp_daily_cap: i32,

    pub updated_at: DateTime<Utc>,
    pub updated_by: Option<String>,
}

impl PointsConfigRow {
    pub fn default_pp_amount_rate() -> Decimal { Decimal::new(25, 1) }   // 2.5
    pub fn default_pp_return_cap() -> Decimal { Decimal::new(20, 2) }    // 0.20
    pub fn default_pp_return_coeff() -> Decimal { Decimal::new(60, 1) }  // 6.0
    pub fn default_pp_daily_cap() -> i32 { 20000 }
    pub fn default_pp_decay_5min() -> Decimal { Decimal::new(5, 1) }     // 0.5
    pub fn default_pp_decay_10min() -> Decimal { Decimal::new(25, 2) }   // 0.25
    pub fn default_hp_rate_per_min() -> Decimal { Decimal::new(3, 5) }   // 0.00003
    pub fn default_hp_daily_cap() -> i32 { 40000 }
}

/// Phase1: Earn Level 配置（来自 earn_level_config 表）
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct EarnLevelConfig {
    pub level: i32,
    pub points_min: i64,
    pub points_max: Option<i64>,
    pub weight: i32,
    pub updated_at: DateTime<Utc>,
    pub updated_by: Option<String>,
}

/// Phase1: RP 触发事件记录（来自 rp_trigger_events 表）
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct RpTriggerEvent {
    pub id: Uuid,
    pub referrer_address: String,
    pub referee_address: String,
    pub trigger_trade_id: Option<Uuid>,
    pub trigger_volume: Decimal,
    pub referrer_rp: i32,
    pub referee_rp: i32,
    pub status: String, // "triggered" | "expired"
    pub epoch_number: i32,
    pub triggered_at: DateTime<Utc>,
    pub expired_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

// ============================================================================
// API Request/Response Models
// ============================================================================

/// 创建Epoch请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateEpochRequest {
    pub epoch_number: i32,
    pub start_time: DateTime<Utc>,
    pub duration_days: i32,
    pub config: Option<serde_json::Value>,
}

/// 更新Epoch状态请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateEpochStatusRequest {
    pub status: EpochStatus,
}

/// 调整用户积分请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdjustPointsRequest {
    pub user_address: String,
    pub epoch_number: i32,
    pub point_type: PointType,
    pub points: Decimal,
    pub reason: String,
}

/// Tier配置请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierConfigRequest {
    pub epoch_number: Option<i32>, // null=全局默认
    pub tiers: Vec<TierConfigItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierConfigItem {
    pub tier_name: String,
    pub min_volume: Decimal,
    pub max_volume: Option<Decimal>,
    pub multiplier: Decimal,
}

/// 用户积分查询响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserPointsResponse {
    pub user_address: String,
    pub epoch_number: i32,
    pub epoch_status: EpochStatus,

    // 积分明细
    pub trading_points: Decimal,
    pub pnl_points: Decimal,
    pub holding_points: Decimal,
    pub referral_points: Decimal,
    pub referral_code: Option<String>,
    pub staking_points: Decimal,
    pub total_points: Decimal,

    // Tier信息
    pub tier: Option<String>,
    pub tier_multiplier: Option<Decimal>,

    // Phase1: Earn Level
    pub earn_level: i32,
    pub earn_level_weight: i32,
    pub earn_level_points_to_next: Decimal,

    // 排名信息
    pub rank: Option<i32>,

    // 统计信息
    pub trading_volume: Decimal,
    pub trade_count: i32,
    pub referral_count: i32,

    pub updated_at: DateTime<Utc>,
}

/// 积分历史查询响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PointsHistoryResponse {
    pub events: Vec<PointsEventDetail>,
    pub total: i64,
    pub page: i32,
    pub page_size: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PointsEventDetail {
    pub id: Uuid,
    pub point_type: PointType,
    pub points: Decimal,
    pub metadata: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
}

impl From<PointsEvent> for PointsEventDetail {
    fn from(event: PointsEvent) -> Self {
        Self {
            id: event.id,
            point_type: event.point_type,
            points: event.points,
            metadata: event.metadata,
            created_at: event.created_at,
        }
    }
}

/// 排行榜查询响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaderboardResponse {
    pub epoch_number: i32,
    pub rank_type: LeaderboardType,
    pub entries: Vec<LeaderboardEntryDetail>,
    pub total: i64,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaderboardEntryDetail {
    pub rank: i32,
    pub user_address: String,
    pub username: Option<String>,
    pub points: Decimal,
    pub tier: Option<String>,
}

impl From<LeaderboardEntry> for LeaderboardEntryDetail {
    fn from(entry: LeaderboardEntry) -> Self {
        Self {
            rank: entry.rank,
            user_address: entry.user_address,
            username: entry.username,
            points: entry.points,
            tier: entry.tier,
        }
    }
}

/// Daily increment leaderboard entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DailyLeaderboardEntry {
    pub rank: i32,
    pub user_address: String,
    pub points_today: Decimal,
    pub tier: Option<String>,
}

/// Daily increment leaderboard response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DailyLeaderboardResponse {
    pub date: String,
    pub epoch_number: i32,
    pub refreshed_at: DateTime<Utc>,
    pub total: i64,
    pub entries: Vec<DailyLeaderboardEntry>,
}

/// Tier信息响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierInfoResponse {
    pub tier: String,
    pub multiplier: Decimal,
    pub min_volume: Decimal,
    pub max_volume: Option<Decimal>,
    pub current_volume: Decimal,
    pub next_tier: Option<NextTierInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NextTierInfo {
    pub tier: String,
    pub multiplier: Decimal,
    pub required_volume: Decimal,
    pub remaining_volume: Decimal,
}

/// Epoch统计信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpochStats {
    pub epoch_number: i32,
    pub status: EpochStatus,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,

    // 统计数据
    pub total_users: i64,
    pub total_points: Decimal,
    pub total_trading_volume: Decimal,
    pub total_trade_count: i64,

    // 各类型积分统计
    pub trading_points_total: Decimal,
    pub pnl_points_total: Decimal,
    pub holding_points_total: Decimal,
    pub referral_points_total: Decimal,
    pub staking_points_total: Decimal,
}

/// 质押记录查询响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StakingRecordsResponse {
    pub records: Vec<StakingRecordDetail>,
    pub total: i64,
    pub total_staked: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StakingRecordDetail {
    pub id: Uuid,
    pub amount: Decimal,
    pub token_address: String,
    pub start_time: DateTime<Utc>,
    pub end_time: Option<DateTime<Utc>>,
    pub status: StakingStatus,
    pub tx_hash: Option<String>,
}

impl From<StakingRecord> for StakingRecordDetail {
    fn from(record: StakingRecord) -> Self {
        Self {
            id: record.id,
            amount: record.amount,
            token_address: record.token_address,
            start_time: record.start_time,
            end_time: record.end_time,
            status: record.status,
            tx_hash: record.tx_hash,
        }
    }
}

// ============================================================================
// Helper Structs
// ============================================================================

/// 积分计算结果
#[derive(Debug, Clone)]
pub struct PointsCalculationResult {
    pub points: Decimal,
    pub point_type: PointType,
    pub tier: Option<String>,
    pub multiplier: Decimal,
    pub metadata: serde_json::Value,
}

/// Tier计算结果（Phase1重写：携带Maker/Taker费率）
#[derive(Debug, Clone)]
pub struct TierCalculationResult {
    pub tier: String,
    pub multiplier: Decimal,     // 保留，兼容旧代码（取 maker_rate）
    pub maker_rate: Decimal,     // per 1000U Maker系数
    pub taker_rate: Decimal,     // per 1000U Taker系数
    pub min_volume: Decimal,
    pub max_volume: Option<Decimal>,
}

/// Phase1: 积分预估请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulatePointsRequest {
    pub trade_amount: Decimal,
    /// "maker" or "taker"
    pub order_type: String,
    /// 1/2/3, 若不传则按用户当前 Tier 计算
    pub tier: Option<i32>,
}

/// Phase1: 积分预估响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulatePointsResponse {
    pub tp_estimate: Decimal,       // 本笔交易 raw 积分（未封顶）
    pub tp_effective: Decimal,      // 实际可获得积分（封顶后）
    pub hp_estimate: Decimal,       // Phase2 占位，当前恒为 0
    pub total_estimate: Decimal,    // tp_effective + hp_estimate
    pub tier: String,
    pub role_rate: Decimal,
    pub daily_cap_status: CapInfo,
    pub weekly_cap_status: CapInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapInfo {
    pub used: Decimal,
    pub cap: i32,
    pub remaining: Decimal,
}

/// 批量积分更新记录
#[derive(Debug, Clone)]
pub struct BatchPointsUpdate {
    pub user_address: String,
    pub epoch_number: i32,
    pub point_type: PointType,
    pub points_delta: Decimal,
    pub metadata: serde_json::Value,
}

// ============================================================================
// Constants
// ============================================================================

/// 默认积分费率
pub mod default_rates {
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    pub const TRADING_RATE: Decimal = dec!(0.0001);  // 0.01% of volume
    pub const PNL_RATE: Decimal = dec!(0.001);       // 0.1% of PnL
    pub const HOLDING_RATE: Decimal = dec!(0.00001); // Per $1 per hour
    pub const REFERRAL_RATE: Decimal = dec!(0.00005); // 0.005% of referee volume
    pub const STAKING_RATE: Decimal = dec!(0.0002);  // 0.02% per day
}

/// 默认Tier倍数
pub mod default_tiers {
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    pub const T1_MULTIPLIER: Decimal = dec!(1.0);
    pub const T2_MULTIPLIER: Decimal = dec!(1.1);
    pub const T3_MULTIPLIER: Decimal = dec!(1.3);
    pub const T4_MULTIPLIER: Decimal = dec!(1.5);

    pub const T1_MIN: Decimal = dec!(0);
    pub const T1_MAX: Decimal = dec!(99999.99);

    pub const T2_MIN: Decimal = dec!(100000);
    pub const T2_MAX: Decimal = dec!(499999.99);

    pub const T3_MIN: Decimal = dec!(500000);
    pub const T3_MAX: Decimal = dec!(999999.99);

    pub const T4_MIN: Decimal = dec!(1000000);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_epoch_status_display() {
        assert_eq!(EpochStatus::Pending.to_string(), "pending");
        assert_eq!(EpochStatus::Active.to_string(), "active");
        assert_eq!(EpochStatus::Ended.to_string(), "ended");
        assert_eq!(EpochStatus::Settled.to_string(), "settled");
    }

    #[test]
    fn test_point_type_display() {
        assert_eq!(PointType::Trading.to_string(), "trading");
        assert_eq!(PointType::Pnl.to_string(), "pnl");
        assert_eq!(PointType::Holding.to_string(), "holding");
        assert_eq!(PointType::Referral.to_string(), "referral");
        assert_eq!(PointType::Staking.to_string(), "staking");
    }

    #[test]
    fn test_leaderboard_type_display() {
        assert_eq!(LeaderboardType::Total.to_string(), "total");
        assert_eq!(LeaderboardType::Trading.to_string(), "trading");
    }
}
