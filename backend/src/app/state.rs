//! Application state definition
//!
//! Contains the shared state passed to all handlers.

use std::sync::Arc;
use tokio::sync::broadcast;
use serde::Serialize;

use crate::cache::CacheManager;
use crate::config::AppConfig;
use crate::db::Database;
use crate::models::order::OrderResponse;
use crate::services::adl::AdlService;
use crate::services::earn::EarnService;
use crate::services::funding_rate::FundingRateService;
use crate::services::kline::KlineService;
use crate::services::liquidation::LiquidationService;
use crate::services::matching::MatchingEngine;
use crate::services::position::PositionService;
use crate::services::price_feed::PriceFeedService;
use crate::services::referral::ReferralService;
use crate::services::trigger_orders::TriggerOrdersService;
use crate::services::withdraw::WithdrawService;
use crate::services::points::PointsService;
use crate::services::market_config::MarketConfigService;

/// Order update event for real-time WebSocket push
#[derive(Debug, Clone, Serialize)]
pub struct OrderUpdateEvent {
    pub user_address: String,
    pub order: OrderResponse,
}

/// Balance update event — emitted by the Postgres LISTEN/NOTIFY task
/// (`notify_balance_change` trigger → `balance_change` channel, see
/// migration 20260423100000_balance_change_notify.sql) whenever
/// `balances` is INSERTed or UPDATEd. Consumed by the WebSocket
/// handler for the `balances` channel so bots can drop REST polling.
///
/// Decimal fields arrive as strings from the trigger (json_build_object
/// with ::text cast) so the receiving side can parse into rust_decimal
/// without going through f64.
#[derive(Debug, Clone, serde::Deserialize, Serialize)]
pub struct BalanceUpdateEvent {
    pub user_address: String,
    pub token: String,
    pub available: String,
    pub frozen: String,
}

/// Unified Margin account event — emitted by the risk worker when a
/// user's uniMMR or status changes. Consumed by the WebSocket handler
/// for private push on the `unified_account` channel.
#[derive(Debug, Clone, Serialize)]
pub struct UnifiedAccountEvent {
    pub user_address: String,
    /// Event kind: "update" | "status_change" | "margin_call" | "reduce_only"
    pub event: String,
    pub uni_mmr: Option<rust_decimal::Decimal>,
    pub total_equity: rust_decimal::Decimal,
    pub available_balance: rust_decimal::Decimal,
    pub account_status: String,
    /// Optional human-readable reason (for status_change / margin_call).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Orders cancelled by the reduce_only enforcement (0 for other events).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub orders_cancelled: Option<i64>,
    pub timestamp: i64,
}

/// Points event — emitted on every TP/PP/HP/RP credit and on cap-hit
/// or season-tick milestones. Consumed by the WebSocket handler for
/// private push on the `points` channel (PRD §9.4).
#[derive(Debug, Clone, Serialize)]
pub struct PointsEventPush {
    pub user_address: String,
    /// "tp_earned" | "pp_earned" | "hp_batch" | "rp_triggered"
    /// | "cap_reached" | "season.countdown" | "earn.level_up"
    pub event: String,
    /// `tp` / `pp` / `hp` / `rp` — present for *_earned events.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub point_type: Option<String>,
    /// Points amount credited in this event.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub amount: Option<rust_decimal::Decimal>,
    /// Daily/weekly cap status when relevant (cap_reached event).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cap_kind: Option<String>,
    /// Free-form reason (e.g. "5min_same_symbol_decay", "tp_daily_cap_5000").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub timestamp: i64,
}

/// Main application state shared across all handlers
pub struct AppState {
    pub config: AppConfig,
    pub db: Database,
    pub cache: Arc<CacheManager>,
    pub matching_engine: Arc<MatchingEngine>,
    /// Spot trading matching engine handle. `None` when SPOT_TRADING_ENABLED=false.
    pub spot_engine: Option<std::sync::Arc<crate::services::spot::matching::types::EngineHandle>>,
    /// Spot vault chain client. `None` when SPOT_ENABLED=false. Withdraw
    /// handler reads `releaseNonces[user]` through this so signed nonces
    /// stay aligned with the on-chain counter.
    pub spot_blockchain: Option<std::sync::Arc<crate::services::spot::blockchain::SpotBlockchainService>>,
    pub withdraw_service: Arc<WithdrawService>,
    pub price_feed_service: Arc<PriceFeedService>,
    pub position_service: Arc<PositionService>,
    pub funding_rate_service: Arc<FundingRateService>,
    pub liquidation_service: Arc<LiquidationService>,
    pub adl_service: Arc<AdlService>,
    pub trigger_orders_service: Arc<TriggerOrdersService>,
    pub referral_service: Arc<ReferralService>,
    pub kline_service: Arc<KlineService>,
    pub earn_service: Arc<EarnService>,
    pub points_service: Arc<PointsService>,
    pub market_config_service: Arc<MarketConfigService>,
    pub order_update_sender: broadcast::Sender<OrderUpdateEvent>,
    pub balance_update_sender: broadcast::Sender<BalanceUpdateEvent>,
    // Spot WS broadcast senders. Public channels (depth/trade/ticker/kline)
    // are populated by ws_publisher draining the spot EngineEvent feed; the
    // private senders mirror the perp `order_update_sender` /
    // `balance_update_sender` pattern but carry spot-specific payloads
    // (see services/spot/ws_messages.rs).
    pub spot_depth_sender:         tokio::sync::broadcast::Sender<crate::services::spot::ws_messages::SpotDepthDiff>,
    pub spot_trade_sender:         tokio::sync::broadcast::Sender<crate::services::spot::ws_messages::SpotTradePush>,
    pub spot_ticker_sender:        tokio::sync::broadcast::Sender<crate::services::spot::ws_messages::SpotTickerPush>,
    pub spot_kline_sender:         tokio::sync::broadcast::Sender<crate::services::spot::ws_messages::SpotKlinePush>,
    pub spot_user_order_sender:    tokio::sync::broadcast::Sender<crate::services::spot::ws_messages::SpotUserOrderPush>,
    pub spot_user_balance_sender:  tokio::sync::broadcast::Sender<crate::services::spot::ws_messages::SpotUserBalancePush>,
    pub unified_account_sender: broadcast::Sender<UnifiedAccountEvent>,
    pub points_event_sender: broadcast::Sender<PointsEventPush>,
    pub vip_tier_event_sender: broadcast::Sender<crate::services::vip_tier::VipTierEvent>,
    pub margin_tiers: crate::services::unified_margin::TierStoreHandle,
    /// Symbol-sharded routing config. When `enabled = false` (default
    /// during rollout) all handlers behave identically to current
    /// single-replica semantics — see `services/sharding/mod.rs`.
    pub sharding: crate::services::sharding::ShardingConfig,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The Postgres trigger (notify_balance_change) emits JSON like:
    ///   {"user_address":"0xabc...","token":"USDT","available":"123.45","frozen":"0"}
    /// This roundtrip test locks in that the Rust decoder accepts exactly
    /// what the trigger emits, including Decimal-as-text fields.
    #[test]
    fn test_balance_update_event_deserialize_matches_trigger_payload() {
        let raw = r#"{"user_address":"0x3f7ba051dca60041ebe705794bc7667bf468d800","token":"USDT","available":"45.00","frozen":"0.00"}"#;
        let ev: BalanceUpdateEvent = serde_json::from_str(raw).unwrap();
        assert_eq!(ev.user_address, "0x3f7ba051dca60041ebe705794bc7667bf468d800");
        assert_eq!(ev.token, "USDT");
        assert_eq!(ev.available, "45.00");
        assert_eq!(ev.frozen, "0.00");
    }

    #[test]
    fn test_balance_update_event_serialize_roundtrip() {
        let ev = BalanceUpdateEvent {
            user_address: "0xabcabcabcabcabcabcabcabcabcabcabcabcabca".to_string(),
            token: "USDT".to_string(),
            available: "123456.78".to_string(),
            frozen: "9.01".to_string(),
        };
        let s = serde_json::to_string(&ev).unwrap();
        let back: BalanceUpdateEvent = serde_json::from_str(&s).unwrap();
        assert_eq!(ev.user_address, back.user_address);
        assert_eq!(ev.token, back.token);
        assert_eq!(ev.available, back.available);
        assert_eq!(ev.frozen, back.frozen);
    }
}
