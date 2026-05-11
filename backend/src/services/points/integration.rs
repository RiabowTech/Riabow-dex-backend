//! Points System Integration
//!
//! Integrates points calculation into existing business flows:
//! - Trade events -> Trading points + Referral points
//! - Position close events -> PnL points
//!
//! All calculations run asynchronously to avoid blocking main business logic.

use crate::services::matching::TradeEvent;
use rust_decimal::Decimal;
use std::sync::Arc;
use tracing::{error, info, Instrument};
use uuid::Uuid;

use super::PointsService;

/// Integration helper for points system
///
/// Provides non-blocking integration points for business events.
pub struct PointsIntegration {
    service: Arc<PointsService>,
}

impl PointsIntegration {
    /// Create new integration instance
    pub fn new(service: Arc<PointsService>) -> Self {
        Self { service }
    }

    /// Handle trade event - calculate trading and referral points
    ///
    /// This is called after a trade is persisted to database.
    /// Spawns async tasks to avoid blocking the trade flow.
    ///
    /// Calculates:
    /// - Trading points for both maker and taker
    /// - Referral points for referrers (if applicable)
    pub fn handle_trade_event(&self, trade: TradeEvent) {
        let service = Arc::clone(&self.service);

        tokio::spawn(async move {
            // Check if points system is enabled
            if !service.is_enabled().await {
                return;
            }

            // PRD §3.2: STP-flagged trades earn no TP / RP.
            if trade.is_self_trade {
                tracing::debug!(
                    "Skipping points for self-trade {} (maker == taker == {})",
                    trade.trade_id, trade.maker_address
                );
                return;
            }

            // Get active epoch
            let epoch = match service.get_active_epoch().await {
                Ok(Some(e)) => e,
                Ok(None) => {
                    // No active epoch, skip points calculation
                    return;
                }
                Err(e) => {
                    error!("Failed to get active epoch for trade {}: {}", trade.trade_id, e);
                    return;
                }
            };

            // Calculate trade volume (price * amount)
            let trade_volume = trade.price * trade.amount;

            use crate::models::points::TradeRole;

            // 1. Maker TP
            if let Err(e) = service.calculate_trading_points(
                &trade.maker_address,
                epoch.epoch_number,
                trade_volume,
                trade.trade_id,
                TradeRole::Maker,
            ).await {
                error!("Failed to calculate TP for maker {}: {}", trade.maker_address, e);
            }

            // 2. Taker TP
            if let Err(e) = service.calculate_trading_points(
                &trade.taker_address,
                epoch.epoch_number,
                trade_volume,
                trade.trade_id,
                TradeRole::Taker,
            ).await {
                error!("Failed to calculate TP for taker {}: {}", trade.taker_address, e);
            }

            // 3. RP 触发检查（Maker）
            if let Err(e) = service.check_and_trigger_rp(
                &trade.maker_address,
                epoch.epoch_number,
                trade_volume,
                trade.trade_id,
            ).await {
                error!("Failed to check_and_trigger_rp for maker {}: {}", trade.maker_address, e);
            }

            // 4. RP 触发检查（Taker）
            if let Err(e) = service.check_and_trigger_rp(
                &trade.taker_address,
                epoch.epoch_number,
                trade_volume,
                trade.trade_id,
            ).await {
                error!("Failed to check_and_trigger_rp for taker {}: {}", trade.taker_address, e);
            }

            info!(
                "Points calculation completed for trade {} (epoch {})",
                trade.trade_id, epoch.epoch_number
            );
        }.instrument(tracing::info_span!("points-trade-handler")));
    }

    /// Handle position close event - calculate PnL points
    ///
    /// This is called after a position is closed.
    /// Spawns async task to avoid blocking.
    pub fn handle_position_close(
        &self,
        user_address: String,
        position_id: Uuid,
        realized_pnl: Decimal,
        collateral_amount: Decimal,
        symbol: String,
    ) {
        let service = Arc::clone(&self.service);

        tokio::spawn(async move {
            if !service.is_enabled().await {
                return;
            }

            let epoch = match service.get_active_epoch().await {
                Ok(Some(e)) => e,
                Ok(None) => return,
                Err(e) => {
                    error!("Failed to get active epoch for position close {}: {}", position_id, e);
                    return;
                }
            };

            // PRD §3.2: 不论盈亏均发放（取 |pnl|）。collateral=0 时跳过。
            if realized_pnl.is_zero() || collateral_amount.is_zero() {
                return;
            }

            if let Err(e) = service
                .calculate_pnl_points(
                    &user_address,
                    epoch.epoch_number,
                    realized_pnl,
                    collateral_amount,
                    &symbol,
                    position_id,
                )
                .await
            {
                error!("Failed to calculate PnL points for position {}: {}", position_id, e);
            } else {
                info!(
                    "PnL points calculated for position {} (user: {}, pnl: {}, collat: {}, sym: {}, epoch: {})",
                    position_id, user_address, realized_pnl, collateral_amount, symbol, epoch.epoch_number
                );
            }
        }.instrument(tracing::info_span!("points-position-close")));
    }
}


// ============================================================================
// Convenience Functions for External Use
// ============================================================================

/// Create integration instance and handle trade event in one call
///
/// Convenience function for use in existing code without holding PointsIntegration.
pub fn handle_trade_event_async(service: Arc<PointsService>, trade: TradeEvent) {
    let integration = PointsIntegration::new(service);
    integration.handle_trade_event(trade);
}

/// Create integration instance and handle position close in one call
///
/// Convenience function for use in existing code without holding PointsIntegration.
pub fn handle_position_close_async(
    service: Arc<PointsService>,
    user_address: String,
    position_id: Uuid,
    realized_pnl: Decimal,
    collateral_amount: Decimal,
    symbol: String,
) {
    let integration = PointsIntegration::new(service);
    integration.handle_position_close(
        user_address,
        position_id,
        realized_pnl,
        collateral_amount,
        symbol,
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trade_volume_calculation() {
        use rust_decimal_macros::dec;

        let price = dec!(50000);
        let amount = dec!(2);
        let volume = price * amount;

        assert_eq!(volume, dec!(100000));
    }

    #[test]
    fn test_positive_pnl() {
        use rust_decimal_macros::dec;

        let pnl = dec!(1000);
        assert!(pnl > Decimal::ZERO);

        let negative_pnl = dec!(-500);
        assert!(negative_pnl <= Decimal::ZERO);
    }
}
