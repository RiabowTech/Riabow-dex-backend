#![allow(dead_code)]
//! Risk Management Service

use rust_decimal::Decimal;

use crate::models::{Position, PositionSide};

pub struct RiskService {
    // TODO: Risk parameters
}

impl RiskService {
    pub fn new() -> Self {
        Self {}
    }

    /// Check if position should be liquidated
    /// Uses GMX-style calculation: check remaining collateral vs min requirements
    pub fn should_liquidate(
        position: &Position,
        mark_price: Decimal,
        maintenance_margin_rate: Decimal,
    ) -> bool {
        // Calculate PnL using size_in_tokens for accuracy (GMX-style)
        let unrealized_pnl = match position.side {
            PositionSide::Long => {
                (position.size_in_tokens * mark_price) - position.size_in_usd
            }
            PositionSide::Short => {
                position.size_in_usd - (position.size_in_tokens * mark_price)
            }
        };

        // Remaining collateral = initial collateral + PnL - accumulated fees
        let total_fees = position.accumulated_funding_fee + position.accumulated_borrowing_fee;
        let remaining_collateral = position.collateral_amount + unrealized_pnl - total_fees;

        // Min collateral based on position size
        let min_collateral = position.size_in_usd * maintenance_margin_rate;

        remaining_collateral < min_collateral
    }

    /// Calculate required margin for new order
    pub fn calculate_required_margin(
        size: Decimal,
        price: Decimal,
        leverage: i32,
    ) -> Decimal {
        let notional = size * price;
        notional / Decimal::from(leverage)
    }

    /// Check if user has sufficient margin
    pub fn has_sufficient_margin(
        available_balance: Decimal,
        required_margin: Decimal,
    ) -> bool {
        available_balance >= required_margin
    }

    /// Calculate position value at risk
    pub fn calculate_var(
        position: &Position,
        _mark_price: Decimal,
        volatility: Decimal,
        confidence: Decimal,
    ) -> Decimal {
        // Use size_in_usd directly as it represents the notional value
        position.size_in_usd * volatility * confidence
    }
}
