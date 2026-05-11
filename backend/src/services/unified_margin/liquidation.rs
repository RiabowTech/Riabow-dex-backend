//! Unified-margin forced liquidation engine.
//!
//! When the risk worker observes a unified account in the `Liquidating`
//! state we close the single worst-PnL position via the shared
//! [`LiquidationService::execute_liquidation`]. That path already
//! handles:
//!
//!   * locking + status flip of the position
//!   * inserting a row into the cross-mode `liquidations` table
//!   * charging the liquidation fee and insurance-fund contribution
//!   * covering bad debt from the per-symbol insurance fund (capped by
//!     `max_insurance_payout_rate`)
//!   * releasing frozen collateral + buffer + opening fee, returning
//!     the remainder to the user's available balance
//!   * sending the liquidation email
//!
//! On top of that common path we additionally:
//!   * record a unified-specific row in `unified_liquidation_records`
//!     (captures the triggering uniMMR / equity for forensics)
//!   * look up the residual shortfall (if any) that the insurance fund
//!     could NOT cover, and invoke [`AdlService::execute_adl`] to
//!     reduce opposite-side profitable positions
//!
//! Out of scope: user-wide cross-symbol netting, platform-level bad
//! debt socialization beyond ADL.

use rust_decimal::Decimal;
use std::sync::Arc;
use uuid::Uuid;

use crate::app::state::AppState;
use crate::models::position::{Position, PositionSide, PositionStatus};
use crate::models::unified_margin::UnifiedRiskSnapshot;

pub struct LiquidationStep {
    pub position_id: Uuid,
    pub symbol: String,
    pub side: PositionSide,
    pub closed_size_usd: Decimal,
    pub closed_size_tokens: Decimal,
    pub mark_price: Decimal,
    pub pnl_realized: Decimal,
    pub collateral_returned: Decimal,
    pub insurance_shortfall: Decimal,
    pub adl_triggered: bool,
}

/// Close the single worst-PnL position for `user_address`.
pub async fn liquidate_one(
    state: &Arc<AppState>,
    user_address: &str,
    snapshot: &UnifiedRiskSnapshot,
) -> anyhow::Result<Option<LiquidationStep>> {
    let positions: Vec<Position> = sqlx::query_as::<_, Position>(
        "SELECT * FROM positions WHERE user_address = $1 AND status = $2",
    )
    .bind(user_address)
    .bind(PositionStatus::Open)
    .fetch_all(&state.db.pool)
    .await?;

    if positions.is_empty() {
        return Ok(None);
    }

    let mut ranked: Vec<(Position, Decimal, Decimal)> = Vec::with_capacity(positions.len());
    for p in positions {
        let mark = state
            .price_feed_service
            .get_mark_price(&p.symbol)
            .await
            .unwrap_or(p.entry_price);
        let pnl = match p.side {
            PositionSide::Long => (p.size_in_tokens * mark) - p.size_in_usd,
            PositionSide::Short => p.size_in_usd - (p.size_in_tokens * mark),
        };
        ranked.push((p, mark, pnl));
    }
    ranked.sort_by(|a, b| a.2.cmp(&b.2)); // ascending: worst first

    let (position, mark_price, _pnl) = ranked.into_iter().next().unwrap();
    let pos_id = position.id;
    let symbol = position.symbol.clone();
    let side = position.side;
    let closed_size_usd = position.size_in_usd;
    let closed_size_tokens = position.size_in_tokens;
    let user = user_address.to_string();

    // Delegate to the shared LiquidationService. It runs in its own tx,
    // handles insurance-fund payout, position state, balance updates,
    // and email alerts.
    let result = state
        .liquidation_service
        .execute_liquidation(&position, mark_price, None)
        .await
        .map_err(|e| anyhow::anyhow!("execute_liquidation failed: {}", e))?;

    // Fetch the `liquidations.id` that was just written so we can wire
    // ADL (and our own record) to it.
    let liquidation_id: Option<Uuid> = sqlx::query_scalar(
        "SELECT id FROM liquidations \
         WHERE position_id = $1 \
         ORDER BY liquidated_at DESC LIMIT 1",
    )
    .bind(pos_id)
    .fetch_optional(&state.db.pool)
    .await
    .unwrap_or(None);

    // Compute insurance-fund shortfall. `remaining_collateral` is
    // already consumed inside execute_liquidation; we derive the gap
    // from (collateral_returned, fees, collateral_amount, pnl).
    // If collateral_returned == 0 and pnl < -collateral, there's likely
    // bad debt. We compute it directly from the same formula:
    //   remaining = collateral + pnl - accumulated_fees
    let accumulated_fees =
        position.accumulated_funding_fee + position.accumulated_borrowing_fee;
    let remaining_collateral =
        position.collateral_amount + result.pnl - accumulated_fees;
    let total_fees_paid =
        result.liquidation_fee + result.insurance_fund_contribution;
    let net_after_fees = remaining_collateral - total_fees_paid;

    // If net_after_fees < 0 then the insurance fund covered up to its
    // cap. The residual that ADL must cover is max(0, shortfall - cap).
    // execute_liquidation caps insurance_payout at
    // `fund.balance * max_insurance_payout_rate`, so fetching that
    // quantity precisely requires reading insurance_fund_transactions.
    let mut shortfall = if net_after_fees < Decimal::ZERO {
        net_after_fees.abs()
    } else {
        Decimal::ZERO
    };

    if shortfall > Decimal::ZERO {
        if let Some(lid) = liquidation_id {
            let insurance_payout: Option<Decimal> = sqlx::query_scalar(
                "SELECT amount FROM insurance_fund_transactions \
                 WHERE liquidation_id = $1 AND transaction_type = 'payout' \
                 LIMIT 1",
            )
            .bind(lid)
            .fetch_optional(&state.db.pool)
            .await
            .unwrap_or(None);
            if let Some(paid) = insurance_payout {
                shortfall = (shortfall - paid).max(Decimal::ZERO);
            }
        }
    }

    // Trigger ADL if insurance fund left a gap. ADL reduces positions
    // on the OPPOSITE side of the liquidated one (they're the winners).
    let mut adl_triggered = false;
    if shortfall > Decimal::ZERO {
        if let Some(lid) = liquidation_id {
            let opposite = match side {
                PositionSide::Long => "short",
                PositionSide::Short => "long",
            };
            match state
                .adl_service
                .execute_adl(&symbol, lid, shortfall, opposite)
                .await
            {
                Ok(_evt) => {
                    adl_triggered = true;
                    tracing::warn!(
                        "ADL triggered: symbol={} liquidation={} shortfall={}",
                        symbol, lid, shortfall
                    );
                }
                Err(e) => {
                    tracing::error!(
                        "ADL dispatch failed: symbol={} shortfall={} err={}",
                        symbol, shortfall, e
                    );
                }
            }
        }
    }

    // Unified-margin forensic record.
    sqlx::query(
        "INSERT INTO unified_liquidation_records \
            (user_address, position_id, symbol, side, closed_size_usd, \
             closed_size_tokens, mark_price, pnl_realized, collateral_returned, \
             trigger_uni_mmr, trigger_equity, liquidation_type) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
    )
    .bind(&user)
    .bind(pos_id)
    .bind(&symbol)
    .bind(side.to_string())
    .bind(closed_size_usd)
    .bind(closed_size_tokens)
    .bind(mark_price)
    .bind(result.pnl)
    .bind(result.collateral_returned)
    .bind(snapshot.uni_mmr)
    .bind(snapshot.total_equity)
    .bind(if adl_triggered { "adl" } else { "full" })
    .execute(&state.db.pool)
    .await?;

    // Metrics.
    crate::services::metrics::UNIFIED_LIQUIDATION_STEPS_TOTAL
        .with_label_values(&[&symbol, if adl_triggered { "1" } else { "0" }])
        .inc();
    if shortfall > Decimal::ZERO {
        use rust_decimal::prelude::ToPrimitive;
        if let Some(v) = shortfall.to_f64() {
            crate::services::metrics::UNIFIED_INSURANCE_SHORTFALL_USD_TOTAL
                .with_label_values(&[&symbol])
                .inc_by(v);
        }
    }

    tracing::warn!(
        "Unified liquidation: user={} pos={} symbol={} side={} size_usd={} mark={} pnl={} shortfall={} adl={}",
        user, pos_id, symbol, side, closed_size_usd, mark_price, result.pnl,
        shortfall, adl_triggered
    );

    Ok(Some(LiquidationStep {
        position_id: pos_id,
        symbol,
        side,
        closed_size_usd,
        closed_size_tokens,
        mark_price,
        pnl_realized: result.pnl,
        collateral_returned: result.collateral_returned,
        insurance_shortfall: shortfall,
        adl_triggered,
    }))
}
