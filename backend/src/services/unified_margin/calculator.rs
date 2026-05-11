//! Unified-margin core formulas.
//!
//! Keep this module pure (no DB, no I/O) so it is unit-testable and
//! reusable from both the risk worker (future) and the order handler.

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;

use crate::models::position::{Position, PositionSide};
use crate::models::unified_margin::{UnifiedAccountStatus, UnifiedRiskSnapshot};
use crate::services::unified_margin::tiers::TierStore;

/// Fallback maintenance-margin rate (0.5%) used when the per-symbol
/// tier ladder is unavailable (e.g. tier store empty during cold start).
/// Matches `PositionConfig::default().maintenance_margin_rate`. The
/// production path always passes `Some(&TierStore)` and reads tier-aware
/// MM via [`TierStore::maintenance_margin`].
pub const MMR_DEFAULT: Decimal = dec!(0.005);

/// Classify account status per §4.1 thresholds.
pub fn classify(uni_mmr: Option<Decimal>) -> UnifiedAccountStatus {
    match uni_mmr {
        None => UnifiedAccountStatus::Normal, // no positions
        Some(m) if m <= dec!(1.05) => UnifiedAccountStatus::Liquidating,
        Some(m) if m <= dec!(1.20) => UnifiedAccountStatus::ReduceOnly,
        Some(m) if m <= dec!(1.50) => UnifiedAccountStatus::Warning2,
        Some(m) if m <= dec!(2.00) => UnifiedAccountStatus::Warning1,
        Some(_) => UnifiedAccountStatus::Normal,
    }
}

/// Compute the live risk snapshot for a unified-margin account.
///
/// `wallet_balance` should be `balances.available + balances.frozen` of
/// the collateral token. `positions` are the user's open positions;
/// `mark_prices` maps position.symbol → current mark price (missing
/// entries fall back to the position's entry_price so the calculation
/// still converges rather than producing a NaN-equivalent).
pub fn compute_risk(
    wallet_balance: Decimal,
    positions: &[Position],
    mark_prices: &HashMap<String, Decimal>,
    mmr: Decimal,
) -> UnifiedRiskSnapshot {
    compute_risk_with_tiers(wallet_balance, positions, mark_prices, mmr, None)
}

/// Tier-aware variant. When `tiers` is `Some`, per-symbol/per-notional
/// maintenance margin replaces the flat `mmr` constant. Initial margin
/// is always notional/leverage (tiers only affect MM).
///
/// **Missing mark prices are conservative, not silent**: we still use
/// `entry_price` for the PnL term (so we don't NaN), but caller can
/// inspect `missing_mark_symbols` to decide whether to alert and/or
/// force-degrade the account into `reduce_only`. The risk worker uses
/// this signal to refuse to clear `reduce_only` while prices are stale.
pub fn compute_risk_with_tiers(
    wallet_balance: Decimal,
    positions: &[Position],
    mark_prices: &HashMap<String, Decimal>,
    fallback_mmr: Decimal,
    tiers: Option<&TierStore>,
) -> UnifiedRiskSnapshot {
    let mut total_unrealized_pnl = Decimal::ZERO;
    let mut total_fees = Decimal::ZERO;
    let mut total_initial_margin = Decimal::ZERO;
    let mut total_maint_margin = Decimal::ZERO;
    let mut total_position_collateral = Decimal::ZERO;
    let mut missing_mark_symbols: Vec<String> = Vec::new();

    for p in positions {
        let mark = match mark_prices.get(&p.symbol).copied() {
            Some(m) => m,
            None => {
                // Track for the caller — entry_price is *only* a
                // numerical fallback so we don't divide-by-zero or NaN.
                missing_mark_symbols.push(p.symbol.clone());
                p.entry_price
            }
        };

        let pnl = match p.side {
            PositionSide::Long => (p.size_in_tokens * mark) - p.size_in_usd,
            PositionSide::Short => p.size_in_usd - (p.size_in_tokens * mark),
        };
        total_unrealized_pnl += pnl;
        total_fees += p.accumulated_funding_fee + p.accumulated_borrowing_fee;
        total_position_collateral += p.collateral_amount;

        let notional = p.size_in_usd;
        if p.leverage > 0 {
            total_initial_margin += notional / Decimal::from(p.leverage);
        }
        total_maint_margin += match tiers {
            Some(ts) => ts.maintenance_margin(&p.symbol, notional),
            None => notional * fallback_mmr,
        };
    }

    // Position collateral was debited from balances at fill time (order.rs:789
    // `available_delta = required_margin - collateral_to_position` makes
    // wallet_balance shrink by the collateral that flowed into
    // positions.collateral_amount). Add it back so equity reflects the user's
    // total funds on the platform, not just the uncommitted cash.
    let total_equity =
        wallet_balance + total_position_collateral + total_unrealized_pnl - total_fees;
    let available_balance = total_equity - total_initial_margin;

    let uni_mmr = if total_maint_margin > Decimal::ZERO {
        Some(total_equity / total_maint_margin)
    } else {
        None
    };

    let mut snap = UnifiedRiskSnapshot {
        wallet_balance,
        total_unrealized_pnl,
        total_accumulated_fees: total_fees,
        total_equity,
        total_initial_margin,
        total_maint_margin,
        uni_mmr,
        available_balance,
        account_status: classify(uni_mmr),
        missing_mark_symbols,
    };

    // Risk-conservative: if any mark price is missing AND the account
    // would otherwise be classified strictly above reduce_only, force
    // it down to ReduceOnly so trading is gated until prices recover.
    if !snap.missing_mark_symbols.is_empty() {
        use UnifiedAccountStatus::*;
        snap.account_status = match snap.account_status {
            Liquidating => Liquidating,
            ReduceOnly => ReduceOnly,
            _ => ReduceOnly,
        };
    }

    snap
}

/// Outcome of pre-trade simulation for a new order under unified margin.
#[derive(Debug, Clone)]
pub struct SimulateResult {
    pub current_uni_mmr: Option<Decimal>,
    pub simulated_uni_mmr: Option<Decimal>,
    pub new_initial_margin: Decimal,
    pub new_maint_margin: Decimal,
    pub available_after: Decimal,
    pub can_open: bool,
    pub reason: Option<&'static str>,
}

/// Gate for POST /orders when user is in unified mode.
///
/// Per doc §6.2: require simulated uniMMR ≥ 1.10 and
/// `available_balance ≥ new_initial_margin`, and reject if account is
/// in reduce_only / liquidating state.
///
/// `new_notional = amount * price`. We deliberately do NOT forecast
/// PnL impact from the new trade (the fill price ≈ mark price at open,
/// so the immediate Δpnl is ~0); that's consistent with the isolated
/// path's behavior.
pub fn simulate_open(
    current: &UnifiedRiskSnapshot,
    new_notional: Decimal,
    leverage: i32,
    mmr: Decimal,
) -> SimulateResult {
    simulate_open_with_tiers(current, "", new_notional, leverage, mmr, None)
}

/// Tier-aware variant. `symbol` is used only when `tiers` is `Some`.
pub fn simulate_open_with_tiers(
    current: &UnifiedRiskSnapshot,
    symbol: &str,
    new_notional: Decimal,
    leverage: i32,
    fallback_mmr: Decimal,
    tiers: Option<&TierStore>,
) -> SimulateResult {
    let new_initial_margin = if leverage > 0 {
        new_notional / Decimal::from(leverage)
    } else {
        new_notional
    };
    let new_maint_margin = match tiers {
        Some(ts) => ts.maintenance_margin(symbol, new_notional),
        None => new_notional * fallback_mmr,
    };

    let sim_total_maint = current.total_maint_margin + new_maint_margin;
    let sim_total_im = current.total_initial_margin + new_initial_margin;
    let sim_uni_mmr = if sim_total_maint > Decimal::ZERO {
        Some(current.total_equity / sim_total_maint)
    } else {
        None
    };
    let available_after = current.total_equity - sim_total_im;

    let mut can_open = true;
    let mut reason: Option<&'static str> = None;

    match current.account_status {
        UnifiedAccountStatus::ReduceOnly => {
            can_open = false;
            reason = Some("account in reduce_only mode");
        }
        UnifiedAccountStatus::Liquidating => {
            can_open = false;
            reason = Some("account is liquidating");
        }
        _ => {}
    }

    if can_open && available_after < Decimal::ZERO {
        can_open = false;
        reason = Some("insufficient available balance after opening");
    }

    if can_open {
        if let Some(m) = sim_uni_mmr {
            if m < dec!(1.10) {
                can_open = false;
                reason = Some("simulated uniMMR would fall below 1.10");
            }
        }
    }

    SimulateResult {
        current_uni_mmr: current.uni_mmr,
        simulated_uni_mmr: sim_uni_mmr,
        new_initial_margin,
        new_maint_margin,
        available_after,
        can_open,
        reason,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use uuid::Uuid;

    fn pos(side: PositionSide, size_usd: Decimal, tokens: Decimal, entry: Decimal, lev: i32) -> Position {
        Position {
            id: Uuid::nil(),
            user_address: "0x".into(),
            symbol: "BTCUSDT".into(),
            side,
            size_in_usd: size_usd,
            size_in_tokens: tokens,
            collateral_amount: size_usd / Decimal::from(lev),
            entry_price: entry,
            leverage: lev,
            liquidation_price: Decimal::ZERO,
            borrowing_factor: Decimal::ZERO,
            funding_fee_amount_per_size: Decimal::ZERO,
            accumulated_funding_fee: Decimal::ZERO,
            accumulated_borrowing_fee: Decimal::ZERO,
            accumulated_trading_fee: Decimal::ZERO,
            unrealized_pnl: Decimal::ZERO,
            realized_pnl: Decimal::ZERO,
            status: crate::models::position::PositionStatus::Open,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            increased_at: None,
            decreased_at: None,
        }
    }

    #[test]
    fn empty_account_has_no_mmr() {
        let r = compute_risk(dec!(1000), &[], &HashMap::new(), MMR_DEFAULT);
        assert_eq!(r.uni_mmr, None);
        assert_eq!(r.available_balance, dec!(1000));
        assert!(matches!(r.account_status, UnifiedAccountStatus::Normal));
    }

    #[test]
    fn long_position_pnl_and_mmr() {
        // 1 BTC long @ 50k, mark 60k, lev 10x — collateral=5000, pnl=+10000
        let positions = vec![pos(PositionSide::Long, dec!(50000), dec!(1), dec!(50000), 10)];
        let mut mp = HashMap::new();
        mp.insert("BTCUSDT".into(), dec!(60000));
        let r = compute_risk(dec!(5000), &positions, &mp, MMR_DEFAULT);
        // equity = wallet(5000) + collateral(5000) + pnl(10000) = 20000
        // maint  = 50000 * 0.005 = 250;  mmr = 20000 / 250 = 80
        assert_eq!(r.total_equity, dec!(20000));
        assert_eq!(r.total_maint_margin, dec!(250));
        assert_eq!(r.uni_mmr, Some(dec!(80)));
    }

    #[test]
    fn simulate_open_rejects_when_mmr_drops_below_110() {
        // 1 BTC long @ 10k, lev 100x — collateral=100, pnl=0 (no mark → entry fallback)
        let positions = vec![pos(PositionSide::Long, dec!(10000), dec!(1), dec!(10000), 100)];
        let mp: HashMap<String, Decimal> = HashMap::new();
        let r = compute_risk(dec!(100), &positions, &mp, MMR_DEFAULT);
        // equity = wallet(100) + collateral(100) + pnl(0) = 200;  maint = 50;  mmr = 4
        assert_eq!(r.uni_mmr, Some(dec!(4)));
        // Adding 20000 notional @ 100x pushes sim IM to 300 while equity stays
        // 200 → available_after = -100, so can_open falls to false on the
        // insufficient-available branch (rather than the MMR branch).
        let sim = simulate_open(&r, dec!(20000), 100, MMR_DEFAULT);
        assert!(!sim.can_open);
    }

    #[test]
    fn classify_boundaries_match_doc() {
        // PRD §4.1 thresholds
        assert!(matches!(classify(None), UnifiedAccountStatus::Normal));
        assert!(matches!(classify(Some(dec!(2.01))), UnifiedAccountStatus::Normal));
        assert!(matches!(classify(Some(dec!(2.00))), UnifiedAccountStatus::Warning1));
        assert!(matches!(classify(Some(dec!(1.51))), UnifiedAccountStatus::Warning1));
        assert!(matches!(classify(Some(dec!(1.50))), UnifiedAccountStatus::Warning2));
        assert!(matches!(classify(Some(dec!(1.21))), UnifiedAccountStatus::Warning2));
        assert!(matches!(classify(Some(dec!(1.20))), UnifiedAccountStatus::ReduceOnly));
        assert!(matches!(classify(Some(dec!(1.06))), UnifiedAccountStatus::ReduceOnly));
        assert!(matches!(classify(Some(dec!(1.05))), UnifiedAccountStatus::Liquidating));
        assert!(matches!(classify(Some(dec!(0.50))), UnifiedAccountStatus::Liquidating));
    }

    #[test]
    fn missing_mark_price_forces_reduce_only() {
        let positions = vec![pos(PositionSide::Long, dec!(10000), dec!(1), dec!(10000), 100)];
        // intentionally empty: no mark for BTCUSDT
        let r = compute_risk(dec!(1_000_000), &positions, &HashMap::new(), MMR_DEFAULT);
        assert_eq!(r.missing_mark_symbols, vec!["BTCUSDT".to_string()]);
        // wallet 1M & MM 50 ⇒ uniMMR = 20_000 (Normal), but missing
        // mark forces a downgrade to ReduceOnly.
        assert!(matches!(r.account_status, UnifiedAccountStatus::ReduceOnly));
    }

    #[test]
    fn short_position_pnl_inverts() {
        let positions = vec![pos(PositionSide::Short, dec!(50000), dec!(1), dec!(50000), 10)];
        let mut mp = HashMap::new();
        mp.insert("BTCUSDT".into(), dec!(40000));
        let r = compute_risk(dec!(5000), &positions, &mp, MMR_DEFAULT);
        // pnl = 50_000 - 40_000 = +10_000
        assert_eq!(r.total_unrealized_pnl, dec!(10000));
    }

    #[test]
    fn fees_subtract_from_equity() {
        // 1 BTC long @ 10k, lev 10x — collateral=1000, pnl=0 (entry fallback)
        let mut p = pos(PositionSide::Long, dec!(10000), dec!(1), dec!(10000), 10);
        p.accumulated_funding_fee = dec!(20);
        p.accumulated_borrowing_fee = dec!(30);
        let r = compute_risk(dec!(1000), &[p], &HashMap::new(), MMR_DEFAULT);
        // equity = wallet(1000) + collateral(1000) + pnl(0) − fees(50) = 1950
        assert_eq!(r.total_equity, dec!(1950));
    }

    #[test]
    fn tier_aware_simulate_with_top_tier() {
        // Build a tier store with the top default tier and simulate a
        // 999_999_999 USD trade — verify MM matches the closed form
        // 999_999_999 * 0.15 - 4_891_300 = 145_108_699.85
        use crate::services::unified_margin::tiers::{MarginTier, TierStore};
        let mut by = HashMap::new();
        by.insert(
            "*".into(),
            vec![MarginTier {
                symbol: "*".into(),
                tier: 8, max_notional: dec!(200_000_000),
                maint_margin_rate: dec!(0.15), max_leverage: 2,
                cum_amount: dec!(4_891_300),
            }],
        );
        let store = TierStore::from_map(by);
        let snap = compute_risk_with_tiers(
            dec!(0), &[], &HashMap::new(), MMR_DEFAULT, Some(&store),
        );
        let sim = simulate_open_with_tiers(
            &snap, "BTCUSDT", dec!(999_999_999), 2, MMR_DEFAULT, Some(&store),
        );
        assert_eq!(sim.new_maint_margin, dec!(145108699.85));
    }

    #[test]
    fn simulate_blocks_when_status_already_reduce_only() {
        // Build a snapshot manually as if classify returned ReduceOnly.
        let snap = UnifiedRiskSnapshot {
            wallet_balance: dec!(0),
            total_unrealized_pnl: dec!(0),
            total_accumulated_fees: dec!(0),
            total_equity: dec!(1000),
            total_initial_margin: dec!(0),
            total_maint_margin: dec!(900),
            uni_mmr: Some(dec!(1.11)),
            available_balance: dec!(1000),
            account_status: UnifiedAccountStatus::ReduceOnly,
            missing_mark_symbols: vec![],
        };
        let sim = simulate_open(&snap, dec!(100), 10, MMR_DEFAULT);
        assert!(!sim.can_open);
        assert_eq!(sim.reason, Some("account in reduce_only mode"));
    }
}
