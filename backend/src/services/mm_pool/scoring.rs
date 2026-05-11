//! Pure-function scoring helpers (no I/O) for MM quality.
//!
//! `quality_score` is a deliberately simple linear combination — the
//! goal at this stage is consistency across snapshots, not absolute
//! interpretability. Per-symbol normalization can be layered on later
//! without touching the scoring math here.

use rust_decimal::Decimal;
use rust_decimal_macros::dec;

pub const W_VOLUME: Decimal = dec!(0.40);
pub const W_SPREAD: Decimal = dec!(0.25);
pub const W_DEPTH: Decimal = dec!(0.20);
pub const W_UPTIME: Decimal = dec!(0.15);

/// Reference points used to non-dimensionalize the raw measurements
/// before weighting. They are tuned so a "typical" MM at the reference
/// values scores ~1.0 in each dimension; outliers above can earn more.
pub const REF_VOLUME_USD: Decimal = dec!(100_000); // per snapshot interval
pub const REF_DEPTH_USD: Decimal = dec!(50_000);
pub const REF_SPREAD_BPS: Decimal = dec!(10); // 0.10%

/// Compose the weighted quality score. `spread_bps == None` (MM has no
/// two-sided quote) contributes 0 to the spread dimension.
pub fn quality_score(
    maker_volume_usd: Decimal,
    spread_bps: Option<Decimal>,
    depth_usd: Decimal,
    is_online: bool,
) -> Decimal {
    let vol_norm = if REF_VOLUME_USD.is_zero() {
        Decimal::ZERO
    } else {
        maker_volume_usd / REF_VOLUME_USD
    };

    // Tighter spread → higher score. spread=0 → REF_SPREAD/spread is
    // unbounded, so we clamp to a max factor of 5×.
    let spread_norm = match spread_bps {
        Some(s) if s > Decimal::ZERO => {
            let raw = REF_SPREAD_BPS / s;
            if raw > dec!(5.0) { dec!(5.0) } else { raw }
        }
        _ => Decimal::ZERO,
    };

    let depth_norm = if REF_DEPTH_USD.is_zero() {
        Decimal::ZERO
    } else {
        depth_usd / REF_DEPTH_USD
    };

    let uptime_norm = if is_online { Decimal::ONE } else { Decimal::ZERO };

    vol_norm * W_VOLUME
        + spread_norm * W_SPREAD
        + depth_norm * W_DEPTH
        + uptime_norm * W_UPTIME
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn baseline_mm_at_reference_scores_about_one() {
        let s = quality_score(REF_VOLUME_USD, Some(REF_SPREAD_BPS), REF_DEPTH_USD, true);
        // 1.0 × (0.40 + 0.25 + 0.20 + 0.15) = 1.0
        assert_eq!(s, dec!(1.00));
    }

    #[test]
    fn no_quote_zero_spread_dim() {
        let s = quality_score(REF_VOLUME_USD, None, REF_DEPTH_USD, true);
        // 1×(0.40+0.20+0.15) = 0.75
        assert_eq!(s, dec!(0.75));
    }

    #[test]
    fn offline_mm_no_uptime_credit() {
        let s = quality_score(Decimal::ZERO, None, Decimal::ZERO, false);
        assert_eq!(s, Decimal::ZERO);
    }
}
