//! VIP 阶梯费率的单一真相源。
//!
//! 产品定义（基于 14 天滚动加权交易量）：
//!
//! | VIP | Notional 区间        | Taker   | Maker   |
//! |-----|----------------------|---------|---------|
//! | 0   | < 5M                 | 0.040%  | 0.010%  |
//! | 1   | 5M   – 25M           | 0.036%  | 0.008%  |
//! | 2   | 25M  – 100M          | 0.032%  | 0.004%  |
//! | 3   | 100M – 500M          | 0.028%  | 0.000%  |
//! | 4   | 500M – 2B            | 0.026%  | 0.000%  |
//! | 5   | ≥ 2B                 | 0.024%  | 0.000%  |
//!
//! 升级即时生效；降级有一日缓冲（次 UTC 00:00 才应用）。

use rust_decimal::Decimal;
use rust_decimal_macros::dec;

#[derive(Debug, Clone, Copy)]
pub struct VipTier {
    pub level: u8,
    pub label: &'static str,
    pub taker: Decimal,
    pub maker: Decimal,
    /// 下沿，含。
    pub volume_min: Decimal,
    /// 上沿，不含。None = +∞。
    pub volume_max: Option<Decimal>,
}

/// 6 档表。`level` 必须与索引一致，从而 `classify` 可以 O(1) 返回。
pub const TIERS: [VipTier; 6] = [
    VipTier {
        level: 0, label: "VIP 0",
        taker: dec!(0.00040), maker: dec!(0.00010),
        volume_min: dec!(0),         volume_max: Some(dec!(5_000_000)),
    },
    VipTier {
        level: 1, label: "VIP 1",
        taker: dec!(0.00036), maker: dec!(0.00008),
        volume_min: dec!(5_000_000), volume_max: Some(dec!(25_000_000)),
    },
    VipTier {
        level: 2, label: "VIP 2",
        taker: dec!(0.00032), maker: dec!(0.00004),
        volume_min: dec!(25_000_000), volume_max: Some(dec!(100_000_000)),
    },
    VipTier {
        level: 3, label: "VIP 3",
        taker: dec!(0.00028), maker: dec!(0.00000),
        volume_min: dec!(100_000_000), volume_max: Some(dec!(500_000_000)),
    },
    VipTier {
        level: 4, label: "VIP 4",
        taker: dec!(0.00026), maker: dec!(0.00000),
        volume_min: dec!(500_000_000), volume_max: Some(dec!(2_000_000_000)),
    },
    VipTier {
        level: 5, label: "VIP 5",
        taker: dec!(0.00024), maker: dec!(0.00000),
        volume_min: dec!(2_000_000_000), volume_max: None,
    },
];

/// 按 14 天滚动交易量落档。返回对档位的静态引用。
pub fn classify(volume_14d: Decimal) -> &'static VipTier {
    // 从高到低扫，第一个满足 `volume >= volume_min` 的就是目标档。
    for t in TIERS.iter().rev() {
        if volume_14d >= t.volume_min {
            return t;
        }
    }
    &TIERS[0]
}

/// 按 tier level 取档位。越界时返回 VIP0。
pub fn by_level(level: u8) -> &'static VipTier {
    TIERS.get(level as usize).unwrap_or(&TIERS[0])
}

/// 当前的推荐折扣（10%）。后续可改为从 config 读取。
pub fn referral_discount() -> Decimal {
    dec!(0.10)
}

/// 代币质押折扣。接口预留，当前恒为 0。
pub fn token_staking_discount(_user_address: &str) -> Decimal {
    Decimal::ZERO
}

/// `discount_multiplier = (1 - referral) × (1 - staking)`。
/// 若订单不适用推荐返佣（例如用户自身就是 referrer），调用方传 `referral_applicable=false`。
pub fn discount_multiplier(user_address: &str, referral_applicable: bool) -> Decimal {
    let r = if referral_applicable { referral_discount() } else { Decimal::ZERO };
    let s = token_staking_discount(user_address);
    (Decimal::ONE - r) * (Decimal::ONE - s)
}

/// 6 位小数（USDC 精度）的 ceil 舍入，避免让用户少付一厘。
pub fn round_fee(v: Decimal) -> Decimal {
    v.round_dp_with_strategy(6, rust_decimal::RoundingStrategy::MidpointAwayFromZero)
}

/// 核心手续费计算。`rate` 应当来自 `VipTier::taker`/`maker`。
pub fn effective_fee(
    notional: Decimal,
    rate: Decimal,
    user_address: &str,
    referral_applicable: bool,
) -> Decimal {
    round_fee(notional * rate * discount_multiplier(user_address, referral_applicable))
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct TierProgress {
    pub next_level: u8,
    pub next_label: &'static str,
    pub required_volume: Decimal,
    pub remaining_volume: Decimal,
    /// 0..1；前端方便画进度条。
    pub percent: Decimal,
}

/// 距下一档进度。已在最高档时返回 None。
pub fn progress_to_next(volume_14d: Decimal) -> Option<TierProgress> {
    let current = classify(volume_14d);
    let next_idx = current.level as usize + 1;
    let next = TIERS.get(next_idx)?;
    let required = next.volume_min;
    let remaining = (required - volume_14d).max(Decimal::ZERO);
    let percent = if required.is_zero() {
        Decimal::ONE
    } else {
        (volume_14d / required).min(Decimal::ONE).max(Decimal::ZERO)
    };
    Some(TierProgress {
        next_level: next.level,
        next_label: next.label,
        required_volume: required,
        remaining_volume: remaining,
        percent,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_boundaries() {
        assert_eq!(classify(dec!(0)).level, 0);
        assert_eq!(classify(dec!(4_999_999)).level, 0);
        assert_eq!(classify(dec!(5_000_000)).level, 1);
        assert_eq!(classify(dec!(24_999_999)).level, 1);
        assert_eq!(classify(dec!(25_000_000)).level, 2);
        assert_eq!(classify(dec!(100_000_000)).level, 3);
        assert_eq!(classify(dec!(499_999_999)).level, 3);
        assert_eq!(classify(dec!(500_000_000)).level, 4);
        assert_eq!(classify(dec!(2_000_000_000)).level, 5);
        assert_eq!(classify(dec!(999_999_999_999)).level, 5);
    }

    #[test]
    fn rates_match_product_spec() {
        assert_eq!(TIERS[0].taker, dec!(0.00040));
        assert_eq!(TIERS[5].maker, dec!(0.00000));
        assert_eq!(TIERS[3].maker, Decimal::ZERO);
    }

    #[test]
    fn fee_applies_referral_10pct_by_default() {
        // notional 100000, rate 0.0004 (VIP0 taker), referral 10% off.
        let fee = effective_fee(dec!(100_000), dec!(0.00040), "0xabc", true);
        // 100000 * 0.0004 * 0.9 = 36
        assert_eq!(fee, dec!(36.000000));
    }

    #[test]
    fn fee_without_referral() {
        let fee = effective_fee(dec!(100_000), dec!(0.00040), "0xabc", false);
        assert_eq!(fee, dec!(40.000000));
    }

    #[test]
    fn progress_from_vip0() {
        let p = progress_to_next(dec!(2_500_000)).unwrap();
        assert_eq!(p.next_level, 1);
        assert_eq!(p.required_volume, dec!(5_000_000));
        assert_eq!(p.remaining_volume, dec!(2_500_000));
    }

    #[test]
    fn progress_at_top_is_none() {
        assert!(progress_to_next(dec!(3_000_000_000)).is_none());
    }
}
