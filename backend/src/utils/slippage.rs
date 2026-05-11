//! 盘口滑点模拟：按档位深度扫单，返回加权平均成交价。

use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use std::str::FromStr;

/// 扫单结果。
pub struct FillSimulation {
    /// 可成交数量（受可用深度限制，可能小于 `amount`）。
    pub filled: Decimal,
    /// 成交总 notional（USD）。
    pub notional: Decimal,
    /// 加权平均成交价；深度为 0 时返回 None。
    pub avg_price: Option<Decimal>,
}

impl FillSimulation {
    pub fn slippage_vs(&self, reference: Decimal) -> Decimal {
        match (self.avg_price, reference.is_zero()) {
            (Some(avg), false) => {
                let diff = (avg - reference).abs();
                diff / reference
            }
            _ => Decimal::ZERO,
        }
    }
}

/// 在一边盘口上模拟吃单。`levels` 期望为 `[["px","sz"], ...]`，按
/// 最优价在前排序（bids 降序 / asks 升序）。调用方保证顺序。
pub fn simulate_fill(levels: &[[String; 2]], amount: Decimal) -> FillSimulation {
    let mut remaining = amount;
    let mut filled = Decimal::ZERO;
    let mut notional = Decimal::ZERO;

    for level in levels {
        if remaining <= Decimal::ZERO {
            break;
        }
        let Ok(px) = Decimal::from_str(&level[0]) else { continue };
        let Ok(sz) = Decimal::from_str(&level[1]) else { continue };
        if sz <= Decimal::ZERO { continue; }

        let take = if sz >= remaining { remaining } else { sz };
        filled += take;
        notional += take * px;
        remaining -= take;
    }

    let avg_price = if filled > Decimal::ZERO {
        Some(notional / filled)
    } else {
        None
    };
    FillSimulation { filled, notional, avg_price }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lvl(px: &str, sz: &str) -> [String; 2] {
        [px.to_string(), sz.to_string()]
    }

    #[test]
    fn empty_book() {
        let sim = simulate_fill(&[], Decimal::from(1));
        assert!(sim.avg_price.is_none());
        assert_eq!(sim.filled, Decimal::ZERO);
    }

    #[test]
    fn single_level_full() {
        let book = vec![lvl("100", "2")];
        let sim = simulate_fill(&book, Decimal::from(1));
        assert_eq!(sim.avg_price, Some(Decimal::from(100)));
        assert_eq!(sim.filled, Decimal::from(1));
    }

    #[test]
    fn multi_level_weighted() {
        let book = vec![lvl("100", "1"), lvl("101", "1"), lvl("102", "1")];
        let sim = simulate_fill(&book, Decimal::from(3));
        assert_eq!(sim.filled, Decimal::from(3));
        assert_eq!(sim.avg_price, Some(Decimal::from(101)));
    }

    #[test]
    fn partial_fill_when_insufficient_depth() {
        let book = vec![lvl("100", "1")];
        let sim = simulate_fill(&book, Decimal::from(5));
        assert_eq!(sim.filled, Decimal::from(1));
    }

    #[test]
    fn slippage_vs_mark() {
        let book = vec![lvl("100", "1"), lvl("110", "1")];
        let sim = simulate_fill(&book, Decimal::from(2));
        // avg = 105, reference 100, slippage = 0.05
        let slip = sim.slippage_vs(Decimal::from(100));
        assert_eq!(slip, Decimal::from_str("0.05").unwrap());
    }
}
