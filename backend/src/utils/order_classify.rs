//! 订单开/平仓判定的共享语义，`preview_order` 与 `create_order` 都调用此处，
//! 避免两个路径对同一单据得出不同结论。

use rust_decimal::Decimal;

#[derive(Debug, Clone, Copy)]
pub struct ClosingInfo {
    /// 本单会减仓（对手方仓位存在且会被消耗）。
    pub is_closing: bool,
    /// reduce_only=true 但没有对手方仓位，matching 层一定会拒单。
    pub reject_reduce_only_no_position: bool,
}

/// `existing_opposite_tokens = None` 表示对手方仓位不存在。
pub fn classify(
    reduce_only: bool,
    req_amount_tokens: Decimal,
    existing_opposite_tokens: Option<Decimal>,
) -> ClosingInfo {
    match (reduce_only, existing_opposite_tokens) {
        (true, Some(opp)) if opp > Decimal::ZERO => ClosingInfo {
            is_closing: true,
            reject_reduce_only_no_position: false,
        },
        (true, _) => ClosingInfo {
            is_closing: false,
            reject_reduce_only_no_position: true,
        },
        (false, Some(opp)) if opp > Decimal::ZERO => ClosingInfo {
            is_closing: req_amount_tokens <= opp,
            reject_reduce_only_no_position: false,
        },
        (false, _) => ClosingInfo {
            is_closing: false,
            reject_reduce_only_no_position: false,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::prelude::*;

    #[test]
    fn reduce_only_no_position_is_rejected() {
        let c = classify(true, Decimal::from(1), None);
        assert!(!c.is_closing);
        assert!(c.reject_reduce_only_no_position);
    }

    #[test]
    fn reduce_only_with_opposite_closes() {
        let c = classify(true, Decimal::from(1), Some(Decimal::from(2)));
        assert!(c.is_closing);
    }

    #[test]
    fn implicit_close_when_amount_within_opposite() {
        let c = classify(false, Decimal::from(1), Some(Decimal::from(2)));
        assert!(c.is_closing);
    }

    #[test]
    fn open_when_amount_exceeds_opposite() {
        let c = classify(false, Decimal::from(3), Some(Decimal::from(2)));
        assert!(!c.is_closing);
    }
}
