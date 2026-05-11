//! Tiered margin ladder (Phase 4).
//!
//! A tier defines a (symbol, notional-bucket) → (mmr, max_leverage,
//! cum_amount) mapping. Maintenance margin for a position with notional
//! `N` in a tier with rate `r` and cumulative deduction `c` is:
//!
//!     mm = N * r - c
//!
//! The `cum_amount` makes the piecewise-linear curve continuous across
//! tier boundaries. The design mirrors Binance perpetual-futures tiers.

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct MarginTier {
    pub symbol: String,
    pub tier: i32,
    pub max_notional: Decimal,
    pub maint_margin_rate: Decimal,
    pub max_leverage: i32,
    pub cum_amount: Decimal,
}

/// In-memory tier store, refreshed infrequently. `'*'` is the fallback
/// key used when a symbol has no explicit ladder.
#[derive(Debug, Default)]
pub struct TierStore {
    // Keyed by symbol (or "*"). Tiers stored in ascending `tier` order.
    by_symbol: HashMap<String, Vec<MarginTier>>,
}

impl TierStore {
    /// Test/admin-side constructor used by unit tests to inject a
    /// hand-built ladder without touching the DB.
    pub fn from_map(by_symbol: HashMap<String, Vec<MarginTier>>) -> Self {
        Self { by_symbol }
    }

    pub async fn load(pool: &PgPool) -> anyhow::Result<Self> {
        let rows: Vec<MarginTier> = sqlx::query_as::<_, MarginTier>(
            "SELECT symbol, tier, max_notional, maint_margin_rate, \
                    max_leverage, cum_amount \
             FROM margin_tiers ORDER BY symbol, tier",
        )
        .fetch_all(pool)
        .await?;
        let mut by_symbol: HashMap<String, Vec<MarginTier>> = HashMap::new();
        for t in rows {
            by_symbol.entry(t.symbol.clone()).or_default().push(t);
        }
        Ok(Self { by_symbol })
    }

    /// Resolve (mmr, cum_amount) for a `symbol` given its position
    /// `notional`. Falls back to the `'*'` default ladder; if the DB is
    /// empty we emit a safe fixed 0.5% with no cum offset.
    pub fn resolve(&self, symbol: &str, notional: Decimal) -> (Decimal, Decimal) {
        let tiers = self
            .by_symbol
            .get(symbol)
            .or_else(|| self.by_symbol.get("*"));
        let Some(tiers) = tiers else {
            return (dec!(0.005), Decimal::ZERO);
        };
        let abs_notional = notional.abs();
        for t in tiers {
            if abs_notional <= t.max_notional {
                return (t.maint_margin_rate, t.cum_amount);
            }
        }
        // Above highest tier — apply the top tier's rate (conservative).
        if let Some(top) = tiers.last() {
            return (top.maint_margin_rate, top.cum_amount);
        }
        (dec!(0.005), Decimal::ZERO)
    }

    /// Compute maintenance margin for a single position notional.
    pub fn maintenance_margin(&self, symbol: &str, notional: Decimal) -> Decimal {
        let (rate, cum) = self.resolve(symbol, notional);
        let raw = notional.abs() * rate - cum;
        raw.max(Decimal::ZERO)
    }
}

/// Shared handle carried in the unified-margin code paths. An RwLock
/// gives us cheap parallel reads; writes happen only on manual reload.
pub type TierStoreHandle = Arc<RwLock<TierStore>>;

pub fn empty_handle() -> TierStoreHandle {
    Arc::new(RwLock::new(TierStore::default()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn default_ladder() -> Vec<MarginTier> {
        // Mirrors the seed in db/mod.rs (PRD §12). Cum amounts ensure
        // continuity at boundaries: MM(N) = N*r - cum.
        vec![
            MarginTier { symbol: "*".into(), tier: 1, max_notional: dec!(50000),     maint_margin_rate: dec!(0.004), max_leverage: 125, cum_amount: dec!(0)       },
            MarginTier { symbol: "*".into(), tier: 2, max_notional: dec!(250000),    maint_margin_rate: dec!(0.005), max_leverage: 100, cum_amount: dec!(50)      },
            MarginTier { symbol: "*".into(), tier: 3, max_notional: dec!(1000000),   maint_margin_rate: dec!(0.01),  max_leverage: 50,  cum_amount: dec!(1300)    },
            MarginTier { symbol: "*".into(), tier: 8, max_notional: dec!(200000000), maint_margin_rate: dec!(0.15),  max_leverage: 2,   cum_amount: dec!(4891300) },
        ]
    }

    fn store_with_default() -> TierStore {
        let mut by = HashMap::new();
        by.insert("*".to_string(), default_ladder());
        TierStore { by_symbol: by }
    }

    #[test]
    fn empty_store_falls_back_to_safe_constant() {
        let s = TierStore::default();
        let (rate, cum) = s.resolve("BTCUSDT", dec!(1));
        assert_eq!(rate, dec!(0.005));
        assert_eq!(cum, dec!(0));
    }

    #[test]
    fn picks_first_tier_under_50k() {
        let s = store_with_default();
        // notional = 1k → tier 1 (cap 50k)
        let mm = s.maintenance_margin("BTCUSDT", dec!(1000));
        assert_eq!(mm, dec!(4)); // 1000 * 0.004 - 0 = 4
    }

    #[test]
    fn picks_top_tier_above_largest_cap() {
        let s = store_with_default();
        let mm = s.maintenance_margin("BTCUSDT", dec!(999_999_999));
        // tier 8: 999_999_999*0.15 - 4_891_300 = 145_108_699.85
        assert_eq!(mm, dec!(145108699.85));
    }

    #[test]
    fn boundary_continuous_at_top_of_tier() {
        // At notional exactly = max_notional of tier 1 (50k), tier 1
        // applies → MM = 50_000 * 0.004 - 0 = 200.
        // Tier 2 cum = 50 ⇒ at 50_000 it would also yield
        // 50_000 * 0.005 - 50 = 200. Boundary is continuous.
        let s = store_with_default();
        assert_eq!(s.maintenance_margin("ETHUSDT", dec!(50000)), dec!(200));
    }

    #[test]
    fn per_symbol_override_beats_default() {
        let mut by = HashMap::new();
        by.insert("*".into(), default_ladder());
        by.insert(
            "BTCUSDT".into(),
            vec![MarginTier {
                symbol: "BTCUSDT".into(),
                tier: 1, max_notional: dec!(10000), maint_margin_rate: dec!(0.003),
                max_leverage: 150, cum_amount: dec!(0),
            }],
        );
        let s = TierStore { by_symbol: by };
        // BTCUSDT 1k → 1k * 0.003 - 0 = 3 (override)
        // ETHUSDT 1k → 1k * 0.004 - 0 = 4 (default ladder)
        assert_eq!(s.maintenance_margin("BTCUSDT", dec!(1000)), dec!(3));
        assert_eq!(s.maintenance_margin("ETHUSDT", dec!(1000)), dec!(4));
    }

    #[test]
    fn negative_notional_uses_abs() {
        let s = store_with_default();
        assert_eq!(
            s.maintenance_margin("BTCUSDT", dec!(-1000)),
            s.maintenance_margin("BTCUSDT", dec!(1000)),
        );
    }
}
