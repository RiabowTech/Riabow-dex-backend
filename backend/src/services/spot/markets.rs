//! Market metadata cache. Loaded from `spot_markets` at startup; refreshed on
//! admin PATCH via `MarketCache::swap`. Read by the engine on every
//! place/validate; reads are RwLock-guarded for cheap concurrent access.

use parking_lot::RwLock;
use rust_decimal::Decimal;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;

use crate::models::spot::SpotMarket;

#[derive(Debug, thiserror::Error)]
pub enum MarketValidationError {
    #[error("INVALID_TICK")]    InvalidTick,
    #[error("INVALID_LOT")]     InvalidLot,
    #[error("BELOW_MIN_NOTIONAL")] BelowMinNotional,
}

impl SpotMarket {
    /// Validate a (price, quantity) pair against the market's tick/lot/min_notional.
    pub fn validate_price_qty(&self, price: Decimal, qty: Decimal)
        -> Result<(), MarketValidationError>
    {
        if (price % self.tick_size) != Decimal::ZERO {
            return Err(MarketValidationError::InvalidTick);
        }
        if (qty % self.lot_size) != Decimal::ZERO {
            return Err(MarketValidationError::InvalidLot);
        }
        if price * qty < self.min_notional {
            return Err(MarketValidationError::BelowMinNotional);
        }
        Ok(())
    }
}

/// Shared, refreshable cache of market metadata.
#[derive(Debug, Default)]
pub struct MarketCache {
    inner: RwLock<HashMap<String, Arc<SpotMarket>>>,
}

impl MarketCache {
    pub fn new() -> Self { Self::default() }

    pub fn get(&self, id: &str) -> Option<Arc<SpotMarket>> {
        self.inner.read().get(id).cloned()
    }

    /// Replace the entire cache atomically (used at startup and after admin PATCH).
    pub fn swap(&self, rows: Vec<SpotMarket>) {
        let map = rows.into_iter().map(|m| (m.id.clone(), Arc::new(m))).collect();
        *self.inner.write() = map;
    }

    pub fn list(&self) -> Vec<Arc<SpotMarket>> {
        self.inner.read().values().cloned().collect()
    }
}

/// Load all markets from DB and return a populated cache.
pub async fn load_initial(pool: &PgPool) -> anyhow::Result<MarketCache> {
    let rows: Vec<SpotMarket> = sqlx::query_as("SELECT * FROM spot_markets ORDER BY id")
        .fetch_all(pool).await?;
    let cache = MarketCache::new();
    cache.swap(rows);
    Ok(cache)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn sample() -> SpotMarket {
        SpotMarket {
            id: "DFUSDT".into(), base_token: "DF".into(), quote_token: "USDT".into(),
            tick_size: dec!(0.0001), lot_size: dec!(0.01), min_notional: dec!(1),
            maker_fee_bps: 10, taker_fee_bps: 20, status: "listed".into(),
            display_name: None, description: None,
            created_at: chrono::Utc::now(), updated_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn validate_tick_lot_min_notional() {
        let m = sample();
        assert!(m.validate_price_qty(dec!(0.5), dec!(10)).is_ok());
        assert!(matches!(m.validate_price_qty(dec!(0.50001), dec!(10)),
                          Err(MarketValidationError::InvalidTick)));
        assert!(matches!(m.validate_price_qty(dec!(0.5), dec!(0.001)),
                          Err(MarketValidationError::InvalidLot)));
        assert!(matches!(m.validate_price_qty(dec!(0.5), dec!(0.01)),
                          Err(MarketValidationError::BelowMinNotional)));
    }

    #[test]
    fn cache_get_returns_none_for_unknown() {
        let cache = MarketCache::new();
        assert!(cache.get("NOPE").is_none());
    }

    #[test]
    fn cache_swap_replaces_state() {
        let cache = MarketCache::new();
        cache.swap(vec![sample()]);
        assert!(cache.get("DFUSDT").is_some());
        cache.swap(vec![]);
        assert!(cache.get("DFUSDT").is_none());
    }
}
