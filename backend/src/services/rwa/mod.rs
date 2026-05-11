pub mod registry;

use std::collections::HashMap;
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use rust_decimal_macros::dec;
use serde::Serialize;

use crate::websocket::external_handler::{get_stats_cache, get_spot_stats_cache, MarketStats};
use registry::{RwaAssetDef, RwaAssetClass, DataSource, RWA_ASSETS, SYMBOL_TO_RWA};

#[derive(Debug, Clone, Serialize)]
pub struct RwaTickerSnapshot {
    pub symbol: String,
    pub display_name: String,
    pub asset_class: String,
    pub mark_price: Decimal,
    pub last_price: Decimal,
    pub price_change_24h: Decimal,
    pub price_change_pct_24h: Decimal,
    pub high_24h: Decimal,
    pub low_24h: Decimal,
    pub volume_24h_usd: Decimal,
    pub open_interest: Decimal,
    pub funding_rate: Decimal,
    pub updated_at: i64,
}

pub struct RwaService;

impl RwaService {
    pub fn get_all_tickers() -> Vec<RwaTickerSnapshot> {
        RWA_ASSETS.iter().filter_map(|asset| {
            let stats = Self::lookup_stats(asset)?;
            Some(Self::to_snapshot(asset, &stats))
        }).collect()
    }

    pub fn get_tickers_by_class(class: RwaAssetClass) -> Vec<RwaTickerSnapshot> {
        RWA_ASSETS.iter()
            .filter(|a| a.asset_class == class)
            .filter_map(|asset| {
                let stats = Self::lookup_stats(asset)?;
                Some(Self::to_snapshot(asset, &stats))
            })
            .collect()
    }

    pub fn get_ticker(symbol: &str) -> Option<RwaTickerSnapshot> {
        let idx = SYMBOL_TO_RWA.get(symbol)?;
        let asset = &RWA_ASSETS[*idx];
        let stats = Self::lookup_stats(asset)?;
        Some(Self::to_snapshot(asset, &stats))
    }

    pub fn get_prices() -> HashMap<String, Decimal> {
        RWA_ASSETS.iter().filter_map(|asset| {
            let stats = Self::lookup_stats(asset)?;
            let mark = Decimal::from_f64(stats.mark_px)?;
            Some((asset.internal_symbol.to_string(), mark))
        }).collect()
    }

    fn lookup_stats(asset: &RwaAssetDef) -> Option<MarketStats> {
        match asset.data_source {
            DataSource::Perps => {
                get_stats_cache().get(asset.hl_ticker).map(|r| r.value().clone())
            }
            DataSource::Spot => {
                get_spot_stats_cache().get(asset.hl_ticker).map(|r| r.value().clone())
            }
        }
    }

    fn to_snapshot(asset: &RwaAssetDef, stats: &MarketStats) -> RwaTickerSnapshot {
        let mark = Decimal::from_f64(stats.mark_px).unwrap_or_default();
        let prev = Decimal::from_f64(stats.prev_day_px).unwrap_or(mark);
        let change = mark - prev;
        let change_pct = if prev.is_zero() {
            Decimal::ZERO
        } else {
            (change / prev) * dec!(100)
        };
        let volume_usd = Decimal::from_f64(stats.day_ntl_vlm).unwrap_or_default();
        let oi = Decimal::from_f64(stats.open_interest).unwrap_or_default();
        let funding = Decimal::from_f64(stats.funding_rate).unwrap_or_default();

        RwaTickerSnapshot {
            symbol: asset.internal_symbol.to_string(),
            display_name: asset.display_name.to_string(),
            asset_class: asset.asset_class.as_str().to_string(),
            mark_price: mark,
            last_price: mark,
            price_change_24h: change,
            price_change_pct_24h: change_pct,
            high_24h: mark,
            low_24h: prev,
            volume_24h_usd: volume_usd,
            open_interest: oi,
            funding_rate: funding,
            updated_at: chrono::Utc::now().timestamp_millis(),
        }
    }
}
