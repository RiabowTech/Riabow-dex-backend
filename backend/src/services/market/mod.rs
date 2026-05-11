#![allow(dead_code)]
//! Market Data Service

use rust_decimal::Decimal;
// use std::collections::HashMap;

pub struct MarketService {
    // TODO: Price feeds, market configs
}

impl MarketService {
    pub fn new() -> Self {
        Self {}
    }

    /// Get current mark price for a symbol
    pub async fn get_mark_price(&self, _symbol: &str) -> anyhow::Result<Decimal> {
        // TODO: Get from price service
        Ok(Decimal::new(50000, 0))
    }

    /// Get funding rate
    pub async fn get_funding_rate(&self, _symbol: &str) -> anyhow::Result<FundingInfo> {
        // TODO: Get from funding service
        Ok(FundingInfo {
            rate: Decimal::ZERO,
            next_funding_time: chrono::Utc::now().timestamp() + 3600,
        })
    }

    /// Get market configuration
    pub fn get_market_config(&self, symbol: &str) -> Option<MarketConfig> {
        // TODO: Load from config/database
        Some(MarketConfig {
            symbol: symbol.to_string(),
            base_asset: "BTC".to_string(),
            quote_asset: "USD".to_string(),
            min_order_size: Decimal::new(1, 4),
            max_order_size: Decimal::new(1000, 0),
            tick_size: Decimal::new(1, 1),
            max_leverage: 100,
            maintenance_margin_rate: Decimal::new(5, 3), // 0.5%
            maker_fee: Decimal::new(2, 4),               // 0.02%
            taker_fee: Decimal::new(5, 4),               // 0.05%
        })
    }
}

#[derive(Debug, Clone)]
pub struct MarketConfig {
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub min_order_size: Decimal,
    pub max_order_size: Decimal,
    pub tick_size: Decimal,
    pub max_leverage: i32,
    pub maintenance_margin_rate: Decimal,
    pub maker_fee: Decimal,
    pub taker_fee: Decimal,
}

#[derive(Debug)]
pub struct FundingInfo {
    pub rate: Decimal,
    pub next_funding_time: i64,
}
