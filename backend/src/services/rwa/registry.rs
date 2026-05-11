use std::collections::HashMap;
use std::sync::LazyLock;
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum RwaAssetClass {
    PreciousMetal,
    Stock,
    Index,
}

impl RwaAssetClass {
    pub fn as_str(&self) -> &'static str {
        match self {
            RwaAssetClass::PreciousMetal => "precious_metal",
            RwaAssetClass::Stock => "stock",
            RwaAssetClass::Index => "index",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "precious_metal" => Some(RwaAssetClass::PreciousMetal),
            "stock" => Some(RwaAssetClass::Stock),
            "index" => Some(RwaAssetClass::Index),
            _ => None,
        }
    }
}

/// Where to look up the price data
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataSource {
    Perps, // metaAndAssetCtxs (perpetual futures)
    Spot,  // spotMetaAndAssetCtxs (spot market)
}

#[derive(Debug)]
pub struct RwaAssetDef {
    pub hl_ticker: &'static str,
    pub internal_symbol: &'static str,
    pub display_name: &'static str,
    pub asset_class: RwaAssetClass,
    #[allow(dead_code)]
    pub price_precision: u8,
    #[allow(dead_code)]
    pub base_currency: &'static str,
    pub data_source: DataSource,
    /// Hyperliquid spot pair index for candle data (e.g. @182 for XAUT0/USDC)
    /// None if no spot trading pair exists yet
    pub spot_pair_index: Option<u32>,
    /// Alternative user-facing symbols (e.g. "XAUUSDT" for gold)
    pub aliases: &'static [&'static str],
}

pub static RWA_ASSETS: LazyLock<Vec<RwaAssetDef>> = LazyLock::new(|| vec![
    // Precious Metals (spot market)
    RwaAssetDef {
        hl_ticker: "XAUT0",
        internal_symbol: "GOLDUSD",
        display_name: "Gold / USD",
        asset_class: RwaAssetClass::PreciousMetal,
        price_precision: 2,
        base_currency: "USD",
        data_source: DataSource::Spot,
        spot_pair_index: Some(182),
        aliases: &["XAUUSDT", "XAUUSD", "GOLDUSDT"],
    },
    RwaAssetDef {
        hl_ticker: "SLV",
        internal_symbol: "SILVERUSD",
        display_name: "Silver / USD",
        asset_class: RwaAssetClass::PreciousMetal,
        price_precision: 3,
        base_currency: "USD",
        data_source: DataSource::Spot,
        spot_pair_index: Some(265),
        aliases: &["XAGUSDT", "XAGUSD", "SILVERUSDT"],
    },
    // US Stocks (spot market)
    RwaAssetDef {
        hl_ticker: "NVDA",
        internal_symbol: "NVDAUSD",
        display_name: "NVIDIA / USD",
        asset_class: RwaAssetClass::Stock,
        price_precision: 2,
        base_currency: "USD",
        data_source: DataSource::Spot,
        spot_pair_index: None, // No spot pair yet on Hyperliquid
        aliases: &["NVDAUSDT"],
    },
    RwaAssetDef {
        hl_ticker: "AAPL",
        internal_symbol: "AAPLUSD",
        display_name: "Apple / USD",
        asset_class: RwaAssetClass::Stock,
        price_precision: 2,
        base_currency: "USD",
        data_source: DataSource::Spot,
        spot_pair_index: Some(268),
        aliases: &["AAPLUSDT"],
    },
    RwaAssetDef {
        hl_ticker: "TSLA",
        internal_symbol: "TSLAUSD",
        display_name: "Tesla / USD",
        asset_class: RwaAssetClass::Stock,
        price_precision: 2,
        base_currency: "USD",
        data_source: DataSource::Spot,
        spot_pair_index: Some(264),
        aliases: &["TSLAUSDT"],
    },
    RwaAssetDef {
        hl_ticker: "GOOGL",
        internal_symbol: "GOOGLUSD",
        display_name: "Google / USD",
        asset_class: RwaAssetClass::Stock,
        price_precision: 2,
        base_currency: "USD",
        data_source: DataSource::Spot,
        spot_pair_index: Some(266),
        aliases: &["GOOGLEUSDT", "GOOGLUSDT", "GOOGLUSD"],
    },
    RwaAssetDef {
        hl_ticker: "AMZN",
        internal_symbol: "AMZNUSD",
        display_name: "Amazon / USD",
        asset_class: RwaAssetClass::Stock,
        price_precision: 2,
        base_currency: "USD",
        data_source: DataSource::Spot,
        spot_pair_index: Some(280),
        aliases: &["AMZNUSDT"],
    },
    RwaAssetDef {
        hl_ticker: "MSFT",
        internal_symbol: "MSFTUSD",
        display_name: "Microsoft / USD",
        asset_class: RwaAssetClass::Stock,
        price_precision: 2,
        base_currency: "USD",
        data_source: DataSource::Spot,
        spot_pair_index: Some(289),
        aliases: &["MSFTUSDT"],
    },
    RwaAssetDef {
        hl_ticker: "META",
        internal_symbol: "METAUSD",
        display_name: "Meta / USD",
        asset_class: RwaAssetClass::Stock,
        price_precision: 2,
        base_currency: "USD",
        data_source: DataSource::Spot,
        spot_pair_index: Some(287),
        aliases: &["METAUSDT"],
    },
    RwaAssetDef {
        hl_ticker: "HOOD",
        internal_symbol: "HOODUSD",
        display_name: "Robinhood / USD",
        asset_class: RwaAssetClass::Stock,
        price_precision: 2,
        base_currency: "USD",
        data_source: DataSource::Spot,
        spot_pair_index: Some(271),
        aliases: &["HOODUSDT"],
    },
    RwaAssetDef {
        hl_ticker: "MSTR",
        internal_symbol: "MSTRUSD",
        display_name: "MicroStrategy / USD",
        asset_class: RwaAssetClass::Stock,
        price_precision: 2,
        base_currency: "USD",
        data_source: DataSource::Spot,
        spot_pair_index: None, // Token exists but no spot pair yet
        aliases: &["MSTRUSDT"],
    },
    RwaAssetDef {
        hl_ticker: "COIN",
        internal_symbol: "COINUSD",
        display_name: "Coinbase / USD",
        asset_class: RwaAssetClass::Stock,
        price_precision: 2,
        base_currency: "USD",
        data_source: DataSource::Spot,
        spot_pair_index: None, // Token exists but no spot pair yet
        aliases: &["COINUSDT"],
    },
    RwaAssetDef {
        hl_ticker: "CRCL",
        internal_symbol: "CRCLUSD",
        display_name: "Circle / USD",
        asset_class: RwaAssetClass::Stock,
        price_precision: 2,
        base_currency: "USD",
        data_source: DataSource::Spot,
        spot_pair_index: Some(263),
        aliases: &["CRCLUSDT"],
    },
    // Indices (spot market)
    RwaAssetDef {
        hl_ticker: "SPY",
        internal_symbol: "SPYUSD",
        display_name: "S&P 500 ETF / USD",
        asset_class: RwaAssetClass::Index,
        price_precision: 2,
        base_currency: "USD",
        data_source: DataSource::Spot,
        spot_pair_index: Some(279),
        aliases: &["SPYUSDT"],
    },
    RwaAssetDef {
        hl_ticker: "QQQ",
        internal_symbol: "QQQUSD",
        display_name: "Nasdaq 100 ETF / USD",
        asset_class: RwaAssetClass::Index,
        price_precision: 2,
        base_currency: "USD",
        data_source: DataSource::Spot,
        spot_pair_index: Some(288),
        aliases: &["QQQUSDT"],
    },
    // SPX perps (also available on perps market)
    RwaAssetDef {
        hl_ticker: "SPX",
        internal_symbol: "SPXUSD",
        display_name: "S&P 500 / USD",
        asset_class: RwaAssetClass::Index,
        price_precision: 2,
        base_currency: "USD",
        data_source: DataSource::Perps,
        spot_pair_index: None, // Uses perps, not spot
        aliases: &["SPXUSDT"],
    },
]);

#[allow(dead_code)]
pub static HL_TO_RWA: LazyLock<HashMap<&'static str, usize>> = LazyLock::new(|| {
    RWA_ASSETS.iter().enumerate().map(|(i, a)| (a.hl_ticker, i)).collect()
});

pub static SYMBOL_TO_RWA: LazyLock<HashMap<&'static str, usize>> = LazyLock::new(|| {
    RWA_ASSETS.iter().enumerate().map(|(i, a)| (a.internal_symbol, i)).collect()
});

/// Maps ALL user-facing symbols (internal + aliases) to RWA asset index
/// Supports: GOLDUSD, XAUUSDT, XAUUSD, GOLDUSDT, SILVERUSD, XAGUSDT, etc.
pub static USER_SYMBOL_TO_RWA: LazyLock<HashMap<String, usize>> = LazyLock::new(|| {
    let mut map = HashMap::new();
    for (i, asset) in RWA_ASSETS.iter().enumerate() {
        // Internal symbol (e.g. GOLDUSD)
        map.insert(asset.internal_symbol.to_string(), i);
        // All aliases (e.g. XAUUSDT, XAUUSD, GOLDUSDT)
        for alias in asset.aliases {
            map.insert(alias.to_string(), i);
        }
        // HL ticker + USDT (e.g. XAUT0USDT) - just in case
        map.insert(format!("{}USDT", asset.hl_ticker), i);
        // HL ticker + USD
        map.insert(format!("{}USD", asset.hl_ticker), i);
    }
    map
});

#[allow(dead_code)]
pub fn is_rwa_symbol(symbol: &str) -> bool {
    SYMBOL_TO_RWA.contains_key(symbol)
}

/// Check if a user-facing symbol (any format) is an RWA asset
#[allow(dead_code)]
pub fn is_rwa_user_symbol(symbol: &str) -> bool {
    USER_SYMBOL_TO_RWA.contains_key(&symbol.to_uppercase())
}

/// Get the Hyperliquid candle coin name for a user-facing symbol.
/// Returns the @N format for spot assets, or the perps ticker for perps assets.
/// Returns None if the symbol is not an RWA asset or has no candle data source.
pub fn get_hl_candle_coin(symbol: &str) -> Option<String> {
    let upper = symbol.to_uppercase();
    let idx = USER_SYMBOL_TO_RWA.get(&upper)?;
    let asset = &RWA_ASSETS[*idx];

    match asset.data_source {
        DataSource::Spot => {
            // Spot candles use @N format
            asset.spot_pair_index.map(|pair_idx| format!("@{}", pair_idx))
        }
        DataSource::Perps => {
            // Perps candles use the HL ticker directly
            Some(asset.hl_ticker.to_string())
        }
    }
}

/// Get the RWA asset definition for a user-facing symbol
pub fn get_rwa_by_user_symbol(symbol: &str) -> Option<&'static RwaAssetDef> {
    let upper = symbol.to_uppercase();
    USER_SYMBOL_TO_RWA.get(&upper).map(|idx| &RWA_ASSETS[*idx])
}
