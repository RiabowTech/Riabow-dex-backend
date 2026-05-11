//! Market Config Service
//!
//! Provides dynamic trading pair management with per-market fee rates,
//! leverage settings, and risk parameters. Replaces the static config-based
//! trading pair system.

pub mod coingecko_worker;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

// ============================================================================
// Types
// ============================================================================

/// Market status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "text")]
#[serde(rename_all = "lowercase")]
#[allow(dead_code)]
pub enum MarketStatus {
    #[sqlx(rename = "active")]
    Active,
    #[sqlx(rename = "suspended")]
    Suspended,
    #[sqlx(rename = "delisted")]
    Delisted,
}

impl std::fmt::Display for MarketStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MarketStatus::Active => write!(f, "active"),
            MarketStatus::Suspended => write!(f, "suspended"),
            MarketStatus::Delisted => write!(f, "delisted"),
        }
    }
}

impl std::str::FromStr for MarketStatus {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "active" => Ok(MarketStatus::Active),
            "suspended" => Ok(MarketStatus::Suspended),
            "delisted" => Ok(MarketStatus::Delisted),
            _ => Err(format!("Invalid market status: {}", s)),
        }
    }
}

/// Market configuration for a single trading pair
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketConfig {
    pub symbol: String,
    pub status: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub display_name: Option<String>,

    // Leverage & Risk
    pub max_leverage: i32,
    pub min_leverage: i32,
    pub maintenance_margin_rate: Decimal,
    pub min_order_size_usd: Decimal,
    pub max_order_size_usd: Decimal,
    pub max_position_size_usd: Decimal,
    /// Per-market aggregate Open Interest cap, long side. Enforced at
    /// order placement (api/handlers/order.rs::create_order) via
    /// FundingRateService::check_oi_cap_with_cap. Spec §1.
    #[serde(default = "default_oi_cap")]
    pub max_long_oi_usd: Decimal,
    /// Per-market aggregate Open Interest cap, short side. Same enforcement
    /// path as max_long_oi_usd.
    #[serde(default = "default_oi_cap")]
    pub max_short_oi_usd: Decimal,

    // Price precision
    pub tick_size: Decimal,
    pub lot_size: Decimal,

    // Base fee rates
    pub base_maker_fee_rate: Decimal,
    pub base_taker_fee_rate: Decimal,
    pub base_position_fee_rate: Decimal,
    pub borrowing_fee_rate_per_hour: Decimal,

    // Dynamic fee boundaries
    pub fee_floor: Decimal,
    pub fee_ceiling: Decimal,

    // Dynamic adjustment
    pub auto_fee_adjust_enabled: bool,
    pub fee_sensitivity: Decimal,

    // Funding rate
    pub funding_rate: Decimal,
    pub settlement_cycle: i32,

    // Meta
    pub category: String,
    pub sort_order: i32,
    pub listing_phase: String,
    pub announcement_at: Option<DateTime<Utc>>,
    pub scheduled_list_at: Option<DateTime<Utc>>,
    pub pre_trade_at: Option<DateTime<Utc>>,
    pub scheduled_delist_at: Option<DateTime<Utc>>,
    pub restrict_new_position_at: Option<DateTime<Utc>>,
    pub close_only_at: Option<DateTime<Utc>>,
    pub fully_delisted_at: Option<DateTime<Utc>>,
    pub delist_reason: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,

    // Market-details extension (Lighter-style; see migration in `ensure_schema`).
    #[serde(default)]
    pub close_out_margin_rate: Option<Decimal>,
    #[serde(default)]
    pub market_cap: Option<Decimal>,
    #[serde(default)]
    pub fully_diluted_valuation: Option<Decimal>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub coingecko_id: Option<String>,
    #[serde(default)]
    pub market_cap_updated_at: Option<DateTime<Utc>>,
}

impl MarketConfig {
    /// Compute the effective listing phase and status based on current time.
    /// The DB stores the phase that was set by the admin, but timestamps define
    /// when transitions actually happen. This method derives the real state.
    pub fn with_effective_phase(mut self) -> Self {
        let now = Utc::now();

        // Listing flow: announced → pre_trade → active
        if self.listing_phase == "announced" {
            if let Some(pre_trade) = self.pre_trade_at {
                if now >= pre_trade {
                    self.listing_phase = "pre_trade".to_string();
                }
            }
            // Direct announced → active (no pre-trade)
            if self.listing_phase == "announced" {
                if let Some(list_at) = self.scheduled_list_at {
                    if now >= list_at {
                        self.listing_phase = "active".to_string();
                        self.status = "active".to_string();
                    }
                }
            }
        }
        if self.listing_phase == "pre_trade" {
            if let Some(list_at) = self.scheduled_list_at {
                if now >= list_at {
                    self.listing_phase = "active".to_string();
                    self.status = "active".to_string();
                }
            }
        }

        // Delist flow: delist_announced → restrict_new → close_only → delisted
        if self.listing_phase == "delist_announced" {
            if let Some(t) = self.restrict_new_position_at {
                if now >= t {
                    self.listing_phase = "restrict_new".to_string();
                }
            }
        }
        if self.listing_phase == "restrict_new" {
            if let Some(t) = self.close_only_at {
                if now >= t {
                    self.listing_phase = "close_only".to_string();
                    self.status = "suspended".to_string();
                }
            }
        }
        if self.listing_phase == "close_only" {
            if let Some(t) = self.fully_delisted_at {
                if now >= t {
                    self.listing_phase = "delisted".to_string();
                    self.status = "delisted".to_string();
                }
            }
        }

        self
    }
}

/// Dynamic fee rates (calculated by FeeAdjustmentWorker)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct DynamicFeeRates {
    pub long_taker_fee: Decimal,
    pub short_taker_fee: Decimal,
    pub maker_fee: Decimal,
    pub imbalance_ratio: Decimal,
    pub updated_at: DateTime<Utc>,
}

/// Market fee snapshot for history/audit
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketFeeSnapshot {
    pub id: i64,
    pub symbol: String,
    pub long_oi_usd: Decimal,
    pub short_oi_usd: Decimal,
    pub total_oi_usd: Decimal,
    pub imbalance_ratio: Decimal,
    pub long_taker_fee: Decimal,
    pub short_taker_fee: Decimal,
    pub maker_fee: Decimal,
    pub created_at: DateTime<Utc>,
}

/// Default $5M per-side OI cap, matching the migration default.
fn default_oi_cap() -> Decimal {
    Decimal::from(5_000_000)
}

/// Request body for creating/updating a market config
#[derive(Debug, Default, Deserialize)]
pub struct MarketConfigRequest {
    pub symbol: Option<String>,
    pub status: Option<String>,
    pub base_asset: Option<String>,
    pub quote_asset: Option<String>,
    pub display_name: Option<String>,
    pub max_leverage: Option<i32>,
    pub min_leverage: Option<i32>,
    pub maintenance_margin_rate: Option<Decimal>,
    pub min_order_size_usd: Option<Decimal>,
    pub max_order_size_usd: Option<Decimal>,
    pub max_position_size_usd: Option<Decimal>,
    pub max_long_oi_usd: Option<Decimal>,
    pub max_short_oi_usd: Option<Decimal>,
    pub tick_size: Option<Decimal>,
    pub lot_size: Option<Decimal>,
    pub base_maker_fee_rate: Option<Decimal>,
    pub base_taker_fee_rate: Option<Decimal>,
    pub base_position_fee_rate: Option<Decimal>,
    pub borrowing_fee_rate_per_hour: Option<Decimal>,
    pub fee_floor: Option<Decimal>,
    pub fee_ceiling: Option<Decimal>,
    pub auto_fee_adjust_enabled: Option<bool>,
    pub fee_sensitivity: Option<Decimal>,
    pub funding_rate: Option<Decimal>,
    pub settlement_cycle: Option<i32>,
    pub category: Option<String>,
    pub sort_order: Option<i32>,
    pub listing_phase: Option<String>,
    pub announcement_at: Option<DateTime<Utc>>,
    pub scheduled_list_at: Option<DateTime<Utc>>,
    pub pre_trade_at: Option<DateTime<Utc>>,
    pub scheduled_delist_at: Option<DateTime<Utc>>,
    pub restrict_new_position_at: Option<DateTime<Utc>>,
    pub close_only_at: Option<DateTime<Utc>>,
    pub fully_delisted_at: Option<DateTime<Utc>>,
    pub delist_reason: Option<String>,

    // Lighter-style market-details fields. All optional so existing
    // admin payloads keep working.
    pub close_out_margin_rate: Option<Decimal>,
    pub market_cap: Option<Decimal>,
    pub fully_diluted_valuation: Option<Decimal>,
    pub description: Option<String>,
    pub coingecko_id: Option<String>,
}

// ============================================================================
// Service
// ============================================================================

/// MarketConfigService manages trading pair configurations
pub struct MarketConfigService {
    pool: PgPool,
    /// In-memory cache: symbol -> MarketConfig
    cache: Arc<RwLock<HashMap<String, MarketConfig>>>,
}

impl MarketConfigService {
    /// Create a new MarketConfigService
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Initialize: create tables and load configs into memory
    pub async fn initialize(&self) -> anyhow::Result<()> {
        self.create_tables().await?;
        self.reload().await?;
        Ok(())
    }

    /// Create database tables if they don't exist
    async fn create_tables(&self) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS market_configs (
                symbol                      VARCHAR(20) PRIMARY KEY,
                status                      VARCHAR(12) NOT NULL DEFAULT 'active',
                base_asset                  VARCHAR(10) NOT NULL,
                quote_asset                 VARCHAR(10) NOT NULL DEFAULT 'USDT',
                display_name                VARCHAR(40),

                max_leverage                INTEGER NOT NULL DEFAULT 100,
                min_leverage                INTEGER NOT NULL DEFAULT 1,
                maintenance_margin_rate     DECIMAL(10,6) NOT NULL DEFAULT 0.005,
                min_order_size_usd          DECIMAL(20,4) NOT NULL DEFAULT 10.0,
                max_order_size_usd          DECIMAL(20,4) NOT NULL DEFAULT 5000000.0,
                max_position_size_usd       DECIMAL(20,4) NOT NULL DEFAULT 10000000.0,
                max_long_oi_usd             NUMERIC(28,2) NOT NULL DEFAULT 5000000,
                max_short_oi_usd            NUMERIC(28,2) NOT NULL DEFAULT 5000000,

                tick_size                   DECIMAL(20,8) NOT NULL DEFAULT 0.1,
                lot_size                    DECIMAL(20,8) NOT NULL DEFAULT 0.001,

                base_maker_fee_rate         DECIMAL(10,6) NOT NULL DEFAULT 0.0002,
                base_taker_fee_rate         DECIMAL(10,6) NOT NULL DEFAULT 0.0005,
                base_position_fee_rate      DECIMAL(10,6) NOT NULL DEFAULT 0.001,
                borrowing_fee_rate_per_hour DECIMAL(10,6) NOT NULL DEFAULT 0.00001,

                fee_floor                   DECIMAL(10,6) NOT NULL DEFAULT 0.0001,
                fee_ceiling                 DECIMAL(10,6) NOT NULL DEFAULT 0.003,

                auto_fee_adjust_enabled     BOOLEAN NOT NULL DEFAULT TRUE,
                fee_sensitivity             DECIMAL(6,4) NOT NULL DEFAULT 1.5,

                funding_rate                DECIMAL(10,6) NOT NULL DEFAULT 0.0001,
                settlement_cycle            INTEGER NOT NULL DEFAULT 8,

                category                    VARCHAR(20) NOT NULL DEFAULT 'crypto',
                sort_order                  INTEGER NOT NULL DEFAULT 100,
                listing_phase               VARCHAR(30) NOT NULL DEFAULT 'announced',
                announcement_at             TIMESTAMPTZ,
                scheduled_list_at           TIMESTAMPTZ,
                pre_trade_at                TIMESTAMPTZ,
                scheduled_delist_at         TIMESTAMPTZ,
                restrict_new_position_at    TIMESTAMPTZ,
                close_only_at               TIMESTAMPTZ,
                fully_delisted_at           TIMESTAMPTZ,
                delist_reason               TEXT,
                created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        // Migration: add listing phase for existing databases
        sqlx::query(
            r#"
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'market_configs' AND column_name = 'listing_phase'
                ) THEN
                    ALTER TABLE market_configs ADD COLUMN listing_phase VARCHAR(30) NOT NULL DEFAULT 'active';
                    ALTER TABLE market_configs ADD COLUMN announcement_at TIMESTAMPTZ;
                    ALTER TABLE market_configs ADD COLUMN scheduled_list_at TIMESTAMPTZ;
                    ALTER TABLE market_configs ADD COLUMN pre_trade_at TIMESTAMPTZ;
                    ALTER TABLE market_configs ADD COLUMN scheduled_delist_at TIMESTAMPTZ;
                    ALTER TABLE market_configs ADD COLUMN restrict_new_position_at TIMESTAMPTZ;
                    ALTER TABLE market_configs ADD COLUMN close_only_at TIMESTAMPTZ;
                    ALTER TABLE market_configs ADD COLUMN fully_delisted_at TIMESTAMPTZ;
                    ALTER TABLE market_configs ADD COLUMN delist_reason TEXT;
                END IF;
            END $$;
            "#,
        )
        .execute(&self.pool)
        .await?;

        // Migration: add category column for existing databases
        sqlx::query(
            r#"
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'market_configs' AND column_name = 'category'
                ) THEN
                    ALTER TABLE market_configs ADD COLUMN category VARCHAR(20) NOT NULL DEFAULT 'crypto';
                END IF;
            END $$;
            "#,
        )
        .execute(&self.pool)
        .await?;

        // Market-details extension (Lighter-style per-market view).
        // Uses ADD COLUMN IF NOT EXISTS — safe to rerun on startup.
        //
        // * close_out_margin_rate: margin level at which the platform
        //   fully closes a position. Sits below maintenance_margin_rate.
        //   Default 2/3 × maintenance preserves the existing behavior
        //   (whatever LIQUIDATION_THRESHOLD env specifies is applied by
        //   the liquidation service; this field is informational and
        //   admin-tunable per symbol).
        // * market_cap / fully_diluted_valuation: USD values, refreshed
        //   hourly by the CoinGecko worker. Nullable — empty until the
        //   worker has had a chance to pull.
        // * description: human-readable blurb ("Bitcoin is the native…").
        // * coingecko_id: identifier the CoinGecko API expects
        //   (e.g. "bitcoin", "ethereum"). NULL → worker skips this row.
        sqlx::query(
            r#"
            ALTER TABLE market_configs
                ADD COLUMN IF NOT EXISTS close_out_margin_rate DECIMAL(8,6),
                ADD COLUMN IF NOT EXISTS market_cap            DECIMAL(30,2),
                ADD COLUMN IF NOT EXISTS fully_diluted_valuation DECIMAL(30,2),
                ADD COLUMN IF NOT EXISTS description           TEXT,
                ADD COLUMN IF NOT EXISTS coingecko_id          VARCHAR(64),
                ADD COLUMN IF NOT EXISTS market_cap_updated_at TIMESTAMPTZ
            "#,
        )
        .execute(&self.pool)
        .await?;

        // Per-market per-side aggregate Open Interest cap (spec §1.1).
        // Default $5M each — top markets are tuned higher post-deploy via
        // scripts/oi_cap_bootstrap.sql. Same DDL as
        // backend/migrations/20260430000000_market_configs_oi_caps.sql so
        // a fresh DB without the sqlx migration step still gets the column.
        sqlx::query(
            r#"
            ALTER TABLE market_configs
                ADD COLUMN IF NOT EXISTS max_long_oi_usd  NUMERIC(28,2) NOT NULL DEFAULT 5000000,
                ADD COLUMN IF NOT EXISTS max_short_oi_usd NUMERIC(28,2) NOT NULL DEFAULT 5000000
            "#,
        )
        .execute(&self.pool)
        .await?;

        // Seed close_out_margin_rate = 2/3 × maintenance where NULL.
        sqlx::query(
            r#"
            UPDATE market_configs
               SET close_out_margin_rate = ROUND(maintenance_margin_rate * 2 / 3, 6)
             WHERE close_out_margin_rate IS NULL
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_market_configs_status ON market_configs(status)",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS market_fee_snapshots (
                id                  BIGSERIAL PRIMARY KEY,
                symbol              VARCHAR(20) NOT NULL,
                long_oi_usd         DECIMAL(30,4) NOT NULL,
                short_oi_usd        DECIMAL(30,4) NOT NULL,
                total_oi_usd        DECIMAL(30,4) NOT NULL,
                imbalance_ratio     DECIMAL(8,6) NOT NULL,
                long_taker_fee      DECIMAL(10,6) NOT NULL,
                short_taker_fee     DECIMAL(10,6) NOT NULL,
                maker_fee           DECIMAL(10,6) NOT NULL,
                created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_fee_snapshots_symbol_time ON market_fee_snapshots(symbol, created_at DESC)",
        )
        .execute(&self.pool)
        .await?;

        tracing::info!("market_configs and market_fee_snapshots tables ensured");
        Ok(())
    }

    /// Reload all configs from DB into memory cache
    pub async fn reload(&self) -> anyhow::Result<()> {
        let rows: Vec<MarketConfig> = sqlx::query_as::<_, MarketConfigRow>(
            r#"
            SELECT symbol, status, base_asset, quote_asset, display_name,
                   max_leverage, min_leverage, maintenance_margin_rate,
                   min_order_size_usd, max_order_size_usd, max_position_size_usd, max_long_oi_usd, max_short_oi_usd,
                   tick_size, lot_size,
                   base_maker_fee_rate, base_taker_fee_rate, base_position_fee_rate,
                   borrowing_fee_rate_per_hour,
                   fee_floor, fee_ceiling, auto_fee_adjust_enabled, fee_sensitivity,
                   funding_rate, settlement_cycle,
                   category, sort_order, 
                   listing_phase, announcement_at, scheduled_list_at, pre_trade_at, 
                   scheduled_delist_at, restrict_new_position_at, close_only_at, 
                   fully_delisted_at, delist_reason, created_at, updated_at,
                   close_out_margin_rate, market_cap, fully_diluted_valuation,
                   description, coingecko_id, market_cap_updated_at
            FROM market_configs
            WHERE status IN ('active', 'suspended')
            ORDER BY sort_order ASC
            "#,
        )
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|r| r.into())
        .collect();

        let mut cache = self.cache.write().await;
        cache.clear();
        for config in rows {
            cache.insert(config.symbol.clone(), config);
        }

        tracing::debug!(
            "MarketConfigService reloaded: {} configs in cache",
            cache.len()
        );
        Ok(())
    }

    /// Check if a symbol is visible and fetching data (not announced/delisted)
    pub async fn is_visible(&self, symbol: &str) -> bool {
        let cache = self.cache.read().await;
        cache.get(symbol)
            .map(|c| c.listing_phase != "announced" && c.listing_phase != "delisted")
            .unwrap_or(false)
    }

    /// Check if a symbol is tradeable (active status)
    pub async fn is_tradeable(&self, symbol: &str) -> bool {
        let cache = self.cache.read().await;
        cache
            .get(symbol)
            .map(|c| c.status == "active" && c.listing_phase == "active")
            .unwrap_or(false)
    }

    /// Get config for a specific symbol
    pub async fn get_config(&self, symbol: &str) -> Option<MarketConfig> {
        let cache = self.cache.read().await;
        cache.get(symbol).cloned()
    }

    /// Get all active configs (applies effective phase calculation)
    pub async fn get_all_active(&self) -> Vec<MarketConfig> {
        let cache = self.cache.read().await;
        cache
            .values()
            .cloned()
            .map(|c| c.with_effective_phase())
            .filter(|c| c.status == "active" && c.listing_phase == "active")
            .collect()
    }

    /// Get all configs (active + suspended)
    pub async fn get_all(&self) -> Vec<MarketConfig> {
        let cache = self.cache.read().await;
        let mut configs: Vec<_> = cache.values().cloned().collect();
        configs.sort_by_key(|c| c.sort_order);
        configs
    }

    /// Get all symbols (for use in matching engine, etc.)
    pub async fn get_trading_symbols(&self) -> Vec<String> {
        let cache = self.cache.read().await;
        cache.values()
            .filter(|c| c.listing_phase == "active")
            .map(|c| c.symbol.clone())
            .collect()
    }

    // ========================================================================
    // CRUD Operations
    // ========================================================================

    /// Get all market configs from DB (including delisted)
    pub async fn list_all_from_db(&self) -> anyhow::Result<Vec<MarketConfig>> {
        let rows = sqlx::query_as::<_, MarketConfigRow>(
            r#"
            SELECT symbol, status, base_asset, quote_asset, display_name,
                   max_leverage, min_leverage, maintenance_margin_rate,
                   min_order_size_usd, max_order_size_usd, max_position_size_usd, max_long_oi_usd, max_short_oi_usd,
                   tick_size, lot_size,
                   base_maker_fee_rate, base_taker_fee_rate, base_position_fee_rate,
                   borrowing_fee_rate_per_hour,
                   fee_floor, fee_ceiling, auto_fee_adjust_enabled, fee_sensitivity,
                   funding_rate, settlement_cycle,
                   category, sort_order, 
                   listing_phase, announcement_at, scheduled_list_at, pre_trade_at, 
                   scheduled_delist_at, restrict_new_position_at, close_only_at, 
                   fully_delisted_at, delist_reason, created_at, updated_at,
                   close_out_margin_rate, market_cap, fully_diluted_valuation,
                   description, coingecko_id, market_cap_updated_at
            FROM market_configs
            ORDER BY sort_order ASC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|r| r.into()).collect())
    }

    /// Get a single config from DB
    pub async fn get_from_db(&self, symbol: &str) -> anyhow::Result<Option<MarketConfig>> {
        let row = sqlx::query_as::<_, MarketConfigRow>(
            r#"
            SELECT symbol, status, base_asset, quote_asset, display_name,
                   max_leverage, min_leverage, maintenance_margin_rate,
                   min_order_size_usd, max_order_size_usd, max_position_size_usd, max_long_oi_usd, max_short_oi_usd,
                   tick_size, lot_size,
                   base_maker_fee_rate, base_taker_fee_rate, base_position_fee_rate,
                   borrowing_fee_rate_per_hour,
                   fee_floor, fee_ceiling, auto_fee_adjust_enabled, fee_sensitivity,
                   funding_rate, settlement_cycle,
                   category, sort_order, 
                   listing_phase, announcement_at, scheduled_list_at, pre_trade_at, 
                   scheduled_delist_at, restrict_new_position_at, close_only_at, 
                   fully_delisted_at, delist_reason, created_at, updated_at,
                   close_out_margin_rate, market_cap, fully_diluted_valuation,
                   description, coingecko_id, market_cap_updated_at
            FROM market_configs
            WHERE symbol = $1
            "#,
        )
        .bind(symbol)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| r.into()))
    }

    /// Create a new market config
    pub async fn create(&self, req: &MarketConfigRequest) -> anyhow::Result<MarketConfig> {
        let symbol = req
            .symbol
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("symbol is required"))?
            .to_uppercase();
        let base_asset = req
            .base_asset
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("base_asset is required"))?
            .to_uppercase();

        sqlx::query(
            r#"
            INSERT INTO market_configs (
                symbol, status, base_asset, quote_asset, display_name,
                max_leverage, min_leverage, maintenance_margin_rate,
                min_order_size_usd, max_order_size_usd, max_position_size_usd, max_long_oi_usd, max_short_oi_usd,
                tick_size, lot_size,
                base_maker_fee_rate, base_taker_fee_rate, base_position_fee_rate,
                borrowing_fee_rate_per_hour,
                fee_floor, fee_ceiling, auto_fee_adjust_enabled, fee_sensitivity,
                funding_rate, settlement_cycle,
                category, sort_order,
                listing_phase, announcement_at, scheduled_list_at, pre_trade_at,
                scheduled_delist_at, restrict_new_position_at, close_only_at,
                fully_delisted_at, delist_reason
            ) VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8,
                $9, $10, $11, $12, $13,
                $14, $15,
                $16, $17, $18, $19,
                $20, $21, $22, $23,
                $24, $25,
                $26, $27,
                $28, $29, $30, $31,
                $32, $33, $34,
                $35, $36
            )
            "#,
        )
        .bind(&symbol)
        .bind(req.status.as_deref().unwrap_or("suspended"))
        .bind(&base_asset)
        .bind(req.quote_asset.as_deref().unwrap_or("USDT"))
        .bind(req.display_name.as_deref().unwrap_or(&format!("{}/USDT Perp", base_asset)))
        .bind(req.max_leverage.unwrap_or(100))
        .bind(req.min_leverage.unwrap_or(1))
        .bind(req.maintenance_margin_rate.unwrap_or(Decimal::new(5, 3)))
        .bind(req.min_order_size_usd.unwrap_or(Decimal::new(10, 0)))
        .bind(req.max_order_size_usd.unwrap_or(Decimal::new(5_000_000, 0)))
        .bind(req.max_position_size_usd.unwrap_or(Decimal::new(10_000_000, 0)))
        .bind(req.max_long_oi_usd.unwrap_or_else(default_oi_cap))
        .bind(req.max_short_oi_usd.unwrap_or_else(default_oi_cap))
        .bind(req.tick_size.unwrap_or(Decimal::new(1, 1)))
        .bind(req.lot_size.unwrap_or(Decimal::new(1, 3)))
        .bind(req.base_maker_fee_rate.unwrap_or(Decimal::new(2, 4)))
        .bind(req.base_taker_fee_rate.unwrap_or(Decimal::new(5, 4)))
        .bind(req.base_position_fee_rate.unwrap_or(Decimal::new(1, 3)))
        .bind(req.borrowing_fee_rate_per_hour.unwrap_or(Decimal::new(1, 5)))
        .bind(req.fee_floor.unwrap_or(Decimal::new(1, 4)))
        .bind(req.fee_ceiling.unwrap_or(Decimal::new(3, 3)))
        .bind(req.auto_fee_adjust_enabled.unwrap_or(true))
        .bind(req.fee_sensitivity.unwrap_or(Decimal::new(15, 1)))
        .bind(req.funding_rate.unwrap_or(Decimal::new(1, 4)))
        .bind(req.settlement_cycle.unwrap_or(8))
        .bind(req.category.as_deref().unwrap_or("crypto"))
        .bind(req.sort_order.unwrap_or(100))
        .bind(req.listing_phase.as_deref().unwrap_or("announced"))
        .bind(req.announcement_at)
        .bind(req.scheduled_list_at)
        .bind(req.pre_trade_at)
        .bind(req.scheduled_delist_at)
        .bind(req.restrict_new_position_at)
        .bind(req.close_only_at)
        .bind(req.fully_delisted_at)
        .bind(req.delist_reason.clone())
        .execute(&self.pool)
        .await?;

        self.reload().await?;

        self.get_from_db(&symbol)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Failed to read back created config"))
    }

    /// Update an existing market config
    pub async fn update(
        &self,
        symbol: &str,
        req: &MarketConfigRequest,
    ) -> anyhow::Result<MarketConfig> {
        // Build dynamic update
        let existing = self
            .get_from_db(symbol)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Market config not found: {}", symbol))?;

        sqlx::query(
            r#"
            UPDATE market_configs SET
                status = $2,
                base_asset = $3,
                quote_asset = $4,
                display_name = $5,
                max_leverage = $6,
                min_leverage = $7,
                maintenance_margin_rate = $8,
                min_order_size_usd = $9,
                max_order_size_usd = $10,
                max_position_size_usd = $11,
                max_long_oi_usd = $12,
                max_short_oi_usd = $13,
                tick_size = $14,
                lot_size = $15,
                base_maker_fee_rate = $16,
                base_taker_fee_rate = $17,
                base_position_fee_rate = $18,
                borrowing_fee_rate_per_hour = $19,
                fee_floor = $20,
                fee_ceiling = $21,
                auto_fee_adjust_enabled = $22,
                fee_sensitivity = $23,
                funding_rate = $24,
                settlement_cycle = $25,
                category = $26,
                sort_order = $27,
                listing_phase = $28,
                announcement_at = $29,
                scheduled_list_at = $30,
                pre_trade_at = $31,
                scheduled_delist_at = $32,
                restrict_new_position_at = $33,
                close_only_at = $34,
                fully_delisted_at = $35,
                delist_reason = $36,
                close_out_margin_rate = $37,
                market_cap = $38,
                fully_diluted_valuation = $39,
                description = $40,
                coingecko_id = $41,
                updated_at = NOW()
            WHERE symbol = $1
            "#,
        )
        .bind(symbol)
        .bind(req.status.as_deref().unwrap_or(&existing.status))
        .bind(req.base_asset.as_deref().unwrap_or(&existing.base_asset))
        .bind(req.quote_asset.as_deref().unwrap_or(&existing.quote_asset))
        .bind(req.display_name.as_deref().or(existing.display_name.as_deref()))
        .bind(req.max_leverage.unwrap_or(existing.max_leverage))
        .bind(req.min_leverage.unwrap_or(existing.min_leverage))
        .bind(req.maintenance_margin_rate.unwrap_or(existing.maintenance_margin_rate))
        .bind(req.min_order_size_usd.unwrap_or(existing.min_order_size_usd))
        .bind(req.max_order_size_usd.unwrap_or(existing.max_order_size_usd))
        .bind(req.max_position_size_usd.unwrap_or(existing.max_position_size_usd))
        .bind(req.max_long_oi_usd.unwrap_or(existing.max_long_oi_usd))
        .bind(req.max_short_oi_usd.unwrap_or(existing.max_short_oi_usd))
        .bind(req.tick_size.unwrap_or(existing.tick_size))
        .bind(req.lot_size.unwrap_or(existing.lot_size))
        .bind(req.base_maker_fee_rate.unwrap_or(existing.base_maker_fee_rate))
        .bind(req.base_taker_fee_rate.unwrap_or(existing.base_taker_fee_rate))
        .bind(req.base_position_fee_rate.unwrap_or(existing.base_position_fee_rate))
        .bind(req.borrowing_fee_rate_per_hour.unwrap_or(existing.borrowing_fee_rate_per_hour))
        .bind(req.fee_floor.unwrap_or(existing.fee_floor))
        .bind(req.fee_ceiling.unwrap_or(existing.fee_ceiling))
        .bind(req.auto_fee_adjust_enabled.unwrap_or(existing.auto_fee_adjust_enabled))
        .bind(req.fee_sensitivity.unwrap_or(existing.fee_sensitivity))
        .bind(req.funding_rate.unwrap_or(existing.funding_rate))
        .bind(req.settlement_cycle.unwrap_or(existing.settlement_cycle))
        .bind(req.category.as_deref().unwrap_or(&existing.category))
        .bind(req.sort_order.unwrap_or(existing.sort_order))
        .bind(req.listing_phase.as_deref().unwrap_or(&existing.listing_phase))
        .bind(req.announcement_at.or(existing.announcement_at))
        .bind(req.scheduled_list_at.or(existing.scheduled_list_at))
        .bind(req.pre_trade_at.or(existing.pre_trade_at))
        .bind(req.scheduled_delist_at.or(existing.scheduled_delist_at))
        .bind(req.restrict_new_position_at.or(existing.restrict_new_position_at))
        .bind(req.close_only_at.or(existing.close_only_at))
        .bind(req.fully_delisted_at.or(existing.fully_delisted_at))
        .bind(req.delist_reason.as_deref().or(existing.delist_reason.as_deref()))
        .bind(req.close_out_margin_rate.or(existing.close_out_margin_rate))
        .bind(req.market_cap.or(existing.market_cap))
        .bind(req.fully_diluted_valuation.or(existing.fully_diluted_valuation))
        .bind(req.description.as_deref().or(existing.description.as_deref()))
        .bind(req.coingecko_id.as_deref().or(existing.coingecko_id.as_deref()))
        .execute(&self.pool)
        .await?;

        self.reload().await?;

        self.get_from_db(symbol)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Failed to read back updated config"))
    }

    /// Update market status
    pub async fn update_status(
        &self,
        symbol: &str,
        new_status: &str,
    ) -> anyhow::Result<MarketConfig> {
        sqlx::query("UPDATE market_configs SET status = $2, updated_at = NOW() WHERE symbol = $1")
            .bind(symbol)
            .bind(new_status)
            .execute(&self.pool)
            .await?;

        self.reload().await?;

        self.get_from_db(symbol)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Market config not found: {}", symbol))
    }

    /// Delete a market config (only delisted)
    pub async fn delete(&self, symbol: &str) -> anyhow::Result<()> {
        sqlx::query("DELETE FROM market_configs WHERE symbol = $1 AND status = 'delisted'")
            .bind(symbol)
            .execute(&self.pool)
            .await?;

        self.reload().await?;
        Ok(())
    }

    // ========================================================================
    // Fee History
    // ========================================================================

    /// Save a fee snapshot
    pub async fn save_fee_snapshot(
        &self,
        symbol: &str,
        long_oi_usd: Decimal,
        short_oi_usd: Decimal,
        imbalance_ratio: Decimal,
        long_taker_fee: Decimal,
        short_taker_fee: Decimal,
        maker_fee: Decimal,
    ) -> anyhow::Result<()> {
        let total_oi_usd = long_oi_usd + short_oi_usd;
        sqlx::query(
            r#"
            INSERT INTO market_fee_snapshots
                (symbol, long_oi_usd, short_oi_usd, total_oi_usd, imbalance_ratio,
                 long_taker_fee, short_taker_fee, maker_fee)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
        )
        .bind(symbol)
        .bind(long_oi_usd)
        .bind(short_oi_usd)
        .bind(total_oi_usd)
        .bind(imbalance_ratio)
        .bind(long_taker_fee)
        .bind(short_taker_fee)
        .bind(maker_fee)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Get fee history for a symbol
    pub async fn get_fee_history(
        &self,
        symbol: &str,
        limit: i64,
    ) -> anyhow::Result<Vec<MarketFeeSnapshot>> {
        let rows = sqlx::query_as::<_, FeeSnapshotRow>(
            r#"
            SELECT id, symbol, long_oi_usd, short_oi_usd, total_oi_usd,
                   imbalance_ratio, long_taker_fee, short_taker_fee, maker_fee, created_at
            FROM market_fee_snapshots
            WHERE symbol = $1
            ORDER BY created_at DESC
            LIMIT $2
            "#,
        )
        .bind(symbol)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|r| r.into()).collect())
    }

    /// Get open interest for a symbol
    pub async fn get_open_interest(
        &self,
        symbol: &str,
    ) -> anyhow::Result<(Decimal, Decimal)> {
        let result: Option<(Decimal, Decimal)> = sqlx::query_as(
            r#"
            SELECT
                COALESCE(SUM(CASE WHEN side = 'long' THEN size_in_usd ELSE 0 END), 0) as long_oi,
                COALESCE(SUM(CASE WHEN side = 'short' THEN size_in_usd ELSE 0 END), 0) as short_oi
            FROM positions
            WHERE symbol = $1 AND status = 'open'
            "#,
        )
        .bind(symbol)
        .fetch_optional(&self.pool)
        .await?;

        Ok(result.unwrap_or((Decimal::ZERO, Decimal::ZERO)))
    }

    /// Advance listing phases in the database based on scheduled timestamps.
    /// Called periodically by a background task to keep DB in sync.
    pub async fn advance_listing_phases(&self) -> anyhow::Result<u64> {
        let mut advanced: u64 = 0;

        let configs = {
            let cache = self.cache.read().await;
            cache.values().cloned().collect::<Vec<_>>()
        };

        let now = Utc::now();

        for config in configs {
            let effective = config.clone().with_effective_phase();
            if effective.listing_phase != config.listing_phase || effective.status != config.status {
                tracing::info!(
                    "Advancing {} phase: {} → {}, status: {} → {}",
                    config.symbol, config.listing_phase, effective.listing_phase,
                    config.status, effective.status
                );
                sqlx::query(
                    "UPDATE market_configs SET listing_phase = $2, status = $3, updated_at = NOW() WHERE symbol = $1"
                )
                .bind(&config.symbol)
                .bind(&effective.listing_phase)
                .bind(&effective.status)
                .execute(&self.pool)
                .await?;
                advanced += 1;
            }
        }

        if advanced > 0 {
            self.reload().await?;
        }

        Ok(advanced)
    }
}

// ============================================================================
// sqlx row types (intermediate mapping)
// ============================================================================

#[derive(sqlx::FromRow)]
struct MarketConfigRow {
    symbol: String,
    status: String,
    base_asset: String,
    quote_asset: String,
    display_name: Option<String>,
    max_leverage: i32,
    min_leverage: i32,
    maintenance_margin_rate: Decimal,
    min_order_size_usd: Decimal,
    max_order_size_usd: Decimal,
    max_position_size_usd: Decimal,
    max_long_oi_usd: Decimal,
    max_short_oi_usd: Decimal,
    tick_size: Decimal,
    lot_size: Decimal,
    base_maker_fee_rate: Decimal,
    base_taker_fee_rate: Decimal,
    base_position_fee_rate: Decimal,
    borrowing_fee_rate_per_hour: Decimal,
    fee_floor: Decimal,
    fee_ceiling: Decimal,
    auto_fee_adjust_enabled: bool,
    fee_sensitivity: Decimal,
    funding_rate: Decimal,
    settlement_cycle: i32,
    category: String,
    sort_order: i32,
    listing_phase: String,
    announcement_at: Option<DateTime<Utc>>,
    scheduled_list_at: Option<DateTime<Utc>>,
    pre_trade_at: Option<DateTime<Utc>>,
    scheduled_delist_at: Option<DateTime<Utc>>,
    restrict_new_position_at: Option<DateTime<Utc>>,
    close_only_at: Option<DateTime<Utc>>,
    fully_delisted_at: Option<DateTime<Utc>>,
    delist_reason: Option<String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    close_out_margin_rate: Option<Decimal>,
    market_cap: Option<Decimal>,
    fully_diluted_valuation: Option<Decimal>,
    description: Option<String>,
    coingecko_id: Option<String>,
    market_cap_updated_at: Option<DateTime<Utc>>,
}

impl From<MarketConfigRow> for MarketConfig {
    fn from(r: MarketConfigRow) -> Self {
        Self {
            symbol: r.symbol,
            status: r.status,
            base_asset: r.base_asset,
            quote_asset: r.quote_asset,
            display_name: r.display_name,
            max_leverage: r.max_leverage,
            min_leverage: r.min_leverage,
            maintenance_margin_rate: r.maintenance_margin_rate,
            min_order_size_usd: r.min_order_size_usd,
            max_order_size_usd: r.max_order_size_usd,
            max_position_size_usd: r.max_position_size_usd,
            max_long_oi_usd: r.max_long_oi_usd,
            max_short_oi_usd: r.max_short_oi_usd,
            tick_size: r.tick_size,
            lot_size: r.lot_size,
            base_maker_fee_rate: r.base_maker_fee_rate,
            base_taker_fee_rate: r.base_taker_fee_rate,
            base_position_fee_rate: r.base_position_fee_rate,
            borrowing_fee_rate_per_hour: r.borrowing_fee_rate_per_hour,
            fee_floor: r.fee_floor,
            fee_ceiling: r.fee_ceiling,
            auto_fee_adjust_enabled: r.auto_fee_adjust_enabled,
            fee_sensitivity: r.fee_sensitivity,
            funding_rate: r.funding_rate,
            settlement_cycle: r.settlement_cycle,
            category: r.category,
            sort_order: r.sort_order,
            listing_phase: r.listing_phase,
            announcement_at: r.announcement_at,
            scheduled_list_at: r.scheduled_list_at,
            pre_trade_at: r.pre_trade_at,
            scheduled_delist_at: r.scheduled_delist_at,
            restrict_new_position_at: r.restrict_new_position_at,
            close_only_at: r.close_only_at,
            fully_delisted_at: r.fully_delisted_at,
            delist_reason: r.delist_reason,
            created_at: r.created_at,
            updated_at: r.updated_at,
            close_out_margin_rate: r.close_out_margin_rate,
            market_cap: r.market_cap,
            fully_diluted_valuation: r.fully_diluted_valuation,
            description: r.description,
            coingecko_id: r.coingecko_id,
            market_cap_updated_at: r.market_cap_updated_at,
        }
    }
}

#[derive(sqlx::FromRow)]
struct FeeSnapshotRow {
    id: i64,
    symbol: String,
    long_oi_usd: Decimal,
    short_oi_usd: Decimal,
    total_oi_usd: Decimal,
    imbalance_ratio: Decimal,
    long_taker_fee: Decimal,
    short_taker_fee: Decimal,
    maker_fee: Decimal,
    created_at: DateTime<Utc>,
}

impl From<FeeSnapshotRow> for MarketFeeSnapshot {
    fn from(r: FeeSnapshotRow) -> Self {
        Self {
            id: r.id,
            symbol: r.symbol,
            long_oi_usd: r.long_oi_usd,
            short_oi_usd: r.short_oi_usd,
            total_oi_usd: r.total_oi_usd,
            imbalance_ratio: r.imbalance_ratio,
            long_taker_fee: r.long_taker_fee,
            short_taker_fee: r.short_taker_fee,
            maker_fee: r.maker_fee,
            created_at: r.created_at,
        }
    }
}
