//! Service bootstrap and initialization
//!
//! Handles initialization of all services at startup.

use std::str::FromStr;
use std::sync::Arc;
use sqlx::migrate::Migrator;
use tokio::sync::broadcast;
use tracing::Instrument;

/// Embedded migration set, compiled from `backend/migrations/*.sql`.
/// `MIGRATOR.run(&pool)` is invoked at startup so a deploy can never come up
/// with stale schema — this is the foot-gun PR 1 (`feat(migrations): restore
/// 18 migration files from git history`, plus the one-shot baseline binary)
/// closed in two steps. PR 1 restored the files and let operators baseline
/// existing environments by inserting the 18 known versions into
/// `_sqlx_migrations` with their precomputed checksums; this PR (PR 2) is the
/// runtime auto-migrate that actually applies anything new.
///
/// Sepolia was baselined at 18 rows on 2026-05-10 before this PR was merged.
/// On startup, sqlx compares each embedded migration's checksum against the
/// row in `_sqlx_migrations`; any drift crashes startup with `VersionMismatch`
/// instead of letting the service serve with an inconsistent schema.
static MIGRATOR: Migrator = sqlx::migrate!();

use crate::app::state::{AppState, BalanceUpdateEvent, OrderUpdateEvent};
use crate::cache::{CacheConfig, CacheManager};
use crate::config::AppConfig;
use crate::constants::channels;
use crate::db::Database;
use crate::models::position::PositionConfig;
use crate::services::adl::AdlService;
use crate::services::blockchain::BlockchainService;
use crate::services::earn::EarnService;
use crate::services::funding_rate::FundingRateService;
use crate::services::keeper::KeeperService;
use crate::services::kline::KlineService;
use crate::services::liquidation::LiquidationService;
use crate::services::matching::{MatchingEngine, OrderFlowOrchestrator};
use crate::services::position::PositionService;
use crate::services::price_feed::{PriceFeedConfig, PriceFeedService};
use crate::services::referral::ReferralService;
use crate::services::trigger_orders::TriggerOrdersService;
use crate::services::withdraw::WithdrawService;
use crate::services::points::PointsService;
use crate::services::alert::AlertService;
use crate::services::market_config::MarketConfigService;

/// Initialize all services and build application state
pub async fn initialize_services(config: AppConfig) -> anyhow::Result<Arc<AppState>> {
    // Force-register all Prometheus metrics so they appear in /metrics from the start
    crate::services::metrics::init_all_metrics();

    // Initialize EIP-712 domain from config
    crate::auth::eip712::init_domain(
        &config.eip712_domain_name,
        config.chain_id,
        &config.vault_address,
    );

    // Initialize database
    let db = Database::connect(&config.database_url).await?;
    tracing::info!("Database connected");

    // Apply any pending migrations. Sqlx panics startup on failure (checksum
    // mismatch, missing migration file, SQL error) — this is the desired
    // behavior: better to crash-loop visibly than serve with stale schema.
    // No-op on environments already at HEAD.
    MIGRATOR.run(&db.pool).await?;
    tracing::info!("Migrations checked: schema at HEAD");

    // Initialize cache manager (Redis)
    let cache_config = CacheConfig::from_env();
    let cache = Arc::new(CacheManager::new(cache_config).await?);
    if cache.is_available() {
        tracing::info!(
            "Cache manager initialized with Redis at {}",
            cache.config().redis_url
        );
    } else {
        tracing::warn!("Cache manager running without Redis (graceful degradation)");
    }

    // Initialize points service early (needed for trade persistence worker)
    let points_config = config.get_points_config();
    let points_enabled = points_config.enabled;
    let points_service = Arc::new(PointsService::with_config(db.pool.clone(), None, points_config));
    tracing::info!("Points service initialized (enabled={})", points_enabled);

    // Initialize market config service (dynamic trading pair management)
    let market_config_service = Arc::new(MarketConfigService::new(db.pool.clone()));
    market_config_service.initialize().await?;

    // Start listing phase advancement loop (every 60 seconds)
    let phase_service = market_config_service.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60));
        loop {
            interval.tick().await;
            match phase_service.advance_listing_phases().await {
                Ok(n) if n > 0 => tracing::info!("Advanced {} market listing phases", n),
                Err(e) => tracing::error!("Failed to advance listing phases: {}", e),
                _ => {}
            }
        }
    });
    tracing::info!("Listing phase advancement loop started (60s interval)");

    // Ensure we have some trading pairs
    let trading_pairs = market_config_service.get_trading_symbols().await;
    if trading_pairs.is_empty() {
        tracing::warn!("No trading pairs found in database. The matching engine will start empty.");
    } else {
        tracing::info!(
            "Loaded {} trading pairs from market_configs",
            trading_pairs.len()
        );
    }

    // Initialize referral service (before matching engine so it can be passed to persistence worker)
    let referral_service = Arc::new(
        ReferralService::with_contracts(
            db.pool.clone(),
            &config.rpc_url,
            &config.referral_rebate_address,
            &config.referral_storage_address,
            &config.backend_signer_private_key,
            config.chain_id,
        )
        .await
        .unwrap_or_else(|e| {
            tracing::warn!(
                "Failed to initialize ReferralService with contracts: {}. Using database-only mode.",
                e
            );
            ReferralService::new(db.pool.clone())
        }),
    );
    referral_service.ensure_schema().await
        .expect("Failed to ensure referral_earnings chain_sync schema");
    tracing::info!("Referral service initialized");

    // Initialize matching engine with configured trading pairs
    // (now uses DB-driven pairs instead of only env config)
    let matching_engine_raw = MatchingEngine::new(trading_pairs.clone());

    // Create orchestrator and start persistence worker
    // This is CRITICAL: without the persistence worker, trades won't be saved to DB
    // and positions won't be created!
    let orchestrator = OrderFlowOrchestrator::new(
        Arc::new(matching_engine_raw),
        db.pool.clone(),
    );
    let matching_engine = orchestrator.start_persistence_worker(Some(points_service.clone()), Some(referral_service.clone()));
    tracing::info!("Matching engine initialized for {:?} with persistence worker and points integration", trading_pairs);

    // Symbol-sharded routing config — needs to exist before recovery so
    // each pod only reloads the orderbooks it owns. Default disabled
    // (single-replica behaviour, owns everything). The view is logged so
    // a misconfigured StatefulSet manifest is loud at startup.
    let sharding = crate::services::sharding::ShardingConfig::from_env();
    tracing::info!(
        "sharding config: enabled={} my_ordinal={} replica_count={} peer_dns_template={}",
        sharding.enabled, sharding.my_ordinal, sharding.replica_count, sharding.peer_dns_template
    );

    // Order recovery disabled - use recovery script instead
    // To recover orders: run scripts/recover_orders.sh
    // Recover open limit orders from database
    match matching_engine.recover_orders_from_db(&db.pool, Some(&sharding)).await {
        Ok(count) => {
            if count > 0 {
                tracing::info!("Recovered {} open limit orders to orderbook", count);
            } else {
                tracing::info!("No open orders to recover");
            }
        }
        Err(e) => {
            tracing::error!("Failed to recover orders from database: {}", e);
            tracing::warn!("Starting with empty orderbook");
        }
    }

    // Ensure auxiliary sync table exists before recovery starts.
    // It is the durable idempotency marker that prevents periodic recovery
    // from re-applying the same trade to positions when trades.position_synced
    // can't be UPDATEd (degraded hypertable).
    if let Err(e) = OrderFlowOrchestrator::ensure_sync_table_schema(&db.pool).await {
        tracing::error!("Failed to ensure trade_position_sync schema: {}", e);
    }

    // Recover unsynced trades in background after a delay to avoid deadlock with persistence worker
    // The persistence worker starts immediately and may compete for advisory locks
    // Also run periodic recovery every 5 minutes to catch any missed trades
    let recovery_pool = db.pool.clone();
    tokio::spawn(async move {
        // Wait for persistence worker to be ready and HTTP server to start
        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
        tracing::info!("🔄 Starting delayed unsynced trades recovery...");

        // Initial recovery - process all unsynced trades
        match OrderFlowOrchestrator::recover_unsynced_trades(&recovery_pool).await {
            Ok(count) => {
                if count > 0 {
                    tracing::info!("✅ Initial recovery: {} trades with missing positions recovered", count);
                }
            }
            Err(e) => {
                tracing::error!("Failed to recover unsynced trades: {}", e);
            }
        }

        // Periodic recovery every 5 minutes
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(300));
        interval.tick().await; // Skip the first immediate tick

        loop {
            interval.tick().await;
            tracing::debug!("🔄 Running periodic unsynced trades recovery...");
            match OrderFlowOrchestrator::recover_unsynced_trades(&recovery_pool).await {
                Ok(count) => {
                    if count > 0 {
                        tracing::info!("✅ Periodic recovery: {} trades recovered", count);
                    }
                }
                Err(e) => {
                    tracing::error!("Periodic recovery failed: {}", e);
                }
            }
        }
    }.instrument(tracing::info_span!("trade-recovery")));

    // Initialize withdraw service
    let withdraw_service = Arc::new(WithdrawService::new(
        &config.backend_signer_private_key,
        &config.vault_address,
        config.chain_id,
        db.pool.clone(),
        &config.collateral_token_symbol,
        &config.collateral_token_address,
        config.collateral_token_decimals,
        &config.rpc_url,
        &config.eip712_domain_name,
    )?);
    tracing::info!(
        "Withdraw service initialized (token: {} @ {}, decimals: {})",
        config.collateral_token_symbol,
        config.collateral_token_address,
        config.collateral_token_decimals
    );

    // Initialize blockchain service and start event listener
    let blockchain_service = Arc::new(
        BlockchainService::new(
            &config.rpc_url,
            &config.vault_address,
            db.pool.clone(),
            config.chain_id,
            config.collateral_token_decimals,
            config.block_sync_lookback,
        )
        .await?,
    );
    tracing::info!(
        "Blockchain service initialized (token decimals: {}, lookback: {} blocks)",
        config.collateral_token_decimals,
        config.block_sync_lookback
    );

    // Start blockchain event listener in background
    let blockchain_service_clone = blockchain_service.clone();
    tokio::spawn(async move {
        tracing::info!("Starting blockchain event listener...");
        blockchain_service_clone.start_event_listener().await;
    }.instrument(tracing::info_span!("blockchain-listener")));

    // Initialize price feed service
    let price_feed_config = PriceFeedConfig {
        top_markets: config.price_feed_top_markets,
        update_interval_secs: config.price_feed_update_interval_secs,
        market_refresh_secs: config.price_feed_market_refresh_secs,
    };
    let price_feed_service = Arc::new(PriceFeedService::with_config(price_feed_config));
    price_feed_service.init_symbols(trading_pairs.clone()).await;

    // Hydrate price cache from database
    if let Err(e) = price_feed_service.hydrate_from_db(&db.pool).await {
        tracing::warn!("Failed to hydrate price feed from DB: {}", e);
    }
    tracing::info!("Price feed service initialized");

    // Initialize position service (points_service was already initialized earlier)
    #[allow(deprecated)] // PositionConfig.min_position_size_usd kept for one cycle; spec §2.1
    let position_config = PositionConfig {
        min_collateral_usd: rust_decimal::Decimal::from_str(&config.min_collateral_usd)
            .unwrap_or(rust_decimal::Decimal::new(10, 0)),
        min_position_size_usd: rust_decimal::Decimal::from_str(&config.min_position_size_usd)
            .unwrap_or(rust_decimal::Decimal::new(100, 0)),
        max_leverage: config.max_leverage,
        maintenance_margin_rate: rust_decimal::Decimal::from_str(&config.maintenance_margin_rate)
            .unwrap_or(rust_decimal::Decimal::new(5, 3)),
        position_fee_rate: rust_decimal::Decimal::from_str(&config.position_fee_rate)
            .unwrap_or(rust_decimal::Decimal::new(1, 3)),
        borrowing_fee_rate_per_hour: rust_decimal::Decimal::new(1, 5),
    };
    let mut ps = PositionService::with_config(db.pool.clone(), position_config);
    ps.set_points_service(points_service.clone());
    let position_service = Arc::new(ps);
    tracing::info!("Position service initialized");

    // Initialize funding rate service
    let funding_rate_service = Arc::new(FundingRateService::new(db.pool.clone()));
    funding_rate_service.ensure_schema().await
        .expect("Failed to ensure funding_rates schema");
    let funding_rate_clone = funding_rate_service.clone();
    funding_rate_clone
        .start_update_loop(trading_pairs.clone(), price_feed_service.clone())
        .await;
    funding_rate_service
        .clone()
        .start_settlement_scheduler(trading_pairs.clone())
        .await;
    tracing::info!("Funding rate service initialized");

    // Initialize liquidation service
    let alert_service = Arc::new(AlertService::from_env());
    let liquidation_service = Arc::new(LiquidationService::new(
        db.pool.clone(),
        position_service.clone(),
        price_feed_service.clone(),
        alert_service,
    ));
    liquidation_service
        .clone()
        .start_liquidation_loop(trading_pairs.clone())
        .await;
    tracing::info!("Liquidation service initialized and ENABLED");

    // Initialize ADL service
    let adl_service = Arc::new(AdlService::new(
        db.pool.clone(),
        position_service.clone(),
        price_feed_service.clone(),
    ));
    adl_service
        .clone()
        .start_ranking_update_loop(trading_pairs.clone())
        .await;
    tracing::info!("ADL service initialized");

    // Initialize trigger orders service
    let trigger_orders_service = Arc::new(TriggerOrdersService::new(
        db.pool.clone(),
        price_feed_service.clone(),
    ));
    // NOTE: Monitoring loop disabled - Keeper service handles trigger order execution
    // to prevent duplicate processing and ensure consistent PnL tracking.
    // The trigger_orders_service is still needed for API endpoints (create, cancel, etc.)
    tracing::info!("Trigger orders service initialized (monitoring delegated to Keeper)");

    // Initialize and start Keeper service
    let keeper_service = Arc::new(KeeperService::new(
        db.pool.clone(),
        matching_engine.clone(),
        price_feed_service.clone(),
    ));
    keeper_service.start(trading_pairs.clone()).await;
    tracing::info!("Keeper service started");

    // Start referral batch sync loop (service already initialized above)
    referral_service.clone().start_batch_sync_loop().await;

    // Initialize earn service
    let earn_contract_address = std::env::var("EARN_CONTRACT_ADDRESS").unwrap_or_default();
    tracing::info!("Initializing Earn service (contract: {})", 
        if earn_contract_address.is_empty() { "NONE" } else { &earn_contract_address });
    
    let mut es = if !earn_contract_address.is_empty() {
        match EarnService::with_contract(
            db.pool.clone(),
            &config.rpc_url,
            &earn_contract_address,
            &config.backend_signer_private_key,
            config.chain_id,
        )
        .await {
            Ok(service) => {
                tracing::info!("✅ EarnService initialized with contract: {}", earn_contract_address);
                service
            }
            Err(e) => {
                tracing::error!(
                    "❌ Failed to initialize EarnService with contract {}: {}. Using database-only mode (NO EVENT LISTENING).",
                    earn_contract_address, e
                );
                EarnService::new(db.pool.clone())
            }
        }
    } else {
        tracing::warn!("EARN_CONTRACT_ADDRESS not set - Earn service will run in database-only mode (NO EVENT LISTENING)");
        EarnService::new(db.pool.clone())
    };
    
    es.set_points_service(points_service.clone());
    let earn_service = Arc::new(es);
    earn_service.clone().start_settlement_scheduler().await;
    earn_service.clone().start_event_listener().await;
    tracing::info!("Earn service initialized (event listener status logged above)");

    // Initialize K-line service
    let kline_service = KlineService::new(Some(db.pool.clone()));
    tracing::info!("K-line service initialized");

    // Create order update broadcast channel
    let (order_update_sender, _) =
        broadcast::channel::<OrderUpdateEvent>(channels::ORDER_UPDATE_CHANNEL_CAPACITY);
    tracing::info!("Order update broadcast channel created");

    // Balance update broadcast channel + Postgres LISTEN task. The trigger
    // `notify_balance_change` (migration 20260423100000_balance_change_notify.sql)
    // emits every INSERT/UPDATE on `balances`; this task decodes the JSON
    // payload and fans out into `balance_update_sender`, which the WS
    // handler reads for the `balances` channel. Capacity 4096 matches
    // roughly 5s of peak trade flow × per-trade balance writes.
    let (balance_update_sender, _) = broadcast::channel::<BalanceUpdateEvent>(4096);
    {
        let pool = db.pool.clone();
        let sender = balance_update_sender.clone();
        tokio::spawn(
            async move {
                loop {
                    let mut listener = match sqlx::postgres::PgListener::connect_with(&pool).await {
                        Ok(l) => l,
                        Err(e) => {
                            tracing::error!(
                                "balance listener: connect failed: {} — retry in 5s",
                                e
                            );
                            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                            continue;
                        }
                    };
                    if let Err(e) = listener.listen("balance_change").await {
                        tracing::error!(
                            "balance listener: LISTEN balance_change failed: {} — retry in 5s",
                            e
                        );
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                        continue;
                    }
                    tracing::info!("balance listener: LISTEN balance_change established");
                    loop {
                        match listener.recv().await {
                            Ok(n) => {
                                match serde_json::from_str::<BalanceUpdateEvent>(n.payload()) {
                                    Ok(ev) => {
                                        // broadcast send() Err only means no
                                        // active subscribers, which is fine.
                                        let _ = sender.send(ev);
                                    }
                                    Err(e) => {
                                        tracing::warn!(
                                            "balance listener: malformed payload: {} — raw: {:?}",
                                            e,
                                            n.payload()
                                        );
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "balance listener: recv error: {} — reconnect",
                                    e
                                );
                                break;
                            }
                        }
                    }
                }
            }
            .instrument(tracing::info_span!("balance-listener")),
        );
    }
    tracing::info!("Balance update broadcast channel created (PG LISTEN/NOTIFY driven)");

    // Create unified-margin account event broadcast channel
    let (unified_account_sender, _) =
        broadcast::channel::<crate::app::state::UnifiedAccountEvent>(1024);
    tracing::info!("Unified margin event broadcast channel created");

    // Points event broadcast channel (Phase 2-B WS push).
    let (points_event_sender, _) =
        broadcast::channel::<crate::app::state::PointsEventPush>(4096);
    points_service.set_ws_sender(points_event_sender.clone()).await;
    tracing::info!("Points event broadcast channel created and wired into PointsService");

    // VIP tier change event broadcast channel + schema.
    let (vip_tier_event_sender, _) =
        broadcast::channel::<crate::services::vip_tier::VipTierEvent>(1024);
    crate::services::vip_tier::ensure_schema(&db.pool).await;

    // Spot WS broadcast senders (sub-project 2 / WS Task 2). The
    // ws_publisher background task fans EngineEvents into these; the
    // /ws handler reads them. Capacities sized per channel: depth/trade/
    // user are higher because they're per-event, ticker/kline are slower.
    let (spot_depth_sender, _)         = broadcast::channel(2048);
    let (spot_trade_sender, _)         = broadcast::channel(2048);
    let (spot_ticker_sender, _)        = broadcast::channel(256);
    let (spot_kline_sender, _)         = broadcast::channel(1024);
    let (spot_user_order_sender, _)    = broadcast::channel(2048);
    let (spot_user_balance_sender, _)  = broadcast::channel(2048);

    // Pre-materialise low-cardinality CounterVec metrics so Grafana
    // dashboards that reference them don't 404 before the first event.
    crate::services::metrics::touch_counter_vec_labels();

    // Start holding points hourly batch scheduler.
    // Aligns to UTC hour boundaries (e.g. 15:00, 16:00) so that restarting
    // the service multiple times within the same hour never triggers more than
    // one batch run for that hour.
    if config.points_holding_enabled {
        let holding_points_svc = points_service.clone();
        let holding_interval_secs = config.points_holding_interval_secs;
        tokio::spawn(async move {
            // Sleep until the next UTC hour boundary before starting the loop.
            let now = chrono::Utc::now();
            let secs_into_hour = (now.timestamp() % holding_interval_secs as i64) as u64;
            let secs_until_next = holding_interval_secs - secs_into_hour;
            tracing::info!(
                "Holding points scheduler: waiting {}s until next hour boundary",
                secs_until_next
            );
            tokio::time::sleep(tokio::time::Duration::from_secs(secs_until_next)).await;

            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(holding_interval_secs));
            loop {
                interval.tick().await;
                match holding_points_svc.get_active_epoch().await {
                    Ok(Some(epoch)) => {
                        match holding_points_svc.calculate_holding_points_batch(epoch.epoch_number).await {
                            Ok(n) => tracing::info!("Holding points batch: updated {} positions (epoch={})", n, epoch.epoch_number),
                            Err(e) => tracing::error!("Holding points batch failed: {}", e),
                        }
                    }
                    Ok(None) => tracing::debug!("Holding points batch skipped: no active epoch"),
                    Err(e) => tracing::error!("Holding points batch: failed to get active epoch: {}", e),
                }
            }
        }.instrument(tracing::info_span!("holding-points-scheduler")));
        tracing::info!("Holding points scheduler started, aligned to UTC hour boundary (interval: {}s)", holding_interval_secs);
    }

    // Start referral commission leaderboard incremental worker (5-min cadence).
    // The Arc<ReferralLeaderboardService> is intentionally not stored in AppState:
    // `start()` clones the Arc into the spawned task, so the service lives for
    // the process lifetime. If graceful shutdown or external control is needed
    // in the future, store this in AppState and add a CancellationToken.
    crate::services::referral_leaderboard::ReferralLeaderboardService::new(db.pool.clone())
        .start();
    tracing::info!("Referral commission leaderboard worker started");

    // Load tiered margin table into memory (Phase 4).
    let margin_tiers = crate::services::unified_margin::tiers::empty_handle();
    match crate::services::unified_margin::TierStore::load(&db.pool).await {
        Ok(store) => {
            *margin_tiers.write().await = store;
            tracing::info!("Margin tiers loaded from DB");
        }
        Err(e) => {
            tracing::warn!("Failed to load margin_tiers ({}), using fallback MMR", e);
        }
    }

    // (sharding config was constructed earlier — see top of fn — so
    // recovery could filter by ownership before the engine accepts traffic.)

    // Spot subsystem (sub-project 1): only boot if SPOT_ENABLED=true
    // (which leaves Config::spot = Some(...)).
    let spot_engine: Option<std::sync::Arc<crate::services::spot::matching::types::EngineHandle>>;
    let spot_blockchain: Option<std::sync::Arc<crate::services::spot::blockchain::SpotBlockchainService>>;
    if let Some(spot_cfg) = config.spot.clone() {
        // Hard fail if the vault address is the zero address — that is the
        // env-bootstrap default; deploying with it is a misconfiguration.
        if spot_cfg.bsc_vault_address == ethers::types::Address::zero() {
            anyhow::bail!("SPOT_ENABLED=true but SPOT_BSC_VAULT_ADDRESS is zero address");
        }

        // Fail-fast: verify the withdraw signer key parses correctly. The actual
        // signer is constructed per-request in the withdraw handler, but here we
        // confirm the key is well-formed before booting; otherwise withdraw
        // requests would 500 at runtime.
        crate::services::spot::withdraw_signer::SpotWithdrawSigner::from_config(&spot_cfg)
            .map_err(|e| anyhow::anyhow!("SPOT_WITHDRAW_SIGNER_PRIVATE_KEY invalid: {}", e))?;

        let pool = db.pool.clone();
        let bc = crate::services::spot::blockchain::SpotBlockchainService::new(&spot_cfg, pool.clone()).await
            .map_err(|e| anyhow::anyhow!("init SpotBlockchainService: {}", e))?;

        // Deposit poller
        let bc_d = bc.clone();
        tokio::spawn(async move { bc_d.run_deposit_poller().await });

        // Withdrawal poller
        let bc_w = bc.clone();
        tokio::spawn(async move { bc_w.run_withdrawal_poller().await });

        // Stash for the withdraw handler's on-chain nonce read.
        spot_blockchain = Some(bc.clone());

        // Reaper
        let reaper = crate::services::spot::reaper::SpotReaper::new(&spot_cfg, pool.clone());
        tokio::spawn(async move { reaper.run().await });

        // Reconciler
        let reconciler = crate::services::spot::reconciler::SpotReconciler::new(&spot_cfg, pool.clone());
        tokio::spawn(async move { reconciler.run().await });

        tracing::info!("spot subsystem online: 4 background tasks spawned");

        // Trading engine + aggregators — gated by SPOT_TRADING_ENABLED
        spot_engine = if spot_cfg.trading_enabled {
            use crate::services::spot::{markets, matching, kline_aggregator, ticker_aggregator};
            let market_cache = std::sync::Arc::new(
                markets::load_initial(&pool).await
                    .map_err(|e| anyhow::anyhow!("spot markets load: {}", e))?
            );
            // MVP: single market hard-coded to DFUSDT
            let market_id = "DFUSDT".to_string();
            let engine_handle = matching::engine::SpotMatchingEngine::start(
                pool.clone(), market_cache.clone(), market_id.clone()
            ).await
                .map_err(|e| anyhow::anyhow!("spot matching engine start: {}", e))?;
            let engine_handle = std::sync::Arc::new(engine_handle);

            // Kline aggregator: subscribes to engine event broadcast
            let pool_k = pool.clone();
            let evrx_k = engine_handle.event_tx.subscribe();
            let kline_tx = spot_kline_sender.clone();
            tokio::spawn(async move { kline_aggregator::run(pool_k, evrx_k, kline_tx).await });

            // Ticker aggregator: 60s rolling recompute
            let pool_t = pool.clone();
            tokio::spawn(async move { ticker_aggregator::run(pool_t).await });

            tracing::info!("spot trading engine online: market={} aggregators=2", market_id);
            Some(engine_handle)
        } else {
            tracing::info!("spot trading engine disabled (SPOT_TRADING_ENABLED != true)");
            None
        };
    } else {
        spot_engine = None;
        spot_blockchain = None;
    }

    // Build application state
    let app_state = Arc::new(AppState {
        config,
        db,
        cache,
        matching_engine,
        spot_engine: spot_engine.clone(),
        spot_blockchain: spot_blockchain.clone(),
        withdraw_service,
        price_feed_service,
        position_service,
        funding_rate_service,
        liquidation_service,
        adl_service,
        trigger_orders_service,
        referral_service,
        kline_service,
        earn_service,
        points_service,
        market_config_service,
        order_update_sender,
        balance_update_sender,
        spot_depth_sender,
        spot_trade_sender,
        spot_ticker_sender,
        spot_kline_sender,
        spot_user_order_sender,
        spot_user_balance_sender,
        unified_account_sender,
        points_event_sender,
        vip_tier_event_sender,
        margin_tiers,
        sharding,
    });

    // Spawn the spot ws publisher AFTER AppState exists, since it needs
    // the per-channel broadcast senders that live on AppState. Only spin
    // it up when the spot trading engine is online — otherwise there are
    // no EngineEvents to drain.
    if let Some(eng) = spot_engine.as_ref() {
        crate::services::spot::ws_publisher::spawn(
            app_state.clone(),
            eng.event_tx.subscribe(),
        );
        tracing::info!("spot ws publisher spawned");
    }

    Ok(app_state)
}
