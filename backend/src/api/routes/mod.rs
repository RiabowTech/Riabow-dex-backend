use axum::{
    middleware as axum_middleware,
    routing::{delete, get, post, put},
    Router,
};
use std::sync::Arc;

use crate::api::handlers;
use crate::api::handlers::spot;
use crate::api::middleware::api_key::api_key_middleware;
use crate::api::middleware::dev_only::dev_only_middleware;
use crate::auth::middleware::{auth_middleware, optional_auth_middleware};
use crate::AppState;

pub fn create_router(state: Arc<AppState>) -> Router<Arc<AppState>> {
    // Public routes (no auth required)
    let public_routes = Router::new()
        .route("/auth/login", post(handlers::auth::login))
        .route("/auth/nonce/:address", get(handlers::auth::get_nonce))
        .route("/markets", get(handlers::market::list_markets))
        .route("/markets/risk-params", get(handlers::market::get_markets_risk_params))
        .route("/markets/:symbol/orderbook", get(handlers::market::get_orderbook))
        .route("/markets/:symbol/trades", get(handlers::market::get_trades))
        .route("/markets/:symbol/ticker", get(handlers::market::get_ticker))
        .route("/markets/:symbol/price", get(handlers::market::get_price))
        .route("/markets/:symbol/details", get(handlers::market::get_market_details))
        // Wallet — token list for deposit/withdraw UI (USDT from perp config + DF from spot config)
        .route("/wallet/tokens", get(handlers::wallet::list_tokens))
        // Spot public market data (no auth). Engine-backed depth degrades to
        // 503 when SPOT_TRADING_ENABLED=false; the rest hit Postgres directly.
        .route("/spot/markets",     get(handlers::spot::market_data::list_markets))
        .route("/spot/markets/:symbol/details", get(handlers::spot::market_data::market_details))
        .route("/spot/depth",       get(handlers::spot::market_data::depth))
        .route("/spot/trades",      get(handlers::spot::market_data::recent_trades))
        .route("/spot/klines",      get(handlers::spot::market_data::klines))
        .route("/spot/ticker/24hr", get(handlers::spot::market_data::ticker_24hr))
        // Public health/ops endpoint — engine state, queue depth (Task 14).
        // Always reachable; reports engine="disabled" when SPOT_TRADING_ENABLED=false.
        .route("/spot/health",      get(handlers::spot::health::health))
        // External markets (alias for frontend compatibility)
        // External markets (Proxied to Hyperliquid)
        .route("/external/markets", get(handlers::external::list_markets_external))
        .route("/external/markets/:symbol/orderbook", get(handlers::external::get_orderbook_external))
        .route("/external/markets/:symbol/trades", get(handlers::external::get_trades_external))
        // /external/markets/:symbol/ticker is intentionally aliased to the internal
        // ticker handler — all user-facing tickers must reflect MM-bot trade activity,
        // never an external feed (HL spot @182 reported XAUUSDT as 0.857, etc.).
        .route("/external/markets/:symbol/ticker", get(handlers::market::get_ticker))
        .route("/external/markets/:symbol/price", get(handlers::market::get_price)) // Keep price as internal or implement external simple price
        .route("/external/markets/:symbol/candles", get(handlers::external::get_candles_external))
        .route("/external/markets/:symbol/candles/latest", get(handlers::kline::get_latest_candle)) // Keep internal latest or implement external
        // K-line/Candles
        .route("/markets/:symbol/candles", get(handlers::kline::get_candles))
        .route("/markets/:symbol/candles/latest", get(handlers::kline::get_latest_candle))
        // Alternative paths for frontend compatibility
        .route("/klines/:symbol/candles", get(handlers::kline::get_candles))
        .route("/klines/:symbol/candles/latest", get(handlers::kline::get_latest_candle))
        // Internal K-line endpoints
        .route("/internal/klines/import", post(handlers::kline::batch_import_klines))
        .route("/internal/klines/repair", get(handlers::kline::repair_klines))
        // Funding rate (public)
        .route("/funding-rates", get(handlers::funding_rate::get_all_funding_rates))
        .route("/funding-rates/:symbol", get(handlers::funding_rate::get_funding_rate))
        .route("/funding-rates/:symbol/history", get(handlers::funding_rate::get_funding_history))
        // Liquidation (public)
        .route("/liquidations/:symbol", get(handlers::liquidation::get_market_liquidations))
        .route("/liquidations/:symbol/config", get(handlers::liquidation::get_liquidation_config))
        .route("/insurance-fund/:symbol", get(handlers::liquidation::get_insurance_fund))
        // ADL (public)
        .route("/adl/:symbol/rankings", get(handlers::adl::get_adl_rankings))
        .route("/adl/:symbol/events", get(handlers::adl::get_market_adl_events))
        .route("/adl/:symbol/config", get(handlers::adl::get_adl_config))
        // Trigger orders config (public)
        .route("/trigger-orders/:symbol/config", get(handlers::trigger_orders::get_trigger_order_config))
        // Referral leaderboard (public)
        .route("/referral/leaderboard", get(handlers::referral::get_commission_leaderboard))
        // Trader PnL leaderboard (public). Backed by TimescaleDB CAGGs;
        // run scripts/leaderboard_caggs.sql once before this becomes useful.
        .route("/leaderboard/traders", get(handlers::leaderboard_traders::get_leaderboard_traders))
        // On-chain referral data (public)
        .route("/referral/on-chain/user-rebate/:address", get(handlers::referral::get_on_chain_user_rebate))
        .route("/referral/on-chain/referral-info/:address", get(handlers::referral::get_on_chain_referral_info))
        .route("/referral/on-chain/claimed/:address", get(handlers::referral::get_on_chain_claimed))
        .route("/referral/on-chain/operator-status", get(handlers::referral::get_operator_status))
        // Earn (public)
        .route("/earn/domain", get(handlers::earn::get_domain))
        .route("/earn/products", get(handlers::earn::list_products))
        .route("/earn/products/:id", get(handlers::earn::get_product))
        .route("/earn/performance", get(handlers::earn::get_performance))
        // Points System
        .route("/points/leaderboard", get(handlers::points::get_leaderboard))
        .route("/points/leaderboard/daily", get(handlers::points::get_daily_leaderboard))
        .route("/epochs", get(handlers::points::get_epochs))
        .route("/points/earn-level-config", get(handlers::points::get_earn_level_config))
        .route("/points/seasons", get(handlers::points::get_seasons))
        // RWA Assets (public)
        .route("/rwa/assets", get(handlers::rwa::list_assets))
        .route("/rwa/assets/:symbol", get(handlers::rwa::get_asset))
        .route("/rwa/prices", get(handlers::rwa::get_prices))
        // Open Interest & Market Data (public, Binance-compatible)
        .route("/open-interest/:symbol", get(handlers::open_interest::get_open_interest))
        .route("/open-interest/:symbol/history", get(handlers::open_interest::get_oi_history))
        .route("/open-interest/:symbol/ratio", get(handlers::open_interest::get_ls_ratio))
        .route("/open-interest/:symbol/accounts", get(handlers::open_interest::get_account_ratio))
        .route("/open-interest/:symbol/top-positions", get(handlers::open_interest::get_top_position_ratio))
        .route("/open-interest/:symbol/top-accounts", get(handlers::open_interest::get_top_account_ratio))
        .route("/open-interest/:symbol/taker-volume", get(handlers::open_interest::get_taker_volume))
        .route("/open-interest/:symbol/leverage-brackets", get(handlers::open_interest::get_leverage_brackets));

    // Protected routes (auth required)
    let protected_routes = Router::new()
        // Account
        .route("/account/profile", get(handlers::account::get_profile))
        .route("/account/balances", get(handlers::account::get_balances))
        .route("/account/positions", get(handlers::account::get_positions))
        .route("/account/orders", get(handlers::account::get_orders))
        .route("/account/trades", get(handlers::account::get_trades))
        .route("/account/send-verification", post(handlers::account::send_verification))
        .route("/account/verify-email", post(handlers::account::verify_email))
        .route("/account/pnl", get(handlers::pnl::get_pnl))
        .route("/account/stats", get(handlers::account_stats::get_account_stats))
        .route("/account/performance-summary", get(handlers::account_stats::get_performance_summary))
        // API Keys Management
        .route("/api-keys", post(handlers::api_keys::create_api_key))
        .route("/api-keys", get(handlers::api_keys::list_api_keys))
        .route("/api-keys/:id", delete(handlers::api_keys::delete_api_key))
        .route("/api-keys/:id", axum::routing::put(handlers::api_keys::update_api_key))
        // Orders. Literal sub-paths must come before `:order_id` so axum
        // doesn't try to parse "open" / "preview" / "batch" as a UUID
        // (R4 P1 #20: GET /orders/open returned 400 "UUID parsing failed").
        .route(
            "/orders",
            post(handlers::order::create_order).get(handlers::order::list_orders),
        )
        .route("/orders/open", get(handlers::order::list_orders))
        .route("/orders/preview", post(handlers::order::preview_order))
        .route(
            "/orders/batch",
            post(handlers::order::batch_cancel).delete(handlers::order::batch_cancel),
        )
        .route(
            "/orders/:order_id",
            get(handlers::order::get_order)
                .put(handlers::order::update_order)
                .delete(handlers::order::cancel_order),
        )
        // Positions. Same literal-before-dynamic precedence issue
        // (R4 P1 #20: /positions/active 400 UUID parse).
        .route("/positions", get(handlers::position::get_positions))
        .route("/positions", post(handlers::position::open_position))
        .route("/positions/active", get(handlers::position::get_positions))
        .route("/positions/:position_id", get(handlers::position::get_position))
        .route("/positions/:position_id/close", post(handlers::position::close_position))
        .route("/positions/:position_id/collateral/add", post(handlers::position::add_collateral))
        .route("/positions/:position_id/collateral/remove", post(handlers::position::remove_collateral))
        .route("/positions/:position_id/liquidation", get(handlers::position::check_liquidation))
        // Position TP/SL (Complete CRUD)
        .route("/positions/:position_id/tp-sl", post(handlers::trigger_orders::set_position_tp_sl))
        .route("/positions/:position_id/tp-sl", get(handlers::trigger_orders::get_position_tp_sl))
        .route("/positions/:position_id/tp-sl", axum::routing::put(handlers::trigger_orders::update_position_tp_sl))
        .route("/positions/:position_id/tp-sl", delete(handlers::trigger_orders::delete_position_tp_sl))
        // Trigger orders
        .route("/trigger-orders", post(handlers::trigger_orders::create_trigger_order))
        .route("/trigger-orders", get(handlers::trigger_orders::get_trigger_orders))
        .route("/trigger-orders/executions", get(handlers::trigger_orders::get_user_executions))
        .route("/trigger-orders/:order_id", get(handlers::trigger_orders::get_trigger_order))
        .route("/trigger-orders/:order_id", delete(handlers::trigger_orders::cancel_trigger_order))
        .route("/trigger-orders/:symbol/stats", get(handlers::trigger_orders::get_user_stats))
        // Deposits & Withdrawals
        .route("/deposit/prepare", post(handlers::deposit::prepare_deposit))
        .route("/deposit/history", get(handlers::deposit::get_history))
        .route("/withdraw/request", post(handlers::withdraw::request_withdraw))
        .route("/withdraw/history", get(handlers::withdraw::get_history))
        .route("/withdraw/:id", get(handlers::withdraw::get_withdrawal))
        .route("/withdraw/:id/cancel", delete(handlers::withdraw::cancel_withdraw))
        .route("/withdraw/:id/confirm", post(handlers::withdraw::confirm_withdraw))
        // Referral
        .route("/referral/codes", post(handlers::referral::create_code))
        .route("/referral/bind", post(handlers::referral::bind_code))
        .route("/referral/unbind", post(handlers::referral::unbind_code))
        .route("/referral/status", get(handlers::referral::get_status))
        .route("/referral/logs", get(handlers::referral::get_logs))
        .route("/referral/dashboard", get(handlers::referral::get_dashboard))
        .route("/referral/claim", post(handlers::referral::claim_earnings))
        .route("/referral/on-chain/claim-signature", post(handlers::referral::get_claim_signature))
        // Funding settlements (user-specific)
        .route("/funding/settlements", get(handlers::funding_rate::get_user_settlements))
        // Liquidation history (user-specific)
        .route("/liquidations/history", get(handlers::liquidation::get_user_liquidations))
        // ADL (user-specific)
        .route("/adl/history", get(handlers::adl::get_user_adl_history))
        .route("/adl/:symbol/stats", get(handlers::adl::get_user_adl_stats))
        // Earn (user-specific)
        .route("/earn/subscriptions", get(handlers::earn::get_positions))
        .route("/earn/subscribe/prepare", post(handlers::earn::prepare_join_plan))
        // Points System
        .route("/points", get(handlers::points::get_user_points))
        .route("/points/balance", get(handlers::points::get_user_points))
        .route("/points/summary", get(handlers::points::get_user_points))
        .route("/points/history", get(handlers::points::get_points_history))
        .route("/points/tier", get(handlers::points::get_tier_info))
        .route("/points/earn-quota", get(handlers::points::get_earn_quota))
        .route("/points/simulate", post(handlers::points::simulate_points))
        // Unified Margin Mode
        .route("/unified/account", get(handlers::unified_margin::get_unified_account))
        .route("/unified/liquidations", get(handlers::unified_margin::get_liquidations))
        .route("/unified/risk/simulate", post(handlers::unified_margin::simulate_open))
        .route("/account/margin-mode", post(handlers::unified_margin::switch_margin_mode))
        // MM Pool (Phase 2)
        .route("/mm/dashboard", get(handlers::mm_pool::get_mm_dashboard))
        .route("/mm/snapshots", get(handlers::mm_pool::get_mm_snapshots))
        // Phase 3: Season distribution + claim
        .route("/points/distribution/:season_id", get(handlers::points::get_distribution))
        .route("/points/claim/:distribution_id", post(handlers::points::claim_distribution))
        .layer(axum_middleware::from_fn_with_state(state.clone(), auth_middleware));

    // Spot subsystem routes (mounted only when Config::spot is Some).
    // All endpoints sit behind the existing EIP-712 auth middleware.
    let spot_routes = if state.config.spot.is_some() {
        Some(
            Router::new()
                .route("/spot/balances",          get(spot::balances::list_balances))
                .route("/spot/deposits",          get(spot::deposits::list_deposits))
                .route("/spot/withdraw/request",  post(spot::withdraw::request_withdraw))
                .route("/spot/withdrawals",       get(spot::withdraw::list_withdrawals))
                .route("/spot/withdrawals/:id",   get(spot::withdraw::get_withdrawal))
                .route("/spot/transfer",          post(spot::transfer::transfer))
                // Spot trading order endpoints (Task 9). axum requires methods
                // on the same path to be chained on a single .route() call,
                // hence the combined POST/DELETE/GET below.
                .route(
                    "/spot/orders",
                    post(spot::orders::place_order)
                        .delete(spot::orders::cancel_all)
                        .get(spot::orders::list_orders),
                )
                .route(
                    "/spot/orders/:id",
                    delete(spot::orders::cancel_order)
                        .get(spot::orders::get_order),
                )
                .route("/spot/trades/me",         get(spot::orders::trades_me))
                .layer(axum_middleware::from_fn_with_state(state.clone(), auth_middleware)),
        )
    } else {
        None
    };

    // Internal API routes (development only)
    let internal_routes = Router::new()
        .route("/internal/trade", post(handlers::internal_trade::create_internal_trade))
        .route("/internal/trades/batch", post(handlers::internal_trade::batch_create_trades))
        .route("/internal/orderbook", post(handlers::internal_orderbook::set_orderbook))
        .route("/internal/klines/clear", delete(handlers::internal_trade::clear_klines))
        // Virtual orders and trades (for market making bots)
        .route("/internal/virtual/order", post(handlers::virtual_orders::create_virtual_order))
        .route("/internal/virtual/orders/batch", post(handlers::virtual_orders::batch_create_virtual_orders))
        .route("/internal/virtual/trade", post(handlers::virtual_orders::create_virtual_trade))
        .route("/internal/virtual/trades/batch", post(handlers::virtual_orders::batch_create_virtual_trades))
        .layer(axum_middleware::from_fn_with_state(state.clone(), dev_only_middleware));

    // Admin API routes (protected by API key)
    let admin_routes = Router::new()
        // Statistics API - accessible with API key
        .route("/admin/stats/trade-volume", get(handlers::admin_stats::get_trade_volume))
        // Points admin API (Phase 1)
        .route("/admin/points/points-config/:epoch", get(handlers::points_admin::get_points_config))
        .route("/admin/points/points-config", post(handlers::points_admin::upsert_points_config))
        .route("/admin/points/earn-level-config", get(handlers::points_admin::get_earn_level_config))
        .route("/admin/points/earn-level-config", post(handlers::points_admin::upsert_earn_level_config))
        .route("/admin/points/trigger-earn-refresh", post(handlers::points_admin::trigger_earn_refresh))
        // Phase 3: manual season snapshot trigger
        .route("/admin/points/snapshot/:season_id", post(handlers::points_admin::admin_trigger_season_snapshot))
        // Earn admin API
        .route("/admin/earn/products", post(handlers::earn::admin_create_plan))
        .route("/admin/earn/products/:id/status", post(handlers::earn::admin_update_status))
        .route("/admin/earn/products/:id/subscriptions", get(handlers::earn::admin_get_plan_positions))
        .route("/admin/earn/products/:id/settle", post(handlers::earn::admin_close_plan))
        // Market config management API
        .route("/admin/markets", get(handlers::admin_market::list_markets))
        .route("/admin/markets", post(handlers::admin_market::create_market))
        .route("/admin/markets/:symbol", get(handlers::admin_market::get_market))
        .route("/admin/markets/:symbol", put(handlers::admin_market::update_market))
        .route("/admin/markets/:symbol/oi-caps", put(handlers::admin_market::update_oi_caps))
        .route("/admin/markets/:symbol/funding-caps", put(handlers::admin_market::update_funding_caps))
        .route("/admin/markets/:symbol", delete(handlers::admin_market::delete_market))
        .route("/admin/markets/:symbol/suspend", post(handlers::admin_market::suspend_market))
        .route("/admin/markets/:symbol/resume", post(handlers::admin_market::resume_market))
        .route("/admin/markets/:symbol/delist", post(handlers::admin_market::delist_market))
        .route("/admin/markets/:symbol/fee-history", get(handlers::admin_market::get_fee_history))
        .route("/admin/markets/:symbol/open-interest", get(handlers::admin_market::get_open_interest))
        // Referral admin
        .route("/admin/referral/update-backend-signer", post(handlers::referral::update_backend_signer))
        // Unified-margin admin (Phase 5)
        .route("/admin/margin-tiers", get(handlers::unified_margin::admin_list_margin_tiers))
        .route("/admin/margin-tiers", post(handlers::unified_margin::admin_upsert_margin_tier))
        .route("/admin/margin-tiers", delete(handlers::unified_margin::admin_delete_margin_tier))
        .route("/admin/margin-tiers/reload", post(handlers::unified_margin::admin_reload_margin_tiers))
        // MM Pool admin
        .route("/admin/mm/members", get(handlers::mm_pool::admin_list_mm_members))
        .route("/admin/mm/members", post(handlers::mm_pool::admin_upsert_mm_member))
        .route("/admin/mm/members/:address", delete(handlers::mm_pool::admin_deactivate_mm_member))
        // Spot admin (Task 14): markets create/update/status + testnet credit.
        // PATCH isn't in the top-level routing import set, so we qualify it.
        .route("/admin/spot/markets",            post(handlers::admin_spot::create_market))
        .route("/admin/spot/markets/:id",        axum::routing::patch(handlers::admin_spot::patch_market))
        .route("/admin/spot/markets/:id/status", axum::routing::patch(handlers::admin_spot::patch_status))
        .route("/admin/spot/balances/credit",    post(handlers::admin_spot::credit_balance))
        .layer(axum_middleware::from_fn_with_state(state.clone(), api_key_middleware));

    // Optionally-authenticated routes: respond with public data to anonymous
    // callers, enriched data when a Bearer JWT is presented. The /fee-vip page
    // hits /account/fee-info both before and after sign-in (to render the
    // tier table for anonymous visitors) so the route MUST stay reachable
    // without auth — see handler `get_fee_info` for the branching.
    let optional_auth_routes = Router::new()
        .route("/account/fee-info", get(handlers::fee_info::get_fee_info))
        .layer(axum_middleware::from_fn_with_state(
            state.clone(),
            optional_auth_middleware,
        ));

    let mut router = Router::new()
        .merge(public_routes)
        .merge(optional_auth_routes)
        .merge(protected_routes)
        .merge(internal_routes)
        .merge(admin_routes);

    if let Some(spot) = spot_routes {
        router = router.merge(spot);
    }

    router
}

/// Developer API routes (Binance-compatible)
/// Mounted at root level: /fapi/v1/* and /futures/data/*
pub fn create_developer_router(state: Arc<AppState>) -> Router<Arc<AppState>> {
    // Public market data endpoints
    let market_routes = Router::new()
        .route("/fapi/v1/ping", get(handlers::developer_market::ping))
        .route("/fapi/v1/time", get(handlers::developer_market::server_time))
        .route("/fapi/v1/exchangeInfo", get(handlers::developer_market::exchange_info))
        .route("/fapi/v1/klines", get(handlers::developer_market::klines))
        .route("/fapi/v1/ticker/price", get(handlers::developer_market::ticker_price))
        .route("/fapi/v1/ticker/24hr", get(handlers::developer_market::ticker_24hr))
        .route("/fapi/v1/ticker/bookTicker", get(handlers::developer_market::ticker_book))
        .route("/fapi/v1/depth", get(handlers::developer_market::depth))
        .route("/fapi/v1/premiumIndex", get(handlers::developer_market::premium_index))
        .route("/fapi/v1/fundingInfo", get(handlers::developer_market::funding_info))
        .route("/fapi/v1/fundingRate", get(handlers::developer_market::funding_rate))
        .route("/fapi/v1/openInterest", get(handlers::developer_market::open_interest))
        .route("/futures/data/openInterestHist", get(handlers::developer_market::open_interest_hist))
        .route("/futures/data/takerlongshortRatio", get(handlers::developer_market::taker_buy_sell_vol))
        .route("/futures/data/topLongShortAccountRatio", get(handlers::developer_market::top_long_short_account_ratio))
        .route("/futures/data/topLongShortPositionRatio", get(handlers::developer_market::top_long_short_position_ratio));

    // Authenticated trade & account endpoints
    let trade_routes = Router::new()
        .route("/fapi/v1/order/test", post(handlers::developer_trade::test_order))
        .route("/fapi/v1/order", post(handlers::developer_trade::new_order)
            .get(handlers::developer_trade::query_order)
            .delete(handlers::developer_trade::cancel_order)
            .put(handlers::developer_trade::modify_order))
        .route("/fapi/v1/openOrders", get(handlers::developer_trade::open_orders))
        .route("/fapi/v1/allOrders", get(handlers::developer_trade::all_orders))
        .route("/fapi/v1/allOpenOrders", delete(handlers::developer_trade::cancel_all_open_orders))
        .route("/fapi/v1/batchOrders", delete(handlers::developer_trade::batch_cancel_orders))
        .route("/fapi/v1/userTrades", get(handlers::developer_trade::user_trades))
        .route("/fapi/v1/userTrades/slippage", get(handlers::developer_trade::user_trades_slippage))
        .route("/fapi/v1/forceOrders", get(handlers::developer_trade::force_orders))
        .route("/fapi/v1/positionRisk", get(handlers::developer_trade::position_risk))
        // Binance Futures exposes both v1 and v2 of positionRisk; some
        // SDKs default to v2. Alias to the same handler — there is no
        // shape difference between the two for our use.
        .route("/fapi/v2/positionRisk", get(handlers::developer_trade::position_risk))
        .route("/fapi/v1/adlQuantile", get(handlers::developer_trade::adl_quantile))
        .route("/fapi/v1/leverage", post(handlers::developer_trade::change_leverage))
        .route("/fapi/v1/openOrder", get(handlers::developer_trade::query_open_order))
        .route("/fapi/v2/balance", get(handlers::developer_account::balance))
        .route("/fapi/v1/positionSide/dual", get(handlers::developer_account::get_position_mode)
            .post(handlers::developer_account::change_position_mode))
        .route("/fapi/v1/marginType", get(handlers::developer_account::get_margin_type)
            .post(handlers::developer_account::change_margin_type))
        .route("/fapi/v1/listenKey", post(handlers::developer_listen_key::create_listen_key)
            .put(handlers::developer_listen_key::keepalive_listen_key)
            .delete(handlers::developer_listen_key::delete_listen_key))
        .route("/fapi/v1/commissionRate", get(handlers::developer_account::commission_rate))
        .route("/fapi/v1/income", get(handlers::developer_account::income))
        .route("/fapi/v1/fundingFeeHistory", get(handlers::developer_account::funding_fee_history))
        .layer(axum_middleware::from_fn_with_state(state.clone(), auth_middleware));

    Router::new()
        .merge(market_routes)
        .merge(trade_routes)
}
