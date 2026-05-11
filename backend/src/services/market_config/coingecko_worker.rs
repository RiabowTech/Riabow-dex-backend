//! CoinGecko worker — refreshes `market_cap` and `fully_diluted_valuation`
//! for every market whose `coingecko_id` is set.
//!
//! Fires hourly. CoinGecko's free tier allows ~30 calls/min, and we
//! batch all symbols into a single `/simple/price` request, so one call
//! covers the entire book.
//!
//! A missing `coingecko_id` simply skips that row; admins can populate
//! via `PUT /admin/markets/:symbol` with `{"coingecko_id": "bitcoin"}`.

use anyhow::{Context, Result};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::app::state::AppState;

const TICK_SECS: u64 = 3600; // 1h
const COINGECKO_URL: &str = "https://api.coingecko.com/api/v3/simple/price";

/// Shape of the `/simple/price` response keyed by coingecko id:
/// `{ "bitcoin": { "usd_market_cap": 1.2e12, "usd_fully_diluted_valuation": ... } }`
#[derive(Debug, Deserialize)]
struct CgEntry {
    #[serde(default)]
    usd_market_cap: Option<f64>,
    #[serde(default)]
    usd_fully_diluted_valuation: Option<f64>,
}

pub fn spawn(state: Arc<AppState>) {
    tokio::spawn(async move {
        tracing::info!("CoinGecko market-cap worker started (interval: {}s)", TICK_SECS);
        // Initial short delay to let other services warm up, then fire
        // once immediately so a freshly restarted service populates
        // caps within minutes rather than after a full hour of wait.
        tokio::time::sleep(Duration::from_secs(30)).await;
        loop {
            if let Err(e) = tick(&state).await {
                tracing::warn!("CoinGecko tick failed: {}", e);
            }
            tokio::time::sleep(Duration::from_secs(TICK_SECS)).await;
        }
    });
}

async fn tick(state: &Arc<AppState>) -> Result<()> {
    // Load the id list. Ordered + deduped for a stable request URL
    // (helps CDN / rate-limit bucketing on CoinGecko's side).
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT symbol, coingecko_id FROM market_configs
         WHERE coingecko_id IS NOT NULL AND coingecko_id <> ''",
    )
    .fetch_all(&state.db.pool)
    .await?;

    if rows.is_empty() {
        return Ok(());
    }

    let mut cg_ids: Vec<String> = rows.iter().map(|(_, cg)| cg.clone()).collect();
    cg_ids.sort();
    cg_ids.dedup();
    let ids_param = cg_ids.join(",");

    // CoinGecko IDs are lowercase ASCII letters / digits / hyphens and
    // the comma separator needs no escaping, so no URL-encoding is
    // required — skipping the dep keeps the crate graph clean.
    let url = format!(
        "{}?ids={}&vs_currencies=usd&include_market_cap=true&include_fully_diluted_valuation=true&precision=full",
        COINGECKO_URL, ids_param
    );

    // CoinGecko's Cloudflare layer rejects requests without a recognisable
    // User-Agent. A demo/pro API key via x-cg-demo-api-key env var is
    // optional — without it we stick to the public rate limit (~30/min).
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .user_agent("ztdx-backend/1.0 (+https://api.ztdx.io)")
        .build()
        .unwrap_or_default();
    let mut req = client.get(&url);
    if let Ok(key) = std::env::var("COINGECKO_API_KEY") {
        if !key.is_empty() {
            // Free-tier demo keys use this header; Pro uses x-cg-pro-api-key.
            req = req.header("x-cg-demo-api-key", key);
        }
    }
    let resp = req.send().await.context("CoinGecko GET failed")?;
    if !resp.status().is_success() {
        anyhow::bail!("CoinGecko returned HTTP {}", resp.status());
    }

    let body: HashMap<String, CgEntry> = resp.json().await.context("parse CoinGecko JSON")?;

    let mut updated = 0u64;
    for (symbol, cg_id) in &rows {
        let Some(entry) = body.get(cg_id) else { continue };
        let mc = entry
            .usd_market_cap
            .and_then(|v| Decimal::try_from(v).ok());
        // CoinGecko 对已达到最大供应的资产（如 BTC）不返回 FDV。
        // 前端展示时希望两者一致，缺失时回退为 market_cap。
        let fdv = entry
            .usd_fully_diluted_valuation
            .and_then(|v| Decimal::try_from(v).ok())
            .or(mc);
        let _ = sqlx::query(
            "UPDATE market_configs
                SET market_cap = COALESCE($2, market_cap),
                    fully_diluted_valuation = COALESCE($3, fully_diluted_valuation),
                    market_cap_updated_at = NOW()
             WHERE symbol = $1",
        )
        .bind(symbol)
        .bind(mc)
        .bind(fdv)
        .execute(&state.db.pool)
        .await;
        updated += 1;
    }

    tracing::info!(
        "CoinGecko refresh: {} ids requested, {} market_configs updated",
        cg_ids.len(),
        updated
    );

    // Reload the in-memory cache so `/markets/:symbol/details` sees
    // the new caps on the very next request.
    let _ = state.market_config_service.reload().await;

    Ok(())
}
