//! 周期性从 Binance fapi 拉 K 线覆盖 klines_historical, 让活跃交易对的 5m/15m/1h
//! K 线数据和 Binance 一致.
//!
//! 背景: 我们自己 matching engine 撮合稀疏, 直接产生的 OHLC/volume 太"难看",
//! 前端 K 线图基本是死水. 这个 worker 每 15 分钟扫一遍 market_configs.status='active'
//! 的所有 symbol, 用 Binance 数据 UPSERT 覆盖. 进行中 bar 也会被洗掉, 这是有意为之
//! —— 直到下次 sweep 之间, 撮合产生的局部噪声会被下一次 sweep 抹平.

use std::sync::Arc;
use std::time::Duration;

use rust_decimal::Decimal;
use tracing::Instrument;

use crate::app::state::AppState;
use crate::services::kline::HistoricalKline;
use crate::services::metrics;

const PERIODS: &[&str] = &["5m", "15m", "1h"];
const LIMIT: u32 = 1500;
const SYNC_INTERVAL: Duration = Duration::from_secs(900); // 15 min
/// 每个 Binance 请求之间留 200ms, 把 weight/min 控制在限额内 (50 syms × 3 periods × 10
/// weight/call ≈ 1500 weight, 配 200ms 间隔走完用 ~30s, 远低于 2400/min IP 限).
const PER_REQUEST_SLEEP: Duration = Duration::from_millis(200);
/// 启动后先等 30s, 别和 bootstrap / migrations 抢 db 连接.
const STARTUP_DELAY: Duration = Duration::from_secs(30);

const BINANCE_BASE: &str = "https://fapi.binance.com";

/// Binance kline 单根 = JSON 数组, 元素混合数字/字符串, 用 Value 接最稳.
type BinanceKline = serde_json::Value;

/// Spawn the background worker. 调用一次, 后台自旋.
pub fn spawn(state: Arc<AppState>) {
    tokio::spawn(
        async move {
            tokio::time::sleep(STARTUP_DELAY).await;
            tracing::info!(
                "Binance kline sync worker started (every {}s, periods={:?}, limit={})",
                SYNC_INTERVAL.as_secs(),
                PERIODS,
                LIMIT,
            );

            // 单 client 复用连接池, 避免每次 sweep 重新握手.
            let client = match reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
            {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!("Failed to build reqwest client, worker exiting: {}", e);
                    return;
                }
            };

            // interval 第一次 tick 立即返回, 我们想立刻跑一次, 不要 skip.
            let mut ticker = tokio::time::interval(SYNC_INTERVAL);
            loop {
                ticker.tick().await;
                if let Err(e) = sync_once(&state, &client).await {
                    tracing::error!("binance-kline-sync sweep failed: {}", e);
                }
            }
        }
        .instrument(tracing::info_span!("binance-kline-sync")),
    );
    tracing::info!("Binance kline sync worker spawned");
}

async fn sync_once(state: &Arc<AppState>, client: &reqwest::Client) -> Result<(), String> {
    let sweep_start = std::time::Instant::now();
    metrics::TASK_LAST_RUN_TIMESTAMP
        .with_label_values(&["binance-kline-sync"])
        .set(chrono::Utc::now().timestamp() as f64);

    let symbols = fetch_active_symbols(state).await?;
    if symbols.is_empty() {
        tracing::warn!("binance-kline-sync: no active symbols in market_configs, skipping");
        return Ok(());
    }

    let total_pairs = symbols.len() * PERIODS.len();
    let mut total_rows = 0usize;
    let mut ok_count = 0usize;
    let mut err_count = 0usize;
    let mut skipped_not_on_binance = 0usize;

    for symbol in &symbols {
        for period in PERIODS {
            let timer = metrics::TaskTimer::start("binance-kline-sync", symbol);
            match sync_one(state, client, symbol, period).await {
                Ok(rows) => {
                    total_rows += rows;
                    ok_count += 1;
                    timer.success();
                }
                Err(e) => {
                    let msg = e.to_string();
                    // Binance 返回 -1121 "Invalid symbol" 表示该币 Binance 没上, 属正常情况.
                    let is_invalid_symbol = msg.contains("\"code\":-1121")
                        || msg.contains("Invalid symbol");
                    if is_invalid_symbol {
                        skipped_not_on_binance += 1;
                        tracing::debug!(
                            "binance-kline-sync skip {} {}: not on Binance",
                            symbol, period
                        );
                    } else {
                        err_count += 1;
                        tracing::warn!(
                            "binance-kline-sync error {} {}: {}",
                            symbol, period, msg
                        );
                    }
                    timer.failure(&msg);
                }
            }
            tokio::time::sleep(PER_REQUEST_SLEEP).await;
        }
    }

    tracing::info!(
        "binance-kline-sync sweep done in {:.1}s: {}/{} ok, {} skipped (not on Binance), {} errors, {} rows upserted",
        sweep_start.elapsed().as_secs_f64(),
        ok_count,
        total_pairs,
        skipped_not_on_binance,
        err_count,
        total_rows,
    );
    Ok(())
}

async fn sync_one(
    state: &Arc<AppState>,
    client: &reqwest::Client,
    symbol: &str,
    period: &str,
) -> Result<usize, String> {
    let candles = fetch_binance_klines(client, symbol, period).await?;
    if candles.is_empty() {
        return Ok(0);
    }

    let klines: Vec<HistoricalKline> = candles
        .iter()
        .filter_map(|c| binance_to_historical(symbol, period, c))
        .collect();

    if klines.is_empty() {
        return Ok(0);
    }

    state
        .kline_service
        .import_historical_klines(&klines)
        .await
        .map_err(|e| format!("db save: {}", e))
}

async fn fetch_active_symbols(state: &Arc<AppState>) -> Result<Vec<String>, String> {
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT symbol FROM market_configs WHERE status = 'active' ORDER BY symbol",
    )
    .fetch_all(&state.db.pool)
    .await
    .map_err(|e| format!("query active symbols: {}", e))?;
    Ok(rows.into_iter().map(|(s,)| s).collect())
}

async fn fetch_binance_klines(
    client: &reqwest::Client,
    symbol: &str,
    interval: &str,
) -> Result<Vec<BinanceKline>, String> {
    let url = format!("{}/fapi/v1/klines", BINANCE_BASE);
    let resp = client
        .get(&url)
        .query(&[
            ("symbol", symbol),
            ("interval", interval),
            ("limit", &LIMIT.to_string()),
        ])
        .send()
        .await
        .map_err(|e| format!("http: {}", e))?;

    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(format!(
            "HTTP {}: {}",
            status.as_u16(),
            body.chars().take(200).collect::<String>()
        ));
    }

    resp.json::<Vec<BinanceKline>>()
        .await
        .map_err(|e| format!("parse json: {}", e))
}

fn binance_to_historical(symbol: &str, period: &str, raw: &BinanceKline) -> Option<HistoricalKline> {
    use std::str::FromStr;
    let arr = raw.as_array()?;
    if arr.len() < 9 {
        return None;
    }
    // Binance kline: [openTime(ms), o, h, l, c, volume, closeTime, quoteVolume, trades, ...]
    let open_time_ms = arr[0].as_i64()?;
    let open = Decimal::from_str(arr[1].as_str()?).ok()?;
    let high = Decimal::from_str(arr[2].as_str()?).ok()?;
    let low = Decimal::from_str(arr[3].as_str()?).ok()?;
    let close = Decimal::from_str(arr[4].as_str()?).ok()?;
    let volume = Decimal::from_str(arr[5].as_str()?).ok()?;
    let quote_volume = arr[7].as_str().and_then(|s| Decimal::from_str(s).ok());
    let trade_count = arr[8].as_i64().map(|n| n as i32);

    Some(HistoricalKline {
        symbol: symbol.to_string(),
        period: period.to_string(),
        open_time: open_time_ms,
        open,
        high,
        low,
        close,
        volume,
        quote_volume,
        trade_count,
    })
}
