//! Time helpers shared across the backend.

/// Hyperliquid 永续合约每小时在 UTC 整点结算资金费；前端需要 Unix 毫秒时间戳。
pub fn next_hourly_funding_ms() -> i64 {
    let now_ms = chrono::Utc::now().timestamp_millis();
    let hour_ms: i64 = 3_600_000;
    ((now_ms / hour_ms) + 1) * hour_ms
}
