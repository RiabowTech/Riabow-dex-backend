use axum::{
    body::Body,
    extract::State,
    http::{header, Request, StatusCode, Uri},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use axum::body::Bytes;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::sync::Arc;

use crate::auth::jwt::JwtManager;
use crate::utils::response::ApiResponse;
use crate::AppState;

use dashmap::DashMap;
use std::time::Instant;

type HmacSha256 = Hmac<Sha256>;

lazy_static::lazy_static! {
    /// Per-API-key rate limiter for `last_used_at` UPDATEs.
    /// Maps api_key UUID → last moment we pushed a DB UPDATE for it.
    /// Skips the UPDATE if the previous one was within the debounce window.
    ///
    /// Motivation: operator 反馈 UPDATE user_api_keys SET last_used_at = NOW()
    /// 在认证热路径上（即使 fire-and-forget）累积大量 row lock + WAL 写入。
    /// 单个活跃 API key 每秒可以被调 10+ 次，一天百万级无谓 UPDATE。
    /// 内存去抖：同一 key 60s 内最多写库一次，缩短统计精度换 DB 负载 ~60×。
    static ref LAST_USED_UPDATED_AT: DashMap<uuid::Uuid, Instant> = DashMap::new();
}
const LAST_USED_DEBOUNCE: std::time::Duration = std::time::Duration::from_secs(60);

#[derive(Clone)]
pub struct AuthUser {
    pub address: String,
    pub is_api_key: bool,
}

/// Projection from `user_api_keys INNER JOIN users` used by the auth hot path.
/// Avoids pulling down full UserApiKey + a second users query.
#[derive(sqlx::FromRow)]
struct ApiKeyAuthRow {
    id: uuid::Uuid,
    #[allow(dead_code)]  // user_id not needed after JOIN (kept for symmetry)
    user_id: uuid::Uuid,
    secret_key: String,
    ip_whitelist: Option<String>,
    status: String,
    address: String,
}

/// Validate HMAC-SHA256 signature (Binance-compatible)
///
/// Signing rule:
///   - Collect all query params EXCEPT `signature`
///   - For POST/PUT/DELETE with body: append raw body string
///   - signature = HMAC-SHA256(secret_key, payload)
///   - timestamp must be within recvWindow (default 5000ms)
fn verify_hmac_signature(
    uri: &Uri,
    body_bytes: &[u8],
    secret_key: &str,
) -> Result<(), &'static str> {
    let query = uri.query().unwrap_or("");

    // Extract signature and timestamp from query params
    let mut signature_val: Option<&str> = None;
    let mut timestamp_val: Option<u64> = None;
    let mut params_without_sig = Vec::new();

    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        if let Some(val) = pair.strip_prefix("signature=") {
            signature_val = Some(val);
        } else {
            if let Some(val) = pair.strip_prefix("timestamp=") {
                timestamp_val = val.parse().ok();
            }
            params_without_sig.push(pair);
        }
    }

    let signature = signature_val.ok_or("Missing signature parameter")?;
    let timestamp = timestamp_val.ok_or("Missing timestamp parameter")?;

    // Validate timestamp (within 5 seconds for testing, 60 seconds for production)
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    let recv_window: u64 = 60_000; // 60 seconds (±window)
    // Bidirectional window — 之前只有 now_ms.saturating_sub(timestamp)，
    // 当 timestamp > now_ms 时 saturating_sub 饱和到 0 总是 <= recv_window，
    // 等价于 future 方向的窗口无限大；实测 timestamp = now + 365 天仍 pass。
    // Replay 保护必须双向，否则时钟回拨 / 故意前置时间戳可绕开。
    let delta = if timestamp >= now_ms {
        timestamp - now_ms
    } else {
        now_ms - timestamp
    };
    if delta > recv_window {
        return Err("Timestamp outside recv window");
    }

    // Build signing payload: query params (without signature) + body
    let mut payload = params_without_sig.join("&");
    if !body_bytes.is_empty() {
        let body_str = std::str::from_utf8(body_bytes).unwrap_or("");
        if !body_str.is_empty() {
            if !payload.is_empty() {
                payload.push_str(&body_str);
            } else {
                payload = body_str.to_string();
            }
        }
    }

    // Compute HMAC-SHA256.
    //
    // Binance's official convention is "URL-encode each value, then sign the
    // resulting query string". `uri.query()` returns the percent-encoded form
    // we received over the wire, so a client following the convention matches
    // exactly. However several real-world callers — notably the in-tree QA
    // hmac_client and a number of light-weight MM bots — sign the *raw*
    // (un-encoded) form and then let their HTTP library percent-encode on
    // the way out. Especially common with `orderIdList=[...]` for
    // `DELETE /fapi/v1/batchOrders`, which previously got -1001
    // SIGNATURE_INVALID (R4 P0 #8).
    //
    // Try the canonical (received) form first; on miss, retry with each
    // pair URL-decoded so callers that signed the raw form also pass. The
    // decoded path strictly increases tolerance — it does not weaken the
    // HMAC since the secret stays in play.
    let try_sign = |payload: &str| -> Result<String, &'static str> {
        let mut mac = HmacSha256::new_from_slice(secret_key.as_bytes())
            .map_err(|_| "Invalid secret key")?;
        mac.update(payload.as_bytes());
        Ok(hex::encode(mac.finalize().into_bytes()))
    };

    let primary = try_sign(&payload)?;
    if primary == signature {
        return Ok(());
    }

    fn percent_decode(input: &str) -> String {
        let bytes = input.as_bytes();
        let mut out = Vec::with_capacity(bytes.len());
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b'%' && i + 2 < bytes.len() {
                let hi = (bytes[i + 1] as char).to_digit(16);
                let lo = (bytes[i + 2] as char).to_digit(16);
                if let (Some(hi), Some(lo)) = (hi, lo) {
                    out.push((hi * 16 + lo) as u8);
                    i += 3;
                    continue;
                }
            } else if bytes[i] == b'+' {
                out.push(b' ');
                i += 1;
                continue;
            }
            out.push(bytes[i]);
            i += 1;
        }
        String::from_utf8_lossy(&out).into_owned()
    }

    let decoded_pairs: Vec<String> = params_without_sig
        .iter()
        .map(|p| {
            let mut split = p.splitn(2, '=');
            let k = split.next().unwrap_or("");
            let v = split.next().unwrap_or("");
            format!("{}={}", percent_decode(k), percent_decode(v))
        })
        .collect();
    let mut decoded_payload = decoded_pairs.join("&");
    if !body_bytes.is_empty() {
        if let Ok(body_str) = std::str::from_utf8(body_bytes) {
            if !body_str.is_empty() {
                if !decoded_payload.is_empty() {
                    decoded_payload.push_str(body_str);
                } else {
                    decoded_payload = body_str.to_string();
                }
            }
        }
    }
    let alt = try_sign(&decoded_payload)?;
    if alt == signature {
        return Ok(());
    }

    tracing::warn!(
        "HMAC signature mismatch: expected={}, got={}, payload={}",
        &primary[..16],
        &signature[..signature.len().min(16)],
        &payload[..payload.len().min(100)]
    );
    Err("Invalid signature")
}

pub async fn auth_middleware(
    State(state): State<Arc<AppState>>,
    mut request: Request<Body>,
    next: Next,
) -> Result<Response, Response> {
    // Check if auth bypass is allowed (ONLY in development environment)
    if state.config.is_auth_disabled() {
        if !state.config.is_development() {
            tracing::error!(
                "🚨 SECURITY: AUTH_DISABLED=true in {} environment! Auth bypass BLOCKED.",
                state.config.environment
            );
        } else {
            let address = request
                .headers()
                .get("X-Test-Address")
                .and_then(|h| h.to_str().ok())
                .map(|s| s.to_string())
                .unwrap_or_else(|| "0x0000000000000000000000000000000000000001".to_string());

            tracing::debug!("⚠️  Auth disabled (dev mode) - using address: {}", address);
            request.extensions_mut().insert(AuthUser { address, is_api_key: false });
            return Ok(next.run(request).await);
        }
    }

    // 1. Check for API Key Header
    if let Some(api_key) = request
        .headers()
        .get("X-MBX-APIKEY")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
    {
        // Hot-path 优化：把 user_api_keys.* + users.address 合并成一次 JOIN。
        // 原实现需要 SELECT api_key → UPDATE last_used_at → SELECT address 三次 DB 往返，
        // 50 并发压测时占满连接池（30→166）。现在一次查齐。
        // last_used_at 的 UPDATE 改为 fire-and-forget（见后）。
        let key_row = sqlx::query_as::<_, ApiKeyAuthRow>(
            r#"
            SELECT k.id, k.user_id, k.secret_key, k.ip_whitelist, k.status, u.address
            FROM user_api_keys k
            INNER JOIN users u ON u.id = k.user_id
            WHERE k.api_key = $1
            "#
        )
            .bind(&api_key)
            .fetch_optional(&state.db.pool)
            .await
            .map_err(|e| {
                tracing::error!("Auth DB error: {}", e);
                (StatusCode::INTERNAL_SERVER_ERROR, Json(ApiResponse::<()>::error("INTERNAL_ERROR", "Internal server error"))).into_response()
            })?;

        if let Some(record) = key_row {
            if record.status == "active" {
                // IP Check
                let ip_allowed = if let Some(ref whitelist) = record.ip_whitelist {
                    if whitelist.is_empty() {
                        true
                    } else {
                        let client_ip = request
                            .headers()
                            .get("X-Forwarded-For")
                            .and_then(|h| h.to_str().ok())
                            .map(|s| s.split(',').next().unwrap_or(s).trim())
                            .unwrap_or("");
                        whitelist.split(',').any(|ip| ip.trim() == client_ip)
                    }
                } else {
                    true
                };

                if !ip_allowed {
                    let client_ip = request
                        .headers()
                        .get("X-Forwarded-For")
                        .and_then(|h| h.to_str().ok())
                        .map(|s| s.split(',').next().unwrap_or(s).trim().to_string())
                        .unwrap_or_else(|| "unknown".to_string());
                    tracing::warn!("Blocked API Key request from IP: {}", client_ip);
                    return Err((StatusCode::FORBIDDEN, Json(ApiResponse::<()>::error("IP_NOT_ALLOWED", &format!("IP address {} is not in the whitelist", client_ip)))).into_response());
                }

                // ── HMAC-SHA256 Signature Verification ──
                // Read body for signature verification, then put it back
                let uri = request.uri().clone();
                let (parts, body) = request.into_parts();
                let body_bytes = axum::body::to_bytes(body, 1024 * 1024) // 1MB limit
                    .await
                    .unwrap_or_else(|_| Bytes::new());

                if let Err(err_msg) = verify_hmac_signature(&uri, &body_bytes, &record.secret_key) {
                    tracing::warn!("API Key HMAC verification failed: {} (key={}...)", err_msg, &api_key[..8]);
                    return Err((StatusCode::UNAUTHORIZED, Json(ApiResponse::<()>::error("SIGNATURE_INVALID", err_msg))).into_response());
                }

                // Reconstruct request with body
                let mut request = Request::from_parts(parts, Body::from(body_bytes));

                // Update last_used_at —— fire-and-forget + 60s 去抖。
                // 旧版（dcdbc9e）每请求都 spawn 一次 UPDATE，虽然不阻塞请求但
                // row lock + WAL 写入在高 QPS 下累积可观（operator 反馈）。
                // 去抖后，同一 key 60s 内最多写一次；last_used_at 精度损失
                // 从 ms 降到 60s，换 DB 负载最高 ~60× 降低。
                let id = record.id;
                let now = Instant::now();
                let should_update = match LAST_USED_UPDATED_AT.get(&id) {
                    Some(prev) => now.duration_since(*prev) >= LAST_USED_DEBOUNCE,
                    None => true,
                };
                if should_update {
                    LAST_USED_UPDATED_AT.insert(id, now);
                    let pool_clone = state.db.pool.clone();
                    tokio::spawn(async move {
                        let _ = sqlx::query("UPDATE user_api_keys SET last_used_at = NOW() WHERE id = $1")
                            .bind(id)
                            .execute(&pool_clone)
                            .await;
                    });
                }

                // address 已在 JOIN 里拿到
                request.extensions_mut().insert(AuthUser { address: record.address, is_api_key: true });
                return Ok(next.run(request).await);
            } else {
                return Err((StatusCode::UNAUTHORIZED, Json(ApiResponse::<()>::error("API_KEY_DISABLED", "API Key is disabled"))).into_response());
            }
        }
        return Err((StatusCode::UNAUTHORIZED, Json(ApiResponse::<()>::error("INVALID_API_KEY", "Invalid API Key"))).into_response());
    }

    // 2. Check for Bearer Token
    let auth_header = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok());

    let token = match auth_header {
        Some(header) if header.starts_with("Bearer ") => &header[7..],
        _ => return Err((StatusCode::UNAUTHORIZED, Json(ApiResponse::<()>::error("UNAUTHORIZED", "Missing or invalid authorization header"))).into_response()),
    };

    let jwt_manager = JwtManager::new(&state.config.jwt_secret, state.config.jwt_expiry_seconds);
    let claims = jwt_manager
        .verify_token(token)
        .map_err(|_| (StatusCode::UNAUTHORIZED, Json(ApiResponse::<()>::error("INVALID_TOKEN", "Invalid or expired token"))).into_response())?;

    request.extensions_mut().insert(AuthUser {
        address: claims.sub,
        is_api_key: false,
    });

    Ok(next.run(request).await)
}

/// Wrapper extension marking an "optionally authenticated" request.
/// Always inserted by `optional_auth_middleware`; `None` when the caller
/// did not present a valid Bearer token.
#[derive(Clone)]
pub struct OptionalAuthUser(pub Option<AuthUser>);

/// Lenient counterpart of `auth_middleware`: never rejects the request.
/// A valid Bearer JWT is decoded into `OptionalAuthUser(Some(_))`; any
/// other state (no header, wrong scheme, expired/invalid token) becomes
/// `OptionalAuthUser(None)`.
///
/// Use for endpoints that have a public surface but optionally enrich the
/// response when the caller is signed in (e.g. `/account/fee-info` returns
/// the public VIP tier table to anonymous callers and adds user-specific
/// fields when a JWT is present).
///
/// API-key + HMAC validation is intentionally NOT performed here:
/// programmatic clients hit the dedicated authenticated routes
/// (e.g. `/fapi/v1/commissionRate`), and we want this hot path to stay free
/// of DB I/O for unauthenticated visitors.
pub async fn optional_auth_middleware(
    State(state): State<Arc<AppState>>,
    mut request: Request<Body>,
    next: Next,
) -> Response {
    if state.config.is_auth_disabled() && state.config.is_development() {
        let address = request
            .headers()
            .get("X-Test-Address")
            .and_then(|h| h.to_str().ok())
            .map(|s| s.to_string())
            .unwrap_or_else(|| "0x0000000000000000000000000000000000000001".to_string());
        request
            .extensions_mut()
            .insert(OptionalAuthUser(Some(AuthUser { address, is_api_key: false })));
        return next.run(request).await;
    }

    let user = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "))
        .and_then(|token| {
            let jwt_manager =
                JwtManager::new(&state.config.jwt_secret, state.config.jwt_expiry_seconds);
            jwt_manager.verify_token(token).ok()
        })
        .map(|claims| AuthUser {
            address: claims.sub,
            is_api_key: false,
        });

    request.extensions_mut().insert(OptionalAuthUser(user));
    next.run(request).await
}
