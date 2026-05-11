//! Developer API — User Data Stream listenKey (Binance-compatible)
//!
//! Bots integrating via Binance fapi SDKs subscribe to the private user-data
//! stream by:
//!   1. Calling `POST /fapi/v1/listenKey` with HMAC auth → receives a
//!      64-char listenKey.
//!   2. Connecting to a WebSocket and authenticating with that listenKey
//!      (e.g. `{"type":"auth","listenKey":"..."}`).
//!   3. Periodically calling `PUT /fapi/v1/listenKey` (every < 60 min) to
//!      extend the TTL.
//!   4. Optionally `DELETE /fapi/v1/listenKey` to revoke.
//!
//! Storage: Redis with a 60-min TTL. Two keys per active listenKey:
//!   - `fapi:listen_key:<key>`             → user_address (lowercased)
//!   - `fapi:listen_key_by_user:<addr>`    → listen_key
//!
//! The reverse map lets us implement Binance's "one active key per user"
//! rule (POST is idempotent: returns the existing key + refreshes TTL when
//! the user already has one). It also lets PUT/DELETE work without a body
//! — they look up the caller's current key via the address from the auth
//! middleware.

use axum::{Extension, Json};
use rand::Rng;
use serde::Serialize;
use std::sync::Arc;

use crate::auth::middleware::AuthUser;
use crate::AppState;

const LISTEN_KEY_TTL_SECS: u64 = 3600;
const LISTEN_KEY_LEN: usize = 64;

fn key_redis_key(listen_key: &str) -> String {
    format!("fapi:listen_key:{}", listen_key)
}

fn user_redis_key(user_address: &str) -> String {
    format!("fapi:listen_key_by_user:{}", user_address.to_lowercase())
}

fn generate_listen_key() -> String {
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut rng = rand::thread_rng();
    (0..LISTEN_KEY_LEN)
        .map(|_| {
            let idx = rng.gen_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect()
}

/// Resolve a listenKey to its user_address (lowercased) and refresh both TTLs.
///
/// Used by the WebSocket auth path. Returns `None` when the key is unknown
/// or expired. On any successful lookup we extend the TTL by another full
/// window — Binance treats an active WS connection as implicit keepalive,
/// matching that behavior here means a long-running bot doesn't have to
/// explicitly call `PUT /listenKey` while the WS is up.
pub async fn resolve_listen_key(state: &AppState, listen_key: &str) -> Option<String> {
    let redis = state.cache.redis()?;
    let address: Option<String> = redis.get(&key_redis_key(listen_key)).await.ok().flatten();
    if let Some(addr) = address.as_ref() {
        let _ = redis.expire(&key_redis_key(listen_key), LISTEN_KEY_TTL_SECS).await;
        let _ = redis.expire(&user_redis_key(addr), LISTEN_KEY_TTL_SECS).await;
    }
    address
}

#[derive(Serialize)]
pub struct ListenKeyResponse {
    #[serde(rename = "listenKey")]
    pub listen_key: String,
}

#[derive(Serialize)]
pub struct BinanceError {
    pub code: i32,
    pub msg: String,
}

type ApiResult<T> =
    Result<Json<T>, (axum::http::StatusCode, Json<BinanceError>)>;

fn internal_error(msg: &str) -> (axum::http::StatusCode, Json<BinanceError>) {
    (
        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        Json(BinanceError {
            code: -1001,
            msg: msg.to_string(),
        }),
    )
}

fn not_found(msg: &str) -> (axum::http::StatusCode, Json<BinanceError>) {
    (
        axum::http::StatusCode::BAD_REQUEST,
        Json(BinanceError {
            code: -1125,
            msg: msg.to_string(),
        }),
    )
}

/// POST /fapi/v1/listenKey
///
/// Idempotent per user. If the caller already has an active listenKey,
/// returns it and refreshes the TTL. Otherwise mints a fresh key.
///
/// Atomicity: the previous read-then-write split could let two concurrent
/// POSTs both observe `None` and both mint distinct keys, leaving the
/// user with two valid listenKeys (Binance contract is exactly one).
/// We claim the user-forward map with `SET key val NX EX ttl` first;
/// at most one POST wins that single-key CAS. Loser path GETs the
/// winner's key and refreshes TTLs. Reverse-map write happens after
/// the CAS — if it fails we delete the forward map so we don't leave a
/// dangling pointer.
pub async fn create_listen_key(
    Extension(auth_user): Extension<AuthUser>,
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
) -> ApiResult<ListenKeyResponse> {
    let address = auth_user.address.to_lowercase();
    let redis = state
        .cache
        .redis()
        .ok_or_else(|| internal_error("listenKey storage unavailable"))?;

    let user_key = user_redis_key(&address);
    let candidate = generate_listen_key();

    let won = redis
        .set_nx_ex(&user_key, candidate.as_str(), LISTEN_KEY_TTL_SECS)
        .await
        .map_err(|e| internal_error(&format!("listenKey CAS failed: {}", e)))?;

    if won {
        if let Err(e) = redis
            .set_ex(&key_redis_key(&candidate), address.as_str(), LISTEN_KEY_TTL_SECS)
            .await
        {
            // Best-effort cleanup of the forward map so a future POST can
            // re-mint cleanly instead of being blocked by a dangling
            // pointer to a key whose reverse map never landed.
            let _ = redis.del(&user_key).await;
            return Err(internal_error(&format!("listenKey reverse-set failed: {}", e)));
        }
        return Ok(Json(ListenKeyResponse { listen_key: candidate }));
    }

    // CAS lost: someone else owns the forward map. Read and return their key.
    let existing: Option<String> = redis
        .get(&user_key)
        .await
        .map_err(|e| internal_error(&format!("listenKey lookup after CAS miss: {}", e)))?;
    let listen_key = existing.ok_or_else(|| {
        // Forward map was set when we did the SET NX, but expired between
        // the NX failure and our GET. Surface as a transient failure so
        // the bot can retry.
        internal_error("listenKey state changed mid-request; retry")
    })?;
    let _ = redis.expire(&key_redis_key(&listen_key), LISTEN_KEY_TTL_SECS).await;
    let _ = redis.expire(&user_key, LISTEN_KEY_TTL_SECS).await;
    Ok(Json(ListenKeyResponse { listen_key }))
}

/// PUT /fapi/v1/listenKey
///
/// Refreshes TTL on the caller's active listenKey. Binance ignores any
/// body; the key is implicit (one per user). Returns 404 -1125 if there
/// is no active key (so the bot can re-issue via POST).
pub async fn keepalive_listen_key(
    Extension(auth_user): Extension<AuthUser>,
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
) -> ApiResult<serde_json::Value> {
    let address = auth_user.address.to_lowercase();
    let redis = state
        .cache
        .redis()
        .ok_or_else(|| internal_error("listenKey storage unavailable"))?;

    let existing: Option<String> = redis
        .get(&user_redis_key(&address))
        .await
        .map_err(|e| internal_error(&format!("listenKey lookup failed: {}", e)))?;

    let listen_key = existing.ok_or_else(|| not_found("This listenKey does not exist."))?;

    let _ = redis.expire(&key_redis_key(&listen_key), LISTEN_KEY_TTL_SECS).await;
    let _ = redis.expire(&user_redis_key(&address), LISTEN_KEY_TTL_SECS).await;

    Ok(Json(serde_json::json!({ "listenKey": listen_key })))
}

/// DELETE /fapi/v1/listenKey
///
/// Revokes the caller's active listenKey. Idempotent — returns success
/// even when nothing was active (Binance behavior).
pub async fn delete_listen_key(
    Extension(auth_user): Extension<AuthUser>,
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
) -> ApiResult<serde_json::Value> {
    let address = auth_user.address.to_lowercase();
    let redis = state
        .cache
        .redis()
        .ok_or_else(|| internal_error("listenKey storage unavailable"))?;

    let existing: Option<String> = redis
        .get(&user_redis_key(&address))
        .await
        .map_err(|e| internal_error(&format!("listenKey lookup failed: {}", e)))?;

    if let Some(listen_key) = existing {
        let _ = redis.del(&key_redis_key(&listen_key)).await;
        let _ = redis.del(&user_redis_key(&address)).await;
    }

    Ok(Json(serde_json::json!({})))
}
