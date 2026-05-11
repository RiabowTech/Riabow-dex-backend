//! Internal pod-to-pod proxy for sharded matching requests.
//!
//! When `ShardingConfig::route_for(symbol)` returns `Forward`, the
//! handler delegates to this module. We rebuild the original HTTP
//! request against the owner pod, set `X-ZTDX-Shard-Forwarded: 1` so
//! the receiving pod can detect routing-loop / hash-drift conditions,
//! and stream the response back to the client.
//!
//! Why a custom proxy instead of axum middleware: the routing decision
//! depends on parsing the request body (specifically `req.symbol` for
//! /fapi/v1/order), so we can't decide before the handler runs. Once
//! the handler has the body parsed, building a small targeted forward
//! is cleaner than re-implementing axum's body extraction in middleware.

use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use axum::Json;
use serde::de::DeserializeOwned;
use serde::Serialize;

use super::FORWARDED_HEADER;

/// Headers we strip when proxying. `host` and `content-length` will be
/// re-set by reqwest; hop-by-hop headers should not propagate.
const STRIP: &[&str] = &[
    "host",
    "content-length",
    "transfer-encoding",
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "upgrade",
];

/// Forward `body` (already-parsed) as JSON to the owner pod and decode
/// the response into `R`. Carries through the original auth headers so
/// HMAC signing remains valid against the same payload bytes — the
/// signature was computed over the raw body, and we re-serialise from
/// the same struct, so the bytes match. Query string is reconstructed
/// from `query_pairs`.
///
/// On forward failure, we return an `internal_error`-shaped response so
/// the caller can map it to whatever its error type is.
pub async fn forward_json<B, R>(
    target_host: &str,
    method: Method,
    path: &str,
    headers: &HeaderMap,
    query_pairs: &[(String, String)],
    body: Option<&B>,
) -> Result<R, ForwardError>
where
    B: Serialize,
    R: DeserializeOwned,
{
    let mut req_builder = build_request(target_host, method, path, headers, query_pairs);
    if let Some(b) = body {
        req_builder = req_builder.json(b);
    }

    let resp = req_builder
        .send()
        .await
        .map_err(|e| ForwardError::Network(e.to_string()))?;

    let status = resp.status();
    let body_text = resp
        .text()
        .await
        .map_err(|e| ForwardError::Network(format!("read response body: {}", e)))?;

    if !status.is_success() {
        // Bubble up the upstream's status + body — the caller maps it.
        return Err(ForwardError::Upstream {
            status: StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY),
            body: body_text,
        });
    }

    let parsed: R = serde_json::from_str(&body_text)
        .map_err(|e| ForwardError::Decode(format!("decode {}: {}", e, body_text)))?;
    Ok(parsed)
}

/// Same as `forward_json` but returns the response as raw JSON value +
/// status, so handlers with mixed success / error envelope shapes (e.g.
/// /fapi/v1/order's Binance error format) can pass through without a
/// typed decode.
pub async fn forward_json_raw<B>(
    target_host: &str,
    method: Method,
    path: &str,
    headers: &HeaderMap,
    query_pairs: &[(String, String)],
    body: Option<&B>,
) -> Result<(StatusCode, serde_json::Value), ForwardError>
where
    B: Serialize,
{
    let mut req_builder = build_request(target_host, method, path, headers, query_pairs);
    if let Some(b) = body {
        req_builder = req_builder.json(b);
    }

    let resp = req_builder
        .send()
        .await
        .map_err(|e| ForwardError::Network(e.to_string()))?;
    let status = StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let body_text = resp
        .text()
        .await
        .map_err(|e| ForwardError::Network(format!("read response body: {}", e)))?;
    let value: serde_json::Value = serde_json::from_str(&body_text)
        .unwrap_or_else(|_| serde_json::Value::String(body_text));
    Ok((status, value))
}

fn build_request(
    target_host: &str,
    method: Method,
    path: &str,
    headers: &HeaderMap,
    query_pairs: &[(String, String)],
) -> reqwest::RequestBuilder {
    let url = format!("http://{}{}", target_host, path);
    let reqwest_method = reqwest::Method::from_bytes(method.as_str().as_bytes())
        .unwrap_or(reqwest::Method::POST);
    let mut req_builder = forwarded_client().request(reqwest_method, &url);

    if !query_pairs.is_empty() {
        // reqwest::query handles percent-encoding; safe for the HMAC
        // since the receiving handler re-reads the parameters from the
        // query string after decoding.
        req_builder = req_builder.query(query_pairs);
    }

    // Carry through every header from the original request except the
    // hop-by-hop ones, then stamp the loop-detection marker on top.
    for (name, value) in headers.iter() {
        let n = name.as_str().to_ascii_lowercase();
        if STRIP.iter().any(|s| *s == n) {
            continue;
        }
        if let Ok(v) = value.to_str() {
            req_builder = req_builder.header(name.as_str(), v);
        }
    }
    req_builder = req_builder.header(FORWARDED_HEADER, "1");
    req_builder
}

fn forwarded_client() -> &'static reqwest::Client {
    use std::sync::OnceLock;
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        // Tight timeouts — same-cluster hop, so anything slow means the
        // owner pod is in trouble and we should fail fast rather than
        // pile up forwarded requests.
        reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_millis(500))
            .timeout(std::time::Duration::from_secs(5))
            .pool_max_idle_per_host(32)
            .build()
            .expect("forwarded client build")
    })
}

#[derive(Debug)]
pub enum ForwardError {
    Network(String),
    Upstream { status: StatusCode, body: String },
    Decode(String),
}

impl std::fmt::Display for ForwardError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Network(s) => write!(f, "shard proxy network error: {}", s),
            Self::Upstream { status, body } => {
                write!(f, "shard proxy upstream {}: {}", status, body)
            }
            Self::Decode(s) => write!(f, "shard proxy decode error: {}", s),
        }
    }
}
