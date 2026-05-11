//! API Key Authentication Middleware
//!
//! Protects admin/internal endpoints with API key authentication.
//! The API key should be passed in the `X-API-Key` header.

use axum::{
    body::Body,
    extract::State,
    http::{Request, StatusCode},
    middleware::Next,
    response::Response,
};
use std::sync::Arc;
use tracing::warn;

use crate::AppState;

/// Header name for the API key
pub const API_KEY_HEADER: &str = "X-API-Key";

/// Middleware to restrict access to endpoints requiring an API key.
///
/// Usage:
/// - Set `ADMIN_API_KEY` environment variable
/// - Pass the key in the `X-API-Key` header
///
/// Returns 401 Unauthorized if:
/// - No API key is configured on the server
/// - No API key is provided in the request
/// - The provided API key doesn't match
pub async fn api_key_middleware(
    State(state): State<Arc<AppState>>,
    request: Request<Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    // Get the configured API key
    let configured_key = match &state.config.admin_api_key {
        Some(key) if !key.is_empty() => key,
        _ => {
            warn!("Admin API key not configured, rejecting request");
            return Err(StatusCode::UNAUTHORIZED);
        }
    };

    // Get the API key from the request header
    let provided_key = request
        .headers()
        .get(API_KEY_HEADER)
        .and_then(|v| v.to_str().ok());

    match provided_key {
        Some(key) if key == configured_key => {
            // API key matches, proceed
            Ok(next.run(request).await)
        }
        Some(_) => {
            warn!("Invalid API key provided");
            Err(StatusCode::UNAUTHORIZED)
        }
        None => {
            warn!("No API key provided in {} header", API_KEY_HEADER);
            Err(StatusCode::UNAUTHORIZED)
        }
    }
}
