//! Router configuration and CORS setup
//!
//! Handles HTTP router creation and environment-aware CORS configuration.

use std::sync::Arc;

use axum::http::{header, HeaderValue, Method};
use axum::{routing::get, Router, Json, response::IntoResponse};
use tower_http::cors::{Any, CorsLayer};
use tower_http::set_header::SetResponseHeaderLayer;
use tower_http::trace::TraceLayer;
use serde_json::json;

use crate::api;
use crate::app::state::AppState;
use crate::config::AppConfig;
use crate::websocket;

/// Build the main application router
pub fn create_router(state: Arc<AppState>) -> Router {
    let cors_layer = create_cors_layer(&state.config);

    Router::new()
        .route("/health", get(health_check))
        .route("/metrics", get(metrics_endpoint))
        .nest("/api/v1", api::routes::create_router(state.clone()))
        // Developer API: mounted at root level for Binance-compatible paths
        // e.g. /fapi/v1/ping, /fapi/v1/klines, /futures/data/openInterestHist
        .merge(api::routes::create_developer_router(state.clone()))
        .nest("/ws", websocket::routes::create_router(state.clone()))
        // HTTP 请求延迟 histogram：QA batch 4 增加，便于观测 p50/p95/p99
        // 和对任意路由做 SLO 告警。MatchedPath 用路由模板避免基数爆炸。
        .layer(axum::middleware::from_fn(crate::api::middleware::http_metrics::http_metrics_middleware))
        .layer(cors_layer)
        .layer(TraceLayer::new_for_http())
        .layer(tower_http::catch_panic::CatchPanicLayer::new())
        // Security headers
        .layer(SetResponseHeaderLayer::if_not_present(
            header::X_CONTENT_TYPE_OPTIONS,
            HeaderValue::from_static("nosniff"),
        ))
        .layer(SetResponseHeaderLayer::if_not_present(
            header::X_FRAME_OPTIONS,
            HeaderValue::from_static("DENY"),
        ))
        .layer(SetResponseHeaderLayer::if_not_present(
            header::HeaderName::from_static("x-xss-protection"),
            HeaderValue::from_static("1; mode=block"),
        ))
        .layer(SetResponseHeaderLayer::if_not_present(
            header::REFERRER_POLICY,
            HeaderValue::from_static("strict-origin-when-cross-origin"),
        ))
        .with_state(state)
}

/// Health check endpoint
async fn health_check() -> impl IntoResponse {
    const VERSION: &str = env!("CARGO_PKG_VERSION");
    Json(json!({
        "status": "ok",
        "version": VERSION
    }))
}

/// Prometheus metrics endpoint
async fn metrics_endpoint() -> impl IntoResponse {
    use axum::http::header;
    let body = crate::services::metrics::gather_metrics();
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
        body,
    )
}

/// Create environment-aware CORS layer
/// - Development: Allow all origins (for local testing convenience)
/// - Staging: Allow test domains + localhost
/// - Production: Allow only production domains
fn create_cors_layer(config: &AppConfig) -> CorsLayer {
    let origins = config.get_cors_origins();

    if origins.is_empty() {
        // Development mode: allow all origins
        tracing::warn!("⚠️  CORS: Allowing ALL origins (development mode)");
        CorsLayer::new()
            .allow_origin(Any)
            .allow_methods(Any)
            .allow_headers(Any)
    } else {
        // Staging/Production: strict origin list
        tracing::info!("🔒 CORS: Restricting to {} allowed origins", origins.len());
        for origin in &origins {
            tracing::debug!("   Allowed origin: {}", origin);
        }

        let origins: Vec<_> = origins.iter().filter_map(|s| s.parse().ok()).collect();

        CorsLayer::new()
            .allow_origin(origins)
            .allow_methods([
                Method::GET,
                Method::POST,
                Method::PUT,
                Method::PATCH,
                Method::DELETE,
                Method::OPTIONS,
            ])
            .allow_headers([
                header::CONTENT_TYPE,
                header::AUTHORIZATION,
                header::HeaderName::from_static("x-api-key"),
                header::HeaderName::from_static("x-mbx-apikey"),
                header::HeaderName::from_static("x-test-address"),
                header::HeaderName::from_static("x-user-address"),
            ])
            .allow_credentials(true)
    }
}
