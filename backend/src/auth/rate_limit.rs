//! Rate Limiting Middleware
//!
//! Implements sliding window rate limiting using DashMap for thread-safe
//! in-memory storage. Suitable for single-instance deployments.
//! For distributed deployments, consider Redis-based rate limiting.

use axum::{
    body::Body,
    extract::ConnectInfo,
    http::{Request, StatusCode},
    middleware::Next,
    response::Response,
};
use dashmap::DashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::Instrument;

/// Rate limiter configuration
#[derive(Clone)]
pub struct RateLimitConfig {
    /// Maximum requests per window
    pub max_requests: u32,
    /// Window duration in seconds
    pub window_secs: u64,
    /// Whether to skip rate limiting for authenticated users
    pub skip_authenticated: bool,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            max_requests: 100,
            window_secs: 60,
            skip_authenticated: false,
        }
    }
}

/// Rate limit entry for a single client
#[derive(Clone)]
struct RateLimitEntry {
    request_count: u32,
    window_start: Instant,
}

/// In-memory rate limiter using DashMap
pub struct RateLimiter {
    entries: DashMap<String, RateLimitEntry>,
    config: RateLimitConfig,
}

impl RateLimiter {
    pub fn new(config: RateLimitConfig) -> Self {
        let limiter = Self {
            entries: DashMap::new(),
            config,
        };

        // Start cleanup task to remove expired entries
        let entries = limiter.entries.clone();
        let window_secs = limiter.config.window_secs;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(window_secs * 2));
            loop {
                interval.tick().await;
                let now = Instant::now();
                let window = Duration::from_secs(window_secs);
                entries.retain(|_, entry| now.duration_since(entry.window_start) < window);
            }
        }.instrument(tracing::info_span!("rate-limit-cleanup")));

        limiter
    }

    /// Check if a request should be allowed
    pub fn check_rate_limit(&self, client_id: &str) -> Result<RateLimitInfo, RateLimitExceeded> {
        let now = Instant::now();
        let window = Duration::from_secs(self.config.window_secs);

        let mut entry = self.entries.entry(client_id.to_string()).or_insert_with(|| {
            RateLimitEntry {
                request_count: 0,
                window_start: now,
            }
        });

        // Reset window if expired
        if now.duration_since(entry.window_start) >= window {
            entry.request_count = 0;
            entry.window_start = now;
        }

        // Increment request count
        entry.request_count += 1;

        let remaining = self.config.max_requests.saturating_sub(entry.request_count);
        let reset_secs = self.config.window_secs
            - now.duration_since(entry.window_start).as_secs().min(self.config.window_secs);

        if entry.request_count > self.config.max_requests {
            return Err(RateLimitExceeded {
                retry_after_secs: reset_secs,
            });
        }

        Ok(RateLimitInfo {
            limit: self.config.max_requests,
            remaining,
            reset_secs,
        })
    }
}

/// Rate limit information returned to client
pub struct RateLimitInfo {
    pub limit: u32,
    pub remaining: u32,
    pub reset_secs: u64,
}

/// Rate limit exceeded error
pub struct RateLimitExceeded {
    pub retry_after_secs: u64,
}

/// Rate limiter state for use in middleware
#[derive(Clone)]
pub struct RateLimiterState(pub Arc<RateLimiter>);

impl RateLimiterState {
    pub fn new(config: RateLimitConfig) -> Self {
        Self(Arc::new(RateLimiter::new(config)))
    }

    pub fn default_api() -> Self {
        Self::new(RateLimitConfig {
            max_requests: 100,
            window_secs: 60,
            skip_authenticated: false,
        })
    }

    pub fn strict() -> Self {
        Self::new(RateLimitConfig {
            max_requests: 10,
            window_secs: 60,
            skip_authenticated: false,
        })
    }

    pub fn order_submission() -> Self {
        Self::new(RateLimitConfig {
            max_requests: 30,
            window_secs: 60,
            skip_authenticated: true, // Require auth anyway
        })
    }
}

/// Rate limiting middleware
pub async fn rate_limit_middleware(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    axum::extract::State(rate_limiter): axum::extract::State<RateLimiterState>,
    request: Request<Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    // Use IP address as client identifier
    let client_id = addr.ip().to_string();

    match rate_limiter.0.check_rate_limit(&client_id) {
        Ok(info) => {
            let mut response = next.run(request).await;

            // Add rate limit headers
            let headers = response.headers_mut();
            headers.insert(
                "X-RateLimit-Limit",
                info.limit.to_string().parse().unwrap(),
            );
            headers.insert(
                "X-RateLimit-Remaining",
                info.remaining.to_string().parse().unwrap(),
            );
            headers.insert(
                "X-RateLimit-Reset",
                info.reset_secs.to_string().parse().unwrap(),
            );

            Ok(response)
        }
        Err(exceeded) => {
            tracing::warn!(
                "Rate limit exceeded for client {}: retry after {} seconds",
                client_id,
                exceeded.retry_after_secs
            );

            let mut response = Response::builder()
                .status(StatusCode::TOO_MANY_REQUESTS)
                .body(Body::from("Too many requests. Please try again later."))
                .unwrap();

            response.headers_mut().insert(
                "Retry-After",
                exceeded.retry_after_secs.to_string().parse().unwrap(),
            );

            Ok(response)
        }
    }
}

/// Simpler rate limit middleware that doesn't require ConnectInfo
/// Uses X-Forwarded-For header or falls back to "unknown"
pub async fn rate_limit_by_header(
    axum::extract::State(rate_limiter): axum::extract::State<RateLimiterState>,
    request: Request<Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    // Try to get client IP from X-Forwarded-For header or X-Real-IP
    let client_id = request
        .headers()
        .get("X-Forwarded-For")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.split(',').next())
        .map(|s| s.trim().to_string())
        .or_else(|| {
            request
                .headers()
                .get("X-Real-IP")
                .and_then(|h| h.to_str().ok())
                .map(|s| s.to_string())
        })
        .unwrap_or_else(|| "unknown".to_string());

    match rate_limiter.0.check_rate_limit(&client_id) {
        Ok(info) => {
            let mut response = next.run(request).await;

            // Add rate limit headers
            let headers = response.headers_mut();
            headers.insert(
                "X-RateLimit-Limit",
                info.limit.to_string().parse().unwrap(),
            );
            headers.insert(
                "X-RateLimit-Remaining",
                info.remaining.to_string().parse().unwrap(),
            );
            headers.insert(
                "X-RateLimit-Reset",
                info.reset_secs.to_string().parse().unwrap(),
            );

            Ok(response)
        }
        Err(exceeded) => {
            tracing::warn!(
                "Rate limit exceeded for client {}: retry after {} seconds",
                client_id,
                exceeded.retry_after_secs
            );

            let mut response = Response::builder()
                .status(StatusCode::TOO_MANY_REQUESTS)
                .body(Body::from("Too many requests. Please try again later."))
                .unwrap();

            response.headers_mut().insert(
                "Retry-After",
                exceeded.retry_after_secs.to_string().parse().unwrap(),
            );

            Ok(response)
        }
    }
}
