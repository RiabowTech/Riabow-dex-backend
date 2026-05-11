//! HTTP request latency metric middleware.
//!
//! Records `http_request_duration_seconds{method, path, status}` Histogram per
//! request. Uses axum `MatchedPath` to label by *route template*
//! (e.g. `/api/v1/orders/:order_id`) instead of concrete URL, preventing
//! cardinality explosion from UUID path params.

use axum::{
    body::Body,
    extract::MatchedPath,
    http::Request,
    middleware::Next,
    response::Response,
};
use std::time::Instant;

use crate::services::metrics::HTTP_REQUEST_DURATION;

pub async fn http_metrics_middleware(request: Request<Body>, next: Next) -> Response {
    let method = request.method().as_str().to_string();
    let path = request
        .extensions()
        .get::<MatchedPath>()
        .map(|p| p.as_str().to_string())
        .unwrap_or_else(|| "<unmatched>".to_string());

    let started = Instant::now();
    let response = next.run(request).await;
    let elapsed = started.elapsed().as_secs_f64();
    let status = response.status().as_u16().to_string();

    HTTP_REQUEST_DURATION
        .with_label_values(&[&method, &path, &status])
        .observe(elapsed);

    response
}
