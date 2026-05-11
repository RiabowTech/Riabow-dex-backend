use axum::{
    body::Body,
    extract::State,
    http::{Request, StatusCode},
    middleware::Next,
    response::Response,
};
use std::sync::Arc;

use crate::AppState;

/// Middleware to restrict access to development environment only
pub async fn dev_only_middleware(
    State(state): State<Arc<AppState>>,
    request: Request<Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    // Check if environment is development
    let is_dev = state.config.environment == "development" 
        || std::env::var("ENVIRONMENT").unwrap_or_default() == "development"
        || std::env::var("ENV").unwrap_or_default() == "development";

    if !is_dev {
        // Return 403 Forbidden if not in development
        return Err(StatusCode::FORBIDDEN);
    }

    Ok(next.run(request).await)
}

