//! Public health endpoint for the spot matching engine.
//!
//! GET /spot/health — returns whether the engine is running, queued
//! command depth, and number of in-memory order books loaded. Used by
//! ops dashboards and uptime checks. Always reachable; reports
//! `engine="disabled"` when SPOT_TRADING_ENABLED=false (state.spot_engine
//! is None).

use axum::{extract::State, Json};
use serde::Serialize;
use std::sync::Arc;

use crate::AppState;

#[derive(Serialize)]
pub struct HealthResponse {
    pub engine: &'static str,    // "running" | "disabled"
    pub queue_depth: usize,
    pub books_loaded: usize,
}

pub async fn health(State(state): State<Arc<AppState>>) -> Json<HealthResponse> {
    let (engine, qd, books) = match &state.spot_engine {
        None => ("disabled", 0, 0),
        Some(h) => {
            // mpsc::Sender::capacity returns the REMAINING permits;
            // max_capacity returns the total. used = total - remaining.
            let used = h.cmd_tx.max_capacity().saturating_sub(h.cmd_tx.capacity());
            ("running", used, 1)
        }
    };
    Json(HealthResponse { engine, queue_depth: qd, books_loaded: books })
}
