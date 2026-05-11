use axum::{
    extract::{
        ws::WebSocketUpgrade,
        State,
    },
    response::Response,
    routing::get,
    Router,
};
use std::sync::Arc;

use crate::websocket::handler::handle_socket;
use crate::websocket::external_handler::handle_external_socket;
// [DISABLED] Binance proxy - using internal data only
// use crate::websocket::binance_proxy::binance_kline_handler;
use crate::AppState;

pub fn create_router(_state: Arc<AppState>) -> Router<Arc<AppState>> {
    Router::new()
        .route("/", get(ws_handler))
        .route("/internal", get(ws_internal_handler))  // Explicit internal data endpoint
        .route("/external", get(ws_external_handler))
        // [DISABLED] Binance kline WebSocket proxy - using internal data only
        // .route("/binance/kline", get(binance_kline_handler))
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> Response {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn ws_internal_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> Response {
    // Same as /ws/ - provides internal real order/trade/position data
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn ws_external_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> Response {
    ws.on_upgrade(move |socket| handle_external_socket(socket, state))
}
