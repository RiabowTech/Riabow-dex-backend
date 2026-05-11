use axum::{
    extract::{Query, WebSocketUpgrade, ws::{Message, WebSocket}},
    response::Response,
};
use futures::{sink::SinkExt, stream::StreamExt};
use serde::Deserialize;
use tokio_tungstenite::connect_async;
use tracing::Instrument;
use reqwest::Url;

#[allow(dead_code)]
#[derive(Deserialize)]
pub struct BinanceWsQuery {
    pub symbol: String,
    pub period: String,
}

#[allow(dead_code)]
pub async fn binance_kline_handler(
    ws: WebSocketUpgrade,
    Query(query): Query<BinanceWsQuery>,
) -> Response {
    ws.on_upgrade(move |socket| handle_binance_proxy(socket, query.symbol, query.period))
}

#[allow(dead_code)]
async fn handle_binance_proxy(client_socket: WebSocket, symbol: String, period: String) {
    let stream_name = format!("{}@kline_{}", symbol.to_lowercase(), period);
    let binance_url = format!("wss://fstream.binance.com/ws/{}", stream_name);

    tracing::info!("Connecting to Binance WS: {}", binance_url);

    let url = match Url::parse(&binance_url) {
        Ok(u) => u,
        Err(e) => {
            tracing::error!("Invalid Binance URL: {}", e);
            let _ = client_socket.close().await;
            return;
        }
    };

    let (ws_stream, _) = match connect_async(url).await {
        Ok(conn) => conn,
        Err(e) => {
            tracing::error!("Failed to connect to Binance WS: {}", e);
            let _ = client_socket.close().await;
            return;
        }
    };

    tracing::info!("Connected to Binance WS for {}", stream_name);

    let (mut binance_write, mut binance_read) = ws_stream.split();
    let (mut client_write, mut client_read) = client_socket.split();

    // Create a channel to signal termination
    let (tx, mut rx) = tokio::sync::broadcast::channel(1);

    // Task to forward messages from Binance to Client
    let tx_clone = tx.clone();
    let binance_to_client = tokio::spawn(async move {
        while let Some(msg) = binance_read.next().await {
            match msg {
                Ok(tokio_tungstenite::tungstenite::Message::Text(text)) => {
                    if client_write.send(Message::Text(text)).await.is_err() {
                        break;
                    }
                }
                Ok(tokio_tungstenite::tungstenite::Message::Binary(bin)) => {
                    if client_write.send(Message::Binary(bin)).await.is_err() {
                        break;
                    }
                }
                Ok(tokio_tungstenite::tungstenite::Message::Ping(data)) => {
                    if client_write.send(Message::Ping(data)).await.is_err() {
                        break;
                    }
                }
                Ok(tokio_tungstenite::tungstenite::Message::Pong(data)) => {
                    if client_write.send(Message::Pong(data)).await.is_err() {
                        break;
                    }
                }
                Ok(tokio_tungstenite::tungstenite::Message::Close(_)) => {
                    break;
                }
                Err(e) => {
                    tracing::error!("Binance WS error: {}", e);
                    break;
                }
                _ => {}
            }
        }
        let _ = tx_clone.send(());
    }.instrument(tracing::info_span!("binance-proxy-to-client")));

    // Task to forward messages from Client to Binance (mostly for Ping/Pong or Close)
    let tx_clone = tx.clone();
    let client_to_binance = tokio::spawn(async move {
        while let Some(msg) = client_read.next().await {
            match msg {
                 Ok(Message::Close(_)) => {
                     let _ = binance_write.close().await;
                     break;
                 }
                 // We generally don't want to forward arbitrary text/binary from client to Binance
                 // as it might interfere with the subscription or cause errors.
                 // But Ping/Pong is okay.
                 Ok(Message::Ping(data)) => {
                      if binance_write.send(tokio_tungstenite::tungstenite::Message::Ping(data)).await.is_err() {
                          break;
                      }
                 }
                 Ok(Message::Pong(data)) => {
                      if binance_write.send(tokio_tungstenite::tungstenite::Message::Pong(data)).await.is_err() {
                          break;
                      }
                 }
                 _ => {}
            }
        }
        let _ = tx_clone.send(());
    }.instrument(tracing::info_span!("binance-proxy-from-client")));

    // Wait for either task to finish
    let _ = rx.recv().await;
    
    // Abort tasks
    binance_to_client.abort();
    client_to_binance.abort();
    
    tracing::info!("Binance proxy connection closed for {}", stream_name);
}
