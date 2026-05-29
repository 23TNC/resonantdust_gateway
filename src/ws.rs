//! Per-connection WebSocket handling.
//!
//! Milestone 1 is transport-only: accept the upgrade, log the connection
//! lifecycle, answer pings, and echo inbound text frames as a placeholder.
//! When the real client<->gate protocol lands, the echo in [`run`] is the only
//! throwaway part — frame decode/dispatch slots in there, and the upstream
//! shard fan-out hangs off the same connection task.

use std::sync::atomic::{AtomicU64, Ordering};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::Response;
use futures_util::{SinkExt, StreamExt};

/// Monotonic per-process connection id, just for log correlation.
static NEXT_CONN_ID: AtomicU64 = AtomicU64::new(1);

/// Axum handler for `GET /ws`: performs the WebSocket upgrade and hands the
/// socket to [`run`] on its own task.
pub async fn handler(upgrade: WebSocketUpgrade) -> Response {
    let id = NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed);
    upgrade.on_upgrade(move |socket| run(socket, id))
}

/// Drive a single client connection until it closes or errors.
async fn run(socket: WebSocket, id: u64) {
    let span = tracing::info_span!("conn", id);
    let _enter = span.enter();
    tracing::info!("client connected");

    let (mut sink, mut stream) = socket.split();

    while let Some(frame) = stream.next().await {
        let msg = match frame {
            Ok(msg) => msg,
            Err(err) => {
                tracing::warn!(%err, "receive error");
                break;
            }
        };

        match msg {
            Message::Text(text) => {
                // Placeholder: echo back until the real protocol decodes here.
                tracing::debug!(bytes = text.len(), "text frame");
                if let Err(err) = sink.send(Message::Text(text)).await {
                    tracing::warn!(%err, "send error");
                    break;
                }
            }
            Message::Binary(bytes) => {
                tracing::debug!(bytes = bytes.len(), "binary frame (ignored)");
            }
            // axum/tungstenite answers ping with pong automatically; we just
            // observe the liveness traffic.
            Message::Ping(_) | Message::Pong(_) => {}
            Message::Close(frame) => {
                tracing::info!(?frame, "client requested close");
                break;
            }
        }
    }

    // Drive the close handshake to completion: on a client-initiated close
    // tungstenite has already queued its echo, and `close` flushes it so the
    // client sees a clean 1000 rather than an abnormal 1006.
    let _ = sink.close().await;
    tracing::info!("client disconnected");
}
