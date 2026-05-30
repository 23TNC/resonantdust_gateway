//! Per-connection WebSocket handling — the gate's client-facing relay.
//!
//! A client subscribes to shard tables (the gate issues the upstream
//! subscription and fans live rows back) and calls reducers (the gate relays to
//! shard's HTTP `/call`). Gate→client messages all funnel through one
//! unbounded mpsc so the SDK's row callbacks (which fire on the SDK's own
//! thread) and the async request loop can both push to the single WS sink,
//! which only the forwarder task owns.
//!
//! Known MVP gaps (deliberate, called out for later): `unsub` doesn't yet tear
//! down the registered callbacks, and each subscriber registers its own
//! upstream callbacks rather than sharing one deduped upstream subscription per
//! query — the fan-out coalescing comes when we measure capacity.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::Response;
use futures_util::{SinkExt, StreamExt};
use spacetimedb_sdk::{DbContext, Table};
use tokio::sync::mpsc::{self, UnboundedSender};
use tracing::{info, info_span, warn, Instrument};

use crate::bindings;
use crate::connections::Pool;
use crate::protocol::{ClientMsg, GateMsg};

/// Monotonic per-process connection id, for log correlation.
static NEXT_CONN_ID: AtomicU64 = AtomicU64::new(1);

/// Axum handler for `GET /ws`: upgrade and drive the connection.
pub async fn handler(upgrade: WebSocketUpgrade, State(pool): State<Arc<Pool>>) -> Response {
    let id = NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed);
    upgrade.on_upgrade(move |socket| run(socket, pool).instrument(info_span!("conn", id)))
}

async fn run(socket: WebSocket, pool: Arc<Pool>) {
    info!("client connected");
    let (mut sink, mut stream) = socket.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();

    // Sole owner of the sink: drain the channel to the client.
    let forward = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if sink.send(Message::Text(msg.into())).await.is_err() {
                break;
            }
        }
        let _ = sink.close().await;
    });

    // Warm the shard upstream up front so the first request doesn't pay it.
    if pool.shard().is_none() {
        let _ = tx.send(
            GateMsg::Error {
                error: "shard upstream unavailable".to_string(),
            }
            .to_json(),
        );
    }

    while let Some(frame) = stream.next().await {
        let msg = match frame {
            Ok(m) => m,
            Err(err) => {
                warn!(%err, "receive error");
                break;
            }
        };
        match msg {
            Message::Text(text) => match serde_json::from_str::<ClientMsg>(&text) {
                Ok(cmsg) => handle(&pool, &tx, cmsg).await,
                Err(err) => {
                    let _ = tx.send(
                        GateMsg::Error {
                            error: format!("bad message: {err}"),
                        }
                        .to_json(),
                    );
                }
            },
            Message::Close(_) => break,
            Message::Binary(_) | Message::Ping(_) | Message::Pong(_) => {}
        }
    }

    drop(tx);
    let _ = forward.await;
    info!("client disconnected");
}

async fn handle(pool: &Arc<Pool>, tx: &UnboundedSender<String>, msg: ClientMsg) {
    match msg {
        ClientMsg::Sub { sid, table, filter } => subscribe(pool, tx, sid, &table, filter.as_deref()),
        ClientMsg::Unsub { sid } => {
            // MVP: callbacks aren't torn down yet.
            info!(sid, "unsub (no-op for now)");
        }
        ClientMsg::Call { cid, reducer, args } => relay_call(pool, tx, cid, &reducer, args).await,
    }
}

// ---- reads: subscribe a shard table, fan rows back -------------------

/// Serialize a generated (sats-`Serialize`) shard row to JSON for the client.
fn row_json<T: spacetimedb_sats::ser::Serialize + ?Sized>(row: &T) -> serde_json::Value {
    spacetimedb_sats::ser::serde::serialize_to(row, serde_json::value::Serializer)
        .unwrap_or(serde_json::Value::Null)
}

/// Register insert/delete callbacks for one shard table that push [`GateMsg::Row`]
/// for subscription `sid`. The table's access trait + accessor are passed in.
macro_rules! relay_table {
    ($conn:expr, $tx:expr, $sid:expr, $access:path, $accessor:ident, $name:literal) => {{
        use $access;
        let conn = &$conn;
        let tx_ins = $tx.clone();
        conn.db().$accessor().on_insert(move |_ctx, row| {
            let _ = tx_ins.send(
                GateMsg::Row {
                    sid: $sid,
                    table: $name.to_string(),
                    op: "insert",
                    row: row_json(row),
                }
                .to_json(),
            );
        });
        let tx_del = $tx.clone();
        conn.db().$accessor().on_delete(move |_ctx, row| {
            let _ = tx_del.send(
                GateMsg::Row {
                    sid: $sid,
                    table: $name.to_string(),
                    op: "delete",
                    row: row_json(row),
                }
                .to_json(),
            );
        });
    }};
}

fn subscribe(
    pool: &Arc<Pool>,
    tx: &UnboundedSender<String>,
    sid: u32,
    table: &str,
    filter: Option<&str>,
) {
    let Some(conn) = pool.shard() else {
        let _ = tx.send(
            GateMsg::Error {
                error: "shard upstream unavailable".to_string(),
            }
            .to_json(),
        );
        return;
    };

    match table {
        "zones" => relay_table!(conn, tx, sid, bindings::shard::zones_table::ZonesTableAccess, zones, "zones"),
        "cards" => relay_table!(conn, tx, sid, bindings::shard::cards_table::CardsTableAccess, cards, "cards"),
        "souls" => relay_table!(conn, tx, sid, bindings::shard::souls_table::SoulsTableAccess, souls, "souls"),
        other => {
            let _ = tx.send(
                GateMsg::Error {
                    error: format!("unsupported table {other:?}"),
                }
                .to_json(),
            );
            return;
        }
    }

    let query = match filter {
        Some(f) => format!("SELECT * FROM {table} WHERE {f}"),
        None => format!("SELECT * FROM {table}"),
    };
    let tx_applied = tx.clone();
    let tx_err = tx.clone();
    conn.subscription_builder()
        .on_applied(move |_ctx| {
            let _ = tx_applied.send(GateMsg::Applied { sid }.to_json());
        })
        .on_error(move |_ctx, err| {
            let _ = tx_err.send(
                GateMsg::Error {
                    error: format!("sub {sid}: {err}"),
                }
                .to_json(),
            );
        })
        .subscribe([query]);
}

// ---- writes: relay a reducer call to shard ---------------------------

async fn relay_call(
    pool: &Arc<Pool>,
    tx: &UnboundedSender<String>,
    cid: u32,
    reducer: &str,
    args: serde_json::Value,
) {
    // Ensure the upstream (and thus the auth token) is established.
    let _ = pool.shard();
    let url = format!(
        "{}/v1/database/{}/call/{}",
        pool.server_uri(),
        pool.shard_db(),
        reducer
    );
    let mut req = reqwest::Client::new().post(&url).json(&args);
    if let Some(token) = pool.shard_token() {
        req = req.bearer_auth(token);
    }

    let reply = match req.send().await {
        Ok(resp) if resp.status().is_success() => GateMsg::CallOk { cid },
        Ok(resp) => {
            let code = resp.status();
            let body = resp.text().await.unwrap_or_default();
            GateMsg::CallErr {
                cid,
                error: format!("{code}: {body}"),
            }
        }
        Err(err) => GateMsg::CallErr {
            cid,
            error: err.to_string(),
        },
    };
    let _ = tx.send(reply.to_json());
}
