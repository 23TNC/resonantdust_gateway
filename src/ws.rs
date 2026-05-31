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
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::Response;
use futures_util::{SinkExt, StreamExt};
use spacetimedb_sdk::{DbContext, Table, TableWithPrimaryKey};
use tokio::sync::mpsc::{self, UnboundedSender};
use tracing::{debug, info, info_span, warn, Instrument};

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

    // This client's own upstream connections — one per backing database the
    // gate fronts: `shard` (cards/souls/…) and `regions` (zones/regions). Both
    // need per-client isolation: SpacetimeDB subscriptions are set-semantics, so
    // a shared upstream silently drops initial rows for a second subscriber.
    // Wait for both to connect before serving subscriptions — subscribing on a
    // not-yet-open connection loses the initial rows. Client `sub` frames buffer
    // in the WS stream meanwhile.
    let upstream_regions = await_ready(pool.fresh_regions(), &tx, "regions").await;
    let upstream_cards = await_ready(pool.fresh_cards(), &tx, "cards").await;

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
                Ok(cmsg) => {
                    handle(
                        &pool,
                        upstream_regions.as_ref(),
                        upstream_cards.as_ref(),
                        &tx,
                        cmsg,
                    )
                    .await
                }
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

    // Tear down this client's upstreams so its subscriptions/cache don't linger
    // (otherwise a later client subscribing to the same rows would find them
    // already cached and receive nothing).
    if let Some(conn) = &upstream_regions {
        let _ = conn.disconnect();
    }
    if let Some(conn) = &upstream_cards {
        let _ = conn.disconnect();
    }
    drop(tx);
    let _ = forward.await;
    info!("client disconnected");
}

/// Await a freshly-built per-client upstream's readiness oneshot (5s budget),
/// emitting a client-visible error and yielding `None` on miss. Generic over
/// the connection type so `shard` and `regions` upstreams share one path.
async fn await_ready<T>(
    built: Option<(T, tokio::sync::oneshot::Receiver<()>)>,
    tx: &UnboundedSender<String>,
    what: &str,
) -> Option<T> {
    match built {
        Some((conn, ready)) => match tokio::time::timeout(Duration::from_secs(5), ready).await {
            Ok(Ok(())) => Some(conn),
            _ => {
                let _ = tx.send(
                    GateMsg::Error {
                        error: format!("{what} upstream connect timed out"),
                    }
                    .to_json(),
                );
                None
            }
        },
        None => {
            let _ = tx.send(
                GateMsg::Error {
                    error: format!("{what} upstream unavailable"),
                }
                .to_json(),
            );
            None
        }
    }
}

async fn handle(
    pool: &Arc<Pool>,
    upstream_regions: Option<&Arc<bindings::regions::DbConnection>>,
    upstream_cards: Option<&Arc<bindings::cards::DbConnection>>,
    tx: &UnboundedSender<String>,
    msg: ClientMsg,
) {
    match msg {
        ClientMsg::Sub { sid, table, filter } => subscribe(
            upstream_regions,
            upstream_cards,
            tx,
            sid,
            &table,
            filter.as_deref(),
        ),
        ClientMsg::Unsub { sid } => {
            // MVP: per-sub teardown not wired; the upstream drops on disconnect.
            info!(sid, "unsub (no-op for now)");
        }
        ClientMsg::Call { cid, reducer, args } => {
            // `propose_action` is no longer a relay — the gate validates the
            // recipe across shards and applies it via narrow reducer calls.
            if reducer == "propose_action" {
                crate::propose::handle(pool, tx, cid, args).await;
            } else {
                relay_call(pool, tx, cid, &reducer, args).await;
            }
        }
    }
}

// ---- reads: subscribe a shard table, fan rows back -------------------

/// Serialize a generated (sats-`Serialize`) shard row to JSON for the client.
/// Two normalizations so the payload drops into the client's generated TS row
/// types: keys are camelCased (the sats bridge emits Rust snake_case), and
/// every number is stringified — u64 fields (`valid_at`, `macro_zone`, …)
/// exceed JS's safe-integer range, so they ride the wire as strings and the
/// client coerces them to `bigint`/`number` per field.
fn row_json<T: spacetimedb_sats::ser::Serialize + ?Sized>(row: &T) -> serde_json::Value {
    let raw = spacetimedb_sats::ser::serde::serialize_to(row, serde_json::value::Serializer)
        .unwrap_or(serde_json::Value::Null);
    normalize(raw)
}

/// Recursively camelCase object keys and stringify numbers (lossless transit
/// for 64-bit ints).
fn normalize(value: serde_json::Value) -> serde_json::Value {
    use serde_json::Value;
    match value {
        Value::Object(map) => Value::Object(
            map.into_iter()
                .map(|(k, v)| (to_camel(&k), normalize(v)))
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.into_iter().map(normalize).collect()),
        Value::Number(n) => Value::String(n.to_string()),
        other => other,
    }
}

fn to_camel(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut upper_next = false;
    for c in s.chars() {
        if c == '_' {
            upper_next = true;
        } else if upper_next {
            out.extend(c.to_uppercase());
            upper_next = false;
        } else {
            out.push(c);
        }
    }
    out
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
                    old: None,
                    row: row_json(row),
                }
                .to_json(),
            );
        });
        let tx_upd = $tx.clone();
        conn.db().$accessor().on_update(move |_ctx, old, new| {
            let _ = tx_upd.send(
                GateMsg::Row {
                    sid: $sid,
                    table: $name.to_string(),
                    op: "update",
                    old: Some(row_json(old)),
                    row: row_json(new),
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
                    old: None,
                    row: row_json(row),
                }
                .to_json(),
            );
        });
    }};
}

/// Issue the upstream subscription for `query` on `$conn` and wire its
/// applied/error callbacks back to the client. Split out from [`relay_table!`]
/// because the two upstreams (`shard`, `regions`) are distinct connection
/// types; both expose `subscription_builder()` via `DbContext`.
macro_rules! issue_sub {
    ($conn:expr, $tx:expr, $sid:expr, $query:expr) => {{
        let sid = $sid;
        let query = $query;
        debug!(sid, %query, "issuing upstream subscription");
        let tx_applied = $tx.clone();
        let tx_err = $tx.clone();
        $conn
            .subscription_builder()
            .on_applied(move |_ctx| {
                debug!(sid, "upstream subscription applied");
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
    }};
}

/// Route a table to its owning upstream: register row callbacks + issue the
/// subscription on `$opt` (an `Option<&Arc<DbConnection>>`), or emit a
/// client-visible error if that upstream isn't connected.
macro_rules! route {
    ($opt:expr, $what:literal, $access:path, $accessor:ident, $name:literal, $tx:expr, $sid:expr, $query:expr) => {{
        match $opt {
            Some(conn) => {
                relay_table!(conn, $tx, $sid, $access, $accessor, $name);
                issue_sub!(conn, $tx, $sid, $query);
            }
            None => {
                let _ = $tx.send(
                    GateMsg::Error {
                        error: format!("{} upstream unavailable", $what),
                    }
                    .to_json(),
                );
            }
        }
    }};
}

/// Fan a client subscription to the upstream that owns the table. `zones` and
/// `regions` live in the `regions` module's database; `cards`/`souls`/
/// `soul_privates` are still in the `shard` monolith. The client is oblivious —
/// it subscribes by table name and the gate picks the backing connection.
fn subscribe(
    regions: Option<&Arc<bindings::regions::DbConnection>>,
    cards: Option<&Arc<bindings::cards::DbConnection>>,
    tx: &UnboundedSender<String>,
    sid: u32,
    table: &str,
    filter: Option<&str>,
) {
    // The client addresses the regions DB's `cards` table (tile-cards) under the
    // logical name `tile_cards` so it doesn't collide with the cards DB's `cards`
    // table; the upstream query still targets the real table name (`cards`).
    let upstream_table = if table == "tile_cards" { "cards" } else { table };
    let query = match filter {
        Some(f) => format!("SELECT * FROM {upstream_table} WHERE {f}"),
        None => format!("SELECT * FROM {upstream_table}"),
    };
    match table {
        // region-owned tables → the per-client `regions` upstream
        "zones" => route!(regions, "regions", bindings::regions::zones_table::ZonesTableAccess, zones, "zones", tx, sid, query),
        "regions" => route!(regions, "regions", bindings::regions::regions_table::RegionsTableAccess, regions, "regions", tx, sid, query),
        // regions-DB tile-cards (the regions module's own `cards` table) → the
        // `regions` upstream, surfaced to the client as `tile_cards`.
        "tile_cards" => route!(regions, "regions", bindings::regions::cards_table::CardsTableAccess, cards, "tile_cards", tx, sid, query),
        // card-owned tables → the per-client `cards` upstream
        "cards" => route!(cards, "cards", bindings::cards::cards_table::CardsTableAccess, cards, "cards", tx, sid, query),
        "souls" => route!(cards, "cards", bindings::cards::souls_table::SoulsTableAccess, souls, "souls", tx, sid, query),
        "soul_privates" => route!(cards, "cards", bindings::cards::soul_privates_table::SoulPrivatesTableAccess, soul_privates, "soul_privates", tx, sid, query),
        other => {
            let _ = tx.send(
                GateMsg::Error {
                    error: format!("unsupported table {other:?}"),
                }
                .to_json(),
            );
        }
    }
}

// ---- writes: relay a reducer call to its owning database -------------

async fn relay_call(
    pool: &Arc<Pool>,
    tx: &UnboundedSender<String>,
    cid: u32,
    reducer: &str,
    args: serde_json::Value,
) {
    // Route the reducer to the database that owns it. The relay is anonymous —
    // gate-called reducers trust their args (auth is the gate's job), so
    // `ctx.sender` is immaterial and no bearer token is needed. `propose_action`
    // never reaches here (it's intercepted by `propose::handle`).
    let db = match reducer {
        "request_zone" | "ensure_region" => pool.regions_db(),
        "spawn_soul" | "add_card" | "place_card" | "request_blueprint" | "move_soul" => {
            pool.cards_db()
        }
        other => {
            // Nothing routes to the retired `shard` monolith anymore.
            let _ = tx.send(
                GateMsg::CallErr {
                    cid,
                    error: format!("gate: no backing database for reducer {other:?}"),
                }
                .to_json(),
            );
            return;
        }
    };
    let url = format!("{}/v1/database/{}/call/{}", pool.server_uri(), db, reducer);
    let reply = match reqwest::Client::new().post(&url).json(&args).send().await {
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
