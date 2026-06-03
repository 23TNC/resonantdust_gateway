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

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::Response;
use futures_util::{SinkExt, StreamExt};
use spacetimedb_sdk::{DbContext, SubscriptionHandle, Table, TableWithPrimaryKey};
use tokio::sync::mpsc::{self, UnboundedSender};
use tracing::{debug, info, info_span, warn, Instrument};

use crate::bindings;
use crate::connections::Pool;
use crate::protocol::{now_micros, ClientMsg, GateMsg};

/// How long a connection may be silent before the gate sends a standalone clock
/// keepalive. Active clients sync via the `call_ok`/`call_err` piggyback, so this
/// only fires for otherwise-idle sockets (plus once immediately on connect, for a
/// fast initial lock). Kept well under any idle-disconnect window.
const IDLE_HEARTBEAT: Duration = Duration::from_secs(10);

/// Monotonic per-process connection id, for log correlation.
static NEXT_CONN_ID: AtomicU64 = AtomicU64::new(1);

/// One live upstream subscription handle, wrapped per owning module so the
/// per-client registry can hold them uniformly and tear them down on `unsub`
/// (the four modules' generated `SubscriptionHandle` types are distinct).
enum UpHandle {
    Regions(bindings::regions::SubscriptionHandle),
    Cards(bindings::cards::SubscriptionHandle),
    Chat(bindings::chat::SubscriptionHandle),
    Players(bindings::players::SubscriptionHandle),
}

impl UpHandle {
    /// Tear down the upstream subscription. `unsubscribe` consumes the handle;
    /// errors (already-ended) are not actionable here.
    fn unsubscribe(self) {
        let _ = match self {
            UpHandle::Regions(h) => h.unsubscribe(),
            UpHandle::Cards(h) => h.unsubscribe(),
            UpHandle::Chat(h) => h.unsubscribe(),
            UpHandle::Players(h) => h.unsubscribe(),
        };
    }
}

/// One deduped upstream subscription: the live handle plus the client sids that
/// share it (refcount). Torn down when the last sid unsubscribes.
struct QueryEntry {
    handle: UpHandle,
    sids: HashSet<u32>,
}

/// Per-connection subscription state. Two jobs: (1) dedup upstream subscriptions
/// by query SQL so re-scoped subs reuse one upstream, and (2) register each
/// table's row callbacks exactly once, so a single upstream row event fans back
/// one `Row` — not one per client sid (the old per-sid table-global callbacks
/// multiplied every row by the live-sub count). Owned by the connection task;
/// mutated only from the single-threaded message loop.
#[derive(Default)]
struct SubRegistry {
    /// query SQL → its live upstream + the sids sharing it.
    queries: HashMap<String, QueryEntry>,
    /// sid → the query it subscribed (reverse lookup for `unsub`).
    sid_query: HashMap<u32, String>,
    /// tables whose row callbacks are already wired on this connection.
    wired: HashSet<&'static str>,
}

impl SubRegistry {
    /// Drop a client sid; if it was the last sharer of its query, tear down the
    /// upstream subscription. Table callbacks stay wired (cheap, shared, and
    /// harmless once their query set is empty — no rows match).
    fn remove_sid(&mut self, sid: u32) {
        let Some(query) = self.sid_query.remove(&sid) else {
            return;
        };
        if let Some(entry) = self.queries.get_mut(&query) {
            entry.sids.remove(&sid);
            if entry.sids.is_empty() {
                if let Some(entry) = self.queries.remove(&query) {
                    entry.handle.unsubscribe();
                }
            }
        }
    }
}

/// Axum handler for `GET /ws`: upgrade and drive the connection.
pub async fn handler(upgrade: WebSocketUpgrade, State(pool): State<Arc<Pool>>) -> Response {
    let id = NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed);
    upgrade.on_upgrade(move |socket| run(socket, pool).instrument(info_span!("conn", id)))
}

async fn run(socket: WebSocket, pool: Arc<Pool>) {
    info!("client connected");
    let (mut sink, mut stream) = socket.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();

    // Sole owner of the sink: drain the channel to the client, and emit the idle
    // clock keepalive. A standalone `time` frame is sent ONLY when the socket has
    // been silent for `IDLE_HEARTBEAT` — every real frame resets the timer, so an
    // active client (which already gets server-time samples piggybacked on its
    // `call_ok`/`call_err` round-trips) never costs a heartbeat. The first tick
    // fires immediately on connect for a fast initial clock lock.
    let forward = tokio::spawn(async move {
        let mut idle = tokio::time::interval(IDLE_HEARTBEAT);
        idle.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased; // prefer draining real traffic over the keepalive
                msg = rx.recv() => match msg {
                    Some(m) => {
                        if sink.send(Message::Text(m.into())).await.is_err() {
                            break;
                        }
                        idle.reset(); // traffic flowed → push the keepalive out
                    }
                    None => break, // channel closed → teardown
                },
                _ = idle.tick() => {
                    let frame = GateMsg::Time { server_micros: now_micros() }.to_json();
                    if sink.send(Message::Text(frame.into())).await.is_err() {
                        break;
                    }
                }
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
    let upstream_chat = await_ready(pool.fresh_chat(), &tx, "chat").await;
    let upstream_players = await_ready(pool.fresh_players(), &tx, "players").await;

    // The gate OWNS this connection's session: WS → player_id, established at
    // login (`claim_or_login` interception reads the player row by name off the
    // players upstream) and injected into the player reducers. Ephemeral
    // gate state — lost on a gate crash, reconstructed when the client
    // reconnects + re-logs-in from shard truth.
    let session: tokio::sync::Mutex<Option<u32>> = tokio::sync::Mutex::new(None);

    // This client's subscription state: dedups upstreams by query and wires each
    // table's row callbacks once. Owned here, mutated only in this loop.
    let mut registry = SubRegistry::default();

    // (Clock sync is handled by the forwarder: `call_ok`/`call_err` piggyback for
    // active clients, plus the idle `Time` keepalive — no separate task.)

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
                        upstream_chat.as_ref(),
                        upstream_players.as_ref(),
                        &session,
                        &mut registry,
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
    if let Some(conn) = &upstream_chat {
        let _ = conn.disconnect();
    }
    if let Some(conn) = &upstream_players {
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
    upstream_chat: Option<&Arc<bindings::chat::DbConnection>>,
    upstream_players: Option<&Arc<bindings::players::DbConnection>>,
    session: &tokio::sync::Mutex<Option<u32>>,
    registry: &mut SubRegistry,
    tx: &UnboundedSender<String>,
    msg: ClientMsg,
) {
    match msg {
        ClientMsg::Sub { sid, table, filter } => subscribe(
            upstream_regions,
            upstream_cards,
            upstream_chat,
            upstream_players,
            registry,
            tx,
            sid,
            &table,
            filter.as_deref(),
        ),
        ClientMsg::Unsub { sid } => {
            // Drop this sid; the upstream tears down when its last sharer leaves.
            registry.remove_sid(sid);
        }
        ClientMsg::Call { cid, reducer, args } => {
            // `propose_action` is no longer a relay — the gate validates the
            // recipe across shards and applies it via narrow reducer calls.
            if reducer == "propose_action" {
                crate::propose::handle(pool, tx, cid, args).await;
            } else if reducer == "claim_or_login" {
                // Login establishes the gate-owned session (WS → player_id).
                login_relay(pool, upstream_players, session, tx, cid, args).await;
            } else {
                relay_call(pool, session, tx, cid, &reducer, args).await;
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

/// Register insert/update/delete callbacks for one table that push
/// [`GateMsg::Row`] tagged by **table name** (sid is a sentinel `0` — the client
/// routes rows by table, not sid). Registered **once per (connection, table)**:
/// the SDK fires these for every row in the connection's whole subscription set,
/// so one set covers all of a table's queries and a row event fans back exactly
/// one `Row`. The table's access trait + accessor are passed in.
macro_rules! relay_table {
    ($conn:expr, $tx:expr, $access:path, $accessor:ident, $name:literal) => {{
        use $access;
        let conn = &$conn;
        let tx_ins = $tx.clone();
        conn.db().$accessor().on_insert(move |_ctx, row| {
            let _ = tx_ins.send(
                GateMsg::Row {
                    sid: 0,
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
                    sid: 0,
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
                    sid: 0,
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
        // Evaluates to the `SubscriptionHandle` so `route!` can store it for
        // refcounted teardown (no trailing `;`).
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
            .subscribe([query])
    }};
}

/// Route a table to its owning upstream with dedup + refcount. If an identical
/// query is already live, attach this sid and ack immediately (rows already
/// flow). Otherwise wire the table's row callbacks once, issue the upstream
/// subscription, and store the handle (wrapped in `$variant`) keyed by query.
/// Emits a client-visible error if the upstream isn't connected.
macro_rules! route {
    ($reg:expr, $opt:expr, $what:literal, $access:path, $accessor:ident, $name:literal, $tx:expr, $sid:expr, $query:expr, $variant:path) => {{
        match $opt {
            Some(conn) => {
                if let Some(entry) = $reg.queries.get_mut(&$query) {
                    // Dedup hit: share the live upstream. Ack synthetically since
                    // no new `on_applied` will fire for this sid.
                    entry.sids.insert($sid);
                    $reg.sid_query.insert($sid, $query);
                    let _ = $tx.send(GateMsg::Applied { sid: $sid }.to_json());
                } else {
                    // First sharer of this query. Wire the table's callbacks once
                    // (covers every query on the table), issue the upstream, and
                    // record the handle for refcounted teardown on `unsub`.
                    if $reg.wired.insert($name) {
                        relay_table!(conn, $tx, $access, $accessor, $name);
                    }
                    let handle = issue_sub!(conn, $tx, $sid, $query.clone());
                    let mut sids = HashSet::new();
                    sids.insert($sid);
                    $reg.queries.insert(
                        $query.clone(),
                        QueryEntry {
                            handle: $variant(handle),
                            sids,
                        },
                    );
                    $reg.sid_query.insert($sid, $query);
                }
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
    chat: Option<&Arc<bindings::chat::DbConnection>>,
    players: Option<&Arc<bindings::players::DbConnection>>,
    registry: &mut SubRegistry,
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
        "zones" => route!(registry, regions, "regions", bindings::regions::zones_table::ZonesTableAccess, zones, "zones", tx, sid, query, UpHandle::Regions),
        "regions" => route!(registry, regions, "regions", bindings::regions::regions_table::RegionsTableAccess, regions, "regions", tx, sid, query, UpHandle::Regions),
        // regions-DB tile-cards (the regions module's own `cards` table) → the
        // `regions` upstream, surfaced to the client as `tile_cards`.
        "tile_cards" => route!(registry, regions, "regions", bindings::regions::cards_table::CardsTableAccess, cards, "tile_cards", tx, sid, query, UpHandle::Regions),
        // card-owned tables → the per-client `cards` upstream
        "cards" => route!(registry, cards, "cards", bindings::cards::cards_table::CardsTableAccess, cards, "cards", tx, sid, query, UpHandle::Cards),
        "souls" => route!(registry, cards, "cards", bindings::cards::souls_table::SoulsTableAccess, souls, "souls", tx, sid, query, UpHandle::Cards),
        "soul_privates" => route!(registry, cards, "cards", bindings::cards::soul_privates_table::SoulPrivatesTableAccess, soul_privates, "soul_privates", tx, sid, query, UpHandle::Cards),
        // chat-owned tables → the per-client `chat` upstream
        "chat_messages" => route!(registry, chat, "chat", bindings::chat::chat_messages_table::ChatMessagesTableAccess, chat_messages, "chat_messages", tx, sid, query, UpHandle::Chat),
        // players auth-DB tables → the per-client `players` upstream
        "players" => route!(registry, players, "players", bindings::players::players_table::PlayersTableAccess, players, "players", tx, sid, query, UpHandle::Players),
        "player_profiles" => route!(registry, players, "players", bindings::players::player_profiles_table::PlayerProfilesTableAccess, player_profiles, "player_profiles", tx, sid, query, UpHandle::Players),
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

// ---- session establishment: claim_or_login + player-id read ----------

/// Relay `claim_or_login` to the players DB, then establish the gate-owned
/// session: read the resulting `player_id` off the per-client players upstream
/// (the client subscribes `players WHERE name = <name>` before logging in, so
/// the row flows in shortly after the commit) and store it. The gate — not the
/// client — is the authority on which player this WS is.
async fn login_relay(
    pool: &Arc<Pool>,
    upstream_players: Option<&Arc<bindings::players::DbConnection>>,
    session: &tokio::sync::Mutex<Option<u32>>,
    tx: &UnboundedSender<String>,
    cid: u32,
    args: serde_json::Value,
) {
    let name = args
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let url = format!(
        "{}/v1/database/{}/call/claim_or_login",
        pool.server_uri(),
        pool.players_db()
    );
    match reqwest::Client::new().post(&url).json(&args).send().await {
        Ok(resp) if resp.status().is_success() => {}
        Ok(resp) => {
            let code = resp.status();
            let body = resp.text().await.unwrap_or_default();
            let _ = tx.send(GateMsg::call_err(cid, format!("{code}: {body}")));
            return;
        }
        Err(err) => {
            let _ = tx.send(GateMsg::call_err(cid, err.to_string()));
            return;
        }
    }
    match read_player_id_by_name(upstream_players, &name).await {
        Some(pid) => {
            *session.lock().await = Some(pid);
            debug!(player_id = pid, name = %name, "gate session established");
        }
        None => warn!(name = %name, "gate: login ok but player row not seen — session not set"),
    }
    let _ = tx.send(GateMsg::call_ok(cid));
}

/// Poll the players upstream's cache for the (any-version) row with `name`,
/// returning its `player_id`. Bounded retry: the SDK delivers the just-committed
/// row asynchronously, so a few hundred ms of slack covers the propagation.
async fn read_player_id_by_name(
    upstream_players: Option<&Arc<bindings::players::DbConnection>>,
    name: &str,
) -> Option<u32> {
    use bindings::players::players_table::PlayersTableAccess;
    let conn = upstream_players?;
    for _ in 0..20 {
        if let Some(p) = conn.db().players().iter().find(|p| p.name == name) {
            return Some(p.player_id);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    None
}

// ---- writes: relay a reducer call to its owning database -------------

async fn relay_call(
    pool: &Arc<Pool>,
    session: &tokio::sync::Mutex<Option<u32>>,
    tx: &UnboundedSender<String>,
    cid: u32,
    reducer: &str,
    args: serde_json::Value,
) {
    // The gate INJECTS the session player_id into the player reducers (it owns
    // the session, so the client never supplies it). `set_last_login` requires
    // an active session; reject if not logged in.
    let mut args = args;
    if reducer == "set_last_login" {
        match *session.lock().await {
            Some(pid) => {
                if let Some(obj) = args.as_object_mut() {
                    obj.insert("player_id".to_string(), serde_json::json!(pid));
                }
            }
            None => {
                let _ = tx.send(GateMsg::call_err(
                    cid,
                    "set_last_login: no session (not logged in)".to_string(),
                ));
                return;
            }
        }
    }
    // Route the reducer to the database that owns it. The relay is anonymous —
    // gate-called reducers trust their args (auth is the gate's job), so
    // `ctx.sender` is immaterial and no bearer token is needed. `propose_action`
    // and `claim_or_login` never reach here (intercepted in `handle`).
    let db = match reducer {
        "request_zone" | "ensure_region" => pool.regions_db(),
        "spawn_soul" | "add_card" | "place_card" | "request_blueprint" | "move_soul" => {
            pool.cards_db()
        }
        "send_chat_message" => pool.chat_db(),
        "set_last_login" => pool.players_db(),
        other => {
            // Nothing routes to the retired `shard` monolith anymore.
            let _ = tx.send(GateMsg::call_err(
                cid,
                format!("gate: no backing database for reducer {other:?}"),
            ));
            return;
        }
    };
    let url = format!("{}/v1/database/{}/call/{}", pool.server_uri(), db, reducer);
    let reply = match reqwest::Client::new().post(&url).json(&args).send().await {
        Ok(resp) if resp.status().is_success() => GateMsg::call_ok(cid),
        Ok(resp) => {
            let code = resp.status();
            let body = resp.text().await.unwrap_or_default();
            GateMsg::call_err(cid, format!("{code}: {body}"))
        }
        Err(err) => GateMsg::call_err(cid, err.to_string()),
    };
    let _ = tx.send(reply);
}
