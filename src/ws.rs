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
use resonantdust_protocol::protocol::{now_micros, ClientMsg, GateMsg, RowOp};

/// How long a connection may be silent before the gate sends a standalone clock
/// keepalive. Active clients sync via the `call_ok`/`call_err` piggyback, so this
/// only fires for otherwise-idle sockets (plus once immediately on connect, for a
/// fast initial lock). Kept well under any idle-disconnect window.
const IDLE_HEARTBEAT: Duration = Duration::from_secs(10);

/// How often the connection loop evaluates outstanding async-call promises.
const PROMISE_POLL: Duration = Duration::from_millis(50);
/// How long a `request_zone`/`ensure_region` promise awaits its `available`/region
/// before timing out into a `call_err` (→ client retry). Materialization is
/// synchronous in the shard; this only bounds the gate's subscription catch-up.
const PROMISE_TIMEOUT: Duration = Duration::from_secs(5);

/// Adapts the per-connection regions subscription to [`crate::promise::RegionView`]
/// so a promise resolver can read a zone's `available` bit at poll time.
struct RegionsView<'a>(Option<&'a bindings::shard::DbConnection>);
impl crate::promise::RegionView for RegionsView<'_> {
    fn region_bits(&self, macro_region: u64) -> Option<(u64, u64)> {
        self.0.and_then(|c| crate::gather::region_bits(c, macro_region))
    }
}

/// Monotonic per-process connection id, for log correlation.
static NEXT_CONN_ID: AtomicU64 = AtomicU64::new(1);

/// One live upstream subscription handle, wrapped per owning module so the
/// per-client registry can hold them uniformly and tear them down on `unsub`
/// (the four modules' generated `SubscriptionHandle` types are distinct).
enum UpHandle {
    Regions(bindings::shard::SubscriptionHandle),
    Cards(bindings::shard::SubscriptionHandle),
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
    /// dedup key (`"<table>\u{1f}<query SQL>"`) → its live upstream + sharing
    /// sids. The table prefix keeps same-SQL/different-DB subs (`cards` vs
    /// `tile_cards`) on separate upstream handles.
    queries: HashMap<String, QueryEntry>,
    /// sid → the dedup key it subscribed (reverse lookup for `unsub`).
    sid_query: HashMap<u32, String>,
    /// tables whose row callbacks are already wired on this connection.
    wired: HashSet<&'static str>,
    /// sid → the world `macro_zone` it OBSERVES (only for `cards WHERE macro_zone
    /// = Z` subs), so `unsub`/disconnect can decrement the zone's observer count.
    observed: HashMap<u32, u64>,
    /// This connection's stable observer identity (the conn id) — what the per-zone
    /// observer count counts distinct of. Connection-, not player-, identity: the
    /// signal is "is another *connection* watching this zone?", which is exactly
    /// what gates move-sync, and it doesn't depend on the (racy) gate session.
    obs_id: u32,
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
    upgrade.on_upgrade(move |socket| run(socket, pool, id).instrument(info_span!("conn", id)))
}

async fn run(socket: WebSocket, pool: Arc<Pool>, conn_id: u64) {
    info!("client connected");
    let (mut sink, mut stream) = socket.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();
    // Register this connection for gate-initiated broadcasts (e.g. the
    // `content_changed` push after a runtime `add_content`).
    pool.register_client(tx.clone());

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
                        // Binary frame: GateMsg is postcard-encoded bytes.
                        if sink.send(Message::Binary(m.into())).await.is_err() {
                            break;
                        }
                        idle.reset(); // traffic flowed → push the keepalive out
                    }
                    None => break, // channel closed → teardown
                },
                _ = idle.tick() => {
                    let frame = GateMsg::Time { server_micros: now_micros() }.to_bytes();
                    if sink.send(Message::Binary(frame.into())).await.is_err() {
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
    let mut registry = SubRegistry { obs_id: conn_id as u32, ..Default::default() };

    // (Clock sync is handled by the forwarder: `call_ok`/`call_err` piggyback for
    // active clients, plus the idle `Time` keepalive — no separate task.)

    // Outstanding async-call promises (`request_zone`/`ensure_region` awaiting their
    // `available`/region in this connection's regions mirror). Resolved on a poll
    // tick interleaved with inbound traffic.
    let mut promises = crate::promise::Promises::new();
    let mut promise_poll = tokio::time::interval(PROMISE_POLL);
    promise_poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            biased; // serve inbound traffic before resolving promises
            frame = stream.next() => {
                let Some(frame) = frame else { break };
                let msg = match frame {
                    Ok(m) => m,
                    Err(err) => {
                        warn!(%err, "receive error");
                        break;
                    }
                };
                match msg {
                    // Binary frame: `ClientMsg` is postcard-encoded.
                    Message::Binary(bytes) => match postcard::from_bytes::<ClientMsg>(&bytes) {
                        Ok(cmsg) => {
                            handle(
                                &pool,
                                upstream_regions.as_ref(),
                                upstream_cards.as_ref(),
                                upstream_chat.as_ref(),
                                upstream_players.as_ref(),
                                &session,
                                &mut registry,
                                &mut promises,
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
                                .to_bytes(),
                            );
                        }
                    },
                    Message::Close(_) => break,
                    Message::Text(_) | Message::Ping(_) | Message::Pong(_) => {}
                }
            }
            _ = promise_poll.tick() => {
                if !promises.is_empty() {
                    let view = RegionsView(upstream_regions.as_deref());
                    for frame in promises.poll(std::time::Instant::now(), &view) {
                        let _ = tx.send(frame);
                    }
                }
            }
        }
    }

    // Drop this connection's observer contributions so its zones decrement on
    // disconnect (else a departed observer would inflate the count).
    for (_sid, zone) in std::mem::take(&mut registry.observed) {
        if let Some(count) = pool.unobserve(zone, registry.obs_id) {
            publish_observers(&pool, zone, count);
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
    tx: &UnboundedSender<Vec<u8>>,
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
                    .to_bytes(),
                );
                None
            }
        },
        None => {
            let _ = tx.send(
                GateMsg::Error {
                    error: format!("{what} upstream unavailable"),
                }
                .to_bytes(),
            );
            None
        }
    }
}

async fn handle(
    pool: &Arc<Pool>,
    upstream_regions: Option<&Arc<bindings::shard::DbConnection>>,
    upstream_cards: Option<&Arc<bindings::shard::DbConnection>>,
    upstream_chat: Option<&Arc<bindings::chat::DbConnection>>,
    upstream_players: Option<&Arc<bindings::players::DbConnection>>,
    session: &tokio::sync::Mutex<Option<u32>>,
    registry: &mut SubRegistry,
    promises: &mut crate::promise::Promises,
    tx: &UnboundedSender<Vec<u8>>,
    msg: ClientMsg,
) {
    match msg {
        ClientMsg::Sub { sid, table, filter } => {
            // Observer counting: a `cards WHERE macro_zone = Z` sub means this
            // connection now observes zone Z. Keyed on the conn id; push the new
            // distinct-observer count if it changed.
            if let Some(zone) = zone_of_card_sub(&table, filter.as_deref()) {
                registry.observed.insert(sid, zone);
                if let Some(count) = pool.observe(zone, registry.obs_id) {
                    publish_observers(pool, zone, count);
                }
            }
            subscribe(
                upstream_regions,
                upstream_cards,
                upstream_chat,
                upstream_players,
                registry,
                tx,
                sid,
                &table,
                filter.as_deref(),
            )
        }
        ClientMsg::Unsub { sid } => {
            // Drop this sid; the upstream tears down when its last sharer leaves.
            if let Some(zone) = registry.observed.remove(&sid) {
                if let Some(count) = pool.unobserve(zone, registry.obs_id) {
                    publish_observers(pool, zone, count);
                }
            }
            registry.remove_sid(sid);
        }
        ClientMsg::Call { cid, client_time_ms, call } => {
            // The typed call → (reducer name, JSON args) — exactly the shape the
            // relay/intercept path below has always consumed (P2 keeps it
            // Value-based; P4 swaps this seam for typed SDK calls).
            let (reducer, args) = call.to_args(client_time_ms);
            // `propose_action` is no longer a relay — the gate validates the
            // recipe across shards and applies it via narrow reducer calls.
            if reducer == "propose_action" {
                crate::propose::handle(pool, tx, cid, args).await;
            } else if reducer == "claim_or_login" {
                // Login establishes the gate-owned session (WS → player_id).
                login_relay(pool, upstream_players, session, tx, cid, args).await;
            } else if reducer == "add_content" {
                // Runtime content authoring: gate-validated + hot-swapped, gated
                // on the session player's `content-author` capability.
                add_content(pool, upstream_players, session, tx, cid, args).await;
            } else if reducer == "modify_content" {
                modify_content(pool, upstream_players, session, tx, cid, args).await;
            } else if reducer == "modify_locale" {
                modify_locale(pool, upstream_players, session, tx, cid, args).await;
            } else if reducer == "modify_visuals" {
                modify_visuals(pool, upstream_players, session, tx, cid, args).await;
            } else if reducer == "upload_master" {
                // Art authoring: write an edited master texture channel to the
                // texture R2 bucket. Same content-author gate as add/modify.
                upload_master(pool, upstream_players, session, tx, cid, args).await;
            } else {
                relay_call(
                    pool,
                    upstream_cards,
                    upstream_regions,
                    upstream_chat,
                    upstream_players,
                    session,
                    promises,
                    tx,
                    cid,
                    reducer,
                    args,
                )
                .await;
            }
        }
    }
}

// ---- reads: subscribe a shard table, fan rows back -------------------

/// Convert a generated SDK binding row → the shared typed wire row
/// ([`RowData`]). One impl per relayed table's row type; the gate ships these
/// postcard-encoded (native ints, no camelCase / number-stringify — the client
/// core is Rust). Field copies are explicit so a binding-codegen reorder can't
/// silently corrupt the positional wire.
use resonantdust_protocol::rows::{CardRow, ChatRow, PlayerRow, RegionRow, RowData, ZoneRow};

trait ToRowData {
    fn to_row_data(&self) -> RowData;
}

impl ToRowData for crate::bindings::shard::card_type::Card {
    fn to_row_data(&self) -> RowData {
        RowData::Card(CardRow {
            valid_at: self.valid_at,
            card_id: self.card_id,
            macro_zone: self.macro_zone,
            micro_location: self.micro_location,
            owner_id: self.owner_id,
            packed_definition: self.packed_definition,
            flags: self.flags,
            flags_bk: self.flags_bk,
            stock: self.stock,
        })
    }
}

impl ToRowData for crate::bindings::shard::zone_type::Zone {
    fn to_row_data(&self) -> RowData {
        RowData::Zone(ZoneRow {
            valid_at: self.valid_at,
            zone_id: self.zone_id,
            macro_zone: self.macro_zone,
            packed_definition: self.packed_definition,
            owner_id: self.owner_id,
            tiles: [
                self.t_0, self.t_1, self.t_2, self.t_3, self.t_4, self.t_5, self.t_6, self.t_7,
                self.t_8, self.t_9, self.t_10, self.t_11, self.t_12,
            ],
        })
    }
}

impl ToRowData for crate::bindings::shard::region_type::Region {
    fn to_row_data(&self) -> RowData {
        RowData::Region(RegionRow {
            macro_region: self.macro_region,
            zone_presence: self.zone_presence,
            zone_available: self.zone_available,
            distance: self.distance,
        })
    }
}

impl ToRowData for crate::bindings::players::player_type::Player {
    fn to_row_data(&self) -> RowData {
        RowData::Player(PlayerRow {
            player_id: self.player_id,
            name: self.name.clone(),
        })
    }
}

impl ToRowData for crate::bindings::chat::chat_message_type::ChatMessage {
    fn to_row_data(&self) -> RowData {
        RowData::Chat(ChatRow {
            sent_at: self.sent_at,
            sender_player_id: self.sender_player_id,
            sender_name: self.sender_name.clone(),
            body: self.body.clone(),
        })
    }
}

/// Build a `Row` frame (postcard bytes). `sid` is a sentinel `0` — the client
/// routes rows by the [`RowData`] variant, not sid.
fn row_frame(op: RowOp, row: RowData) -> Vec<u8> {
    GateMsg::Row { sid: 0, op, row }.to_bytes()
}

/// Register insert/update/delete callbacks for one table that push a
/// [`GateMsg::Row`] (the table is the [`RowData`] variant). Registered **once per
/// (connection, table)**: the SDK fires these for every row in the connection's
/// whole subscription set, so one set covers all of a table's queries and a row
/// event fans back exactly one `Row`. The row type must `impl ToRowData`. (The
/// `$name` literal is kept for call-site symmetry with `route!`; the wire no
/// longer carries it.)
macro_rules! relay_table {
    ($conn:expr, $tx:expr, $access:path, $accessor:ident, $name:literal) => {{
        use $access;
        let conn = &$conn;
        let tx_ins = $tx.clone();
        conn.db().$accessor().on_insert(move |_ctx, row| {
            let _ = tx_ins.send(row_frame(RowOp::Insert, row.to_row_data()));
        });
        let tx_upd = $tx.clone();
        conn.db().$accessor().on_update(move |_ctx, _old, new| {
            let _ = tx_upd.send(row_frame(RowOp::Update, new.to_row_data()));
        });
        let tx_del = $tx.clone();
        conn.db().$accessor().on_delete(move |_ctx, row| {
            let _ = tx_del.send(row_frame(RowOp::Delete, row.to_row_data()));
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
                let _ = tx_applied.send(GateMsg::Applied { sid }.to_bytes());
            })
            .on_error(move |_ctx, err| {
                let _ = tx_err.send(
                    GateMsg::Error {
                        error: format!("sub {sid}: {err}"),
                    }
                    .to_bytes(),
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
        // Dedup key = client-facing table name + the query SQL. The name matters
        // because the SAME SQL can target different upstream DBs: `cards` (the
        // cards shard) and `tile_cards` (the regions DB's own `cards` table) both
        // produce `SELECT * FROM cards WHERE macro_zone = …`. Keying on the SQL
        // alone collapsed them onto one upstream, so promoted tile-cards never
        // got subscribed/relayed. The name disambiguates the two.
        let key = format!("{}\u{1f}{}", $name, $query);
        match $opt {
            Some(conn) => {
                if let Some(entry) = $reg.queries.get_mut(&key) {
                    // Dedup hit: share the live upstream. Ack synthetically since
                    // no new `on_applied` will fire for this sid.
                    entry.sids.insert($sid);
                    $reg.sid_query.insert($sid, key);
                    let _ = $tx.send(GateMsg::Applied { sid: $sid }.to_bytes());
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
                        key.clone(),
                        QueryEntry {
                            handle: $variant(handle),
                            sids,
                        },
                    );
                    $reg.sid_query.insert($sid, key);
                }
            }
            None => {
                let _ = $tx.send(
                    GateMsg::Error {
                        error: format!("{} upstream unavailable", $what),
                    }
                    .to_bytes(),
                );
            }
        }
    }};
}

/// The world `macro_zone` a card-subscription covers, if its filter is exactly
/// `macro_zone = <N>` (the client's per-zone card sub). Drives observer counting;
/// other card subs (`owner_id = …` rosters/inventory) don't count as world
/// observers and return `None`.
fn zone_of_card_sub(table: &str, filter: Option<&str>) -> Option<u64> {
    if table != "cards" {
        return None;
    }
    filter?
        .trim()
        .strip_prefix("macro_zone")?
        .trim_start()
        .strip_prefix('=')?
        .trim()
        .parse::<u64>()
        .ok()
}

/// Broadcast a zone's new observer count to every connected client (each filters
/// to the zones it cares about). A small, infrequent frame (observer counts only
/// change on anchor-tier sub/unsub), so an unscoped broadcast is fine for now.
fn publish_observers(pool: &Arc<Pool>, zone: u64, observers: u32) {
    pool.broadcast(GateMsg::zone_observers(zone, observers));
}

/// Fan a client subscription to the upstream that owns the table. `zones` and
/// `regions` live in the `regions` module's database; `cards`/`souls`/
/// `soul_privates` are still in the `shard` monolith. The client is oblivious —
/// it subscribes by table name and the gate picks the backing connection.
fn subscribe(
    regions: Option<&Arc<bindings::shard::DbConnection>>,
    cards: Option<&Arc<bindings::shard::DbConnection>>,
    chat: Option<&Arc<bindings::chat::DbConnection>>,
    players: Option<&Arc<bindings::players::DbConnection>>,
    registry: &mut SubRegistry,
    tx: &UnboundedSender<Vec<u8>>,
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
        "zones" => route!(registry, regions, "regions", bindings::shard::zones_table::ZonesTableAccess, zones, "zones", tx, sid, query, UpHandle::Regions),
        "regions" => route!(registry, regions, "regions", bindings::shard::regions_table::RegionsTableAccess, regions, "regions", tx, sid, query, UpHandle::Regions),
        // regions-DB tile-cards (the regions module's own `cards` table) → the
        // `regions` upstream, surfaced to the client as `tile_cards`.
        "tile_cards" => route!(registry, regions, "regions", bindings::shard::cards_table::CardsTableAccess, cards, "tile_cards", tx, sid, query, UpHandle::Regions),
        // card-owned tables → the per-client `cards` upstream
        "cards" => route!(registry, cards, "cards", bindings::shard::cards_table::CardsTableAccess, cards, "cards", tx, sid, query, UpHandle::Cards),
        // chat-owned tables → the per-client `chat` upstream
        "chat_messages" => route!(registry, chat, "chat", bindings::chat::chat_messages_table::ChatMessagesTableAccess, chat_messages, "chat_messages", tx, sid, query, UpHandle::Chat),
        // players auth-DB tables → the per-client `players` upstream
        "players" => route!(registry, players, "players", bindings::players::players_table::PlayersTableAccess, players, "players", tx, sid, query, UpHandle::Players),
        // souls / soul_privates / player_profiles dropped — no client subscribes
        // to them, and they'd need RowData variants the wire doesn't define. A
        // stray request hits the `other` arm below (an explicit unsupported-table
        // error), which is correct.
        other => {
            let _ = tx.send(
                GateMsg::Error {
                    error: format!("unsupported table {other:?}"),
                }
                .to_bytes(),
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
    tx: &UnboundedSender<Vec<u8>>,
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
    match crate::connections::http_client().post(&url).json(&args).send().await {
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

// ---- runtime content authoring (add_content) -------------------------

/// Permission-bit mirror of the `players` module (`PLAYER_FLAG_PERMS_SHIFT` /
/// `PERM_CONTENT_AUTHOR`). The capability byte is `Player.flags` bits 8..=15;
/// bit 0 of it is content-author. Kept as a local copy because the gate doesn't
/// depend on the `players` module crate — keep the two in sync.
const PLAYER_PERMS_SHIFT: u32 = 8;
const PERM_CONTENT_AUTHOR: u8 = 1 << 0;

/// Latest-version `flags` for `player_id` off the players upstream cache, or
/// `None` if no row is visible yet. Rows are valid-time versioned; within one
/// `player_id` the raw `valid_at` orders by time, so the max is the live row.
fn player_flags(
    upstream_players: Option<&Arc<bindings::players::DbConnection>>,
    player_id: u32,
) -> Option<u32> {
    use bindings::players::players_table::PlayersTableAccess;
    let conn = upstream_players?;
    conn.db()
        .players()
        .iter()
        .filter(|p| p.player_id == player_id)
        .max_by_key(|p| p.valid_at)
        .map(|p| p.flags)
}

/// Handle `add_content`: authorize, then validate + hot-swap a NEW `.rd` source.
async fn add_content(
    pool: &Arc<Pool>,
    upstream_players: Option<&Arc<bindings::players::DbConnection>>,
    session: &tokio::sync::Mutex<Option<u32>>,
    tx: &UnboundedSender<Vec<u8>>,
    cid: u32,
    args: serde_json::Value,
) {
    let result = async {
        reject_if_peer(pool)?;
        require_content_author(upstream_players, session).await?;
        let name = arg_str(&args, "name")?;
        let text = arg_str(&args, "text")?;
        pool.add_content(name, text).await
    }
    .await;
    reply_content(pool, tx, cid, "add_content", result);
}

/// Handle `modify_content`: authorize, then append a new version of an existing
/// card `lineage` (the gate assigns the version) + hot-swap.
async fn modify_content(
    pool: &Arc<Pool>,
    upstream_players: Option<&Arc<bindings::players::DbConnection>>,
    session: &tokio::sync::Mutex<Option<u32>>,
    tx: &UnboundedSender<Vec<u8>>,
    cid: u32,
    args: serde_json::Value,
) {
    let result = async {
        reject_if_peer(pool)?;
        require_content_author(upstream_players, session).await?;
        let lineage = arg_str(&args, "lineage")?;
        let text = arg_str(&args, "text")?;
        pool.modify_content(lineage, text).await
    }
    .await;
    reply_content(pool, tx, cid, "modify_content", result);
}

/// Handle `modify_locale`: authorize, then replace a locale `domain`'s JSON
/// (validate + hot-swap + persist). Broadcasts `content_changed` like the `.rd`
/// author path, so clients reload the new strings.
async fn modify_locale(
    pool: &Arc<Pool>,
    upstream_players: Option<&Arc<bindings::players::DbConnection>>,
    session: &tokio::sync::Mutex<Option<u32>>,
    tx: &UnboundedSender<Vec<u8>>,
    cid: u32,
    args: serde_json::Value,
) {
    let result = async {
        reject_if_peer(pool)?;
        require_content_author(upstream_players, session).await?;
        let domain = arg_str(&args, "domain")?;
        let json = arg_str(&args, "json")?;
        pool.modify_locale(domain, json).await
    }
    .await;
    reply_content(pool, tx, cid, "modify_locale", result);
}

/// Handle `modify_visuals`: authorize, then replace a visuals source `name`
/// (`visuals/…`) with `text` (validate + hot-swap + persist). Broadcasts
/// `content_changed` like the `.rd`/locale author paths, so clients reload.
async fn modify_visuals(
    pool: &Arc<Pool>,
    upstream_players: Option<&Arc<bindings::players::DbConnection>>,
    session: &tokio::sync::Mutex<Option<u32>>,
    tx: &UnboundedSender<Vec<u8>>,
    cid: u32,
    args: serde_json::Value,
) {
    let result = async {
        reject_if_peer(pool)?;
        require_content_author(upstream_players, session).await?;
        let name = arg_str(&args, "name")?;
        let text = arg_str(&args, "text")?;
        pool.modify_visuals(name, text).await
    }
    .await;
    reply_content(pool, tx, cid, "modify_visuals", result);
}

/// Handle `upload_master`: authorize, then write an edited master texture channel
/// (`aspect`/`faction`/`variant`/`channel` + base64 PNG `data`) to the texture R2
/// bucket. Same content-author gate as `add_content`; no content hot-swap (masters
/// are the LOD source, not the rendered corpus), so it just replies ok/err.
async fn upload_master(
    pool: &Arc<Pool>,
    upstream_players: Option<&Arc<bindings::players::DbConnection>>,
    session: &tokio::sync::Mutex<Option<u32>>,
    tx: &UnboundedSender<Vec<u8>>,
    cid: u32,
    args: serde_json::Value,
) {
    use base64::Engine;
    let result = async {
        reject_if_peer(pool)?;
        require_content_author(upstream_players, session).await?;
        let aspect = arg_str(&args, "aspect")?;
        let faction = arg_str(&args, "faction")?;
        let variant = arg_str(&args, "variant")?;
        let channel = arg_str(&args, "channel")?;
        let data_b64 = arg_str(&args, "data")?;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(data_b64.as_bytes())
            .map_err(|e| format!("decode base64 data: {e}"))?;
        pool.upload_master(&aspect, &faction, &variant, &channel, bytes).await
    }
    .await;
    let reply = match result {
        Ok(key) => {
            info!(cid, %key, "master texture uploaded");
            GateMsg::call_ok(cid)
        }
        Err(error) => {
            warn!(cid, %error, "master upload rejected");
            GateMsg::call_err(cid, error)
        }
    };
    let _ = tx.send(reply);
}

/// Reject authoring on a **peer** gate. Only the content authority owns the
/// canonical `.rd` files; peers mirror it in memory and would have nowhere to
/// persist (and would be overwritten on the next poll). Clients author against
/// the authority; peers receive the change via the poll → `content_changed` push.
fn reject_if_peer(pool: &Arc<Pool>) -> Result<(), String> {
    match pool.content_authority() {
        Some(url) => Err(format!(
            "this gate is a content peer; author against the authority ({url})"
        )),
        None => Ok(()),
    }
}

/// Authorize the session player against the `content-author` capability.
async fn require_content_author(
    upstream_players: Option<&Arc<bindings::players::DbConnection>>,
    session: &tokio::sync::Mutex<Option<u32>>,
) -> Result<(), String> {
    let player_id = (*session.lock().await).ok_or_else(|| "not logged in".to_string())?;
    let flags = player_flags(upstream_players, player_id)
        .ok_or_else(|| format!("no player row for {player_id}"))?;
    if ((flags >> PLAYER_PERMS_SHIFT) & 0xFF) as u8 & PERM_CONTENT_AUTHOR == 0 {
        return Err("not authorized (requires content-author)".to_string());
    }
    Ok(())
}

/// Required string arg `key` from a call's `args`.
fn arg_str(args: &serde_json::Value, key: &str) -> Result<String, String> {
    args.get(key)
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .ok_or_else(|| format!("missing or non-string `{key}`"))
}

/// Reply CallOk + broadcast `content_changed` on success; CallErr otherwise.
fn reply_content(
    pool: &Arc<Pool>,
    tx: &UnboundedSender<Vec<u8>>,
    cid: u32,
    op: &str,
    result: Result<String, String>,
) {
    let reply = match result {
        Ok(version) => {
            info!(cid, op, %version, "content op applied");
            pool.broadcast(GateMsg::content_changed(version));
            GateMsg::call_ok(cid)
        }
        Err(error) => {
            warn!(cid, op, %error, "content op rejected");
            GateMsg::call_err(cid, error)
        }
    };
    let _ = tx.send(reply);
}

// ---- writes: relay a reducer call to its owning database -------------

/// How long to await a reducer's `_then` completion before giving up (the SDK
/// event never arrived). Matches the old HTTP relay's effective ceiling.
const REDUCER_CALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// `create_card` via the generated SDK binding (BSATN) — P4. Reads the gate-
/// injected args `Value` field-by-field into the typed reducer call, then
/// **awaits** the reducer's completion before returning — preserving the gate's
/// per-connection serialization (the old HTTP `/call` relay blocked the message
/// loop until the reducer committed; downstream ops rely on that ordering). The
/// `_then` callback (fired on the connection's background loop) builds the
/// CallOk/CallErr reply at commit time; a oneshot hands it back here to send.
async fn sdk_create_card(
    cards: Option<&Arc<bindings::shard::DbConnection>>,
    tx: &UnboundedSender<Vec<u8>>,
    cid: u32,
    args: &serde_json::Value,
) {
    use bindings::shard::create_card; // the reducer extension trait
    let Some(conn) = cards else {
        let _ = tx.send(GateMsg::call_err(
            cid,
            "create_card: cards upstream not connected".to_string(),
        ));
        return;
    };
    let u = |k: &str| args.get(k).and_then(serde_json::Value::as_u64).unwrap_or(0);
    let (done_tx, done_rx) = tokio::sync::oneshot::channel::<Vec<u8>>();
    let dispatch = conn.reducers.create_card_then(
        u("client_time_ms"),
        u("owner_id") as u32,
        u("surface") as u8,
        u("packed_definition") as u16,
        u("stock") as u32,
        u("macro_zone"),
        u("q") as u8,
        u("r") as u8,
        u("distance") as u16,
        move |_ctx, res| {
            let reply = match res {
                Ok(Ok(())) => GateMsg::call_ok(cid),
                Ok(Err(e)) => GateMsg::call_err(cid, e),
                Err(internal) => GateMsg::call_err(cid, format!("create_card: {internal}")),
            };
            let _ = done_tx.send(reply);
        },
    );
    if let Err(e) = dispatch {
        let _ = tx.send(GateMsg::call_err(cid, format!("create_card dispatch: {e}")));
        return;
    }
    let reply = match tokio::time::timeout(REDUCER_CALL_TIMEOUT, done_rx).await {
        Ok(Ok(reply)) => reply,
        Ok(Err(_)) => GateMsg::call_err(cid, "create_card: completion callback dropped".to_string()),
        Err(_) => GateMsg::call_err(cid, "create_card: reducer timed out".to_string()),
    };
    let _ = tx.send(reply);
}

async fn relay_call(
    pool: &Arc<Pool>,
    cards: Option<&Arc<bindings::shard::DbConnection>>,
    _regions: Option<&Arc<bindings::shard::DbConnection>>,
    _chat: Option<&Arc<bindings::chat::DbConnection>>,
    _players: Option<&Arc<bindings::players::DbConnection>>,
    session: &tokio::sync::Mutex<Option<u32>>,
    promises: &mut crate::promise::Promises,
    tx: &UnboundedSender<Vec<u8>>,
    cid: u32,
    reducer: &str,
    args: serde_json::Value,
) {
    // The gate INJECTS the session player_id into the player reducers (it owns
    // the session, so the client never supplies it). `set_last_login` requires
    // an active session; reject if not logged in.
    let mut args = args;
    // request_zone: the gate owns worldgen now — compute the zone's tile bytes
    // here from the DSL bundle and inject them, so the regions reducer just
    // stores them (no DSL on the server). Non-world surfaces get an empty Vec,
    // leaving the reducer's own rect/disk seeding path.
    if reducer == "request_zone" {
        if let Some(obj) = args.as_object_mut() {
            let macro_zone = obj
                .get("macro_zone")
                .or_else(|| obj.get("macroZone"))
                .and_then(serde_json::Value::as_u64);
            if let Some(mz) = macro_zone {
                let tiles = crate::worldgen::tiles_for_zone(&pool.content(), mz);
                obj.insert("tiles".to_string(), serde_json::json!(tiles));
            }
        }
    }
    // ensure_region: the region's disk `distance` is owner-derived (a container's
    // `inventory` aspect) — the shard can't see cards, so the gate resolves it
    // once here (u16::MAX for the world) and injects it. The shard then bounds the
    // whole region (presence + tile mask) by this single value.
    if reducer == "ensure_region" {
        if let Some(obj) = args.as_object_mut() {
            let macro_zone = obj
                .get("macro_zone")
                .or_else(|| obj.get("macroZone"))
                .and_then(serde_json::Value::as_u64);
            if let Some(mz) = macro_zone {
                let distance = crate::gather::region_distance(pool, mz).await;
                obj.insert("distance".to_string(), serde_json::json!(distance));
            }
        }
    }
    // create_card: the single generic card-mint primitive. Resolve the content
    // name (`card_key`, e.g. "player_soul" / "human" / "corpus") → packed def
    // gate-side; the (content-agnostic) cards reducer takes `packed_definition`.
    if reducer == "create_card" {
        if let Some(obj) = args.as_object_mut() {
            if let Some(key) = obj.get("card_key").and_then(|v| v.as_str()).map(String::from) {
                let bundle = pool.content();
                let packed = bundle.packed_def(&key).unwrap_or(0);
                // Seed the new card's per-instance stock u32 from its `@define`
                // stock defaults (the content-agnostic shard can't derive these).
                let stock = resonantdust_dsl::bridge::stock_default_u32(&bundle, &key);
                obj.remove("card_key");
                obj.insert("packed_definition".to_string(), serde_json::json!(packed));
                obj.insert("stock".to_string(), serde_json::json!(stock));
            }
            // Disk radius of the destination container, so default placement
            // (`first_free_cell`) only picks cells that exist in the region disk.
            let owner = obj.get("owner_id").and_then(serde_json::Value::as_u64).unwrap_or(0) as u32;
            let surface = obj.get("surface").and_then(serde_json::Value::as_u64).unwrap_or(0) as u8;
            let mz = resonantdust_codec::packed::pack_macro_zone_full(owner, surface, 0, 0);
            let distance = crate::gather::region_distance(pool, mz).await;
            obj.insert("distance".to_string(), serde_json::json!(distance));
        }
    }
    // request_blueprint: the gate computes the builder cap (the soul def's folded
    // `builder` aspect) and injects it; the reducer compares it to the soul's
    // live `active_blueprints`. Cap is the player_soul's builder aspect — blueprint
    // requests are for the player's soul (the only blueprint-requesting soul def).
    if reducer == "request_blueprint" {
        if let Some(obj) = args.as_object_mut() {
            let bundle = pool.content();
            let cap = crate::content::def_aspect_total(&bundle, "player_soul", "builder");
            obj.insert("max_active".to_string(), serde_json::json!(cap.max(0) as i32));
            // Resolve the blueprint id → its `<blueprint>.card` ref → packed def,
            // so the module spawns the right card without a blueprint registry.
            let packed = obj
                .get("blueprint_id")
                .and_then(|v| v.as_u64())
                .and_then(|id| bundle.blueprint_name(id as u16))
                .and_then(|name| bundle.blueprint_card(name))
                .and_then(|card| bundle.packed_def(&card))
                .unwrap_or(0);
            obj.insert("blueprint_packed_def".to_string(), serde_json::json!(packed));
        }
    }
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
    // move_soul: the gate owns the CONTENT-derived timing. Re-derive `arrival_ms`
    // from the soul's `speed` (`soul_def`) and the `from`/`dest` tile `cost`s
    // (worldgen), validate hex-adjacency, and OVERRIDE the client's `arrival_ms`.
    // The shard separately verifies `soul_def` + the soul's real cell at
    // `depart_ms` == `from`, so a spoofed input just gets the move rejected there.
    if reducer == "move_soul" {
        let bundle = pool.content();
        let g = |k: &str| args.get(k).and_then(serde_json::Value::as_i64);
        let soul_def = args.get("soul_def").and_then(serde_json::Value::as_u64).unwrap_or(0) as u16;
        let (from_q, from_r) = (g("from_q").unwrap_or(0) as i32, g("from_r").unwrap_or(0) as i32);
        let depart_ms = args.get("depart_ms").and_then(serde_json::Value::as_u64).unwrap_or(0);
        let dest_macro = args
            .get("dest")
            .and_then(|d| d.get("macro_zone"))
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        let dest_micro = args
            .get("dest")
            .and_then(|d| d.get("micro_location"))
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0) as u32;
        let (dq, dr) = {
            use resonantdust_codec::packed::{unpack_macro_zone, unpack_micro_loose, world_tile};
            let (zq, zr) = unpack_macro_zone(dest_macro);
            let (lq, lr, _, _) = unpack_micro_loose(dest_micro);
            (world_tile(zq, lq), world_tile(zr, lr))
        };
        let reject = |msg: String| {
            let _ = tx.send(GateMsg::call_err(cid, format!("move_soul: {msg}")));
        };
        let adjacent =
            matches!((dq - from_q, dr - from_r), (1, 0) | (-1, 0) | (0, 1) | (0, -1) | (1, -1) | (-1, 1));
        let speed = resonantdust_dsl::defs::aspect_value(&bundle, soul_def, "speed").unwrap_or(0);
        match (
            adjacent,
            speed,
            crate::worldgen::tile_cost_at(&bundle, from_q, from_r),
            crate::worldgen::tile_cost_at(&bundle, dq, dr),
        ) {
            (false, ..) => return reject(format!("dest ({dq},{dr}) not adjacent to from ({from_q},{from_r})")),
            (_, s, ..) if s <= 0 => return reject(format!("def {soul_def} has no speed (not a mover)")),
            (_, _, None, _) | (_, _, _, None) => {
                return reject("no tile cost for from/dest cell".to_string())
            }
            (true, speed, Some(cf), Some(cd)) => {
                let travel = (((cf + cd) * 1000) / (2 * speed)).max(1) as u64;
                if let Some(obj) = args.as_object_mut() {
                    obj.insert("arrival_ms".to_string(), serde_json::json!(depart_ms + travel));
                }
            }
        }
    }
    // ── P4: SDK-bound reducers (BSATN) — converted one at a time; the rest fall
    // through to the HTTP `/call` relay below. ──
    if reducer == "create_card" {
        sdk_create_card(cards, tx, cid, &args).await;
        return;
    }
    // Route the reducer to the database that owns it. The relay is anonymous —
    // gate-called reducers trust their args (auth is the gate's job), so
    // `ctx.sender` is immaterial and no bearer token is needed. `propose_action`
    // and `claim_or_login` never reach here (intercepted in `handle`).
    let db = match reducer {
        "request_zone" | "ensure_region" => pool.regions_db(),
        "create_card" | "place_card" | "move_cards" | "request_blueprint" | "move_soul" => {
            pool.cards_db()
        }
        "send_chat_message" => pool.chat_db(),
        "set_last_login" | "create_player" => pool.players_db(),
        other => {
            // Nothing routes to the retired `shard` monolith anymore.
            let _ = tx.send(GateMsg::call_err(
                cid,
                format!("gate: no backing database for reducer {other:?}"),
            ));
            return;
        }
    };
    let now = std::time::Instant::now();
    let url = format!("{}/v1/database/{}/call/{}", pool.server_uri(), db, reducer);

    // Worldgen reducers (`request_zone`/`ensure_region`) don't know their true
    // outcome at HTTP-reply time — it's observed on the regions subscription. They
    // ACCEPT a promise that resolves on the server-truth bit (so a silent no-op
    // times out → the client retries instead of latching). DEDUP: if an identical
    // request (same reducer + macro_zone) is already in flight or just resolved,
    // skip the redundant shard POST and co-wait on the same result.
    if let Some(resolve) = worldgen_promise(reducer, &args) {
        let key = worldgen_key(reducer, &args);
        if promises.is_duplicate(&key, now) {
            let _ = tx.send(promises.accept(cid, key, PROMISE_TIMEOUT, now, resolve));
            return;
        }
        let reply = match crate::connections::http_client().post(&url).json(&args).send().await {
            Ok(resp) if resp.status().is_success() => {
                let _ = tx.send(promises.accept(cid, key, PROMISE_TIMEOUT, now, resolve));
                return;
            }
            Ok(resp) => {
                let code = resp.status();
                let body = resp.text().await.unwrap_or_default();
                GateMsg::call_err(cid, format!("{code}: {body}"))
            }
            Err(err) => GateMsg::call_err(cid, err.to_string()),
        };
        let _ = tx.send(reply);
        return;
    }

    // Everything else: the HTTP result IS the outcome.
    let reply = match crate::connections::http_client().post(&url).json(&args).send().await {
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

/// Dedup identity for a worldgen call — `"<reducer>:<macro_zone>"`.
fn worldgen_key(reducer: &str, args: &serde_json::Value) -> String {
    let mz = args
        .get("macro_zone")
        .or_else(|| args.get("macroZone"))
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    format!("{reducer}:{mz}")
}

/// Build the promise resolver for a worldgen reducer, or `None` for reducers whose
/// outcome is fully known at HTTP-reply time (those reply `call_ok` directly).
///
/// - `request_zone`: resolves `Ok` once the zone's `available` bit flips in its
///   region. A silent no-op (no region, zone already cleared) never flips it, so
///   the promise times out and the client re-requests.
/// - `ensure_region`: resolves `Ok` once the region exists in the gate's mirror.
fn worldgen_promise(
    reducer: &str,
    args: &serde_json::Value,
) -> Option<Box<dyn FnMut(&dyn crate::promise::RegionView) -> crate::promise::Resolution + Send>> {
    use crate::promise::Resolution;
    use resonantdust_codec::packed::region_of_zone;
    let mz = args
        .get("macro_zone")
        .or_else(|| args.get("macroZone"))
        .and_then(serde_json::Value::as_u64)?;
    let (macro_region, bit) = region_of_zone(mz);
    match reducer {
        "request_zone" => {
            let mask = 1u64 << bit;
            Some(Box::new(move |view: &dyn crate::promise::RegionView| {
                match view.region_bits(macro_region) {
                    Some((_, avail)) if avail & mask != 0 => Resolution::Ok,
                    _ => Resolution::Pending,
                }
            }))
        }
        "ensure_region" => Some(Box::new(move |view: &dyn crate::promise::RegionView| {
            match view.region_bits(macro_region) {
                Some(_) => Resolution::Ok,
                None => Resolution::Pending,
            }
        })),
        _ => None,
    }
}
