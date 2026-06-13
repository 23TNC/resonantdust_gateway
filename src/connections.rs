//! Upstream connection pool.
//!
//! Each generated module (`cards`, `regions`, `players`, …) has its OWN
//! `DbConnection` type with distinct table/reducer APIs, so a single uniform
//! handle map is impossible. Instead the `connector!` macro stamps one
//! connect fn per module (using that module's concrete builder), and [`Pool`]
//! caches the live connections — lazily, by shard id — behind `Arc`s the gate
//! can clone and use from any thread.
//!
//! The SDK runs each connection's message loop on its own thread (callbacks
//! fire there); the `Arc<DbConnection>` we hold keeps it reachable for
//! subscriptions and reducer calls.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};

use tracing::{error, info, warn};

use crate::bindings;
use crate::config::GateConfig;

/// The shared HTTP client for all upstream `/call` + subscribe requests.
/// `reqwest::Client` owns a connection pool and is internally `Arc`, so one
/// process-wide instance (cheaply cloned at call sites) reuses connections
/// instead of building and discarding a pool per request.
pub fn http_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(reqwest::Client::new)
}

/// Stamp a `connect_<module>(uri, db_name, alive) -> Option<Arc<DbConnection>>`
/// using the module's concrete builder + the standard lifecycle logging.
///
/// `alive` is a shared flag the connection clears the instant it becomes
/// unusable — a disconnect (the DB was redeployed/wiped/restarted) or a failed
/// connect. The pool checks it on every getter and rebuilds a dead connection,
/// so a replaced upstream DB self-heals instead of stalling every gather on a
/// cached-but-dead `Arc` forever (the SDK does NOT auto-reconnect).
macro_rules! connector {
    ($fn:ident, $module:ident) => {
        fn $fn(
            uri: &str,
            db_name: &str,
            alive: Arc<AtomicBool>,
        ) -> Option<Arc<bindings::$module::DbConnection>> {
            use bindings::$module::DbConnection;
            let name = db_name.to_string();
            let built = DbConnection::builder()
                .with_uri(uri)
                .with_database_name(db_name)
                .on_connect({
                    let n = name.clone();
                    move |_ctx, identity, _token| info!(db = %n, %identity, "upstream connected")
                })
                .on_connect_error({
                    let n = name.clone();
                    let alive = alive.clone();
                    move |_ctx, err| {
                        alive.store(false, Ordering::SeqCst);
                        error!(db = %n, %err, "upstream connect error")
                    }
                })
                .on_disconnect({
                    let n = name.clone();
                    let alive = alive.clone();
                    move |_ctx, err| {
                        alive.store(false, Ordering::SeqCst);
                        match err {
                            Some(err) => warn!(db = %n, %err, "upstream disconnected"),
                            None => info!(db = %n, "upstream disconnected"),
                        }
                    }
                })
                .build();

            match built {
                Ok(conn) => {
                    conn.run_threaded();
                    Some(Arc::new(conn))
                }
                Err(err) => {
                    alive.store(false, Ordering::SeqCst);
                    error!(db = %name, %err, "failed to build connection");
                    None
                }
            }
        }
    };
}

// The unified `shard` data module backs both the cards DB and the region DBs,
// so both upstreams use the same `shard` bindings (one schema, two DB names).
connector!(connect_cards, shard);
connector!(connect_regions, shard);
connector!(connect_regionindex, regionindex);

/// Lazy pool of upstream connections, keyed by shard id where applicable.
/// Cheap to share (`Arc<Pool>`); getters connect-on-miss and cache.
pub struct Pool {
    cfg: GateConfig,
    /// The DSL content the gate runs + serves: the [`Bundle`] the recipe
    /// pipeline reads, plus the same corpus pre-serialized for `/content`.
    ///
    /// Behind an `RwLock<Arc<…>>` so `add_content` can hot-swap a validated new
    /// version live. Readers take a cheap `Arc` snapshot ([`Pool::content`]) and
    /// run a whole action against one consistent version even if a swap races.
    content: RwLock<Arc<crate::content::LoadedContent>>,
    // Each pooled connection is cached with an `alive` flag it clears on
    // disconnect/failed-connect; a getter rebuilds the entry when it's dead, so a
    // redeployed/wiped upstream DB self-heals (see `connector!`).
    cards: Mutex<HashMap<u16, (Arc<bindings::shard::DbConnection>, Arc<AtomicBool>)>>,
    regions: Mutex<HashMap<u16, (Arc<bindings::shard::DbConnection>, Arc<AtomicBool>)>>,
    regions_index: Mutex<Option<(Arc<bindings::regionindex::DbConnection>, Arc<AtomicBool>)>>,
    /// Live client WS senders, for gate-initiated broadcasts (e.g. the
    /// `content_changed` push after `add_content`). Dead senders are pruned
    /// lazily on the next broadcast.
    clients: Mutex<Vec<tokio::sync::mpsc::UnboundedSender<String>>>,
    /// Per-zone distinct-player OBSERVER counts, derived from card-subscriptions
    /// (`cards WHERE macro_zone = Z`). `zone -> (player -> refcount)`; a player
    /// observes a zone while ≥1 of its card-subs covers it, so `observers =
    /// players with refcount > 0`. The gate pushes changes to the `zone_observers`
    /// table; clients gate move-sync on it (commit-based position, Phase 2).
    observers: Mutex<HashMap<u64, HashMap<u32, u32>>>,
}

impl Pool {
    pub fn new(cfg: GateConfig, content: crate::content::LoadedContent) -> Self {
        Self {
            cfg,
            content: RwLock::new(Arc::new(content)),
            cards: Mutex::new(HashMap::new()),
            regions: Mutex::new(HashMap::new()),
            regions_index: Mutex::new(None),
            clients: Mutex::new(Vec::new()),
            observers: Mutex::new(HashMap::new()),
        }
    }

    /// Record that `player` has one more card-sub covering `zone`. Returns the new
    /// distinct-observer count IF it changed (the player went 0→1), else `None`.
    pub fn observe(&self, zone: u64, player: u32) -> Option<u32> {
        let mut map = self.observers.lock().unwrap();
        let z = map.entry(zone).or_default();
        let rc = z.entry(player).or_insert(0);
        *rc += 1;
        (*rc == 1).then(|| z.len() as u32)
    }

    /// Drop one of `player`'s card-subs covering `zone`. Returns the new distinct
    /// count IF it changed (the player went 1→0), else `None`.
    pub fn unobserve(&self, zone: u64, player: u32) -> Option<u32> {
        let mut map = self.observers.lock().unwrap();
        let z = map.get_mut(&zone)?;
        let rc = z.get_mut(&player)?;
        *rc -= 1;
        if *rc > 0 {
            return None;
        }
        z.remove(&player);
        let n = z.len() as u32;
        if z.is_empty() {
            map.remove(&zone);
        }
        Some(n)
    }

    /// Register a client's WS sender for gate-initiated broadcasts. Called once
    /// per connection; the sender is pruned on the next broadcast after the
    /// client disconnects (its receiver drops → `send` errors).
    pub fn register_client(&self, tx: tokio::sync::mpsc::UnboundedSender<String>) {
        self.clients.lock().unwrap().push(tx);
    }

    /// Send `msg` to every live client, pruning any whose channel has closed.
    pub fn broadcast(&self, msg: String) {
        self.clients.lock().unwrap().retain(|tx| tx.send(msg.clone()).is_ok());
    }

    pub fn config(&self) -> &GateConfig {
        &self.cfg
    }

    /// A snapshot of the current content bundle (the VM + defs the recipe
    /// pipeline runs). Cheap `Arc` clone; hold it for the duration of an action
    /// so the whole operation sees one consistent content version.
    pub fn content(&self) -> Arc<resonantdust_dsl::loader::Bundle> {
        self.content.read().unwrap().bundle.clone()
    }

    /// The pre-serialized `GET /content` body (the `.rd` corpus + locales + version
    /// the client loads). Cheap clone of an `Arc<str>`.
    pub fn content_json(&self) -> Arc<str> {
        self.content.read().unwrap().payload_json.clone()
    }

    /// The corpus version fingerprint as a hex string (for `GET /content-version`).
    pub fn content_version_hex(&self) -> String {
        format!("{:016x}", self.content.read().unwrap().version)
    }

    /// The corpus version fingerprint as the raw `u64` (for the R2 re-poll's
    /// change check, which compares against a freshly-fetched fingerprint).
    pub fn content_version_num(&self) -> u64 {
        self.content.read().unwrap().version
    }

    /// Add a new `.rd` source `(name, text)` to the live content, validating the
    /// merged corpus and hot-swapping it on success. Returns the new version
    /// fingerprint (hex). On `Err` the live content is untouched (validation
    /// runs against a candidate before the swap). `name` must not already be
    /// present — an in-place change is `modify_content`.
    pub fn add_content(&self, name: String, text: String) -> Result<String, String> {
        // Validate against a snapshot WITHOUT holding the write lock (load can be
        // non-trivial; readers must not block on it).
        let current = self.content.read().unwrap().clone();
        let next = current.with_added_source(name, text)?;
        self.persist_and_swap(next)
    }

    /// Append a new **version** of an existing card `lineage` (the gate assigns
    /// the version number), validating + hot-swapping on success. Same lock
    /// discipline as [`add_content`]. Returns the new version fingerprint (hex).
    pub fn modify_content(&self, lineage: String, text: String) -> Result<String, String> {
        let current = self.content.read().unwrap().clone();
        let next = current.with_modified_source(lineage, text)?;
        self.persist_and_swap(next)
    }

    /// Persist the newly-appended runtime source to disk (durable across
    /// restart), then hot-swap the validated candidate in. Persist BEFORE the
    /// swap so a write failure leaves live content untouched.
    fn persist_and_swap(&self, next: crate::content::LoadedContent) -> Result<String, String> {
        if let Some((name, text)) = next.sources.last() {
            crate::content::persist_source(name, text)?;
        }
        let version_hex = format!("{:016x}", next.version);
        *self.content.write().unwrap() = Arc::new(next);
        Ok(version_hex)
    }

    /// Hot-swap in content fetched from the authority — a **peer** gate's update
    /// path. Unlike [`add_content`]/[`modify_content`] this does NOT touch disk:
    /// the peer holds no canonical files, it mirrors the authority's corpus in
    /// memory. The poll task calls this after a `/content-version` change, then
    /// broadcasts `content_changed` to its own clients.
    pub fn swap_content(&self, next: crate::content::LoadedContent) {
        *self.content.write().unwrap() = Arc::new(next);
    }

    /// The content-authority URL if this gate is a **peer** (`Some`), or `None`
    /// if this gate IS the authority. Used to gate authoring (peers reject
    /// `add`/`modify`) and to drive the poll task.
    pub fn content_authority(&self) -> Option<&str> {
        self.cfg.content_authority.as_deref()
    }

    /// Connection to the `cards` shard `shard`, establishing it on first use and
    /// re-establishing it if the cached one has died (DB redeploy/wipe).
    pub fn cards(&self, shard: u16) -> Option<Arc<bindings::shard::DbConnection>> {
        let mut map = self.cards.lock().unwrap();
        if let Some((conn, alive)) = map.get(&shard) {
            if alive.load(Ordering::SeqCst) {
                return Some(conn.clone());
            }
        }
        let alive = Arc::new(AtomicBool::new(true));
        let conn = connect_cards(&self.cfg.uri, &self.cfg.cards_db(shard), alive.clone())?;
        map.insert(shard, (conn.clone(), alive));
        Some(conn)
    }

    /// Connection to the `regions` shard `shard`, establishing it on first use and
    /// re-establishing it if the cached one has died (DB redeploy/wipe).
    pub fn regions(&self, shard: u16) -> Option<Arc<bindings::shard::DbConnection>> {
        let mut map = self.regions.lock().unwrap();
        if let Some((conn, alive)) = map.get(&shard) {
            if alive.load(Ordering::SeqCst) {
                return Some(conn.clone());
            }
        }
        let alive = Arc::new(AtomicBool::new(true));
        let conn = connect_regions(&self.cfg.uri, &self.cfg.regions_db(shard), alive.clone())?;
        map.insert(shard, (conn.clone(), alive));
        Some(conn)
    }

    /// Connection to the single `regionindex` DB (region → regions shard),
    /// re-establishing it if the cached one has died (DB redeploy/wipe).
    pub fn regions_index(&self) -> Option<Arc<bindings::regionindex::DbConnection>> {
        let mut slot = self.regions_index.lock().unwrap();
        if let Some((conn, alive)) = slot.as_ref() {
            if alive.load(Ordering::SeqCst) {
                return Some(conn.clone());
            }
        }
        let alive = Arc::new(AtomicBool::new(true));
        let conn = connect_regionindex(&self.cfg.uri, &self.cfg.regions_index_db(), alive.clone())?;
        *slot = Some((conn.clone(), alive));
        Some(conn)
    }

    /// The base HTTP URL of the SpacetimeDB server (for write relays).
    pub fn server_uri(&self) -> &str {
        &self.cfg.uri
    }

    /// Create a NEW, uncached per-client upstream to the `regions` shard, plus a
    /// oneshot that fires once it has connected. Each client WS gets its OWN
    /// upstream so subscriptions and the SDK row cache are isolated — a *shared*
    /// upstream silently drops rows for a second subscriber, because SpacetimeDB
    /// subscriptions are set-semantics: `on_insert` fires only when a row first
    /// enters the cache, so a second subscription to already-cached rows
    /// delivers nothing. Await the receiver before subscribing (subscribing
    /// before the connection is open loses the initial rows) and `disconnect()`
    /// on teardown. Shard 0 today (positional region sharding is future work).
    pub fn fresh_regions(
        &self,
    ) -> Option<(
        Arc<bindings::shard::DbConnection>,
        tokio::sync::oneshot::Receiver<()>,
    )> {
        use bindings::shard::DbConnection;
        let db_name = self.cfg.regions_db(0);
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let ready_tx = Arc::new(Mutex::new(Some(ready_tx)));

        let built = DbConnection::builder()
            .with_uri(self.cfg.uri.as_str())
            .with_database_name(db_name)
            .on_connect({
                let ready_tx = ready_tx.clone();
                move |_ctx, identity, _token| {
                    info!(%identity, "client regions upstream connected");
                    if let Some(tx) = ready_tx.lock().unwrap().take() {
                        let _ = tx.send(());
                    }
                }
            })
            .on_connect_error(|_ctx, err| error!(%err, "client regions upstream connect error"))
            .on_disconnect(|_ctx, err| match err {
                Some(err) => warn!(%err, "client regions upstream disconnected"),
                None => info!("client regions upstream disconnected"),
            })
            .build();

        match built {
            Ok(conn) => {
                conn.run_threaded();
                Some((Arc::new(conn), ready_rx))
            }
            Err(err) => {
                error!(%err, "failed to build client regions upstream");
                None
            }
        }
    }

    /// `regions` shard database name for reducer-call relays (shard 0 today).
    pub fn regions_db(&self) -> String {
        self.cfg.regions_db(0)
    }

    /// `cards` shard database name for reducer-call relays (shard 0 today).
    pub fn cards_db(&self) -> String {
        self.cfg.cards_db(0)
    }

    /// `chat` database name for reducer-call relays (single global feed).
    pub fn chat_db(&self) -> String {
        self.cfg.chat_db()
    }

    /// `players` auth-DB name for reducer-call relays (single instance today).
    pub fn players_db(&self) -> String {
        self.cfg.players_db()
    }

    /// Per-client upstream to the single `players` auth DB, mirroring
    /// [`fresh_cards`]. The client's `players` / `player_profiles` reads route
    /// here; the gate also reads the player row off this upstream after login to
    /// learn the `player_id` for its session map.
    pub fn fresh_players(
        &self,
    ) -> Option<(
        Arc<bindings::players::DbConnection>,
        tokio::sync::oneshot::Receiver<()>,
    )> {
        use bindings::players::DbConnection;
        let db_name = self.cfg.players_db();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let ready_tx = Arc::new(Mutex::new(Some(ready_tx)));

        let built = DbConnection::builder()
            .with_uri(self.cfg.uri.as_str())
            .with_database_name(db_name)
            .on_connect({
                let ready_tx = ready_tx.clone();
                move |_ctx, identity, _token| {
                    info!(%identity, "client players upstream connected");
                    if let Some(tx) = ready_tx.lock().unwrap().take() {
                        let _ = tx.send(());
                    }
                }
            })
            .on_connect_error(|_ctx, err| error!(%err, "client players upstream connect error"))
            .on_disconnect(|_ctx, err| match err {
                Some(err) => warn!(%err, "client players upstream disconnected"),
                None => info!("client players upstream disconnected"),
            })
            .build();

        match built {
            Ok(conn) => {
                conn.run_threaded();
                Some((Arc::new(conn), ready_rx))
            }
            Err(err) => {
                error!(%err, "failed to build client players upstream");
                None
            }
        }
    }

    /// Per-client upstream to the single `chat` DB, mirroring [`fresh_cards`].
    /// `chat_messages` reads route here; each client needs its OWN chat upstream
    /// for the same set-semantics reason (a shared upstream drops the backlog for
    /// a second subscriber).
    pub fn fresh_chat(
        &self,
    ) -> Option<(
        Arc<bindings::chat::DbConnection>,
        tokio::sync::oneshot::Receiver<()>,
    )> {
        use bindings::chat::DbConnection;
        let db_name = self.cfg.chat_db();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let ready_tx = Arc::new(Mutex::new(Some(ready_tx)));

        let built = DbConnection::builder()
            .with_uri(self.cfg.uri.as_str())
            .with_database_name(db_name)
            .on_connect({
                let ready_tx = ready_tx.clone();
                move |_ctx, identity, _token| {
                    info!(%identity, "client chat upstream connected");
                    if let Some(tx) = ready_tx.lock().unwrap().take() {
                        let _ = tx.send(());
                    }
                }
            })
            .on_connect_error(|_ctx, err| error!(%err, "client chat upstream connect error"))
            .on_disconnect(|_ctx, err| match err {
                Some(err) => warn!(%err, "client chat upstream disconnected"),
                None => info!("client chat upstream disconnected"),
            })
            .build();

        match built {
            Ok(conn) => {
                conn.run_threaded();
                Some((Arc::new(conn), ready_rx))
            }
            Err(err) => {
                error!(%err, "failed to build client chat upstream");
                None
            }
        }
    }

    /// Per-client upstream to the `cards` shard, mirroring [`fresh_regions`].
    /// `cards`/`souls`/`soul_privates` reads route here; each client needs its
    /// OWN cards upstream for the same set-semantics reason. Shard 0 today
    /// (owner sharding is future work).
    pub fn fresh_cards(
        &self,
    ) -> Option<(
        Arc<bindings::shard::DbConnection>,
        tokio::sync::oneshot::Receiver<()>,
    )> {
        use bindings::shard::DbConnection;
        let db_name = self.cfg.cards_db(0);
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let ready_tx = Arc::new(Mutex::new(Some(ready_tx)));

        let built = DbConnection::builder()
            .with_uri(self.cfg.uri.as_str())
            .with_database_name(db_name)
            .on_connect({
                let ready_tx = ready_tx.clone();
                move |_ctx, identity, _token| {
                    info!(%identity, "client cards upstream connected");
                    if let Some(tx) = ready_tx.lock().unwrap().take() {
                        let _ = tx.send(());
                    }
                }
            })
            .on_connect_error(|_ctx, err| error!(%err, "client cards upstream connect error"))
            .on_disconnect(|_ctx, err| match err {
                Some(err) => warn!(%err, "client cards upstream disconnected"),
                None => info!("client cards upstream disconnected"),
            })
            .build();

        match built {
            Ok(conn) => {
                conn.run_threaded();
                Some((Arc::new(conn), ready_rx))
            }
            Err(err) => {
                error!(%err, "failed to build client cards upstream");
                None
            }
        }
    }
}
