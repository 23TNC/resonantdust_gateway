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
    clients: Mutex<Vec<tokio::sync::mpsc::UnboundedSender<Vec<u8>>>>,
    /// Per-zone distinct-player OBSERVER counts, derived from card-subscriptions
    /// (`cards WHERE macro_zone = Z`). `zone -> (player -> refcount)`; a player
    /// observes a zone while ≥1 of its card-subs covers it, so `observers =
    /// players with refcount > 0`. The gate pushes changes to the `zone_observers`
    /// table; clients gate move-sync on it (commit-based position, Phase 2).
    observers: Mutex<HashMap<u64, HashMap<u32, u32>>>,
    /// The S3 store for an **authoring authority** that keeps its canonical corpus
    /// in R2 — `Some` when R2 write creds are configured. Authored sources are
    /// written here (so they're durable + propagate); `None` → persist to disk.
    r2_store: Option<Arc<crate::s3::R2Store>>,
    /// The S3 store for master TEXTURE uploads — a SEPARATE bucket from content
    /// (masters live in e.g. `resonantdust-assets`). `Some` when `TEXTURE_R2_*`
    /// (or the shared `R2_*`) creds are configured; `None` → texture authoring is
    /// rejected (there's no disk fallback — masters belong in the asset bucket).
    texture_store: Option<Arc<crate::s3::R2Store>>,
    /// The newest build-version snapshot (`versions.json`) the build host has
    /// pushed via `PUT /versions`. Served back as `latest` on `GET /versions` so a
    /// client can compare its baked bundle AND this gate's baked snapshot against
    /// the freshest deployed build — a source of truth independent of any one
    /// running binary. Seeded from R2 (`meta/versions.json`) at startup when a
    /// content store is configured, so a freshly restarted gate still has it.
    latest_versions: RwLock<Option<String>>,
}

/// R2 key (under the content prefix) for the pushed build-version ledger.
const LATEST_VERSIONS_KEY: &str = "meta/versions.json";

impl Pool {
    pub fn new(
        cfg: GateConfig,
        content: crate::content::LoadedContent,
        r2_store: Option<Arc<crate::s3::R2Store>>,
        texture_store: Option<Arc<crate::s3::R2Store>>,
    ) -> Self {
        Self {
            cfg,
            content: RwLock::new(Arc::new(content)),
            cards: Mutex::new(HashMap::new()),
            regions: Mutex::new(HashMap::new()),
            regions_index: Mutex::new(None),
            clients: Mutex::new(Vec::new()),
            observers: Mutex::new(HashMap::new()),
            r2_store,
            texture_store,
            latest_versions: RwLock::new(None),
        }
    }

    /// Seed the `latest` build-version snapshot from R2 at startup (best-effort).
    /// A no-op when no content store is configured (local disk/HTTP envs keep
    /// `latest` empty until the build host pushes one via `PUT /versions`).
    pub async fn seed_latest_versions(&self) {
        let Some(store) = &self.r2_store else { return };
        match store.get(LATEST_VERSIONS_KEY).await {
            Ok(json) => {
                tracing::info!("versions: seeded `latest` from R2 ({} bytes)", json.len());
                *self.latest_versions.write().unwrap() = Some(json);
            }
            // Absent on first deploy (404) — not an error; the next push creates it.
            Err(err) => tracing::info!(%err, "versions: no `latest` in R2 yet"),
        }
    }

    /// The newest pushed build-version snapshot, or `None` if none seen yet.
    pub fn latest_versions(&self) -> Option<String> {
        self.latest_versions.read().unwrap().clone()
    }

    /// Record the build host's freshest `versions.json` (`PUT /versions`): keep it
    /// in memory for `GET /versions` and, when a content store is configured,
    /// persist it to R2 so a restarted gate re-seeds it. Persist failure is fatal
    /// to the request (the in-memory copy is only set on success, so a retry is
    /// clean) — returns the byte count stored on success.
    pub async fn set_latest_versions(&self, json: String) -> Result<usize, String> {
        let n = json.len();
        if let Some(store) = &self.r2_store {
            store.put(LATEST_VERSIONS_KEY, json.as_bytes()).await?;
        }
        *self.latest_versions.write().unwrap() = Some(json);
        Ok(n)
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
    pub fn register_client(&self, tx: tokio::sync::mpsc::UnboundedSender<Vec<u8>>) {
        self.clients.lock().unwrap().push(tx);
    }

    /// Send `msg` (an encoded `GateMsg` frame) to every live client, pruning any
    /// whose channel has closed.
    pub fn broadcast(&self, msg: Vec<u8>) {
        self.clients.lock().unwrap().retain(|tx| tx.send(msg.clone()).is_ok());
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
    /// merged corpus, persisting it (R2 or disk), and hot-swapping it on success.
    /// Returns the new version fingerprint (hex). On `Err` the live content is
    /// untouched (validation + persist run against a candidate before the swap).
    /// `name` must not already be present — an in-place change is `modify_content`.
    pub async fn add_content(&self, name: String, text: String) -> Result<String, String> {
        // Validate against a snapshot WITHOUT holding the write lock (load can be
        // non-trivial; readers must not block on it).
        let current = self.content.read().unwrap().clone();
        let next = current.with_added_source(name, text)?;
        self.persist_and_swap(next).await
    }

    /// Append a new **version** of an existing card `lineage` (the gate assigns
    /// the version number), validating + persisting + hot-swapping on success.
    /// Same lock discipline as [`add_content`]. Returns the new version (hex).
    pub async fn modify_content(&self, lineage: String, text: String) -> Result<String, String> {
        let current = self.content.read().unwrap().clone();
        let next = current.with_modified_source(lineage, text)?;
        self.persist_and_swap(next).await
    }

    /// Replace a locale `domain`'s JSON, validating (rebuild) + persisting (R2 or
    /// disk) + hot-swapping. Same lock discipline as [`modify_content`]; locales
    /// aren't versioned (a domain is one JSON blob). Returns the new version (hex).
    pub async fn modify_locale(&self, domain: String, json: String) -> Result<String, String> {
        let current = self.content.read().unwrap().clone();
        let next = current.with_modified_locale(domain.clone(), json.clone())?;
        match &self.r2_store {
            Some(store) => crate::content::persist_locale_s3(store, &domain, &json).await?,
            None => crate::content::persist_locale(&domain, &json)?,
        }
        let version_hex = format!("{:016x}", next.version);
        *self.content.write().unwrap() = Arc::new(next);
        Ok(version_hex)
    }

    /// Replace a visuals source `name` (`visuals/…`) with `text`, validating
    /// (rebuild) + persisting (R2 or disk) + hot-swapping. Same lock discipline as
    /// [`modify_locale`]; visuals are overwritten in place (not versioned). Returns
    /// the new version (hex).
    pub async fn modify_visuals(&self, name: String, text: String) -> Result<String, String> {
        let current = self.content.read().unwrap().clone();
        let next = current.with_modified_visuals(name.clone(), text.clone())?;
        match &self.r2_store {
            Some(store) => crate::content::persist_visuals_s3(store, &name, &text).await?,
            None => crate::content::persist_visuals(&name, &text)?,
        }
        let version_hex = format!("{:016x}", next.version);
        *self.content.write().unwrap() = Arc::new(next);
        Ok(version_hex)
    }

    /// Persist the newly-appended runtime source — to the R2 store if this gate
    /// authors to a bucket (the unified store), else to local disk — then hot-swap
    /// the validated candidate in. Persist BEFORE the swap so a write failure
    /// leaves live content untouched. No lock is held across the `await`.
    async fn persist_and_swap(
        &self,
        next: crate::content::LoadedContent,
    ) -> Result<String, String> {
        if let Some((name, text)) = next.sources.last() {
            match &self.r2_store {
                Some(store) => crate::content::persist_source_s3(store, name, text).await?,
                None => crate::content::persist_source(name, text)?,
            }
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

    /// Write an edited master texture channel to the texture R2 bucket at
    /// `textures/master/<aspect>/<faction>/<variant>.<channel>.png` — the in-app
    /// art editor's "save master" path, mirroring `add_content` for DSL. Validates
    /// the channel + path segments (no traversal) + a PNG magic sniff, then PUTs.
    /// Returns the written key. Masters are the LOD source, not what the game
    /// renders, so there's no broadcast here — `bin/art lod` regenerates the LODs.
    pub async fn upload_master(
        &self,
        aspect: &str,
        faction: &str,
        variant: &str,
        channel: &str,
        bytes: Vec<u8>,
    ) -> Result<String, String> {
        const CHANNELS: [&str; 4] = ["diffuse", "albedo", "normal", "emissive"];
        if !CHANNELS.contains(&channel) {
            return Err(format!("bad channel {channel:?} (diffuse|albedo|normal|emissive)"));
        }
        // Path-segment safety: alnum/_/-/. only, and no `..` — the segments land
        // in an object key, so reject anything that could escape the prefix.
        for (label, seg) in [("aspect", aspect), ("faction", faction), ("variant", variant)] {
            let ok = !seg.is_empty()
                && !seg.contains("..")
                && seg.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'));
            if !ok {
                return Err(format!("bad {label} {seg:?} (alnum / _ - . only, no `..`)"));
            }
        }
        if bytes.len() < 8 || bytes[..8] != [0x89, b'P', b'N', b'G', b'\r', b'\n', 0x1a, b'\n'] {
            return Err("payload is not a PNG".to_string());
        }
        let store = self.texture_store.as_ref().ok_or_else(|| {
            "texture authoring not configured (set TEXTURE_R2_BUCKET + R2 creds)".to_string()
        })?;
        let key = format!("textures/master/{aspect}/{faction}/{variant}.{channel}.png");
        store.put(&key, &bytes).await?;
        Ok(key)
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
