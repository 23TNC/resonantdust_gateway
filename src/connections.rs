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
use std::sync::{Arc, Mutex};

use tracing::{error, info, warn};

use crate::bindings;
use crate::config::GateConfig;

/// Stamp a `connect_<module>(uri, db_name) -> Option<Arc<DbConnection>>`
/// using the module's concrete builder + the standard lifecycle logging.
macro_rules! connector {
    ($fn:ident, $module:ident) => {
        fn $fn(uri: &str, db_name: &str) -> Option<Arc<bindings::$module::DbConnection>> {
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
                    move |_ctx, err| error!(db = %n, %err, "upstream connect error")
                })
                .on_disconnect({
                    let n = name.clone();
                    move |_ctx, err| match err {
                        Some(err) => warn!(db = %n, %err, "upstream disconnected"),
                        None => info!(db = %n, "upstream disconnected"),
                    }
                })
                .build();

            match built {
                Ok(conn) => {
                    conn.run_threaded();
                    Some(Arc::new(conn))
                }
                Err(err) => {
                    error!(db = %name, %err, "failed to build connection");
                    None
                }
            }
        }
    };
}

connector!(connect_cards, cards);
connector!(connect_regions, regions);
connector!(connect_regionindex, regionindex);

/// Lazy pool of upstream connections, keyed by shard id where applicable.
/// Cheap to share (`Arc<Pool>`); getters connect-on-miss and cache.
pub struct Pool {
    cfg: GateConfig,
    cards: Mutex<HashMap<u16, Arc<bindings::cards::DbConnection>>>,
    regions: Mutex<HashMap<u16, Arc<bindings::regions::DbConnection>>>,
    regions_index: Mutex<Option<Arc<bindings::regionindex::DbConnection>>>,
}

impl Pool {
    pub fn new(cfg: GateConfig) -> Self {
        Self {
            cfg,
            cards: Mutex::new(HashMap::new()),
            regions: Mutex::new(HashMap::new()),
            regions_index: Mutex::new(None),
        }
    }

    pub fn config(&self) -> &GateConfig {
        &self.cfg
    }

    /// Connection to the `cards` shard `shard`, establishing it on first use.
    pub fn cards(&self, shard: u16) -> Option<Arc<bindings::cards::DbConnection>> {
        let mut map = self.cards.lock().unwrap();
        if let Some(conn) = map.get(&shard) {
            return Some(conn.clone());
        }
        let conn = connect_cards(&self.cfg.uri, &self.cfg.cards_db(shard))?;
        map.insert(shard, conn.clone());
        Some(conn)
    }

    /// Connection to the `regions` shard `shard`, establishing it on first use.
    pub fn regions(&self, shard: u16) -> Option<Arc<bindings::regions::DbConnection>> {
        let mut map = self.regions.lock().unwrap();
        if let Some(conn) = map.get(&shard) {
            return Some(conn.clone());
        }
        let conn = connect_regions(&self.cfg.uri, &self.cfg.regions_db(shard))?;
        map.insert(shard, conn.clone());
        Some(conn)
    }

    /// Connection to the single `regionindex` DB (region → regions shard).
    pub fn regions_index(&self) -> Option<Arc<bindings::regionindex::DbConnection>> {
        let mut slot = self.regions_index.lock().unwrap();
        if let Some(conn) = slot.as_ref() {
            return Some(conn.clone());
        }
        let conn = connect_regionindex(&self.cfg.uri, &self.cfg.regions_index_db())?;
        *slot = Some(conn.clone());
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
        Arc<bindings::regions::DbConnection>,
        tokio::sync::oneshot::Receiver<()>,
    )> {
        use bindings::regions::DbConnection;
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

    /// Per-client upstream to the `cards` shard, mirroring [`fresh_regions`].
    /// `cards`/`souls`/`soul_privates` reads route here; each client needs its
    /// OWN cards upstream for the same set-semantics reason. Shard 0 today
    /// (owner sharding is future work).
    pub fn fresh_cards(
        &self,
    ) -> Option<(
        Arc<bindings::cards::DbConnection>,
        tokio::sync::oneshot::Receiver<()>,
    )> {
        use bindings::cards::DbConnection;
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
