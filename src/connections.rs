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
connector!(connect_players, players);
connector!(connect_regionindex, regionindex);

/// Connect to the `shard` monolith (the relay target). Unlike the macro
/// connectors it captures the connection token into `token_slot` — the gate
/// reuses it to authenticate write relays to shard's HTTP `/call` endpoint.
fn connect_shard(
    uri: &str,
    db_name: &str,
    token_slot: Arc<Mutex<Option<String>>>,
) -> Option<Arc<bindings::shard::DbConnection>> {
    use bindings::shard::DbConnection;
    let name = db_name.to_string();
    let built = DbConnection::builder()
        .with_uri(uri)
        .with_database_name(db_name)
        .on_connect({
            let n = name.clone();
            move |_ctx, identity, token| {
                info!(db = %n, %identity, "upstream connected");
                *token_slot.lock().unwrap() = Some(token.to_string());
            }
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

/// Lazy pool of upstream connections, keyed by shard id where applicable.
/// Cheap to share (`Arc<Pool>`); getters connect-on-miss and cache.
pub struct Pool {
    cfg: GateConfig,
    cards: Mutex<HashMap<u16, Arc<bindings::cards::DbConnection>>>,
    regions: Mutex<HashMap<u16, Arc<bindings::regions::DbConnection>>>,
    players: Mutex<Option<Arc<bindings::players::DbConnection>>>,
    regions_index: Mutex<Option<Arc<bindings::regionindex::DbConnection>>>,
    shard: Mutex<Option<Arc<bindings::shard::DbConnection>>>,
    /// Shard's connection token, captured on connect; used to auth write
    /// relays to shard's HTTP `/call` endpoint.
    shard_token: Arc<Mutex<Option<String>>>,
}

impl Pool {
    pub fn new(cfg: GateConfig) -> Self {
        Self {
            cfg,
            cards: Mutex::new(HashMap::new()),
            regions: Mutex::new(HashMap::new()),
            players: Mutex::new(None),
            regions_index: Mutex::new(None),
            shard: Mutex::new(None),
            shard_token: Arc::new(Mutex::new(None)),
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

    /// Connection to the single `players` auth/index DB.
    pub fn players(&self) -> Option<Arc<bindings::players::DbConnection>> {
        let mut slot = self.players.lock().unwrap();
        if let Some(conn) = slot.as_ref() {
            return Some(conn.clone());
        }
        let conn = connect_players(&self.cfg.uri, &self.cfg.players_db())?;
        *slot = Some(conn.clone());
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

    /// Connection to the `shard` monolith (the relay target).
    pub fn shard(&self) -> Option<Arc<bindings::shard::DbConnection>> {
        let mut slot = self.shard.lock().unwrap();
        if let Some(conn) = slot.as_ref() {
            return Some(conn.clone());
        }
        let conn = connect_shard(
            &self.cfg.uri,
            &self.cfg.shard_db(),
            self.shard_token.clone(),
        )?;
        *slot = Some(conn.clone());
        Some(conn)
    }

    /// The base HTTP URL of the SpacetimeDB server (for write relays).
    pub fn server_uri(&self) -> &str {
        &self.cfg.uri
    }

    /// The `shard` database name.
    pub fn shard_db(&self) -> String {
        self.cfg.shard_db()
    }

    /// Shard's captured connection token, if connected yet.
    pub fn shard_token(&self) -> Option<String> {
        self.shard_token.lock().unwrap().clone()
    }
}
