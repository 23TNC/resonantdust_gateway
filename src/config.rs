//! Static deployment config: the SpacetimeDB server URI + environment, plus
//! the naming convention that turns a shard id into a concrete database name.
//!
//! This answers *what a shard's database is called*. *Which* shard a given
//! player/region lives on is answered separately by the index DBs (`players`
//! player→data_shard, `regions-index` region→shard); a card names its own
//! shard in the top 12 bits of its id (see `routing`).

/// SpacetimeDB server the shards live on. On the `resonantdust` network the
/// spacetime container is reachable as `start`; override with `GATE_STDB_URI`.
const DEFAULT_STDB_URI: &str = "http://start:3000";
const DEFAULT_ENV: &str = "dev";

/// Resolved gateway configuration.
pub struct GateConfig {
    pub uri: String,
    pub env: String,
    /// Content-authority URL (`GATE_CONTENT_AUTHORITY`). `None` → this gate IS
    /// the authority: it reads `.rd` files from disk, serves `/content`, and
    /// accepts `add`/`modify`. `Some(url)` → this gate is a **peer**: it fetches
    /// `/content` from `url` at startup, polls `url/content-version`, and rejects
    /// authoring (clients author against the authority). Content coordination is
    /// HTTP-to-authority; SpacetimeDB stays game-state-only.
    pub content_authority: Option<String>,
}

impl GateConfig {
    pub fn from_env() -> Self {
        // Environment selection unifies on `RD_ENV` (shared with `bin/st`), with
        // `GATE_ENV` kept as a per-tool override. Precedence: GATE_ENV > RD_ENV >
        // `dev`. `bin/gate` resolves this and passes `GATE_ENV` explicitly, so
        // the `RD_ENV` fallback only matters for a direct binary run.
        let env = std::env::var("GATE_ENV")
            .or_else(|_| std::env::var("RD_ENV"))
            .unwrap_or_else(|_| DEFAULT_ENV.to_string());
        let content_authority = std::env::var("GATE_CONTENT_AUTHORITY")
            .ok()
            .filter(|s| !s.is_empty());
        Self {
            uri: std::env::var("GATE_STDB_URI").unwrap_or_else(|_| DEFAULT_STDB_URI.to_string()),
            env,
            content_authority,
        }
    }

    /// `cards` shard database name. The numeric suffix is the `data_shard`
    /// (== `card_shard_of(card_id)`); each deployed instance's `DATA_SHARD`
    /// constant must match its suffix.
    pub fn cards_db(&self, shard: u16) -> String {
        format!("resonantdust-{}-cards-{}", self.env, shard)
    }

    /// `regions` shard database name (suffix == its `data_shard`).
    pub fn regions_db(&self, shard: u16) -> String {
        format!("resonantdust-{}-regions-{}", self.env, shard)
    }

    /// The `regionindex` database mapping a region → its `regions` shard.
    /// Single instance today (shard 0).
    pub fn regions_index_db(&self) -> String {
        format!("resonantdust-{}-regionindex-0", self.env)
    }

    /// The single `chat` database (world chat is one global feed; no sharding).
    pub fn chat_db(&self) -> String {
        format!("resonantdust-{}-chat-0", self.env)
    }

    /// The single `players` auth DB (player record + profile). One instance
    /// today; the future per-gate-index lives on a canonical control-plane.
    pub fn players_db(&self) -> String {
        format!("resonantdust-{}-players-0", self.env)
    }
}
