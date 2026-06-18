//! resonantdust gateway — the bridge between the client and the SpacetimeDB
//! data shards. It serves an inbound WS listener (`ws`) for clients and holds
//! a lazy pool of upstream connections (`connections`) to the sharded modules,
//! routed via `routing` and named via `config`. Recipe gather/validate/apply
//! land in later workstreams.

mod apply;
mod bindings;
mod config;
mod connections;
mod content;
mod gather;
mod lod;
mod promise;
mod propose;
mod routing;
mod s3;
mod validation;
mod worldgen;
mod ws;

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::{routing::get, Router};
use tokio::net::TcpListener;
use tokio::signal;

/// Address the gate listens on. `0.0.0.0` so the published container port
/// reaches it; override with `GATE_LISTEN`.
const DEFAULT_LISTEN: &str = "0.0.0.0:8473";

/// Component version snapshot, baked in at compile time by `bin/versions` (which
/// `bin/gate build` runs first). Source-closure hashes of every build unit —
/// `{build, generated, components{hash,seq}}`. Served verbatim at `/versions` so
/// a client can detect it's talking to a stale gate/shard. `include_str!` (not a
/// runtime read) because the remote box has no repo checkout; the copy lives in
/// `gateway/src/` so it's reachable past the docker bind-mount boundary.
const VERSIONS_JSON: &str = include_str!("versions.json");

#[tokio::main]
async fn main() {
    init_tracing();

    let listen = std::env::var("GATE_LISTEN").unwrap_or_else(|_| DEFAULT_LISTEN.to_string());

    // Build the lazy upstream pool. Connections to the per-module databases
    // (cards / regions / regionindex) are established on first use, per client
    // for reads and on demand for gather. Nothing connects to the retired
    // `shard` monolith anymore.
    let cfg = config::GateConfig::from_env();
    tracing::info!(uri = %cfg.uri, env = %cfg.env, "gate config");

    // Load the DSL content (the runtime recipe engine + the corpus served to
    // clients) before serving. Topology:
    //   • authority (`GATE_CONTENT_AUTHORITY` unset) — owns the canonical corpus,
    //     serves `/content`, accepts add/modify. It reads that corpus from local
    //     disk (default), a public object store over HTTP (`CONTENT_BASE_URL`), or
    //     — when R2 write creds are set — the S3 API (so authoring writes back to
    //     the same strongly-consistent bucket: the unified store).
    //   • peer (`GATE_CONTENT_AUTHORITY=<url>`) — fetches `/content` from the
    //     authority at startup and polls `<url>/content-version` for changes,
    //     mirroring the corpus in memory. Authoring is rejected on peers.
    // Propagation is HTTP authority→peer→client; SpacetimeDB stays game-only.
    let authority = cfg.content_authority.clone();
    // S3 store for an authoring authority (write creds set). When present it is
    // ALSO the read/poll source — strongly consistent, so the gate's own poll
    // reads back exactly what it authored (no public-CDN revert race).
    let r2_store = s3::R2Store::from_env().map(Arc::new);
    // Master-texture writes target a SEPARATE bucket (the asset bucket), so the
    // art editor's "save master" can persist edited PNGs. Independent of the
    // content store — either can be configured without the other.
    let texture_store = s3::R2Store::textures_from_env().map(Arc::new);
    if texture_store.is_some() {
        tracing::info!("texture authoring: enabled (R2)");
    }
    // The authority's read source: S3 if authoring, else the public HTTP base, else
    // None (disk). `r2_store` is cloned in so it can also serve writes via the Pool.
    let content_src = match (&r2_store, &cfg.content_base_url) {
        (Some(store), _) => Some(content::ContentSrc::S3(store.clone())),
        (None, Some(base)) => Some(content::ContentSrc::Http(base.clone())),
        (None, None) => None,
    };
    let content = match &authority {
        None => match &content_src {
            Some(src) => {
                let kind = match src {
                    content::ContentSrc::S3(_) => "S3 R2",
                    content::ContentSrc::Http(_) => "HTTP R2",
                };
                tracing::info!(kind, "content: authority (object-store-backed)");
                load_src_with_retry(src).await
            }
            None => {
                tracing::info!("content: authority (disk-backed)");
                content::load_content()
            }
        },
        Some(url) => {
            tracing::info!(%url, "content: peer (fetching from authority)");
            fetch_authority_content(url).await
        }
    };
    let pool = Arc::new(connections::Pool::new(cfg, content, r2_store, texture_store));
    // Re-seed the `latest` build-version snapshot from R2 (best-effort) so a
    // freshly restarted gate serves it on `/versions` without waiting for a push.
    pool.seed_latest_versions().await;

    // A peer keeps its in-memory corpus in sync by polling the authority. An
    // object-store-backed authority keeps ITS corpus in sync by polling the store —
    // so an upload (or its own authoring) propagates authority→peer→client with no
    // restart.
    if authority.is_some() {
        spawn_content_poll(pool.clone());
    } else if let Some(src) = content_src {
        spawn_content_poll_src(pool.clone(), src);
    }

    let app = Router::new()
        .route("/health", get(health))
        .route("/ws", get(ws::handler))
        // Server-authoritative content: clients load the same `.rd` corpus +
        // locales the gate validates against, so they agree by construction.
        .route("/content", get(serve_content))
        .route("/content-version", get(serve_content_version))
        // Build fingerprints: baked component hashes + the live content version,
        // so a client can flag a stale deployment per-component. PUT pushes the
        // build host's freshest snapshot (authority-only) so `latest` is an
        // out-of-band truth, independent of any one running binary.
        .route("/versions", get(serve_versions).put(put_versions))
        // On-demand LOD: the client falls back here when its R2-direct fetch
        // 404s. The gate serves the cached LOD or generates it from the master.
        // `{*rest}` captures `<stem>.<channel>.png` (the stem has `/`s).
        .route("/textures/lod/{size}/{*rest}", get(serve_lod))
        .with_state(pool);

    let listener = match TcpListener::bind(&listen).await {
        Ok(l) => l,
        Err(err) => {
            tracing::error!(%listen, %err, "failed to bind listener");
            std::process::exit(1);
        }
    };

    let local = listener
        .local_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| listen.clone());
    tracing::info!(addr = %local, "gate listening");

    if let Err(err) = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
    {
        tracing::error!(%err, "server error");
        std::process::exit(1);
    }

    tracing::info!("gate stopped");
}

/// Liveness probe — `200 OK` with a tiny body. Used by compose healthchecks
/// and manual `curl`.
async fn health() -> &'static str {
    "ok"
}

/// Load + build the base corpus from an object store — an **authority** gate's
/// startup load over a [`content::ContentSrc`] (HTTP or S3). Retries on a fixed
/// backoff so a transient blip (or a bucket still being populated) doesn't kill
/// the gate; exits the process if the load keeps failing (an authority with no
/// corpus can't serve clients). A *load* failure (bad/missing manifest, 404,
/// parse error) is fatal just like the disk path's panic — broken content must
/// not reach clients.
async fn load_src_with_retry(src: &content::ContentSrc) -> content::LoadedContent {
    for attempt in 1..=15u32 {
        match content::load_content_src(src).await {
            Ok(c) => {
                tracing::info!(
                    files = c.sources.len(),
                    locale_domains = c.locales.len(),
                    version = %format!("{:016x}", c.version),
                    "content: loaded from object store"
                );
                return c;
            }
            Err(e) => tracing::warn!(attempt, error = %e, "content: store load failed, retrying"),
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
    tracing::error!("content: store load failed after retries; exiting");
    std::process::exit(1);
}

/// Spawn the object-store re-poll loop — an **object-store-backed authority**'s
/// live-update path. Every `CONTENT_POLL_SECS` (default 10) it re-fetches the
/// corpus; on a fingerprint change it revalidates, hot-swaps the live corpus, and
/// broadcasts `content_changed` to this gate's clients. Peers see the new
/// `/content-version` on their own poll and mirror it. A bad fetch/parse leaves
/// live content untouched (a broken upload never reaches clients). This is the
/// authority analog of [`spawn_content_poll`] (which mirrors an upstream gate).
fn spawn_content_poll_src(pool: Arc<connections::Pool>, src: content::ContentSrc) {
    let secs = std::env::var("CONTENT_POLL_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|&s| s > 0)
        .unwrap_or(10);
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(secs));
        loop {
            tick.tick().await;
            match content::poll_content_src(&src, pool.content_version_num()).await {
                Ok(Some(next)) => {
                    pool.swap_content(next);
                    let version = pool.content_version_hex();
                    tracing::info!(%version, "authority: content updated from store");
                    pool.broadcast(resonantdust_protocol::protocol::GateMsg::content_changed(version));
                }
                Ok(None) => {} // unchanged — the common case
                Err(e) => tracing::warn!(error = %e, "authority: store poll failed"),
            }
        }
    });
}

/// Fetch + build the corpus from the content authority — a **peer**'s startup
/// load. Retries on a fixed backoff so a peer can start before/alongside the
/// authority; exits the process if the authority stays unreachable (a peer with
/// no content can't serve clients).
async fn fetch_authority_content(url: &str) -> content::LoadedContent {
    let endpoint = format!("{}/content", url.trim_end_matches('/'));
    let client = connections::http_client();
    for attempt in 1..=30u32 {
        match client.get(&endpoint).send().await {
            Ok(resp) => match resp.text().await {
                Ok(body) => match content::build_from_payload(&body) {
                    Ok(c) => {
                        tracing::info!(%endpoint, "content: fetched from authority");
                        return c;
                    }
                    Err(e) => tracing::error!(%endpoint, error = %e, "content: bad authority payload"),
                },
                Err(e) => tracing::warn!(%endpoint, error = %e, "content: read body failed"),
            },
            Err(e) => {
                tracing::warn!(%endpoint, attempt, error = %e, "content: authority unreachable, retrying")
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
    tracing::error!(%endpoint, "content: authority unreachable after retries; exiting");
    std::process::exit(1);
}

/// Spawn the peer poll loop. Every few seconds it reads the authority's
/// `/content-version`; on a change it re-fetches `/content`, hot-swaps the live
/// corpus, and broadcasts `content_changed` to this peer's own clients (who then
/// reload from this gate). This is the authority→peer→client propagation path —
/// no content ever touches SpacetimeDB.
fn spawn_content_poll(pool: Arc<connections::Pool>) {
    let Some(base) = pool.content_authority().map(|s| s.trim_end_matches('/').to_string()) else {
        return; // not a peer — nothing to poll
    };
    let ver_url = format!("{base}/content-version");
    let content_url = format!("{base}/content");
    tokio::spawn(async move {
        let client = connections::http_client();
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(3));
        loop {
            tick.tick().await;
            let remote = match client.get(&ver_url).send().await {
                Ok(r) => match r.text().await {
                    Ok(t) => t.trim().to_string(),
                    Err(_) => continue,
                },
                Err(_) => continue, // authority blip — try again next tick
            };
            if remote.is_empty() || remote == pool.content_version_hex() {
                continue;
            }
            let body = match client.get(&content_url).send().await {
                Ok(r) => match r.text().await {
                    Ok(t) => t,
                    Err(e) => {
                        tracing::warn!(error = %e, "peer: read /content failed");
                        continue;
                    }
                },
                Err(e) => {
                    tracing::warn!(error = %e, "peer: fetch /content failed");
                    continue;
                }
            };
            match content::build_from_payload(&body) {
                Ok(next) => {
                    pool.swap_content(next);
                    let version = pool.content_version_hex();
                    tracing::info!(%version, "peer: content updated from authority");
                    pool.broadcast(resonantdust_protocol::protocol::GateMsg::content_changed(version));
                }
                Err(e) => tracing::warn!(error = %e, "peer: bad authority payload"),
            }
        }
    });
}

/// `GET /content` — the full corpus the client loads: `{version, rd, locales}`,
/// pre-serialized at startup. `rd` feeds `new Content(...)`, `locales` feeds
/// `new Locales(...)`. The exact bytes the gate validates against.
async fn serve_content(State(pool): State<Arc<connections::Pool>>) -> impl IntoResponse {
    (
        [
            (axum::http::header::CONTENT_TYPE, "application/json"),
            // The client is served from a different origin (its own dev server /
            // CDN) than the gate, so the content fetch is cross-origin.
            (axum::http::header::ACCESS_CONTROL_ALLOW_ORIGIN, "*"),
        ],
        pool.content_json().to_string(),
    )
}

/// `GET /content-version` — just the corpus fingerprint (hex). Cheap to poll;
/// the client compares it to detect a content change (live reload, later).
async fn serve_content_version(State(pool): State<Arc<connections::Pool>>) -> impl IntoResponse {
    (
        [(axum::http::header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")],
        pool.content_version_hex(),
    )
}

/// `GET /versions` — the gate's build fingerprints. Merges the compile-time
/// `VERSIONS_JSON` snapshot (every component's source-closure hash) with the
/// gate's LIVE content version (`content_live`) — content hot-swaps without a
/// rebuild, so its live fingerprint is the authoritative one, separate from the
/// baked source hash. The client compares `server.components` against its own
/// build-injected snapshot to spot a stale gate/shard.
async fn serve_versions(State(pool): State<Arc<connections::Pool>>) -> impl IntoResponse {
    // `latest` is the build host's freshest pushed snapshot (an out-of-band truth,
    // so it can flag a stale gate too) — verbatim JSON object, or `null` if none
    // has been pushed/seeded yet.
    let latest = pool.latest_versions();
    let body = format!(
        "{{\"server\":{server},\"content_live\":\"{live}\",\"latest\":{latest}}}",
        server = VERSIONS_JSON.trim(),
        live = pool.content_version_hex(),
        latest = latest.as_deref().map(str::trim).unwrap_or("null"),
    );
    (
        [
            (axum::http::header::CONTENT_TYPE, "application/json"),
            (axum::http::header::ACCESS_CONTROL_ALLOW_ORIGIN, "*"),
        ],
        body,
    )
}

/// `PUT /versions` — the build host pushes its freshly built `versions.json` here
/// after a deploy (see `bin/redeploy`). The gate stores it (memory + R2 when a
/// content store is configured) and echoes it back as `latest` on `GET`. Authority
/// only: a peer gate has no canonical store and rejects, mirroring content
/// authoring (clients/build hosts target the authority).
async fn put_versions(
    State(pool): State<Arc<connections::Pool>>,
    body: String,
) -> impl IntoResponse {
    if pool.content_authority().is_some() {
        return (StatusCode::FORBIDDEN, "versions: peer gate rejects push (target the authority)".to_string());
    }
    // Guard against a truncated/garbage push corrupting `latest` for everyone:
    // it must parse as a JSON object before we store it.
    if serde_json::from_str::<serde_json::Value>(&body).map(|v| !v.is_object()).unwrap_or(true) {
        return (StatusCode::BAD_REQUEST, "versions: body must be a JSON object".to_string());
    }
    match pool.set_latest_versions(body).await {
        Ok(n) => (StatusCode::OK, format!("versions: stored {n} bytes")),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, format!("versions: store failed: {err}")),
    }
}

/// Query string for [`serve_lod`]. `v` is the master-version hash the client
/// stamps (`?v=<hash>`) so the gate can detect a stale cached LOD; absent for a
/// legacy client (then any cached object is served).
#[derive(serde::Deserialize)]
struct LodQuery {
    v: Option<String>,
}

/// `GET /textures/lod/{size}/{*rest}` — on-demand LOD. The client uses this as a
/// fallback when its R2-direct fetch misses; the gate serves the cached LOD or
/// generates it from the master (see [`lod::ensure`]). Returns `image/png` with a
/// permissive CORS header (WebGL rejects cross-origin textures without it) and a
/// long cache lifetime (the bytes for a `{size,stem,channel}` are immutable —
/// versioning, when it lands, lives in the path, not in-place mutation).
async fn serve_lod(
    State(pool): State<Arc<connections::Pool>>,
    axum::extract::Path((size, rest)): axum::extract::Path<(u32, String)>,
    axum::extract::Query(q): axum::extract::Query<LodQuery>,
) -> axum::response::Response {
    match lod::ensure(&pool, size, &rest, q.v.as_deref()).await {
        Ok(bytes) => (
            [
                (axum::http::header::CONTENT_TYPE, "image/png"),
                (axum::http::header::ACCESS_CONTROL_ALLOW_ORIGIN, "*"),
                (axum::http::header::CACHE_CONTROL, "public, max-age=31536000, immutable"),
            ],
            bytes,
        )
            .into_response(),
        Err(err) => {
            // A 404 (no master) is normal — log noisier failures only.
            if err.status != StatusCode::NOT_FOUND {
                tracing::warn!(status = %err.status, msg = %err.msg, "lod: ensure failed");
            }
            // CORS on errors too: the client fetches cross-origin, so without this
            // header the browser can't read even a clean 404 — it surfaces as an
            // opaque `TypeError: Failed to fetch` (masterless stems hit this).
            (
                err.status,
                [(axum::http::header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")],
                err.msg,
            )
                .into_response()
        }
    }
}

/// Initialize `tracing` as the single logging path (the Rust analog of the
/// client's `debug.log` discipline — no bare `println!`). Level is controlled
/// by `RUST_LOG`, defaulting to `info`.
fn init_tracing() {
    use tracing_subscriber::{fmt, EnvFilter};

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(filter).init();
}

/// Resolve when either SIGINT (Ctrl-C, as from a foregrounded `gate publish`)
/// or SIGTERM (`gate down` / compose stop) arrives, triggering graceful
/// shutdown of in-flight connections.
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(err) => tracing::error!(%err, "failed to install SIGTERM handler"),
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }

    tracing::info!("shutdown signal received");
}
