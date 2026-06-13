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
mod propose;
mod routing;
mod validation;
mod worldgen;
mod ws;

use std::sync::Arc;

use axum::extract::State;
use axum::response::IntoResponse;
use axum::{routing::get, Router};
use tokio::net::TcpListener;
use tokio::signal;

/// Address the gate listens on. `0.0.0.0` so the published container port
/// reaches it; override with `GATE_LISTEN`.
const DEFAULT_LISTEN: &str = "0.0.0.0:8473";

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
    //     serves `/content`, accepts add/modify. It reads that corpus either from
    //     local disk (default) or, if `CONTENT_BASE_URL` is set, from a public
    //     object store (R2) via a manifest — so content can be updated by
    //     uploading to the bucket.
    //   • peer (`GATE_CONTENT_AUTHORITY=<url>`) — fetches `/content` from the
    //     authority at startup and polls `<url>/content-version` for changes,
    //     mirroring the corpus in memory. Authoring is rejected on peers.
    // Propagation is HTTP authority→peer→client; SpacetimeDB stays game-only.
    let authority = cfg.content_authority.clone();
    let content_base_url = cfg.content_base_url.clone();
    let content = match &authority {
        None => match &content_base_url {
            Some(base) => {
                tracing::info!(%base, "content: authority (R2-backed)");
                load_r2_content(base).await
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
    let pool = Arc::new(connections::Pool::new(cfg, content));

    // A peer keeps its in-memory corpus in sync by polling the authority.
    if authority.is_some() {
        spawn_content_poll(pool.clone());
    }

    let app = Router::new()
        .route("/health", get(health))
        .route("/ws", get(ws::handler))
        // Server-authoritative content: clients load the same `.rd` corpus +
        // locales the gate validates against, so they agree by construction.
        .route("/content", get(serve_content))
        .route("/content-version", get(serve_content_version))
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

/// Load + build the base corpus from R2/HTTP — an **authority** gate's
/// `CONTENT_BASE_URL` startup load. Retries on a fixed backoff so a transient
/// network blip (or a bucket still being populated) doesn't kill the gate; exits
/// the process if the load keeps failing (an authority with no corpus can't serve
/// clients). A *load* failure (bad/missing manifest, 404, parse error) is fatal
/// just like the disk path's panic — broken content must not reach clients.
async fn load_r2_content(base: &str) -> content::LoadedContent {
    for attempt in 1..=15u32 {
        match content::load_content_r2(base).await {
            Ok(c) => {
                tracing::info!(
                    %base,
                    files = c.sources.len(),
                    locale_domains = c.locales.len(),
                    version = %format!("{:016x}", c.version),
                    "content: loaded from R2"
                );
                return c;
            }
            Err(e) => {
                tracing::warn!(%base, attempt, error = %e, "content: R2 load failed, retrying")
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
    tracing::error!(%base, "content: R2 load failed after retries; exiting");
    std::process::exit(1);
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
