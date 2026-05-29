//! resonantdust gateway — the bridge between the client and the SpacetimeDB
//! data shards. Milestone 1 stands up the inbound listener only: a WS endpoint
//! clients connect to, plus a health check. Upstream shard connections, the
//! real client<->gate protocol, and multi-shard merge land in later milestones
//! (see `protocol.rs` / `upstream.rs` when they arrive).

mod bindings;
mod upstream;
mod ws;

use axum::{routing::get, Router};
use tokio::net::TcpListener;
use tokio::signal;

/// Address the gate listens on. `0.0.0.0` so the published container port
/// reaches it; override with `GATE_LISTEN`.
const DEFAULT_LISTEN: &str = "0.0.0.0:8080";

/// SpacetimeDB server the shards live on. On the `resonantdust` network the
/// spacetime container is reachable as `start`; override with `GATE_STDB_URI`.
const DEFAULT_STDB_URI: &str = "http://start:3000";

#[tokio::main]
async fn main() {
    init_tracing();

    let listen = std::env::var("GATE_LISTEN").unwrap_or_else(|_| DEFAULT_LISTEN.to_string());

    // Connect upstream to the shard before serving clients. Held for the
    // process lifetime so the SDK message loop keeps running; a build failure
    // is logged but does not stop the inbound listener.
    let env = std::env::var("GATE_ENV").unwrap_or_else(|_| "dev".to_string());
    let uri = std::env::var("GATE_STDB_URI").unwrap_or_else(|_| DEFAULT_STDB_URI.to_string());
    let _shard = upstream::spawn_shard(uri, format!("resonantdust-{env}-shard"));

    let app = Router::new()
        .route("/health", get(health))
        .route("/ws", get(ws::handler));

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
