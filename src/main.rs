//! resonantdust gateway — the bridge between the client and the SpacetimeDB
//! data shards. It serves an inbound WS listener (`ws`) for clients and holds
//! a lazy pool of upstream connections (`connections`) to the sharded modules,
//! routed via `routing` and named via `config`. Recipe gather/validate/apply
//! land in later workstreams.

mod apply;
mod bindings;
mod config;
mod connections;
mod gather;
mod propose;
mod protocol;
mod routing;
mod validation;
mod ws;

use std::sync::Arc;

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
    let pool = Arc::new(connections::Pool::new(cfg));

    let app = Router::new()
        .route("/health", get(health))
        .route("/ws", get(ws::handler))
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
