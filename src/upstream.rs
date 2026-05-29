//! Upstream SpacetimeDB client connections.
//!
//! Milestone 2: connect the gate to a single shard database as a client,
//! subscribe to one table (`zones`), and log what arrives — proving the
//! upstream half of the bridge pipe. The SDK runs its own message loop on a
//! dedicated thread; row/lifecycle callbacks fire there and log via `tracing`.
//!
//! Still to come: multi-shard fan-out, the merge/PK-dedupe across shards,
//! `macro_zone`→`data_shard` routing, and client-token passthrough (this
//! milestone connects with a fresh anonymous identity).

use spacetimedb_sdk::{DbContext, Table};
use tracing::{error, info, warn};

use crate::bindings::shard::zones_table::ZonesTableAccess;
use crate::bindings::shard::DbConnection;

/// Connect to one shard database and start its message loop on a dedicated
/// thread. Returns the live connection (which the caller must keep alive for
/// the connection to persist), or `None` if the initial build failed — in
/// which case the gate's inbound listener still comes up.
pub fn spawn_shard(uri: String, module: String) -> Option<DbConnection> {
    info!(%uri, %module, "connecting to shard upstream");

    let build = DbConnection::builder()
        .with_uri(uri)
        .with_database_name(module)
        .on_connect(|ctx, identity, _token| {
            info!(%identity, "shard connected");
            subscribe_zones(ctx);
        })
        .on_connect_error(|_ctx, err| error!(%err, "shard connect error"))
        .on_disconnect(|_ctx, err| match err {
            Some(err) => warn!(%err, "shard disconnected with error"),
            None => info!("shard disconnected"),
        })
        .build();

    match build {
        Ok(conn) => {
            // Spawn the SDK message loop; callbacks fire on this thread.
            conn.run_threaded();
            Some(conn)
        }
        Err(err) => {
            error!(%err, "failed to build shard connection");
            None
        }
    }
}

/// Smoke-test subscription: watch the `zones` table and log inserts plus the
/// applied row count. Replaced by the real client-driven subscription routing
/// in a later milestone.
fn subscribe_zones(ctx: &DbConnection) {
    ctx.db().zones().on_insert(|_ctx, zone| {
        info!(
            macro_zone = zone.macro_zone,
            data_shard = zone.data_shard,
            "zone row"
        );
    });

    ctx.subscription_builder()
        .on_applied(|ctx| {
            info!(count = ctx.db().zones().count(), "zones subscription applied");
        })
        .on_error(|_ctx, err| error!(%err, "zones subscription error"))
        .subscribe(["SELECT * FROM zones"]);
}
