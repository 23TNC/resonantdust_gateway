//! On-demand silhouette-geometry sidecars from masters.
//!
//! Mirrors [`crate::lod`]: derive a per-sprite JSON sidecar (boundary polygons +
//! triangulation, computed from the master alpha) lazily on first request and
//! cache it in R2 thereafter. The heavy lifting (marching squares + Visvalingam–
//! Whyatt + earcut) lives in the shared `resonantdust-geometry` crate; this is
//! only the master-fetch + cache + version plumbing, reusing the LOD path's
//! freshness check and per-key lock.
//!
//! Delivery: the client blocks on a geometry BUNDLE at login (assembled from these
//! per-object pieces, co-versioned with the manifest). This per-object path is the
//! internal cache the bundle is built from and the art editor's one-card refresh.

use axum::http::StatusCode;

use crate::connections::Pool;
use crate::lod::{fresh_cached, key_lock};

/// The master channel whose alpha is the silhouette. `albedo` is what the client
/// displays (and de-light leaves alpha untouched), so the geometry matches the
/// rendered sprite — and if it's absent the LODs are too, so the sprite can't
/// render anyway.
const ALPHA_CHANNEL: &str = "albedo";

/// A failed ensure, carrying the HTTP status the route should return.
pub struct GeoError {
    pub status: StatusCode,
    pub msg: String,
}

impl GeoError {
    fn new(status: StatusCode, msg: impl Into<String>) -> Self {
        Self { status, msg: msg.into() }
    }
}

/// Ensure the sidecar for `rest` (`<stem>.json`) exists in R2 and return its JSON
/// bytes. The R2 object key is version-less; `version` (`?v=<hash>`, the master's
/// srchash) gates freshness exactly like [`crate::lod::ensure`]: a cached object
/// whose stored `srchash` differs is stale, regenerated from the current master
/// and overwritten in place.
pub async fn ensure(pool: &Pool, rest: &str, version: Option<&str>) -> Result<Vec<u8>, GeoError> {
    let store = pool.texture_store().ok_or_else(|| {
        GeoError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "texture store not configured (set TEXTURE_R2_BUCKET + R2 creds)",
        )
    })?;

    // No traversal — `rest` lands in an object key.
    if rest.contains("..") {
        return Err(GeoError::new(StatusCode::BAD_REQUEST, "`..` not allowed in path"));
    }
    let stem = rest
        .strip_suffix(".json")
        .ok_or_else(|| GeoError::new(StatusCode::BAD_REQUEST, "path must end with .json"))?;

    let geo_key = format!("textures/geo/{rest}");
    let master_key = format!("textures/master/{stem}.{ALPHA_CHANNEL}.png");

    // Fast path: a cached sidecar matching the requested version.
    if let Some(bytes) = fresh_cached(store, &geo_key, version)
        .await
        .map_err(|e| GeoError::new(StatusCode::BAD_GATEWAY, e))?
    {
        return Ok(bytes);
    }

    // Slow path under the per-key lock so a burst of identical misses collapses.
    let lock = key_lock(&geo_key);
    let _guard = lock.lock().await;
    if let Some(bytes) = fresh_cached(store, &geo_key, version)
        .await
        .map_err(|e| GeoError::new(StatusCode::BAD_GATEWAY, e))?
    {
        return Ok(bytes);
    }

    let master = store
        .get_bytes(&master_key)
        .await
        .map_err(|e| GeoError::new(StatusCode::BAD_GATEWAY, e))?
        .ok_or_else(|| GeoError::new(StatusCode::NOT_FOUND, format!("no master at {master_key}")))?;

    let sidecar = resonantdust_geometry::generate(&master, &resonantdust_geometry::Options::default())
        .map_err(|e| GeoError::new(StatusCode::UNPROCESSABLE_ENTITY, e))?;
    let json = serde_json::to_vec(&sidecar)
        .map_err(|e| GeoError::new(StatusCode::INTERNAL_SERVER_ERROR, format!("serialize: {e}")))?;

    store
        .put_lod(&geo_key, &json, "public, max-age=31536000, immutable", version)
        .await
        .map_err(|e| GeoError::new(StatusCode::BAD_GATEWAY, e))?;

    tracing::info!(
        key = %geo_key,
        bytes = json.len(),
        version = version.unwrap_or("-"),
        "geo: generated from master"
    );
    Ok(json)
}
