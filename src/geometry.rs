//! On-demand silhouette-geometry sidecars from masters.
//!
//! Mirrors [`crate::lod`]: derive a per-sprite JSON sidecar (boundary polygons +
//! triangulation, computed from the master alpha) lazily on first request and
//! cache it in R2 thereafter. The heavy lifting (marching squares + Visvalingam–
//! Whyatt + earcut) lives in the shared `resonantdust-geometry` crate; this is
//! only the master-fetch + cache plumbing, reusing the LOD path's per-key lock.
//!
//! Delivery: the client blocks on a geometry BUNDLE at login (assembled from these
//! per-object pieces, co-versioned with the manifest). This per-object path is the
//! internal cache the bundle is built from and the art editor's one-card refresh.

use axum::http::StatusCode;

use crate::connections::Pool;
use crate::lod::{key_lock, mark_master_absent, master_is_absent};

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

/// Ensure the sidecar for the request path `rest` exists in R2 and return its
/// JSON bytes. `rest` is laid out as `<object_dir>/<version>/<variation>.json`,
/// mirroring [`crate::lod::ensure`]: the version is a PATH SEGMENT at the object
/// boundary, so a re-mastered object is a distinct key (404 → regenerate), no
/// `srchash` freshness check. The master is version-less, so its key drops the
/// version segment.
pub async fn ensure(pool: &Pool, rest: &str) -> Result<Vec<u8>, GeoError> {
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
    // <object_dir…>/<version>/<variation>.json — parse from the right.
    let parts: Vec<&str> = rest.split('/').collect();
    let n = parts.len();
    if n < 3 {
        return Err(GeoError::new(StatusCode::BAD_REQUEST, format!("malformed geo path {rest:?}")));
    }
    let file = parts[n - 1];
    // parts[n - 2] is the version segment — encoded in the key (no freshness check).
    let object_dir = parts[..n - 2].join("/");
    let variation = file
        .strip_suffix(".json")
        .ok_or_else(|| GeoError::new(StatusCode::BAD_REQUEST, "path must end with .json"))?;

    let geo_key = format!("textures/geo/{rest}");
    let master_key = format!("textures/master/{object_dir}/{variation}.{ALPHA_CHANNEL}.png");

    // Negative cache shared with the LOD path (same master key): a master proven
    // absent yields no sidecar either → 404 now, no R2 round-trip.
    if master_is_absent(&master_key) {
        return Err(GeoError::new(StatusCode::NOT_FOUND, format!("no master at {master_key} (cached absent)")));
    }

    // Fast path: the versioned sidecar exists.
    if let Some(bytes) = store
        .get_bytes(&geo_key)
        .await
        .map_err(|e| GeoError::new(StatusCode::BAD_GATEWAY, e))?
    {
        return Ok(bytes);
    }

    // Slow path under the per-key lock so a burst of identical misses collapses.
    let lock = key_lock(&geo_key);
    let _guard = lock.lock().await;
    if let Some(bytes) = store
        .get_bytes(&geo_key)
        .await
        .map_err(|e| GeoError::new(StatusCode::BAD_GATEWAY, e))?
    {
        return Ok(bytes);
    }

    let master = match store
        .get_bytes(&master_key)
        .await
        .map_err(|e| GeoError::new(StatusCode::BAD_GATEWAY, e))?
    {
        Some(bytes) => bytes,
        None => {
            mark_master_absent(&master_key);
            return Err(GeoError::new(StatusCode::NOT_FOUND, format!("no master at {master_key}")));
        }
    };

    let sidecar = resonantdust_geometry::generate(&master, &resonantdust_geometry::Options::default())
        .map_err(|e| GeoError::new(StatusCode::UNPROCESSABLE_ENTITY, e))?;
    let json = serde_json::to_vec(&sidecar)
        .map_err(|e| GeoError::new(StatusCode::INTERNAL_SERVER_ERROR, format!("serialize: {e}")))?;

    store
        .put_cached(&geo_key, &json, "public, max-age=31536000, immutable")
        .await
        .map_err(|e| GeoError::new(StatusCode::BAD_GATEWAY, e))?;

    tracing::info!(key = %geo_key, bytes = json.len(), "geo: generated from master");
    Ok(json)
}
