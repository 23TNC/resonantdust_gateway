//! On-demand LOD generation from masters.
//!
//! The offline `art lod` pyramid is gone: LODs are generated lazily, the first
//! time a client actually asks for one, and cached in R2 thereafter — so we only
//! ever generate and store the sizes that get used.
//!
//! The client fetches LODs **R2-direct** on the hot path (the CDN serves every
//! cache hit, keeping the gate out of the texture-bandwidth budget). This module
//! backs the **fallback**: when the R2-direct fetch 404s (the LOD doesn't exist
//! yet, or a versioned master made the old one stale), the client re-requests the
//! same path from the gate at `GET /textures/lod/{size}/{stem}.{channel}.png`.
//! The gate then:
//!   1. checks R2 for the LOD (another client may have just generated it),
//!   2. else pulls the **master** channel, Lanczos3-downscales it to `size`
//!      (never upscaling), and `PUT`s the result to R2,
//!   3. returns the PNG bytes to this requester directly — so the first global
//!      miss doesn't depend on CDN propagation of the just-written object.
//! Every later client worldwide gets it straight from R2/CDN.
//!
//! A per-key in-flight lock collapses the burst (the login preview prewarm asks
//! for every stem at once): concurrent misses for the same LOD generate once.

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::{Arc, Mutex, OnceLock};

use axum::http::StatusCode;

use crate::connections::Pool;
use crate::s3::R2Store;

/// The LOD bucket sizes the client requests (mirrors `view/src/assets/lodUrls.ts`
/// `LOD_SIZES`). We only generate these — an arbitrary `size` is rejected so a
/// malformed/hostile request can't fill R2 with junk resolutions.
const LOD_SIZES: [u32; 7] = [16, 32, 64, 128, 256, 512, 1024];

/// Renderable map channels we generate LODs for. `diffuse` (the lit source) is
/// deliberately absent — the client displays the de-lit `albedo`, so the lit
/// master never becomes a LOD.
const CHANNELS: [&str; 3] = ["albedo", "normal", "emissive"];

/// A failed ensure, carrying the HTTP status the route should return.
pub struct LodError {
    pub status: StatusCode,
    pub msg: String,
}

impl LodError {
    fn new(status: StatusCode, msg: impl Into<String>) -> Self {
        Self { status, msg: msg.into() }
    }
}

/// Ensure the LOD for `rest` (`<stem>.<channel>.png`) at `size` exists in R2 and
/// return its PNG bytes. `rest` is the wildcard tail of the route: the same
/// relative key the client uses against R2, so master/LOD addressing is a pure
/// prefix swap. The R2 object key is version-LESS; `version` (the `?v=<hash>`
/// master-version from the client) gates freshness: a cached object whose stored
/// `srchash` differs from `version` is stale — regenerated from the current master
/// and overwritten in place. `None` (a legacy client without `?v`) serves any
/// cached object as-is.
pub async fn ensure(
    pool: &Pool,
    size: u32,
    rest: &str,
    version: Option<&str>,
) -> Result<Vec<u8>, LodError> {
    let store = pool.texture_store().ok_or_else(|| {
        LodError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "texture store not configured (set TEXTURE_R2_BUCKET + R2 creds)",
        )
    })?;

    // ── Validate the request ───────────────────────────────────────────
    if !LOD_SIZES.contains(&size) {
        return Err(LodError::new(StatusCode::BAD_REQUEST, format!("bad LOD size {size}")));
    }
    // No traversal — `rest` lands in an object key.
    if rest.contains("..") {
        return Err(LodError::new(StatusCode::BAD_REQUEST, "`..` not allowed in path"));
    }
    let stem_channel = rest
        .strip_suffix(".png")
        .ok_or_else(|| LodError::new(StatusCode::BAD_REQUEST, "path must end with .png"))?;
    let channel = stem_channel.rsplit('.').next().unwrap_or_default();
    if !CHANNELS.contains(&channel) {
        return Err(LodError::new(
            StatusCode::BAD_REQUEST,
            format!("bad channel {channel:?} (albedo|normal|emissive)"),
        ));
    }

    // master/LOD keys are the same relative path under different prefixes.
    let lod_key = format!("textures/lod/{size}/{rest}");
    let master_key = format!("textures/master/{rest}");

    // Fast path: a cached object that matches the requested version — serve
    // without taking the generation lock. A version mismatch (master changed)
    // falls through to regenerate.
    if let Some(bytes) = fresh_cached(store, &lod_key, version)
        .await
        .map_err(|e| LodError::new(StatusCode::BAD_GATEWAY, e))?
    {
        return Ok(bytes);
    }

    // Slow path: generate under a per-key lock so a burst of identical misses
    // (e.g. the preview prewarm) collapses to one master pull + downscale.
    let lock = key_lock(&lod_key);
    let _guard = lock.lock().await;

    // Re-check inside the lock: a racing task may have generated the current
    // version while we waited.
    if let Some(bytes) = fresh_cached(store, &lod_key, version)
        .await
        .map_err(|e| LodError::new(StatusCode::BAD_GATEWAY, e))?
    {
        return Ok(bytes);
    }

    let master = store
        .get_bytes(&master_key)
        .await
        .map_err(|e| LodError::new(StatusCode::BAD_GATEWAY, e))?
        .ok_or_else(|| LodError::new(StatusCode::NOT_FOUND, format!("no master at {master_key}")))?;

    let out = downscale(&master, size)
        .map_err(|e| LodError::new(StatusCode::UNPROCESSABLE_ENTITY, e))?;

    // Immutable cache directive (each `?v=` URL is a distinct immutable resource
    // on the client/CDN) + the master-version `srchash` so a later request can
    // tell whether this object is stale. R2 key is version-less → overwrite in
    // place, no accumulation.
    store
        .put_lod(&lod_key, &out, "public, max-age=31536000, immutable", version)
        .await
        .map_err(|e| LodError::new(StatusCode::BAD_GATEWAY, e))?;

    tracing::info!(key = %lod_key, bytes = out.len(), version = version.unwrap_or("-"), "lod: generated from master");
    Ok(out)
}

/// Return the cached LOD bytes IFF the object exists AND is current for `version`
/// — i.e. `version` is `None` (legacy, serve anything) or the object's stored
/// `srchash` equals `version`. A present-but-stale object returns `None`, so the
/// caller regenerates from the current master.
async fn fresh_cached(
    store: &R2Store,
    lod_key: &str,
    version: Option<&str>,
) -> Result<Option<Vec<u8>>, String> {
    match store.get_with_meta(lod_key).await? {
        Some((bytes, srchash)) => {
            let current = version.map_or(true, |v| srchash.as_deref() == Some(v));
            Ok(current.then_some(bytes))
        }
        None => Ok(None),
    }
}

/// Decode `master_png`, Lanczos3-downscale it so its longest side is `size`
/// (preserving aspect; **never upscaling** — a master already at/under `size` is
/// re-encoded at native resolution, since no smaller real detail exists), and
/// re-encode as a fresh, metadata-stripped PNG. Pure Rust (the `image` crate's
/// `png` codec + `imageops`), so it builds in the toolchain-free `rust:slim`
/// image. Alpha rides through decode/resize/encode intact.
fn downscale(master_png: &[u8], size: u32) -> Result<Vec<u8>, String> {
    let img = image::load_from_memory(master_png).map_err(|e| format!("decode master: {e}"))?;
    let (w, h) = (img.width(), img.height());
    if w == 0 || h == 0 {
        return Err("master has a zero dimension".to_string());
    }
    let longest = w.max(h);
    let out = if size >= longest {
        img
    } else {
        let scale = size as f64 / longest as f64;
        let nw = ((w as f64 * scale).round() as u32).max(1);
        let nh = ((h as f64 * scale).round() as u32).max(1);
        img.resize_exact(nw, nh, image::imageops::FilterType::Lanczos3)
    };
    let mut buf = Vec::new();
    out.write_to(&mut Cursor::new(&mut buf), image::ImageFormat::Png)
        .map_err(|e| format!("encode lod: {e}"))?;
    Ok(buf)
}

/// The process-wide per-LOD-key generation lock. One `tokio::Mutex` per `lod_key`
/// so only the first of N concurrent misses for the same key pulls + generates;
/// the rest wait and then read the freshly-written object. Entries are bounded by
/// the texture working set (one per distinct generated LOD) and left in place —
/// a slow leak at most, swept whenever a GC of generated LODs lands.
fn key_lock(key: &str) -> Arc<tokio::sync::Mutex<()>> {
    static LOCKS: OnceLock<Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>> = OnceLock::new();
    let map = LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = map.lock().unwrap();
    guard
        .entry(key.to_string())
        .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
        .clone()
}
