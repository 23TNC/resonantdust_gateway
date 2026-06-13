//! Load the DSL content bundle at gate startup.
//!
//! The `.rd` corpus is bind-mounted at `/workspace/content/data` (see
//! `compose.yml`). We parse it once into a [`Bundle`] — the runtime replacement
//! for the compile-time `resonantdust-content` registries. The recipe pipeline
//! reads it (defs, catalog, the VM). A bad corpus fails the gate loudly rather
//! than serving half-loaded.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use resonantdust_dsl::loader::{load, Bundle};
use resonantdust_dsl::locales::Locales;

/// Where the `.rd` corpus lives in-container (overridable via `CONTENT_DIR`).
const CONTENT_DIR: &str = "/workspace/content/data";
/// Sibling tree holding the client-only `:visuals` facets (same `::name` keys as
/// `data/`), loaded after the data corpus so each visuals facet folds onto its
/// already-defined card. Defaults to a sibling of `CONTENT_DIR`; override via
/// `VISUALS_DIR`.
const VISUALS_SUBDIR: &str = "visuals";
/// Where the locale JSON lives (`<domain>/<lang>.json`); defaults to a sibling of
/// the `.rd` corpus. Overridable via `LOCALES_DIR`.
const LOCALES_SUBDIR: &str = "locales";
/// The locale language the gate serves today. Multi-lang is a future `?lang=`
/// parameter on the content endpoint; the loader is already keyed by domain.
const LOCALE_LANG: &str = "en";

/// The corpus the gate loaded at startup: the parsed [`Bundle`] it validates
/// against, plus the **exact same bytes** pre-serialized for the `/content`
/// endpoint and a version fingerprint. Serving the same corpus the gate runs on
/// means client and gate agree by construction (no drift).
pub struct LoadedContent {
    pub bundle: Arc<Bundle>,
    /// FNV-1a fingerprint of the `.rd` corpus (the P4 multi-gate invariant).
    pub version: u64,
    /// Pre-serialized `GET /content` body: `{version, rd:[[name,text]…],
    /// locales:[[domain,json]…]}`. Built once; the client feeds `rd` to
    /// `new Content(...)` and `locales` to `new Locales(...)`.
    pub payload_json: Arc<str>,
    /// The raw `(name, text)` `.rd` sources this content was built from, sorted.
    /// Retained so a runtime `add_content` can rebuild from `sources + new`.
    pub sources: Vec<(String, String)>,
    /// The raw `(domain, json)` locale sources, sorted. Retained for the same
    /// rebuild path.
    pub locales: Vec<(String, String)>,
}

/// Subdir (under the content root) that holds runtime-authored sources, kept
/// separate from the hand-authored base. Files are named `<seq>_<hint>.rd`; the
/// zero-padded `seq` makes them sort **after** every base dir and **in append
/// order** among themselves, which is exactly the load order the append-stable
/// def-ids need. The gate persists here; the dir is gitignored.
const RUNTIME_SUBDIR: &str = "runtime";

impl LoadedContent {
    /// A new content set with `text` appended as a runtime source + revalidated.
    /// `hint` (the client's name) only flavors the generated filename. Errors if
    /// the merged corpus fails to load; the caller leaves live content untouched
    /// on `Err`, and persists the new source file only on `Ok`.
    pub fn with_added_source(&self, hint: String, text: String) -> Result<LoadedContent, String> {
        // `add` = a NEW lineage. Reject if any def the source declares is already
        // a known lineage (that's `modify_content`, which versions it) — without
        // this, a re-add silently replaces a def in place, breaking the
        // append-only / immutable-version contract.
        for def in def_headers(&text) {
            let lin = resonantdust_dsl::loader::lineage(&def);
            if self.bundle.card_head(lin).is_some() || self.bundle.recipe_head(lin).is_some() {
                return Err(format!(
                    "add_content: lineage {lin:?} already exists — use modify_content to version it"
                ));
            }
        }
        self.append_runtime(&hint, text)
    }

    /// A new content set with a fresh **version** of an existing card `lineage`
    /// appended + revalidated. The submitted `text` defines the modified card
    /// under its bare logical header (`::apple>`); the gate finds the lineage
    /// head, assigns the next version, and rewrites the header to
    /// `::apple.<next>>`. The old version stays (existing instances keep it,
    /// recipes match the lineage), and `create` now resolves the new head. Errors
    /// if the lineage doesn't exist (use `add_content`) or the corpus fails to
    /// load.
    pub fn with_modified_source(
        &self,
        lineage: String,
        text: String,
    ) -> Result<LoadedContent, String> {
        let head = self.bundle.card_head(&lineage).ok_or_else(|| {
            format!("modify_content: no card lineage {lineage:?} (use add_content)")
        })?;
        let next = resonantdust_dsl::loader::version_of(head) + 1;

        let needle = format!("::{lineage}>");
        if !text.contains(&needle) {
            return Err(format!(
                "modify_content: text must define the card under its logical header `{needle}`"
            ));
        }
        let versioned = text.replacen(&needle, &format!("::{lineage}.{next}>"), 1);
        self.append_runtime(&format!("{lineage}.{next}"), versioned)
    }

    /// Append `text` as the next runtime source and rebuild. The source name is
    /// `runtime/<seq:06>_<hint>.rd` — `seq` = the next free runtime index, so it
    /// appends after all existing sources (append-stable ids). The new source is
    /// always `self.sources.last()` on `Ok`, which the caller persists to disk.
    fn append_runtime(&self, hint: &str, text: String) -> Result<LoadedContent, String> {
        let seq = self.next_runtime_seq();
        let hint = hint.strip_suffix(".rd").unwrap_or(hint);
        let safe: String = hint
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() || c == '.' || c == '-' { c } else { '_' })
            .collect();
        let name = format!("{RUNTIME_SUBDIR}/{seq:06}_{safe}.rd");
        let mut sources = self.sources.clone();
        sources.push((name, text));
        build_content(sources, self.locales.clone())
    }

    /// The next runtime sequence index = max existing `runtime/<seq>_…` + 1 (0 if
    /// none). Parsed from the source names load_content read off disk, so it
    /// survives restart.
    fn next_runtime_seq(&self) -> u32 {
        self.sources
            .iter()
            .filter_map(|(n, _)| n.strip_prefix(&format!("{RUNTIME_SUBDIR}/")))
            .filter_map(|rest| rest.get(..6).and_then(|s| s.parse::<u32>().ok()))
            .max()
            .map_or(0, |m| m + 1)
    }
}

/// Build a [`LoadedContent`] from an authority's `/content` payload — a **peer**
/// gate's load path. Parses `{rd, locales}` and rebuilds via [`build_content`],
/// so the peer's bundle + version match the authority's by construction (same
/// ordered sources → same hash). The peer never touches disk for content.
pub fn build_from_payload(json: &str) -> Result<LoadedContent, String> {
    #[derive(serde::Deserialize)]
    struct Payload {
        rd: Vec<(String, String)>,
        locales: Vec<(String, String)>,
    }
    let p: Payload =
        serde_json::from_str(json).map_err(|e| format!("parse /content payload: {e}"))?;
    build_content(p.rd, p.locales)
}

/// The def names a `.rd` source declares — each `::name>` header (inline `:facet`
/// stripped: `::a:visuals>` → `a`). A cheap line scan, enough for `add_content`'s
/// lineage dedup; full structure parsing happens in `load`.
fn def_headers(text: &str) -> Vec<String> {
    text.lines()
        .filter_map(|l| {
            let inner = l.trim().strip_prefix("::")?.strip_suffix('>')?;
            Some(inner.split(':').next().unwrap_or(inner).to_string())
        })
        .collect()
}

/// Persist a runtime source to disk under the content root (so it survives a
/// gate restart — `load_content` reads it back, sorted after the base by its
/// `runtime/<seq>` name). `name` is the relative source name (e.g.
/// `runtime/000000_apple.rd`). The gate's content dir is bind-mounted rw.
pub fn persist_source(name: &str, text: &str) -> Result<(), String> {
    let dir = std::env::var("CONTENT_DIR").unwrap_or_else(|_| CONTENT_DIR.to_string());
    let path = Path::new(&dir).join(name);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("persist {name:?}: mkdir: {e}"))?;
    }
    std::fs::write(&path, text).map_err(|e| format!("persist {name:?}: write: {e}"))
}

/// Parse the `.rd` corpus into a [`Bundle`] and read + validate the locale JSON,
/// returning everything the gate needs to both run and serve content. Panics on
/// a bad corpus or bad locale JSON — a gate serving broken content to clients is
/// worse than not starting.
pub fn load_content() -> LoadedContent {
    let dir = std::env::var("CONTENT_DIR").unwrap_or_else(|_| CONTENT_DIR.to_string());
    // The base corpus is two parallel trees: `data/` (server-authoritative card
    // logic) and a sibling `visuals/` (client-only render facets, same `::name`
    // keys). Load data FIRST so a card's def-id derives from its `:data` file;
    // the loader then folds each `:visuals` facet onto the existing def (see
    // loader::index_defs). Within each tree, sorted order IS the append-stable
    // id assignment; visuals never introduce or renumber a def.
    let visuals_dir = std::env::var("VISUALS_DIR").unwrap_or_else(|_| {
        Path::new(&dir)
            .parent()
            .unwrap_or(Path::new(&dir))
            .join(VISUALS_SUBDIR)
            .display()
            .to_string()
    });

    // Collect one tree into `(name, text)` sources, sorted, with names keyed by
    // path RELATIVE to that tree's root (under `prefix`) — so the names the client
    // sees + error messages are stable + machine-independent.
    let read_tree = |root: &str, prefix: &str| -> Vec<(String, String)> {
        let mut files = Vec::new();
        collect_rd(Path::new(root), &mut files);
        files.sort();
        files
            .iter()
            .map(|f| {
                let text = std::fs::read_to_string(f)
                    .unwrap_or_else(|e| panic!("read {}: {e}", f.display()));
                let rel = f.strip_prefix(root).unwrap_or(f.as_path()).display().to_string();
                (format!("{prefix}{rel}"), text)
            })
            .collect()
    };

    let mut sources = read_tree(&dir, "");
    sources.extend(read_tree(&visuals_dir, "visuals/"));

    let locales = read_locales(&dir);
    match build_content(sources, locales) {
        Ok(c) => {
            tracing::info!(
                dir = %dir,
                files = c.sources.len(),
                locale_domains = c.locales.len(),
                cards = c.bundle.card_ids.len(),
                recipes = c.bundle.recipe_ids.len(),
                aspects = c.bundle.table.aspects.len(),
                version = %format!("{:016x}", c.version),
                "content bundle loaded"
            );
            c
        }
        // A gate serving broken content to clients is worse than not starting.
        Err(e) => panic!("content load failed under {dir}: {e}"),
    }
}

/// The manifest an R2/HTTP-sourced authority reads to learn its corpus — the
/// object store can't list a directory the way a disk walk can, so the bucket
/// carries an explicit, ordered index. Keys are paths relative to
/// `CONTENT_BASE_URL`, mirroring the repo layout (`data/…`, `visuals/…`,
/// `locales/<domain>/<lang>.json`). Order within each list is irrelevant — the
/// gate sorts to reproduce the disk load's append-stable ordering exactly.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct Manifest {
    /// `data/` `.rd` keys (server-authoritative card logic).
    #[serde(default)]
    pub data: Vec<String>,
    /// `visuals/` `.rd` keys (client-only render facets, same `::name`).
    #[serde(default)]
    pub visuals: Vec<String>,
    /// `domain → locales/<domain>/<lang>.json` key.
    #[serde(default)]
    pub locales: std::collections::BTreeMap<String, String>,
}

/// Where an authority reads its base corpus from. `Http` is a public read-only
/// origin (e.g. r2.dev) reached with plain GETs; `S3` is the strongly-consistent
/// S3 API (R2/MinIO) used when the gate also AUTHORS — so its own poll reads back
/// exactly what it just wrote, with no public-CDN staleness revert race.
pub enum ContentSrc {
    /// Base URL prefix (no trailing slash needed); a key `k` is `GET {base}/{k}`.
    Http(String),
    /// S3 store; a key `k` is a `GetObject` on the bucket.
    S3(std::sync::Arc<crate::s3::R2Store>),
}

impl ContentSrc {
    /// Fetch one object body by its manifest-relative key (`manifest.json`,
    /// `data/…`, `visuals/…`, `locales/…`).
    async fn get(&self, key: &str) -> Result<String, String> {
        match self {
            ContentSrc::Http(base) => {
                fetch_text(crate::connections::http_client(), &format!("{}/{key}", base.trim_end_matches('/'))).await
            }
            ContentSrc::S3(store) => store.get(key).await,
        }
    }
}

/// Load the base corpus from an object store (R2 public HTTP or the S3 API)
/// instead of local disk — the **authority** gate's `CONTENT_BASE_URL` / R2-creds
/// path. Fetches `manifest.json`, then every listed object, and feeds the
/// **unchanged** [`build_content`]. Source names are normalized to match
/// [`load_content`]'s on-disk layout (`data/` stripped, `visuals/` kept), so the
/// version fingerprint is byte-identical to a disk load of the same files — the
/// migration is lossless and client/gate stay in lockstep.
pub async fn load_content_src(src: &ContentSrc) -> Result<LoadedContent, String> {
    let (sources, locales) = fetch_sources(src).await?;
    build_content(sources, locales)
}

/// Poll the corpus and, if its fingerprint differs from `current`, parse +
/// validate + return the new [`LoadedContent`]; otherwise `Ok(None)` (unchanged
/// — we skip the parse). The authority's re-poll loop calls this every few
/// seconds: a *fetch* every tick, but only a *build* on an actual change. A bad
/// fetch (store blip) returns `Err`; the caller logs and keeps live content.
pub async fn poll_content_src(src: &ContentSrc, current: u64) -> Result<Option<LoadedContent>, String> {
    let (sources, locales) = fetch_sources(src).await?;
    if content_version(&sources, &locales) == current {
        return Ok(None);
    }
    build_content(sources, locales).map(Some)
}

/// Persist a newly-authored runtime source to an S3 store + add it to the
/// manifest, so a restart / poll / peer sees it (the unified-store authoring
/// path). `name` is the load-relative source name (e.g. `runtime/000000_apple.rd`);
/// the object key prepends `data/` — the inverse of the load's `data/` strip — so
/// a reload reads it back under the same name (and `next_runtime_seq` keeps
/// counting). The manifest's `data` list gains the key (idempotent). Strongly
/// consistent, so the authority's own poll won't revert it.
pub async fn persist_source_s3(
    store: &crate::s3::R2Store,
    name: &str,
    text: &str,
) -> Result<(), String> {
    let key = format!("data/{name}");
    store.put(&key, text.as_bytes()).await?;
    let mut manifest: Manifest = serde_json::from_str(&store.get("manifest.json").await?)
        .map_err(|e| format!("parse manifest for update: {e}"))?;
    if !manifest.data.iter().any(|k| k == &key) {
        manifest.data.push(key);
    }
    let body = serde_json::to_string_pretty(&manifest)
        .map_err(|e| format!("serialize manifest: {e}"))?;
    store.put("manifest.json", body.as_bytes()).await
}

/// Fetch the raw ordered `(name, text)` `.rd` sources + `(domain, json)` locales
/// from a [`ContentSrc`], normalized to the on-disk layout (so the fingerprint
/// matches a disk load — see [`load_content`]). Shared by the startup load and
/// the re-poll, over either the HTTP or S3 source.
async fn fetch_sources(
    src: &ContentSrc,
) -> Result<(Vec<(String, String)>, Vec<(String, String)>), String> {
    let manifest: Manifest = serde_json::from_str(&src.get("manifest.json").await?)
        .map_err(|e| format!("parse manifest.json: {e}"))?;

    // `data/` sources: name = key with the `data/` prefix stripped, matching the
    // disk load's `read_tree(dir, "")`. Sort to reproduce its `files.sort()`.
    let mut sources: Vec<(String, String)> = Vec::new();
    for key in &manifest.data {
        let text = src.get(key).await?;
        sources.push((key.strip_prefix("data/").unwrap_or(key).to_string(), text));
    }
    sources.sort();

    // `visuals/` sources: name kept as `visuals/…`, matching `read_tree(_, "visuals/")`.
    // They load AFTER the whole data tree, so each facet folds onto its def.
    let mut visuals: Vec<(String, String)> = Vec::new();
    for key in &manifest.visuals {
        let text = src.get(key).await?;
        let name = if key.starts_with("visuals/") { key.clone() } else { format!("visuals/{key}") };
        visuals.push((name, text));
    }
    visuals.sort();
    sources.extend(visuals);

    // Locales: `(domain, json)`, sorted by domain like `read_locales`.
    let mut locales: Vec<(String, String)> = Vec::new();
    for (domain, key) in &manifest.locales {
        let json = src.get(key).await?;
        locales.push((domain.clone(), json));
    }
    locales.sort();

    Ok((sources, locales))
}

/// `GET` a URL and return its body as text, mapping a transport error or any
/// non-2xx status into a human-readable `Err` (so a 404 on a manifest-listed key
/// fails the load loudly rather than silently dropping a source).
async fn fetch_text(client: &reqwest::Client, url: &str) -> Result<String, String> {
    let resp = client.get(url).send().await.map_err(|e| format!("GET {url}: {e}"))?;
    let status = resp.status();
    if !status.is_success() {
        return Err(format!("GET {url}: HTTP {status}"));
    }
    resp.text().await.map_err(|e| format!("read {url}: {e}"))
}

/// Build a [`LoadedContent`] from raw sources + locales, validating both —
/// **non-panicking** so both startup ([`load_content`]) and runtime
/// `add_content` share one path. Source **order is preserved** (not sorted): it
/// is the append-stable def-id assignment, so callers control it — `load_content`
/// serves the base in sorted-file order, `add_content` appends. Returns a
/// human-readable `Err` on any parse/resolve/locale problem.
pub fn build_content(
    sources: Vec<(String, String)>,
    locales: Vec<(String, String)>,
) -> Result<LoadedContent, String> {
    // Content-version fingerprint (P4 invariant): a hash of the ordered
    // `(name, text)` sources + `(domain, json)` locales. All gates in a
    // multi-gate deploy MUST agree on the canonical order (hence `version`) —
    // divergent order means divergent ids.
    let version = content_version(&sources, &locales);

    // Validate the locale JSON (the gate doesn't render strings; this confirms
    // it parses before we serve it).
    Locales::load(&locales).map_err(|e| format!("locale load failed: {e}"))?;

    let bundle = load(&sources).map_err(|errors| {
        let preview: Vec<String> = errors
            .iter()
            .take(5)
            .map(|e| format!("{}: {}", e.file, e.message))
            .collect();
        format!("{} problem(s): {}", errors.len(), preview.join("; "))
    })?;

    let payload_json: Arc<str> = serde_json::to_string(&serde_json::json!({
        "version": format!("{version:016x}"),
        "rd": sources,
        "locales": locales,
    }))
    .map_err(|e| format!("serialize content payload: {e}"))?
    .into();

    Ok(LoadedContent {
        bundle: Arc::new(bundle),
        version,
        payload_json,
        sources,
        locales,
    })
}

/// Read `<content-root>/locales/<domain>/<lang>.json` for each domain subdir,
/// returning `(domain, json)` pairs in sorted order (so the payload + any future
/// fingerprint are deterministic). Missing locale dir → empty (the gate still
/// serves an empty `locales` array). The corpus root is the parent of the `.rd`
/// dir; override the whole path via `LOCALES_DIR`.
fn read_locales(rd_dir: &str) -> Vec<(String, String)> {
    let locales_dir = std::env::var("LOCALES_DIR").unwrap_or_else(|_| {
        Path::new(rd_dir)
            .parent()
            .unwrap_or(Path::new(rd_dir))
            .join(LOCALES_SUBDIR)
            .display()
            .to_string()
    });
    let Ok(entries) = std::fs::read_dir(&locales_dir) else {
        tracing::warn!(dir = %locales_dir, "no locales dir; serving none");
        return Vec::new();
    };
    let mut out: Vec<(String, String)> = Vec::new();
    for entry in entries.flatten() {
        let domain_dir = entry.path();
        if !domain_dir.is_dir() {
            continue;
        }
        let Some(domain) = domain_dir.file_name().and_then(|s| s.to_str()).map(String::from) else {
            continue;
        };
        let file = domain_dir.join(format!("{LOCALE_LANG}.json"));
        match std::fs::read_to_string(&file) {
            Ok(json) => out.push((domain, json)),
            Err(_) => tracing::warn!(domain = %domain, "no {LOCALE_LANG}.json; skipping"),
        }
    }
    out.sort();
    out
}

/// A stable 64-bit fingerprint of the loaded corpus — FNV-1a over each sorted
/// source's relative name + bytes, then each locale `domain` + bytes.
/// Deterministic across machines (no hashing of absolute paths or timestamps),
/// so two gates with identical content log the same value. Locales participate
/// so a locale-only edit changes the version and triggers a client reload. Not
/// cryptographic; a deploy-time equality check, not a security gate.
fn content_version(sources: &[(String, String)], locales: &[(String, String)]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325; // FNV-1a offset basis
    let mut feed = |bytes: &[u8]| {
        for &b in bytes {
            h ^= b as u64;
            h = h.wrapping_mul(0x0000_0100_0000_01b3); // FNV prime
        }
    };
    for (name, text) in sources {
        // hash the file basename (not the absolute path) + contents
        let base = name.rsplit('/').next().unwrap_or(name);
        feed(base.as_bytes());
        feed(b"\0");
        feed(text.as_bytes());
        feed(b"\0");
    }
    for (domain, json) in locales {
        feed(domain.as_bytes());
        feed(b"\0");
        feed(json.as_bytes());
        feed(b"\0");
    }
    h
}

/// The folded value of `aspect` on a card def (by name) — its static aspects
/// with the `satisfies` hierarchy rolled up (so `builder` sums `crafting`, …).
/// The gate-side replacement for the cards module's old `def_aspect_total`;
/// used to compute the blueprint builder-cap. `0` for an unknown def/aspect.
pub fn def_aspect_total(bundle: &Bundle, name: &str, aspect: &str) -> i64 {
    use resonantdust_dsl::bridge::{card_view, Card};
    use resonantdust_dsl::vm::{Cell, Store};
    let Some(def_id) = bundle.card_def_id(name) else {
        return 0;
    };
    let view = card_view(bundle, &Card { def_id, stock: Vec::new() });
    Store::with_root(view).read(&format!("aspect.{aspect}")).map(Cell::as_int).unwrap_or(0)
}

fn collect_rd(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let p = entry.path();
        if p.is_dir() {
            collect_rd(&p, out);
        } else if p.extension().map(|x| x == "rd").unwrap_or(false) {
            out.push(p);
        }
    }
}
