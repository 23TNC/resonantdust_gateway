//! Load the DSL content bundle at gate startup.
//!
//! The `.rd` corpus is bind-mounted at `/workspace/content/data` (see
//! `compose.yml`). We parse it once into a [`Bundle`] — the runtime replacement
//! for the compile-time `resonantdust-content` registries. The recipe pipeline
//! reads it (defs, catalog, the VM). A bad corpus fails the gate loudly rather
//! than serving half-loaded.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use resonantdust_data::loader::{load, Bundle};

/// Where the `.rd` corpus lives in-container (overridable via `CONTENT_DIR`).
const CONTENT_DIR: &str = "/workspace/content/data";

/// Parse every `.rd` under the content dir into a shared [`Bundle`]. Panics on a
/// bad corpus — a gate serving recipes against broken content is worse than not
/// starting.
pub fn load_bundle() -> Arc<Bundle> {
    let dir = std::env::var("CONTENT_DIR").unwrap_or_else(|_| CONTENT_DIR.to_string());
    let mut files = Vec::new();
    collect_rd(Path::new(&dir), &mut files);
    files.sort();

    let sources: Vec<(String, String)> = files
        .iter()
        .map(|f| {
            let text = std::fs::read_to_string(f)
                .unwrap_or_else(|e| panic!("read {}: {e}", f.display()));
            (f.display().to_string(), text)
        })
        .collect();

    match load(&sources) {
        Ok(bundle) => {
            tracing::info!(
                dir = %dir,
                files = sources.len(),
                cards = bundle.card_ids.len(),
                recipes = bundle.recipe_ids.len(),
                aspects = bundle.table.aspects.len(),
                "content bundle loaded"
            );
            Arc::new(bundle)
        }
        Err(errors) => {
            for e in errors.iter().take(20) {
                tracing::error!(file = %e.file, "{}", e.message);
            }
            panic!("content load failed: {} problem(s) under {dir}", errors.len());
        }
    }
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
