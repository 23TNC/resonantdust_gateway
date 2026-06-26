//! Gate-side world terrain generation.
//!
//! Worldgen moved off the SpacetimeDB modules (the gate owns all DSL/content
//! work — plan `01_gate_authority_pivot`). The gate computes a zone's packed
//! tile bytes from the loaded DSL [`Bundle`] + shared noise; the regions
//! `request_zone` reducer just stores them (content-agnostic).

use resonantdust_codec::packed::{
    set_tile_full, surface_of, tile_slot, unpack_macro_zone, world_tile, WORLD_LAYER, ZONE_SIZE,
    ZONE_TILE_U64_COUNT,
};
use resonantdust_dsl::loader::Bundle;

/// Canonical world seed — chosen so the spawn zone, macro_zone (3,3) (world
/// tiles 24..31, where the harness seeds souls), is entirely forest. Found by
/// `resonantdust-data --bin seedsearch`; full forest at (3,3) with a healthy
/// world mix (≈58% plains / 24% forest / 16% mountain / 2% desert). The noise
/// parity lock (`noise::tests`) pins a SEPARATE fixed reference seed (0x27) for
/// the bit-for-bit Rust↔TS check — it is intentionally not this value.
const WORLD_SEED: u64 = 1;

/// The 16 packed tile-u64s the `request_zone` reducer should store for
/// `macro_zone`. World surface → DSL-generated terrain (per-cell biome select +
/// tile `@init`). Non-world surfaces → a dense `inventory`-tile grid (each cell
/// a slot card-box; rect/inventory zones carry no terrain). Always returns the
/// full 16-u64 array.
pub fn tiles_for_zone(bundle: &Bundle, macro_zone: u64) -> Vec<u64> {
    let mut tiles = [0u64; ZONE_TILE_U64_COUNT];

    if surface_of(macro_zone) != WORLD_LAYER {
        // Non-world: a dense grid of the `inventory` tile (the DSL def id) — each
        // cell renders a slot card-box (`rect_card`) so the inventory reads as a
        // grid of slots rather than blank. Falls back to `empty` if the content
        // lacks an `inventory` tile.
        let slot = bundle
            .packed_def("inventory")
            .or_else(|| bundle.packed_def("empty"))
            .map(|p| p & 0x0FFF)
            .unwrap_or(0);
        // Fill only the LOGICAL 7×7 cells; the shard's disk mask clips further.
        for lr in 0..ZONE_SIZE {
            for lc in 0..ZONE_SIZE {
                set_tile_full(&mut tiles, tile_slot(lc as u8, lr as u8), slot, 0, 0);
            }
        }
        return tiles.to_vec();
    }

    let (zq, zr) = unpack_macro_zone(macro_zone);
    for lr in 0..ZONE_SIZE {
        for lc in 0..ZONE_SIZE {
            let (def_id, [s0, s1]) = resonantdust_dsl::worldgen::generate_tile(
                bundle,
                world_tile(zq, lc as u8),
                world_tile(zr, lr as u8),
                WORLD_SEED,
            );
            set_tile_full(&mut tiles, tile_slot(lc as u8, lr as u8), def_id, s0, s1);
        }
    }
    tiles.to_vec()
}

/// The `cost` aspect of the world tile at hex `(wq, wr)` — biome (from the same
/// `WORLD_SEED` worldgen) → tile def name → folded `cost`. `None` if no biome
/// resolves. Used by the gate to price a `move_card` step authoritatively.
pub fn tile_cost_at(bundle: &Bundle, wq: i32, wr: i32) -> Option<i64> {
    let climate = resonantdust_dsl::noise::climate_floats(wq, wr, WORLD_SEED);
    let name = resonantdust_dsl::worldgen::select_biome(bundle, &climate)?;
    Some(crate::content::def_aspect_total(bundle, &name, "cost"))
}
