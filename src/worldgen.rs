//! Gate-side world terrain generation.
//!
//! Worldgen moved off the SpacetimeDB modules (the gate owns all DSL/content
//! work — plan `01_gate_authority_pivot`). The gate computes a zone's packed
//! tile bytes from the loaded DSL [`Bundle`] + shared noise; the regions
//! `request_zone` reducer just stores them (content-agnostic).

use resonantdust_data::packed::{
    self, surface_of, unpack_macro_zone, WORLD_LAYER, ZONE_TILE_U64_COUNT,
};
use resonantdust_data::loader::Bundle;

/// Canonical world seed — chosen so the spawn origin sits in the forest biome
/// (see `resonantdust_data::noise::tests::origin_lands_in_forest_envelope`).
const WORLD_SEED: u64 = 0x27;

/// The 16 packed tile-u64s the `request_zone` reducer should store for
/// `macro_zone`. World surface → DSL-generated terrain (per-cell biome select +
/// tile `@init`). Non-world surfaces → a dense `empty`-tile grid (rect/inventory
/// zones carry no terrain). Always returns the full 16-u64 array.
pub fn tiles_for_zone(bundle: &Bundle, macro_zone: u64) -> Vec<u64> {
    let mut tiles = [0u64; ZONE_TILE_U64_COUNT];

    if surface_of(macro_zone) != WORLD_LAYER {
        // Non-world: a dense grid of the "empty" tile (the DSL def id).
        let empty = bundle.packed_def("empty").map(|p| p & 0x0FFF).unwrap_or(0);
        for r in 0..8u8 {
            let row = [(empty, 0u8, 0u8); 8];
            packed::set_tile_row(&mut tiles, r as usize, &row);
        }
        return tiles.to_vec();
    }

    let (zq, zr) = unpack_macro_zone(macro_zone);
    let base_q = zq as i32 * 8;
    let base_r = zr as i32 * 8;
    for r in 0..8u8 {
        let mut row = [(0u16, 0u8, 0u8); 8];
        for c in 0..8u8 {
            let (def_id, [s0, s1]) = resonantdust_data::worldgen::generate_tile(
                bundle,
                base_q + c as i32,
                base_r + r as i32,
                WORLD_SEED,
            );
            row[c as usize] = (def_id, s0, s1);
        }
        packed::set_tile_row(&mut tiles, r as usize, &row);
    }
    tiles.to_vec()
}
