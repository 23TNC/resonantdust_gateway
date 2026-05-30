//! Pure routing derivations — which shard owns a card or a region. Thin
//! wrappers over the shared `content` bit-packing so the gate and the
//! SpacetimeDB modules agree on the encoding by construction.

use resonantdust_content::packed;

/// The `cards` shard that owns `card_id` (the top 12 bits of the u32). The
/// gate routes card lookups by this directly — no index needed.
pub fn card_shard(card_id: u32) -> u16 {
    packed::card_shard_of(card_id)
}

/// Map a zone's `macro_zone` to `(macro_region, intra_region_bit)`. The
/// `regions-index` DB then maps that `macro_region` to its `regions` shard.
pub fn region_of_zone(macro_zone: u64) -> (u64, u8) {
    packed::region_of_zone(macro_zone)
}

#[cfg(test)]
mod tests {
    use super::*;
    use resonantdust_content::packed::pack_card_id;

    #[test]
    fn card_shard_reads_top_bits() {
        assert_eq!(card_shard(pack_card_id(1, 1024)), 1);
        assert_eq!(card_shard(pack_card_id(7, 5)), 7);
        assert_eq!(card_shard(0), 0);
    }
}
