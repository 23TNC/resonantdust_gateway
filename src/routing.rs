//! Pure routing derivations — which shard owns a card or a region. Thin
//! wrappers over the shared `content` bit-packing so the gate and the
//! SpacetimeDB modules agree on the encoding by construction.

use resonantdust_codec::packed;

/// The shard that owns `card_id` WITHIN its database (0..2047, the 11-bit shard
/// field). Pair with `packed::card_db_of` to route.
pub fn card_shard(card_id: u32) -> u16 {
    packed::card_shard_within_db(card_id)
}

/// Map a zone's `macro_zone` to `(macro_region, intra_region_bit)`. The
/// `regions-index` DB then maps that `macro_region` to its `regions` shard.
pub fn region_of_zone(macro_zone: u64) -> (u64, u8) {
    packed::region_of_zone(macro_zone)
}

#[cfg(test)]
mod tests {
    use super::*;
    use resonantdust_codec::packed::{card_db_of, pack_card_id, CARD_DB_CARDS, CARD_DB_REGIONS};

    #[test]
    fn card_routing_reads_id() {
        // cards DB, shard 1.
        let id = pack_card_id(CARD_DB_CARDS, 1, 1024);
        assert_eq!(card_db_of(id), CARD_DB_CARDS);
        assert_eq!(card_shard(id), 1);
        // regions DB (tile-card), shard 7.
        let id = pack_card_id(CARD_DB_REGIONS, 7, 5);
        assert_eq!(card_db_of(id), CARD_DB_REGIONS);
        assert_eq!(card_shard(id), 7);
        // sentinel → cards DB, shard 0.
        assert_eq!(card_db_of(0), CARD_DB_CARDS);
        assert_eq!(card_shard(0), 0);
    }
}
