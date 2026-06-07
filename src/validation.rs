//! `CardStore` adapter — wires the storage-agnostic core
//! (`resonantdust_data::recipe_state`) to the gate's [`Snapshot`] so
//! `validate_bindings` (ownership / not-dead / holds / dedup state checks) can
//! read cards. The snapshot already holds the latest version per card, so
//! [`CardStore::card_at`] ignores `time_ms` and returns it directly.
//!
//! Recipe *semantics* (input match + output plan) moved to the DSL vm
//! (`crate::dsl_recipe`); only the state-validation adapter remains here.

use resonantdust_data::recipe_state::{CardStore, CardView};

use crate::gather::Snapshot;

impl CardStore for Snapshot {
    fn card_at(&self, card_id: u32, _time_ms: u64) -> Option<CardView> {
        self.cards.get(&card_id).map(|c| CardView {
            card_id: c.card_id,
            owner_id: c.owner_id,
            micro_location: c.micro_location,
            macro_zone: c.macro_zone,
            packed_definition: c.packed_definition,
            flags: c.flags,
        })
    }
}
