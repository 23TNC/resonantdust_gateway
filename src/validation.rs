//! Recipe validation over a gathered snapshot — the thin adapter wiring the
//! storage-agnostic core (`resonantdust_content::recipe_validate`) to the
//! gate's [`Snapshot`]. The snapshot already holds the latest version per
//! card, so [`CardStore::card_at`] ignores `time_ms` and returns it directly.

use resonantdust_content::recipe_core::recipe;
use resonantdust_content::recipe_validate::{validate_input, CardStore, CardView, SyntheticTile};

use crate::gather::{Proposal, Snapshot};

impl CardStore for Snapshot {
    fn card_at(&self, card_id: u32, _time_ms: u64) -> Option<CardView> {
        self.cards.get(&card_id).map(|c| CardView {
            card_id: c.card_id,
            owner_id: c.owner_id,
            micro_location: c.micro_location,
            macro_zone: c.macro_zone,
            packed_definition: c.packed_definition,
            flags_state: c.flags_state,
            flags_bk: c.flags_bk,
        })
    }
}

/// Validate `proposal`'s recipe against the gathered snapshot. `Ok(())` means
/// the bound stack satisfies the recipe's input predicates. `synthetic` is the
/// tile derived from the gathered zone (via [`crate::gather::synthetic_tile`])
/// for the branch-0 slot, or `None` for recipes that don't target a tile.
pub fn validate(
    snap: &Snapshot,
    proposal: &Proposal,
    synthetic: Option<SyntheticTile>,
    now_ms: u64,
) -> Result<(), String> {
    let recipe = recipe(proposal.recipe_id)
        .map_err(|e| format!("recipe registry: {e}"))?
        .ok_or_else(|| format!("unknown recipe id {}", proposal.recipe_id))?;

    validate_input(
        snap,
        recipe,
        proposal.root,
        &proposal.bindings,
        synthetic,
        now_ms,
    )
}
