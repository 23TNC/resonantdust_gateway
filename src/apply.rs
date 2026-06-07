//! Apply — materialize a validated [`ActionPlan`] on the data shards.
//!
//! One coarse reducer call PER DATABASE: `apply_action_tile` on the region DB
//! (the synthetic tile) and `apply_action` on the cards DB (bound cards + the
//! card-side effects). Each runs in a single transaction, so the client receives
//! one commit per shard — one `now` row + one fully-formed `completion` row per
//! card — instead of the old one-call-per-hold-kind/effect decomposition (which
//! let the client promote a half-written completion row → the cut-tree flicker).
//!
//! No cross-DB transaction exists, so the two calls are sequenced: the tile
//! first (its exclusive `slot_hold` is the most-contended guard, so a
//! concurrent-cut rejection fails fast before the cards DB is touched), then the
//! cards apply. Each coarse reducer still writes its own self-expiring release
//! rows, so a partial cross-shard failure self-heals at `completion_ms`. The
//! dedup gate (`claim_pending`) guards exact-duplicate submission.
//!
//! Card-only v1: `Effect::CreateDeferred` (stack.N.create) and world-terrain
//! effects are rejected upstream in the content planner; this never sees them.

use std::collections::BTreeSet;

use serde_json::{json, Value};
use tracing::debug;

use resonantdust_data::packed::micro_loose_cell;
use resonantdust_data::plan::{ActionPlan, Effect, HoldKinds};

use crate::connections::Pool;
use crate::gather::{Proposal, Snapshot};

/// Single `cards` shard today (owner-sharding is future work).
const CARDS_SHARD: u16 = 0;

// Hold-kind bit positions — must match `shard::gate_api::hold_kind`.
const K_TOUCH: u8 = 0;
const K_SLOT_HOLD: u8 = 1;
const K_SLOT_SHARE: u8 = 2;
const K_POSITION_HOLD: u8 = 3;

/// `soul` card_type nibble (content/cards/types.json). The owning-soul walk
/// stops at the first owner of this type.
const SOUL_CARD_TYPE: u8 = 6;

/// Soul-stat slot for a stat-card name → `(field, byte_index)` where field is
/// the soul-stat selector (0=stats, 1=fatigued, 2=injured) and byte is
/// corpus=0/anima=1/sollertia=2/aether=3. The gate owns this mapping. `None` for
/// non-stat cards.
fn stat_slot(name: &str) -> Option<(u8, u8)> {
    let (field, base) = match name.strip_suffix("_dim") {
        Some(b) => (1u8, b), // fatigued
        None => (0u8, name), // stats
    };
    let byte = match base {
        "corpus" => 0,
        "anima" => 1,
        "sollertia" => 2,
        "aether" => 3,
        _ => return None,
    };
    Some((field, byte))
}

/// Walk `card_id`'s `owner_id` chain in the snapshot to the owning soul card
/// (first owner whose packed type nibble is `soul`). `None` if no soul in chain.
fn owning_soul(snap: &Snapshot, card_id: u32) -> Option<u32> {
    let mut cur = card_id;
    for _ in 0..16 {
        let c = snap.cards.get(&cur)?;
        if ((c.packed_definition >> 12) & 0xF) as u8 == SOUL_CARD_TYPE {
            return Some(cur);
        }
        if c.owner_id == 0 || c.owner_id == cur {
            return None;
        }
        cur = c.owner_id;
    }
    None
}

/// Per-card hold bitmask for a HELD bound card: `touch` (always — a held card is
/// kept alive) plus the verb's fields. Bit `i` = hold-kind `i`.
fn hold_mask(kinds: &HoldKinds) -> u8 {
    let mut m = 1u8 << K_TOUCH;
    if kinds.slot_hold {
        m |= 1 << K_SLOT_HOLD;
    }
    if kinds.slot_share {
        m |= 1 << K_SLOT_SHARE;
    }
    if kinds.position_hold {
        m |= 1 << K_POSITION_HOLD;
    }
    m
}

/// Tile hold bitmask — like [`hold_mask`] but WITHOUT `touch` (tiles were never
/// touch-held; matches the retired `tile_lease`).
fn tile_hold_mask(kinds: &HoldKinds) -> u8 {
    let mut m = 0u8;
    if kinds.slot_hold {
        m |= 1 << K_SLOT_HOLD;
    }
    if kinds.slot_share {
        m |= 1 << K_SLOT_SHARE;
    }
    if kinds.position_hold {
        m |= 1 << K_POSITION_HOLD;
    }
    m
}

/// Materialize `plan` for `proposal`. `now_ms` stamps the hold acquires (which
/// lock the bound cards / tile in place for the action's life); the releases,
/// per-card finalize, and completion effects are future-stamped at
/// `now_ms + plan.duration_ms()`.
pub async fn apply(
    pool: &Pool,
    snap: &Snapshot,
    proposal: &Proposal,
    plan: &ActionPlan,
    now_ms: u64,
) -> Result<(), String> {
    let completion_ms = now_ms + plan.duration_ms();
    let cards_db = pool.config().cards_db(CARDS_SHARD);
    let client = crate::connections::http_client().clone();

    // 1. Dedup gate — gate-only (`pending_actions` is never relayed to clients).
    call(
        &client,
        pool,
        &cards_db,
        "claim_pending",
        json!({
            "recipe_id": proposal.recipe_id,
            "root": proposal.root,
            "bindings": proposal.bindings,
            "completion_ms": completion_ms,
        }),
    )
    .await?;

    // 2. Tile (region DB) — one transaction, when the recipe targets the
    //    synthetic tile. First, so the exclusive-cut guard fails fast.
    if let Some(kinds) = &plan.tile_holds {
        let (q, r) = micro_loose_cell(proposal.micro_location);
        let mut stock_slots: Vec<u8> = Vec::new();
        let mut stock_ops: Vec<u8> = Vec::new();
        let mut stock_deltas: Vec<u8> = Vec::new();
        for effect in &plan.effects {
            if let Effect::ModifyTileStock { slot, op, delta } = effect {
                stock_slots.push(*slot);
                stock_ops.push(op.code());
                stock_deltas.push(*delta);
            }
        }
        let regions_db = pool.regions_db();
        call(
            &client,
            pool,
            &regions_db,
            "apply_action_tile",
            json!({
                "now_ms": now_ms,
                "completion_ms": completion_ms,
                "surface": proposal.surface,
                "macro_zone": proposal.macro_zone,
                "q": q,
                "r": r,
                "hold_mask": tile_hold_mask(kinds),
                "stock_slots": stock_slots,
                "stock_ops": stock_ops,
                "stock_deltas": stock_deltas,
            }),
        )
        .await?;
    }

    // 3. Cards DB — one transaction. Bound set = root + bindings (deduped, minus
    //    the sentinel-0 tile). A held card's mask is `touch | verb fields`; a
    //    bound-but-unheld card's mask is 0 (it only needs finalizing).
    let mut bound: BTreeSet<u32> = BTreeSet::new();
    if proposal.root != 0 {
        bound.insert(proposal.root);
    }
    for row in &proposal.bindings {
        for &id in row {
            if id != 0 {
                bound.insert(id);
            }
        }
    }
    let bound_ids: Vec<u32> = bound.iter().copied().collect();
    let bound_masks: Vec<u8> = bound_ids
        .iter()
        .map(|id| plan.holds.get(id).map(hold_mask).unwrap_or(0))
        .collect();

    // Effects → parallel arrays. Soul-stat deltas are gate-resolved here from the
    // snapshot + content (the gate owns the stat-card → soul mapping), then
    // applied inside the reducer.
    let mut destroy_ids: Vec<u32> = Vec::new();
    let mut create_defs: Vec<u16> = Vec::new();
    let mut create_surfaces: Vec<u8> = Vec::new();
    let mut create_macro_zones: Vec<u64> = Vec::new();
    let mut create_owners: Vec<u32> = Vec::new();
    let mut unlock_targets: Vec<u32> = Vec::new();
    let mut unlock_blueprints: Vec<u16> = Vec::new();
    let mut stat_souls: Vec<u32> = Vec::new();
    let mut stat_fields: Vec<u8> = Vec::new();
    let mut stat_bytes: Vec<u8> = Vec::new();
    let mut stat_deltas: Vec<i8> = Vec::new();

    for effect in &plan.effects {
        match effect {
            Effect::Destroy { card_id } => {
                destroy_ids.push(*card_id);
                // A destroyed stat card decrements its soul's counter.
                if let Some(card) = snap.cards.get(card_id) {
                    if let Some((field, byte)) =
                        pool.content().name_for_packed(card.packed_definition).and_then(stat_slot)
                    {
                        if let Some(soul) = owning_soul(snap, card.owner_id) {
                            stat_souls.push(soul);
                            stat_fields.push(field);
                            stat_bytes.push(byte);
                            stat_deltas.push(-1);
                        }
                    }
                }
            }
            Effect::Create {
                def_key,
                surface,
                macro_zone,
                owner_id,
            } => {
                let packed_def = pool
                    .content()
                    .packed_def(def_key)
                    .ok_or_else(|| format!("create: def {def_key:?} not in DSL content"))?;
                create_defs.push(packed_def);
                create_surfaces.push(*surface);
                create_macro_zones.push(*macro_zone);
                create_owners.push(*owner_id);
                // A created stat card increments its soul's counter.
                if let Some((field, byte)) = stat_slot(def_key) {
                    if let Some(soul) = owning_soul(snap, *owner_id) {
                        stat_souls.push(soul);
                        stat_fields.push(field);
                        stat_bytes.push(byte);
                        stat_deltas.push(1);
                    }
                }
            }
            Effect::CreateDeferred { .. } => {
                return Err(
                    "apply: CreateDeferred (stack.N.create) not yet supported in gateway v1"
                        .to_string(),
                );
            }
            Effect::ModifyTileStock { .. } => { /* applied on the region DB above */ }
            Effect::UnlockBlueprint {
                blueprint_id,
                target_card_id,
            } => {
                unlock_targets.push(*target_card_id);
                unlock_blueprints.push(*blueprint_id);
            }
        }
    }

    call(
        &client,
        pool,
        &cards_db,
        "apply_action",
        json!({
            "now_ms": now_ms,
            "completion_ms": completion_ms,
            "bound_ids": bound_ids,
            "bound_masks": bound_masks,
            "destroy_ids": destroy_ids,
            "create_defs": create_defs,
            "create_surfaces": create_surfaces,
            "create_macro_zones": create_macro_zones,
            "create_owners": create_owners,
            "unlock_targets": unlock_targets,
            "unlock_blueprints": unlock_blueprints,
            "stat_souls": stat_souls,
            "stat_fields": stat_fields,
            "stat_bytes": stat_bytes,
            "stat_deltas": stat_deltas,
        }),
    )
    .await?;

    Ok(())
}

/// POST one reducer call to `db`'s HTTP `/call`. Anonymous — gate-called
/// reducers trust their args, so `ctx.sender` is immaterial. u64 args ride as
/// JSON numbers (lossless server-side).
async fn call(
    client: &reqwest::Client,
    pool: &Pool,
    db: &str,
    reducer: &str,
    args: Value,
) -> Result<(), String> {
    let url = format!("{}/v1/database/{}/call/{}", pool.server_uri(), db, reducer);
    debug!(reducer, %db, "apply call");
    match client.post(&url).json(&args).send().await {
        Ok(resp) if resp.status().is_success() => Ok(()),
        Ok(resp) => {
            let code = resp.status();
            let body = resp.text().await.unwrap_or_default();
            Err(format!("{reducer}: {code}: {body}"))
        }
        Err(err) => Err(format!("{reducer}: {err}")),
    }
}
