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
use std::sync::Arc;

use resonantdust_codec::packed::micro_loose_cell;
use resonantdust_codec::plan::{ActionPlan, Effect, HoldKinds};

use crate::connections::Pool;
use crate::gather::{Proposal, Snapshot};

/// Single `cards` shard today (owner-sharding is future work).

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
    cards: Option<&Arc<crate::bindings::shard::DbConnection>>,
    regions: Option<&Arc<crate::bindings::shard::DbConnection>>,
    snap: &Snapshot,
    proposal: &Proposal,
    plan: &ActionPlan,
    now_ms: u64,
) -> Result<(), String> {
    use crate::bindings::shard::{apply_action, apply_action_tile, claim_pending};
    let completion_ms = now_ms + plan.duration_ms();
    let cards_conn = cards.ok_or_else(|| "apply: cards upstream not connected".to_string())?;

    // 1. Dedup gate — gate-only (`pending_actions` is never relayed to clients).
    {
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<Result<(), String>>();
        let dispatch = cards_conn.reducers.claim_pending_then(
            proposal.recipe_id,
            proposal.root,
            proposal.bindings.clone(),
            completion_ms,
            move |_ctx, res| {
                let _ = done_tx.send(res.unwrap_or_else(|e| Err(format!("internal: {e}"))));
            },
        );
        await_result("claim_pending", dispatch, done_rx).await?;
    }

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
        let regions_conn =
            regions.ok_or_else(|| "apply: regions upstream not connected".to_string())?;
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<Result<(), String>>();
        let dispatch = regions_conn.reducers.apply_action_tile_then(
            now_ms,
            completion_ms,
            proposal.surface,
            proposal.macro_zone,
            q,
            r,
            tile_hold_mask(kinds),
            stock_slots,
            stock_ops,
            stock_deltas,
            move |_ctx, res| {
                let _ = done_tx.send(res.unwrap_or_else(|e| Err(format!("internal: {e}"))));
            },
        );
        await_result("apply_action_tile", dispatch, done_rx).await?;
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
    let mut create_stocks: Vec<u32> = Vec::new();
    // Per-created-card transient tag (0 = none): set when a sibling create nests
    // in this card, so the shard can register `tag -> minted id`.
    let mut create_tags: Vec<u8> = Vec::new();
    // Per-product container disk radius, so a recipe output lands in a cell that
    // EXISTS in its target region disk (mirrors create_card's `distance`).
    let mut create_distances: Vec<u16> = Vec::new();
    let mut stat_souls: Vec<u32> = Vec::new();
    let mut stat_fields: Vec<u8> = Vec::new();
    let mut stat_bytes: Vec<u8> = Vec::new();
    let mut stat_deltas: Vec<i8> = Vec::new();
    let mut stock_card_ids: Vec<u32> = Vec::new();
    let mut stock_values: Vec<u32> = Vec::new();

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
                stock,
                tag,
            } => {
                let bundle = pool.content();
                let packed_def = bundle
                    .packed_def(def_key)
                    .ok_or_else(|| format!("create: def {def_key:?} not in DSL content"))?;
                create_defs.push(packed_def);
                create_surfaces.push(*surface);
                create_macro_zones.push(*macro_zone);
                create_owners.push(*owner_id);
                // The full per-card stock u32 — `@define` defaults with any
                // same-plan `&handle.aspect.x set` already folded in by the rules
                // translation, so a created card needs no follow-up SetCardStock.
                create_stocks.push(*stock);
                create_tags.push((*tag).min(u8::MAX as u32) as u8);
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
            Effect::SetCardStock { card_id, stock } => {
                // Gate-computed absolute new stock for the card (write @completion).
                stock_card_ids.push(*card_id);
                stock_values.push(*stock);
            }
        }
    }

    // Stack splice: a destroyed card that is a stack ROOT orphans its members
    // (they point to it via `micro_location`). The SHARED `stack::plan_splice`
    // (destroy-side sibling of `plan_place`, over the snapshot's `StackStore`)
    // re-roots them; we just map its `Write`s to the reducer's parallel arrays.
    // Folded into `apply_action`'s per-card completion rows (no extra writes), so a
    // member that's also bound (held + finalized @completion) coalesces.
    let mut reroot_ids: Vec<u32> = Vec::new();
    let mut reroot_macro_zones: Vec<u64> = Vec::new();
    let mut reroot_micro_locations: Vec<u32> = Vec::new();
    let mut reroot_stack_states: Vec<u8> = Vec::new();
    for w in resonantdust_state::stack::plan_splice(snap, &destroy_ids, now_ms) {
        let (micro_location, flags) = w.micro.apply(0);
        reroot_ids.push(w.card_id);
        reroot_macro_zones.push(w.macro_zone);
        reroot_micro_locations.push(micro_location);
        reroot_stack_states.push((flags & resonantdust_codec::card_model::placement_mask()) as u8);
    }

    // Disk radius per created card — authoritative (reads the owner card like
    // `ensure_region`/`create_card`), so recipe outputs only land on cells that
    // exist in the target region disk. The gather snapshot isn't a reliable
    // source (the product's owner soul isn't always in it).
    for i in 0..create_owners.len() {
        // A tag owner is a card created in THIS batch — its region doesn't exist
        // to query yet; the shard places it unbounded into the freshly-minted
        // owner's (empty) inventory. Real owners resolve their disk radius here.
        if resonantdust_codec::packed::is_tag(create_owners[i]) {
            create_distances.push(u16::MAX);
            continue;
        }
        let mz = resonantdust_codec::packed::pack_macro_zone_full(create_owners[i], create_surfaces[i], 0, 0);
        create_distances.push(crate::gather::region_distance(pool, mz).await);
    }

    let (done_tx, done_rx) = tokio::sync::oneshot::channel::<Result<(), String>>();
    let dispatch = cards_conn.reducers.apply_action_then(
        now_ms,
        completion_ms,
        bound_ids,
        bound_masks,
        destroy_ids,
        create_defs,
        create_surfaces,
        create_macro_zones,
        create_owners,
        create_distances,
        create_stocks,
        create_tags,
        stat_souls,
        stat_fields,
        stat_bytes,
        stat_deltas,
        stock_card_ids,
        stock_values,
        reroot_ids,
        reroot_macro_zones,
        reroot_micro_locations,
        reroot_stack_states,
        move |_ctx, res| {
            let _ = done_tx.send(res.unwrap_or_else(|e| Err(format!("internal: {e}"))));
        },
    );
    await_result("apply_action", dispatch, done_rx).await?;

    Ok(())
}

/// Await a gate-driven reducer's completion, returning its result for `?`
/// propagation (these apply steps are sequential — each must commit before the
/// next). `dispatch` is the SDK send result; the `_then` callback resolves
/// `done_rx` with the reducer's own `Result`.
async fn await_result<E: std::fmt::Display>(
    reducer: &str,
    dispatch: Result<(), E>,
    done_rx: tokio::sync::oneshot::Receiver<Result<(), String>>,
) -> Result<(), String> {
    dispatch.map_err(|e| format!("{reducer} dispatch: {e}"))?;
    match tokio::time::timeout(std::time::Duration::from_secs(5), done_rx).await {
        Ok(Ok(r)) => r.map_err(|e| format!("{reducer}: {e}")),
        Ok(Err(_)) => Err(format!("{reducer}: completion callback dropped")),
        Err(_) => Err(format!("{reducer}: reducer timed out")),
    }
}

