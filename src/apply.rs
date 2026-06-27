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
use resonantdust_codec::plan::{ActionPlan, Effect};

use crate::connections::Pool;
use crate::gather::{Proposal, Snapshot};

/// Single `cards` shard today (owner-sharding is future work).

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

    // Per-effect time: an effect stamped at `sys.time = at` (DSL seconds) fires at
    // `now_ms + at*1000`. `at = 0` → now (acquire); `at = duration` → completion.
    let eff_ms = |at: i64| now_ms + (at.max(0) as u64) * 1000;

    // 2. Tile (region DB) — the synthetic tile's GAMEPLAY stock (slots 0/1, the
    //    zone-savable terrain). A tile's runtime holds (claim/touch, schema slots
    //    ≥ 2) have no zone storage, so they're dropped here — tile-hold concurrency
    //    exclusion is a remaining limitation. Applied at completion_ms (the tile's
    //    gameplay mutation is at the action window; t=0 hold writes were filtered).
    const TILE_ZONE_SLOTS: u8 = 2;
    let mut stock_slots: Vec<u8> = Vec::new();
    let mut stock_ops: Vec<u8> = Vec::new();
    let mut stock_deltas: Vec<u8> = Vec::new();
    for te in &plan.effects {
        if let Effect::ModifyTileStock { slot, op, delta } = &te.effect {
            if *slot >= TILE_ZONE_SLOTS {
                continue; // runtime tile hold — no zone slot, dropped
            }
            stock_slots.push(*slot);
            stock_ops.push(op.code());
            stock_deltas.push(*delta);
        }
    }
    if !stock_slots.is_empty() {
        let (q, r) = micro_loose_cell(proposal.micro_location);
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
            0, // tile hold mask — TODO: runtime tile holds (see above)
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
    // Holds-as-stock: bound cards no longer carry a gate-computed hold mask — the
    // recipe writes `data.claim`/`touch`/… via SetCardStock effects. So every
    // bound card's mask is `0` (finalize-only). TODO: the shard still needs the
    // claim/touch STOCK writes to keep a card alive for the action's window and to
    // gate concurrent claims; until the reducer reads those bits, in-flight holds
    // aren't enforced.

    // Effects → parallel arrays. Global aspects (holds / dead / reap) are `LogOp`s
    // (op-log); per-def gameplay/pstyle are `SetCardStock`; spawns are `Create`.
    let mut create_defs: Vec<u16> = Vec::new();
    let mut create_surfaces: Vec<u8> = Vec::new();
    let mut create_macro_zones: Vec<u64> = Vec::new();
    let mut create_owners: Vec<u32> = Vec::new();
    let mut create_stocks: Vec<u64> = Vec::new();
    let mut create_tags: Vec<u8> = Vec::new();
    let mut create_distances: Vec<u16> = Vec::new();
    let mut create_cells: Vec<i64> = Vec::new();
    let mut create_times: Vec<u64> = Vec::new();
    // `move` is retired (no replacement syscall); kept empty for the signature.
    let move_ids: Vec<u32> = Vec::new();
    let move_surfaces: Vec<u8> = Vec::new();
    let move_macro_zones: Vec<u64> = Vec::new();
    let move_owners: Vec<u32> = Vec::new();
    let mut move_distances: Vec<u16> = Vec::new();
    let mut stat_souls: Vec<u32> = Vec::new();
    let mut stat_fields: Vec<u8> = Vec::new();
    let mut stat_bytes: Vec<u8> = Vec::new();
    let mut stat_deltas: Vec<i8> = Vec::new();
    let mut stock_card_ids: Vec<u32> = Vec::new();
    let mut stock_values: Vec<u64> = Vec::new();
    let mut stock_times: Vec<u64> = Vec::new();
    // Op-log deltas for GLOBAL aspects (holds / dead / reap), packed one struct
    // per op (the reducer arg-count ceiling rules out 5 parallel scalar arrays).
    let mut logops: Vec<crate::bindings::shard::LogOpArg> = Vec::new();

    for te in &plan.effects {
        match &te.effect {
            Effect::Create {
                def_key,
                surface,
                macro_zone,
                owner_id,
                stock,
                tag,
                micro_location,
            } => {
                let bundle = pool.content();
                let packed_def = bundle
                    .packed_def(def_key)
                    .ok_or_else(|| format!("create: def {def_key:?} not in DSL content"))?;
                create_defs.push(packed_def);
                create_surfaces.push(*surface);
                create_macro_zones.push(*macro_zone);
                create_owners.push(*owner_id);
                // The full per-card stock u64 — `@define` defaults with any
                // same-plan `&h.data.x set` already folded in by the rules
                // translation, so a created card needs no follow-up SetCardStock.
                create_stocks.push(*stock);
                create_tags.push((*tag).min(u8::MAX as u32) as u8);
                create_cells.push(micro_location.map(|m| m as i64).unwrap_or(-1));
                create_times.push(eff_ms(te.at));
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
            Effect::ModifyTileStock { .. } => { /* applied on the region DB above */ }
            Effect::SetCardStock { card_id, stock } => {
                // Gate-computed absolute new stock for a PER-DEF aspect (gameplay
                // wood/pine, pstyle), future-stamped at the effect's time.
                stock_card_ids.push(*card_id);
                stock_values.push(*stock);
                stock_times.push(eff_ms(te.at));
            }
            Effect::LogOp { card_id, aspect_id, op, modifier } => {
                // Global aspect (holds / dead / reap) op-log delta, future-stamped.
                logops.push(crate::bindings::shard::LogOpArg {
                    card_id: *card_id,
                    aspect_id: *aspect_id,
                    op: *op,
                    modifier: *modifier,
                    time_ms: eff_ms(te.at),
                });
                // A card marked dead (Dead inc) decrements its soul's stat counter
                // — the side-effect the old Destroy path carried.
                if *aspect_id == resonantdust_codec::aspects::StockAspect::Dead.id() && *modifier > 0 {
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
    // Cards being killed this action drive the splice (re-root a destroyed root's
    // members) — derive them from the `Dead` LogOps (was the old `destroy_ids`).
    let dead_ids: Vec<u32> = logops
        .iter()
        .filter(|l| l.aspect_id == resonantdust_codec::aspects::StockAspect::Dead.id() && l.modifier > 0)
        .map(|l| l.card_id)
        .collect();
    for w in resonantdust_state::stack::plan_splice(snap, &dead_ids, now_ms) {
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

    // Disk radius per moved card's destination — same authority as creates, so a
    // relocated card lands on a cell that exists in the target region disk.
    for i in 0..move_ids.len() {
        let mz = resonantdust_codec::packed::pack_macro_zone_full(move_owners[i], move_surfaces[i], 0, 0);
        move_distances.push(crate::gather::region_distance(pool, mz).await);
    }

    let (done_tx, done_rx) = tokio::sync::oneshot::channel::<Result<(), String>>();
    let dispatch = cards_conn.reducers.apply_action_then(
        completion_ms,
        bound_ids,
        create_defs,
        create_surfaces,
        create_macro_zones,
        create_owners,
        create_distances,
        create_stocks,
        create_tags,
        create_cells,
        create_times,
        stat_souls,
        stat_fields,
        stat_bytes,
        stat_deltas,
        stock_card_ids,
        stock_values,
        stock_times,
        reroot_ids,
        reroot_macro_zones,
        reroot_micro_locations,
        reroot_stack_states,
        move_ids,
        move_surfaces,
        move_macro_zones,
        move_owners,
        move_distances,
        logops,
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

