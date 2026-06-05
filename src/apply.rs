//! Apply — decompose a validated [`ActionPlan`] into the narrow reducer calls
//! that materialize it on the `cards` shard.
//!
//! No cross-DB transaction exists, so this is a best-effort sequence of
//! idempotent, future-stamped `/call`s; the dedup gate (`claim_pending`) guards
//! exact-duplicate submission and the per-card holds guard conflicting actions.
//! Ordering mirrors `shard::propose_action`: dedup → chain-stitch (now) →
//! acquire holds (now) → completion effects (future) → release holds (future).
//!
//! Card-only v1: `Effect::CreateDeferred` (stack.N.create) and the world-terrain
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

// Hold-kind selectors — must match `cards::gate_api::hold_kind`.
const K_TOUCH: u8 = 0;
const K_SLOT_HOLD: u8 = 1;
const K_SLOT_SHARE: u8 = 2;
const K_POSITION_HOLD: u8 = 3;

/// `soul` card_type nibble (content/cards/types.json). The owning-soul walk
/// stops at the first owner of this type.
const SOUL_CARD_TYPE: u8 = 6;

/// Soul-stat slot for a stat-card name → `(field, byte_index)` where field is
/// the `set_soul_stat` selector (0=stats, 1=fatigued, 2=injured) and byte is
/// corpus=0/anima=1/sollertia=2/aether=3. The gate owns this mapping now (it was
/// the cards module's `stat_map`). `None` for non-stat cards.
fn stat_slot(name: &str) -> Option<(u8, u8)> {
    let (field, base) = match name.strip_suffix("_dim") {
        Some(b) => (1u8, b),      // fatigued
        None => (0u8, name),      // stats
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

/// Materialize `plan` for `proposal` on the cards shard. `now_ms` stamps the
/// hold acquires (which lock the bound cards in place); completion effects, hold
/// releases, and the per-card finalize are future-stamped at
/// `now_ms + plan.duration_ms()`.
pub async fn apply(
    pool: &Pool,
    snap: &Snapshot,
    proposal: &Proposal,
    plan: &ActionPlan,
    now_ms: u64,
) -> Result<(), String> {
    let completion_ms = now_ms + plan.duration_ms();
    let db = pool.config().cards_db(CARDS_SHARD);
    let client = crate::connections::http_client().clone();

    // 1. Dedup gate — reject duplicates before any writes land.
    call(
        &client,
        pool,
        &db,
        "claim_pending",
        json!({
            "recipe_id": proposal.recipe_id,
            "root": proposal.root,
            "bindings": proposal.bindings,
            "completion_ms": completion_ms,
        }),
    )
    .await?;

    // 2. We do NOT re-write input positions. The bound stack is canonical —
    //    it's exactly the configuration the recipe validated against — so we
    //    lock the cards where they already are via the holds below. (The
    //    monolith's chain-stitch re-indexed members by their binding *offset*,
    //    which is a recipe slot, not a stack position, and would relocate a
    //    player-placed card. Output positioning is a separate concern, handled
    //    by the Create effects.)

    // 2b. Promote + LEASE the action's tile when the recipe targets a synthetic
    //     tile: a single `acquire_tile_lease` takes the hold at now_ms AND writes
    //     its release at completion_ms atomically. An exclusive slot_hold already
    //     held → the reducer rejects → this apply aborts → the client gets
    //     `call_err`. That is the multi-gate concurrent-action guard; the lease
    //     means any leases already taken self-expire at completion_ms, so no
    //     rollback is needed on a fail-fast abort.
    if let Some(kinds) = &plan.tile_holds {
        tile_lease(&client, pool, proposal, kinds, now_ms, completion_ms).await?;
    }

    // 3. LEASE holds at now_ms→completion_ms (touch always; flavors per plan).
    //    Each `acquire_lease` is a self-expiring lock: it acquires now and writes
    //    its own release at completion, atomically, and the exclusive verbs
    //    check-and-set (reject if already held) — so two gates racing the same
    //    card resolve to one winner, the loser fail-fast aborts here.
    for (&card_id, kinds) in &plan.holds {
        if card_id == 0 {
            continue;
        }
        card_lease(&client, pool, &db, card_id, K_TOUCH, now_ms, completion_ms).await?;
        if kinds.slot_hold {
            card_lease(&client, pool, &db, card_id, K_SLOT_HOLD, now_ms, completion_ms).await?;
        }
        if kinds.slot_share {
            card_lease(&client, pool, &db, card_id, K_SLOT_SHARE, now_ms, completion_ms).await?;
        }
        if kinds.position_hold {
            card_lease(&client, pool, &db, card_id, K_POSITION_HOLD, now_ms, completion_ms).await?;
        }
    }

    // 4. Completion effects, future-stamped at completion_ms.
    for effect in &plan.effects {
        match effect {
            Effect::Destroy { card_id } => {
                call(
                    &client,
                    pool,
                    &db,
                    "destroy_card",
                    json!({ "card_id": card_id, "time_ms": completion_ms }),
                )
                .await?;
                // Soul-stats (gate-owned): a destroyed stat card decrements its
                // soul's counter. Resolve name → slot → owning soul from the snap.
                if let Some(card) = snap.cards.get(card_id) {
                    if let Some((field, byte)) =
                        pool.content().name_for_packed(card.packed_definition).and_then(stat_slot)
                    {
                        if let Some(soul) = owning_soul(snap, card.owner_id) {
                            soul_stat(&client, pool, &db, soul, field, byte, -1, completion_ms).await?;
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
                // The gate resolves the def name → packed_def from its Bundle;
                // the module just stores the opaque id (content-agnostic).
                let packed_def = pool
                    .content()
                    .packed_def(def_key)
                    .ok_or_else(|| format!("create: def {def_key:?} not in DSL content"))?;
                call(
                    &client,
                    pool,
                    &db,
                    "create_card",
                    json!({
                        "time_ms": completion_ms,
                        "packed_def": packed_def,
                        "surface": surface,
                        "macro_zone": macro_zone,
                        "owner_id": owner_id,
                    }),
                )
                .await?;
                // Soul-stats (gate-owned): a created stat card increments its
                // soul's counter.
                if let Some((field, byte)) = stat_slot(def_key) {
                    if let Some(soul) = owning_soul(snap, *owner_id) {
                        soul_stat(&client, pool, &db, soul, field, byte, 1, completion_ms).await?;
                    }
                }
            }
            Effect::CreateDeferred { .. } => {
                return Err(
                    "apply: CreateDeferred (stack.N.create) not yet supported in gateway v1"
                        .to_string(),
                );
            }
            Effect::ModifyTileStock { slot, op, delta } => {
                // Cross-DB: the tile lives in the regions zone. It was already
                // promoted + locked up front (step 2b), so this only mutates its
                // stock, future-stamped at completion. (set_tile_stock still
                // find-or-creates defensively.) Cell comes from the proposal.
                let (q, r) = micro_loose_cell(proposal.micro_location);
                let regions_db = pool.regions_db();
                call(
                    &client,
                    pool,
                    &regions_db,
                    "set_tile_stock",
                    json!({
                        "time_ms": completion_ms,
                        "surface": proposal.surface,
                        "macro_zone": proposal.macro_zone,
                        "q": q,
                        "r": r,
                        "slot": slot,
                        "op": op.code(),
                        "delta": delta,
                    }),
                )
                .await?;
            }
            Effect::UnlockBlueprint {
                blueprint_id,
                target_card_id,
            } => {
                // Card-side: set the discovery bit on the soul's SoulPrivate.
                // blueprint_id is the Bundle id (resolved at plan translation).
                call(
                    &client,
                    pool,
                    &db,
                    "unlock_blueprint",
                    json!({ "target_card_id": target_card_id, "blueprint_id": blueprint_id }),
                )
                .await?;
            }
        }
    }

    // 5. Releases are NOT a separate pass — each lease in steps 2b/3 already
    //    wrote its own release at completion_ms (self-expiring lock). Once a
    //    tile-card is hold-free and clean, the regions GC sweep demotes it.

    // 6. Finalize every bound card at completion_ms: clear pos_need/pos_want and
    //    stamp progress_style (0 = no bar) so the actor's progress bar renders on
    //    its completion row. Root + all bindings, deduped. Composes with the
    //    dead/hold-release writes already made at completion_ms.
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
    for &card_id in &bound {
        let style = plan.styles.get(&card_id).copied().unwrap_or(0);
        call(
            &client,
            pool,
            &db,
            "finalize_card",
            json!({ "card_id": card_id, "time_ms": completion_ms, "progress_style": style }),
        )
        .await?;
    }

    // Note: `release_pending` is NOT called — the dedup row persists until
    // `completion_ms` and is reaped by the cards GC, so a resubmit of the same
    // tuple is rejected for the action's full lifetime.
    Ok(())
}

/// Push a soul-stat delta to the cards shard (`set_soul_stat`) — the gate-owned
/// soul-stats path. `field`: 0=stats, 1=fatigued, 2=injured.
#[allow(clippy::too_many_arguments)]
async fn soul_stat(
    client: &reqwest::Client,
    pool: &Pool,
    db: &str,
    soul_card_id: u32,
    field: u8,
    byte_index: u8,
    delta: i8,
    time_ms: u64,
) -> Result<(), String> {
    call(
        client,
        pool,
        db,
        "set_soul_stat",
        json!({
            "soul_card_id": soul_card_id,
            "field": field,
            "byte_index": byte_index,
            "delta": delta,
            "time_ms": time_ms,
        }),
    )
    .await
}

/// Take a self-expiring lease of hold `kind` on a `cards`-shard card:
/// `acquire_lease` acquires at `acquire_ms` and writes the release at
/// `release_ms` in one transaction, rejecting (→ caller's `?` aborts) if an
/// exclusive kind is already held.
async fn card_lease(
    client: &reqwest::Client,
    pool: &Pool,
    db: &str,
    card_id: u32,
    kind: u8,
    acquire_ms: u64,
    release_ms: u64,
) -> Result<(), String> {
    call(
        client,
        pool,
        db,
        "acquire_lease",
        json!({ "card_id": card_id, "kind": kind, "acquire_ms": acquire_ms, "release_ms": release_ms }),
    )
    .await
}

/// Lease the action's **synthetic-tile** holds on the regions tile-card at the
/// proposal's cell — position-keyed, so the gate never needs the tile-card's id.
/// `acquire_tile_lease` promotes + holds at `acquire_ms` and writes the release
/// at `release_ms`; an exclusive slot_hold already held → reject → caller aborts.
async fn tile_lease(
    client: &reqwest::Client,
    pool: &Pool,
    proposal: &Proposal,
    kinds: &HoldKinds,
    acquire_ms: u64,
    release_ms: u64,
) -> Result<(), String> {
    let (q, r) = micro_loose_cell(proposal.micro_location);
    let db = pool.regions_db();
    let base = |kind: u8| {
        json!({
            "surface": proposal.surface,
            "macro_zone": proposal.macro_zone,
            "q": q,
            "r": r,
            "kind": kind,
            "acquire_ms": acquire_ms,
            "release_ms": release_ms,
        })
    };
    let kind = if kinds.slot_hold { K_SLOT_HOLD } else { K_SLOT_SHARE };
    call(client, pool, &db, "acquire_tile_lease", base(kind)).await?;
    if kinds.position_hold {
        call(client, pool, &db, "acquire_tile_lease", base(K_POSITION_HOLD)).await?;
    }
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
