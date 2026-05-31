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

use resonantdust_content::packed::micro_loose_cell;
use resonantdust_content::recipe_plan::{ActionPlan, Effect};

use crate::connections::Pool;
use crate::gather::Proposal;

/// Single `cards` shard today (owner-sharding is future work).
const CARDS_SHARD: u16 = 0;

// Hold-kind selectors — must match `cards::gate_api::hold_kind`.
const K_TOUCH: u8 = 0;
const K_SLOT_HOLD: u8 = 1;
const K_SLOT_SHARE: u8 = 2;
const K_POSITION_HOLD: u8 = 3;

/// Materialize `plan` for `proposal` on the cards shard. `now_ms` stamps the
/// hold acquires (which lock the bound cards in place); completion effects, hold
/// releases, and the per-card finalize are future-stamped at
/// `now_ms + plan.duration_ms()`.
pub async fn apply(
    pool: &Pool,
    proposal: &Proposal,
    plan: &ActionPlan,
    now_ms: u64,
) -> Result<(), String> {
    let completion_ms = now_ms + plan.duration_ms();
    let db = pool.config().cards_db(CARDS_SHARD);
    let client = reqwest::Client::new();

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

    // 3. Acquire holds at now_ms (touch always; flavors per plan) — the lock
    //    that pins each bound card at its validated position for the action.
    for (&card_id, kinds) in &plan.holds {
        if card_id == 0 {
            continue;
        }
        acquire_release(&client, pool, &db, "acquire_hold", card_id, now_ms, K_TOUCH).await?;
        if kinds.slot_hold {
            acquire_release(&client, pool, &db, "acquire_hold", card_id, now_ms, K_SLOT_HOLD).await?;
        }
        if kinds.slot_share {
            acquire_release(&client, pool, &db, "acquire_hold", card_id, now_ms, K_SLOT_SHARE)
                .await?;
        }
        if kinds.position_hold {
            acquire_release(&client, pool, &db, "acquire_hold", card_id, now_ms, K_POSITION_HOLD)
                .await?;
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
            }
            Effect::Create {
                def_key,
                surface,
                macro_zone,
                owner_id,
            } => {
                call(
                    &client,
                    pool,
                    &db,
                    "create_card",
                    json!({
                        "time_ms": completion_ms,
                        "def_key": def_key,
                        "surface": surface,
                        "macro_zone": macro_zone,
                        "owner_id": owner_id,
                    }),
                )
                .await?;
            }
            Effect::CreateDeferred { .. } => {
                return Err(
                    "apply: CreateDeferred (stack.N.create) not yet supported in gateway v1"
                        .to_string(),
                );
            }
            Effect::ModifyTileStock { slot, op, delta } => {
                // Cross-DB: the tile lives in the regions zone. Promote+mutate is
                // one regions reducer call; surface/macro_zone/cell come from the
                // proposal (the action's synthetic-tile location).
                let (q, r) = micro_loose_cell(proposal.micro_location);
                let regions_db = pool.regions_db();
                call(
                    &client,
                    pool,
                    &regions_db,
                    "modify_tile_stock",
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
                blueprint_key,
                target_card_id,
            } => {
                // Card-side: set the discovery bit on the soul's SoulPrivate.
                call(
                    &client,
                    pool,
                    &db,
                    "unlock_blueprint",
                    json!({ "target_card_id": target_card_id, "blueprint_key": blueprint_key }),
                )
                .await?;
            }
        }
    }

    // 5. Release holds at completion_ms (mirror of the acquire pass).
    for (&card_id, kinds) in &plan.holds {
        if card_id == 0 {
            continue;
        }
        acquire_release(&client, pool, &db, "release_hold", card_id, completion_ms, K_TOUCH).await?;
        if kinds.slot_hold {
            acquire_release(&client, pool, &db, "release_hold", card_id, completion_ms, K_SLOT_HOLD)
                .await?;
        }
        if kinds.slot_share {
            acquire_release(
                &client,
                pool,
                &db,
                "release_hold",
                card_id,
                completion_ms,
                K_SLOT_SHARE,
            )
            .await?;
        }
        if kinds.position_hold {
            acquire_release(
                &client,
                pool,
                &db,
                "release_hold",
                card_id,
                completion_ms,
                K_POSITION_HOLD,
            )
            .await?;
        }
    }

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

async fn acquire_release(
    client: &reqwest::Client,
    pool: &Pool,
    db: &str,
    reducer: &str,
    card_id: u32,
    time_ms: u64,
    kind: u8,
) -> Result<(), String> {
    call(
        client,
        pool,
        db,
        reducer,
        json!({ "card_id": card_id, "time_ms": time_ms, "kind": kind }),
    )
    .await
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
