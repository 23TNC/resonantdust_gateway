//! propose_action — the gate IS the recipe validator.
//!
//! A `propose_action` no longer relays to a single database; it can't, because a
//! recipe spans `cards` and `regions`. Instead the gate gathers the operating
//! set, validates the recipe over that snapshot (input predicates + stack/world
//! binding checks), computes the plan, and applies it via narrow reducer calls
//! across the shards. Card-only effects (create/destroy/move/holds/dedup) today;
//! world-terrain effects are rejected by the content planner.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::Value;
use tokio::sync::mpsc::UnboundedSender;
use tracing::{debug, warn};

use resonantdust_content::recipe_core::recipe;
use resonantdust_content::recipe_plan::plan_output;
use resonantdust_content::recipe_validate::validate_bindings;

use crate::apply;
use crate::connections::Pool;
use crate::gather::{gather, synthetic_tile, Proposal};
use crate::protocol::GateMsg;
use crate::validation::validate;

/// Handle a `propose_action` call: run the pipeline and reply CallOk/CallErr.
pub async fn handle(pool: &Arc<Pool>, tx: &UnboundedSender<String>, cid: u32, args: Value) {
    let reply = match propose(pool, args).await {
        Ok(()) => GateMsg::CallOk { cid },
        Err(error) => {
            warn!(cid, %error, "propose_action rejected");
            GateMsg::CallErr { cid, error }
        }
    };
    let _ = tx.send(reply.to_json());
}

async fn propose(pool: &Pool, args: Value) -> Result<(), String> {
    let proposal = Proposal {
        recipe_id: get_u64(&args, "recipe_id")? as u16,
        surface: get_u64(&args, "surface")? as u8,
        macro_zone: get_u64(&args, "macro_zone")?,
        micro_location: get_u64(&args, "micro_location")? as u32,
        root: get_u64(&args, "root")? as u32,
        bindings: get_bindings(&args)?,
    };
    let caller_player_id = get_u64(&args, "caller_player_id")? as u32;
    let client_time_ms = get_u64(&args, "client_time_ms")?;
    let now_ms = effective_now_ms(client_time_ms);
    debug!(
        recipe_id = proposal.recipe_id,
        root = proposal.root,
        caller_player_id,
        now_ms,
        "propose_action"
    );

    // Gather the operating set across the cards/regions shards.
    let snap = gather(pool, &proposal)
        .await
        .map_err(|e| format!("gather: {e}"))?;

    let recipe = recipe(proposal.recipe_id)
        .map_err(|e| format!("recipe registry: {e}"))?
        .ok_or_else(|| format!("unknown recipe id {}", proposal.recipe_id))?;

    // Derive the synthetic tile (branch-0 slot) from the gathered zone at the
    // action cell — `None` for non-tile recipes (harmless).
    let synthetic = synthetic_tile(&snap, proposal.micro_location);

    // Validate: input predicates (recipe shape) + stack/world binding checks
    // (existence, not-dead, holds, ownership, magnetic, dup).
    validate(&snap, &proposal, synthetic, now_ms)?;
    validate_bindings(
        &snap,
        recipe,
        proposal.recipe_id,
        proposal.root,
        &proposal.bindings,
        caller_player_id,
        now_ms,
    )?;

    // Plan the output tape into effects + holds + duration.
    let plan = plan_output(&snap, recipe, &proposal.bindings, proposal.root, synthetic, now_ms)?;

    // Apply across the shards (future-stamped at completion).
    apply::apply(pool, &proposal, &plan, now_ms).await
}

/// Resolve the action's `now_ms`: the client's clock, clamped to not exceed the
/// gate's wall clock. The client renders on a buffered clock behind true server
/// time, so stamping at the client's value lands new rows on the client's
/// timeline (matches `shard::cards::effective_now_ms`'s `min(client, server)`).
fn effective_now_ms(client_time_ms: u64) -> u64 {
    let server = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(client_time_ms);
    client_time_ms.min(server)
}

fn get_u64(args: &Value, key: &str) -> Result<u64, String> {
    args.get(key)
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("propose: missing or non-numeric arg {key:?}"))
}

fn get_bindings(args: &Value) -> Result<Vec<Vec<u32>>, String> {
    let outer = args
        .get("bindings")
        .and_then(Value::as_array)
        .ok_or_else(|| "propose: `bindings` must be an array".to_string())?;
    outer
        .iter()
        .map(|row| {
            let inner = row
                .as_array()
                .ok_or_else(|| "propose: each binding row must be an array".to_string())?;
            inner
                .iter()
                .map(|v| {
                    v.as_u64()
                        .map(|n| n as u32)
                        .ok_or_else(|| "propose: binding entries must be numbers".to_string())
                })
                .collect::<Result<Vec<u32>, String>>()
        })
        .collect()
}
