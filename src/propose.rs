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

use resonantdust_data::recipe_state::validate_bindings;

use crate::apply;
use crate::connections::Pool;
use crate::gather::{gather, synthetic_tile, Proposal};
use resonantdust_rules::dsl_recipe;
use resonantdust_data::protocol::GateMsg;

/// Handle a `propose_action` call: run the pipeline and reply CallOk/CallErr.
pub async fn handle(pool: &Arc<Pool>, tx: &UnboundedSender<String>, cid: u32, args: Value) {
    let reply = match propose(pool, args).await {
        Ok(()) => GateMsg::call_ok(cid),
        Err(error) => {
            warn!(cid, %error, "propose_action rejected");
            GateMsg::call_err(cid, error)
        }
    };
    let _ = tx.send(reply);
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
    let now_ms = effective_now_ms(client_time_ms)?;
    debug!(
        recipe_id = proposal.recipe_id,
        root = proposal.root,
        bindings = ?proposal.bindings,
        caller_player_id,
        now_ms,
        "propose_action"
    );

    // Gather the operating set across the cards/regions shards.
    let snap = gather(pool, &proposal)
        .await
        .map_err(|e| format!("gather: {e}"))?;

    let bundle = pool.content();

    // The recipe NAME (Bundle id space) keys the DSL recipe. No legacy registry.
    let recipe_name = bundle
        .recipe_name(proposal.recipe_id)
        .ok_or_else(|| format!("unknown recipe id {}", proposal.recipe_id))?
        .to_string();

    // Derive the synthetic tile (branch-0 slot) from the gathered zone at the
    // action cell — `None` for non-tile recipes (harmless).
    let synthetic = synthetic_tile(&snap, proposal.micro_location);

    // Match @input + plan @output on the DSL vm, translated to the ActionPlan
    // `apply` consumes. The match step replaces the legacy `validate_input`, and
    // the plan's per-card holds drive the `wants_exclusive` gate below.
    let plan = dsl_recipe::run(
        &bundle,
        &snap,
        &recipe_name,
        proposal.root,
        &proposal.bindings,
        synthetic,
        now_ms,
    )?;

    // State validation (orthogonal to recipe semantics): existence, not-dead,
    // holds, ownership, dup. `wants_exclusive` comes from the plan's per-card
    // `slot_hold`.
    validate_bindings(
        &snap,
        proposal.recipe_id,
        proposal.root,
        &proposal.bindings,
        caller_player_id,
        now_ms,
        |card_id| plan.holds.get(&card_id).is_some_and(|h| h.slot_hold),
    )?;

    // Apply across the shards (future-stamped at completion).
    apply::apply(pool, &snap, &proposal, &plan, now_ms).await
}

/// Backward grace: reject a proposal whose `client_time_ms` is more than this
/// behind the gate's wall clock. Mirrors `shard::cards::BACKWARD_GRACE_MS`.
///
/// Why it matters for action fault-tolerance: outputs are forward-stamped at
/// `now_ms + duration`. If the gate processes a proposal late (slow / stalled),
/// stamping at the (now-stale) `client_time` would land the output in the
/// client's past. Rejecting the stale proposal — instead of applying it — lets
/// the client's fresh resend (a new `client_time`) re-stamp the start into the
/// future. The dedup (`claim_pending`) then blocks the duplicate once one
/// attempt lands, so retries never double-apply (no re-stamp / undo needed).
const BACKWARD_GRACE_MS: u64 = 10_000;

/// Forward grace: reject a proposal whose `client_time_ms` is more than this
/// AHEAD of the gate's wall clock. The client deliberately runs on a buffered
/// clock BEHIND true server time (`client_delay` ≥ 1.5s), so any meaningful
/// "ahead" is clock skew/a bug — surface it (the client's `correct_from_drift`
/// re-seats and resends) rather than silently clamping, which would land
/// completion rows at unexpected times. Small grace absorbs extrapolation jitter.
const FORWARD_GRACE_MS: u64 = 1_000;

/// Resolve the action's `now_ms`: the client's clock, **rejected if too far in
/// the past OR future**. The client runs behind true server time, so stamping at
/// its value lands new rows on the client's timeline; we no longer clamp (the
/// user found clamping yields unexpected results — a future stamp is a real
/// signal to reject, not hide).
fn effective_now_ms(client_time_ms: u64) -> Result<u64, String> {
    let server = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(client_time_ms);
    let behind = server.saturating_sub(client_time_ms);
    if behind > BACKWARD_GRACE_MS {
        return Err(format!(
            "time_drift:client_behind_by={behind} (server={server}, client={client_time_ms}) — stale proposal, resend"
        ));
    }
    let ahead = client_time_ms.saturating_sub(server);
    if ahead > FORWARD_GRACE_MS {
        return Err(format!(
            "time_drift:client_ahead_by={ahead} (server={server}, client={client_time_ms}) — future timestamp, resend"
        ));
    }
    Ok(client_time_ms)
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
