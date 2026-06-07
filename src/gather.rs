//! Gather — assemble the in-memory snapshot a recipe needs to validate.
//!
//! Given a proposal (recipe + card_ids + location), route each card to its
//! `cards` shard and the location to its `regions` shard (via `regionindex`),
//! subscribe the needed rows, wait for the subscriptions to apply, and read the
//! latest version of each row into a [`Snapshot`]. Validation (W4) runs over
//! the snapshot; apply (W7) writes the outcome back.
//!
//! The SDK runs each connection's message loop on its own thread and fires the
//! `on_applied` callback there; [`sub_await!`] bridges that to async via a
//! oneshot so the gather can `await` each subscription.
//!
//! Cache note: subscriptions stay active on the connection after we read (the
//! gate runs warm). Reads always collapse to the latest version per key, so a
//! growing cache stays correct; explicit unsubscribe/eviction is a later
//! concern (W7).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use resonantdust_data::card_model;
use resonantdust_data::packed::{
    micro_loose_cell, pack_definition, tile_full, unpack_definition, unpack_zone_definition,
    valid_at_time,
};

/// `card_type` of a promoted tile-card. Mirrors `regions::cards::TILE_CARD_TYPE`.
const TILE_CARD_TYPE: u8 = 7;
use spacetimedb_sdk::{DbContext, Table};
use tracing::debug;

use crate::bindings;
use crate::connections::Pool;
use crate::routing;

/// How long to wait for a subscription to apply before giving up.
const SUB_TIMEOUT: Duration = Duration::from_secs(5);

/// A recipe proposal — the `propose_action` arguments the gate validates.
#[derive(Debug, Clone)]
pub struct Proposal {
    pub recipe_id: u16,
    pub surface: u8,
    pub macro_zone: u64,
    pub micro_location: u32,
    pub root: u32,
    pub bindings: Vec<Vec<u32>>,
}

impl Proposal {
    /// All real card ids the proposal names (root + bindings, minus the `0`
    /// synthetic-tile sentinel), deduped.
    pub fn card_ids(&self) -> Vec<u32> {
        let mut ids: Vec<u32> = self
            .bindings
            .iter()
            .flatten()
            .copied()
            .filter(|&id| id != 0)
            .collect();
        if self.root != 0 {
            ids.push(self.root);
        }
        ids.sort_unstable();
        ids.dedup();
        ids
    }
}

/// The rows a recipe needs, read at their latest version.
#[derive(Default)]
pub struct Snapshot {
    /// Every card in the operating set, keyed by `card_id` (latest version).
    pub cards: HashMap<u32, bindings::shard::Card>,
    /// The zone the action lands in, if it exists.
    pub zone: Option<bindings::shard::Zone>,
    /// A promoted tile-card at the action cell (regions DB), if one exists. Takes
    /// priority over the zone slot when deriving the synthetic tile, so a
    /// repeated action reads the live (decremented / held) stock rather than the
    /// stale zone bytes (the zone only catches up on GC demotion).
    pub tile_card: Option<bindings::shard::Card>,
    /// The region containing that zone, if it exists.
    pub region: Option<bindings::shard::Region>,
    /// The region's per-`data_shard` card-shard ref counts (latest each).
    pub card_shards: Vec<bindings::shard::CardShard>,
    /// Which `regions` shard holds the region, if assigned.
    pub region_shard: Option<bindings::regionindex::RegionShard>,
}

#[derive(Debug)]
pub enum GatherError {
    /// A required upstream connection could not be established.
    NoConnection(String),
    /// A subscription did not apply within [`SUB_TIMEOUT`].
    Timeout(String),
    /// The subscription's result channel was dropped before applying.
    Dropped(String),
    /// The subscription reported an error.
    Subscription(String),
}

impl std::fmt::Display for GatherError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GatherError::NoConnection(s) => write!(f, "no upstream connection: {s}"),
            GatherError::Timeout(s) => write!(f, "subscription timed out: {s}"),
            GatherError::Dropped(s) => write!(f, "subscription channel dropped: {s}"),
            GatherError::Subscription(s) => write!(f, "subscription error: {s}"),
        }
    }
}

/// Stamp an `async fn $fn(conn, queries) -> Result<(), GatherError>` that
/// subscribes the given queries on `$module`'s connection and awaits the
/// `on_applied` (or `on_error`) callback via a oneshot.
macro_rules! sub_await {
    ($fn:ident, $module:ident, $label:literal) => {
        async fn $fn(
            conn: &bindings::$module::DbConnection,
            queries: Vec<String>,
        ) -> Result<(), GatherError> {
            let label = $label;
            let (tx, rx) = tokio::sync::oneshot::channel::<Result<(), String>>();
            // Either on_applied or on_error fires once; whichever wins sends.
            let tx = Arc::new(Mutex::new(Some(tx)));
            let tx_ok = tx.clone();
            let tx_err = tx.clone();
            conn.subscription_builder()
                .on_applied(move |_ctx| {
                    if let Some(t) = tx_ok.lock().unwrap().take() {
                        let _ = t.send(Ok(()));
                    }
                })
                .on_error(move |_ctx, err| {
                    if let Some(t) = tx_err.lock().unwrap().take() {
                        let _ = t.send(Err(err.to_string()));
                    }
                })
                .subscribe(queries);

            match tokio::time::timeout(SUB_TIMEOUT, rx).await {
                Err(_) => Err(GatherError::Timeout(label.to_string())),
                Ok(Err(_)) => Err(GatherError::Dropped(label.to_string())),
                Ok(Ok(Err(msg))) => Err(GatherError::Subscription(msg)),
                Ok(Ok(Ok(()))) => Ok(()),
            }
        }
    };
}

// Both data upstreams run the unified `shard` module, so they use the `shard`
// bindings; the "cards"/"regions" labels stay for accurate gather diagnostics.
sub_await!(cards_sub, shard, "cards");
sub_await!(regions_sub, shard, "regions");
sub_await!(regionindex_sub, regionindex, "regionindex");

/// Build the snapshot for `proposal`.
pub async fn gather(pool: &Pool, proposal: &Proposal) -> Result<Snapshot, GatherError> {
    let mut snap = Snapshot::default();

    gather_cards(pool, proposal, &mut snap).await?;
    gather_region(pool, proposal, &mut snap).await?;

    debug!(
        cards = snap.cards.len(),
        zone = snap.zone.is_some(),
        region = snap.region.is_some(),
        card_shards = snap.card_shards.len(),
        region_shard = snap.region_shard.is_some(),
        "gather complete"
    );
    Ok(snap)
}

/// Two-phase card gather: per shard, read the named cards to learn their
/// owners, then subscribe by owner to pull the whole operating set (stacks,
/// equipment, inventory siblings).
async fn gather_cards(
    pool: &Pool,
    proposal: &Proposal,
    snap: &mut Snapshot,
) -> Result<(), GatherError> {
    let mut by_shard: HashMap<u16, Vec<u32>> = HashMap::new();
    for id in proposal.card_ids() {
        by_shard.entry(routing::card_shard(id)).or_default().push(id);
    }

    for (shard, ids) in by_shard {
        let conn = pool
            .cards(shard)
            .ok_or_else(|| GatherError::NoConnection(format!("cards-{shard}")))?;

        // Phase 1: fetch the named cards to learn their owners.
        let q1 = ids
            .iter()
            .map(|id| format!("SELECT * FROM cards WHERE card_id = {id}"))
            .collect();
        cards_sub(&conn, q1).await?;

        let id_set: HashSet<u32> = ids.iter().copied().collect();
        let owners: HashSet<u32> = latest_cards(&conn)
            .into_iter()
            .filter(|c| id_set.contains(&c.card_id))
            .map(|c| c.owner_id)
            .collect();

        // Phase 2: subscribe by owner to gather the full operating set.
        if !owners.is_empty() {
            let q2 = owners
                .iter()
                .map(|o| format!("SELECT * FROM cards WHERE owner_id = {o}"))
                .collect();
            cards_sub(&conn, q2).await?;
        }

        for c in latest_cards(&conn) {
            snap.cards.insert(c.card_id, c);
        }
    }
    Ok(())
}

/// Region gather: derive the region from the zone's macro_zone, look up which
/// `regions` shard holds it via `regionindex`, then pull the zone, region, and
/// card-shard hints from that shard.
async fn gather_region(
    pool: &Pool,
    proposal: &Proposal,
    snap: &mut Snapshot,
) -> Result<(), GatherError> {
    let (macro_region, _bit) = routing::region_of_zone(proposal.macro_zone);

    let rix = pool
        .regions_index()
        .ok_or_else(|| GatherError::NoConnection("regionindex".to_string()))?;
    regionindex_sub(
        &rix,
        vec![format!(
            "SELECT * FROM region_shards WHERE macro_region = {macro_region}"
        )],
    )
    .await?;

    snap.region_shard = {
        use bindings::regionindex::region_shards_table::RegionShardsTableAccess;
        rix.db()
            .region_shards()
            .iter()
            .find(|r| r.macro_region == macro_region)
    };

    // Single regions shard today: if the index has no entry for this region,
    // default to shard 0 so world/tile zones are still reachable. (Positional
    // region sharding will populate `regionindex` and make this a real lookup.)
    let region_shard = snap.region_shard.as_ref().map(|r| r.data_shard).unwrap_or(0);

    let conn = pool
        .regions(region_shard)
        .ok_or_else(|| GatherError::NoConnection(format!("regions-{region_shard}")))?;
    regions_sub(
        &conn,
        vec![
            format!("SELECT * FROM zones WHERE macro_zone = {}", proposal.macro_zone),
            format!("SELECT * FROM regions WHERE macro_region = {macro_region}"),
            // Promoted tile-cards on this zone (regions DB's own `cards` table) —
            // card-priority for the synthetic tile, and visibility of an in-flight
            // hold to validation.
            format!("SELECT * FROM cards WHERE macro_zone = {}", proposal.macro_zone),
            "SELECT * FROM card_shards".to_string(),
        ],
    )
    .await?;

    snap.zone = latest_zone(&conn, proposal.macro_zone);
    snap.region = latest_region(&conn, macro_region);
    snap.card_shards = latest_card_shards(&conn);
    let (q, r) = micro_loose_cell(proposal.micro_location);
    snap.tile_card = latest_tile_card_at(&conn, proposal.macro_zone, q, r);
    Ok(())
}

/// Find the promoted tile-card at hex `(q, r)` of `macro_zone` (regions DB),
/// latest version — a `TILE_CARD_TYPE` card placed loose (snapped) at the cell.
/// `None` if no tile has been promoted there.
fn latest_tile_card_at(
    conn: &bindings::shard::DbConnection,
    macro_zone: u64,
    q: u8,
    r: u8,
) -> Option<bindings::shard::Card> {
    use bindings::shard::cards_table::CardsTableAccess;
    let mut latest: HashMap<u32, bindings::shard::Card> = HashMap::new();
    for c in conn.db().cards().iter() {
        if c.macro_zone != macro_zone {
            continue;
        }
        let keep = latest
            .get(&c.card_id)
            .map_or(true, |p| valid_at_time(c.valid_at) >= valid_at_time(p.valid_at));
        if keep {
            latest.insert(c.card_id, c);
        }
    }
    latest.into_values().find(|c| {
        let (card_type, _) = unpack_definition(c.packed_definition);
        card_type == TILE_CARD_TYPE
            && !card_model::micro_is_card(c.flags)
            && micro_loose_cell(c.micro_location) == (q, r)
    })
}

/// Derive the synthetic tile for a recipe's branch-0 slot at the action cell,
/// `(packed_def, (stock0, stock1))`. **Card-priority**: a promoted tile-card at
/// the cell ([`Snapshot::tile_card`], gathered at this same cell) wins over the
/// zone slot, so a repeated action reads the live stock rather than the stale
/// zone bytes. Falls back to the zone slot. Returns `None` if neither resolves
/// (no zone / out of range / empty cell) — recipes that don't reference a tile
/// pass `None` harmlessly.
pub fn synthetic_tile(snap: &Snapshot, micro_location: u32) -> Option<(u16, (u8, u8))> {
    if let Some(card) = &snap.tile_card {
        return Some((
            card.packed_definition,
            (
                card_model::stock(card.stock, 0),
                card_model::stock(card.stock, 1),
            ),
        ));
    }
    let zone = snap.zone.as_ref()?;
    let (q, r) = micro_loose_cell(micro_location);
    if q >= 8 || r >= 8 {
        return None;
    }
    let tiles = [
        zone.t_0, zone.t_1, zone.t_2, zone.t_3, zone.t_4, zone.t_5, zone.t_6, zone.t_7, zone.t_8,
        zone.t_9, zone.t_10, zone.t_11, zone.t_12, zone.t_13, zone.t_14, zone.t_15,
    ];
    let (def_id, stock0, stock1) = tile_full(&tiles, (r as usize) * 8 + q as usize);
    if def_id == 0 {
        return None;
    }
    let packed_def = pack_definition(unpack_zone_definition(zone.packed_definition), def_id);
    Some((packed_def, (stock0, stock1)))
}

// --- latest-version readers (collapse the cache's history to current) ---

fn latest_cards(conn: &bindings::shard::DbConnection) -> Vec<bindings::shard::Card> {
    use bindings::shard::cards_table::CardsTableAccess;
    let mut latest: HashMap<u32, bindings::shard::Card> = HashMap::new();
    for c in conn.db().cards().iter() {
        let keep = latest
            .get(&c.card_id)
            .map_or(true, |p| valid_at_time(c.valid_at) >= valid_at_time(p.valid_at));
        if keep {
            latest.insert(c.card_id, c);
        }
    }
    latest.into_values().collect()
}

fn latest_zone(conn: &bindings::shard::DbConnection, macro_zone: u64) -> Option<bindings::shard::Zone> {
    use bindings::shard::zones_table::ZonesTableAccess;
    conn.db()
        .zones()
        .iter()
        .filter(|z| z.macro_zone == macro_zone)
        .max_by_key(|z| valid_at_time(z.valid_at))
}

fn latest_region(
    conn: &bindings::shard::DbConnection,
    macro_region: u64,
) -> Option<bindings::shard::Region> {
    use bindings::shard::regions_table::RegionsTableAccess;
    conn.db()
        .regions()
        .iter()
        .filter(|r| r.macro_region == macro_region)
        .max_by_key(|r| valid_at_time(r.valid_at))
}

fn latest_card_shards(conn: &bindings::shard::DbConnection) -> Vec<bindings::shard::CardShard> {
    use bindings::shard::card_shards_table::CardShardsTableAccess;
    let mut latest: HashMap<u16, bindings::shard::CardShard> = HashMap::new();
    for cs in conn.db().card_shards().iter() {
        let keep = latest
            .get(&cs.data_shard)
            .map_or(true, |p| valid_at_time(cs.valid_at) >= valid_at_time(p.valid_at));
        if keep {
            latest.insert(cs.data_shard, cs);
        }
    }
    latest.into_values().collect()
}
