//! Client ↔ gate wire protocol (JSON over the WS).
//!
//! Relay-first shape while the gate fronts the `shard` monolith: the client
//! subscribes to tables (the gate fans out live rows) and calls reducers (the
//! gate relays to shard). Intentionally thin for now; as the gate absorbs
//! validation + sharding this stays the stable client contract, with table/
//! reducer names giving way to intent-shaped messages.

use serde::{Deserialize, Serialize};

/// A message from a client to the gate. `t` tags the variant.
#[derive(Debug, Deserialize)]
#[serde(tag = "t", rename_all = "snake_case")]
pub enum ClientMsg {
    /// Subscribe to a table, optionally filtered. The gate issues the upstream
    /// subscription and streams matching rows back as [`GateMsg::Row`].
    Sub {
        sid: u32,
        table: String,
        /// Raw SQL `WHERE` clause body (without the keyword), e.g.
        /// `owner_id = 1024`. None → whole table.
        #[serde(default)]
        filter: Option<String>,
    },
    /// Drop a subscription. (Callback teardown is a later refinement.)
    Unsub { sid: u32 },
    /// Call a shard reducer with positional JSON args; the gate relays it.
    Call {
        cid: u32,
        reducer: String,
        #[serde(default)]
        args: serde_json::Value,
    },
}

/// A message from the gate to a client.
#[derive(Debug, Serialize)]
#[serde(tag = "t", rename_all = "snake_case")]
pub enum GateMsg {
    /// The subscription `sid` is applied (initial rows have been delivered).
    Applied { sid: u32 },
    /// A row event on a subscribed table.
    Row {
        sid: u32,
        table: String,
        /// `"insert"`, `"update"`, or `"delete"`.
        op: &'static str,
        /// Present only for `"update"` — the prior row.
        #[serde(skip_serializing_if = "Option::is_none")]
        old: Option<serde_json::Value>,
        row: serde_json::Value,
    },
    /// Reducer call `cid` succeeded. Carries the gate's wall clock at reply time
    /// (`server_micros`) so the client gets a server-time sample piggybacked on
    /// its own round-trip — the "spacetime way" (the SDK rode the timestamp on
    /// reducer events). Active clients sync their clock from this for free; the
    /// standalone [`Time`](GateMsg::Time) frame then only fills idle gaps.
    CallOk { cid: u32, server_micros: String },
    /// Reducer call `cid` failed. Also carries `server_micros` — a rejected call
    /// is still a round-trip, so it's a valid clock sample.
    CallErr {
        cid: u32,
        error: String,
        server_micros: String,
    },
    /// A protocol-level error not tied to a specific request.
    Error { error: String },
    /// Server-clock keepalive: the gate's wall clock in microseconds since the
    /// unix epoch (string-encoded — exceeds JS safe-integer range). Emitted
    /// **only after the socket has been idle** for the keepalive interval (the
    /// first one fires immediately on connect for a fast initial lock); active
    /// clients get their samples from `call_ok`/`call_err` instead, so this
    /// never costs an active connection a byte. The client feeds it to its clock
    /// discipline (`serverNowMs`) so it tracks the timeline the gate
    /// future-stamps on. For one gate the gate's wall clock IS the canonical
    /// clock; multi-gate, the gate first syncs to a master clock and forwards
    /// that here (this frame is unchanged).
    Time { server_micros: String },
}

impl GateMsg {
    /// Serialize to a JSON string for the WS sink.
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|e| {
            format!("{{\"t\":\"error\",\"error\":\"serialize: {e}\"}}")
        })
    }

    /// Build a stamped `call_ok` reply, serialized for the sink. Stamps the
    /// gate's wall clock at call time so the client's round-trip carries a fresh
    /// server-time sample (see [`CallOk`](GateMsg::CallOk)).
    pub fn call_ok(cid: u32) -> String {
        GateMsg::CallOk {
            cid,
            server_micros: now_micros(),
        }
        .to_json()
    }

    /// Build a stamped `call_err` reply, serialized for the sink.
    pub fn call_err(cid: u32, error: String) -> String {
        GateMsg::CallErr {
            cid,
            error,
            server_micros: now_micros(),
        }
        .to_json()
    }
}

/// The gate's wall clock in microseconds since the unix epoch, string-encoded
/// (the value exceeds JS's safe-integer range, so it rides the wire as a string
/// and the client coerces to `bigint`). The single source of `server_micros`
/// for both the `call_ok`/`call_err` piggyback and the idle `Time` keepalive.
pub fn now_micros() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
        .to_string()
}
