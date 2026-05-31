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
    /// Reducer call `cid` succeeded.
    CallOk { cid: u32 },
    /// Reducer call `cid` failed.
    CallErr { cid: u32, error: String },
    /// A protocol-level error not tied to a specific request.
    Error { error: String },
}

impl GateMsg {
    /// Serialize to a JSON string for the WS sink.
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|e| {
            format!("{{\"t\":\"error\",\"error\":\"serialize: {e}\"}}")
        })
    }
}
