//! Gate-side promise registry — the server half of the protocol's async-call
//! primitive (the client half is the client's `Outbox`).
//!
//! A reducer call whose true outcome isn't known at reply time is ACCEPTED as a
//! promise: the gate replies [`GateMsg::CallPromise`] immediately and registers a
//! resolver. Each [`poll`](Promises::poll) — driven off the connection loop's
//! tick — evaluates every pending resolver against the latest gate state and
//! emits the eventual `call_ok` / `call_err` for that `cid`, or, past the
//! promised deadline, a timeout error the client retries.
//!
//! Generic by construction: a resolver is a closure `&dyn` [`RegionView`] →
//! [`Resolution`], so ANY async condition over region state plugs in — a zone's
//! `available` bit flipping, a region coming into existence, etc. The view is
//! supplied at poll time (the per-connection regions subscription isn't `Clone`,
//! so resolvers can't capture it — they capture only the plain coordinates they
//! watch). Per-connection: `cid`s are per-connection, so each WS task owns one
//! registry.

use std::time::{Duration, Instant};

use resonantdust_protocol::protocol::GateMsg;

/// What a resolver can ask about region state — the gate implements this over its
/// regions subscription cache. `(presence, available)` for the latest row of
/// `macro_region`, or `None` if the gate hasn't mirrored that region.
pub trait RegionView {
    fn region_bits(&self, macro_region: u64) -> Option<(u64, u64)>;
}

/// A pending promise's verdict this poll.
pub enum Resolution {
    /// Not yet — keep awaiting.
    Pending,
    /// Done — reply `call_ok`.
    Ok,
    /// Failed — reply `call_err(reason)`; the client retries with back-off.
    Err(String),
}

struct Promise {
    cid: u32,
    deadline: Instant,
    resolve: Box<dyn FnMut(&dyn RegionView) -> Resolution + Send>,
}

/// A per-connection registry of outstanding async calls.
#[derive(Default)]
pub struct Promises {
    items: Vec<Promise>,
}

impl Promises {
    pub fn new() -> Self {
        Self::default()
    }

    /// Accept `cid` for async resolution; returns the `call_promise` frame to send
    /// the client now. `resolve` is evaluated each [`poll`](Self::poll) until it
    /// returns `Ok`/`Err`, or `timeout` elapses (→ a timeout `call_err`).
    pub fn accept(
        &mut self,
        cid: u32,
        timeout: Duration,
        now: Instant,
        resolve: impl FnMut(&dyn RegionView) -> Resolution + Send + 'static,
    ) -> String {
        self.items.push(Promise { cid, deadline: now + timeout, resolve: Box::new(resolve) });
        GateMsg::call_promise(cid, timeout.as_millis() as u64)
    }

    /// Evaluate every pending promise against `view`; return the resolution frames
    /// to send. Resolved and timed-out promises are removed; pending ones stay.
    pub fn poll(&mut self, now: Instant, view: &dyn RegionView) -> Vec<String> {
        let mut out = Vec::new();
        self.items.retain_mut(|p| match (p.resolve)(view) {
            Resolution::Ok => {
                out.push(GateMsg::call_ok(p.cid));
                false
            }
            Resolution::Err(e) => {
                out.push(GateMsg::call_err(p.cid, e));
                false
            }
            Resolution::Pending => {
                if now >= p.deadline {
                    out.push(GateMsg::call_err(p.cid, "promise timed out".to_string()));
                    false
                } else {
                    true
                }
            }
        });
        out
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// A mutable mock of the gate's region cache.
    #[derive(Default)]
    struct MockView(HashMap<u64, (u64, u64)>);
    impl RegionView for MockView {
        fn region_bits(&self, macro_region: u64) -> Option<(u64, u64)> {
            self.0.get(&macro_region).copied()
        }
    }

    #[test]
    fn resolves_ok_when_available_bit_appears() {
        let mut p = Promises::new();
        let now = Instant::now();
        // request_zone for region 100, bit 5: resolve when available has bit 5.
        let mask = 1u64 << 5;
        let frame = p.accept(7, Duration::from_secs(10), now, move |view| {
            match view.region_bits(100) {
                Some((_, avail)) if avail & mask != 0 => Resolution::Ok,
                _ => Resolution::Pending,
            }
        });
        assert!(frame.contains("call_promise"));
        let mut view = MockView::default();
        view.0.insert(100, (mask, 0)); // present, not available
        assert!(p.poll(now, &view).is_empty(), "pending while unavailable");
        view.0.insert(100, (mask, mask)); // available now
        let out = p.poll(now, &view);
        assert_eq!(out.len(), 1);
        assert!(out[0].contains("call_ok") && out[0].contains("\"cid\":7"));
        assert!(p.is_empty());
    }

    #[test]
    fn times_out_into_an_error() {
        let mut p = Promises::new();
        let now = Instant::now();
        p.accept(9, Duration::from_secs(5), now, |_| Resolution::Pending);
        let view = MockView::default();
        assert!(p.poll(now, &view).is_empty());
        let out = p.poll(now + Duration::from_secs(6), &view);
        assert_eq!(out.len(), 1);
        assert!(out[0].contains("call_err") && out[0].contains("\"cid\":9"));
        assert!(p.is_empty());
    }
}
