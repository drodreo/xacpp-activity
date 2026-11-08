//! Multi-activity registry: the session table, per-activity event routing,
//! change notification with version numbers, and atomic full-snapshot reads.
//!
//! Library-side half of the buffer & projection model (ARCHITECTURE.md,
//! "Library / Binding Boundary" and "Buffer & Projection Model"):
//!
//! - **Registry** — sessions are organized per `activity_id`, instance-level
//!   (they outlive page bind/unbind). `register` is the admission point:
//!   events for unregistered activities are dropped.
//! - **Routing** — inbound wire events go straight to the owning session's
//!   unified conversion entry (envelope runtime → reducer). No detour
//!   through platform code.
//! - **Projection protocol** — every session carries a `u64` version that
//!   increments only when the projected state (model or interactions)
//!   actually changes; bookkeeping-only mutations (seq advance, buffer
//!   accumulation for dropped events) never bump it, so coalescing
//!   consumers never read without a reason. `snapshot` returns a
//!   [`HubSnapshot`] (version, rendered items, pending interactions)
//!   taken under one lock — always atomic.
//! - **Change notification** — an optional callback fires after a mutation
//!   is committed, *outside* the registry lock, carrying
//!   `(activity_id, version)`. Coalescing is allowed: consumers must treat
//!   the version they read as authoritative, not count notifications.
//! - **Buffer steady state** — finalized-turn content is trimmed to a
//!   30-part cap in whole-turn units. Settlement happens at turn
//!   finalization and after history bulk ingest; an in-flight part never
//!   triggers a trim. The trim is a UI-visible mutation: it rides the same
//!   commit (one version bump for ingest + trim).
//! - **Content validity** — a session's content is Void from admission
//!   until the first authoritative latest-window history ingest turns it
//!   Live; live events, interaction registration, replays and paging never
//!   flip it. Validity rides the snapshot so consumers can decide between
//!   a history pull and a zero-round-trip replay without side bookkeeping.
//! - **Drafts** — un-sent input lives in the consumer-held
//!   [`crate::draft::DraftStore`], separate from sessions: it survives
//!   registry voiding and dies only with the consumer. Drafts never enter
//!   the session fingerprint — typing bumps no projection version.
//!
//! Concurrency shape: async, no runtime is created here. The registry is
//! `Send + Sync` (interior `tokio::sync::Mutex`); callbacks must be
//! `Send + Sync`. A callback must not re-enter the same `ActivityHub` —
//! the async mutex is non-reentrant and re-entry deadlocks silently, with
//! no poison signal to surface it; read snapshots after the callback
//! returns instead.

/// Buffer steady-state cap: terminal (finalized-turn) content parts held
/// per activity (ARCHITECTURE.md, "Buffer & Projection Model").
pub const STEADY_STATE_PART_CAP: usize = 30;

/// Wire paging cursor carried by `conversation_history` responses
/// (`nextBeforeSeqInTopic` / `hasMore`, camelCase). The session-side
/// bookkeeping form of "where the authoritative window ends and whether
/// older pages exist".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WindowCursor {
    pub next_before_seq: Option<u64>,
    pub has_more: bool,
}

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;
use tokio::sync::Mutex;
use xacpp::wire::events::XacppEvent;
use xacpp::wire::message::XacppResponse;

use crate::envelope::{ActivityEventCategory, ActivityEventEnvelope};
use crate::event::ActivityEvent;
use crate::interaction::{Interaction, InteractionAnswer, InteractionError};
use crate::model::{ActivityModel, SnapshotItem};
use crate::runtime::{ActivityRuntime, ConsumeOutcome, SyncPlan};

// ---------------------------------------------------------------------------
// Session: one activity's runtime + model + projection version.
// ---------------------------------------------------------------------------

/// One activity's session: the envelope runtime, the projection model, and
/// the version/notification bookkeeping. Created by
/// [`ActivityHub::register`] (or directly, for consumers that manage
/// single sessions without a registry).
///
/// Every mutating entry point goes through [`ActivitySession::commit`],
/// which bumps the version — and arms the notification — only when the
/// fingerprint (model + interactions) actually changed.
pub struct ActivitySession {
    runtime: ActivityRuntime,
    model: ActivityModel,
    /// Content validity: `false` (Void) from admission until the first
    /// authoritative latest-window history ingest turns it Live
    /// (`apply_history`). Live events, interaction registration, replays
    /// and paging never flip it. UI-visible via [`HubSnapshot`].
    content_live: bool,
    /// Latest-window paging cursor (`nextBeforeSeqInTopic` / `hasMore`):
    /// where the authoritative window ends in the server-side ordering and
    /// whether older pages exist. Registered at authoritative latest-window
    /// ingests (reset together with the window) and advanced by older-page
    /// bookkeeping — the UI consumes it from the snapshot instead of
    /// keeping private cursor state (private state dies with a page; the
    /// session outlives pages). UI-visible via [`HubSnapshot`].
    next_before_seq: Option<u64>,
    has_more: bool,
    version: u64,
    pending_notify: Option<u64>,
}

/// A latest-window rebuild starts with a fresh model, so same-pair cache
/// knowledge carried only by the live event needs one explicit carry-over.
/// History's `None` means "not persisted", never "zero" or "erase known".
fn restore_known_pair_cache(previous: &ActivityModel, fresh: &mut ActivityModel) {
    let Some(previous) = previous.latest_pair_complete.as_ref() else {
        return;
    };
    let Some(fresh) = fresh.latest_pair_complete.as_mut() else {
        return;
    };
    if previous.pair == fresh.pair && fresh.token_usage.cache_hit_tokens.is_none() {
        fresh.token_usage.cache_hit_tokens = previous.token_usage.cache_hit_tokens;
    }
}

impl ActivitySession {
    /// Creates an empty session for one activity (version 0, content Void,
    /// no window cursor).
    pub fn new(activity_id: impl Into<String>) -> Self {
        Self {
            runtime: ActivityRuntime::new(activity_id),
            model: ActivityModel::new(),
            content_live: false,
            next_before_seq: None,
            has_more: false,
            version: 0,
            pending_notify: None,
        }
    }

    /// The session's activity id.
    pub fn activity_id(&self) -> &str {
        self.runtime.activity_id()
    }

    /// Current projection version (monotonic; increments only on observed
    /// state changes).
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Rendered snapshot of the model (independent copy).
    pub fn snapshot(&self) -> Vec<SnapshotItem> {
        self.model.snapshot()
    }

    /// Turn-level activity (any in-flight `Streaming` turn) — the
    /// authoritative active surface for input/button state.
    pub fn turn_active(&self) -> bool {
        self.model.turn_active()
    }

    /// Current pair epoch (advanced by live `pair_complete`; 0 before the
    /// first pair). Read-only accessor, same shape as `turn_active`.
    pub fn pair_epoch(&self) -> u64 {
        self.runtime.pair_epoch()
    }

    /// Monotonic turn-closing marker (live `complete` arrivals only).
    pub fn turn_completed(&self) -> u64 {
        self.model.turn_completed
    }

    // ------------------------------------------------------------------
    // Content validity.
    // ------------------------------------------------------------------

    /// Whether the session's content is Live (an authoritative
    /// latest-window history ingest has landed since admission).
    pub fn content_live(&self) -> bool {
        self.content_live
    }

    /// The latest-window paging cursor: where the authoritative window
    /// ends in the server-side ordering and whether older pages exist.
    /// `None`/`false` until the first cursor-carrying ingest.
    pub fn window_cursor(&self) -> (Option<u64>, bool) {
        (self.next_before_seq, self.has_more)
    }

    // ------------------------------------------------------------------
    // Event ingestion (the unified conversion entries).
    // ------------------------------------------------------------------

    /// Ingests one live wire event: typed conversion → envelope dispatch
    /// (seq/pair bookkeeping) → model application. Returns `false` when the
    /// event was dropped unparsed (malformed/unknown wire shape).
    ///
    /// Turn finalization settles the buffer steady state (whole-turn trim
    /// to [`STEADY_STATE_PART_CAP`]); in-flight parts never reach that
    /// branch. The trim rides the same commit: one version bump covers
    /// ingestion and trim.
    pub fn handle_live_event(
        &mut self,
        category: ActivityEventCategory,
        event: XacppEvent,
    ) -> bool {
        let typed = match ActivityEvent::from_xacpp_event(&event) {
            Ok(typed) => typed,
            Err(err) => {
                log::warn!(
                    "[registry] dropping malformed live event `{}` for `{}`: {err}",
                    event.name,
                    self.activity_id()
                );
                return false;
            }
        };
        self.commit(|session| {
            let finalized_before = session.model.finalized_pairs.len();
            let envelope = session.runtime.dispatch(category, typed);
            session.model.apply_envelope(&envelope);
            if session.model.finalized_pairs.len() > finalized_before {
                session.model.trim_to_steady_state(STEADY_STATE_PART_CAP);
            }
        });
        true
    }

    /// Applies a current-pair buffer replay batch (reconnect recovery).
    /// A replay-carried finalization settles the buffer steady state like
    /// the live path.
    pub fn apply_replay(&mut self, envelopes: impl IntoIterator<Item = ActivityEventEnvelope>) {
        self.commit(|session| {
            let finalized_before = session.model.finalized_pairs.len();
            session.model.apply_buffer_replay(envelopes);
            if session.model.finalized_pairs.len() > finalized_before {
                session.model.trim_to_steady_state(STEADY_STATE_PART_CAP);
            }
        });
    }

    /// Applies the authoritative latest-window history batch (page enter /
    /// reconnect realign). The window REPLACES the projection: the batch
    /// builds a fresh [`ActivityModel`], then carries over the live
    /// in-flight tail (`runtime.current_pair_events()` — the events of the
    /// pair still executing, which the authoritative window cannot contain
    /// yet) and, when the latest history pair is the same pair, the known
    /// live cache-hit fact that history cannot persist. The rebuilt
    /// projection then settles the buffer steady state (trim). The old
    /// window's content is dropped wholesale — a stale window merged by
    /// identity into fresh content is not a thing this entry produces.
    ///
    /// The ingest flips content validity to Live and rewrites the window
    /// cursor from `cursor` (no cursor information ⇒ no older history).
    /// The monotonic `turn_completed` marker is carried over untouched —
    /// it counts live closures already observed; a realign neither
    /// advances nor rewinds it.
    ///
    /// Older-page pulls never enter here: paging writes no content back —
    /// the cursor advances through [`Self::set_window_cursor`] alone.
    pub fn apply_history(&mut self, events: Vec<XacppEvent>, cursor: Option<WindowCursor>) {
        self.commit(|session| {
            session.content_live = true;
            session.next_before_seq = cursor.and_then(|c| c.next_before_seq);
            session.has_more = cursor.is_some_and(|c| c.has_more);
            let mut fresh = ActivityModel::new();
            fresh.apply_history_events(events);
            // Carry over only the real in-flight tail (BufferReplay source:
            // never fakes the turn-closed signal).
            let tail = session.runtime.current_pair_events();
            fresh.apply_buffer_replay(tail);
            restore_known_pair_cache(&session.model, &mut fresh);
            fresh.turn_completed = session.model.turn_completed;
            fresh.trim_to_steady_state(STEADY_STATE_PART_CAP);
            session.model = fresh;
        });
    }

    /// Rebuilds the authoritative history in full — the copy-build /
    /// align-archive channel. Same authoritative semantics as
    /// [`Self::apply_history`] (content validity flips to Live, the window
    /// cursor is rewritten from `cursor`, fresh model, `turn_completed`
    /// carried over), but the in-flight tail is not replayed (the archive
    /// is the complete record) and the steady-state trim is skipped: the
    /// archive is the full record, never a capped window. Semantic split:
    /// [`Self::apply_history`] is the live steady-state channel (tail
    /// carried, trim enforced); `apply_history_rebuild` is the
    /// full-archive channel.
    pub fn apply_history_rebuild(&mut self, events: Vec<XacppEvent>, cursor: Option<WindowCursor>) {
        self.commit(|session| {
            session.content_live = true;
            session.next_before_seq = cursor.and_then(|c| c.next_before_seq);
            session.has_more = cursor.is_some_and(|c| c.has_more);
            let mut fresh = ActivityModel::new();
            fresh.apply_history_events(events);
            restore_known_pair_cache(&session.model, &mut fresh);
            fresh.turn_completed = session.model.turn_completed;
            // No tail replay, no trim: the rebuild channel archives the
            // full record.
            session.model = fresh;
        });
    }

    /// Registers the window paging cursor directly (content untouched) —
    /// the bookkeeping-only seam for older-page pulls, whose content never
    /// enters the buffer (ARCHITECTURE.md, "Paging does not write back")
    /// but whose cursor state must advance: the cursor lives on the
    /// session, so the paging chain survives page re-entry without the UI
    /// keeping private state. A UI-visible mutation like any other commit.
    pub fn set_window_cursor(&mut self, cursor: WindowCursor) {
        self.commit(|session| {
            session.next_before_seq = cursor.next_before_seq;
            session.has_more = cursor.has_more;
        });
    }

    // ------------------------------------------------------------------
    // Reliability + interactions (delegated to the runtime).
    // ------------------------------------------------------------------

    /// Computes the recovery plan for a consumer reporting its last applied
    /// seq (delegates to [`ActivityRuntime::need_sync`]).
    pub fn need_sync(&self, last_applied_seq: u64) -> SyncPlan {
        self.runtime.need_sync(last_applied_seq)
    }

    /// Registers an interaction from a consumer's `on_command`. Identity
    /// dedup semantics live in [`ConsumeOutcome`].
    pub fn consume_command(
        &mut self,
        name: &str,
        data: Value,
    ) -> Result<ConsumeOutcome, InteractionError> {
        self.commit(|session| session.runtime.consume_command(name, data))
    }

    /// Answers a pending interaction; returns the wire response the
    /// consumer's session handler must return to the transport.
    pub fn respond(
        &mut self,
        interaction_id: u64,
        answer: InteractionAnswer,
    ) -> Result<XacppResponse, InteractionError> {
        self.commit(|session| session.runtime.respond(interaction_id, answer))
    }

    /// Explicitly degrades a pending interaction (undeliverable answer).
    pub fn degrade(
        &mut self,
        interaction_id: u64,
        reason: impl Into<String>,
    ) -> Result<(), InteractionError> {
        self.commit(|session| session.runtime.degrade(interaction_id, reason))
    }

    /// Snapshot of pending interactions (for rendering authorization /
    /// question cards).
    pub fn pending_interactions(&self) -> Vec<Interaction> {
        self.runtime.pending_interactions()
    }

    // ------------------------------------------------------------------
    // Mutation commit: version bump + notification arming.
    // ------------------------------------------------------------------

    /// Runs one mutation and commits it: the version increments — and the
    /// notification arms — only when the fingerprint changed. Bookkeeping
    /// that no projection consumer can observe (seq advance, buffer
    /// accumulation for dropped events) leaves the fingerprint equal and
    /// therefore produces no read.
    fn commit<R>(&mut self, mutate: impl FnOnce(&mut Self) -> R) -> R {
        let before = self.fingerprint();
        let out = mutate(self);
        if self.fingerprint() != before {
            self.version += 1;
            self.pending_notify = Some(self.version);
        }
        out
    }

    /// The observed state: model (UI projection), interactions
    /// (authorization/question cards), content validity, and the window
    /// paging cursor. All four are UI-visible surfaces; the 30-part
    /// steady-state cap bounds the clone cost the same way it bounds
    /// snapshot cost. Drafts are deliberately absent — they live in the
    /// consumer-held `DraftStore` and typing must never bump the
    /// projection version.
    fn fingerprint(&self) -> (ActivityModel, Vec<Interaction>, bool, Option<u64>, bool) {
        (
            self.model.clone(),
            self.runtime.interactions().to_vec(),
            self.content_live,
            self.next_before_seq,
            self.has_more,
        )
    }

    /// Takes the armed notification, if any (registry-side, under lock).
    fn take_notification(&mut self) -> Option<u64> {
        self.pending_notify.take()
    }
}

// ---------------------------------------------------------------------------
// Hub: the registry itself.
// ---------------------------------------------------------------------------

/// Atomic full snapshot of one activity's projection surface: the version,
/// the rendered model items, the pending interactions, and the turn-level
/// activity. Interactions are not render-order entries ([`SnapshotItem`]) —
/// they are the parallel surface the UI consumes for authorization /
/// question cards.
#[derive(Debug, Clone, PartialEq)]
pub struct HubSnapshot {
    /// Projection version (monotonic; bumps on every observed change,
    /// interaction mutations included).
    pub version: u64,
    /// Rendered model items, in render order.
    pub items: Vec<SnapshotItem>,
    /// Pending interactions (authorization / question cards). Answered and
    /// degraded interactions are excluded.
    pub pending_interactions: Vec<Interaction>,
    /// Turn-level activity: `true` while any turn is `Streaming`. The
    /// authoritative in-flight surface — consumers drive input/button
    /// state from it instead of proxy-inferring from per-entry streaming
    /// flags.
    pub turn_active: bool,
    /// Monotonic turn-closing marker: advances when a turn-level
    /// `complete` arrives on the live path (the cancel path's only
    /// terminal signal included). `pair_complete` closes one loop, not
    /// the turn, and never advances it; buffer replay and history
    /// ingest never advance it either.
    pub turn_completed: u64,
    /// Content validity: `false` (Void) until the first authoritative
    /// latest-window history ingest since admission. Consumers entering a
    /// page pull history when this is `false`, and replay the projection
    /// with zero round-trip when `true`.
    pub content_live: bool,
    /// Latest-window paging cursor: where the authoritative window ends in
    /// the server-side ordering (`None` = no older history) and whether
    /// older pages exist. Registered at authoritative latest-window
    /// ingests, advanced by older-page bookkeeping; consumers page from
    /// this instead of keeping private cursor state.
    pub next_before_seq: Option<u64>,
    pub has_more: bool,
    /// Most recently applied `pair_complete`, carrying the current
    /// activity's actual context-window and token-usage snapshot. `None`
    /// before the first completed pair; cache unknown remains `None` inside
    /// the payload and is never converted to zero.
    pub latest_pair_complete: Option<crate::event::PairCompletePayload>,
}

#[derive(Default)]
struct HubInner {
    sessions: HashMap<String, ActivitySession>,
    on_change: Option<ChangeCallback>,
}

/// Multi-activity registry: session table, event routing, change
/// notification, atomic snapshot reads.
///
/// Lock discipline: the inner mutex is held only across state access and
/// mutation; the change callback always fires after the lock is released.
/// Callbacks must not re-enter the hub (the async mutex is non-reentrant
/// and re-entry deadlocks silently).
#[derive(Default)]
pub struct ActivityHub {
    inner: Mutex<HubInner>,
}

impl ActivityHub {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers (admits) an activity, lazily creating its session.
    /// Idempotent: an already-registered id keeps its live session
    /// (returns `false`). Returns `true` when the session was newly
    /// created. This is the admission boundary — events for ids that were
    /// never registered are dropped at the router. A newly created
    /// session's content is Void (see [`HubSnapshot::content_live`]).
    pub async fn register(&self, activity_id: &str) -> bool {
        let mut inner = self.lock().await;
        match inner.sessions.entry(activity_id.to_string()) {
            std::collections::hash_map::Entry::Occupied(_) => false,
            std::collections::hash_map::Entry::Vacant(slot) => {
                slot.insert(ActivitySession::new(activity_id));
                true
            }
        }
    }

    /// Removes a session (screen-off / activity closed). Returns `true`
    /// when a session existed.
    pub async fn unregister(&self, activity_id: &str) -> bool {
        let mut inner = self.lock().await;
        inner.sessions.remove(activity_id).is_some()
    }

    /// Whether the activity is admitted (registered).
    pub async fn is_registered(&self, activity_id: &str) -> bool {
        self.lock().await.sessions.contains_key(activity_id)
    }

    /// Installs the change callback (replacing any previous one).
    ///
    /// Contract: after each committed mutation the callback fires once with
    /// `(activity_id, version)`, outside the registry lock. Coalescing is
    /// allowed — intermediate versions are not guaranteed to fire one by
    /// one; the consumer treats the version it reads as authoritative. The
    /// callback must not re-enter the hub.
    pub async fn set_on_change(&self, callback: NewChangeCallback) {
        self.lock().await.on_change = Some(Arc::from(callback));
    }

    /// Routes one inbound wire event to its owning session. Events for
    /// unregistered activities are dropped (admission = registered).
    /// Returns whether the event was consumed by a session.
    pub async fn route_event(
        &self,
        activity_id: &str,
        category: ActivityEventCategory,
        event: XacppEvent,
    ) -> bool {
        let notification = {
            let mut inner = self.lock().await;
            let Some(session) = inner.sessions.get_mut(activity_id) else {
                log::debug!(
                    "[registry] dropping event `{}` for unregistered activity `{activity_id}`",
                    event.name
                );
                return false;
            };
            session.handle_live_event(category, event);
            session
                .take_notification()
                .map(|version| (version, inner.on_change.clone()))
        };
        self.notify(activity_id, notification);
        true
    }

    /// Atomic full-snapshot read: the projection version, the rendered
    /// items, the pending interactions, the turn-level activity, the
    /// content validity and the window paging cursor, taken under one
    /// lock. `None` when the activity is not registered.
    pub async fn snapshot(&self, activity_id: &str) -> Option<HubSnapshot> {
        let inner = self.lock().await;
        let session = inner.sessions.get(activity_id)?;
        Some(HubSnapshot {
            version: session.version(),
            items: session.snapshot(),
            pending_interactions: session.pending_interactions(),
            turn_active: session.turn_active(),
            turn_completed: session.turn_completed(),
            content_live: session.content_live(),
            next_before_seq: session.next_before_seq,
            has_more: session.has_more,
            latest_pair_complete: session.model.latest_pair_complete.clone(),
        })
    }

    /// Current projection version, `None` when not registered.
    pub async fn version(&self, activity_id: &str) -> Option<u64> {
        let inner = self.lock().await;
        Some(inner.sessions.get(activity_id)?.version())
    }

    /// Voids the whole registry: every session dies — buffers and pending
    /// interactions included. The binding layer calls this on connection
    /// death (ARCHITECTURE.md, "Disconnection"); recovery is pull-based
    /// (authoritative history pull on next admission, suspended
    /// interactions re-pushed by the serve). Returns the number of
    /// sessions dropped.
    pub async fn unregister_all(&self) -> usize {
        let mut inner = self.lock().await;
        let dropped = inner.sessions.len();
        inner.sessions.clear();
        dropped
    }

    /// Answers a pending interaction. The reply is the wire response the
    /// binding layer sends back over the command response channel — the
    /// library does not send it. `Ok(None)` when the activity is not
    /// registered; the [`InteractionError`] cases (unknown id, already
    /// answered, kind mismatch) are the session state machine's
    /// rejections, and a rejected answer leaves the interaction unchanged.
    /// A successful answer is a UI-visible mutation: the version bumps and
    /// the change notification fires exactly like any other committed
    /// mutation.
    pub async fn respond(
        &self,
        activity_id: &str,
        interaction_id: u64,
        answer: InteractionAnswer,
    ) -> Result<Option<XacppResponse>, InteractionError> {
        match self
            .with_session(activity_id, |session| {
                session.respond(interaction_id, answer)
            })
            .await
        {
            None => Ok(None),
            Some(result) => result.map(Some),
        }
    }

    /// Runs `f` with exclusive access to the session under one lock — the
    /// composition point for multi-step flows (e.g. `need_sync` followed by
    /// `apply_replay`, or a history pull). Mutations committed inside arm
    /// the notification exactly like the routed path; the callback fires
    /// after the lock is released. `None` when the activity is not
    /// registered.
    pub async fn with_session<R>(
        &self,
        activity_id: &str,
        f: impl FnOnce(&mut ActivitySession) -> R,
    ) -> Option<R> {
        let (notification, out) = {
            let mut inner = self.lock().await;
            let session = inner.sessions.get_mut(activity_id)?;
            let out = f(session);
            let notification = session
                .take_notification()
                .map(|version| (version, inner.on_change.clone()));
            (notification, out)
        };
        self.notify(activity_id, notification);
        Some(out)
    }

    // ------------------------------------------------------------------

    async fn lock(&self) -> tokio::sync::MutexGuard<'_, HubInner> {
        // Async mutexes carry no poison state: a panicking holder releases
        // the lock normally, so there is no `expect` here.
        self.inner.lock().await
    }

    /// Fires the change callback outside the lock (guarded on presence).
    fn notify(&self, activity_id: &str, notification: Option<(u64, Option<ChangeCallback>)>) {
        if let Some((version, Some(callback))) = notification {
            callback(activity_id, version);
        }
    }
}

/// Shared handle to the installed change callback.
type ChangeCallback = Arc<dyn Fn(&str, u64) + Send + Sync>;

/// Caller-side form of [`ActivityHub::set_on_change`].
type NewChangeCallback = Box<dyn Fn(&str, u64) + Send + Sync>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::PairCompletePayload;
    use xacpp::wire::events::TokenUsage;

    fn pair_complete(pair: &str, total_tokens: u32, cache_hit_tokens: Option<u32>) -> XacppEvent {
        ActivityEvent::PairComplete(PairCompletePayload {
            pair: pair.into(),
            context_window: 1_000,
            token_usage: TokenUsage {
                prompt_tokens: 100,
                completion_tokens: 20,
                total_tokens,
                cache_hit_tokens,
                ..Default::default()
            },
            tier: None,
            model: None,
        })
        .to_wire()
    }

    #[tokio::test]
    async fn snapshot_atomically_exposes_latest_pair_usage() {
        let hub = ActivityHub::new();
        assert!(hub.register("a1").await);
        assert!(
            hub.route_event(
                "a1",
                ActivityEventCategory::Default,
                pair_complete("p1", 120, Some(90)),
            )
            .await
        );
        assert!(
            hub.route_event(
                "a1",
                ActivityEventCategory::Default,
                pair_complete("p2", 240, None),
            )
            .await
        );

        let snapshot = hub.snapshot("a1").await.expect("registered snapshot");
        let latest = snapshot
            .latest_pair_complete
            .as_ref()
            .expect("latest pair usage");
        assert_eq!(latest.pair, "p2");
        assert_eq!(latest.token_usage.total_tokens, 240);
        assert_eq!(latest.token_usage.cache_hit_tokens, None);

        // An identical replay is projection-idempotent: no new version and no
        // fabricated cache value.
        let version = snapshot.version;
        assert!(
            hub.route_event(
                "a1",
                ActivityEventCategory::Default,
                pair_complete("p2", 240, None),
            )
            .await
        );
        let replayed = hub.snapshot("a1").await.expect("registered snapshot");
        assert_eq!(replayed.version, version);
        assert_eq!(
            replayed
                .latest_pair_complete
                .expect("latest pair usage")
                .token_usage
                .cache_hit_tokens,
            None
        );
    }

    #[tokio::test]
    async fn history_rebuild_preserves_only_same_pair_known_cache() {
        let hub = ActivityHub::new();
        hub.register("a1").await;
        hub.route_event(
            "a1",
            ActivityEventCategory::Default,
            pair_complete("p1", 120, Some(90)),
        )
        .await;

        hub.with_session("a1", |session| {
            session.apply_history(vec![pair_complete("p1", 120, None)], None);
        })
        .await
        .expect("registered session");
        let same_pair = hub
            .snapshot("a1")
            .await
            .and_then(|snapshot| snapshot.latest_pair_complete)
            .expect("same-pair usage");
        assert_eq!(same_pair.token_usage.cache_hit_tokens, Some(90));

        hub.with_session("a1", |session| {
            session.apply_history(vec![pair_complete("p2", 240, None)], None);
        })
        .await
        .expect("registered session");
        let different_pair = hub
            .snapshot("a1")
            .await
            .and_then(|snapshot| snapshot.latest_pair_complete)
            .expect("different-pair usage");
        assert_eq!(different_pair.pair, "p2");
        assert_eq!(different_pair.token_usage.cache_hit_tokens, None);
    }
}
