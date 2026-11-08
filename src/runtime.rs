//! Activity runtime: per-activity envelope dispatch state.
//!
//! One [`ActivityRuntime`] instance per activity, owned by the consumer (the
//! library never forces a global registry; hot paths receive the instance
//! directly). Dispatch semantics are ported verbatim from agentx
//! `activity_runtime.rs` — anchor lines cited in tests:
//!
//! - `seq` saturating-increments before the envelope is built
//!   (agentx L394/L395, also L432/L433 for the external path).
//! - `write_buffer` (agentx L564-573): `pair_complete` clears the current
//!   pair buffer *then* bumps `pair_epoch` (clear precedes the bump, L567
//!   before L568); `complete` is skipped entirely (L570); every other event
//!   is pushed onto the buffer (L572).
//! - The `pair_complete` envelope itself carries the pre-bump epoch, because
//!   the envelope is constructed before the bookkeeping runs (L395 vs L568).

use serde_json::Value;
use xacpp::wire::message::XacppResponse;

use crate::envelope::{ActivityEventCategory, ActivityEventEnvelope};
use crate::event::ActivityEvent;
use crate::interaction::{
    Interaction, InteractionAnswer, InteractionError, InteractionKind, InteractionPayload,
    InteractionState, build_response,
};

/// Recovery plan produced by [`ActivityRuntime::need_sync`].
///
/// Scope note (architecture): dropped pushes during a disconnect never
/// reached the library, so no seq was ever assigned to them — seq cannot
/// and should not detect connection-level loss. Reconnect/re-entry signals
/// are the consumer's job and map directly to pulling the projection via
/// `apply_history_events` (pushes are best-effort; reliability lives in the
/// pull path). `need_sync` covers the remaining scenario: a UI consumer
/// that paused consuming (activity recreation, page switch, collector
/// detaching) resumes and reports its last applied seq.
#[derive(Debug, Clone, PartialEq)]
pub enum SyncPlan {
    /// Consumer is caught up; nothing to do.
    UpToDate,
    /// The gap is fully covered by the current-pair buffer: replay the
    /// returned envelopes, then report `new_last_seq` as applied.
    BufferReplay {
        /// Buffered envelopes after `last_applied_seq` (a snapshot).
        envelopes: Vec<ActivityEventEnvelope>,
        /// Runtime's current last seq — the consumer's new watermark.
        new_last_seq: u64,
    },
    /// The gap exceeds the buffer's coverage (empty buffer, or the consumer
    /// is behind the buffer start): pull the authoritative projection via
    /// `apply_history_events`.
    NeedHistory,
}

/// Outcome of [`ActivityRuntime::consume_command`].
///
/// Identity-keyed dedup (`(activity, request_id)`; the activity scoping is
/// inherent in the per-activity runtime) turns serve re-pushes into one of
/// three outcomes instead of a fresh duplicate interaction:
#[derive(Debug)]
pub enum ConsumeOutcome {
    /// Fresh interaction (first arrival with this identity, the legacy
    /// no-requestId compat path, or a re-push onto a `Degraded` record).
    /// The handle is newly allocated; the consumer attaches its reply
    /// channel to it.
    New(u64),
    /// Re-push onto an already-`Pending` interaction with the same
    /// identity: no new object, no new UI snapshot entry. The returned
    /// handle is the existing one — the consumer re-arms its reply channel
    /// on it (replacing the stale sender).
    Reattached(u64),
    /// Re-push onto an already-`Answered` interaction with the same
    /// identity: the wire response built from the stored answer is
    /// delivered directly — the consumer must return it to the transport
    /// without waiting for a user reply. State is not modified.
    Answered(XacppResponse),
}

/// Per-activity runtime state: seq/pair-epoch counters plus the current-pair
/// replay buffer.
#[derive(Debug, Clone, PartialEq)]
pub struct ActivityRuntime {
    activity_id: String,
    seq: u64,
    pair_epoch: u64,
    current_pair_buffer: Vec<ActivityEventEnvelope>,
    /// Pending/answered interaction objects (instance-level: the consumer
    /// holding the runtime keeps these alive across activity recreation).
    interactions: Vec<Interaction>,
    next_interaction_id: u64,
}

impl ActivityRuntime {
    /// Creates a runtime for one activity. One instance per activity; the
    /// consumer owns it (no global registry).
    pub fn new(activity_id: impl Into<String>) -> Self {
        Self {
            activity_id: activity_id.into(),
            seq: 0,
            pair_epoch: 0,
            current_pair_buffer: Vec::new(),
            interactions: Vec::new(),
            next_interaction_id: 0,
        }
    }

    /// The runtime's activity id.
    pub fn activity_id(&self) -> &str {
        &self.activity_id
    }

    /// Crate-internal read view of the interactions table (registry-side
    /// change detection covers answered/degraded transitions too; the
    /// public render surface is `pending_interactions`).
    pub(crate) fn interactions(&self) -> &[Interaction] {
        &self.interactions
    }

    /// Dispatches one event: assigns the next seq, builds the envelope, and
    /// applies the buffer bookkeeping. Returns the envelope.
    pub fn dispatch(
        &mut self,
        category: ActivityEventCategory,
        payload: ActivityEvent,
    ) -> ActivityEventEnvelope {
        // agentx L394: seq saturating-increment runs before envelope build.
        self.seq = self.seq.saturating_add(1);
        // agentx L395: envelope carries the current (pre-bump) pair_epoch.
        let envelope = ActivityEventEnvelope {
            activity_id: self.activity_id.clone(),
            seq: self.seq,
            pair_epoch: self.pair_epoch,
            category,
            payload,
        };
        self.apply_buffer_bookkeeping(&envelope);
        envelope
    }

    /// Buffer bookkeeping ported from agentx `write_buffer` (L564-573).
    fn apply_buffer_bookkeeping(&mut self, envelope: &ActivityEventEnvelope) {
        match &envelope.payload {
            // agentx L566-569: clear first, then bump the epoch; the
            // pair_complete envelope itself never enters the buffer.
            ActivityEvent::PairComplete(_) => {
                self.current_pair_buffer.clear();
                self.pair_epoch = self.pair_epoch.saturating_add(1);
            }
            // agentx L570: complete is skipped entirely.
            ActivityEvent::Complete(_) => {}
            // agentx L572: everything else accumulates.
            _ => self.current_pair_buffer.push(envelope.clone()),
        }
    }

    /// Snapshot of the current pair buffer — the replay source for gap
    /// recovery (agentx `get_activity_current_pair_events` semantics).
    pub fn current_pair_events(&self) -> Vec<ActivityEventEnvelope> {
        self.current_pair_buffer.clone()
    }

    /// Last assigned seq (0 before the first dispatch).
    pub fn last_seq(&self) -> u64 {
        self.seq
    }

    /// Current pair epoch.
    pub fn pair_epoch(&self) -> u64 {
        self.pair_epoch
    }

    // ------------------------------------------------------------------
    // Interactions (commands, not events — see interaction.rs).
    // ------------------------------------------------------------------

    /// Registers an interaction from a consumer's `on_command`: `name` is
    /// the command name, `data` its arguments. Returns the consume outcome
    /// (see [`ConsumeOutcome`]). Unknown command names are rejected (the
    /// consumer falls through to its other command handling).
    pub fn consume_command(
        &mut self,
        name: &str,
        data: Value,
    ) -> Result<ConsumeOutcome, InteractionError> {
        let (kind, payload) = match name {
            "action_request" => {
                let payload: xacpp::wire::events::ActionRequestPayload =
                    serde_json::from_value(data).map_err(|source| {
                        InteractionError::MalformedPayload {
                            name: name.to_string(),
                            source,
                        }
                    })?;
                (InteractionKind::Action, InteractionPayload::Action(payload))
            }
            "question" => {
                let payload: xacpp::wire::events::QuestionPayload = serde_json::from_value(data)
                    .map_err(|source| InteractionError::MalformedPayload {
                        name: name.to_string(),
                        source,
                    })?;
                (
                    InteractionKind::Question,
                    InteractionPayload::Question(payload),
                )
            }
            "sensitive_info_operation" => {
                let payload: xacpp::wire::events::SensitiveInfoOperationPayload =
                    serde_json::from_value(data).map_err(|source| {
                        InteractionError::MalformedPayload {
                            name: name.to_string(),
                            source,
                        }
                    })?;
                (
                    InteractionKind::SensitiveInfoOperation,
                    InteractionPayload::SensitiveInfoOperation(payload),
                )
            }
            other => return Err(InteractionError::UnknownCommand(other.to_string())),
        };
        self.admit_interaction(kind, payload)
    }

    /// Identity-keyed admission. The dedup key is `(activity, request_id)` —
    /// the activity scoping is inherent in the per-activity runtime, so
    /// lookups match on `kind` + `request_id`.
    ///
    /// - Same identity `Pending` → reattach to the existing handle.
    /// - Same identity `Answered` → idempotent re-answer from the stored
    ///   answer; state is not modified.
    /// - Same identity `Degraded` → allocate fresh: `Degraded` is a one-way
    ///   terminal state ("consumer cannot deliver"), and a re-push after
    ///   degradation is precisely a second chance to deliver — the fresh
    ///   object stays answerable without touching the state machine.
    fn admit_interaction(
        &mut self,
        kind: InteractionKind,
        payload: InteractionPayload,
    ) -> Result<ConsumeOutcome, InteractionError> {
        let request_id = match &payload {
            InteractionPayload::Action(payload) => payload.request_id.clone(),
            InteractionPayload::Question(payload) => payload.request_id.clone(),
            // The sensitive_info_operation payload has no requestId field on
            // the wire (xacpp `events::interaction`); it always takes the
            // no-identity path below.
            InteractionPayload::SensitiveInfoOperation(_) => String::new(),
        };
        if !request_id.is_empty() {
            let existing = self
                .interactions
                .iter()
                .find(|interaction| {
                    interaction.kind == kind && interaction.request_id == request_id
                })
                .map(|interaction| (interaction.id, interaction.state.clone()));
            match existing {
                Some((id, InteractionState::Pending)) => {
                    return Ok(ConsumeOutcome::Reattached(id));
                }
                Some((_, InteractionState::Answered { answer })) => {
                    let response = build_response(kind, &answer)?;
                    return Ok(ConsumeOutcome::Answered(response));
                }
                // Not found, or Degraded (terminal — see doc above): fall
                // through to fresh allocation.
                _ => {}
            }
        }
        // Empty request id (or no same-identity record above) allocates
        // fresh. COMPAT: empty string = legacy serve without requestId
        // (payload defaults deserialize to an empty string; also covers
        // sensitive_info_operation payloads that lack the field on the
        // wire) — no identity key to dedupe on, keep allocating per call;
        // lifted in a later version.
        let id = self.next_interaction_id;
        self.next_interaction_id += 1;
        self.interactions.push(Interaction {
            id,
            kind,
            request_id,
            payload,
            state: InteractionState::Pending,
        });
        Ok(ConsumeOutcome::New(id))
    }

    /// Answers a pending interaction. Returns the wire response the
    /// consumer's session handler must return to the transport (the reply
    /// travels the command response channel — the library does not send it).
    /// Pending → Answered is one-way; a second respond is rejected.
    pub fn respond(
        &mut self,
        interaction_id: u64,
        answer: InteractionAnswer,
    ) -> Result<XacppResponse, InteractionError> {
        let Some(interaction) = self
            .interactions
            .iter_mut()
            .find(|interaction| interaction.id == interaction_id)
        else {
            return Err(InteractionError::UnknownId(interaction_id));
        };
        if !matches!(interaction.state, InteractionState::Pending) {
            return Err(InteractionError::AlreadyAnswered { id: interaction_id });
        }
        let response = build_response(interaction.kind, &answer)?;
        interaction.state = InteractionState::Answered { answer };
        Ok(response)
    }

    /// Explicitly degrades a pending interaction (consumer cannot deliver
    /// an answer — disconnect and similar). The state records the reason;
    /// later `respond` calls on it are rejected.
    pub fn degrade(
        &mut self,
        interaction_id: u64,
        reason: impl Into<String>,
    ) -> Result<(), InteractionError> {
        let Some(interaction) = self
            .interactions
            .iter_mut()
            .find(|interaction| interaction.id == interaction_id)
        else {
            return Err(InteractionError::UnknownId(interaction_id));
        };
        if !matches!(interaction.state, InteractionState::Pending) {
            return Err(InteractionError::AlreadyAnswered { id: interaction_id });
        }
        interaction.state = InteractionState::Degraded {
            reason: reason.into(),
        };
        Ok(())
    }

    /// Snapshot of pending interactions (for rendering authorization /
    /// question cards). Independent copy; answered/degraded excluded.
    pub fn pending_interactions(&self) -> Vec<Interaction> {
        self.interactions
            .iter()
            .filter(|interaction| matches!(interaction.state, InteractionState::Pending))
            .cloned()
            .collect()
    }

    /// Computes the recovery plan for a consumer reporting
    /// `last_applied_seq` (the seq of the last envelope it applied).
    ///
    /// Decision table:
    /// - `last_applied_seq >= last_seq` → `UpToDate`
    /// - buffer non-empty and `buffer[0].seq <= last_applied_seq + 1`
    ///   (the gap is contiguous with the buffer start) → `BufferReplay`
    ///   carrying the buffered envelopes after `last_applied_seq`
    /// - otherwise (buffer empty, or the consumer is behind the buffer
    ///   start) → `NeedHistory`
    pub fn need_sync(&self, last_applied_seq: u64) -> SyncPlan {
        if last_applied_seq >= self.seq {
            return SyncPlan::UpToDate;
        }
        match self.current_pair_buffer.first() {
            Some(first) if first.seq <= last_applied_seq.saturating_add(1) => {
                SyncPlan::BufferReplay {
                    envelopes: self
                        .current_pair_buffer
                        .iter()
                        .filter(|envelope| envelope.seq > last_applied_seq)
                        .cloned()
                        .collect(),
                    new_last_seq: self.seq,
                }
            }
            _ => SyncPlan::NeedHistory,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{
        BlockBoundaryPayload, CompletePayload, PairCompletePayload, ThinkContentPayload,
    };
    use crate::model::ActivityModel;
    use xacpp::wire::events::TokenUsage;

    fn think_delta(n: &str) -> ActivityEvent {
        ActivityEvent::ThinkDelta(ThinkContentPayload {
            content: n.into(),
            round: "r1".into(),
            pair: "p1".into(),
            part_id: "tp0".into(),
        })
    }

    fn content_start() -> ActivityEvent {
        ActivityEvent::ContentStart(BlockBoundaryPayload {
            round: "r1".into(),
            pair: "p1".into(),
            part_id: "cp0".into(),
        })
    }

    fn pair_complete() -> ActivityEvent {
        ActivityEvent::PairComplete(PairCompletePayload {
            pair: "p1".into(),
            context_window: 0,
            token_usage: TokenUsage::default(),
            tier: None,
            model: None,
        })
    }

    fn complete() -> ActivityEvent {
        ActivityEvent::Complete(CompletePayload {
            round: "r1".into(),
            assistant_reply: vec![],
            abort_reason: None,
            canceled: false,
        })
    }

    fn dispatch_all(runtime: &mut ActivityRuntime, payloads: Vec<ActivityEvent>) {
        for payload in payloads {
            runtime.dispatch(ActivityEventCategory::Default, payload);
        }
    }

    /// agentx anchor L394: seq saturating-increments on every dispatch.
    #[test]
    fn seq_is_monotonic_across_dispatches() {
        let mut runtime = ActivityRuntime::new("act-1");
        assert_eq!(runtime.last_seq(), 0);
        for expected in 1u64..=5 {
            let envelope = runtime.dispatch(ActivityEventCategory::Default, think_delta("x"));
            assert_eq!(envelope.seq, expected);
            assert_eq!(runtime.last_seq(), expected);
        }
    }

    /// agentx anchor L568: pair_complete bumps the epoch exactly once.
    #[test]
    fn pair_complete_bumps_epoch_exactly_once() {
        let mut runtime = ActivityRuntime::new("act-1");
        assert_eq!(runtime.pair_epoch(), 0);
        dispatch_all(
            &mut runtime,
            vec![think_delta("a"), pair_complete(), pair_complete()],
        );
        assert_eq!(runtime.pair_epoch(), 2);
    }

    /// Non-boundary events never touch the epoch.
    #[test]
    fn non_boundary_events_leave_epoch_untouched() {
        let mut runtime = ActivityRuntime::new("act-1");
        dispatch_all(
            &mut runtime,
            vec![think_delta("a"), content_start(), complete()],
        );
        assert_eq!(runtime.pair_epoch(), 0);
    }

    /// agentx anchor L572: normal events accumulate in the buffer.
    #[test]
    fn buffer_accumulates_normal_events() {
        let mut runtime = ActivityRuntime::new("act-1");
        dispatch_all(&mut runtime, vec![think_delta("a"), content_start()]);
        let buffer = runtime.current_pair_events();
        assert_eq!(buffer.len(), 2);
        assert_eq!(buffer[0].seq, 1);
        assert_eq!(buffer[1].seq, 2);
    }

    /// agentx anchors L567/L568: pair_complete clears the buffer and never
    /// enters it itself.
    #[test]
    fn pair_complete_clears_buffer_and_is_not_buffered() {
        let mut runtime = ActivityRuntime::new("act-1");
        dispatch_all(&mut runtime, vec![think_delta("a"), content_start()]);
        assert_eq!(runtime.current_pair_events().len(), 2);

        let envelope = runtime.dispatch(ActivityEventCategory::Default, pair_complete());
        assert!(runtime.current_pair_events().is_empty());
        // The pair_complete envelope itself is not in the buffer.
        assert!(
            !runtime
                .current_pair_events()
                .iter()
                .any(|e| e.seq == envelope.seq)
        );
    }

    /// agentx anchor L395 vs L568: the pair_complete envelope carries the
    /// pre-bump epoch (envelope built before bookkeeping).
    #[test]
    fn pair_complete_envelope_carries_pre_bump_epoch() {
        let mut runtime = ActivityRuntime::new("act-1");
        let envelope = runtime.dispatch(ActivityEventCategory::Default, pair_complete());
        assert_eq!(envelope.pair_epoch, 0);
        assert_eq!(runtime.pair_epoch(), 1);

        let next = runtime.dispatch(ActivityEventCategory::Default, pair_complete());
        assert_eq!(next.pair_epoch, 1);
        assert_eq!(runtime.pair_epoch(), 2);
    }

    /// agentx anchor L570: complete gets a seq but skips the buffer.
    #[test]
    fn complete_gets_seq_but_skips_buffer() {
        let mut runtime = ActivityRuntime::new("act-1");
        dispatch_all(&mut runtime, vec![think_delta("a")]);
        let before = runtime.current_pair_events().len();

        let envelope = runtime.dispatch(ActivityEventCategory::Default, complete());
        assert_eq!(envelope.seq, 2);
        assert_eq!(runtime.current_pair_events().len(), before);
    }

    /// After a clear the next pair accumulates from empty.
    #[test]
    fn buffer_starts_fresh_after_clear() {
        let mut runtime = ActivityRuntime::new("act-1");
        dispatch_all(
            &mut runtime,
            vec![think_delta("a"), pair_complete(), think_delta("b")],
        );
        let buffer = runtime.current_pair_events();
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer[0].seq, 3);
        assert_eq!(buffer[0].pair_epoch, 1);
    }

    /// Saturating neighborhood: u64::MAX must not panic (agentx anchor L394
    /// saturating_add).
    #[test]
    fn saturating_seq_neighborhood_does_not_panic() {
        let mut runtime = ActivityRuntime::new("act-1");
        runtime.seq = u64::MAX;

        let first = runtime.dispatch(ActivityEventCategory::Default, think_delta("a"));
        assert_eq!(first.seq, u64::MAX);
        let second = runtime.dispatch(ActivityEventCategory::Default, think_delta("b"));
        assert_eq!(second.seq, u64::MAX);
        assert_eq!(runtime.last_seq(), u64::MAX);
    }

    /// Snapshot semantics: current_pair_events returns an independent copy.
    #[test]
    fn current_pair_events_returns_a_snapshot() {
        let mut runtime = ActivityRuntime::new("act-1");
        runtime.dispatch(ActivityEventCategory::Default, think_delta("a"));
        let mut snapshot = runtime.current_pair_events();
        assert_eq!(snapshot.len(), 1);

        snapshot.push(ActivityEventEnvelope {
            activity_id: "act-1".into(),
            seq: 99,
            pair_epoch: 0,
            category: ActivityEventCategory::Default,
            payload: think_delta("injected"),
        });
        assert_eq!(runtime.current_pair_events().len(), 1);
    }

    // ---- need_sync decision table ----

    /// UpToDate: watermark at (or, defensively, ahead of) last_seq.
    #[test]
    fn need_sync_up_to_date_when_caught_up() {
        let mut runtime = ActivityRuntime::new("act-1");
        for i in 0..3 {
            runtime.dispatch(
                ActivityEventCategory::Default,
                think_delta(&format!("d{i}")),
            );
        }
        assert_eq!(runtime.last_seq(), 3);
        assert_eq!(runtime.need_sync(3), SyncPlan::UpToDate);
        assert_eq!(runtime.need_sync(5), SyncPlan::UpToDate);
    }

    /// BufferReplay boundaries: gap starting exactly at the buffer head
    /// ("exact cover") and one already applied ("off by one").
    #[test]
    fn need_sync_buffer_replay_covers_contiguous_gap() {
        let mut runtime = ActivityRuntime::new("act-1");
        for i in 0..3 {
            runtime.dispatch(
                ActivityEventCategory::Default,
                think_delta(&format!("d{i}")),
            );
        }

        // Exact cover: nothing applied, buffer[0].seq == 0 + 1.
        let SyncPlan::BufferReplay {
            envelopes,
            new_last_seq,
        } = runtime.need_sync(0)
        else {
            panic!("expected BufferReplay for exact cover");
        };
        assert_eq!(
            envelopes.iter().map(|e| e.seq).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(new_last_seq, 3);

        // Off by one: consumer applied seq 1; only 2..3 replay.
        let SyncPlan::BufferReplay {
            envelopes,
            new_last_seq,
        } = runtime.need_sync(1)
        else {
            panic!("expected BufferReplay for off-by-one");
        };
        assert_eq!(
            envelopes.iter().map(|e| e.seq).collect::<Vec<_>>(),
            vec![2, 3]
        );
        assert_eq!(new_last_seq, 3);
    }

    /// NeedHistory: consumer behind the buffer start (gap spans turns).
    #[test]
    fn need_sync_need_history_when_behind_buffer_start() {
        let mut runtime = ActivityRuntime::new("act-1");
        for i in 0..3 {
            runtime.dispatch(
                ActivityEventCategory::Default,
                think_delta(&format!("d{i}")),
            );
        }
        runtime.dispatch(ActivityEventCategory::Default, pair_complete());
        for i in 0..2 {
            runtime.dispatch(
                ActivityEventCategory::Default,
                think_delta(&format!("e{i}")),
            );
        }
        // Buffer now holds seq 5..6; consumer is at seq 2 (behind 5-1=4).
        assert_eq!(runtime.need_sync(2), SyncPlan::NeedHistory);
        // Contiguous with the new buffer start (4 + 1 == 5): replay works.
        assert!(matches!(
            runtime.need_sync(4),
            SyncPlan::BufferReplay { .. }
        ));
    }

    /// NeedHistory: buffer empty (pair_complete itself was the missed
    /// envelope) — nothing to replay from.
    #[test]
    fn need_sync_need_history_when_buffer_empty() {
        let mut runtime = ActivityRuntime::new("act-1");
        for i in 0..3 {
            runtime.dispatch(
                ActivityEventCategory::Default,
                think_delta(&format!("d{i}")),
            );
        }
        runtime.dispatch(ActivityEventCategory::Default, pair_complete());
        // last_seq == 4 (the pair_complete); buffer is empty.
        assert_eq!(runtime.need_sync(3), SyncPlan::NeedHistory);
        assert_eq!(runtime.need_sync(4), SyncPlan::UpToDate);
    }

    /// Closed loop: consumer applies live, pauses, resumes via
    /// BufferReplay, and the advanced watermark reports UpToDate.
    #[test]
    fn buffer_replay_advances_watermark_to_up_to_date() {
        let mut runtime = ActivityRuntime::new("act-1");
        let mut model = ActivityModel::new();

        // Consumer applies the first envelope live, then detaches.
        let first = runtime.dispatch(ActivityEventCategory::Default, think_delta("d0"));
        model.apply_envelope(&first);
        for i in 1..3 {
            runtime.dispatch(
                ActivityEventCategory::Default,
                think_delta(&format!("d{i}")),
            );
        }

        // Resume with watermark = 1: replay covers d1..d2.
        let SyncPlan::BufferReplay {
            envelopes,
            new_last_seq,
        } = runtime.need_sync(1)
        else {
            panic!("expected BufferReplay");
        };
        model.apply_buffer_replay(envelopes);
        assert_eq!(
            model.get_turns()[0].think.as_ref().unwrap().content,
            "d0d1d2"
        );

        assert_eq!(runtime.need_sync(new_last_seq), SyncPlan::UpToDate);
    }

    // ---- interactions (commands, not events) ----

    use crate::interaction::{InteractionAnswer, InteractionError, InteractionState};
    use xacpp::wire::events::{
        ActionResponse, QuestionResponse, SensitiveInfoOperationResponse, SensitiveInfoResult,
    };

    /// Test helper: consume with no requestId (legacy path → always `New`).
    fn consume_action(runtime: &mut ActivityRuntime) -> u64 {
        match runtime
            .consume_command(
                "action_request",
                serde_json::json!({
                    "toolName": "shell",
                    "arguments": "{\"cmd\":\"date\"}",
                    "actionId": "a-1",
                    "description": "run date",
                    "alert": "info",
                    "intent": "clock"
                }),
            )
            .expect("action_request must register")
        {
            ConsumeOutcome::New(id) => id,
            other => panic!("expected fresh allocation, got {other:?}"),
        }
    }

    /// Test helper: consume with no requestId (legacy path → always `New`).
    fn consume_question(runtime: &mut ActivityRuntime) -> u64 {
        match runtime
            .consume_command(
                "question",
                serde_json::json!({"question": "continue?", "options": ["yes", "no"]}),
            )
            .expect("question must register")
        {
            ConsumeOutcome::New(id) => id,
            other => panic!("expected fresh allocation, got {other:?}"),
        }
    }

    /// Test helper (the payload has no requestId field on the wire at all).
    fn consume_sensitive_info_operation(runtime: &mut ActivityRuntime) -> u64 {
        match runtime
            .consume_command(
                "sensitive_info_operation",
                serde_json::json!({
                    "operation": {
                        "type": "collect",
                        "items": [{
                            "key": "API_KEY",
                            "displayText": "••••",
                            "hint": "service token",
                            "siType": "secret"
                        }]
                    },
                    "origin": ["child-act"]
                }),
            )
            .expect("sensitive_info_operation must register")
        {
            ConsumeOutcome::New(id) => id,
            other => panic!("expected fresh allocation, got {other:?}"),
        }
    }

    /// Registration: both kinds parse payloads and get library ids.
    #[test]
    fn interactions_register_and_parse_payloads() {
        let mut runtime = ActivityRuntime::new("act-1");
        let action_id = consume_action(&mut runtime);
        let question_id = consume_question(&mut runtime);
        assert_ne!(action_id, question_id, "ids are library-assigned handles");

        let pending = runtime.pending_interactions();
        assert_eq!(pending.len(), 2);
        assert!(pending.iter().all(|i| i.state == InteractionState::Pending));
        match &pending[0].payload {
            InteractionPayload::Action(payload) => {
                assert_eq!(payload.tool_name, "shell");
                assert_eq!(payload.action_id, "a-1");
                assert_eq!(payload.alert, xacpp::wire::events::AlertLevel::Info);
            }
            other => panic!("expected Action payload, got {other:?}"),
        }
        match &pending[1].payload {
            InteractionPayload::Question(payload) => {
                assert_eq!(payload.question, "continue?");
                assert_eq!(payload.options, vec!["yes".to_string(), "no".to_string()]);
            }
            other => panic!("expected Question payload, got {other:?}"),
        }
    }

    /// Malformed payloads and unknown command names are rejected.
    #[test]
    fn consume_command_rejects_malformed_and_unknown() {
        let mut runtime = ActivityRuntime::new("act-1");
        let err = runtime
            .consume_command("action_request", serde_json::json!({"toolName": "shell"}))
            .unwrap_err();
        assert!(matches!(err, InteractionError::MalformedPayload { .. }));

        let err = runtime
            .consume_command("some_other_command", serde_json::json!({}))
            .unwrap_err();
        assert!(
            matches!(err, InteractionError::UnknownCommand(name) if name == "some_other_command")
        );
        assert!(runtime.pending_interactions().is_empty());
    }

    // ---- identity-keyed dedup (ConsumeOutcome) ----

    /// A test payload with a requestId attached.
    fn action_command(request_id: &str) -> serde_json::Value {
        serde_json::json!({
            "toolName": "shell",
            "arguments": "{}",
            "actionId": "a-1",
            "description": "run date",
            "alert": "info",
            "intent": "clock",
            "requestId": request_id
        })
    }

    /// Asserts the outcome is a fresh allocation and returns its handle.
    fn expect_new(outcome: ConsumeOutcome) -> u64 {
        match outcome {
            ConsumeOutcome::New(id) => id,
            other => panic!("expected fresh allocation, got {other:?}"),
        }
    }

    /// First arrival with a requestId allocates and stores the identity on
    /// the interaction.
    #[test]
    fn dedup_first_arrival_allocates_and_stores_identity() {
        let mut runtime = ActivityRuntime::new("act-1");
        let id = expect_new(
            runtime
                .consume_command("action_request", action_command("req-1"))
                .expect("action_request must register"),
        );
        let pending = runtime.pending_interactions();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, id);
        assert_eq!(pending[0].request_id, "req-1");
        assert_eq!(pending[0].state, InteractionState::Pending);
    }

    /// Re-push onto a Pending interaction with the same identity: the
    /// existing handle comes back (`Reattached`), no new snapshot entry
    /// appears, the id counter is not consumed, and the existing handle
    /// stays answerable (the consumer re-arms its reply channel on it).
    #[test]
    fn dedup_pending_repush_reattaches_without_new_entry() {
        let mut runtime = ActivityRuntime::new("act-1");
        let first = expect_new(
            runtime
                .consume_command("action_request", action_command("req-1"))
                .expect("first push must register"),
        );

        let second = match runtime
            .consume_command("action_request", action_command("req-1"))
            .expect("re-push must register")
        {
            ConsumeOutcome::Reattached(id) => id,
            other => panic!("expected reattachment, got {other:?}"),
        };
        assert_eq!(second, first, "the existing handle is returned");

        let pending = runtime.pending_interactions();
        assert_eq!(pending.len(), 1, "no new card in the snapshot face");
        assert_eq!(pending[0].id, first);

        // The id counter was not consumed by the re-push: the next legacy
        // push allocates first + 1.
        let next = expect_new(
            runtime
                .consume_command("action_request", action_command(""))
                .expect("legacy push must register"),
        );
        assert_eq!(next, first + 1);

        // The existing handle remains answerable (reply channel re-armed).
        let response = runtime
            .respond(first, InteractionAnswer::Action(ActionResponse::Approve))
            .expect("answerable after reattachment");
        assert_eq!(
            serde_json::to_value(&response).unwrap(),
            serde_json::json!({"kind": "generic", "name": "action", "data": {"type": "approve"}})
        );
    }

    /// Re-push onto an Answered interaction with the same identity: the
    /// stored answer is rebuilt into the wire response and delivered
    /// directly (`Answered`), the state is not modified, the pending face
    /// stays empty, and further re-pushes are equally idempotent.
    #[test]
    fn dedup_answered_repush_reanswers_idempotently() {
        let mut runtime = ActivityRuntime::new("act-1");
        let id = expect_new(
            runtime
                .consume_command("action_request", action_command("req-1"))
                .expect("first push must register"),
        );
        let original = runtime
            .respond(
                id,
                InteractionAnswer::Action(ActionResponse::Reject {
                    reason: "no".into(),
                }),
            )
            .expect("answer the pending card");
        assert!(runtime.pending_interactions().is_empty());

        for push in 1..=2 {
            let response = match runtime
                .consume_command("action_request", action_command("req-1"))
                .expect("re-push must register")
            {
                ConsumeOutcome::Answered(response) => response,
                other => panic!("push {push}: expected idempotent re-answer, got {other:?}"),
            };
            assert_eq!(
                serde_json::to_value(&response).unwrap(),
                serde_json::to_value(&original).unwrap(),
                "push {push}: the stored answer is rebuilt verbatim"
            );
        }

        // State untouched by the re-answers: a direct respond is still
        // rejected as already-answered, and no new entry ever appeared.
        let err = runtime
            .respond(id, InteractionAnswer::Action(ActionResponse::Approve))
            .unwrap_err();
        assert!(matches!(err, InteractionError::AlreadyAnswered { .. }));
        assert!(runtime.pending_interactions().is_empty());
    }

    /// Empty requestId = legacy serve without an identity (COMPAT path):
    /// every push allocates fresh, duplicates included.
    #[test]
    fn dedup_empty_request_id_allocates_every_time() {
        let mut runtime = ActivityRuntime::new("act-1");
        let first = expect_new(
            runtime
                .consume_command("action_request", action_command(""))
                .expect("first push must register"),
        );
        let second = expect_new(
            runtime
                .consume_command("action_request", action_command(""))
                .expect("duplicate push must register"),
        );
        assert_ne!(first, second, "legacy path allocates every time");
        assert_eq!(runtime.pending_interactions().len(), 2);
    }

    /// Question payloads dedup symmetrically (`QuestionPayload.request_id`),
    /// and the question-shaped answer is what gets re-delivered.
    #[test]
    fn dedup_question_payload_identity() {
        let mut runtime = ActivityRuntime::new("act-1");
        let command = serde_json::json!({
            "question": "continue?",
            "options": ["yes", "no"],
            "requestId": "q-1"
        });
        let id = expect_new(
            runtime
                .consume_command("question", command.clone())
                .expect("first push must register"),
        );
        runtime
            .respond(
                id,
                InteractionAnswer::Question(QuestionResponse::Answer {
                    content: "yes".into(),
                }),
            )
            .expect("answer the pending card");

        let response = match runtime
            .consume_command("question", command)
            .expect("re-push must register")
        {
            ConsumeOutcome::Answered(response) => response,
            other => panic!("expected idempotent re-answer, got {other:?}"),
        };
        assert_eq!(
            serde_json::to_value(&response).unwrap(),
            serde_json::json!({
                "kind": "generic",
                "name": "question",
                "data": {"type": "answer", "content": "yes"}
            })
        );
        assert!(runtime.pending_interactions().is_empty());
    }

    /// A re-push onto a `Degraded` record allocates a fresh answerable
    /// interaction (`Degraded` is terminal; the re-push is the second
    /// chance to deliver). Kind scoping: the same requestId under a
    /// different kind never dedups.
    #[test]
    fn dedup_degraded_repush_allocates_fresh_and_kind_scopes() {
        let mut runtime = ActivityRuntime::new("act-1");
        let first = expect_new(
            runtime
                .consume_command("action_request", action_command("req-1"))
                .expect("first push must register"),
        );
        runtime
            .degrade(first, "transport gone")
            .expect("degrade the pending card");

        let second = expect_new(
            runtime
                .consume_command("action_request", action_command("req-1"))
                .expect("re-push after degrade must register"),
        );
        assert_ne!(second, first, "a degraded record never reattaches");
        assert_eq!(
            runtime.pending_interactions().len(),
            1,
            "the fresh object is the only answerable card"
        );

        // Kind scoping: a question with the same requestId is a distinct
        // identity (lookups match on kind + request_id).
        let question = expect_new(
            runtime
                .consume_command(
                    "question",
                    serde_json::json!({"question": "continue?", "options": [], "requestId": "req-1"}),
                )
                .expect("question push must register"),
        );
        assert_ne!(question, second, "kind is part of the lookup key");
    }

    /// End to end: a `sensitive_info_operation` command registers as
    /// Pending (the payload parses and the origin chain passes through
    /// with it), and the answer produces the protocol-contract wire shape:
    /// response name `sensitive_info_operation` (xacpp
    /// `events::interaction` module doc) with the response serde as data.
    #[test]
    fn sensitive_info_operation_end_to_end_wire_shape() {
        let mut runtime = ActivityRuntime::new("act-1");

        let err = runtime
            .consume_command(
                "sensitive_info_operation",
                serde_json::json!({"operation": {"type": "collect"}}),
            )
            .unwrap_err();
        assert!(matches!(err, InteractionError::MalformedPayload { .. }));

        let id = consume_sensitive_info_operation(&mut runtime);
        let pending = runtime.pending_interactions();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, id);
        assert_eq!(pending[0].kind, InteractionKind::SensitiveInfoOperation);
        assert_eq!(pending[0].state, InteractionState::Pending);
        match &pending[0].payload {
            InteractionPayload::SensitiveInfoOperation(payload) => {
                assert_eq!(payload.origin, vec!["child-act".to_string()]);
                match &payload.operation {
                    xacpp::wire::events::SensitiveInfoOperation::Collect { items } => {
                        assert_eq!(items.len(), 1);
                        assert_eq!(items[0].key, "API_KEY");
                        assert_eq!(
                            items[0].si_type,
                            xacpp::wire::events::SensitiveInfoType::Secret
                        );
                    }
                    other => panic!("expected Collect operation, got {other:?}"),
                }
            }
            other => panic!("expected SensitiveInfoOperation payload, got {other:?}"),
        }

        let response = runtime
            .respond(
                id,
                InteractionAnswer::SensitiveInfoOperation(SensitiveInfoOperationResponse {
                    results: vec![SensitiveInfoResult::Provided {
                        key: "API_KEY".into(),
                        value: "s3cret".into(),
                    }],
                }),
            )
            .unwrap();
        assert_eq!(
            serde_json::to_value(&response).unwrap(),
            serde_json::json!({
                "kind": "generic",
                "name": "sensitive_info_operation",
                "data": {"results": [{"type": "provided", "key": "API_KEY", "value": "s3cret"}]}
            })
        );
        assert!(runtime.pending_interactions().is_empty());
    }

    /// Kind mismatch: a SensitiveInfoOperation interaction rejects answers
    /// of the other kinds and stays Pending.
    #[test]
    fn sensitive_info_operation_rejects_kind_mismatch() {
        let mut runtime = ActivityRuntime::new("act-1");
        let id = consume_sensitive_info_operation(&mut runtime);
        let err = runtime
            .respond(id, InteractionAnswer::Action(ActionResponse::Approve))
            .unwrap_err();
        assert!(matches!(err, InteractionError::KindMismatch));
        assert_eq!(runtime.pending_interactions().len(), 1);
    }

    /// All five answer variants produce the wire shapes both reference
    /// implementations agree on: XacppResponse::generic with response name
    /// action/question and the typed serde as data.
    #[test]
    fn respond_builds_agreed_wire_shapes() {
        let mut runtime = ActivityRuntime::new("act-1");
        let action_id = consume_action(&mut runtime);
        let question_id = consume_question(&mut runtime);

        // approve
        let response = runtime
            .respond(
                action_id,
                InteractionAnswer::Action(ActionResponse::Approve),
            )
            .unwrap();
        assert_eq!(
            serde_json::to_value(&response).unwrap(),
            serde_json::json!({"kind": "generic", "name": "action", "data": {"type": "approve"}})
        );

        // approve_always (fresh interaction)
        let id = consume_action(&mut runtime);
        let response = runtime
            .respond(id, InteractionAnswer::Action(ActionResponse::ApproveAlways))
            .unwrap();
        assert_eq!(
            serde_json::to_value(&response).unwrap()["data"],
            serde_json::json!({"type": "approve_always"})
        );

        // reject { reason }
        let id = consume_action(&mut runtime);
        let response = runtime
            .respond(
                id,
                InteractionAnswer::Action(ActionResponse::Reject {
                    reason: "no".into(),
                }),
            )
            .unwrap();
        assert_eq!(
            serde_json::to_value(&response).unwrap()["data"],
            serde_json::json!({"type": "reject", "reason": "no"})
        );

        // answer { content }
        let response = runtime
            .respond(
                question_id,
                InteractionAnswer::Question(QuestionResponse::Answer {
                    content: "yes".into(),
                }),
            )
            .unwrap();
        assert_eq!(
            serde_json::to_value(&response).unwrap(),
            serde_json::json!({
                "kind": "generic", "name": "question",
                "data": {"type": "answer", "content": "yes"}
            })
        );

        // skip { reason }
        let id = consume_question(&mut runtime);
        let response = runtime
            .respond(
                id,
                InteractionAnswer::Question(QuestionResponse::Skip {
                    reason: Some("later".into()),
                }),
            )
            .unwrap();
        assert_eq!(
            serde_json::to_value(&response).unwrap()["data"],
            serde_json::json!({"type": "skip", "reason": "later"})
        );
    }

    /// Pending → Answered is one-way: duplicates and unknown ids rejected;
    /// answered interactions leave the pending snapshot.
    #[test]
    fn respond_state_machine_and_rejections() {
        let mut runtime = ActivityRuntime::new("act-1");
        let action_id = consume_action(&mut runtime);

        let response = runtime
            .respond(
                action_id,
                InteractionAnswer::Action(ActionResponse::Approve),
            )
            .unwrap();
        assert!(matches!(
            response,
            XacppResponse::Generic { ref name, .. } if name == "action"
        ));

        let err = runtime
            .respond(
                action_id,
                InteractionAnswer::Action(ActionResponse::Approve),
            )
            .unwrap_err();
        assert!(matches!(err, InteractionError::AlreadyAnswered { id } if id == action_id));

        let err = runtime
            .respond(999, InteractionAnswer::Action(ActionResponse::Approve))
            .unwrap_err();
        assert!(matches!(err, InteractionError::UnknownId(999)));

        assert!(runtime.pending_interactions().is_empty());
    }

    /// Kind mismatch: an Action interaction rejects a Question answer.
    #[test]
    fn respond_rejects_kind_mismatch() {
        let mut runtime = ActivityRuntime::new("act-1");
        let action_id = consume_action(&mut runtime);
        let err = runtime
            .respond(
                action_id,
                InteractionAnswer::Question(QuestionResponse::Answer {
                    content: "yes".into(),
                }),
            )
            .unwrap_err();
        assert!(matches!(err, InteractionError::KindMismatch));
        // Still pending: the wrong-kind answer changed nothing.
        assert_eq!(runtime.pending_interactions().len(), 1);
    }

    /// Explicit degradation: state records the reason, later respond is
    /// rejected, and degrading an answered interaction is rejected.
    #[test]
    fn degrade_records_reason_and_blocks_later_respond() {
        let mut runtime = ActivityRuntime::new("act-1");
        let degraded_id = consume_action(&mut runtime);
        runtime.degrade(degraded_id, "connection lost").unwrap();

        assert!(
            runtime
                .pending_interactions()
                .iter()
                .all(|interaction| interaction.id != degraded_id),
            "degraded leaves the pending snapshot"
        );

        // Full state inspection through the answered/degraded record.
        let err = runtime
            .respond(
                degraded_id,
                InteractionAnswer::Action(ActionResponse::Approve),
            )
            .unwrap_err();
        assert!(matches!(err, InteractionError::AlreadyAnswered { .. }));

        let answered_id = consume_action(&mut runtime);
        runtime
            .respond(
                answered_id,
                InteractionAnswer::Action(ActionResponse::Approve),
            )
            .unwrap();
        let err = runtime.degrade(answered_id, "too late").unwrap_err();
        assert!(matches!(err, InteractionError::AlreadyAnswered { .. }));
    }

    /// pending_interactions returns an independent snapshot.
    #[test]
    fn pending_interactions_snapshot_is_independent() {
        let mut runtime = ActivityRuntime::new("act-1");
        consume_action(&mut runtime);
        let mut snapshot = runtime.pending_interactions();
        assert_eq!(snapshot.len(), 1);
        snapshot.clear();
        assert_eq!(runtime.pending_interactions().len(), 1);
    }
}
