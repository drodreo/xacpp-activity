//! Conversion layer: the single entry point for all event sources.
//!
//! One event stream, one path. Every event — arriving one by one on the
//! live stream or in a batch pulled from `conversation_history` — flows
//! through the same [`ActivityModel::apply_one`]; arrival pacing is not a
//! processing concept. A pulled batch replays events; identity hits against
//! already-built entries make the replay converge idempotently.
//!
//! Identity-addressed merge, no state machines:
//! - user bubble — stream order; adjacent bubbles with identical canonical
//!   inputs (a re-pushed `start`) merge at snapshot derivation. Hidden
//!   part types (non-`default`/`references`) never enter the entry; a
//!   media-only input is kept verbatim.
//! - think block — identity `pair` (one think block per loop iteration on
//!   the wire; live and projection part ids must not split it).
//! - content block — identity `(pair, partId)`.
//! - tool card — identity `requestId`. ToolUse opens the card; ToolResult
//!   advances it by exactly three status-driven rules: terminal statuses
//!   complete the card (never demoted by a late `Running` placeholder),
//!   `Running` refreshes the content of a card that is not already
//!   terminal, and an unknown status refreshes the content while keeping
//!   the known status (unknown never erases a known state). A late ToolUse
//!   only completes the declaration of the card an orphan result opened —
//!   never its lifecycle. The lifecycle itself is derived (`ToolCard::
//!   done`), not stored: no second running/done state exists.
//! - process message — stream order.
//!
//! Block evolution, one rule per identity: deltas append → a full part
//! seals the block (later deltas for the identity drop) → a sealed or
//! closed block ignores further increments. A closed pair (its
//! `pair_complete` — or the turn-level `complete` settling unclosed pairs —
//! has arrived) ignores further block events entirely: the engine produces
//! no new content for a closed loop, and a projection replay of a closed
//! pair must stay a no-op. PairComplete uses the pair the event itself
//! names (never an active-pointer guess); Complete settles every still-open
//! pair and freezes unclosed blocks.
//!
//! There is no drop-without-a-home: every event has a deterministic
//! destination in every arrival order.
//!
//! Live-stream discipline (binding-layer contract): live events render
//! only; PairComplete triggers the authoritative history pull whose batch
//! re-enters through this same path.

use xacpp::wire::events::{ContentPart, XacppEvent};

use crate::envelope::ActivityEventEnvelope;
use crate::error::ActivityError;
use crate::event::{ActivityEvent, ToolResultStatus};
use crate::model::{
    ActivityModel, ProcessLevel, ProcessMessage, RenderEntry, ToolCard, ToolResult,
    canonical_inputs_hash, user_visible_inputs,
};

/// Where an event came from. All three flow through the same per-event
/// handler; the source only gates the live-only `turn_completed` advance
/// (a replay or history batch must not fake "the current turn closed").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventSource {
    /// Real-time stream.
    Live,
    /// Current-pair buffer replay (reconnect recovery; a rebuild channel —
    /// consumed on a fresh session).
    BufferReplay,
    /// `conversation_history` projection (authoritative pull).
    HistoryProjection,
}

/// Wire prefix of projection-replayed part ids (the `conversation_history`
/// projection re-expresses persisted blocks as `replay:thinking:{pair}` /
/// `replay:text:{pair}:{idx}`; the live stream carries provider part ids).
const REPLAY_PART_PREFIX: &str = "replay:";

impl ActivityModel {
    /// Applies one live-stream envelope.
    pub fn apply_envelope(&mut self, envelope: &ActivityEventEnvelope) {
        self.apply_one(&envelope.payload, EventSource::Live);
    }

    /// Applies a batch of buffered envelopes (current-pair replay).
    pub fn apply_buffer_replay(
        &mut self,
        envelopes: impl IntoIterator<Item = ActivityEventEnvelope>,
    ) {
        self.replay_user_seen.clear();
        for envelope in envelopes {
            self.apply_one(&envelope.payload, EventSource::BufferReplay);
        }
    }

    /// Applies pulled history events. The batch walks the same per-event
    /// path as the live stream: identity hits evolve entries in place,
    /// identity misses append — re-pulling a window that was already
    /// consumed leaves the projection unchanged (snapshot-level
    /// idempotence).
    pub fn apply_history_events(&mut self, events: impl IntoIterator<Item = XacppEvent>) {
        self.replay_user_seen.clear();
        for event in events {
            match ActivityEvent::from_xacpp_event(&event) {
                Ok(typed) => self.apply_one(&typed, EventSource::HistoryProjection),
                Err(ActivityError::UnknownEvent(name)) => {
                    log::warn!("[reducer] dropping unknown history event `{name}`");
                }
                Err(err) => {
                    log::warn!(
                        "[reducer] dropping malformed history event `{}`: {err}",
                        event.name
                    );
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // The single per-event entry.
    // ------------------------------------------------------------------

    fn apply_one(&mut self, event: &ActivityEvent, source: EventSource) {
        match event {
            ActivityEvent::Start(payload) => {
                self.last_inputs = payload.inputs.clone();
                let inputs = user_visible_inputs(&payload.inputs);
                if inputs.is_empty() {
                    return;
                }
                // Live starts are always a fresh user send — identical
                // content twice is legitimate and must not be swallowed.
                // A replayed start is a re-delivery only while its nth
                // in-batch occurrence still has a same-hash entry behind
                // it; past that, the window carries genuinely new input.
                if source != EventSource::Live && self.replayed_user_duplicate(&inputs) {
                    return;
                }
                self.entries.push(RenderEntry::User { inputs, seq: 0 });
            }

            // ---- Think block (identity: pair) ----
            ActivityEvent::ThinkStart(p) => {
                if self.pair_closed(&p.pair) {
                    self.warn_late("think_start", &p.pair);
                    return;
                }
                if self.think_index(&p.pair).is_none() {
                    self.entries.push(RenderEntry::Think {
                        pair: p.pair.clone(),
                        content: String::new(),
                        streaming: true,
                        sealed: false,
                        last_delta: None,
                    });
                }
                // Hit: idempotent (the earlier delta/part already built it).
            }
            ActivityEvent::ThinkDelta(p) => {
                if self.pair_closed(&p.pair) {
                    self.warn_late("think", &p.pair);
                    return;
                }
                match self.think_index(&p.pair) {
                    Some(idx) => {
                        let RenderEntry::Think {
                            content,
                            sealed,
                            last_delta,
                            ..
                        } = &mut self.entries[idx]
                        else {
                            unreachable!("think_index guarantees the variant");
                        };
                        if *sealed || last_delta.as_deref() == Some(p.content.as_str()) {
                            return;
                        }
                        content.push_str(&p.content);
                        *last_delta = Some(p.content.clone());
                    }
                    None => {
                        // Implicit open: a delta ahead of its boundary.
                        self.entries.push(RenderEntry::Think {
                            pair: p.pair.clone(),
                            content: p.content.clone(),
                            streaming: true,
                            sealed: false,
                            last_delta: Some(p.content.clone()),
                        });
                    }
                }
            }
            ActivityEvent::ThinkPart(p) => {
                if self.pair_closed(&p.pair) {
                    self.warn_late("think_part", &p.pair);
                    return;
                }
                match self.think_index(&p.pair) {
                    Some(idx) => {
                        let RenderEntry::Think {
                            content, sealed, ..
                        } = &mut self.entries[idx]
                        else {
                            unreachable!("think_index guarantees the variant");
                        };
                        *content = p.content.clone();
                        *sealed = true;
                    }
                    None => self.entries.push(RenderEntry::Think {
                        pair: p.pair.clone(),
                        content: p.content.clone(),
                        streaming: true,
                        sealed: true,
                        last_delta: None,
                    }),
                }
            }
            ActivityEvent::ThinkEnd(p) => {
                // Terminal boundary: idempotent on every channel — but a
                // live-channel arrival for an already-closed pair is the
                // late-event anomaly and warns (a pulled replay of a
                // consumed window stays silent).
                if source != EventSource::HistoryProjection && self.pair_closed(&p.pair) {
                    self.warn_late("think_end", &p.pair);
                    return;
                }
                if let Some(idx) = self.think_index(&p.pair)
                    && let RenderEntry::Think { streaming, .. } = &mut self.entries[idx]
                {
                    *streaming = false;
                }
            }

            // ---- Content blocks (identity: pair + partId) ----
            ActivityEvent::ContentStart(p) => {
                if self.pair_closed(&p.pair) {
                    self.warn_late("content_start", &p.pair);
                    return;
                }
                if self.content_index(&p.pair, &p.part_id).is_none()
                    && self.replay_cover_target(&p.pair, &p.part_id).is_none()
                {
                    self.entries.push(RenderEntry::Content {
                        pair: p.pair.clone(),
                        part_id: p.part_id.clone(),
                        text: String::new(),
                        media: Vec::new(),
                        streaming: true,
                        sealed: false,
                        last_delta: None,
                    });
                }
                // Hit (identity or replay cover): idempotent — the block
                // already exists.
            }
            ActivityEvent::ContentDelta(e) => {
                if self.pair_closed(&e.pair) {
                    self.warn_late("content_delta", &e.pair);
                    return;
                }
                let dedup = serde_json::to_string(&e.payload).ok();
                match content_part_id(&e.payload) {
                    Some(part_id) => match self.content_index(&e.pair, part_id) {
                        Some(idx) => {
                            self.evolve_content(idx, part_id, &e.payload, dedup.as_deref(), false);
                        }
                        None => {
                            // Replay-prefixed delta for a block the live
                            // stream is still building: append there.
                            let idx = match self.replay_cover_target(&e.pair, part_id) {
                                Some(idx) => idx,
                                None => self.open_content(&e.pair, part_id),
                            };
                            self.evolve_content(idx, part_id, &e.payload, dedup.as_deref(), false);
                        }
                    },
                    None => {
                        // No routing key: append to the pair's last
                        // streaming block; implicit open when none exists.
                        if let Some(idx) = self.last_streaming_content(&e.pair) {
                            self.evolve_content(idx, "", &e.payload, dedup.as_deref(), false);
                        } else {
                            let idx = self.open_content(&e.pair, "");
                            self.evolve_content(idx, "", &e.payload, dedup.as_deref(), false);
                        }
                    }
                }
            }
            ActivityEvent::ContentPart(e) => {
                if self.pair_closed(&e.pair) {
                    self.warn_late("content_part", &e.pair);
                    return;
                }
                match content_part_id(&e.payload) {
                    Some(part_id) => match self.content_index(&e.pair, part_id) {
                        Some(idx) => self.evolve_content(idx, part_id, &e.payload, None, true),
                        None => {
                            // Projection replay part ids (`replay:` prefix)
                            // re-express blocks the live stream may already
                            // be building: with an open streaming block in
                            // this pair, the part is that block's terminal
                            // state (the window carries the whole persisted
                            // message; block order matches the stream).
                            if let Some(idx) = self.replay_cover_target(&e.pair, part_id) {
                                self.evolve_content(idx, part_id, &e.payload, None, true);
                            } else {
                                let idx = self.open_content(&e.pair, part_id);
                                self.evolve_content(idx, part_id, &e.payload, None, true);
                            }
                        }
                    },
                    None => {
                        // No routing key: replace the pair's last block
                        // (any seal state — keyless media parts ride the
                        // streaming tail); implicit open when none exists.
                        if let Some(idx) = self.last_content(&e.pair) {
                            self.evolve_content(idx, "", &e.payload, None, true);
                        } else {
                            let idx = self.open_content(&e.pair, "");
                            self.evolve_content(idx, "", &e.payload, None, true);
                        }
                    }
                }
            }
            ActivityEvent::ContentEnd(p) => {
                if source != EventSource::HistoryProjection && self.pair_closed(&p.pair) {
                    self.warn_late("content_end", &p.pair);
                    return;
                }
                if let Some(idx) = self.content_index(&p.pair, &p.part_id)
                    && let RenderEntry::Content { streaming, .. } = &mut self.entries[idx]
                {
                    *streaming = false;
                }
            }

            // ---- Tool cards (identity: requestId) ----
            ActivityEvent::ToolUse(e) => match self.card_index(&e.request_id) {
                Some(idx) => {
                    // Late / replayed declaration: complete the fields the
                    // card is missing (an orphan result opened it empty);
                    // never touch its lifecycle.
                    let RenderEntry::Tool(card) = &mut self.entries[idx] else {
                        unreachable!("card_index guarantees the variant");
                    };
                    if card.tool_name.is_empty() {
                        card.tool_name = e.tool_name.clone();
                    }
                    if card.arguments.is_empty() {
                        card.arguments = e.arguments.clone();
                    }
                    if card.intent.is_empty() {
                        card.intent = e.intent.clone();
                    }
                }
                None => {
                    self.entries.push(RenderEntry::Tool(ToolCard {
                        request_id: e.request_id.clone(),
                        tool_name: e.tool_name.clone(),
                        index: e.index,
                        arguments: e.arguments.clone(),
                        intent: e.intent.clone(),
                        result: None,
                    }));
                }
            },
            ActivityEvent::ToolResult(e) => {
                // Merge rules (exactly three, status-driven):
                //   terminal → the card takes the result (done);
                //   Running  → refresh the content of a card that is not
                //              already terminal (a terminal card is never
                //              demoted by a late Running placeholder);
                //   unknown  → refresh the content but keep the card's
                //              known status (unknown never erases a known
                //              state).
                let incoming = ToolResult {
                    parts: e.parts.clone(),
                    status: e.status,
                    is_background: e.is_background,
                    is_error: e.is_error,
                    auto: e.auto,
                };
                match self.card_index(&e.request_id) {
                    Some(idx) => {
                        let RenderEntry::Tool(card) = &mut self.entries[idx] else {
                            unreachable!("card_index guarantees the variant");
                        };
                        match e.status {
                            Some(
                                ToolResultStatus::Completed
                                | ToolResultStatus::Cancelled
                                | ToolResultStatus::Error,
                            ) => {
                                card.result = Some(incoming);
                            }
                            Some(ToolResultStatus::Running) => {
                                if card.done() != Some(true) {
                                    card.result = Some(incoming);
                                }
                            }
                            None => match &mut card.result {
                                Some(existing) => {
                                    existing.parts = incoming.parts;
                                    existing.is_error = incoming.is_error;
                                    existing.auto = incoming.auto;
                                    existing.is_background =
                                        incoming.is_background.or(existing.is_background);
                                    // status untouched: known stays known.
                                }
                                None => card.result = Some(incoming),
                            },
                        }
                    }
                    None => {
                        // Orphan: the ToolUse never arrived (isolated Tool
                        // message projection / window cut). Open the card
                        // from the result itself; the lifecycle derives
                        // from the status like any other card.
                        self.entries.push(RenderEntry::Tool(ToolCard {
                            request_id: e.request_id.clone(),
                            tool_name: e.tool_name.clone(),
                            index: e.index,
                            arguments: String::new(),
                            intent: String::new(),
                            result: Some(incoming),
                        }));
                    }
                }
            }

            // ---- Turn lifecycle ----
            ActivityEvent::PairComplete(p) => {
                let known_cache = self
                    .latest_pair_complete
                    .as_ref()
                    .filter(|current| current.pair == p.pair)
                    .and_then(|current| current.token_usage.cache_hit_tokens);
                let mut latest = p.clone();
                // History currently cannot persist provider cache facts. An
                // unknown replay of the same pair must not erase the live
                // observation; a different pair always replaces the snapshot.
                if latest.token_usage.cache_hit_tokens.is_none() {
                    latest.token_usage.cache_hit_tokens = known_cache;
                }
                self.latest_pair_complete = Some(latest);
                self.close_pair(&p.pair);
            }
            ActivityEvent::Complete(e) => {
                // Turn-level close: settle every still-open pair (the
                // cancel path's only terminal signal — the killed loop never
                // settles or persists). Idempotent: with nothing open this
                // is a no-op.
                let open: Vec<String> = {
                    let mut pairs: std::collections::HashSet<String> = Default::default();
                    for entry in &self.entries {
                        if let Some(pair) = entry.pair() {
                            pairs.insert(pair.to_string());
                        }
                    }
                    let mut open: Vec<String> = pairs
                        .into_iter()
                        .filter(|pair| !self.finalized_pairs.contains(pair))
                        .collect();
                    open.sort();
                    open
                };
                for pair in &open {
                    self.close_pair(pair);
                }
                if source == EventSource::Live {
                    self.turn_completed += 1;
                }
                // Kept verbatim: consumers read the final assistant reply.
                self.side_events.push(ActivityEvent::Complete(e.clone()));
            }

            // ---- Process messages (stream order) ----
            ActivityEvent::Info(e) => self.process.push(ProcessMessage {
                level: ProcessLevel::Info,
                title: e.title.clone(),
                content: e.content.clone(),
            }),
            ActivityEvent::Warn(e) => self.process.push(ProcessMessage {
                level: ProcessLevel::Warn,
                title: e.title.clone(),
                content: e.content.clone(),
            }),
            ActivityEvent::Error(e) => self.process.push(ProcessMessage {
                level: ProcessLevel::Error,
                title: e.title.clone(),
                content: e.content.clone(),
            }),
            ActivityEvent::Notify(e) => self.process.push(ProcessMessage {
                level: ProcessLevel::Notify,
                title: String::new(),
                content: e.message.clone(),
            }),

            // Side events kept verbatim (complete, security_alert, upload,
            // sub-activity lifecycle, waiting_command, checklist_update,
            // resource_diff).
            other => self.side_events.push(other.clone()),
        }
    }

    // ------------------------------------------------------------------
    // Identity lookups (linear scans; the entry count is bounded by the
    // steady-state cap plus the in-flight tail).
    // ------------------------------------------------------------------

    /// Whether a replayed user start is a re-delivery. The batch's nth
    /// occurrence of these canonical inputs (counted in `replay_user_seen`)
    /// duplicates the entries while they already hold at least n same-hash
    /// user entries; user entries are append-only (the close-pair sweep
    /// drops only empty think/content blocks), so the scan reflects every
    /// entry ever pushed.
    fn replayed_user_duplicate(&mut self, inputs: &[ContentPart]) -> bool {
        let hash = canonical_inputs_hash(inputs);
        let occurrence = self
            .replay_user_seen
            .entry(hash)
            .and_modify(|count| *count += 1)
            .or_insert(1);
        let held = self
            .entries
            .iter()
            .filter(|entry| {
                matches!(entry, RenderEntry::User { inputs, .. } if canonical_inputs_hash(inputs) == hash)
            })
            .count();
        *occurrence <= held
    }

    /// Whether the pair's turn is already closed.
    fn pair_closed(&self, pair: &str) -> bool {
        self.finalized_pairs.contains(pair)
    }

    /// Index of the pair's think entry (identity: pair).
    fn think_index(&self, pair: &str) -> Option<usize> {
        self.entries
            .iter()
            .position(|entry| matches!(entry, RenderEntry::Think { pair: p, .. } if p == pair))
    }

    /// Index of the content entry with identity `(pair, part_id)`.
    fn content_index(&self, pair: &str, part_id: &str) -> Option<usize> {
        self.entries.iter().position(|entry| {
            matches!(entry, RenderEntry::Content { pair: p, part_id: id, .. }
                if p == pair && id == part_id)
        })
    }

    /// Index of the pair's last streaming (unsealed) content entry — the
    /// append target of a keyless delta.
    fn last_streaming_content(&self, pair: &str) -> Option<usize> {
        self.entries.iter().rposition(|entry| {
            matches!(entry, RenderEntry::Content { pair: p, sealed, .. }
                if p == pair && !sealed)
        })
    }

    /// Index of the pair's last content entry (any seal state) — the
    /// replace target of a keyless part.
    fn last_content(&self, pair: &str) -> Option<usize> {
        self.entries
            .iter()
            .rposition(|entry| matches!(entry, RenderEntry::Content { pair: p, .. } if p == pair))
    }

    /// Index of the card with identity `request_id`.
    fn card_index(&self, request_id: &str) -> Option<usize> {
        self.entries.iter().position(
            |entry| matches!(entry, RenderEntry::Tool(card) if card.request_id == request_id),
        )
    }

    /// The replay-cover target: when a projection `replay:`-prefixed part
    /// misses its `(pair, partId)` identity, it is the terminal state of
    /// the pair's currently open streaming block — return that block's
    /// index. `None` = genuinely new block (open pair with nothing
    /// streaming, or a non-replay part id).
    fn replay_cover_target(&self, pair: &str, part_id: &str) -> Option<usize> {
        if !part_id.starts_with(REPLAY_PART_PREFIX) {
            return None;
        }
        self.entries.iter().rposition(|entry| {
            matches!(entry, RenderEntry::Content { pair: p, sealed, .. }
                if p == pair && !sealed)
        })
    }

    /// Opens a content entry (identity `(pair, part_id)`); returns its
    /// index.
    fn open_content(&mut self, pair: &str, part_id: &str) -> usize {
        self.entries.push(RenderEntry::Content {
            pair: pair.to_string(),
            part_id: part_id.to_string(),
            text: String::new(),
            media: Vec::new(),
            streaming: true,
            sealed: false,
            last_delta: None,
        });
        self.entries.len() - 1
    }

    /// Evolves one content entry in place: delta appends (deduped), part
    /// replaces and seals.
    fn evolve_content(
        &mut self,
        idx: usize,
        part_id: &str,
        part: &ContentPart,
        dedup: Option<&str>,
        is_part: bool,
    ) {
        let RenderEntry::Content {
            text,
            media,
            sealed,
            last_delta,
            ..
        } = &mut self.entries[idx]
        else {
            unreachable!("index comes from a content lookup");
        };
        if is_part {
            // Full snapshot: replace and seal (later deltas for the
            // identity drop).
            match part {
                ContentPart::Text(t) => {
                    *text = t.text.clone();
                    media.clear();
                }
                other => {
                    text.clear();
                    *media = vec![other.clone()];
                }
            }
            *sealed = true;
            return;
        }
        // Delta: drop when sealed or an adjacent duplicate.
        if *sealed || last_delta.as_deref() == dedup {
            return;
        }
        match part {
            ContentPart::Text(t) => text.push_str(&t.text),
            other => media.push(other.clone()),
        }
        if !part_id.is_empty() && dedup.is_some() {
            *last_delta = dedup.map(str::to_string);
        }
    }

    /// Closes the pair's turn: records the fact, settles streaming flags,
    /// and drops the pair's empty blocks (a think part / content delta that
    /// never carried content leaves no entry behind).
    fn close_pair(&mut self, pair: &str) {
        if !self.finalized_pairs.insert(pair.to_string()) {
            return; // already closed — idempotent
        }
        log::info!("[reducer] turn closed: pair={pair}");
        self.entries.retain(|entry| match entry {
            RenderEntry::Think {
                pair: p, content, ..
            } if p == pair => !content.is_empty(),
            RenderEntry::Content {
                pair: p,
                text,
                media,
                ..
            } if p == pair => !text.is_empty() || !media.is_empty(),
            _ => true,
        });
        for entry in &mut self.entries {
            match entry {
                RenderEntry::Think {
                    pair: p, streaming, ..
                } if p == pair => {
                    *streaming = false;
                }
                RenderEntry::Content {
                    pair: p, streaming, ..
                } if p == pair => {
                    *streaming = false;
                }
                _ => {}
            }
        }
    }

    /// Warn helper for late events on a closed pair (the engine produces no
    /// new content for a closed loop; a stream of these marks a
    /// replay/duplication anomaly upstream).
    fn warn_late(&self, event: &str, pair: &str) {
        log::warn!("[reducer] dropping late `{event}` for closed pair `{pair}`");
    }
}

/// Routing key of a content part (xacpp exposes `part_id` per part variant).
fn content_part_id(part: &ContentPart) -> Option<&str> {
    match part {
        ContentPart::Text(p) => p.part_id.as_deref(),
        ContentPart::Image(p) => p.part_id.as_deref(),
        ContentPart::Audio(p) => p.part_id.as_deref(),
        ContentPart::Video(p) => p.part_id.as_deref(),
        ContentPart::File(p) => p.part_id.as_deref(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{
        BlockBoundaryPayload, CompletePayload, PairCompletePayload, StartPayload,
        ThinkContentPayload, ToolResultPayload, ToolResultStatus,
    };
    use crate::model::{SnapshotItem, TurnStatus, inputs_text};

    fn think_start(pair: &str, part_id: &str) -> ActivityEvent {
        ActivityEvent::ThinkStart(BlockBoundaryPayload {
            round: "r1".into(),
            pair: pair.into(),
            part_id: part_id.into(),
        })
    }

    fn think_delta(pair: &str, part_id: &str, content: &str) -> ActivityEvent {
        ActivityEvent::ThinkDelta(ThinkContentPayload {
            content: content.into(),
            round: "r1".into(),
            pair: pair.into(),
            part_id: part_id.into(),
        })
    }

    fn think_part(pair: &str, part_id: &str, content: &str) -> ActivityEvent {
        ActivityEvent::ThinkPart(ThinkContentPayload {
            content: content.into(),
            round: "r1".into(),
            pair: pair.into(),
            part_id: part_id.into(),
        })
    }

    fn think_end(pair: &str, part_id: &str) -> ActivityEvent {
        ActivityEvent::ThinkEnd(BlockBoundaryPayload {
            round: "r1".into(),
            pair: pair.into(),
            part_id: part_id.into(),
        })
    }

    fn content_start(pair: &str, part_id: &str) -> ActivityEvent {
        ActivityEvent::ContentStart(BlockBoundaryPayload {
            round: "r1".into(),
            pair: pair.into(),
            part_id: part_id.into(),
        })
    }

    fn content_delta(pair: &str, part_id: &str, text: &str) -> ActivityEvent {
        ActivityEvent::ContentDelta(xacpp::wire::events::ContentDeltaEvent {
            round: "r1".into(),
            pair: pair.into(),
            payload: text_part(part_id, text),
        })
    }

    fn content_part(pair: &str, part_id: &str, text: &str) -> ActivityEvent {
        ActivityEvent::ContentPart(xacpp::wire::events::ContentPartEvent {
            round: "r1".into(),
            pair: pair.into(),
            payload: text_part(part_id, text),
        })
    }

    fn content_end(pair: &str, part_id: &str) -> ActivityEvent {
        ActivityEvent::ContentEnd(BlockBoundaryPayload {
            round: "r1".into(),
            pair: pair.into(),
            part_id: part_id.into(),
        })
    }

    fn text_part(part_id: &str, text: &str) -> xacpp::wire::events::ContentPart {
        xacpp::wire::events::ContentPart::Text(xacpp::wire::events::TextPart {
            text: text.into(),
            part_id: Some(part_id.into()),
            part_type: "default".into(),
        })
    }

    fn tool_use(request_id: &str) -> ActivityEvent {
        ActivityEvent::ToolUse(xacpp::wire::events::ToolUseEvent {
            request_id: request_id.into(),
            tool_name: "shell".into(),
            index: 0,
            arguments: "{}".into(),
            intent: "run".into(),
        })
    }

    fn tool_result(request_id: &str) -> ActivityEvent {
        tool_result_named(request_id, "shell", Some(ToolResultStatus::Completed))
    }

    fn tool_result_named(
        request_id: &str,
        tool_name: &str,
        status: Option<ToolResultStatus>,
    ) -> ActivityEvent {
        ActivityEvent::ToolResult(ToolResultPayload {
            request_id: request_id.into(),
            tool_name: tool_name.into(),
            index: 0,
            parts: vec![text_part("", "result")],
            status,
            is_background: None,
            is_error: false,
            auto: false,
        })
    }

    fn pair_complete(pair: &str) -> ActivityEvent {
        ActivityEvent::PairComplete(PairCompletePayload {
            pair: pair.into(),
            context_window: 1,
            token_usage: Default::default(),
            tier: None,
            model: None,
        })
    }

    fn start(text: &str) -> ActivityEvent {
        ActivityEvent::Start(StartPayload {
            inputs: vec![text_part("", text)],
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

    fn live(model: &mut ActivityModel, events: Vec<ActivityEvent>) {
        for event in events {
            model.apply_one(&event, EventSource::Live);
        }
    }

    fn pull(model: &mut ActivityModel, events: Vec<ActivityEvent>) {
        for event in events {
            model.apply_one(&event, EventSource::HistoryProjection);
        }
    }

    fn wire_of(event: &ActivityEvent) -> xacpp::wire::events::XacppEvent {
        event.to_wire()
    }

    fn snapshot_texts(model: &ActivityModel) -> Vec<String> {
        model
            .snapshot()
            .into_iter()
            .filter_map(|item| match item {
                SnapshotItem::Text { text, .. } => Some(text),
                _ => None,
            })
            .collect()
    }

    fn snapshot_thinks(model: &ActivityModel) -> Vec<String> {
        model
            .snapshot()
            .into_iter()
            .filter_map(|item| match item {
                SnapshotItem::Think { text, .. } => Some(text),
                _ => None,
            })
            .collect()
    }

    fn snapshot_cards(model: &ActivityModel) -> Vec<(String, String, Option<bool>, bool)> {
        model
            .snapshot()
            .into_iter()
            .filter_map(|item| match item {
                SnapshotItem::Tool {
                    request_id,
                    tool_name,
                    done,
                    ..
                } => Some((request_id, tool_name, done, false)),
                _ => None,
            })
            .collect()
    }

    // ---------------------------------------------------------- contracts

    #[test]
    fn pair_complete_retains_latest_actual_usage() {
        let mut m = ActivityModel::new();
        let first = PairCompletePayload {
            pair: "p1".into(),
            context_window: 1_000,
            token_usage: xacpp::wire::events::TokenUsage {
                prompt_tokens: 100,
                completion_tokens: 20,
                total_tokens: 120,
                cache_hit_tokens: Some(90),
                ..Default::default()
            },
            tier: None,
            model: None,
        };
        let second = PairCompletePayload {
            pair: "p2".into(),
            context_window: 2_000,
            token_usage: xacpp::wire::events::TokenUsage {
                prompt_tokens: 200,
                completion_tokens: 40,
                total_tokens: 240,
                cache_hit_tokens: None,
                ..Default::default()
            },
            tier: None,
            model: None,
        };

        live(
            &mut m,
            vec![
                ActivityEvent::PairComplete(first),
                ActivityEvent::PairComplete(second.clone()),
            ],
        );

        let latest = m.latest_pair_complete.as_ref().expect("latest pair usage");
        assert_eq!(latest, &second);
        assert_eq!(latest.token_usage.cache_hit_tokens, None);
    }

    #[test]
    fn same_pair_history_unknown_cache_does_not_erase_live_fact() {
        let mut m = ActivityModel::new();
        let live_payload = PairCompletePayload {
            pair: "p1".into(),
            context_window: 1_000,
            token_usage: xacpp::wire::events::TokenUsage {
                prompt_tokens: 100,
                completion_tokens: 20,
                total_tokens: 120,
                cache_hit_tokens: Some(90),
                ..Default::default()
            },
            tier: None,
            model: None,
        };
        let history_payload = PairCompletePayload {
            token_usage: xacpp::wire::events::TokenUsage {
                total_tokens: 121,
                cache_hit_tokens: None,
                ..live_payload.token_usage.clone()
            },
            ..live_payload.clone()
        };

        live(&mut m, vec![ActivityEvent::PairComplete(live_payload)]);
        pull(&mut m, vec![ActivityEvent::PairComplete(history_payload)]);

        let latest = m.latest_pair_complete.expect("latest pair usage");
        assert_eq!(latest.token_usage.total_tokens, 121);
        assert_eq!(latest.token_usage.cache_hit_tokens, Some(90));
    }

    #[test]
    fn full_turn_modeling_with_tool_card() {
        let mut m = ActivityModel::new();
        live(
            &mut m,
            vec![
                start("run it"),
                think_start("p1", "t0"),
                think_delta("p1", "t0", "thinking "),
                think_end("p1", "t0"),
                content_start("p1", "c0"),
                content_delta("p1", "c0", "hello "),
                tool_use("rq1"),
                tool_result("rq1"),
                content_delta("p1", "c0", "world"),
                content_end("p1", "c0"),
                pair_complete("p1"),
            ],
        );
        assert_eq!(snapshot_thinks(&m), vec!["thinking ".to_string()]);
        assert_eq!(snapshot_texts(&m), vec!["hello world".to_string()]);
        let cards = snapshot_cards(&m);
        assert_eq!(cards.len(), 1);
        assert_eq!(cards[0].0, "rq1");
        assert_eq!(cards[0].1, "shell");
        assert_eq!(cards[0].2, Some(true), "card done");
        let turns = m.get_turns();
        assert_eq!(turns.len(), 1);
        assert_eq!(turns[0].id, "p1");
        assert_eq!(turns[0].status, TurnStatus::Finalized);
        assert!(!m.turn_active(), "turn closed by pair_complete");
    }

    #[test]
    fn delta_and_part_streams_converge() {
        let mut live_model = ActivityModel::new();
        live(
            &mut live_model,
            vec![
                content_start("p1", "c0"),
                content_delta("p1", "c0", "he"),
                content_delta("p1", "c0", "llo"),
                content_end("p1", "c0"),
            ],
        );
        let mut part_model = ActivityModel::new();
        part(&mut part_model);
        assert_eq!(
            snapshot_texts(&live_model),
            snapshot_texts(&part_model),
            "delta stream and part snapshot converge on the same text"
        );
    }

    fn part(m: &mut ActivityModel) {
        live(
            m,
            vec![content_start("p1", "c0"), content_part("p1", "c0", "hello")],
        );
    }

    #[test]
    fn duplicate_deltas_are_idempotent() {
        let mut m = ActivityModel::new();
        let delta = content_delta("p1", "c0", "same");
        live(
            &mut m,
            vec![content_start("p1", "c0"), delta.clone(), delta],
        );
        assert_eq!(snapshot_texts(&m), vec!["same".to_string()]);
    }

    #[test]
    fn part_seals_block_and_later_deltas_drop() {
        let mut m = ActivityModel::new();
        live(
            &mut m,
            vec![
                content_start("p1", "c0"),
                content_part("p1", "c0", "sealed"),
                content_delta("p1", "c0", " late"),
                think_start("p1", "t0"),
                think_part("p1", "t0", "sealed think"),
                think_delta("p1", "t0", " late"),
            ],
        );
        assert_eq!(snapshot_texts(&m), vec!["sealed".to_string()]);
        assert_eq!(snapshot_thinks(&m), vec!["sealed think".to_string()]);
    }

    #[test]
    fn out_of_order_delta_before_start_settles() {
        let mut m = ActivityModel::new();
        live(
            &mut m,
            vec![
                think_delta("p9", "t9", "early"),
                think_start("p9", "t9"),
                think_delta("p9", "t9", " late"),
            ],
        );
        assert_eq!(snapshot_thinks(&m), vec!["early late".to_string()]);
    }

    // NOTE: model-level "re-pull the same window is a no-op" tests were
    // retired with the identity-merge semantics — the latest-window entry
    // now REPLACES the projection with a fresh model (registry layer), and
    // its idempotence is pinned by the session-level rebuild tests
    // (tests/latest_window_rebuild.rs).

    #[test]
    fn pair_complete_switches_turns() {
        let mut m = ActivityModel::new();
        live(
            &mut m,
            vec![
                content_start("p1", "c0"),
                content_part("p1", "c0", "one"),
                pair_complete("p1"),
                content_start("p2", "c1"),
                content_part("p2", "c1", "two"),
                pair_complete("p2"),
            ],
        );
        let turns = m.get_turns();
        assert_eq!(turns.len(), 2);
        assert_eq!(turns[0].contents[0].text, "one");
        assert_eq!(turns[1].contents[0].text, "two");
        assert!(turns.iter().all(|t| t.status == TurnStatus::Finalized));
    }

    #[test]
    fn empty_blocks_are_cleaned_at_close() {
        let mut m = ActivityModel::new();
        live(
            &mut m,
            vec![
                think_start("p1", "t0"),
                think_end("p1", "t0"),
                content_start("p1", "kept"),
                content_part("p1", "kept", "kept"),
                pair_complete("p1"),
            ],
        );
        let turns = m.get_turns();
        assert_eq!(turns.len(), 1);
        assert!(turns[0].think.is_none(), "empty think dropped at close");
        assert_eq!(turns[0].contents.len(), 1);
    }

    #[test]
    fn trace_family_maps_to_process_messages() {
        let mut m = ActivityModel::new();
        live(
            &mut m,
            vec![ActivityEvent::Notify(xacpp::wire::events::NotifyPayload {
                message: "heads up".into(),
            })],
        );
        let items = m.snapshot();
        assert!(items.iter().any(|item| matches!(
            item,
            SnapshotItem::Process { content, .. } if content == "heads up"
        )));
    }

    #[test]
    fn side_events_and_start_inputs() {
        let mut m = ActivityModel::new();
        live(&mut m, vec![start("inputs text"), complete()]);
        assert_eq!(m.last_inputs.len(), 1);
        assert!(matches!(
            m.side_events.first(),
            Some(ActivityEvent::Complete(_))
        ));
    }

    // ------------------------------------------------------ orphan cards

    #[test]
    fn orphan_tool_result_builds_completed_card_live() {
        let mut m = ActivityModel::new();
        live(
            &mut m,
            vec![tool_result_named(
                "rq1",
                "",
                Some(ToolResultStatus::Completed),
            )],
        );
        let cards = snapshot_cards(&m);
        assert_eq!(cards.len(), 1, "orphan result keeps its data");
        assert_eq!(cards[0].0, "rq1");
        assert_eq!(cards[0].2, Some(true), "orphan terminal result is done");
    }

    #[test]
    fn orphan_tool_result_builds_completed_card_projected() {
        let mut m = ActivityModel::new();
        pull(
            &mut m,
            vec![tool_result_named(
                "rq1",
                "",
                Some(ToolResultStatus::Completed),
            )],
        );
        assert_eq!(snapshot_cards(&m).len(), 1);
    }

    #[test]
    fn late_tool_use_completes_orphan_declaration() {
        let mut m = ActivityModel::new();
        // Result first (window cut), its ToolUse later.
        pull(
            &mut m,
            vec![tool_result_named(
                "rq1",
                "",
                Some(ToolResultStatus::Completed),
            )],
        );
        live(&mut m, vec![tool_use("rq1")]);
        let cards = snapshot_cards(&m);
        assert_eq!(cards.len(), 1, "same identity merges onto one card");
        assert_eq!(cards[0].1, "shell", "ToolUse completes the declaration");
        assert_eq!(cards[0].2, Some(true), "result preserved");
    }

    #[test]
    fn background_running_placeholder_then_final_report() {
        let mut m = ActivityModel::new();
        live(
            &mut m,
            vec![
                tool_use("bg1"),
                tool_result_named("bg1", "shell", Some(ToolResultStatus::Running)),
                tool_result_named("bg1", "shell", Some(ToolResultStatus::Completed)),
            ],
        );
        let cards = snapshot_cards(&m);
        assert_eq!(cards[0].2, Some(true), "final report closes the card");

        // A pulled window still carrying the Running placeholder must not
        // demote the locally completed card.
        let before = m.snapshot();
        pull(
            &mut m,
            vec![tool_result_named(
                "bg1",
                "shell",
                Some(ToolResultStatus::Running),
            )],
        );
        assert_eq!(
            before,
            m.snapshot(),
            "completion is never demoted by a window"
        );
    }

    #[test]
    fn pulled_terminal_state_overrides_streaming_intermediate() {
        let mut m = ActivityModel::new();
        // Streaming: deltas accumulate.
        live(
            &mut m,
            vec![
                content_start("p1", "c0"),
                content_delta("p1", "c0", "partial"),
                think_start("p1", "t0"),
                think_delta("p1", "t0", "mid"),
            ],
        );
        // The pulled window carries the sealed terminal state for the same
        // identities.
        pull(
            &mut m,
            vec![
                think_part("p1", "t0", "mid final"),
                content_part("p1", "c0", "final text"),
                pair_complete("p1"),
            ],
        );
        assert_eq!(snapshot_thinks(&m), vec!["mid final".to_string()]);
        assert_eq!(snapshot_texts(&m), vec!["final text".to_string()]);
        assert!(!m.turn_active(), "window closes the turn");
    }

    #[test]
    fn window_replacement_never_demotes_completed_tool_card() {
        let mut m = ActivityModel::new();
        live(
            &mut m,
            vec![tool_use("rq1"), tool_result("rq1"), pair_complete("p1")],
        );
        let before = m.snapshot();
        // Window replay: ToolUse + ToolResult + close, same identities.
        pull(
            &mut m,
            vec![tool_use("rq1"), tool_result("rq1"), pair_complete("p1")],
        );
        assert_eq!(
            before,
            m.snapshot(),
            "completed card survives the window replay"
        );
    }

    #[test]
    fn no_reasoning_turn_produces_normal_card() {
        // The rewrite's core fix: ToolUse has no drop path — a turn with no
        // think/content preamble builds a normal card (the historical
        // active-pair guard used to discard it, leaving an orphan result).
        let mut m = ActivityModel::new();
        pull(
            &mut m,
            vec![tool_use("rq1"), pair_complete("p1"), tool_result("rq1")],
        );
        let cards = snapshot_cards(&m);
        assert_eq!(
            cards.len(),
            1,
            "one card — not a discarded use + orphan result"
        );
        assert_eq!(cards[0].1, "shell", "card name from ToolUse");
        assert_eq!(cards[0].2, Some(true), "result landed on the same card");
    }

    // ------------------------------------------------- lifecycle / cancel

    #[test]
    fn complete_settles_unclosed_pairs_on_cancel() {
        let mut m = ActivityModel::new();
        live(
            &mut m,
            vec![
                content_start("p1", "c0"),
                content_delta("p1", "c0", "partial"),
                complete(),
            ],
        );
        assert!(!m.turn_active(), "complete settles the open pair");
        assert_eq!(m.turn_completed, 1);
        assert_eq!(
            snapshot_texts(&m),
            vec!["partial".to_string()],
            "partial content frozen"
        );
        let turns = m.get_turns();
        assert_eq!(turns[0].status, TurnStatus::Finalized);
    }

    #[test]
    fn complete_is_noop_after_pair_complete() {
        let mut m = ActivityModel::new();
        live(
            &mut m,
            vec![
                content_start("p1", "c0"),
                content_part("p1", "c0", "one"),
                pair_complete("p1"),
                complete(),
            ],
        );
        assert_eq!(
            m.turn_completed, 1,
            "complete still advances the live marker"
        );
        let turns = m.get_turns();
        assert_eq!(turns.len(), 1);
        assert_eq!(turns[0].contents.len(), 1);
    }

    #[test]
    fn late_delta_after_close_is_dropped() {
        let mut m = ActivityModel::new();
        live(
            &mut m,
            vec![
                content_start("p1", "c0"),
                content_part("p1", "c0", "sealed"),
                pair_complete("p1"),
                content_delta("p1", "c0", " late"),
                think_delta("p1", "t0", " late"),
            ],
        );
        assert_eq!(snapshot_texts(&m), vec!["sealed".to_string()]);
        assert!(snapshot_thinks(&m).is_empty(), "closed pair opens nothing");
    }

    #[test]
    fn live_complete_advances_turn_completed_projection_does_not() {
        let mut live_m = ActivityModel::new();
        live(&mut live_m, vec![complete()]);
        assert_eq!(live_m.turn_completed, 1);

        let mut pulled = ActivityModel::new();
        pull(&mut pulled, vec![complete()]);
        assert_eq!(
            pulled.turn_completed, 0,
            "history must not fake the close signal"
        );

        let mut replayed = ActivityModel::new();
        let complete = complete();
        replayed.apply_one(&complete, EventSource::BufferReplay);
        assert_eq!(
            replayed.turn_completed, 0,
            "replay must not fake the close signal"
        );
    }

    // ----------------------------------------------------- bubble hygiene

    // NOTE: the replay-probe-based "replayed start merges" test was retired
    // with the probe itself. The snapshot-level adjacent-identical-bubble
    // merge is a pure snapshot derivation and stays pinned by the live-form
    // test below.

    #[test]
    fn adjacent_identical_starts_render_one_bubble() {
        // Two identical adjacent bubbles (nothing rendered in between —
        // the re-push shape) render as one at snapshot derivation.
        let mut m = ActivityModel::new();
        live(&mut m, vec![start("q"), start("q")]);
        let users: Vec<String> = m
            .snapshot()
            .into_iter()
            .filter_map(|item| match item {
                SnapshotItem::User { inputs, .. } => Some(inputs_text(&inputs)),
                _ => None,
            })
            .collect();
        assert_eq!(
            users,
            vec!["q".to_string()],
            "adjacent identical bubbles merge"
        );
    }

    // -------------------------------------------------------------------
    // Start replay dedup (reducer level). The snapshot adjacent-merge
    // above is a rendering derivation and stays; this pins the model-layer
    // fact: replayed starts whose entries already exist do not append.
    // -------------------------------------------------------------------

    fn user_entry_count(model: &ActivityModel) -> usize {
        model
            .entries
            .iter()
            .filter(|entry| matches!(entry, RenderEntry::User { .. }))
            .count()
    }

    #[test]
    fn replay_start_duplicates_skipped() {
        // A replay window re-delivering starts whose entries already exist
        // appends nothing — on both replay channels.
        let mut m = ActivityModel::new();
        live(&mut m, vec![start("q"), start("q")]);
        assert_eq!(user_entry_count(&m), 2);
        pull(&mut m, vec![start("q"), start("q")]);
        assert_eq!(
            user_entry_count(&m),
            2,
            "history-projection replays of existing starts must not append"
        );
        let envelopes: Vec<ActivityEventEnvelope> = vec![start("q"), start("q")]
            .into_iter()
            .map(|payload| ActivityEventEnvelope {
                activity_id: "a1".into(),
                seq: 0,
                pair_epoch: 0,
                category: Default::default(),
                payload,
            })
            .collect();
        m.apply_buffer_replay(envelopes);
        assert_eq!(
            user_entry_count(&m),
            2,
            "buffer replays of existing starts must not append"
        );
    }

    #[test]
    fn replay_start_new_occurrence_pushed() {
        // A replay window carrying one more same-content start than the
        // entries already hold means a genuinely new input arrived inside
        // the window — that occurrence appends.
        let mut m = ActivityModel::new();
        live(&mut m, vec![start("q"), start("q")]);
        pull(&mut m, vec![start("q"), start("q"), start("q")]);
        assert_eq!(
            user_entry_count(&m),
            3,
            "the occurrence beyond the existing entries is a new input"
        );
    }

    #[test]
    fn live_identical_starts_both_pushed() {
        // Live starts are never deduped: two identical user sends are two
        // legitimate inputs. The snapshot's adjacent-bubble merge is a
        // separate rendering derivation (pinned above) — this is the
        // model-layer boundary.
        let mut m = ActivityModel::new();
        live(&mut m, vec![start("q"), start("q")]);
        assert_eq!(user_entry_count(&m), 2, "live starts always append");
    }

    #[test]
    fn repeated_identical_input_renders_two_bubbles() {
        let mut m = ActivityModel::new();
        live(
            &mut m,
            vec![
                start("q"),
                content_part("p1", "c0", "a"),
                pair_complete("p1"),
                start("q"),
                content_part("p2", "c1", "b"),
                pair_complete("p2"),
            ],
        );
        let users: Vec<Option<String>> = m
            .snapshot()
            .into_iter()
            .filter_map(|item| match item {
                SnapshotItem::User { pair, .. } => Some(pair),
                _ => None,
            })
            .collect();
        assert_eq!(users.len(), 2, "two inputs = two bubbles");
        assert_eq!(users[0].as_deref(), Some("p1"));
        assert_eq!(users[1].as_deref(), Some("p2"));
    }

    #[test]
    fn degenerate_turn_keeps_bubble_unpaired() {
        let mut m = ActivityModel::new();
        live(&mut m, vec![start("only")]);
        let users: Vec<Option<String>> = m
            .snapshot()
            .into_iter()
            .filter_map(|item| match item {
                SnapshotItem::User { pair, .. } => Some(pair),
                _ => None,
            })
            .collect();
        assert_eq!(users, vec![None], "no paired entry in range");
    }

    // ------------------------------------------------------ query / keys

    #[test]
    fn get_turns_snapshot_is_independent() {
        let mut m = ActivityModel::new();
        live(
            &mut m,
            vec![content_start("p1", "c0"), content_part("p1", "c0", "one")],
        );
        let mut snapshot = m.get_turns();
        snapshot[0].contents[0].text = "mutated".into();
        assert_eq!(m.get_turns()[0].contents[0].text, "one");
        assert_eq!(m.get_turns()[0].id, "p1");
    }

    #[test]
    fn snapshot_keys_stable_within_one_model() {
        // Keys are derived deterministically from identity; the derivation
        // is stable across repeated snapshot reads of the same model.
        let mut m = ActivityModel::new();
        let batch = vec![
            start("q"),
            think_part("p1", "replay:t0", "think"),
            content_part("p1", "replay:c0", "one"),
            content_part("p1", "replay:c1", "two"),
            pair_complete("p1"),
        ];
        pull(&mut m, batch);
        let keys_before = item_keys(&m.snapshot());
        assert_eq!(
            keys_before,
            item_keys(&m.snapshot()),
            "keys are stable across repeated snapshot reads"
        );
    }

    fn item_keys(items: &[SnapshotItem]) -> Vec<String> {
        items
            .iter()
            .map(|item| match item {
                SnapshotItem::User { key, .. }
                | SnapshotItem::Think { key, .. }
                | SnapshotItem::Text { key, .. }
                | SnapshotItem::Tool { key, .. }
                | SnapshotItem::Process { key, .. } => key.clone(),
            })
            .collect()
    }

    #[test]
    fn interleaved_pairs_keep_render_order() {
        let mut m = ActivityModel::new();
        live(
            &mut m,
            vec![
                start("q1"),
                content_start("p1", "c0"),
                content_part("p1", "c0", "one"),
                pair_complete("p1"),
                start("q2"),
                content_start("p2", "c1"),
                content_part("p2", "c1", "two"),
                pair_complete("p2"),
            ],
        );
        let texts = snapshot_texts(&m);
        assert_eq!(texts, vec!["one".to_string(), "two".to_string()]);
    }

    #[test]
    fn wire_roundtrip_through_apply_path() {
        // The typed vocabulary survives a wire round-trip inside the
        // reducer's own application path (from_xacpp_event → apply_one).
        let mut m = ActivityModel::new();
        let events = vec![
            start("q"),
            think_part("p1", "t0", "thinking"),
            content_part("p1", "c0", "answer"),
            tool_use("rq1"),
            tool_result("rq1"),
            pair_complete("p1"),
        ];
        for event in &events {
            let wire = wire_of(event);
            let typed = ActivityEvent::from_xacpp_event(&wire).expect("round-trip");
            m.apply_one(&typed, EventSource::HistoryProjection);
        }
        assert_eq!(snapshot_texts(&m), vec!["answer".to_string()]);
        assert_eq!(snapshot_cards(&m).len(), 1);
    }
}
