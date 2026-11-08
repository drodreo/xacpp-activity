//! Session model: flat render entries, derived turn views, process messages.
//!
//! One render structure: entries are a flat, append-ordered list where every
//! entry carries its own identity —
//! - user bubble: stream order (the `start` event has no turn identity);
//!   its content identity is the canonical-input hash (see [`SnapshotItem`])
//!   — materialization-only field drift across live/history cannot split or
//!   merge two logical inputs
//! - think block: `pair` (the wire carries exactly one think block per loop
//!   iteration, so the pair is the block identity; provider part ids differ
//!   between live and projection replays and must not split the block)
//! - content block: `(pair, partId)`
//! - tool card: `requestId`
//! - process message: stream order
//!
//! Merge is identity-addressed: an arriving event finds its entry by
//! identity and evolves it in place (block evolution: delta appends → part
//! seals → later deltas for the sealed identity drop; card evolution:
//! ToolUse opens → ToolResult advances by the three status rules, with the
//! lifecycle itself derived via `ToolCard::done` — a terminal card is never
//! demoted by a late Running placeholder, and unknown never erases a known
//! state). Entries are never rebuilt or replaced by batches — a pulled
//! window is applied to a fresh model by the session layer (replacement,
//! not merge) and replays the same event stream through the same path.
//!
//! Wire part ids appear on content entries as identity components; they
//! never surface to consumers (SnapshotItem keys derive from pair / part
//! order / request id only).
//!
//! `Turn` is a derived view (see [`ActivityModel::get_turns`]): entries
//! grouped by pair in render order. It exists for the query API and the
//! steady-state trim; the render path (snapshot) walks entries directly.

use std::hash::Hasher;

use xacpp::wire::events::{
    AudioPart, ContentPart, FilePart, FileRef, ImagePart, TextPart, VideoPart,
};

use crate::event::{ActivityEvent, ToolResultStatus};

/// Lifecycle state of a derived turn view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnStatus {
    /// Turn is still open (no `pair_complete` for its pair has arrived).
    Streaming,
    /// Turn closed by `pair_complete` (or by `complete` settling unclosed
    /// pairs on the cancel path).
    Finalized,
}

/// Thinking block of a turn view (at most one per pair).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ThinkBlock {
    /// Accumulated thinking text (deltas append, parts replace).
    pub content: String,
    /// False once the block was sealed (`think_part`) or its turn closed.
    pub streaming: bool,
}

/// One content block of a turn view.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ContentBlock {
    /// Accumulated text content (text deltas append, text parts replace).
    pub text: String,
    /// Non-text media parts (images/audio/video/files).
    pub media: Vec<ContentPart>,
    /// False once the block was sealed (`content_part`) or its turn closed.
    pub streaming: bool,
}

/// Result of one tool call.
#[derive(Debug, Clone, PartialEq)]
pub struct ToolResult {
    /// Multimodal result parts (a `Running` placeholder carries its own
    /// partial content).
    pub parts: Vec<ContentPart>,
    /// Tool call status (`None` = unknown — no known lifecycle fact;
    /// never interpreted as a concrete terminal state).
    pub status: Option<ToolResultStatus>,
    /// True when the tool keeps running in a background process group;
    /// `None` = unknown.
    pub is_background: Option<bool>,
    /// LLM-protocol tool-level error flag (orthogonal to `status`).
    pub is_error: bool,
    /// True when this result was produced under the activity `auto`
    /// authorization mode (serve-side self-approval, no user interaction).
    pub auto: bool,
}

/// One tool call card. Identity: `request_id`.
#[derive(Debug, Clone, PartialEq)]
pub struct ToolCard {
    /// ToolRequest id (also the `tool_result` correlation key).
    pub request_id: String,
    /// Tool name (from the `tool_use`; a card opened by an orphan
    /// `tool_result` carries the result's own — possibly empty — name until
    /// the matching `tool_use` arrives and completes the declaration).
    pub tool_name: String,
    /// Tool call sequence number.
    pub index: u32,
    /// Human-readable arguments summary (as carried on the wire).
    pub arguments: String,
    /// Intent description.
    pub intent: String,
    /// Latest known result (`None` = no `tool_result` arrived yet; a
    /// `Running` placeholder is `Some` with `Some(Running)`; an
    /// unknown-status result is `Some` with `status: None`).
    pub result: Option<ToolResult>,
}

impl ToolCard {
    /// Lifecycle derivation — a pure function of `result` (no stored
    /// second lifecycle state):
    /// - `None` result (only a `ToolUse` seen): running;
    /// - `Some(Running)` placeholder: running;
    /// - `Some(terminal status)`: done;
    /// - `Some(unknown status)`: `None` — the lifecycle fact is not
    ///   decided, and is never rendered as either running or done.
    pub fn done(&self) -> Option<bool> {
        match &self.result {
            None => Some(false),
            Some(result) => result
                .status
                .map(|status| !matches!(status, ToolResultStatus::Running)),
        }
    }
}

/// One ReAct loop iteration view (one `pair`): think block, contents, tools.
///
/// Derived — never stored. Cards without a preceding block entry (orphan
/// results whose `tool_use` never arrived) do not belong to any local pair
/// and are skipped by this view; they still render through the snapshot.
#[derive(Debug, Clone, PartialEq)]
pub struct Turn {
    /// Turn identity: the wire `pair` value.
    pub id: String,
    /// Thinking block (None = no think events arrived, or cleaned up when
    /// empty).
    pub think: Option<ThinkBlock>,
    /// Ordered content blocks.
    pub contents: Vec<ContentBlock>,
    /// Tool calls of this turn (render-order association: cards follow the
    /// closest preceding block entry's pair).
    pub tools: Vec<ToolCard>,
    /// Lifecycle state.
    pub status: TurnStatus,
}

/// Severity of a process message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessLevel {
    Info,
    Warn,
    Error,
    Notify,
}

/// Trace/notification side message (info/warn/error/notify), stored verbatim
/// — the model never renders UI copy.
#[derive(Debug, Clone, PartialEq)]
pub struct ProcessMessage {
    pub level: ProcessLevel,
    /// Title as carried on the wire (empty for notify).
    pub title: String,
    /// Body content (for notify: the message).
    pub content: String,
}

/// Flat render-order entry with intrinsic identity (see module docs).
#[derive(Debug, Clone, PartialEq)]
pub enum RenderEntry {
    /// User bubble (the `start` inputs with hidden parts filtered out).
    /// Stream-ordered; no turn identity. The bubble's text is derived via
    /// [`inputs_text`] — never stored a second time.
    User {
        /// The complete user-visible ordered content parts (only
        /// `default`/`references` part types survive; a media-only input
        /// is kept verbatim).
        inputs: Vec<ContentPart>,
        /// Invoke-internal input sequence (single-message aggregation
        /// keeps it 0).
        seq: usize,
    },
    /// Thinking block. Identity: `pair` (one think block per loop iteration).
    Think {
        pair: String,
        content: String,
        /// False once sealed or the turn closed.
        streaming: bool,
        /// A full-snapshot part landed — later deltas for this identity drop.
        sealed: bool,
        /// Content of the last accepted delta (adjacent-duplicate dedup key).
        last_delta: Option<String>,
    },
    /// Content block. Identity: `(pair, part_id)`.
    Content {
        pair: String,
        part_id: String,
        text: String,
        media: Vec<ContentPart>,
        streaming: bool,
        sealed: bool,
        last_delta: Option<String>,
    },
    /// Tool card. Identity: `request_id`.
    Tool(ToolCard),
}

impl RenderEntry {
    /// The pair a block entry belongs to (`None` for bubbles and cards).
    pub(crate) fn pair(&self) -> Option<&str> {
        match self {
            RenderEntry::Think { pair, .. } | RenderEntry::Content { pair, .. } => Some(pair),
            _ => None,
        }
    }

    /// Whether this entry still streams — the single derivation shared by
    /// the snapshot and the turn view: the entry's own open flag AND the
    /// pair not yet closed. `content_end` settles the flag, `pair_complete`
    /// closes the pair; either alone ends streaming, so no consumer can
    /// observe divergent streaming surfaces.
    fn streams(&self, finalized: &std::collections::HashSet<String>) -> bool {
        match self {
            RenderEntry::Think {
                pair, streaming, ..
            }
            | RenderEntry::Content {
                pair, streaming, ..
            } => *streaming && !finalized.contains(pair),
            _ => false,
        }
    }

    /// Snapshot part count this entry contributes to the steady-state cap
    /// (empty blocks contribute nothing; running cards contribute while
    /// pinned — see [`ActivityModel::trim_to_steady_state`]).
    fn part_count(&self) -> usize {
        match self {
            RenderEntry::Think { content, .. } => usize::from(!content.is_empty()),
            RenderEntry::Content { text, media, .. } => {
                usize::from(!text.is_empty() || !media.is_empty())
            }
            RenderEntry::Tool(_) => 1,
            RenderEntry::User { .. } => 0,
        }
    }
}

/// Rendered snapshot item with a library-derived stable key.
///
/// Key derivation rules (single point in the library):
/// `u:{canonical-hash}[#{n}]` / `think:{pair}` /
/// `text:{pair}:{idx}` / `tool:{request_id}` / `process:{position}`.
///
/// The user-bubble key is the hash of the canonical user inputs (see
/// [`canonical_part`]): identity fields only, so a materialization-only
/// drift (`part_id`, `local_uri`, `remote_url`, `remote_expires_at`,
/// `require_organized` with a present content sha) keeps the key stable
/// across live and history channels, while two genuinely different inputs
/// never share one. Distinct entries whose canonical inputs coincide carry
/// a deterministic `#{n}` suffix (`#2`, `#3`, …) — no random keys.
#[derive(Debug, Clone, PartialEq)]
pub enum SnapshotItem {
    User {
        key: String,
        /// The same user-visible ordered parts the entry holds; text is
        /// derived via [`inputs_text`] — never duplicated.
        inputs: Vec<ContentPart>,
        /// The bubble's own turn pair (derived at snapshot time: the first
        /// paired entry after the bubble, before the next bubble; `None` =
        /// no paired entry in range — in-flight instant or degenerate turn).
        pair: Option<String>,
        /// Invoke-internal input sequence (single-message aggregation
        /// keeps it 0).
        seq: usize,
    },
    Think {
        key: String,
        text: String,
        streaming: bool,
    },
    Text {
        key: String,
        text: String,
        streaming: bool,
        media_json: String,
    },
    Tool {
        key: String,
        request_id: String,
        tool_name: String,
        arguments: String,
        intent: String,
        /// Lifecycle: `Some(true)` = done, `Some(false)` = still running,
        /// `None` = unknown (a result arrived with unknown status). Derived
        /// from the result — never a `false` stand-in for unknown.
        done: Option<bool>,
        result_text: Option<String>,
        result_empty: bool,
        /// `auto` fact; `None` while no result arrived (unknown is never
        /// rendered as `false`).
        auto: Option<bool>,
        /// LLM-protocol error flag; `None` while no result arrived.
        is_error: Option<bool>,
        /// Background fact; `None` = no result or unknown.
        is_background: Option<bool>,
    },
    Process {
        key: String,
        level: String,
        title: String,
        content: String,
    },
}

/// The activity session model: everything the conversion layer produces.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ActivityModel {
    /// Flat render order (stream order). Entries are appended on first
    /// sight of an identity and evolved in place afterwards — never rebuilt.
    pub entries: Vec<RenderEntry>,
    /// Process messages in arrival order (stream-ordered, append-only).
    pub process: Vec<ProcessMessage>,
    /// Side events kept verbatim (complete, security_alert, upload,
    /// sub-activity lifecycle, waiting_command, checklist_update).
    pub side_events: Vec<ActivityEvent>,
    /// Inputs of the most recent `start` event.
    pub last_inputs: Vec<ContentPart>,
    /// Pairs whose turns have been closed (the `pair_complete` record, plus
    /// unclosed pairs settled by `complete`). This is the turn-lifecycle
    /// fact — the only authority for turn status; there is no active-pair
    /// pointer.
    pub finalized_pairs: std::collections::HashSet<String>,
    /// Most recently applied `pair_complete` payload, including its actual
    /// context window and token usage. Latest-window history rebuilds the
    /// model in stream order; older pages never write back, so this remains
    /// the current activity's latest completed-pair fact without a second
    /// consumer-side aggregation state.
    pub latest_pair_complete: Option<crate::event::PairCompletePayload>,
    /// Monotonic turn-closing marker: bumped when a turn-level `complete`
    /// arrives on the live path (the cancel path's only terminal signal
    /// included). Replay and history batches never advance it — a rebuilt
    /// projection must not fake the "the turn I am waiting for has closed"
    /// signal that button state machines consume.
    pub turn_completed: u64,
    /// Occurrence counter of canonical user-input hashes within the replay
    /// batch currently being applied (cleared at each replay batch entry).
    /// `apply_one` consults it to tell a re-delivered start — the batch's
    /// nth occurrence of inputs the entries already hold n of — from a
    /// genuinely new same-content input inside the window.
    pub replay_user_seen: std::collections::HashMap<u64, usize>,
}

impl ActivityModel {
    /// Creates an empty model.
    pub fn new() -> Self {
        Self::default()
    }

    /// Turn views derived from the flat entries (independent copy —
    /// mutations on the returned value never affect the model). For
    /// cold-start/rebuild consumers.
    ///
    /// Derivation walks the render order and groups entries by pair; cards
    /// associate with the closest preceding block entry's pair. A card with
    /// no preceding block (an orphan result whose `tool_use` never arrived)
    /// has no local pair and is skipped here — it still renders in the
    /// snapshot.
    pub fn get_turns(&self) -> Vec<Turn> {
        let mut turns: Vec<Turn> = Vec::new();
        let mut current: Option<usize> = None;
        for entry in &self.entries {
            match entry {
                RenderEntry::Think { pair, content, .. } => {
                    let idx = ensure_view_turn(&mut turns, pair, &self.finalized_pairs);
                    turns[idx].think = Some(ThinkBlock {
                        content: content.clone(),
                        // Single derivation, identical to the snapshot's.
                        streaming: entry.streams(&self.finalized_pairs),
                    });
                    current = Some(idx);
                }
                RenderEntry::Content {
                    pair, text, media, ..
                } => {
                    let idx = ensure_view_turn(&mut turns, pair, &self.finalized_pairs);
                    turns[idx].contents.push(ContentBlock {
                        text: text.clone(),
                        media: media.clone(),
                        streaming: entry.streams(&self.finalized_pairs),
                    });
                    current = Some(idx);
                }
                RenderEntry::Tool(card) => {
                    if let Some(idx) = current {
                        turns[idx].tools.push(card.clone());
                    }
                }
                RenderEntry::User { .. } => {
                    current = None;
                }
            }
        }
        turns
    }

    /// Number of turns currently held (derived view).
    pub fn turns_count(&self) -> usize {
        self.get_turns().len()
    }

    /// Turn-level activity: `true` while any turn is still open — a pair
    /// with rendered block entries that has not seen its `pair_complete`.
    /// This is the projection's authoritative active surface — consumers
    /// drive input/button state from it instead of proxy-inferring activity
    /// from per-entry streaming flags. Orphan cards never contribute.
    pub fn turn_active(&self) -> bool {
        let mut pairs: std::collections::HashSet<&str> = Default::default();
        for entry in &self.entries {
            if let Some(pair) = entry.pair() {
                pairs.insert(pair);
            }
        }
        pairs
            .into_iter()
            .any(|pair| !self.finalized_pairs.contains(pair))
    }

    /// Rendered snapshot: the flat entries expanded into items with
    /// library-derived stable keys, followed by the process sequence.
    ///
    /// User-bubble derivation (pure function of the entries, idempotent
    /// under replay):
    /// - a bubble's `pair` = the first paired entry after it, before the
    ///   next bubble;
    /// - adjacent bubbles with identical canonical inputs (zero entries
    ///   between them — the replay of an already-consumed window re-pushing
    ///   its `start`) render as one;
    /// - each bubble's key is the canonical-input hash; same-canonical
    ///   non-adjacent bubbles get a deterministic `#{n}` suffix.
    pub fn snapshot(&self) -> Vec<SnapshotItem> {
        let mut out: Vec<SnapshotItem> = Vec::new();
        // Index into `out` of the bubble still waiting for its pair.
        let mut awaiting_pair: Option<usize> = None;
        // Per-pair content-entry counter for the `text:{pair}:{idx}` keys.
        let mut content_seq: std::collections::HashMap<&str, usize> = Default::default();
        // Per-canonical-hash occurrence counter for the `#{n}` key suffix.
        let mut user_key_counts: std::collections::HashMap<u64, usize> = Default::default();

        for entry in &self.entries {
            match entry {
                RenderEntry::User { inputs, seq } => {
                    // Replay merge: a bubble with identical canonical inputs
                    // immediately after a bubble (nothing rendered in
                    // between) is a re-pushed `start` — keep one.
                    if let Some(SnapshotItem::User { inputs: prev, .. }) = out.last()
                        && canonical_inputs_eq(prev, inputs)
                    {
                        continue;
                    }
                    let hash = canonical_inputs_hash(inputs);
                    let occurrence = user_key_counts
                        .entry(hash)
                        .and_modify(|c| *c += 1)
                        .or_insert(0);
                    let key = if *occurrence == 0 {
                        format!("u:{hash:016x}")
                    } else {
                        // nth occurrence of the same canonical input → `#n`.
                        format!("u:{hash:016x}#{}", *occurrence + 1)
                    };
                    out.push(SnapshotItem::User {
                        key,
                        inputs: inputs.clone(),
                        pair: None,
                        seq: *seq,
                    });
                    awaiting_pair = Some(out.len() - 1);
                }
                RenderEntry::Think { pair, content, .. } => {
                    if !content.is_empty() {
                        out.push(SnapshotItem::Think {
                            key: format!("think:{pair}"),
                            text: content.clone(),
                            streaming: entry.streams(&self.finalized_pairs),
                        });
                    }
                    fill_user_pair(&mut out, &awaiting_pair, pair);
                    awaiting_pair = None;
                }
                RenderEntry::Content {
                    pair, text, media, ..
                } => {
                    if !text.is_empty() || !media.is_empty() {
                        let counter = content_seq.entry(pair.as_str()).or_insert(0);
                        let idx = *counter;
                        *counter += 1;
                        out.push(SnapshotItem::Text {
                            key: format!("text:{pair}:{idx}"),
                            text: text.clone(),
                            streaming: entry.streams(&self.finalized_pairs),
                            media_json: serde_json::to_string(media).unwrap_or_default(),
                        });
                    }
                    fill_user_pair(&mut out, &awaiting_pair, pair);
                    awaiting_pair = None;
                }
                RenderEntry::Tool(card) => {
                    // Facts derived from the result only; absent result
                    // renders unknown as `None` — never as `false`.
                    let (auto, is_error, is_background, result_text) = match &card.result {
                        Some(result) => (
                            Some(result.auto),
                            Some(result.is_error),
                            result.is_background,
                            Some(
                                result
                                    .parts
                                    .iter()
                                    .filter_map(|part| match part {
                                        ContentPart::Text(text) => Some(text.text.clone()),
                                        _ => None,
                                    })
                                    .collect::<Vec<_>>()
                                    .join(""),
                            ),
                        ),
                        None => (None, None, None, None),
                    };
                    out.push(SnapshotItem::Tool {
                        key: format!("tool:{}", card.request_id),
                        request_id: card.request_id.clone(),
                        tool_name: card.tool_name.clone(),
                        arguments: card.arguments.clone(),
                        intent: card.intent.clone(),
                        done: card.done(),
                        result_text,
                        result_empty: false,
                        auto,
                        is_error,
                        is_background,
                    });
                }
            }
        }
        for (idx, message) in self.process.iter().enumerate() {
            let level = match message.level {
                ProcessLevel::Info => "info",
                ProcessLevel::Warn => "warn",
                ProcessLevel::Error => "error",
                ProcessLevel::Notify => "notify",
            };
            out.push(SnapshotItem::Process {
                key: format!("process:{idx}"),
                level: level.to_string(),
                title: message.title.clone(),
                content: message.content.clone(),
            });
        }
        out
    }

    // ------------------------------------------------------------------
    // Buffer steady state (whole-segment trim).
    // ------------------------------------------------------------------

    /// Buffer steady-state trim: drop the oldest render segments — a
    /// segment is one closed pair's entry run together with the user
    /// bubbles directly preceding it — until the closed pairs' part count
    /// is within `cap`. Half segments are never produced: the pair's entry
    /// run is the atomic trim unit. Open pairs and still-running cards
    /// never count toward the cap and stop the walk (an in-flight part
    /// never triggers a trim; a running background card pins its segment).
    /// The caller settles at turn finalization and after history bulk
    /// ingest.
    ///
    /// Returns whether anything was trimmed.
    pub(crate) fn trim_to_steady_state(&mut self, cap: usize) -> bool {
        // Segment the entries: each maximal run belonging to one trim unit.
        //   (a) a pair's block/card run (cards associate with the run's
        //       pair — the render-order derivation, same as get_turns),
        //   (b) a leading/trailing bubble run (absorbed by the run that
        //       follows; a trailing run with nothing after stands alone),
        //   (c) a card with no preceding block (orphan; its own unit).
        struct Segment {
            parts: usize,
            start: usize,
            /// Pair this segment belongs to (`None` = pure bubble run or
            /// orphan card unit).
            pair: Option<String>,
            /// A still-running card in the segment pins it (no drop).
            pinned: bool,
        }

        let mut segments: Vec<Segment> = Vec::new();
        let mut bubble_start: Option<usize> = None;
        for (idx, entry) in self.entries.iter().enumerate() {
            match entry {
                RenderEntry::User { .. } => {
                    if bubble_start.is_none() {
                        bubble_start = Some(idx);
                    }
                }
                RenderEntry::Think { pair, .. } | RenderEntry::Content { pair, .. } => {
                    let parts = entry.part_count();
                    match segments.last_mut() {
                        Some(seg) if seg.pair.as_deref() == Some(pair.as_str()) => {
                            seg.parts += parts;
                        }
                        _ => segments.push(Segment {
                            parts,
                            start: bubble_start.take().unwrap_or(idx),
                            pair: Some(pair.clone()),
                            pinned: false,
                        }),
                    }
                }
                RenderEntry::Tool(card) => {
                    let parts = entry.part_count();
                    match segments.last_mut() {
                        // A done card rides the pair run it follows.
                        Some(seg)
                            if seg.pair.is_some()
                                && card.done() == Some(true)
                                && bubble_start.is_none() =>
                        {
                            seg.parts += parts;
                        }
                        // Running or unknown-lifecycle card (or orphan slot):
                        // pins its own unit. Unknown pins as pure data
                        // protection — its lifecycle is undecided, dropping
                        // it could lose the only copy; it is not treated as
                        // an execution state.
                        _ => segments.push(Segment {
                            parts,
                            start: bubble_start.take().unwrap_or(idx),
                            pair: None,
                            pinned: card.done() != Some(true),
                        }),
                    }
                }
            }
        }
        // Trailing bubble run (degenerate turn / in-flight instant).
        if let Some(start) = bubble_start {
            segments.push(Segment {
                parts: 0,
                start,
                pair: None,
                pinned: false,
            });
        }

        // Drop oldest whole closed segments only while the closed-part
        // total exceeds the cap — down to the smallest prefix whose removal
        // leaves the remainder within cap. An open pair, an unclosed unit,
        // or a pinned segment stops the walk (everything from there on
        // stays).
        let closed_parts = |segment: &Segment| {
            !segment.pinned
                && segment
                    .pair
                    .as_ref()
                    .is_some_and(|pair| self.finalized_pairs.contains(pair))
        };
        let total_closed: usize = segments
            .iter()
            .filter(|s| closed_parts(s))
            .map(|s| s.parts)
            .sum();
        if total_closed <= cap {
            return false;
        }
        let mut remaining = total_closed;
        let mut cut = 0usize;
        let mut dropped_units = 0usize;
        let mut dropped_parts = 0usize;
        for (i, segment) in segments.iter().enumerate() {
            if !closed_parts(segment) {
                break;
            }
            remaining -= segment.parts;
            dropped_units += 1;
            dropped_parts += segment.parts;
            // The cut is the end of the dropped run: the next segment's
            // start (or everything, for the trailing segment).
            cut = segments
                .get(i + 1)
                .map(|next| next.start)
                .unwrap_or(self.entries.len());
            if remaining <= cap {
                break;
            }
        }
        let total = segments.iter().map(|s| s.parts).sum::<usize>();
        self.entries.drain(0..cut);
        log::info!(
            "[model] steady-state trim: dropped {dropped_units} segment(s) / {dropped_parts} part(s), {} terminal part(s) remain (cap {cap})",
            total - dropped_parts
        );
        true
    }
}

/// Text of user-visible inputs: text parts concatenated in order (the
/// user-bubble text surface; the single derivation every consumer shares —
/// bubble text is never stored a second time).
pub fn inputs_text(inputs: &[ContentPart]) -> String {
    inputs
        .iter()
        .filter_map(|part| match part {
            ContentPart::Text(text) => Some(text.text.clone()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("")
}

/// Filters the user-hidden parts out of a `start` payload: only the
/// `default` and `references` part types are user-visible; every other
/// part type (engine-internal fragments) never enters a user entry. The
/// order of the surviving parts is preserved verbatim.
pub(crate) fn user_visible_inputs(inputs: &[ContentPart]) -> Vec<ContentPart> {
    inputs
        .iter()
        .filter(|part| {
            let part_type = match part {
                ContentPart::Text(p) => p.part_type.as_str(),
                ContentPart::Image(p) => p.part_type.as_str(),
                ContentPart::Audio(p) => p.part_type.as_str(),
                ContentPart::Video(p) => p.part_type.as_str(),
                ContentPart::File(p) => p.part_type.as_str(),
            };
            part_type == "default" || part_type == "references"
        })
        .cloned()
        .collect()
}

/// Canonical (materialization-stable) form of one content part: identity
/// fields only. `part_id` is a wire-tracing fact and always stripped; for
/// media sources a present content `sha256` **is** the identity, so the
/// materialization products (`local_uri`, `remote_url`, `remote_expires_at`,
/// `require_organized`) are stripped too — live and history re-expressions
/// of one logical input land on the same canonical value. With an empty
/// sha the controlled `local_uri`/`remote_url` pair is kept as the fallback
/// identity, so two different files never collapse. Content facts (text,
/// mime type, size, media dimensions) are identity carriers and stay.
fn canonical_part(part: &ContentPart) -> ContentPart {
    match part {
        ContentPart::Text(p) => ContentPart::Text(TextPart {
            part_id: None,
            ..p.clone()
        }),
        ContentPart::Image(p) => ContentPart::Image(ImagePart {
            part_id: None,
            source: canonical_ref(&p.source),
            ..p.clone()
        }),
        ContentPart::Audio(p) => ContentPart::Audio(AudioPart {
            part_id: None,
            source: canonical_ref(&p.source),
            ..p.clone()
        }),
        ContentPart::Video(p) => ContentPart::Video(VideoPart {
            part_id: None,
            source: canonical_ref(&p.source),
            ..p.clone()
        }),
        ContentPart::File(p) => ContentPart::File(FilePart {
            part_id: None,
            source: canonical_ref(&p.source),
            ..p.clone()
        }),
    }
}

/// Canonical form of a media file reference (see [`canonical_part`]).
fn canonical_ref(r: &FileRef) -> FileRef {
    let identified_by_sha = !r.sha256.is_empty();
    FileRef {
        local_uri: if identified_by_sha {
            String::new()
        } else {
            r.local_uri.clone()
        },
        remote_url: if identified_by_sha {
            String::new()
        } else {
            r.remote_url.clone()
        },
        remote_expires_at: None,
        require_organized: false,
        ..r.clone()
    }
}

/// Canonical equality of two user inputs (the adjacent-replay merge
/// predicate).
fn canonical_inputs_eq(a: &[ContentPart], b: &[ContentPart]) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b)
            .all(|(x, y)| canonical_part(x) == canonical_part(y))
}

/// Deterministic identity hash of canonical user inputs: the canonical
/// value serializes field-order-fixed, and the hasher is a fixed-key
/// `DefaultHasher` — the same canonical inputs hash identically in every
/// process.
pub(crate) fn canonical_inputs_hash(inputs: &[ContentPart]) -> u64 {
    let canonical: Vec<ContentPart> = inputs.iter().map(canonical_part).collect();
    let bytes = serde_json::to_vec(&canonical).unwrap_or_default();
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    hasher.write(&bytes);
    hasher.finish()
}

/// Fills the pending bubble's pair with `pair` (first paired entry wins).
fn fill_user_pair(out: &mut [SnapshotItem], pending_user: &Option<usize>, pair: &str) {
    if let Some(pos) = pending_user
        && let Some(SnapshotItem::User { pair: slot, .. }) = out.get_mut(*pos)
    {
        *slot = Some(pair.to_string());
    }
}

/// Ensures a view turn for `pair` exists; returns its index.
fn ensure_view_turn(
    turns: &mut Vec<Turn>,
    pair: &str,
    finalized: &std::collections::HashSet<String>,
) -> usize {
    if let Some(pos) = turns.iter().position(|turn| turn.id == pair) {
        return pos;
    }
    turns.push(Turn {
        id: pair.to_string(),
        think: None,
        contents: Vec::new(),
        tools: Vec::new(),
        status: if finalized.contains(pair) {
            TurnStatus::Finalized
        } else {
            TurnStatus::Streaming
        },
    });
    turns.len() - 1
}
