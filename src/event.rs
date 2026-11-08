//! Strongly-typed activity event vocabulary (client-side view of the wire).
//!
//! Wire facts (verified against x-agent xacpp listener `event_bridge.rs`
//! `convert_event` and the `conversation_history` projection):
//! - An activity event travels as `xacpp::wire::events::XacppEvent { name, data }`
//!   wrapped in `XacppActivityEvent { activity, event }`.
//! - `think` family payloads carry `content/round/pair/partId` at the top
//!   level of `data` (camelCase `partId`).
//! - `content_delta` / `content_part` payloads nest the content under
//!   `payload` (`{ round, pair, payload: ContentPart }`).
//! - `conversation_history` responses carry `events: Vec<XacppEvent>` with
//!   projection part ids prefixed `replay:`.
//!
//! [`ActivityEvent`] is the single typed entry point for all of these: parse
//! with [`ActivityEvent::from_wire`], emit with [`ActivityEvent::to_wire`].

use serde::{Deserialize, Serialize};
use serde_json::Value;
use xacpp::wire::events::{
    ActivityStartEvent, ContentDeltaEvent, ContentPart, ContentPartEvent, ModelTier, NotifyPayload,
    SecurityAlertEvent, TokenUsage, ToolUseEvent, TraceableEvent, UploadEvent, XacppEvent,
};

use crate::error::ActivityError;

// ---------------------------------------------------------------------------
// Local payload structs for wire shapes not covered by xacpp 0.8.3 types.
// ---------------------------------------------------------------------------

/// Payload of `think` (delta) and `think_part` (full snapshot):
/// `{ content, round, pair, partId }`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ThinkContentPayload {
    /// Delta text (`think`) or complete snapshot text (`think_part`).
    pub content: String,
    /// Unique identifier for one engine run.
    pub round: String,
    /// Unique identifier for one loop iteration.
    pub pair: String,
    /// Part identifier for block routing.
    pub part_id: String,
}

/// Payload of block boundary events (`think_start`, `think_end`,
/// `content_start`, `content_end`): `{ round, pair, partId }`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BlockBoundaryPayload {
    /// Unique identifier for one engine run.
    pub round: String,
    /// Unique identifier for one loop iteration.
    pub pair: String,
    /// Part identifier for block routing.
    pub part_id: String,
}

/// Tool call result status (client-side view of the x-agent core
/// `ToolResultStatus`).
///
/// Wire-carried on `tool_result.status` (snake_case); absent means
/// unknown — never interpreted as a concrete terminal state (the optional
/// carrier is [`ToolResultPayload::status`], an `Option` of this enum).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolResultStatus {
    /// Background placeholder: the tool keeps running (timed-out shell
    /// moved to background / persistent process); a final report follows
    /// on the live stream only.
    Running,
    /// Terminal success.
    Completed,
    /// Terminal cancellation.
    Cancelled,
    /// Terminal failure (protocol-level tool error; command exit≠0 stays
    /// `Completed` with the error in `parts`).
    Error,
}

/// Payload of `tool_result` (strict 1.2 wire contract).
///
/// Required members: `requestId/toolName/index/parts/auto/isError`.
/// `status` / `isBackground` are optional facts — absent means unknown,
/// never a concrete default.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ToolResultPayload {
    /// ToolRequest.id.
    pub request_id: String,
    /// Tool name (empty on projection-carried results — the isolated Tool
    /// message persists no tool name; the consumer-side card keeps the
    /// name from its `tool_use`).
    pub tool_name: String,
    /// Tool call sequence number.
    pub index: u32,
    /// Tool execution result content (multimodal parts; required — empty
    /// means an empty result, not an absent one).
    pub parts: Vec<ContentPart>,
    /// Tool call status (see [`ToolResultStatus`]); absent = unknown.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<ToolResultStatus>,
    /// True when the tool keeps running in a background process group
    /// (timed-out shell / persistent transfer) — informational; the
    /// `Running` status is the card-lifecycle fact. Absent = unknown.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_background: Option<bool>,
    /// LLM-protocol tool-level error flag (orthogonal to `status`).
    pub is_error: bool,
    /// True when this result was produced under the activity `auto`
    /// authorization mode (serve-side self-approval, no user interaction).
    pub auto: bool,
}

/// Payload of `resource_diff`: `{ requestId, toolName, index, round,
/// filePath, diff }` (shape mirrors x-agent core `ResourceDiffEvent`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ResourceDiffPayload {
    /// ToolRequest id this diff belongs to.
    pub request_id: String,
    /// Tool name.
    pub tool_name: String,
    /// Tool call sequence number.
    pub index: u32,
    /// Unique identifier for one engine run.
    pub round: String,
    /// Modified file path (as passed in by the caller).
    pub file_path: String,
    /// Full unified diff, untruncated.
    pub diff: String,
}

/// Payload of `pair_complete`: `{ pair, contextWindow, tokenUsage,
/// tier?, model? }`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PairCompletePayload {
    /// Unique identifier for the loop iteration this completion closes.
    pub pair: String,
    /// Current model context window size (token count).
    pub context_window: u32,
    /// Token usage of the completed pair.
    pub token_usage: TokenUsage,
    /// Model tier for this pair; absent = unknown.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tier: Option<ModelTier>,
    /// Model display name for this pair; absent = unknown.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
}

/// Payload of `complete`: `{ round, assistantReply, canceled,
/// abortReason? }`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CompletePayload {
    /// Unique identifier of one engine run (turn-level identity).
    pub round: String,
    /// Final assistant reply parts.
    pub assistant_reply: Vec<ContentPart>,
    /// Abort reason when the run was aborted; absence MUST NOT be used to
    /// infer `canceled` — `canceled` is authoritative.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub abort_reason: Option<String>,
    /// True when the run was canceled by the user.
    pub canceled: bool,
}

/// Payload of `start`: `{ inputs: Vec<ContentPart> }`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct StartPayload {
    /// User input content parts.
    pub inputs: Vec<ContentPart>,
}

/// Payload of `activity_done`: `{ activity }`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ActivityDonePayload {
    /// SubActivity unique identifier.
    pub activity: String,
}

/// Payload of `activity_aborted`: `{ activity, reason }`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ActivityAbortedPayload {
    /// SubActivity unique identifier.
    pub activity: String,
    /// Abort reason.
    pub reason: String,
}

/// Payload of `checklist_update`: `{ checklist }` (opaque JSON snapshot).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ChecklistUpdatePayload {
    /// Full checklist snapshot as raw JSON.
    pub checklist: Value,
}

// ---------------------------------------------------------------------------
// Typed event vocabulary.
// ---------------------------------------------------------------------------

/// Strongly-typed activity event: one variant per wire event name.
///
/// Wire name mapping is documented on each variant and enforced by tests
/// (`event_name` covers the full vocabulary).
///
/// `PartialEq` is hand-written (field-level): most xacpp 0.8.3 payload types
/// do not implement `PartialEq`, so the derive would not compile.
#[derive(Debug, Clone)]
pub enum ActivityEvent {
    /// `start` — invoke started, carries user input parts.
    Start(StartPayload),

    /// `think_start` — thinking block opened.
    ThinkStart(BlockBoundaryPayload),
    /// `think` — thinking text delta.
    ThinkDelta(ThinkContentPayload),
    /// `think_part` — thinking block full snapshot.
    ThinkPart(ThinkContentPayload),
    /// `think_end` — thinking block closed.
    ThinkEnd(BlockBoundaryPayload),

    /// `content_start` — content block opened.
    ContentStart(BlockBoundaryPayload),
    /// `content_delta` — unified multimodal content increment (append).
    ContentDelta(ContentDeltaEvent),
    /// `content_part` — unified multimodal content part (replace).
    ContentPart(ContentPartEvent),
    /// `content_end` — content block closed.
    ContentEnd(BlockBoundaryPayload),

    /// `tool_use` — tool call started.
    ToolUse(ToolUseEvent),
    /// `tool_result` — tool call finished (local payload: wire members +
    /// tolerantly parsed status/isBackground/isError).
    ToolResult(ToolResultPayload),
    /// `resource_diff` — full file diff produced by a tool.
    ResourceDiff(ResourceDiffPayload),

    /// `info` — informational trace.
    Info(TraceableEvent),
    /// `warn` — warning trace.
    Warn(TraceableEvent),
    /// `error` — non-terminal error trace.
    Error(TraceableEvent),
    /// `notify` — one-way user notification.
    Notify(NotifyPayload),
    /// `security_alert` — security threat alert.
    SecurityAlert(SecurityAlertEvent),
    /// `upload` — file upload progress/completion/failure.
    Upload(UploadEvent),

    /// `pair_complete` — one ReAct loop iteration finished.
    PairComplete(PairCompletePayload),
    /// `complete` — invoke finished.
    Complete(CompletePayload),

    /// `activity_start` — SubActivity task started.
    ActivityStart(ActivityStartEvent),
    /// `activity_done` — SubActivity task finished.
    ActivityDone(ActivityDonePayload),
    /// `activity_aborted` — SubActivity task aborted.
    ActivityAborted(ActivityAbortedPayload),
    /// `waiting_command` — SubActivity cancelled, waiting for commands.
    WaitingCommand,
    /// `checklist_update` — checklist state snapshot.
    ChecklistUpdate(ChecklistUpdatePayload),
}

impl PartialEq for ActivityEvent {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Start(a), Self::Start(b)) => a == b,
            (Self::ThinkStart(a), Self::ThinkStart(b))
            | (Self::ThinkEnd(a), Self::ThinkEnd(b))
            | (Self::ContentStart(a), Self::ContentStart(b))
            | (Self::ContentEnd(a), Self::ContentEnd(b)) => a == b,
            (Self::ThinkDelta(a), Self::ThinkDelta(b))
            | (Self::ThinkPart(a), Self::ThinkPart(b)) => a == b,
            (Self::ContentDelta(a), Self::ContentDelta(b)) => {
                a.round == b.round && a.pair == b.pair && a.payload == b.payload
            }
            (Self::ContentPart(a), Self::ContentPart(b)) => {
                a.round == b.round && a.pair == b.pair && a.payload == b.payload
            }
            (Self::ToolUse(a), Self::ToolUse(b)) => {
                a.request_id == b.request_id
                    && a.tool_name == b.tool_name
                    && a.index == b.index
                    && a.arguments == b.arguments
                    && a.intent == b.intent
            }
            (Self::ToolResult(a), Self::ToolResult(b)) => {
                a.request_id == b.request_id
                    && a.tool_name == b.tool_name
                    && a.index == b.index
                    && a.parts == b.parts
                    && a.status == b.status
                    && a.is_background == b.is_background
                    && a.is_error == b.is_error
                    && a.auto == b.auto
            }
            (Self::ResourceDiff(a), Self::ResourceDiff(b)) => a == b,
            (Self::Info(a), Self::Info(b))
            | (Self::Warn(a), Self::Warn(b))
            | (Self::Error(a), Self::Error(b)) => a.title == b.title && a.content == b.content,
            (Self::Notify(a), Self::Notify(b)) => a.message == b.message,
            (Self::SecurityAlert(a), Self::SecurityAlert(b)) => {
                a.event_id == b.event_id
                    && a.tool_name == b.tool_name
                    && a.alert_level == b.alert_level
                    && a.description == b.description
                    && a.threat_type == b.threat_type
                    && a.matched_pattern == b.matched_pattern
                    && a.context_snippet == b.context_snippet
            }
            (Self::Upload(a), Self::Upload(b)) => match (a, b) {
                (
                    UploadEvent::Progress {
                        name: an,
                        uploaded: au,
                        total: at,
                    },
                    UploadEvent::Progress {
                        name: bn,
                        uploaded: bu,
                        total: bt,
                    },
                ) => an == bn && au == bu && at == bt,
                (
                    UploadEvent::Completed {
                        name: an,
                        media_source: am,
                    },
                    UploadEvent::Completed {
                        name: bn,
                        media_source: bm,
                    },
                ) => an == bn && am == bm,
                (
                    UploadEvent::Error {
                        name: an,
                        error: ae,
                    },
                    UploadEvent::Error {
                        name: bn,
                        error: be,
                    },
                ) => an == bn && ae == be,
                _ => false,
            },
            (Self::PairComplete(a), Self::PairComplete(b)) => a == b,
            (Self::Complete(a), Self::Complete(b)) => a == b,
            (Self::ActivityStart(a), Self::ActivityStart(b)) => {
                a.goal == b.goal && a.activity == b.activity && a.metadata == b.metadata
            }
            (Self::ActivityDone(a), Self::ActivityDone(b)) => a == b,
            (Self::ActivityAborted(a), Self::ActivityAborted(b)) => a == b,
            (Self::WaitingCommand, Self::WaitingCommand) => true,
            (Self::ChecklistUpdate(a), Self::ChecklistUpdate(b)) => a == b,
            _ => false,
        }
    }
}

impl ActivityEvent {
    /// Wire event name for this variant.
    pub fn event_name(&self) -> &'static str {
        match self {
            ActivityEvent::Start(_) => "start",
            ActivityEvent::ThinkStart(_) => "think_start",
            ActivityEvent::ThinkDelta(_) => "think",
            ActivityEvent::ThinkPart(_) => "think_part",
            ActivityEvent::ThinkEnd(_) => "think_end",
            ActivityEvent::ContentStart(_) => "content_start",
            ActivityEvent::ContentDelta(_) => "content_delta",
            ActivityEvent::ContentPart(_) => "content_part",
            ActivityEvent::ContentEnd(_) => "content_end",
            ActivityEvent::ToolUse(_) => "tool_use",
            ActivityEvent::ToolResult(_) => "tool_result",
            ActivityEvent::ResourceDiff(_) => "resource_diff",
            ActivityEvent::Info(_) => "info",
            ActivityEvent::Warn(_) => "warn",
            ActivityEvent::Error(_) => "error",
            ActivityEvent::Notify(_) => "notify",
            ActivityEvent::SecurityAlert(_) => "security_alert",
            ActivityEvent::Upload(_) => "upload",
            ActivityEvent::PairComplete(_) => "pair_complete",
            ActivityEvent::Complete(_) => "complete",
            ActivityEvent::ActivityStart(_) => "activity_start",
            ActivityEvent::ActivityDone(_) => "activity_done",
            ActivityEvent::ActivityAborted(_) => "activity_aborted",
            ActivityEvent::WaitingCommand => "waiting_command",
            ActivityEvent::ChecklistUpdate(_) => "checklist_update",
        }
    }

    /// Parse a wire event (`name` + `data`) into the typed vocabulary.
    pub fn from_wire(name: &str, data: Value) -> Result<Self, ActivityError> {
        let malformed = |source: serde_json::Error| ActivityError::MalformedPayload {
            name: name.to_string(),
            source,
        };
        match name {
            "start" => serde_json::from_value::<StartPayload>(data)
                .map(ActivityEvent::Start)
                .map_err(malformed),
            "think_start" => serde_json::from_value::<BlockBoundaryPayload>(data)
                .map(ActivityEvent::ThinkStart)
                .map_err(malformed),
            "think" => serde_json::from_value::<ThinkContentPayload>(data)
                .map(ActivityEvent::ThinkDelta)
                .map_err(malformed),
            "think_part" => serde_json::from_value::<ThinkContentPayload>(data)
                .map(ActivityEvent::ThinkPart)
                .map_err(malformed),
            "think_end" => serde_json::from_value::<BlockBoundaryPayload>(data)
                .map(ActivityEvent::ThinkEnd)
                .map_err(malformed),
            "content_start" => serde_json::from_value::<BlockBoundaryPayload>(data)
                .map(ActivityEvent::ContentStart)
                .map_err(malformed),
            "content_delta" => serde_json::from_value::<ContentDeltaEvent>(data)
                .map(ActivityEvent::ContentDelta)
                .map_err(malformed),
            "content_part" => serde_json::from_value::<ContentPartEvent>(data)
                .map(ActivityEvent::ContentPart)
                .map_err(malformed),
            "content_end" => serde_json::from_value::<BlockBoundaryPayload>(data)
                .map(ActivityEvent::ContentEnd)
                .map_err(malformed),
            "tool_use" => serde_json::from_value::<ToolUseEvent>(data)
                .map(ActivityEvent::ToolUse)
                .map_err(malformed),
            "tool_result" => serde_json::from_value::<ToolResultPayload>(data)
                .map(ActivityEvent::ToolResult)
                .map_err(malformed),
            "resource_diff" => serde_json::from_value::<ResourceDiffPayload>(data)
                .map(ActivityEvent::ResourceDiff)
                .map_err(malformed),
            "info" => serde_json::from_value::<TraceableEvent>(data)
                .map(ActivityEvent::Info)
                .map_err(malformed),
            "warn" => serde_json::from_value::<TraceableEvent>(data)
                .map(ActivityEvent::Warn)
                .map_err(malformed),
            "error" => serde_json::from_value::<TraceableEvent>(data)
                .map(ActivityEvent::Error)
                .map_err(malformed),
            "notify" => serde_json::from_value::<NotifyPayload>(data)
                .map(ActivityEvent::Notify)
                .map_err(malformed),
            "security_alert" => serde_json::from_value::<SecurityAlertEvent>(data)
                .map(ActivityEvent::SecurityAlert)
                .map_err(malformed),
            "upload" => serde_json::from_value::<UploadEvent>(data)
                .map(ActivityEvent::Upload)
                .map_err(malformed),
            "pair_complete" => serde_json::from_value::<PairCompletePayload>(data)
                .map(ActivityEvent::PairComplete)
                .map_err(malformed),
            "complete" => serde_json::from_value::<CompletePayload>(data)
                .map(ActivityEvent::Complete)
                .map_err(malformed),
            "activity_start" => serde_json::from_value::<ActivityStartEvent>(data)
                .map(ActivityEvent::ActivityStart)
                .map_err(malformed),
            "activity_done" => serde_json::from_value::<ActivityDonePayload>(data)
                .map(ActivityEvent::ActivityDone)
                .map_err(malformed),
            "activity_aborted" => serde_json::from_value::<ActivityAbortedPayload>(data)
                .map(ActivityEvent::ActivityAborted)
                .map_err(malformed),
            "waiting_command" => Ok(ActivityEvent::WaitingCommand),
            "checklist_update" => serde_json::from_value::<ChecklistUpdatePayload>(data)
                .map(ActivityEvent::ChecklistUpdate)
                .map_err(malformed),
            other => Err(ActivityError::UnknownEvent(other.to_string())),
        }
    }

    /// Serialize back into the wire form.
    pub fn to_wire(&self) -> XacppEvent {
        let data = match self {
            ActivityEvent::Start(p) => serde_json::to_value(p),
            ActivityEvent::ThinkStart(p) => serde_json::to_value(p),
            ActivityEvent::ThinkDelta(p) => serde_json::to_value(p),
            ActivityEvent::ThinkPart(p) => serde_json::to_value(p),
            ActivityEvent::ThinkEnd(p) => serde_json::to_value(p),
            ActivityEvent::ContentStart(p) => serde_json::to_value(p),
            ActivityEvent::ContentDelta(p) => serde_json::to_value(p),
            ActivityEvent::ContentPart(p) => serde_json::to_value(p),
            ActivityEvent::ContentEnd(p) => serde_json::to_value(p),
            ActivityEvent::ToolUse(p) => serde_json::to_value(p),
            ActivityEvent::ToolResult(p) => serde_json::to_value(p),
            ActivityEvent::ResourceDiff(p) => serde_json::to_value(p),
            ActivityEvent::Info(p) => serde_json::to_value(p),
            ActivityEvent::Warn(p) => serde_json::to_value(p),
            ActivityEvent::Error(p) => serde_json::to_value(p),
            ActivityEvent::Notify(p) => serde_json::to_value(p),
            ActivityEvent::SecurityAlert(p) => serde_json::to_value(p),
            ActivityEvent::Upload(p) => serde_json::to_value(p),
            ActivityEvent::PairComplete(p) => serde_json::to_value(p),
            ActivityEvent::Complete(p) => serde_json::to_value(p),
            ActivityEvent::ActivityStart(p) => serde_json::to_value(p),
            ActivityEvent::ActivityDone(p) => serde_json::to_value(p),
            ActivityEvent::ActivityAborted(p) => serde_json::to_value(p),
            ActivityEvent::WaitingCommand => Ok(Value::Object(Default::default())),
            ActivityEvent::ChecklistUpdate(p) => serde_json::to_value(p),
        }
        .unwrap_or_default();
        XacppEvent::new(self.event_name(), data)
    }

    /// Parse from an [`XacppEvent`] (transport-decoded form).
    pub fn from_xacpp_event(event: &XacppEvent) -> Result<Self, ActivityError> {
        Self::from_wire(&event.name, event.data.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Full vocabulary: every variant maps to its wire name and back.
    #[test]
    fn event_name_covers_full_vocabulary() {
        let cases: Vec<(ActivityEvent, &str)> = vec![
            (
                ActivityEvent::Start(StartPayload { inputs: vec![] }),
                "start",
            ),
            (
                ActivityEvent::ThinkStart(BlockBoundaryPayload {
                    round: "r".into(),
                    pair: "p".into(),
                    part_id: "t".into(),
                }),
                "think_start",
            ),
            (
                ActivityEvent::ThinkDelta(ThinkContentPayload {
                    content: "c".into(),
                    round: "r".into(),
                    pair: "p".into(),
                    part_id: "t".into(),
                }),
                "think",
            ),
            (
                ActivityEvent::ThinkPart(ThinkContentPayload {
                    content: "c".into(),
                    round: "r".into(),
                    pair: "p".into(),
                    part_id: "t".into(),
                }),
                "think_part",
            ),
            (
                ActivityEvent::ThinkEnd(BlockBoundaryPayload {
                    round: "r".into(),
                    pair: "p".into(),
                    part_id: "t".into(),
                }),
                "think_end",
            ),
            (
                ActivityEvent::ContentStart(BlockBoundaryPayload {
                    round: "r".into(),
                    pair: "p".into(),
                    part_id: "c".into(),
                }),
                "content_start",
            ),
            (
                ActivityEvent::ContentDelta(ContentDeltaEvent {
                    round: "r".into(),
                    pair: "p".into(),
                    payload: ContentPart::Text(xacpp::wire::events::TextPart {
                        text: "d".into(),
                        part_id: None,
                        part_type: "default".to_string(),
                    }),
                }),
                "content_delta",
            ),
            (
                ActivityEvent::ContentPart(ContentPartEvent {
                    round: "r".into(),
                    pair: "p".into(),
                    payload: ContentPart::Text(xacpp::wire::events::TextPart {
                        text: "d".into(),
                        part_id: None,
                        part_type: "default".to_string(),
                    }),
                }),
                "content_part",
            ),
            (
                ActivityEvent::ContentEnd(BlockBoundaryPayload {
                    round: "r".into(),
                    pair: "p".into(),
                    part_id: "c".into(),
                }),
                "content_end",
            ),
            (
                ActivityEvent::ToolUse(ToolUseEvent {
                    request_id: "rq".into(),
                    tool_name: "shell".into(),
                    index: 0,
                    arguments: "{}".into(),
                    intent: String::new(),
                }),
                "tool_use",
            ),
            (
                ActivityEvent::ToolResult(ToolResultPayload {
                    request_id: "rq".into(),
                    tool_name: "shell".into(),
                    index: 0,
                    parts: vec![],
                    status: Some(ToolResultStatus::Completed),
                    is_background: Some(false),
                    is_error: false,
                    auto: false,
                }),
                "tool_result",
            ),
            (
                ActivityEvent::ResourceDiff(ResourceDiffPayload {
                    request_id: "rq".into(),
                    tool_name: "edit".into(),
                    index: 0,
                    round: "r".into(),
                    file_path: "/a".into(),
                    diff: "@@".into(),
                }),
                "resource_diff",
            ),
            (
                ActivityEvent::Info(TraceableEvent {
                    title: "t".into(),
                    content: String::new(),
                }),
                "info",
            ),
            (
                ActivityEvent::Warn(TraceableEvent {
                    title: "t".into(),
                    content: String::new(),
                }),
                "warn",
            ),
            (
                ActivityEvent::Error(TraceableEvent {
                    title: "t".into(),
                    content: String::new(),
                }),
                "error",
            ),
            (
                ActivityEvent::Notify(NotifyPayload {
                    message: "m".into(),
                }),
                "notify",
            ),
            (
                ActivityEvent::SecurityAlert(SecurityAlertEvent {
                    event_id: "e".into(),
                    tool_name: "shell".into(),
                    alert_level: xacpp::wire::events::AlertLevel::Warn,
                    description: "d".into(),
                    threat_type: None,
                    matched_pattern: None,
                    context_snippet: None,
                }),
                "security_alert",
            ),
            (
                ActivityEvent::Upload(UploadEvent::Progress {
                    name: "f".into(),
                    uploaded: 1,
                    total: 2,
                }),
                "upload",
            ),
            (
                ActivityEvent::PairComplete(PairCompletePayload {
                    pair: "p".into(),
                    context_window: 1,
                    token_usage: TokenUsage::default(),
                    tier: None,
                    model: None,
                }),
                "pair_complete",
            ),
            (
                ActivityEvent::Complete(CompletePayload {
                    round: "r".into(),
                    assistant_reply: vec![],
                    abort_reason: None,
                    canceled: false,
                }),
                "complete",
            ),
            (
                ActivityEvent::ActivityStart(ActivityStartEvent {
                    goal: "g".into(),
                    activity: "a".into(),
                    metadata: None,
                }),
                "activity_start",
            ),
            (
                ActivityEvent::ActivityDone(ActivityDonePayload {
                    activity: "a".into(),
                }),
                "activity_done",
            ),
            (
                ActivityEvent::ActivityAborted(ActivityAbortedPayload {
                    activity: "a".into(),
                    reason: "r".into(),
                }),
                "activity_aborted",
            ),
            (ActivityEvent::WaitingCommand, "waiting_command"),
            (
                ActivityEvent::ChecklistUpdate(ChecklistUpdatePayload {
                    checklist: serde_json::json!([]),
                }),
                "checklist_update",
            ),
        ];

        for (event, expected_name) in cases {
            assert_eq!(event.event_name(), expected_name);
            let wire = event.to_wire();
            assert_eq!(wire.name, expected_name);
            let parsed = ActivityEvent::from_wire(&wire.name, wire.data).unwrap();
            assert_eq!(
                parsed, event,
                "round-trip must be lossless for {expected_name}"
            );
        }
    }

    #[test]
    fn unknown_name_is_rejected() {
        let err = ActivityEvent::from_wire("no_such_event", serde_json::json!({})).unwrap_err();
        assert!(matches!(err, ActivityError::UnknownEvent(n) if n == "no_such_event"));
    }

    #[test]
    fn malformed_payload_carries_event_name() {
        // `think` requires content/round/pair/partId; empty object must fail.
        let err = ActivityEvent::from_wire("think", serde_json::json!({})).unwrap_err();
        assert!(matches!(err, ActivityError::MalformedPayload { ref name, .. } if name == "think"));
    }

    #[test]
    fn waiting_command_accepts_empty_data() {
        let event = ActivityEvent::from_wire("waiting_command", serde_json::json!({})).unwrap();
        assert_eq!(event, ActivityEvent::WaitingCommand);
        // Missing data entirely must also be accepted (transport default).
        let event = ActivityEvent::from_wire("waiting_command", Value::Null).unwrap();
        assert_eq!(event, ActivityEvent::WaitingCommand);
    }
}
