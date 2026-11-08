//! Interaction objects: pending authorization / question handles.
//!
//! Interactions are commands, not events — they arrive through
//! [`ActivityRuntime::consume_command`] (the consumer's `on_command`
//! handler), an entry parallel to the event stream (`apply_envelope`).
//!
//! The interaction id is a library-assigned handle: xacpp interactions are
//! request-response commands and the reply travels the command response
//! channel, so the id only lives inside the library (the consumer never
//! fabricates wire-side ids).
//!
//! Degradation is explicit: when the consumer cannot deliver an answer
//! (disconnect and similar), it calls [`ActivityRuntime::degrade`] and the
//! interaction records `Degraded { reason }`. The library never performs an
//! implicit auto-reject — the xamobile core's implicit channel-closed
//! reject is corrected here into an explicit consumer decision.

use serde_json::Value;
use thiserror::Error;
use xacpp::wire::events::{
    ActionRequestPayload, ActionResponse, QuestionPayload, QuestionResponse,
    SensitiveInfoOperationPayload, SensitiveInfoOperationResponse,
};
use xacpp::wire::message::XacppResponse;

/// Kind of an interaction command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InteractionKind {
    /// `action_request` command.
    Action,
    /// `question` command.
    Question,
    /// `sensitive_info_operation` command.
    SensitiveInfoOperation,
}

impl InteractionKind {
    /// Wire command name this kind arrives as.
    pub fn command_name(&self) -> &'static str {
        match self {
            InteractionKind::Action => "action_request",
            InteractionKind::Question => "question",
            InteractionKind::SensitiveInfoOperation => "sensitive_info_operation",
        }
    }

    /// Wire response name the reply must carry. For `action` / `question`
    /// this is what serve side `event_bridge::deliver_pending` expects;
    /// `sensitive_info_operation` follows the protocol contract declared in
    /// xacpp `events::interaction` (response `name` ==
    /// `"sensitive_info_operation"` — not shortened like `action_request`
    /// → `action`). The serve side has no expectation for it today (that
    /// interaction is retired there).
    pub fn response_name(&self) -> &'static str {
        match self {
            InteractionKind::Action => "action",
            InteractionKind::Question => "question",
            InteractionKind::SensitiveInfoOperation => "sensitive_info_operation",
        }
    }
}

/// Parsed command payload of an interaction.
#[derive(Debug, Clone)]
pub enum InteractionPayload {
    Action(ActionRequestPayload),
    Question(QuestionPayload),
    SensitiveInfoOperation(SensitiveInfoOperationPayload),
}

/// Equality via the serde wire form (xacpp 0.8.3 payload types lack
/// `PartialEq`; the serde shape is the contract, so comparing serialized
/// forms is the faithful equality).
impl PartialEq for InteractionPayload {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Action(a), Self::Action(b)) => {
                serde_json::to_value(a).ok() == serde_json::to_value(b).ok()
            }
            (Self::Question(a), Self::Question(b)) => {
                serde_json::to_value(a).ok() == serde_json::to_value(b).ok()
            }
            (Self::SensitiveInfoOperation(a), Self::SensitiveInfoOperation(b)) => {
                serde_json::to_value(a).ok() == serde_json::to_value(b).ok()
            }
            _ => false,
        }
    }
}

/// The consumer's answer.
#[derive(Debug, Clone)]
pub enum InteractionAnswer {
    Action(ActionResponse),
    Question(QuestionResponse),
    SensitiveInfoOperation(SensitiveInfoOperationResponse),
}

impl PartialEq for InteractionAnswer {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Action(a), Self::Action(b)) => {
                serde_json::to_value(a).ok() == serde_json::to_value(b).ok()
            }
            (Self::Question(a), Self::Question(b)) => {
                serde_json::to_value(a).ok() == serde_json::to_value(b).ok()
            }
            (Self::SensitiveInfoOperation(a), Self::SensitiveInfoOperation(b)) => {
                serde_json::to_value(a).ok() == serde_json::to_value(b).ok()
            }
            _ => false,
        }
    }
}

/// Lifecycle state: Pending → Answered | Degraded (one-way).
#[derive(Debug, Clone, PartialEq)]
pub enum InteractionState {
    /// Waiting for the consumer.
    Pending,
    /// Consumer answered via `respond`.
    Answered {
        /// The answer that was sent.
        answer: InteractionAnswer,
    },
    /// Consumer explicitly degraded via `degrade`.
    Degraded {
        /// Why the answer could not be delivered.
        reason: String,
    },
}

/// One interaction object.
#[derive(Debug, Clone, PartialEq)]
pub struct Interaction {
    /// Library-assigned handle.
    pub id: u64,
    /// Command kind.
    pub kind: InteractionKind,
    /// Engine-level correlation identity carried from the wire payload
    /// (`ActionRequestPayload::request_id` / `QuestionPayload::request_id`).
    ///
    /// Dedup key is `(activity, request_id)` — the activity scoping is
    /// inherent in the per-activity runtime, so lookups match on
    /// `kind` + `request_id`. Empty = the serve did not send a requestId
    /// (see `consume_command`); no dedup applies to it.
    pub request_id: String,
    /// Parsed payload.
    pub payload: InteractionPayload,
    /// Lifecycle state.
    pub state: InteractionState,
}

/// Interaction handling errors.
#[derive(Debug, Error)]
pub enum InteractionError {
    /// No interaction with this handle.
    #[error("unknown interaction id: {0}")]
    UnknownId(u64),

    /// The interaction already left the Pending state.
    #[error("interaction {id} has already left the pending state")]
    AlreadyAnswered { id: u64 },

    /// The answer variant does not match the interaction kind.
    #[error("answer kind does not match the interaction kind")]
    KindMismatch,

    /// `consume_command` was called with a non-interaction command.
    #[error("unknown interaction command: `{0}`")]
    UnknownCommand(String),

    /// The command payload does not match the declared shape.
    #[error("malformed `{name}` payload: {source}")]
    MalformedPayload {
        /// Command name that failed to parse.
        name: String,
        /// Underlying serde error.
        source: serde_json::Error,
    },
}

/// Builds the wire response for an interaction answer.
///
/// Shape (verified against both sides): `XacppResponse::generic` with
/// response name `action` / `question` / `sensitive_info_operation` and
/// the xacpp response serde as `data`. Serve side
/// `event_bridge::deliver_pending` expects exactly this for action /
/// question (`XacppResponse::Generic { name, data }` with `name ==
/// "action" / "question"`), and xamobile core `interaction_response`
/// produces the same. The `sensitive_info_operation` name is the protocol
/// contract declared in xacpp `events::interaction` (the serve side
/// retired that interaction, so no `deliver_pending` expectation exists
/// for it).
pub(crate) fn build_response(
    kind: InteractionKind,
    answer: &InteractionAnswer,
) -> Result<XacppResponse, InteractionError> {
    let payload: Value = match (kind, answer) {
        (InteractionKind::Action, InteractionAnswer::Action(response)) => {
            serde_json::to_value(response).unwrap_or_default()
        }
        (InteractionKind::Question, InteractionAnswer::Question(response)) => {
            serde_json::to_value(response).unwrap_or_default()
        }
        (
            InteractionKind::SensitiveInfoOperation,
            InteractionAnswer::SensitiveInfoOperation(response),
        ) => serde_json::to_value(response).unwrap_or_default(),
        _ => return Err(InteractionError::KindMismatch),
    };
    Ok(XacppResponse::generic(kind.response_name(), payload))
}
