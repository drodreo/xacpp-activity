//! Contract supplement: projection event sequences (replay: part ids) and
//! live sequences must converge to the same terminal turn for the same
//! logical content.
//!
//! Fixture sources:
//! - Live stream shapes: `event_bridge.rs` `convert_event` (provider part
//!   ids on delta payloads, think family top-level partId).
//! - Projection shapes: `conversation_projection.rs` — think partId =
//!   `replay:thinking:{pair}`, text partId = `replay:text:{pair}:{idx}`,
//!   full-snapshot parts (no deltas), zero-placeholder pair_complete.

use xacpp::wire::events::{
    ContentDeltaEvent, ContentPart, ContentPartEvent, TextPart, TokenUsage, ToolUseEvent,
};
use xacpp_activity::event::{
    BlockBoundaryPayload, PairCompletePayload, StartPayload, ThinkContentPayload, ToolResultPayload,
};
use xacpp_activity::{
    ActivityEvent, ActivityEventCategory, ActivityEventEnvelope, ActivityModel, SnapshotItem,
    TurnStatus,
};

/// Drives one live event through the public envelope entry.
fn apply_live(model: &mut ActivityModel, event: ActivityEvent) {
    model.apply_envelope(&envelope_of(event));
}

/// Drives one projection batch through the public history entry.
fn project_batch(model: &mut ActivityModel, events: Vec<ActivityEvent>) {
    model.apply_history_events(events.into_iter().map(|e| e.to_wire()));
}

fn envelope_of(event: ActivityEvent) -> ActivityEventEnvelope {
    ActivityEventEnvelope {
        activity_id: "act-1".into(),
        seq: 0,
        pair_epoch: 0,
        category: ActivityEventCategory::Default,
        payload: event,
    }
}

fn text_part(part_id: &str, text: &str) -> ContentPart {
    ContentPart::Text(TextPart {
        text: text.into(),
        part_id: Some(part_id.into()),
        part_type: "default".to_string(),
    })
}

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

fn content_end(pair: &str, part_id: &str) -> ActivityEvent {
    ActivityEvent::ContentEnd(BlockBoundaryPayload {
        round: "r1".into(),
        pair: pair.into(),
        part_id: part_id.into(),
    })
}

fn pair_complete(pair: &str) -> ActivityEvent {
    ActivityEvent::PairComplete(PairCompletePayload {
        pair: pair.into(),
        context_window: 0,
        token_usage: TokenUsage::default(),
        tier: None,
        model: None,
    })
}

fn start(inputs: &[&str]) -> ActivityEvent {
    ActivityEvent::Start(StartPayload {
        inputs: inputs.iter().map(|t| text_part("input", t)).collect(),
    })
}

fn tool_use(request_id: &str) -> ActivityEvent {
    ActivityEvent::ToolUse(ToolUseEvent {
        request_id: request_id.into(),
        tool_name: "shell".into(),
        index: 0,
        arguments: r#"{"cmd":"date"}"#.into(),
        intent: "clock".into(),
    })
}

fn tool_result(request_id: &str) -> ActivityEvent {
    ActivityEvent::ToolResult(ToolResultPayload {
        request_id: request_id.into(),
        tool_name: "shell".into(),
        index: 0,
        parts: vec![text_part("out", "2026-10-05")],
        status: Some(xacpp_activity::ToolResultStatus::Completed),
        is_background: None,
        is_error: false,
        auto: false,
    })
}

/// The same logical turn delivered as a live delta stream (provider part
/// ids) and as a projection part stream (replay: part ids) must converge to
/// the identical terminal turn.
#[test]
fn projection_and_live_converge_to_same_turn() {
    // ---- live model ----
    let mut live_model = ActivityModel::new();
    apply_live(&mut live_model, start(&["what time is it"]));
    apply_live(&mut live_model, think_start("p1", "live-t0"));
    apply_live(&mut live_model, think_delta("p1", "live-t0", "user wants "));
    apply_live(&mut live_model, think_delta("p1", "live-t0", "the time"));
    apply_live(&mut live_model, think_end("p1", "live-t0"));
    apply_live(&mut live_model, content_start("p1", "live-c0"));
    apply_live(
        &mut live_model,
        ActivityEvent::ContentDelta(ContentDeltaEvent {
            round: "r1".into(),
            pair: "p1".into(),
            payload: text_part("live-c0", "it is "),
        }),
    );
    apply_live(
        &mut live_model,
        ActivityEvent::ContentDelta(ContentDeltaEvent {
            round: "r1".into(),
            pair: "p1".into(),
            payload: text_part("live-c0", "10 am"),
        }),
    );
    apply_live(&mut live_model, content_end("p1", "live-c0"));
    apply_live(&mut live_model, tool_use("req-1"));
    apply_live(&mut live_model, tool_result("req-1"));
    apply_live(&mut live_model, pair_complete("p1"));

    // ---- projection model (same logical content, replay part ids) ----
    let mut projected = ActivityModel::new();
    project_batch(
        &mut projected,
        vec![
            start(&["what time is it"]),
            think_start("p1", "replay:thinking:p1"),
            think_part("p1", "replay:thinking:p1", "user wants the time"),
            think_end("p1", "replay:thinking:p1"),
            content_start("p1", "replay:text:p1:0"),
            ActivityEvent::ContentPart(ContentPartEvent {
                round: "r1".into(),
                pair: "p1".into(),
                payload: text_part("replay:text:p1:0", "it is 10 am"),
            }),
            content_end("p1", "replay:text:p1:0"),
            tool_use("req-1"),
            tool_result("req-1"),
            pair_complete("p1"),
        ],
    );

    // ---- convergence ----
    let live_turns = live_model.get_turns();
    let projected_turns = projected.get_turns();
    assert_eq!(live_turns.len(), 1);
    assert_eq!(projected_turns.len(), 1);
    assert_eq!(live_turns[0].id, "p1");
    assert_eq!(projected_turns[0].id, "p1");
    // Terminal think/content/tool state identical across both forms.
    assert_eq!(live_turns[0].think, projected_turns[0].think);
    assert_eq!(live_turns[0].contents, projected_turns[0].contents);
    assert_eq!(live_turns[0].tools, projected_turns[0].tools);
    assert_eq!(live_turns[0].status, TurnStatus::Finalized);
    assert_eq!(projected_turns[0].status, TurnStatus::Finalized);
    assert_eq!(live_model.last_inputs, projected.last_inputs);
}

/// Resync flow: a live-streaming half turn is closed by the projection
/// (turn-level replacement), then a full projection replay of the same pair
/// is a no-op (idempotent).
#[test]
fn resync_flow_replacement_then_idempotent_replay() {
    let mut model = ActivityModel::new();

    // Live partial turn.
    apply_live(&mut model, think_start("p1", "live-t0"));
    apply_live(&mut model, think_delta("p1", "live-t0", "streaming…"));

    // Resync: projection delivers the authoritative snapshot.
    project_batch(
        &mut model,
        vec![
            think_part("p1", "replay:thinking:p1", "complete reasoning"),
            think_end("p1", "replay:thinking:p1"),
            pair_complete("p1"),
        ],
    );

    let turns = model.get_turns();
    assert_eq!(turns.len(), 1);
    assert_eq!(
        turns[0].think.as_ref().unwrap().content,
        "complete reasoning"
    );
    assert_eq!(turns[0].status, TurnStatus::Finalized);

    // Full projection replay of the same pair: no duplication, same state.
    let snapshot = model.clone();
    project_batch(
        &mut model,
        vec![
            think_start("p1", "replay:thinking:p1"),
            think_part("p1", "replay:thinking:p1", "complete reasoning"),
            think_end("p1", "replay:thinking:p1"),
            pair_complete("p1"),
        ],
    );

    assert_eq!(model, snapshot, "replay after replacement is idempotent");
}

/// Collects the `auto` flags of all snapshot `Tool` items in order.
fn tool_auto_flags(model: &ActivityModel) -> Vec<Option<bool>> {
    model
        .snapshot()
        .iter()
        .filter_map(|item| match item {
            SnapshotItem::Tool { auto, .. } => Some(*auto),
            _ => None,
        })
        .collect()
}

/// The auto self-approval flag rides `tool_result` through to the snapshot
/// `Tool` item (the surface bindings consume).
#[test]
fn snapshot_tool_item_carries_auto_flag() {
    // Explicit true: serve-side self-approval reaches the snapshot item.
    let mut model = ActivityModel::new();
    apply_live(&mut model, think_start("p1", "t0"));
    apply_live(&mut model, tool_use("req-1"));
    apply_live(
        &mut model,
        ActivityEvent::ToolResult(ToolResultPayload {
            request_id: "req-1".into(),
            tool_name: "shell".into(),
            index: 0,
            parts: vec![text_part("out-0", "done")],
            status: Some(xacpp_activity::ToolResultStatus::Completed),
            is_background: None,
            is_error: false,
            auto: true,
        }),
    );
    assert_eq!(tool_auto_flags(&model), vec![Some(true)]);

    // Explicit false: a user-approved call stays marked non-auto.
    let mut model = ActivityModel::new();
    apply_live(&mut model, think_start("p1", "t0"));
    apply_live(&mut model, tool_use("req-1"));
    apply_live(&mut model, tool_result("req-1"));
    assert_eq!(tool_auto_flags(&model), vec![Some(false)]);

    // Strict 1.2: the wire `auto` member is required — a payload without
    // it is malformed and rejected, never silently defaulted.
    let missing_auto = ActivityEvent::from_wire(
        "tool_result",
        serde_json::json!({
            "requestId": "req-1",
            "toolName": "shell",
            "index": 0,
            "parts": [],
            "isError": false
        }),
    );
    assert!(missing_auto.is_err(), "missing auto must fail under 1.2");

    // Pending card (no result yet): auto reads as `None` (unknown — never
    // a `false` stand-in) alongside done=Some(false).
    let mut model = ActivityModel::new();
    apply_live(&mut model, think_start("p1", "t0"));
    apply_live(&mut model, tool_use("req-1"));
    assert_eq!(tool_auto_flags(&model), vec![None]);
}
