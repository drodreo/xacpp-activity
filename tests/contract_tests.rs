//! Contract tests: real wire JSON samples frozen as fixtures, asserted to
//! round-trip into the typed vocabulary without loss.
//!
//! Fixture sources (annotated per test):
//! - `event_bridge.rs` `convert_event` in the x-agent xacpp listener worktree
//!   (`~/Projects/drodreo/x-agent-xacpp-listener`) — authoritative real-time
//!   wire shapes (`think` family top-level camelCase `partId`, `content`
//!   family nested `payload`, `pair_complete`/`complete` camelCase).
//! - `conversation_projection.rs` in the same worktree — replay part ids
//!   (`replay:thinking:{pair}`, `replay:text:{pair}:{idx}`).
//! - `interaction.rs` in xacpp 0.8.3 — interaction command/response serde
//!   shapes (`tag = "type"`, snake_case variants, camelCase fields).

use serde_json::{Value, json};
use xacpp::wire::events::{ActionResponse, QuestionResponse};
use xacpp_activity::ActivityEvent;
use xacpp_activity::event::{BlockBoundaryPayload, ThinkContentPayload};

/// Parse a raw wire JSON object (`{"name": ..., "data": ...}`) the way the
/// transport layer would, then lift it into the typed vocabulary.
fn parse_wire(raw: Value) -> ActivityEvent {
    let event: xacpp::wire::events::XacppEvent = serde_json::from_value(raw).unwrap();
    ActivityEvent::from_xacpp_event(&event).unwrap()
}

// ---- think family: top-level camelCase partId (source: convert_event) ----

#[test]
fn contract_think_delta() {
    // Source: AgentEvent::ThinkDelta -> {"content","round","pair","partId"}
    let event = parse_wire(json!({
        "name": "think",
        "data": {"content": "thinking…", "round": "r1", "pair": "p1", "partId": "tp0"}
    }));
    match event {
        ActivityEvent::ThinkDelta(ThinkContentPayload {
            content,
            round,
            pair,
            part_id,
        }) => {
            assert_eq!(content, "thinking…");
            assert_eq!(round, "r1");
            assert_eq!(pair, "p1");
            assert_eq!(part_id, "tp0");
        }
        other => panic!("expected ThinkDelta, got {other:?}"),
    }
}

#[test]
fn contract_think_part_replay_snapshot() {
    // Source: projection ThinkPart, partId = "replay:thinking:{pair}"
    let event = parse_wire(json!({
        "name": "think_part",
        "data": {"content": "full reasoning", "round": "r1", "pair": "p1", "partId": "replay:thinking:p1"}
    }));
    match event {
        ActivityEvent::ThinkPart(p) => {
            assert_eq!(p.part_id, "replay:thinking:p1");
            assert_eq!(p.content, "full reasoning");
        }
        other => panic!("expected ThinkPart, got {other:?}"),
    }
}

#[test]
fn contract_block_boundaries() {
    // Source: convert_event ThinkStart/ThinkEnd/ContentStart/ContentEnd ->
    // {"partId","round","pair"} (key order in the literal differs per event;
    // JSON objects are unordered so one shape covers all four).
    for name in ["think_start", "think_end", "content_start", "content_end"] {
        let event = parse_wire(json!({
            "name": name,
            "data": {"partId": "replay:text:p1:0", "round": "r1", "pair": "p1"}
        }));
        let payload = match (&event, name) {
            (ActivityEvent::ThinkStart(p), "think_start")
            | (ActivityEvent::ThinkEnd(p), "think_end")
            | (ActivityEvent::ContentStart(p), "content_start")
            | (ActivityEvent::ContentEnd(p), "content_end") => p.clone(),
            _ => panic!("unexpected variant for {name}"),
        };
        assert_eq!(
            payload,
            BlockBoundaryPayload {
                round: "r1".into(),
                pair: "p1".into(),
                part_id: "replay:text:p1:0".into(),
            }
        );
    }
}

// ---- content family: nested payload (source: convert_event + content.rs) ----

#[test]
fn contract_content_delta_text() {
    // Source: ContentDeltaEvent {round, pair, payload: ContentPart} camelCase;
    // ContentPart::Text = {"type":"text","text":...,"partId":...}
    let event = parse_wire(json!({
        "name": "content_delta",
        "data": {
            "round": "r1",
            "pair": "p1",
            "payload": {"type": "text", "text": "hello", "partId": "live-part-1"}
        }
    }));
    match event {
        ActivityEvent::ContentDelta(e) => {
            assert_eq!(e.round, "r1");
            assert_eq!(e.pair, "p1");
            match &e.payload {
                xacpp::wire::events::ContentPart::Text(t) => {
                    assert_eq!(t.text, "hello");
                    assert_eq!(t.part_id.as_deref(), Some("live-part-1"));
                }
                other => panic!("expected Text part, got {other:?}"),
            }
        }
        other => panic!("expected ContentDelta, got {other:?}"),
    }
}

#[test]
fn contract_content_part_replay_text() {
    // Source: projection flush_text -> partId = "replay:text:{pair}:{idx}"
    let event = parse_wire(json!({
        "name": "content_part",
        "data": {
            "round": "r1",
            "pair": "p1",
            "payload": {"type": "text", "text": "merged text", "partId": "replay:text:p1:0"}
        }
    }));
    match event {
        ActivityEvent::ContentPart(e) => match &e.payload {
            xacpp::wire::events::ContentPart::Text(t) => {
                assert_eq!(t.part_id.as_deref(), Some("replay:text:p1:0"));
                assert_eq!(t.text, "merged text");
            }
            other => panic!("expected Text part, got {other:?}"),
        },
        other => panic!("expected ContentPart, got {other:?}"),
    }
}

// ---- start / complete / pair_complete (source: convert_event) ----

#[test]
fn contract_start_inputs() {
    let event = parse_wire(json!({
        "name": "start",
        "data": {"inputs": [{"type": "text", "text": "hi", "partId": "in1"}]}
    }));
    match event {
        ActivityEvent::Start(p) => {
            assert_eq!(p.inputs.len(), 1);
            match &p.inputs[0] {
                xacpp::wire::events::ContentPart::Text(t) => {
                    assert_eq!(t.text, "hi");
                    assert_eq!(t.part_id.as_deref(), Some("in1"));
                }
                other => panic!("expected Text part, got {other:?}"),
            }
        }
        other => panic!("expected Start, got {other:?}"),
    }
}

#[test]
fn contract_complete_assistant_reply() {
    // Source (strict 1.2): Complete -> {"round", "assistantReply",
    // [ContentPart...], "canceled"}; abortReason optional (absent here).
    let event = parse_wire(json!({
        "name": "complete",
        "data": {"round": "r1", "assistantReply": [{"type": "text", "text": "done"}], "canceled": false}
    }));
    match event {
        ActivityEvent::Complete(p) => {
            assert_eq!(p.round, "r1");
            assert_eq!(p.assistant_reply.len(), 1);
            assert!(!p.canceled);
            assert_eq!(p.abort_reason, None);
        }
        other => panic!("expected Complete, got {other:?}"),
    }
}

#[test]
fn contract_pair_complete_usage() {
    // Source: PairComplete -> {"pair", "contextWindow", "tokenUsage":
    // TokenUsage} (xacpp TokenUsage: id/message/promptTokens/completionTokens/totalTokens)
    let event = parse_wire(json!({
        "name": "pair_complete",
        "data": {
            "pair": "p1",
            "contextWindow": 200000,
            "tokenUsage": {
                "id": "u1",
                "message": "m1",
                "promptTokens": 120,
                "completionTokens": 80,
                "totalTokens": 200
            }
        }
    }));
    match event {
        ActivityEvent::PairComplete(p) => {
            assert_eq!(p.pair, "p1");
            assert_eq!(p.context_window, 200000);
            assert_eq!(p.token_usage.prompt_tokens, 120);
            assert_eq!(p.token_usage.completion_tokens, 80);
            assert_eq!(p.token_usage.total_tokens, 200);
        }
        other => panic!("expected PairComplete, got {other:?}"),
    }
}

// ---- tool family (source: convert_event + payload.rs camelCase) ----

#[test]
fn contract_tool_use_and_result() {
    let use_event = parse_wire(json!({
        "name": "tool_use",
        "data": {
            "requestId": "call-1",
            "toolName": "shell",
            "index": 0,
            "arguments": "{\"path\":\"/tmp\"}",
            "intent": "list dir"
        }
    }));
    match use_event {
        ActivityEvent::ToolUse(e) => {
            assert_eq!(e.request_id, "call-1");
            assert_eq!(e.tool_name, "shell");
            assert_eq!(e.index, 0);
            assert_eq!(e.arguments, "{\"path\":\"/tmp\"}");
            assert_eq!(e.intent, "list dir");
        }
        other => panic!("expected ToolUse, got {other:?}"),
    }

    let result_event = parse_wire(json!({
        "name": "tool_result",
        "data": {
            "requestId": "call-1",
            "toolName": "shell",
            "index": 0,
            "parts": [{"type": "text", "text": "files…"}],
            "status": "completed",
            "isError": false,
            "auto": false
        }
    }));
    match result_event {
        ActivityEvent::ToolResult(e) => {
            assert_eq!(e.request_id, "call-1");
            assert_eq!(e.parts.len(), 1);
            assert_eq!(e.status, Some(xacpp_activity::ToolResultStatus::Completed));
            assert!(!e.is_error);
            assert!(!e.auto);
        }
        other => panic!("expected ToolResult, got {other:?}"),
    }
}

#[test]
fn contract_resource_diff() {
    // Source: x-agent core ResourceDiffEvent passthrough (camelCase)
    let event = parse_wire(json!({
        "name": "resource_diff",
        "data": {
            "requestId": "call-1",
            "toolName": "edit",
            "index": 0,
            "round": "r1",
            "filePath": "/tmp/a.rs",
            "diff": "--- a\n+++ b\n@@ -1 +1 @@"
        }
    }));
    match event {
        ActivityEvent::ResourceDiff(p) => {
            assert_eq!(p.file_path, "/tmp/a.rs");
            assert!(p.diff.contains("@@"));
        }
        other => panic!("expected ResourceDiff, got {other:?}"),
    }
}

// ---- trace / notify / alert / upload (source: convert_event) ----

#[test]
fn contract_info_warn_error_notify() {
    for name in ["info", "warn", "error"] {
        let event = parse_wire(json!({
            "name": name,
            "data": {"title": "head", "content": "detail"}
        }));
        let (title, content) = match (&event, name) {
            (ActivityEvent::Info(e), "info")
            | (ActivityEvent::Warn(e), "warn")
            | (ActivityEvent::Error(e), "error") => (e.title.as_str(), e.content.as_str()),
            _ => panic!("unexpected variant for {name}"),
        };
        assert_eq!((title, content), ("head", "detail"));
    }

    let event = parse_wire(json!({
        "name": "notify",
        "data": {"message": "done!"}
    }));
    match event {
        ActivityEvent::Notify(p) => assert_eq!(p.message, "done!"),
        other => panic!("expected Notify, got {other:?}"),
    }
}

#[test]
fn contract_security_alert() {
    let event = parse_wire(json!({
        "name": "security_alert",
        "data": {
            "eventId": "se-1",
            "toolName": "shell",
            "alertLevel": "warn",
            "description": "suspicious command",
            "threatType": "destructive",
            "matchedPattern": "rm -rf",
            "contextSnippet": "rm -rf /"
        }
    }));
    match event {
        ActivityEvent::SecurityAlert(e) => {
            assert_eq!(e.event_id, "se-1");
            assert_eq!(e.alert_level, xacpp::wire::events::AlertLevel::Warn);
            assert_eq!(e.threat_type.as_deref(), Some("destructive"));
        }
        other => panic!("expected SecurityAlert, got {other:?}"),
    }
}

#[test]
fn contract_upload_progress() {
    // Source: UploadEvent tag="upload_event" (literal key; rename_all applies
    // to variant values only -> "progress"/"completed"/"error")
    let event = parse_wire(json!({
        "name": "upload",
        "data": {"upload_event": "progress", "name": "file.bin", "uploaded": 5, "total": 10}
    }));
    match event {
        ActivityEvent::Upload(xacpp::wire::events::UploadEvent::Progress {
            name,
            uploaded,
            total,
        }) => {
            assert_eq!(name, "file.bin");
            assert_eq!((uploaded, total), (5, 10));
        }
        other => panic!("expected Upload::Progress, got {other:?}"),
    }
}

// ---- sub-activity family (source: convert_event) ----

#[test]
fn contract_activity_lifecycle_events() {
    let start = parse_wire(json!({
        "name": "activity_start",
        "data": {"goal": "do thing", "activity": "sub-1"}
    }));
    match start {
        ActivityEvent::ActivityStart(e) => {
            assert_eq!(
                (e.goal.as_str(), e.activity.as_str()),
                ("do thing", "sub-1")
            );
        }
        other => panic!("expected ActivityStart, got {other:?}"),
    }

    let done = parse_wire(json!({
        "name": "activity_done",
        "data": {"activity": "sub-1"}
    }));
    match done {
        ActivityEvent::ActivityDone(p) => assert_eq!(p.activity, "sub-1"),
        other => panic!("expected ActivityDone, got {other:?}"),
    }

    let aborted = parse_wire(json!({
        "name": "activity_aborted",
        "data": {"activity": "sub-1", "reason": "timeout"}
    }));
    match aborted {
        ActivityEvent::ActivityAborted(p) => {
            assert_eq!(
                (p.activity.as_str(), p.reason.as_str()),
                ("sub-1", "timeout")
            );
        }
        other => panic!("expected ActivityAborted, got {other:?}"),
    }

    let waiting = parse_wire(json!({"name": "waiting_command", "data": {}}));
    assert_eq!(waiting, ActivityEvent::WaitingCommand);

    let checklist = parse_wire(json!({
        "name": "checklist_update",
        "data": {"checklist": [{"text": "step", "status": "pending"}]}
    }));
    match checklist {
        ActivityEvent::ChecklistUpdate(p) => {
            assert_eq!(p.checklist[0]["text"], "step");
        }
        other => panic!("expected ChecklistUpdate, got {other:?}"),
    }
}

// ---- interaction command/response shapes (source: xacpp 0.8.3 interaction.rs) ----

#[test]
fn contract_interaction_payloads_and_responses() {
    // action_request command arguments
    let payload: xacpp::wire::events::ActionRequestPayload = serde_json::from_value(json!({
        "toolName": "shell",
        "arguments": "{\"cmd\":\"date\"}",
        "actionId": "act-1",
        "description": "run date",
        "alert": "info",
        "intent": "clock"
    }))
    .unwrap();
    assert_eq!(payload.tool_name, "shell");

    // action response data: {"type": "approve"} family
    let approve: ActionResponse = serde_json::from_value(json!({"type": "approve"})).unwrap();
    assert!(matches!(approve, ActionResponse::Approve));
    let approve_always: ActionResponse =
        serde_json::from_value(json!({"type": "approve_always"})).unwrap();
    assert!(matches!(approve_always, ActionResponse::ApproveAlways));
    let reject: ActionResponse =
        serde_json::from_value(json!({"type": "reject", "reason": "no"})).unwrap();
    assert!(matches!(reject, ActionResponse::Reject { ref reason } if reason == "no"));

    // question command arguments + response data
    let q: xacpp::wire::events::QuestionPayload = serde_json::from_value(json!({
        "question": "continue?",
        "options": ["yes", "no"]
    }))
    .unwrap();
    assert_eq!(q.options.len(), 2);
    let answer: QuestionResponse =
        serde_json::from_value(json!({"type": "answer", "content": "yes"})).unwrap();
    assert!(matches!(answer, QuestionResponse::Answer { ref content } if content == "yes"));
    let skip: QuestionResponse =
        serde_json::from_value(json!({"type": "skip", "reason": "later"})).unwrap();
    assert!(
        matches!(skip, QuestionResponse::Skip { ref reason } if reason.as_deref() == Some("later"))
    );
}

// ---- conversation_history response envelope (source: session_handler.rs) ----

#[test]
fn contract_conversation_history_response_events() {
    // Source: XacppResponse::generic("conversation_history",
    //   {"events": [...], "nextBeforeSeqInTopic": ..., "hasMore": ...})
    let data = json!({
        "events": [
            {"name": "start", "data": {"inputs": [{"type": "text", "text": "q"}]}},
            {"name": "think_part", "data": {"content": "reasoning", "round": "r1", "pair": "p1", "partId": "replay:thinking:p1"}},
            {"name": "content_part", "data": {"round": "r1", "pair": "p1", "payload": {"type": "text", "text": "a", "partId": "replay:text:p1:0"}}},
            {"name": "pair_complete", "data": {"pair": "p1", "contextWindow": 0, "tokenUsage": {"id": "", "message": "", "promptTokens": 0, "completionTokens": 0, "totalTokens": 0}}}
        ],
        "nextBeforeSeqInTopic": 3,
        "hasMore": true
    });

    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct HistoryResponse {
        events: Vec<xacpp::wire::events::XacppEvent>,
        next_before_seq_in_topic: Option<i64>,
        has_more: bool,
    }

    let resp: HistoryResponse = serde_json::from_value(data).unwrap();
    assert!(resp.has_more);
    assert_eq!(resp.next_before_seq_in_topic, Some(3));

    let parsed: Vec<ActivityEvent> = resp
        .events
        .iter()
        .map(ActivityEvent::from_xacpp_event)
        .collect::<Result<_, _>>()
        .unwrap();
    let names: Vec<&str> = parsed.iter().map(|e| e.event_name()).collect();
    assert_eq!(
        names,
        vec!["start", "think_part", "content_part", "pair_complete"]
    );

    // Replay part ids survive the round-trip untouched.
    match &parsed[1] {
        ActivityEvent::ThinkPart(p) => assert_eq!(p.part_id, "replay:thinking:p1"),
        other => panic!("expected ThinkPart, got {other:?}"),
    }
    match &parsed[2] {
        ActivityEvent::ContentPart(e) => match &e.payload {
            xacpp::wire::events::ContentPart::Text(t) => {
                assert_eq!(t.part_id.as_deref(), Some("replay:text:p1:0"))
            }
            other => panic!("expected Text part, got {other:?}"),
        },
        other => panic!("expected ContentPart, got {other:?}"),
    }
}

// ---- round-trip losslessness on representative wire literals ----

#[test]
fn roundtrip_wire_data_is_lossless() {
    // Serialized form of each parse path must equal the original data when
    // re-emitted (field naming preserved byte-for-byte via camelCase).
    let samples = vec![
        json!({
            "name": "think",
            "data": {"content": "c", "round": "r", "pair": "p", "partId": "t"}
        }),
        json!({
            "name": "content_delta",
            "data": {"round": "r", "pair": "p", "payload": {"type": "text", "text": "x", "partId": "id", "partType": "default"}}
        }),
        json!({
            "name": "tool_use",
            "data": {"requestId": "rq", "toolName": "shell", "index": 3, "arguments": "{}", "intent": "i"}
        }),
        json!({
            "name": "pair_complete",
            "data": {"pair": "p", "contextWindow": 8, "tokenUsage": {"id": "", "message": "", "promptTokens": 1, "completionTokens": 2, "totalTokens": 3}}
        }),
    ];

    for sample in samples {
        let expected_data = sample["data"].clone();
        let event = parse_wire(sample.clone());
        let wire = event.to_wire();
        assert_eq!(wire.name, sample["name"], "name must be preserved");
        assert_eq!(
            wire.data, expected_data,
            "data must round-trip losslessly for {}",
            sample["name"]
        );
    }
}

#[test]
fn unknown_and_malformed_wire_forms_are_rejected() {
    let event: Result<xacpp::wire::events::XacppEvent, _> =
        serde_json::from_value(json!({"name": "mystery", "data": {}}));
    let event = event.unwrap();
    let err = ActivityEvent::from_xacpp_event(&event).unwrap_err();
    assert!(matches!(
        err,
        xacpp_activity::ActivityError::UnknownEvent(_)
    ));

    let event: xacpp::wire::events::XacppEvent =
        serde_json::from_value(json!({"name": "think", "data": {"content": "no routing"}}))
            .unwrap();
    let err = ActivityEvent::from_xacpp_event(&event).unwrap_err();
    assert!(matches!(
        err,
        xacpp_activity::ActivityError::MalformedPayload { .. }
    ));
}
