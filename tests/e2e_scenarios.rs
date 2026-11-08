//! End-to-end scenario tests: a minimal in-memory Responder drives the full
//! library chain (envelope runtime → conversion → model → reliability →
//! interactions) over real wire event/command shapes.
//!
//! Test-harness decision (phase 6): an in-memory Responder instead of the
//! xacpp-rs peer_e2e harness. The xacpp-rs facility (memory-duplex
//! transports + full Peer establishment/negotiation) exercises the protocol
//! layer — that layer is the consumer's connection channel here
//! (`XacppSession`), and it is already covered by xacpp-rs's own e2e tests.
//! This library's public API consumes post-protocol shapes
//! (`XacppEvent` / command name+data), so scenarios feed exactly those,
//! frozen from the phase-1/3 contract fixtures — no new wire shapes are
//! invented, no ports, no network.

use serde_json::{Value, json};
use xacpp::wire::events::{ActionResponse, QuestionResponse, XacppEvent};
use xacpp_activity::{
    ActivityEvent, ActivityEventCategory, ActivityModel, ActivityRuntime, ConsumeOutcome,
    InteractionAnswer, InteractionState, SyncPlan, TurnStatus,
};

/// Unwraps a fresh allocation. Every `consume_command` payload in this
/// file carries no requestId, so the outcome is always `New`.
fn expect_new(outcome: ConsumeOutcome) -> u64 {
    match outcome {
        ConsumeOutcome::New(id) => id,
        other => panic!("expected fresh allocation, got {other:?}"),
    }
}

/// The consumer device: per-activity runtime + session model + watermark,
/// the shape xamobile/agentx consumers keep.
struct Consumer {
    runtime: ActivityRuntime,
    model: ActivityModel,
    last_applied: u64,
}

impl Consumer {
    fn new() -> Self {
        Self {
            runtime: ActivityRuntime::new("act-1"),
            model: ActivityModel::new(),
            last_applied: 0,
        }
    }

    /// Applies one wire event live (dispatch assigns the seq, the model
    /// consumes the envelope, the watermark follows).
    fn on_wire_event(&mut self, name: &str, data: Value) -> u64 {
        let wire = wire_event(name, data);
        let typed =
            ActivityEvent::from_xacpp_event(&wire).expect("scenario wire shapes must parse");
        let envelope = self.runtime.dispatch(ActivityEventCategory::Default, typed);
        self.model.apply_envelope(&envelope);
        self.last_applied = envelope.seq;
        envelope.seq
    }

    /// Pulls the projection (history) as one authoritative batch.
    fn pull_history(&mut self, events: Vec<(String, Value)>) {
        let wire: Vec<XacppEvent> = events
            .into_iter()
            .map(|(name, data)| wire_event(&name, data))
            .collect();
        self.model.apply_history_events(wire);
    }
}

/// Wire event literal (same shape the transport decodes: {name, data}).
fn wire_event(name: &str, data: Value) -> XacppEvent {
    XacppEvent::new(name, data)
}

fn think_start(pair: &str, part_id: &str) -> (String, Value) {
    (
        "think_start".into(),
        json!({"partId": part_id, "round": "r1", "pair": pair}),
    )
}

fn think_delta(pair: &str, part_id: &str, content: &str) -> (String, Value) {
    (
        "think".into(),
        json!({"content": content, "round": "r1", "pair": pair, "partId": part_id}),
    )
}

fn think_part(pair: &str, part_id: &str, content: &str) -> (String, Value) {
    (
        "think_part".into(),
        json!({"content": content, "round": "r1", "pair": pair, "partId": part_id}),
    )
}

fn think_end(pair: &str, part_id: &str) -> (String, Value) {
    (
        "think_end".into(),
        json!({"partId": part_id, "round": "r1", "pair": pair}),
    )
}

fn content_start(pair: &str, part_id: &str) -> (String, Value) {
    (
        "content_start".into(),
        json!({"partId": part_id, "round": "r1", "pair": pair}),
    )
}

fn content_delta(pair: &str, part_id: &str, text: &str) -> (String, Value) {
    (
        "content_delta".into(),
        json!({"round": "r1", "pair": pair, "payload": {"type": "text", "text": text, "partId": part_id}}),
    )
}

fn content_part(pair: &str, part_id: &str, text: &str) -> (String, Value) {
    (
        "content_part".into(),
        json!({"round": "r1", "pair": pair, "payload": {"type": "text", "text": text, "partId": part_id}}),
    )
}

fn content_end(pair: &str, part_id: &str) -> (String, Value) {
    (
        "content_end".into(),
        json!({"partId": part_id, "round": "r1", "pair": pair}),
    )
}

fn tool_use(request_id: &str) -> (String, Value) {
    (
        "tool_use".into(),
        json!({"requestId": request_id, "toolName": "shell", "index": 0, "arguments": "{\"cmd\":\"date\"}", "intent": "clock"}),
    )
}

fn tool_result(request_id: &str) -> (String, Value) {
    (
        "tool_result".into(),
        json!({"requestId": request_id, "toolName": "shell", "index": 0, "parts": [{"type": "text", "text": "2026-10-05"}], "status": "completed", "isError": false, "auto": false}),
    )
}

fn pair_complete(pair: &str) -> (String, Value) {
    (
        "pair_complete".into(),
        json!({"pair": pair, "contextWindow": 200000, "tokenUsage": {"id": "u1", "message": "m1", "promptTokens": 120, "completionTokens": 80, "totalTokens": 200}}),
    )
}

fn complete() -> (String, Value) {
    (
        "complete".into(),
        json!({"round": "r1", "assistantReply": [{"type": "text", "text": "done"}], "canceled": false}),
    )
}

fn trace(name: &str) -> (String, Value) {
    (
        name.to_string(),
        json!({"title": format!("{name} head"), "content": format!("{name} body")}),
    )
}

/// Scenario 1: a full assistant turn streams in and models exactly.
#[test]
fn scenario_full_turn_end_to_end() {
    let mut consumer = Consumer::new();

    consumer.on_wire_event(
        "start",
        json!({"inputs": [{"type": "text", "text": "what time"}]}),
    );
    consumer.on_wire_event("think_start", think_start("p1", "live-t0").1);
    consumer.on_wire_event("think", think_delta("p1", "live-t0", "user wants ").1);
    consumer.on_wire_event("think", think_delta("p1", "live-t0", "the time").1);
    consumer.on_wire_event("think_end", think_end("p1", "live-t0").1);
    // Two text parts in one turn.
    consumer.on_wire_event("content_start", content_start("p1", "live-c0").1);
    consumer.on_wire_event("content_delta", content_delta("p1", "live-c0", "it is ").1);
    consumer.on_wire_event("content_delta", content_delta("p1", "live-c0", "10 am. ").1);
    consumer.on_wire_event("content_end", content_end("p1", "live-c0").1);
    consumer.on_wire_event("content_start", content_start("p1", "live-c1").1);
    consumer.on_wire_event(
        "content_part",
        content_part("p1", "live-c1", "Elapsed: none.").1,
    );
    consumer.on_wire_event("content_end", content_end("p1", "live-c1").1);
    consumer.on_wire_event("tool_use", tool_use("req-1").1);
    consumer.on_wire_event("tool_result", tool_result("req-1").1);
    consumer.on_wire_event("info", trace("info").1);
    consumer.on_wire_event("warn", trace("warn").1);
    consumer.on_wire_event("error", trace("error").1);
    consumer.on_wire_event("notify", json!({"message": "done!"}));
    consumer.on_wire_event("pair_complete", pair_complete("p1").1);
    consumer.on_wire_event("complete", complete().1);

    // Turn terminal state, field by field.
    assert_eq!(consumer.model.get_turns().len(), 1);
    let turn = &consumer.model.get_turns()[0];
    assert_eq!(turn.id, "p1");
    assert_eq!(turn.think.as_ref().unwrap().content, "user wants the time");
    assert!(!turn.think.as_ref().unwrap().streaming);
    assert_eq!(turn.contents.len(), 2);
    assert_eq!(turn.contents[0].text, "it is 10 am. ");
    assert_eq!(turn.contents[1].text, "Elapsed: none.");
    assert_eq!(turn.tools.len(), 1);
    assert_eq!(turn.tools[0].request_id, "req-1");
    assert_eq!(turn.tools[0].tool_name, "shell");
    assert_eq!(turn.tools[0].result.as_ref().unwrap().parts.len(), 1);
    assert_eq!(turn.status, TurnStatus::Finalized);
    assert!(consumer.model.finalized_pairs.contains("p1"));

    // Process + side events + inputs.
    assert_eq!(consumer.model.process.len(), 4);
    assert_eq!(consumer.model.last_inputs.len(), 1);
    assert!(
        consumer
            .model
            .side_events
            .iter()
            .any(|e| matches!(e, ActivityEvent::Complete(_)))
    );

    // Pair-complete bookkeeping and buffer lifecycle.
    assert_eq!(consumer.runtime.pair_epoch(), 1);
    assert!(consumer.runtime.current_pair_events().is_empty());
}

/// Scenario 2: live stream truncates mid-turn, the consumer pulls the
/// authoritative projection, then the new live stream continues — zero
/// duplicated blocks, zero lost parts, and late increments for the
/// finalized pair are dropped.
#[test]
fn scenario_disconnect_resync_convergence() {
    let mut consumer = Consumer::new();

    // Live stream truncates mid-think (push loss window).
    consumer.on_wire_event("think_start", think_start("p1", "live-t0").1);
    consumer.on_wire_event("think", think_delta("p1", "live-t0", "stream").1);
    // ... connection drops here: mid-think, mid-turn.

    // Consumer re-enters and pulls the projection: the authoritative full
    // turn (replay: part ids) replaces whatever partial state existed.
    consumer.pull_history(vec![
        think_start("p1", "replay:thinking:p1"),
        think_part("p1", "replay:thinking:p1", "stream full thinking"),
        think_end("p1", "replay:thinking:p1"),
        content_start("p1", "replay:text:p1:0"),
        content_part("p1", "replay:text:p1:0", "final answer"),
        content_end("p1", "replay:text:p1:0"),
        tool_use("req-1"),
        tool_result("req-1"),
        pair_complete("p1"),
    ]);

    assert_eq!(
        consumer.model.get_turns().len(),
        1,
        "partial live turn replaced"
    );
    let turn = &consumer.model.get_turns()[0];
    assert_eq!(turn.id, "p1");
    assert_eq!(turn.think.as_ref().unwrap().content, "stream full thinking");
    assert_eq!(turn.contents.len(), 1);
    assert_eq!(turn.contents[0].text, "final answer");
    assert_eq!(turn.status, TurnStatus::Finalized);

    // Reconnected live stream continues with a new pair.
    consumer.on_wire_event("think_start", think_start("p2", "live-t1").1);
    consumer.on_wire_event("think", think_delta("p2", "live-t1", "next pair").1);
    consumer.on_wire_event("pair_complete", pair_complete("p2").1);

    // Late buffered increment for the finalized p1 arrives after the
    // resync (stale backlog flushed by the reconnect): must be dropped.
    consumer.on_wire_event("think", think_delta("p1", "live-t0", " late").1);
    consumer.on_wire_event("content_start", content_start("p1", "live-c0").1);

    assert_eq!(
        consumer.model.get_turns().len(),
        2,
        "p1 + p2, no duplication"
    );
    assert_eq!(
        consumer.model.get_turns()[0]
            .think
            .as_ref()
            .unwrap()
            .content,
        "stream full thinking",
        "p1 stays the projection snapshot"
    );
    assert_eq!(consumer.model.get_turns()[0].contents.len(), 1);
    assert_eq!(consumer.model.get_turns()[1].id, "p2");
    assert_eq!(
        consumer.model.get_turns()[1]
            .think
            .as_ref()
            .unwrap()
            .content,
        "next pair"
    );
}

/// Scenario 3: interaction closed loop over the wire command shapes.
#[test]
fn scenario_interaction_closed_loop() {
    let mut consumer = Consumer::new();

    // Responder sends action_request / question commands (wire argument
    // shapes from the phase-1 contract fixtures).
    let action_id = consumer
        .runtime
        .consume_command(
            "action_request",
            json!({
                "toolName": "shell",
                "arguments": "{\"cmd\":\"date\"}",
                "actionId": "a-1",
                "description": "run date",
                "alert": "warn",
                "intent": "clock"
            }),
        )
        .map(expect_new)
        .unwrap();
    let question_id = consumer
        .runtime
        .consume_command(
            "question",
            json!({"question": "continue?", "options": ["yes", "no"]}),
        )
        .map(expect_new)
        .unwrap();

    // UI renders the pending snapshot.
    let pending = consumer.runtime.pending_interactions();
    assert_eq!(pending.len(), 2);

    // Approve the action: wire response shape asserted field by field.
    let response = consumer
        .runtime
        .respond(
            action_id,
            InteractionAnswer::Action(ActionResponse::Approve),
        )
        .unwrap();
    assert_eq!(
        serde_json::to_value(&response).unwrap(),
        json!({"kind": "generic", "name": "action", "data": {"type": "approve"}})
    );

    // Answer the question.
    let response = consumer
        .runtime
        .respond(
            question_id,
            InteractionAnswer::Question(QuestionResponse::Answer {
                content: "yes".into(),
            }),
        )
        .unwrap();
    assert_eq!(
        serde_json::to_value(&response).unwrap(),
        json!({
            "kind": "generic", "name": "question",
            "data": {"type": "answer", "content": "yes"}
        })
    );
    assert!(consumer.runtime.pending_interactions().is_empty());

    // Degradation path: a new interaction the consumer cannot deliver.
    let stranded = consumer
        .runtime
        .consume_command(
            "action_request",
            json!({
                "toolName": "shell",
                "arguments": "{}",
                "actionId": "a-2",
                "description": "d",
                "alert": "info",
                "intent": ""
            }),
        )
        .map(expect_new)
        .unwrap();
    consumer
        .runtime
        .degrade(stranded, "connection lost")
        .unwrap();
    assert!(
        consumer
            .runtime
            .pending_interactions()
            .iter()
            .all(|i| i.id != stranded)
    );
    let err = consumer
        .runtime
        .respond(stranded, InteractionAnswer::Action(ActionResponse::Approve))
        .unwrap_err();
    assert!(matches!(
        err,
        xacpp_activity::InteractionError::AlreadyAnswered { .. }
    ));

    // State trail kept for audit.
    assert_eq!(consumer.runtime.pending_interactions().len(), 0);
    assert_eq!(pending[0].state, InteractionState::Pending);
}

/// Scenario 4: envelopes buffered while the consumer was detached; recovery
/// via need_sync BufferReplay; pagination idempotency on top.
#[test]
fn scenario_need_sync_buffer_replay_and_pagination() {
    let mut consumer = Consumer::new();

    // Events 1..5 arrive at the runtime (buffered); the consumer only
    // applied the first two before detaching.
    let mut all_envelopes = Vec::new();
    for i in 0..5 {
        let typed = ActivityEvent::from_xacpp_event(&wire_event(
            "think",
            think_delta("p1", "t0", &format!("w{i} ")).1,
        ))
        .expect("scenario wire shapes must parse");
        let envelope = consumer
            .runtime
            .dispatch(ActivityEventCategory::Default, typed);
        all_envelopes.push(envelope.clone());
        if envelope.seq <= 2 {
            consumer.model.apply_envelope(&envelope);
            consumer.last_applied = envelope.seq;
        }
    }

    // Resume: the gap is contiguous with the buffer start.
    let SyncPlan::BufferReplay {
        envelopes,
        new_last_seq,
    } = consumer.runtime.need_sync(consumer.last_applied)
    else {
        panic!("expected BufferReplay for a contiguous gap");
    };
    assert_eq!(envelopes.len(), 3);
    assert_eq!(new_last_seq, 5);

    consumer.model.apply_buffer_replay(envelopes);
    consumer.last_applied = new_last_seq;
    assert_eq!(
        consumer.model.get_turns()[0]
            .think
            .as_ref()
            .unwrap()
            .content,
        "w0 w1 w2 w3 w4 "
    );
    assert_eq!(
        consumer.runtime.need_sync(consumer.last_applied),
        SyncPlan::UpToDate
    );

    // Full-reference equivalence: a consumer that never detached converges
    // to the same model.
    let mut reference = Consumer::new();
    for envelope in &all_envelopes {
        reference.model.apply_envelope(envelope);
    }
    assert_eq!(consumer.model, reference.model);

    // Pagination on top: out-of-order pages plus a duplicate stay idempotent.
    consumer.pull_history(vec![
        think_start("p3", "replay:thinking:p3"),
        think_part("p3", "replay:thinking:p3", "t3"),
        think_end("p3", "replay:thinking:p3"),
        pair_complete("p3"),
        think_start("p4", "replay:thinking:p4"),
        think_part("p4", "replay:thinking:p4", "t4"),
        think_end("p4", "replay:thinking:p4"),
        pair_complete("p4"),
    ]);
    consumer.pull_history(vec![
        think_start("p2", "replay:thinking:p2"),
        think_part("p2", "replay:thinking:p2", "t2"),
        think_end("p2", "replay:thinking:p2"),
        pair_complete("p2"),
    ]);
    // Duplicate p3/p4 page (pagination retry).
    consumer.pull_history(vec![
        think_start("p3", "replay:thinking:p3"),
        think_part("p3", "replay:thinking:p3", "t3"),
        think_end("p3", "replay:thinking:p3"),
        pair_complete("p3"),
    ]);

    assert_eq!(consumer.model.turns_count(), 4, "p1..p4, no duplicates");
    let mut ids: Vec<String> = consumer
        .model
        .get_turns()
        .iter()
        .map(|turn| turn.id.clone())
        .collect();
    ids.sort();
    assert_eq!(ids, vec!["p1", "p2", "p3", "p4"]);
}
