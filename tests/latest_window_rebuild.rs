//! Permanent regression: the minimal-model + strict-1.2 contract surface.
//!
//! Every scenario here is a verdict from the 2026-10-25 minimal-model
//! audit (the counter-examples that failed the pre-rewrite library):
//! 1. LatestWindow REPLACES the projection (A→B leaves no A behind); the
//!    only carried-over state is the real in-flight tail.
//! 2. Older-page pulls write no content — the cursor advances alone.
//! 3. One streaming derivation: snapshot and turn view agree after
//!    `content_end`, after `pair_complete`, and in between.
//! 4. Tool-card lifecycle is derived (`ToolCard::done`), merged by exactly
//!    three status rules — no stored second state, unknown never erases a
//!    known state, Running never demotes a terminal card.
//! 5. Result facts (`auto` / `isError` / `isBackground`) reach the
//!    snapshot as Options; unknown is never rendered as `false`.
//! 6. Strict 1.2 wire: required members fail loud when missing; optional
//!    members default to unknown; new `PairComplete` / `Complete` fields
//!    round-trip.

mod common;

use common::wire;
use serde_json::json;
use xacpp::wire::events::XacppEvent;
use xacpp_activity::{
    ActivityEvent, ActivityEventCategory, ActivityEventEnvelope, ActivityHub, ActivityModel,
    ActivitySession, SnapshotItem, ToolResultStatus, WindowCursor,
};

const ACT: &str = "act-a";

// ---------------------------------------------------------------- window A→B

/// Window A (one closed turn) then window B (a different closed turn):
/// the projection after B contains no trace of A — replacement, not merge.
#[tokio::test]
async fn latest_window_replacement_leaves_no_stale_content() {
    let mut session = ActivitySession::new(ACT);

    let window_a = vec![
        start_ev("question a"),
        content_part_ev("pA", "replay:cA", "answer a"),
        pair_complete_ev("pA"),
    ];
    session.apply_history(window_a, None);
    let snapshot = session
        .snapshot()
        .iter()
        .filter_map(|item| match item {
            SnapshotItem::User { inputs, .. } => Some(xacpp_activity::inputs_text(inputs)),
            SnapshotItem::Text { text, .. } => Some(text.clone()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("|");
    assert!(snapshot.contains("answer a"), "window A rendered");

    let window_b = vec![
        start_ev("question b"),
        content_part_ev("pB", "replay:cB", "answer b"),
        pair_complete_ev("pB"),
    ];
    session.apply_history(window_b, None);

    let texts: Vec<String> = session
        .snapshot()
        .iter()
        .filter_map(|item| match item {
            SnapshotItem::User { inputs, .. } => Some(xacpp_activity::inputs_text(inputs)),
            SnapshotItem::Text { text, .. } => Some(text.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        texts,
        vec!["question b".to_string(), "answer b".to_string()],
        "A→B: the projection holds B only — no stale A content"
    );
    assert!(!session.turn_active(), "window B's turn is closed");
}

/// The in-flight tail (a live pair still executing when the window lands)
/// survives the rebuild: window content + the real tail, nothing else.
#[tokio::test]
async fn latest_window_rebuild_keeps_only_the_live_tail() {
    let hub = ActivityHub::new();
    hub.register(ACT).await;

    // Live in-flight tail: a pair with a content block still open (no
    // content_end, no pair_complete) — buffered by the runtime.
    common::MockFeed::new()
        .live(ACT, common::start_ev("live input"))
        .live(ACT, common::content_start_ev("pLive", "live-c0"))
        .live(
            ACT,
            common::content_delta_ev("pLive", "live-c0", "streaming…"),
        )
        .drive_all(&hub)
        .await;

    // The authoritative window (only past turns — it cannot contain the
    // in-flight pair).
    hub.with_session(ACT, |session| {
        session.apply_history(
            vec![
                start_ev("past input"),
                content_part_ev("pPast", "replay:cP", "past answer"),
                pair_complete_ev("pPast"),
            ],
            None,
        )
    })
    .await;

    let items = hub.snapshot(ACT).await.expect("registered").items;
    let users: Vec<String> = items
        .iter()
        .filter_map(|item| match item {
            SnapshotItem::User { inputs, .. } => Some(xacpp_activity::inputs_text(inputs)),
            _ => None,
        })
        .collect();
    assert_eq!(
        users,
        vec!["past input".to_string(), "live input".to_string()],
        "window content first, then the carried-over live tail"
    );
    let joined = common::text_of(&items);
    assert!(
        joined.contains("past answer") && joined.contains("streaming…"),
        "window content and tail content both render: {joined}"
    );
    assert!(
        session_turn_active(&hub).await,
        "the in-flight pair keeps the session active"
    );
}

async fn session_turn_active(hub: &ActivityHub) -> bool {
    hub.snapshot(ACT).await.expect("registered").turn_active
}

// ---------------------------------------------------------------- older pages

/// Older-page pulls write no content back: the cursor-only step advances
/// the bookkeeping and the snapshot is untouched.
#[tokio::test]
async fn older_page_writes_no_content_only_cursor() {
    let hub = ActivityHub::new();
    hub.register(ACT).await;

    hub.with_session(ACT, |session| {
        session.apply_history(
            vec![
                start_ev("question"),
                content_part_ev("p1", "replay:c0", "answer"),
                pair_complete_ev("p1"),
            ],
            Some(WindowCursor {
                next_before_seq: Some(42),
                has_more: true,
            }),
        )
    })
    .await;
    let before = hub.snapshot(ACT).await.expect("registered");

    // The older-page pull: cursor alone (content never enters the buffer).
    hub.with_session(ACT, |session| {
        session.set_window_cursor(WindowCursor {
            next_before_seq: Some(41),
            has_more: true,
        })
    })
    .await;

    let after = hub.snapshot(ACT).await.expect("registered");
    assert_eq!(after.items, before.items, "paging wrote no content");
    assert_eq!(
        (after.next_before_seq, after.has_more),
        (Some(41), true),
        "cursor advanced"
    );
    assert_eq!(
        after.content_live, before.content_live,
        "validity untouched"
    );
}

// ---------------------------------------------------------------- streaming

/// One streaming derivation: after `content_end` (flag settled, pair still
/// open) BOTH the snapshot and the turn view read non-streaming; after
/// `pair_complete` both still do; with neither, both read streaming.
#[tokio::test]
async fn snapshot_and_turn_view_streaming_agree() {
    // Phase 1: block open, pair open — both surfaces streaming.
    let mut m = ActivityModel::new();
    feed_live(
        &mut m,
        vec![
            content_start_ev("p1", "c0"),
            common::content_delta_ev("p1", "c0", "text"),
        ],
    );
    assert!(snapshot_streaming(&m), "open block streams (snapshot)");
    assert!(turns_streaming(&m), "open block streams (turn view)");

    // Phase 2: content_end settles the flag; the pair is still open —
    // both surfaces must agree (the old divergence point).
    feed_live(&mut m, vec![common::content_end_ev("p1", "c0")]);
    assert!(
        !snapshot_streaming(&m),
        "content_end ends streaming (snapshot)"
    );
    assert!(
        !turns_streaming(&m),
        "content_end ends streaming (turn view)"
    );

    // Phase 3: pair_complete closes the pair — still agreeing.
    feed_live(&mut m, vec![common::pair_complete_ev("p1")]);
    assert!(!snapshot_streaming(&m));
    assert!(!turns_streaming(&m));

    // Phase 4 (the other divergence shape): flag open, pair closed —
    // both surfaces read non-streaming.
    let mut m2 = ActivityModel::new();
    feed_live(
        &mut m2,
        vec![
            content_start_ev("p2", "c0"),
            common::content_delta_ev("p2", "c0", "text"),
            common::pair_complete_ev("p2"),
        ],
    );
    assert!(!snapshot_streaming(&m2), "closed pair ends streaming");
    assert!(!turns_streaming(&m2), "closed pair ends streaming");
}

fn feed_live(model: &mut ActivityModel, events: Vec<XacppEvent>) {
    for event in events {
        let typed = ActivityEvent::from_xacpp_event(&event).expect("typed");
        model.apply_envelope(&envelope_of(typed));
    }
}

/// Live-path envelope wrapper (the public application entry).
fn envelope_of(event: ActivityEvent) -> ActivityEventEnvelope {
    ActivityEventEnvelope {
        activity_id: ACT.into(),
        seq: 0,
        pair_epoch: 0,
        category: ActivityEventCategory::Default,
        payload: event,
    }
}

fn snapshot_streaming(model: &ActivityModel) -> bool {
    model.snapshot().iter().any(|item| match item {
        SnapshotItem::Text { streaming, .. } | SnapshotItem::Think { streaming, .. } => *streaming,
        _ => false,
    })
}

fn turns_streaming(model: &ActivityModel) -> bool {
    model.get_turns().iter().any(|turn| {
        turn.think.as_ref().is_some_and(|think| think.streaming)
            || turn.contents.iter().any(|content| content.streaming)
    })
}

// ---------------------------------------------------------------- tool card lifecycle

fn card_done(model: &ActivityModel, request_id: &str) -> Option<bool> {
    model
        .snapshot()
        .iter()
        .filter_map(|item| match item {
            SnapshotItem::Tool {
                request_id: rq,
                done,
                ..
            } if rq == request_id => Some(*done),
            _ => None,
        })
        .next()
        .expect("card present")
}

fn feed_typed(model: &mut ActivityModel, events: Vec<ActivityEvent>) {
    for event in events {
        model.apply_envelope(&envelope_of(event));
    }
}

fn tool_use(id: &str) -> ActivityEvent {
    ActivityEvent::from_xacpp_event(&common::tool_use_ev(id)).unwrap()
}

fn tool_result_with(id: &str, status: Option<ToolResultStatus>) -> ActivityEvent {
    use xacpp_activity::event::ToolResultPayload;
    ActivityEvent::ToolResult(ToolResultPayload {
        request_id: id.into(),
        tool_name: "shell".into(),
        index: 0,
        parts: vec![text_part("result text")],
        status,
        is_background: None,
        is_error: false,
        auto: false,
    })
}

fn text_part(text: &str) -> xacpp::wire::events::ContentPart {
    xacpp::wire::events::ContentPart::Text(xacpp::wire::events::TextPart {
        text: text.into(),
        part_id: None,
        part_type: "default".into(),
    })
}

/// The done() derivation table: ToolUse only → running; Running → running;
/// terminal → done; unknown status → unknown (None).
#[tokio::test]
async fn done_derivation_table() {
    let mut m = ActivityModel::new();
    feed_typed(&mut m, vec![tool_use("rq")]);
    assert_eq!(card_done(&m, "rq"), Some(false), "ToolUse only: running");

    feed_typed(
        &mut m,
        vec![tool_result_with("rq", Some(ToolResultStatus::Running))],
    );
    assert_eq!(
        card_done(&m, "rq"),
        Some(false),
        "Running placeholder: running"
    );

    feed_typed(
        &mut m,
        vec![tool_result_with("rq", Some(ToolResultStatus::Completed))],
    );
    assert_eq!(card_done(&m, "rq"), Some(true), "terminal: done");

    let mut m2 = ActivityModel::new();
    feed_typed(&mut m2, vec![tool_use("rq2")]);
    feed_typed(&mut m2, vec![tool_result_with("rq2", None)]);
    assert_eq!(
        card_done(&m2, "rq2"),
        None,
        "unknown status: lifecycle unknown, never forced to running/done"
    );
}

/// Unknown never erases a known state; Running never demotes a terminal.
#[tokio::test]
async fn merge_rules_protect_known_state() {
    // unknown lands on a known-Running card: content refreshes, the known
    // status survives.
    let mut m = ActivityModel::new();
    feed_typed(
        &mut m,
        vec![
            tool_use("rq"),
            tool_result_with("rq", Some(ToolResultStatus::Running)),
        ],
    );
    feed_typed(&mut m, vec![tool_result_with("rq", None)]);
    assert_eq!(
        card_done(&m, "rq"),
        Some(false),
        "unknown kept the known Running status"
    );

    // unknown lands on a known-terminal card: the terminal survives.
    let mut m2 = ActivityModel::new();
    feed_typed(
        &mut m2,
        vec![
            tool_use("rq2"),
            tool_result_with("rq2", Some(ToolResultStatus::Completed)),
        ],
    );
    feed_typed(&mut m2, vec![tool_result_with("rq2", None)]);
    assert_eq!(
        card_done(&m2, "rq2"),
        Some(true),
        "unknown kept the known terminal status"
    );

    // A late Running placeholder lands on a terminal card: no demotion.
    let mut m3 = ActivityModel::new();
    feed_typed(
        &mut m3,
        vec![
            tool_use("rq3"),
            tool_result_with("rq3", Some(ToolResultStatus::Completed)),
        ],
    );
    feed_typed(
        &mut m3,
        vec![tool_result_with("rq3", Some(ToolResultStatus::Running))],
    );
    assert_eq!(
        card_done(&m3, "rq3"),
        Some(true),
        "late Running did not demote the terminal card"
    );
}

// ---------------------------------------------------------------- result facts

fn card_facts(
    model: &ActivityModel,
    request_id: &str,
) -> (Option<bool>, Option<bool>, Option<bool>, Option<String>) {
    model
        .snapshot()
        .iter()
        .find_map(|item| match item {
            SnapshotItem::Tool {
                request_id: rq,
                auto,
                is_error,
                is_background,
                result_text,
                ..
            } if rq == request_id => Some((*auto, *is_error, *is_background, result_text.clone())),
            _ => None,
        })
        .expect("card present")
}

/// Result facts reach the snapshot; unknown/absent is `None`, never `false`.
#[tokio::test]
async fn result_facts_surface_as_options() {
    use xacpp_activity::event::ToolResultPayload;

    // No result yet: all facts unknown (None).
    let mut m = ActivityModel::new();
    feed_typed(&mut m, vec![tool_use("rq")]);
    assert_eq!(
        card_facts(&m, "rq"),
        (None, None, None, None),
        "pending card: every fact unknown"
    );

    // Full result: facts arrive as Some, unknown status still renders the
    // content facts while done stays None.
    let mut m2 = ActivityModel::new();
    feed_typed(&mut m2, vec![tool_use("rq2")]);
    feed_typed(
        &mut m2,
        vec![ActivityEvent::ToolResult(ToolResultPayload {
            request_id: "rq2".into(),
            tool_name: "shell".into(),
            index: 0,
            parts: vec![text_part("the answer")],
            status: None,
            is_background: Some(true),
            is_error: true,
            auto: true,
        })],
    );
    let (auto, is_error, is_background, result_text) = card_facts(&m2, "rq2");
    assert_eq!(auto, Some(true));
    assert_eq!(is_error, Some(true));
    assert_eq!(is_background, Some(true));
    assert_eq!(result_text.as_deref(), Some("the answer"));
    assert_eq!(card_done(&m2, "rq2"), None, "unknown status → done None");
}

// ---------------------------------------------------------------- strict 1.2 wire

/// Strict 1.2 round-trips: optional members default to unknown (never a
/// concrete default); new PairComplete/Complete members survive the round
/// trip.
#[tokio::test]
async fn strict_wire_roundtrip() {
    // pair_complete without tier/model: unknown, and the round trip
    // preserves the optionality (fields are skipped when None).
    let pc = wire(
        "pair_complete",
        json!({"pair": "p1", "contextWindow": 0,
               "tokenUsage": {"id": "", "message": "", "promptTokens": 0,
                              "completionTokens": 0, "totalTokens": 0}}),
    );
    let typed = ActivityEvent::from_xacpp_event(&pc).unwrap();
    match &typed {
        ActivityEvent::PairComplete(p) => {
            assert_eq!(p.tier, None, "absent tier = unknown");
            assert_eq!(p.model, None, "absent model = unknown");
        }
        other => panic!("expected PairComplete, got {other:?}"),
    }
    let back = typed.to_wire();
    assert_eq!(back.data, pc.data, "optional-absent form round-trips");

    // pair_complete with tier/model: values survive.
    let pc2 = wire(
        "pair_complete",
        json!({"pair": "p2", "contextWindow": 128,
               "tokenUsage": {"id": "u", "message": "m", "promptTokens": 1,
                              "completionTokens": 2, "totalTokens": 3},
               "tier": "lite", "model": "test-model"}),
    );
    match ActivityEvent::from_xacpp_event(&pc2).unwrap() {
        ActivityEvent::PairComplete(p) => {
            assert!(
                matches!(p.tier, Some(xacpp_activity::ModelTier::Lite)),
                "tier survives the round trip"
            );
            assert_eq!(p.model.as_deref(), Some("test-model"));
        }
        other => panic!("expected PairComplete, got {other:?}"),
    }

    // complete with cancel facts: both members survive; abortReason absent
    // never implies canceled.
    let cp = wire(
        "complete",
        json!({"round": "r1", "assistantReply": [], "canceled": true}),
    );
    match ActivityEvent::from_xacpp_event(&cp).unwrap() {
        ActivityEvent::Complete(p) => {
            assert!(p.canceled);
            assert_eq!(p.abort_reason, None);
        }
        other => panic!("expected Complete, got {other:?}"),
    }
    let cp2 = wire(
        "complete",
        json!({"round": "r2", "assistantReply": [], "canceled": false,
               "abortReason": "user abort"}),
    );
    match ActivityEvent::from_xacpp_event(&cp2).unwrap() {
        ActivityEvent::Complete(p) => {
            assert!(!p.canceled, "canceled is authoritative, not abortReason");
            assert_eq!(p.abort_reason.as_deref(), Some("user abort"));
        }
        other => panic!("expected Complete, got {other:?}"),
    }
}

/// Strict 1.2 required members fail loud when missing — never silently
/// defaulted.
#[tokio::test]
async fn strict_required_members_reject_missing() {
    // complete without canceled.
    let no_canceled = wire("complete", json!({"round": "r1", "assistantReply": []}));
    assert!(
        ActivityEvent::from_xacpp_event(&no_canceled).is_err(),
        "complete without canceled must fail"
    );

    // tool_result without isError / without auto / without parts.
    for (label, data) in [
        (
            "no isError",
            json!({"requestId": "r", "toolName": "s", "index": 0,
                              "parts": [], "auto": false}),
        ),
        (
            "no auto",
            json!({"requestId": "r", "toolName": "s", "index": 0,
                           "parts": [], "isError": false}),
        ),
        (
            "no parts",
            json!({"requestId": "r", "toolName": "s", "index": 0,
                            "isError": false, "auto": false}),
        ),
    ] {
        let event = wire("tool_result", data);
        assert!(
            ActivityEvent::from_xacpp_event(&event).is_err(),
            "tool_result {label} must fail under strict 1.2"
        );
    }

    // pair_complete without contextWindow / without tokenUsage.
    let token_usage = json!({"id": "", "message": "", "promptTokens": 0,
                             "completionTokens": 0, "totalTokens": 0});
    for (label, data) in [
        (
            "no contextWindow",
            json!({"pair": "p1", "tokenUsage": token_usage}),
        ),
        ("no tokenUsage", json!({"pair": "p1", "contextWindow": 0})),
    ] {
        let event = wire("pair_complete", data);
        assert!(
            ActivityEvent::from_xacpp_event(&event).is_err(),
            "pair_complete {label} must fail under strict 1.2"
        );
    }

    // tool_result status/isBackground absent = unknown (parses fine).
    let unknown_facts = wire(
        "tool_result",
        json!({"requestId": "r", "toolName": "s", "index": 0, "parts": [],
               "isError": false, "auto": false}),
    );
    match ActivityEvent::from_xacpp_event(&unknown_facts).unwrap() {
        ActivityEvent::ToolResult(p) => {
            assert_eq!(p.status, None, "absent status = unknown");
            assert_eq!(p.is_background, None, "absent isBackground = unknown");
        }
        other => panic!("expected ToolResult, got {other:?}"),
    }
}

// ---------------------------------------------------------------- helpers

fn start_ev(text: &str) -> XacppEvent {
    wire("start", json!({"inputs": [{"type": "text", "text": text}]}))
}

fn content_start_ev(pair: &str, part_id: &str) -> XacppEvent {
    wire(
        "content_start",
        json!({"partId": part_id, "round": "r1", "pair": pair}),
    )
}

fn content_part_ev(pair: &str, part_id: &str, text: &str) -> XacppEvent {
    wire(
        "content_part",
        json!({"round": "r1", "pair": pair,
               "payload": {"type": "text", "text": text, "partId": part_id}}),
    )
}

fn pair_complete_ev(pair: &str) -> XacppEvent {
    wire(
        "pair_complete",
        json!({"pair": pair, "contextWindow": 0,
               "tokenUsage": {"id": "", "message": "", "promptTokens": 0,
                              "completionTokens": 0, "totalTokens": 0}}),
    )
}
