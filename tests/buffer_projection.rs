//! Buffer & projection model scenarios, driven by the shared mock feed
//! (`mod common`): projection protocol (version/notification semantics),
//! routing & admission, and page re-entry consistency.

mod common;

use common::{
    MockFeed, NotificationLog, complete_ev, content_delta_ev, content_end_ev, content_start_ev,
    pair_complete_ev, start_ev, text_of, think_delta_ev, think_end_ev, think_of, think_start_ev,
    tool_result_ev, tool_use_ev,
};
use xacpp::wire::events::ActionResponse;
use xacpp_activity::{
    ActivityEventCategory, ActivityHub, ConsumeOutcome, HubSnapshot, InteractionAnswer,
    InteractionKind, InteractionPayload, InteractionState, SnapshotItem, SyncPlan,
};

/// Unwraps a fresh allocation. Every `consume_command` payload in this
/// file carries no requestId, so the outcome is always `New`.
fn expect_new(outcome: ConsumeOutcome) -> u64 {
    match outcome {
        ConsumeOutcome::New(id) => id,
        other => panic!("expected fresh allocation, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// a) Projection protocol: version numbers track observable mutations.
// ---------------------------------------------------------------------------

/// Versions increment per observable mutation; deduped/dropped repeats and
/// finalized-pair late increments leave the version — and the notification
/// log — untouched (no mutation → no read).
#[tokio::test]
async fn version_increases_on_mutation_only() {
    let hub = ActivityHub::new();
    assert!(hub.register("act-a").await);
    let log = NotificationLog::attach(&hub).await;
    assert_eq!(hub.version("act-a").await, Some(0));

    let mut feed = MockFeed::new()
        .live("act-a", start_ev("hello"))
        // Two distinct think deltas: two mutations.
        .live("act-a", think_start_ev("p1", "t0"))
        .live("act-a", think_delta_ev("p1", "t0", "aa"))
        .live("act-a", think_delta_ev("p1", "t0", "bb"))
        // Adjacent duplicate delta: dedup drop, no mutation.
        .live("act-a", think_delta_ev("p1", "t0", "bb"))
        .live("act-a", think_end_ev("p1", "t0"))
        // Distinct content deltas mutate; the identical repeat is deduped.
        .live("act-a", content_start_ev("p1", "c0"))
        .live("act-a", content_delta_ev("p1", "c0", "body"))
        .live("act-a", content_delta_ev("p1", "c0", "body"))
        // The pair closes; every live increment for p1 after this point
        // (finalized-pair guard) is a drop.
        .live("act-a", pair_complete_ev("p1"))
        .live("act-a", think_delta_ev("p1", "t0", "late"));
    assert_eq!(feed.drive_all(&hub).await, 11);

    // Observable mutations: start, think_start, "aa", "bb", think_end,
    // content_start, "body", pair_complete → eight. The duplicate think
    // delta, the duplicate content delta, and the post-finalization late
    // delta produced nothing.
    assert_eq!(log.versions_of("act-a"), vec![1, 2, 3, 4, 5, 6, 7, 8]);
    assert_eq!(hub.version("act-a").await, Some(8));

    let HubSnapshot { version, items, .. } = hub.snapshot("act-a").await.expect("registered");
    assert_eq!(version, 8);
    assert_eq!(think_of(&items), "aabb");
    assert_eq!(text_of(&items), "body");
}

/// Snapshot and version are read atomically: the version returned with a
/// snapshot is exactly the version whose mutations the snapshot contains.
#[tokio::test]
async fn snapshot_and_version_are_atomically_consistent() {
    let hub = ActivityHub::new();
    hub.register("act-a").await;
    let log = NotificationLog::attach(&hub).await;

    let mut feed = MockFeed::new()
        .live("act-a", start_ev("q"))
        .live("act-a", think_start_ev("p1", "t0"))
        .live("act-a", think_delta_ev("p1", "t0", "part one"));
    feed.drive_all(&hub).await;

    let HubSnapshot {
        version: v1,
        items: items1,
        ..
    } = hub.snapshot("act-a").await.expect("registered");
    assert_eq!(v1, log.versions_of("act-a").last().copied().unwrap());
    assert_eq!(think_of(&items1), "part one");

    // One more mutation: the new snapshot pairs the new version with the
    // new content — never a stale pairing in either direction.
    let mut feed = MockFeed::new().live("act-a", think_delta_ev("p1", "t0", " plus two"));
    feed.drive_all(&hub).await;

    let HubSnapshot {
        version: v2,
        items: items2,
        ..
    } = hub.snapshot("act-a").await.expect("registered");
    assert_eq!(v2, v1 + 1);
    assert_eq!(think_of(&items2), "part one plus two");
    assert_eq!(v2, log.versions_of("act-a").last().copied().unwrap());
}

/// Interaction mutations (command registration, answering) bump the same
/// version — authorization cards are a UI-visible surface.
#[tokio::test]
async fn interaction_mutations_bump_the_version() {
    let hub = ActivityHub::new();
    hub.register("act-a").await;
    let log = NotificationLog::attach(&hub).await;

    let v0 = hub.version("act-a").await.unwrap();
    let id = hub
        .with_session("act-a", |session| {
            session.consume_command(
                "action_request",
                serde_json::json!({
                    "toolName": "shell",
                    "arguments": "{}",
                    "actionId": "a-1",
                    "description": "run date",
                    "alert": "info",
                    "intent": "clock"
                }),
            )
        })
        .await
        .expect("registered")
        .map(expect_new)
        .expect("valid command");
    assert_eq!(hub.version("act-a").await, Some(v0 + 1));

    // Answering the pending card mutates too.
    hub.with_session("act-a", |session| {
        session
            .respond(id, InteractionAnswer::Action(ActionResponse::Approve))
            .expect("pending")
    })
    .await
    .expect("registered");
    assert_eq!(hub.version("act-a").await, Some(v0 + 2));
    assert_eq!(
        log.versions_of("act-a"),
        vec![v0 + 1, v0 + 2],
        "each interaction mutation fired exactly one notification"
    );
}

/// Interaction cards surface in the hub snapshot: a registered command
/// appears on the `pending_interactions` face (parallel to, not inside,
/// the render-order `items`), answering and degrading remove it, and the
/// snapshot carries the same version the notifications report.
#[tokio::test]
async fn interactions_surface_in_the_snapshot() {
    let hub = ActivityHub::new();
    hub.register("act-a").await;
    let log = NotificationLog::attach(&hub).await;

    // Give the model face real content so the parallel-face assertion is
    // substantive.
    let mut feed = MockFeed::new().live("act-a", start_ev("第一问"));
    assert_eq!(feed.drive_all(&hub).await, 1);
    let before = hub.snapshot("act-a").await.expect("registered");
    assert!(
        matches!(
            &before.items[..],
            [SnapshotItem::User { inputs, .. }] if xacpp_activity::inputs_text(inputs) == "第一问"
        ),
        "the start event is the sole model entry"
    );
    assert!(before.pending_interactions.is_empty());
    let version_after_start = before.version;

    let command = |action_id: &str| {
        serde_json::json!({
            "toolName": "shell",
            "arguments": "{}",
            "actionId": action_id,
            "description": "run date",
            "alert": "info",
            "intent": "clock"
        })
    };

    let first = hub
        .with_session("act-a", |session| {
            session.consume_command("action_request", command("a-1"))
        })
        .await
        .expect("registered")
        .map(expect_new)
        .expect("valid command");

    let snapshot = hub.snapshot("act-a").await.expect("registered");
    assert_eq!(
        snapshot.items, before.items,
        "interaction cards are not render-order entries"
    );
    assert_eq!(snapshot.version, version_after_start + 1);
    assert_eq!(
        snapshot.version,
        log.versions_of("act-a").last().copied().unwrap(),
        "the snapshot version is the notified version"
    );
    assert_eq!(snapshot.pending_interactions.len(), 1);
    let card = &snapshot.pending_interactions[0];
    assert_eq!(card.id, first);
    assert_eq!(card.kind, InteractionKind::Action);
    assert_eq!(card.state, InteractionState::Pending);
    assert!(
        matches!(&card.payload, InteractionPayload::Action(p) if p.description == "run date"),
        "the snapshot carries the card content the UI renders"
    );

    // Answering removes the card from the snapshot face.
    hub.with_session("act-a", |session| {
        session
            .respond(first, InteractionAnswer::Action(ActionResponse::Approve))
            .expect("pending")
    })
    .await
    .expect("registered");
    let snapshot = hub.snapshot("act-a").await.expect("registered");
    assert!(snapshot.pending_interactions.is_empty());
    assert_eq!(snapshot.version, version_after_start + 2);

    // An explicit degrade removes it the same way.
    let second = hub
        .with_session("act-a", |session| {
            session.consume_command("action_request", command("a-2"))
        })
        .await
        .expect("registered")
        .map(expect_new)
        .expect("valid command");
    assert_eq!(
        hub.snapshot("act-a")
            .await
            .expect("registered")
            .pending_interactions
            .len(),
        1
    );
    hub.with_session("act-a", |session| {
        session.degrade(second, "transport gone").expect("pending")
    })
    .await
    .expect("registered");
    let snapshot = hub.snapshot("act-a").await.expect("registered");
    assert!(snapshot.pending_interactions.is_empty());
    assert_eq!(snapshot.version, version_after_start + 4);
}

/// The hub-level `respond` entry answers a pending card and returns the
/// wire response for the transport; the card leaves the snapshot face and
/// the mutation rides the version/notification path.
#[tokio::test]
async fn hub_respond_answers_and_bumps_the_version() {
    let hub = ActivityHub::new();
    hub.register("act-a").await;
    let log = NotificationLog::attach(&hub).await;

    let v0 = hub.version("act-a").await.unwrap();
    let id = hub
        .with_session("act-a", |session| {
            session.consume_command(
                "action_request",
                serde_json::json!({
                    "toolName": "shell",
                    "arguments": "{}",
                    "actionId": "a-1",
                    "description": "run date",
                    "alert": "info",
                    "intent": "clock"
                }),
            )
        })
        .await
        .expect("registered")
        .map(expect_new)
        .expect("valid command");
    assert_eq!(hub.version("act-a").await, Some(v0 + 1));
    assert_eq!(
        hub.snapshot("act-a")
            .await
            .unwrap()
            .pending_interactions
            .len(),
        1
    );

    // Answer through the hub entry: the reply is the wire shape the
    // binding layer sends over the transport.
    let response = hub
        .respond(
            "act-a",
            id,
            InteractionAnswer::Action(ActionResponse::Approve),
        )
        .await
        .expect("state machine accepts the pending id")
        .expect("registered activity");
    assert_eq!(
        serde_json::to_value(&response).unwrap(),
        serde_json::json!({"kind": "generic", "name": "action", "data": {"type": "approve"}})
    );
    let snapshot = hub.snapshot("act-a").await.expect("registered");
    assert!(snapshot.pending_interactions.is_empty());
    assert_eq!(snapshot.version, v0 + 2);
    assert_eq!(log.versions_of("act-a"), vec![v0 + 1, v0 + 2]);

    // An unregistered activity reports `Ok(None)` — distinct from the
    // state machine rejections.
    let none = hub
        .respond(
            "no-such-act",
            id,
            InteractionAnswer::Action(ActionResponse::Approve),
        )
        .await
        .expect("no state machine error");
    assert!(none.is_none(), "unregistered activity yields None");
}

/// The hub entry surfaces the state machine rejections unchanged: unknown
/// ids error, a second answer on an already-answered id errors, and the
/// failed attempts leave the pending face untouched.
#[tokio::test]
async fn hub_respond_rejects_unknown_and_duplicate_answers() {
    let hub = ActivityHub::new();
    hub.register("act-a").await;
    let id = hub
        .with_session("act-a", |session| {
            session.consume_command(
                "action_request",
                serde_json::json!({
                    "toolName": "shell",
                    "arguments": "{}",
                    "actionId": "a-1",
                    "description": "run date",
                    "alert": "info",
                    "intent": "clock"
                }),
            )
        })
        .await
        .expect("registered")
        .map(expect_new)
        .expect("valid command");

    // Unknown id.
    let err = hub
        .respond(
            "act-a",
            999,
            InteractionAnswer::Action(ActionResponse::Approve),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        xacpp_activity::InteractionError::UnknownId(999)
    ));
    assert_eq!(
        hub.snapshot("act-a")
            .await
            .unwrap()
            .pending_interactions
            .len(),
        1
    );

    // First answer succeeds; the duplicate is rejected.
    hub.respond(
        "act-a",
        id,
        InteractionAnswer::Action(ActionResponse::Approve),
    )
    .await
    .expect("pending")
    .expect("registered");
    let err = hub
        .respond(
            "act-a",
            id,
            InteractionAnswer::Action(ActionResponse::Approve),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        xacpp_activity::InteractionError::AlreadyAnswered { id: answered } if answered == id
    ));
    let snapshot = hub.snapshot("act-a").await.expect("registered");
    assert!(snapshot.pending_interactions.is_empty());
}

// ---------------------------------------------------------------------------
// b) Routing and admission.
// ---------------------------------------------------------------------------

/// Events route to the owning session by activity id; mixed interleaved
/// feeds keep the sessions independent, and notifications carry the routed
/// activity id.
#[tokio::test]
async fn routes_by_activity_and_admits_registered_only() {
    let hub = ActivityHub::new();
    hub.register("act-a").await;
    hub.register("act-b").await;
    let log = NotificationLog::attach(&hub).await;

    let mut feed = MockFeed::new()
        .live("act-a", start_ev("for a"))
        .live("act-b", start_ev("for b"))
        .live("act-a", think_start_ev("pa", "t0"))
        .live("act-a", think_delta_ev("pa", "t0", "alpha"))
        .live_on(
            "act-b",
            ActivityEventCategory::Background,
            think_start_ev("pb", "t0"),
        )
        .live("act-b", think_delta_ev("pb", "t0", "beta"));
    feed.drive_all(&hub).await;

    let HubSnapshot { items: items_a, .. } = hub.snapshot("act-a").await.expect("registered");
    let HubSnapshot { items: items_b, .. } = hub.snapshot("act-b").await.expect("registered");
    assert_eq!(think_of(&items_a), "alpha");
    assert_eq!(think_of(&items_b), "beta");
    let users_a: Vec<String> = items_a
        .iter()
        .filter_map(|item| match item {
            SnapshotItem::User { inputs, .. } => Some(xacpp_activity::inputs_text(inputs)),
            _ => None,
        })
        .collect();
    assert_eq!(users_a, vec!["for a".to_string()]);
    assert_eq!(
        log.entries(),
        vec![
            ("act-a".to_string(), 1),
            ("act-b".to_string(), 1),
            ("act-a".to_string(), 2),
            ("act-a".to_string(), 3),
            ("act-b".to_string(), 2),
            ("act-b".to_string(), 3),
        ]
    );
}

/// Events for unregistered activities are dropped at the router (admission
/// = registered): not consumed, not notified, not snapshotted. Registering
/// later admits the same id (lazy creation).
#[tokio::test]
async fn drops_unregistered_and_lazy_creates_on_register() {
    let hub = ActivityHub::new();
    let log = NotificationLog::attach(&hub).await;

    let mut feed = MockFeed::new().live("act-c", start_ev("orphan"));
    assert!(
        !feed.drive_step(&hub).await,
        "unregistered activity: dropped"
    );
    assert_eq!(hub.snapshot("act-c").await, None);
    assert_eq!(hub.version("act-c").await, None);
    assert!(log.entries().is_empty(), "no notification for a drop");

    // Registration is the lazy creation point; the idempotent re-register
    // keeps the (now live) session untouched.
    assert!(hub.register("act-c").await);
    assert!(!hub.register("act-c").await);

    let mut feed = MockFeed::new()
        .live("act-c", start_ev("for c"))
        .live("act-c", think_start_ev("pc", "t0"))
        .live("act-c", think_delta_ev("pc", "t0", "gamma"));
    feed.drive_all(&hub).await;

    let HubSnapshot { version, items, .. } = hub.snapshot("act-c").await.expect("registered");
    assert_eq!(version, 3);
    assert_eq!(think_of(&items), "gamma");
    assert_eq!(log.versions_of("act-c"), vec![1, 2, 3]);
}

/// Unregister drops the whole session (screen-off); a re-register starts
/// from zero.
#[tokio::test]
async fn unregister_removes_the_session() {
    let hub = ActivityHub::new();
    hub.register("act-a").await;
    let mut feed = MockFeed::new().live("act-a", start_ev("gone soon"));
    feed.drive_all(&hub).await;
    assert_eq!(hub.version("act-a").await, Some(1));

    assert!(hub.unregister("act-a").await);
    assert!(
        !hub.unregister("act-a").await,
        "second unregister: nothing left"
    );
    assert_eq!(hub.snapshot("act-a").await, None);
    assert!(
        hub.register("act-a").await,
        "re-register creates a fresh session"
    );
    assert_eq!(hub.version("act-a").await, Some(0));
}

// ---------------------------------------------------------------------------
// c) Page re-entry consistency.
// ---------------------------------------------------------------------------

/// Page re-entry with the connection alive: replay the snapshot (zero
/// round-trip), keep feeding, re-read — content is continuous, nothing
/// lost, nothing duplicated.
#[tokio::test]
async fn page_reentry_snapshot_continuity() {
    let hub = ActivityHub::new();
    hub.register("act-a").await;
    let log = NotificationLog::attach(&hub).await;

    // Page open: pair 1 streams to completion.
    let mut feed = MockFeed::new()
        .live("act-a", start_ev("what time"))
        .live("act-a", think_start_ev("p1", "t0"))
        .live("act-a", think_delta_ev("p1", "t0", "user wants "))
        .live("act-a", think_delta_ev("p1", "t0", "the time"))
        .live("act-a", think_end_ev("p1", "t0"))
        .live("act-a", content_start_ev("p1", "c0"))
        .live("act-a", content_delta_ev("p1", "c0", "it is 10 am"))
        .live("act-a", content_end_ev("p1", "c0"))
        .live("act-a", pair_complete_ev("p1"));
    feed.drive_all(&hub).await;

    let HubSnapshot {
        items: replayed, ..
    } = hub.snapshot("act-a").await.expect("registered");

    // Page away; pair 2 starts streaming while the page is closed (the
    // library keeps ingesting — the session lives on across pages).
    let mut feed = MockFeed::new()
        .live("act-a", think_start_ev("p2", "t1"))
        .live("act-a", think_delta_ev("p2", "t1", "checking "));
    feed.drive_all(&hub).await;

    // Page back: the replayed snapshot is a plain read (zero round-trip),
    // and it already contains the closed-page increments.
    let HubSnapshot { version, items, .. } = hub.snapshot("act-a").await.expect("registered");
    assert_eq!(version, log.versions_of("act-a").last().copied().unwrap());
    assert_eq!(think_of(&items), "user wants the timechecking ");
    assert_eq!(text_of(&items), "it is 10 am");
    assert_eq!(
        items.len(),
        replayed.len() + 1,
        "exactly the pair-2 think entry appeared"
    );

    // Keep feeding: pair 2 completes on the reopened page.
    let mut feed = MockFeed::new()
        .live("act-a", think_delta_ev("p2", "t1", "the clock"))
        .live("act-a", think_end_ev("p2", "t1"))
        .live("act-a", content_start_ev("p2", "c1"))
        .live("act-a", content_delta_ev("p2", "c1", "10:00"))
        .live("act-a", content_end_ev("p2", "c1"))
        .live("act-a", pair_complete_ev("p2"))
        .live("act-a", complete_ev());
    feed.drive_all(&hub).await;

    let HubSnapshot {
        items: final_items, ..
    } = hub.snapshot("act-a").await.expect("registered");
    // Nothing lost, nothing duplicated, order preserved.
    assert_eq!(
        think_of(&final_items),
        "user wants the timechecking the clock"
    );
    assert_eq!(text_of(&final_items), "it is 10 am10:00");
    // A consumer reporting the full watermark needs no recovery; one
    // reporting nothing is past the buffer's coverage (both pairs
    // finalized → buffer cleared) and goes to history.
    let plans = hub
        .with_session("act-a", |session| {
            (session.need_sync(u64::MAX), session.need_sync(0))
        })
        .await;
    let (caught_up, from_scratch) = plans.expect("registered");
    assert!(matches!(caught_up, SyncPlan::UpToDate));
    assert!(matches!(from_scratch, SyncPlan::NeedHistory));
}

/// A consumer that fell behind rebuilds through the buffer-replay channel:
/// a fresh session (activity re-opened after eviction) replaying the
/// buffered envelopes reproduces the same projection the live path built.
#[tokio::test]
async fn buffer_replay_rebuilds_an_equivalent_projection() {
    let hub = ActivityHub::new();
    hub.register("act-a").await;
    let log = NotificationLog::attach(&hub).await;

    let mut feed = MockFeed::new()
        .live("act-a", start_ev("q"))
        .live("act-a", think_start_ev("p1", "t0"))
        .live("act-a", think_delta_ev("p1", "t0", "thinking"))
        .live("act-a", content_start_ev("p1", "c0"))
        .live("act-a", content_delta_ev("p1", "c0", "answer"));
    feed.drive_all(&hub).await;

    let HubSnapshot {
        items: live_built, ..
    } = hub.snapshot("act-a").await.expect("registered");

    // The consumer reports seq 0: the gap is covered by the current-pair
    // buffer (pair_complete never ran, so every event is still buffered).
    let plan = hub
        .with_session("act-a", |session| session.need_sync(0))
        .await
        .expect("registered");
    let SyncPlan::BufferReplay {
        envelopes,
        new_last_seq,
    } = plan
    else {
        panic!("expected BufferReplay, got {plan:?}");
    };
    assert_eq!(
        envelopes.len(),
        5,
        "every event of the open pair is buffered"
    );

    // Rebuild on a fresh session (as if the original session was evicted):
    // the replay batch alone reproduces the live-built projection.
    assert!(hub.unregister("act-a").await);
    assert!(hub.register("act-a").await);

    let mut feed = MockFeed::new().replay("act-a", envelopes);
    feed.drive_all(&hub).await;

    let HubSnapshot { items: rebuilt, .. } = hub.snapshot("act-a").await.expect("registered");
    assert_eq!(live_built, rebuilt, "replay rebuilds the same projection");
    assert_eq!(
        log.versions_of("act-a").last().copied(),
        Some(1),
        "one replay batch = one committed mutation"
    );
    let _ = new_last_seq; // watermark follows the runtime's last seq (5)
}

/// History realign (authoritative replacement) stays idempotent across a
/// live tail: re-pulling the same projection after new in-flight live
/// content keeps the projection turn identical and the live tail intact —
/// continuous, nothing lost, nothing duplicated.
#[tokio::test]
async fn history_realign_is_idempotent_with_live_tail() {
    let hub = ActivityHub::new();
    hub.register("act-a").await;
    let _log = NotificationLog::attach(&hub).await;

    // Live stream builds turn p1 and finalizes it.
    let mut feed = MockFeed::new()
        .live("act-a", start_ev("what time"))
        .live("act-a", think_start_ev("p1", "live-t0"))
        .live(
            "act-a",
            think_delta_ev("p1", "live-t0", "user wants the time"),
        )
        .live("act-a", think_end_ev("p1", "live-t0"))
        .live("act-a", content_start_ev("p1", "live-c0"))
        .live("act-a", content_delta_ev("p1", "live-c0", "10 am"))
        .live("act-a", content_end_ev("p1", "live-c0"))
        .live("act-a", pair_complete_ev("p1"));
    feed.drive_all(&hub).await;

    // Disconnect + page re-entry: the authoritative projection pull
    // (LatestWindow) replaces the display with the projection's view.
    let projection = vec![
        (
            "start".to_string(),
            serde_json::json!({"inputs": [{"type": "text", "text": "what time"}]}),
        ),
        (
            "think_start".to_string(),
            serde_json::json!({"partId": "replay:t0", "round": "r1", "pair": "p1"}),
        ),
        (
            "think".to_string(),
            serde_json::json!({"content": "user wants the time", "round": "r1", "pair": "p1", "partId": "replay:t0"}),
        ),
        (
            "think_end".to_string(),
            serde_json::json!({"partId": "replay:t0", "round": "r1", "pair": "p1"}),
        ),
        (
            "content_start".to_string(),
            serde_json::json!({"partId": "replay:c0", "round": "r1", "pair": "p1"}),
        ),
        (
            "content_delta".to_string(),
            serde_json::json!({"round": "r1", "pair": "p1", "payload": {"type": "text", "text": "10 am", "partId": "replay:c0"}}),
        ),
        (
            "content_end".to_string(),
            serde_json::json!({"partId": "replay:c0", "round": "r1", "pair": "p1"}),
        ),
    ];
    let mut feed = MockFeed::new().history("act-a", projection.clone());
    feed.drive_all(&hub).await;
    let HubSnapshot {
        items: after_first_pull,
        ..
    } = hub.snapshot("act-a").await.expect("registered");
    assert_eq!(text_of(&after_first_pull), "10 am");
    assert_eq!(think_of(&after_first_pull), "user wants the time");

    // Live continues: an in-flight pair-2 turn streams past the projection.
    let mut feed = MockFeed::new()
        .live("act-a", think_start_ev("p2", "live-t1"))
        .live("act-a", think_delta_ev("p2", "live-t1", " tail "));
    feed.drive_all(&hub).await;
    let HubSnapshot {
        items: with_live_tail,
        ..
    } = hub.snapshot("act-a").await.expect("registered");
    assert_eq!(
        think_of(&with_live_tail),
        "user wants the time tail ",
        "in-flight live turn survives at the projection's tail"
    );

    // Re-pull the same projection: the projection turn is rebuilt
    // identically (idempotent replacement) and the live tail is kept.
    let mut feed = MockFeed::new().history("act-a", projection);
    feed.drive_all(&hub).await;
    let HubSnapshot {
        items: after_second_pull,
        ..
    } = hub.snapshot("act-a").await.expect("registered");
    assert_eq!(
        with_live_tail, after_second_pull,
        "realign is idempotent: same projection + same live tail"
    );

    let think_keys = hub
        .with_session("act-a", |session| {
            session
                .snapshot()
                .iter()
                .filter_map(|item| match item {
                    SnapshotItem::Think { key, .. } => Some(key.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>()
        })
        .await;
    let think_keys = think_keys.expect("registered");
    assert_eq!(
        think_keys,
        vec!["think:p1".to_string(), "think:p2".to_string()],
        "projection think and live think both render"
    );
}

/// Out-of-order injection: a think delta ahead of its `think_start` is
/// implicitly anchored (unrouted fallback), and the late boundary event
/// finds the block already built — content stays intact.
#[tokio::test]
async fn out_of_order_delta_is_implicitly_anchored() {
    let hub = ActivityHub::new();
    hub.register("act-a").await;

    let mut feed = MockFeed::new()
        .live("act-a", think_delta_ev("p9", "t9", "early"))
        .live("act-a", think_start_ev("p9", "t9"))
        .live("act-a", think_delta_ev("p9", "t9", " late"));
    feed.drive_all(&hub).await;

    let HubSnapshot { items, .. } = hub.snapshot("act-a").await.expect("registered");
    assert_eq!(think_of(&items), "early late");
}

/// End-to-end through the hub: tools, process messages, and content all
/// land in the routed session's snapshot (the router is the only entry).
#[tokio::test]
async fn routed_session_covers_tools_process_and_content() {
    let hub = ActivityHub::new();
    hub.register("act-a").await;

    let mut feed = MockFeed::new()
        .live("act-a", start_ev("run a tool"))
        .live("act-a", think_start_ev("p1", "t0"))
        .live("act-a", content_start_ev("p1", "c0"))
        .live("act-a", tool_use_ev("req-1"))
        .live("act-a", tool_result_ev("req-1"))
        .live(
            "act-a",
            common::wire("notify", serde_json::json!({"message": "heads up"})),
        )
        .live("act-a", content_delta_ev("p1", "c0", "done"))
        .live("act-a", content_end_ev("p1", "c0"))
        .live("act-a", pair_complete_ev("p1"))
        .live("act-a", complete_ev());
    feed.drive_all(&hub).await;

    let HubSnapshot { items, .. } = hub.snapshot("act-a").await.expect("registered");
    let tools: Vec<_> = items
        .iter()
        .filter_map(|item| match item {
            SnapshotItem::Tool {
                request_id, done, ..
            } => Some((request_id.as_str(), *done)),
            _ => None,
        })
        .collect();
    assert_eq!(tools, vec![("req-1", Some(true))]);
    assert!(items.iter().any(|item| matches!(
        item,
        SnapshotItem::Process { content, .. } if content == "heads up"
    )));
    assert_eq!(text_of(&items), "done");
}
