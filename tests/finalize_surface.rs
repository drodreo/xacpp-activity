//! Finalization correctness and the turn-level activity surface.
//!
//! Scenarios from the D1 simulator verification (hanging streaming parts
//! after turn finalization, silent finalized-pair drops, missing
//! turn-level active surface) plus the version-notification chain check:
//! a finalizing `pair_complete` must ride the hub commit (version bump +
//! change notification) even when it only settles dangling parts.

mod common;

use std::sync::{Arc, Mutex, OnceLock};

use common::{
    MockFeed, NotificationLog, complete_ev, pair_complete_ev, start_ev, think_end_ev, wire,
};
use xacpp_activity::event::CompletePayload;
use xacpp_activity::{
    ActivityEvent, ActivityEventCategory, ActivityEventEnvelope, ActivityHub, HubSnapshot,
    SnapshotItem,
};

// ---------------------------------------------------------------------------
// Log capture (warn-visibility assertion for the finalized-pair guard).
// ---------------------------------------------------------------------------

static LOG_RECORDS: OnceLock<Arc<Mutex<Vec<String>>>> = OnceLock::new();

struct CaptureLogger;

/// Stateless capturing logger (records sink into [`LOG_RECORDS`]; installed
/// via `log::set_logger`, which takes a `&'static` logger).
static CAPTURE_LOGGER: CaptureLogger = CaptureLogger;

impl log::Log for CaptureLogger {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        metadata.level() <= log::Level::Warn
    }

    fn log(&self, record: &log::Record) {
        if self.enabled(record.metadata()) {
            LOG_RECORDS
                .get()
                .expect("log records initialized before logger use")
                .lock()
                .expect("log records lock")
                .push(format!("{}", record.args()));
        }
    }

    fn flush(&self) {}
}

/// Installs the capturing warn-level logger (once per test process) and
/// returns the record sink.
fn captured_warnings() -> Arc<Mutex<Vec<String>>> {
    let records = Arc::new(Mutex::new(Vec::new()));
    if LOG_RECORDS.set(records.clone()).is_ok() {
        log::set_logger(&CAPTURE_LOGGER).expect("logger installed once");
        log::set_max_level(log::LevelFilter::Warn);
    }
    LOG_RECORDS.get().expect("log records").clone()
}

// ---------------------------------------------------------------------------
// Local snapshot helpers.
// ---------------------------------------------------------------------------

/// The `streaming` flag of the single Think item, if any.
fn think_streaming(items: &[SnapshotItem]) -> Option<bool> {
    items.iter().find_map(|item| match item {
        SnapshotItem::Think { streaming, .. } => Some(*streaming),
        _ => None,
    })
}

/// The `streaming` flag of every Text item, in render order.
fn text_streaming_flags(items: &[SnapshotItem]) -> Vec<bool> {
    items
        .iter()
        .filter_map(|item| match item {
            SnapshotItem::Text { streaming, .. } => Some(*streaming),
            _ => None,
        })
        .collect()
}

/// A think part (full snapshot form, no `think_start` — the reducer opens
/// the slot implicitly).
fn think_part_ev(pair: &str, part_id: &str, content: &str) -> xacpp::wire::events::XacppEvent {
    wire(
        "think_part",
        serde_json::json!({"content": content, "round": "r1", "pair": pair, "partId": part_id}),
    )
}

// ---------------------------------------------------------------------------
// Scenarios.
// ---------------------------------------------------------------------------

/// A think part that never received its `think_end` must still settle when
/// the turn finalizes: the snapshot reflects terminal content (streaming
/// flag cleared), the turn-level surface goes inactive, and the
/// finalization rides the hub commit — version bump + change notification.
#[tokio::test]
async fn hanging_parts_settle_when_the_turn_finalizes() {
    let hub = ActivityHub::new();
    hub.register("act-a").await;
    let log = NotificationLog::attach(&hub).await;

    MockFeed::new()
        .live("act-a", start_ev("hi"))
        .live("act-a", think_part_ev("p1", "t0", "truncated thinking"))
        // No think_end: the part hangs in streaming state…
        .live("act-a", pair_complete_ev("p1"))
        .drive_all(&hub)
        .await;

    let HubSnapshot {
        version,
        items,
        turn_active,
        ..
    } = hub.snapshot("act-a").await.expect("registered");
    assert_eq!(version, 3, "start + think_part + pair_complete all commit");
    assert_eq!(
        think_streaming(&items),
        Some(false),
        "finalized turn must not carry a streaming part"
    );
    assert!(!turn_active, "finalized turn is not active");
    assert_eq!(
        log.versions_of("act-a"),
        vec![1, 2, 3],
        "finalization fires the change notification"
    );
}

/// A late live event for a finalized pair is dropped with a warning (not
/// silently): the snapshot state is unchanged and no version bumps.
#[tokio::test]
async fn late_events_for_a_finalized_pair_warn_and_drop() {
    let warnings = captured_warnings();
    let hub = ActivityHub::new();
    hub.register("act-a").await;
    let _log = NotificationLog::attach(&hub).await;

    MockFeed::new()
        .live("act-a", start_ev("hi"))
        .live("act-a", think_part_ev("p1", "t0", "thought"))
        .live("act-a", pair_complete_ev("p1"))
        .drive_all(&hub)
        .await;
    let before = hub.snapshot("act-a").await.expect("registered");

    // Late increment for the finalized pair: dropped + warned.
    MockFeed::new()
        .live("act-a", think_end_ev("p1", "t0"))
        .drive_all(&hub)
        .await;

    let after = hub.snapshot("act-a").await.expect("registered");
    assert_eq!(after.version, before.version, "drop commits nothing");
    assert_eq!(after.items, before.items, "snapshot state unchanged");
    assert!(
        warnings
            .lock()
            .expect("log records lock")
            .iter()
            .any(|line| line.contains("think_end") && line.contains("p1")),
        "the drop is warned with event name and pair: {:?}",
        warnings.lock().expect("log records lock")
    );
}

/// The turn-level active surface tracks the in-flight turn: true while
/// streaming, false once the turn finalizes.
#[tokio::test]
async fn turn_active_tracks_the_streaming_turn() {
    let hub = ActivityHub::new();
    hub.register("act-a").await;

    let mut feed = MockFeed::new()
        .live("act-a", start_ev("hi"))
        .live("act-a", think_part_ev("p1", "t0", "thinking"));
    feed.drive_step(&hub).await;
    feed.drive_step(&hub).await;

    assert!(
        hub.snapshot("act-a").await.expect("registered").turn_active,
        "an in-flight streaming turn is active"
    );

    MockFeed::new()
        .live("act-a", pair_complete_ev("p1"))
        .drive_all(&hub)
        .await;

    assert!(
        !hub.snapshot("act-a").await.expect("registered").turn_active,
        "finalization ends the activity"
    );
}

/// Mixed turns: finalized history plus one in-flight turn stays active;
/// content parts finalized by earlier turns never leak activity.
#[tokio::test]
async fn turn_active_with_mixed_finalized_and_streaming_turns() {
    let hub = ActivityHub::new();
    hub.register("act-a").await;

    MockFeed::new()
        .live("act-a", start_ev("hi"))
        .live("act-a", think_part_ev("p1", "t0", "first"))
        .live("act-a", pair_complete_ev("p1"))
        .drive_all(&hub)
        .await;

    MockFeed::new()
        .live("act-a", think_part_ev("p2", "t0", "second"))
        .drive_all(&hub)
        .await;

    let snapshot = hub.snapshot("act-a").await.expect("registered");
    assert!(snapshot.turn_active, "streaming p2 keeps the activity on");
    assert_eq!(
        text_streaming_flags(&snapshot.items),
        Vec::<bool>::new(),
        "no content parts exist yet; activity comes from the turn, not entries"
    );

    MockFeed::new()
        .live("act-a", pair_complete_ev("p2"))
        .live("act-a", complete_ev())
        .drive_all(&hub)
        .await;

    assert!(
        !hub.snapshot("act-a").await.expect("registered").turn_active,
        "both turns finalized — activity off"
    );
}

// ---------------------------------------------------------------------------
// Turn-closing marker (`turn_completed`).
// ---------------------------------------------------------------------------

/// The marker advances exactly once per live turn-level `complete` (the
/// cancel path's only terminal signal included) and the advance rides the
/// hub commit (version bump + change notification). `pair_complete` closes
/// one ReAct loop, not the turn, and never advances it.
#[tokio::test]
async fn live_complete_advances_turn_completed_pair_complete_does_not() {
    let hub = ActivityHub::new();
    hub.register("act-a").await;
    let log = NotificationLog::attach(&hub).await;

    assert_eq!(
        hub.snapshot("act-a")
            .await
            .expect("registered")
            .turn_completed,
        0,
        "fresh session: no closed turns"
    );

    // Loop close only: `pair_complete` must not advance the marker even
    // though it finalizes the turn (turn_active goes off).
    MockFeed::new()
        .live("act-a", start_ev("hi"))
        .live("act-a", think_part_ev("p1", "t0", "thinking"))
        .live("act-a", pair_complete_ev("p1"))
        .drive_all(&hub)
        .await;
    let snapshot = hub.snapshot("act-a").await.expect("registered");
    assert_eq!(
        snapshot.turn_completed, 0,
        "pair_complete closes a loop, not the turn"
    );
    assert!(!snapshot.turn_active, "loop close finalizes the turn");

    // Turn-level close: live `complete` advances the marker once and the
    // advance is a UI-visible commit.
    let versions_before = log.versions_of("act-a").len();
    MockFeed::new()
        .live("act-a", complete_ev())
        .drive_all(&hub)
        .await;
    let snapshot = hub.snapshot("act-a").await.expect("registered");
    assert_eq!(
        snapshot.turn_completed, 1,
        "live complete advances the marker exactly once"
    );
    assert_eq!(
        log.versions_of("act-a").len(),
        versions_before + 1,
        "marker advance rides the commit (version notification fired)"
    );
}

/// Consecutive live turns: the marker counts monotonic closures (0→1→2).
#[tokio::test]
async fn turn_completed_is_monotonic_across_turns() {
    let hub = ActivityHub::new();
    hub.register("act-a").await;

    MockFeed::new()
        .live("act-a", start_ev("first"))
        .live("act-a", think_part_ev("p1", "t0", "one"))
        .live("act-a", pair_complete_ev("p1"))
        .live("act-a", complete_ev())
        .drive_all(&hub)
        .await;
    assert_eq!(
        hub.snapshot("act-a")
            .await
            .expect("registered")
            .turn_completed,
        1
    );

    MockFeed::new()
        .live("act-a", start_ev("second"))
        .live("act-a", think_part_ev("p2", "t0", "two"))
        .live("act-a", pair_complete_ev("p2"))
        .live("act-a", complete_ev())
        .drive_all(&hub)
        .await;
    assert_eq!(
        hub.snapshot("act-a")
            .await
            .expect("registered")
            .turn_completed,
        2,
        "marker counts every live turn closure"
    );
}

/// History channels carry past turns: a `complete` inside a rebuilt
/// projection is a record of an old closure, not a live signal — neither
/// the latest-window rebuild (`apply_history`) nor the full-archive
/// rebuild (`apply_history_rebuild`) may advance the marker, or a realign
/// would fake "the turn I am waiting for has closed".
#[tokio::test]
async fn history_batches_never_advance_turn_completed() {
    let hub = ActivityHub::new();
    hub.register("act-a").await;

    let batch = vec![
        (
            "start".to_string(),
            serde_json::json!({"inputs": [{"type": "text", "text": "hi"}]}),
        ),
        (
            "think_part".to_string(),
            serde_json::json!({"content": "old", "round": "r1", "pair": "p1", "partId": "t0"}),
        ),
        (
            "complete".to_string(),
            serde_json::json!({"round": "r1", "assistantReply": [], "canceled": false}),
        ),
    ];
    for channel in ["latest-window", "full-archive"] {
        let mut feed = MockFeed::new().history("act-a", batch.clone());
        if channel == "full-archive" {
            hub.with_session("act-a", |session| {
                session.apply_history_rebuild(
                    batch
                        .iter()
                        .map(|(name, data)| common::wire(name, data.clone()))
                        .collect(),
                    None,
                )
            })
            .await;
        }
        feed.drive_all(&hub).await;
        assert_eq!(
            hub.snapshot("act-a")
                .await
                .expect("registered")
                .turn_completed,
            0,
            "{channel} batch must not fake a live close"
        );
    }
}

/// Buffer replay defensively must not advance the marker either. The live
/// stream never buffers `complete` (it gets a seq but skips the buffer),
/// so a replay-carried one is a structural impossibility — this guards
/// the invariant should the buffer policy ever change.
#[tokio::test]
async fn buffer_replay_never_advances_turn_completed() {
    let hub = ActivityHub::new();
    hub.register("act-a").await;

    let envelopes = vec![ActivityEventEnvelope {
        activity_id: "act-a".into(),
        seq: 1,
        pair_epoch: 0,
        category: ActivityEventCategory::Default,
        payload: ActivityEvent::Complete(CompletePayload {
            round: "r1".into(),
            assistant_reply: vec![],
            abort_reason: None,
            canceled: false,
        }),
    }];
    hub.with_session("act-a", |session| session.apply_replay(envelopes))
        .await;

    assert_eq!(
        hub.snapshot("act-a")
            .await
            .expect("registered")
            .turn_completed,
        0,
        "replay must not fake a live close"
    );
}
