//! Buffer steady state (30-part whole-turn trim) and draft scenarios,
//! driven by the shared mock feed (`mod common`).

mod common;

use common::{
    MockFeed, NotificationLog, content_delta_ev, content_end_ev, content_start_ev,
    pair_complete_ev, start_ev, text_of, think_delta_ev, think_end_ev, think_of, think_start_ev,
};
use serde_json::json;
use xacpp::wire::events::XacppEvent;
use xacpp_activity::{
    ActivityHub, DraftStore, HubSnapshot, STEADY_STATE_PART_CAP, SnapshotItem, WindowCursor,
};

const ACT: &str = "act-a";

// ---------------------------------------------------------------------------
// Shared helpers.
// ---------------------------------------------------------------------------

/// Streams one full live turn: start + think + the given content blocks +
/// pair_complete. A finalized turn holds `1 + contents.len()` terminal
/// parts (one think, one per content block).
async fn feed_turn(hub: &ActivityHub, pair: &str, user: &str, think: &str, contents: &[&str]) {
    let think_part = format!("{pair}-t");
    let mut feed = MockFeed::new()
        .live(ACT, start_ev(user))
        .live(ACT, think_start_ev(pair, &think_part))
        .live(ACT, think_delta_ev(pair, &think_part, think))
        .live(ACT, think_end_ev(pair, &think_part));
    feed.drive_all(hub).await;
    for (idx, text) in contents.iter().enumerate() {
        let part = format!("{pair}-c{idx}");
        let mut feed = MockFeed::new()
            .live(ACT, content_start_ev(pair, &part))
            .live(ACT, content_delta_ev(pair, &part, text))
            .live(ACT, content_end_ev(pair, &part));
        feed.drive_all(hub).await;
    }
    let mut feed = MockFeed::new().live(ACT, pair_complete_ev(pair));
    feed.drive_all(hub).await;
}

/// Streams one live turn up to (but excluding) its finalization: start +
/// think + the given content blocks, left in the Streaming state.
async fn feed_streaming_turn(
    hub: &ActivityHub,
    pair: &str,
    user: &str,
    think: &str,
    contents: &[&str],
) {
    let think_part = format!("{pair}-t");
    let mut feed = MockFeed::new()
        .live(ACT, start_ev(user))
        .live(ACT, think_start_ev(pair, &think_part))
        .live(ACT, think_delta_ev(pair, &think_part, think));
    feed.drive_all(hub).await;
    for (idx, text) in contents.iter().enumerate() {
        let part = format!("{pair}-c{idx}");
        let mut feed = MockFeed::new()
            .live(ACT, content_start_ev(pair, &part))
            .live(ACT, content_delta_ev(pair, &part, text));
        feed.drive_all(hub).await;
    }
}

/// Builds a LatestWindow-style projection batch: a full turn per pair
/// (start + think + one content block + pair_complete) — 2 terminal parts
/// per turn.
fn projection_batch(pairs: impl Iterator<Item = usize>) -> Vec<(String, serde_json::Value)> {
    let mut events = Vec::new();
    for i in pairs {
        let pair = format!("p{i}");
        let think_part = format!("{pair}-t");
        let content_part = format!("{pair}-c0");
        events.push((
            "start".to_string(),
            json!({"inputs": [{"type": "text", "text": format!("q{i}")}]}),
        ));
        events.push((
            "think_start".to_string(),
            json!({"partId": think_part.as_str(), "round": "r1", "pair": pair.as_str()}),
        ));
        events.push((
            "think".to_string(),
            json!({"content": format!("t{i}"), "round": "r1", "pair": pair.as_str(),
                   "partId": think_part.as_str()}),
        ));
        events.push((
            "think_end".to_string(),
            json!({"partId": think_part.as_str(), "round": "r1", "pair": pair.as_str()}),
        ));
        events.push((
            "content_start".to_string(),
            json!({"partId": content_part.as_str(), "round": "r1", "pair": pair.as_str()}),
        ));
        events.push((
            "content_delta".to_string(),
            json!({"round": "r1", "pair": pair.as_str(), "payload": {
                "type": "text", "text": format!("c{i}"), "partId": content_part.as_str()}}),
        ));
        events.push((
            "content_end".to_string(),
            json!({"partId": content_part.as_str(), "round": "r1", "pair": pair.as_str()}),
        ));
        events.push((
            "pair_complete".to_string(),
            json!({"pair": pair.as_str(), "contextWindow": 200000,
                   "tokenUsage": {"id": "u1", "message": "m1", "promptTokens": 120,
                                  "completionTokens": 80, "totalTokens": 200}}),
        ));
    }
    events
}

/// User bubble texts in render order.
async fn users(hub: &ActivityHub) -> Vec<String> {
    hub.snapshot(ACT)
        .await
        .expect("registered")
        .items
        .iter()
        .filter_map(|item| match item {
            SnapshotItem::User { inputs, .. } => Some(xacpp_activity::inputs_text(inputs)),
            _ => None,
        })
        .collect()
}

/// The think texts concatenated in render order, built from a range of
/// turn numbers (each turn i thinks `"t{i}"`).
fn think_text(range: std::ops::RangeInclusive<usize>) -> String {
    range.map(|i| format!("t{i}")).collect()
}

// ---------------------------------------------------------------------------
// Buffer steady state: whole-turn trim at the settlement points.
// ---------------------------------------------------------------------------

/// a) Multi-turn injection past the cap settles to the steady state at
/// finalization, dropping the oldest *whole* turns. The dropped turns'
/// user bubbles ride along in the render prefix — no half turns, no
/// orphan bubbles.
#[tokio::test]
async fn trim_settles_to_the_cap_dropping_oldest_whole_turns() {
    let hub = ActivityHub::new();
    hub.register(ACT).await;
    assert_eq!(STEADY_STATE_PART_CAP, 30, "the scenarios size against 30");

    // 11 turns × 3 terminal parts (think + 2 contents) = 33 > 30: the
    // oldest whole turn (3 parts) drops, leaving exactly 30.
    for i in 1..=11 {
        feed_turn(
            &hub,
            &format!("p{i}"),
            &format!("q{i}"),
            &format!("t{i}"),
            &[&format!("c{i}a"), &format!("c{i}b")],
        )
        .await;
    }

    let HubSnapshot { items, .. } = hub.snapshot(ACT).await.expect("registered");
    assert_eq!(think_of(&items), think_text(2..=11));
    assert_eq!(
        text_of(&items),
        (2..=11).map(|i| format!("c{i}ac{i}b")).collect::<String>()
    );
    assert_eq!(
        users(&hub).await,
        (2..=11).map(|i| format!("q{i}")).collect::<Vec<_>>(),
        "the dropped turn's user bubble rode along"
    );
}

/// b) In-flight parts never trigger a trim: a Streaming turn can hold any
/// number of parts (20 content blocks here) while the finalized count
/// sits at the cap; the settle happens only at its finalization moment.
#[tokio::test]
async fn streaming_parts_never_trigger_trim() {
    let hub = ActivityHub::new();
    hub.register(ACT).await;

    // 15 turns × 2 terminal parts = exactly the cap: nothing trimmed.
    for i in 1..=15 {
        feed_turn(
            &hub,
            &format!("p{i}"),
            &format!("q{i}"),
            &format!("t{i}"),
            &["only"],
        )
        .await;
    }
    let HubSnapshot { items: at_cap, .. } = hub.snapshot(ACT).await.expect("registered");
    assert!(
        think_of(&at_cap).contains("t1"),
        "at the cap nothing is trimmed"
    );
    assert_eq!(text_of(&at_cap), "only".repeat(15));

    // Turn 16 streams 20 content blocks in flight: the finalized count
    // stays at 30, so the flood of in-flight parts trims nothing.
    feed_streaming_turn(&hub, "p16", "q16", "t16", &["streaming"; 20]).await;
    let HubSnapshot {
        items: while_streaming,
        ..
    } = hub.snapshot(ACT).await.expect("registered");
    assert!(
        think_of(&while_streaming).contains("t1"),
        "in-flight parts never trigger a trim"
    );
    assert_eq!(
        users(&hub).await,
        (1..=16).map(|i| format!("q{i}")).collect::<Vec<_>>()
    );

    // Finalization settles, counting the whole turn: p16's 20 in-flight
    // blocks become terminal parts at that moment — 30 + 21 = 51 > 30 →
    // the oldest whole turns drop until 29 remain (whole-turn granularity).
    let mut feed = MockFeed::new()
        .live(ACT, think_end_ev("p16", "p16-t"))
        .live(ACT, content_end_ev("p16", "p16-c19"))
        .live(ACT, pair_complete_ev("p16"));
    feed.drive_all(&hub).await;

    let HubSnapshot { items: settled, .. } = hub.snapshot(ACT).await.expect("registered");
    assert_eq!(think_of(&settled), think_text(12..=16));
    assert_eq!(
        text_of(&settled),
        "only".repeat(4) + &"streaming".repeat(20)
    );
    assert_eq!(
        users(&hub).await,
        (12..=16).map(|i| format!("q{i}")).collect::<Vec<_>>()
    );
}

/// c) A history projection batch ingested in bulk settles the same way:
/// 18 whole turns (36 parts) land in one batch and trim back to the most
/// recent 30 parts. Re-pulling the identical batch is a version-level
/// no-op — the batch rebuild resurrects the trimmed turns and the
/// settlement trims them right back within the same commit.
#[tokio::test]
async fn history_bulk_ingest_settles_to_steady_state() {
    let hub = ActivityHub::new();
    hub.register(ACT).await;

    let batch = projection_batch(1..=18);
    let mut feed = MockFeed::new().history(ACT, batch.clone());
    feed.drive_all(&hub).await;

    let HubSnapshot { version, items, .. } = hub.snapshot(ACT).await.expect("registered");
    assert_eq!(
        think_of(&items),
        think_text(4..=18),
        "three oldest whole turns (6 parts) dropped: 36 → 30"
    );
    assert_eq!(
        text_of(&items),
        (4..=18).map(|i| format!("c{i}")).collect::<String>()
    );
    assert_eq!(
        users(&hub).await,
        (4..=18).map(|i| format!("q{i}")).collect::<Vec<_>>()
    );

    let mut feed = MockFeed::new().history(ACT, batch);
    feed.drive_all(&hub).await;
    let HubSnapshot {
        version: after,
        items: items_again,
        ..
    } = hub.snapshot(ACT).await.expect("registered");
    assert_eq!(after, version, "re-pull + settle = no net mutation");
    assert_eq!(items, items_again);
}

/// e) The rebuild channel archives the full record: the same 18-turn
/// batch (36 parts) that the live steady-state channel trims to 30 lands
/// untouched via `apply_history_rebuild` — no settlement — while the
/// authoritative-ingest semantics (content validity flips to Live, the
/// window cursor rewritten) hold as in `apply_history`.
#[tokio::test]
async fn rebuild_channel_archives_full_record() {
    let hub = ActivityHub::new();
    hub.register(ACT).await;

    let wire: Vec<XacppEvent> = projection_batch(1..=18)
        .into_iter()
        .map(|(name, data)| XacppEvent::new(name, data))
        .collect();
    hub.with_session(ACT, |session| {
        session.apply_history_rebuild(
            wire,
            Some(WindowCursor {
                next_before_seq: Some(1),
                has_more: true,
            }),
        );
    })
    .await;

    let HubSnapshot {
        items,
        content_live,
        next_before_seq,
        has_more,
        ..
    } = hub.snapshot(ACT).await.expect("registered");
    assert!(
        content_live,
        "rebuild is an authoritative ingest: validity flips to Live"
    );
    assert_eq!(next_before_seq, Some(1));
    assert!(has_more);
    assert_eq!(
        think_of(&items),
        think_text(1..=18),
        "all 18 turns (36 parts) archived — no trim"
    );
    assert_eq!(
        text_of(&items),
        (1..=18).map(|i| format!("c{i}")).collect::<String>()
    );
    assert_eq!(
        users(&hub).await,
        (1..=18).map(|i| format!("q{i}")).collect::<Vec<_>>(),
        "no user bubble dropped"
    );
}

/// d) The trim is a UI-visible mutation through the batch A machinery:
/// finalization + settle bump the version exactly once, fire exactly one
/// notification, and the re-read snapshot reflects the trimmed content.
#[tokio::test]
async fn trim_bumps_version_once_and_snapshot_reflects_it() {
    let hub = ActivityHub::new();
    hub.register(ACT).await;
    let log = NotificationLog::attach(&hub).await;

    // 15 turns = exactly the cap (nothing trimmed while filling).
    for i in 1..=15 {
        feed_turn(
            &hub,
            &format!("p{i}"),
            &format!("q{i}"),
            &format!("t{i}"),
            &["only"],
        )
        .await;
    }

    // Stream turn 16 (in-flight, no settle), then finalize it: the settle
    // rides the pair_complete commit.
    feed_streaming_turn(&hub, "p16", "q16", "t16", &["only"]).await;
    let v_before = hub.version(ACT).await.unwrap();
    let notifications_before = log.versions_of(ACT).len();

    let mut feed = MockFeed::new().live(ACT, pair_complete_ev("p16"));
    feed.drive_all(&hub).await;

    let v_after = hub.version(ACT).await.unwrap();
    assert_eq!(
        v_after,
        v_before + 1,
        "finalization + trim = one committed mutation"
    );
    assert_eq!(
        log.versions_of(ACT).len(),
        notifications_before + 1,
        "exactly one notification for the settled mutation"
    );
    assert_eq!(log.versions_of(ACT).last().copied(), Some(v_after));

    let HubSnapshot { items, .. } = hub.snapshot(ACT).await.expect("registered");
    assert_eq!(
        think_of(&items),
        think_text(2..=16),
        "32 → 30: the oldest whole turn dropped"
    );
    assert_eq!(text_of(&items), "only".repeat(15));
}

// ---------------------------------------------------------------------------
// Drafts (consumer-held DraftStore; the registry carries no draft state).
// ---------------------------------------------------------------------------

/// a) Drafts live outside the registry: writes need no admission, and the
/// store is untouched by `unregister` / `unregister_all` — process-lifetime
/// semantics (the consumer holds the store; hub voiding never reaches it).
#[tokio::test]
async fn drafts_outlive_sessions_and_registry_voiding() {
    let hub = ActivityHub::new();
    let drafts = DraftStore::new();

    // No session at all: the write still lands.
    drafts.set(ACT, "no session yet").await;
    assert_eq!(drafts.get(ACT).await, Some("no session yet".to_string()));

    hub.register(ACT).await;
    drafts.set(ACT, "still typing").await;
    assert!(hub.unregister(ACT).await);
    assert_eq!(
        drafts.get(ACT).await,
        Some("still typing".to_string()),
        "draft survives unregister"
    );

    hub.register(ACT).await;
    hub.register("other").await;
    assert_eq!(hub.unregister_all().await, 2);
    assert_eq!(
        drafts.get(ACT).await,
        Some("still typing".to_string()),
        "draft survives registry voiding"
    );
}

/// b) Draft writes are not projection mutations: no version bump, no
/// notification — typing must never churn the projection.
#[tokio::test]
async fn drafts_never_touch_the_projection() {
    let hub = ActivityHub::new();
    hub.register(ACT).await;
    let log = NotificationLog::attach(&hub).await;
    let drafts = DraftStore::new();

    drafts.set(ACT, "d1").await;
    drafts.set(ACT, "d2").await;
    drafts.set(ACT, "").await;
    assert_eq!(hub.version(ACT).await, Some(0));
    assert!(log.versions_of(ACT).is_empty());
}
