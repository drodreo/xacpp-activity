//! Parallel-activity semantics pins.
//!
//! Contract (xacpp 1.0): one connection carries many activities; ordering
//! promises are **per-activity** — intra-activity events arrive in order,
//! cross-activity arrival order is unspecified and consumers must not rely
//! on it. The hub routes by activity id to the owning session
//! (ARCHITECTURE.md, "Event Path").
//!
//! What these tests pin:
//! - deep cross-activity interleaving of full turn streams (every
//!   same-turn event separated by foreign-activity events, under two
//!   interleave strategies) reproduces, per activity, exactly the model a
//!   solo feed produces — full `HubSnapshot` equality, plus named content
//!   assertions for readable failure surfaces;
//! - authoritative latest-window history batches landing interleaved
//!   across activities stay per-activity: projection rebuild, cursor
//!   registration and content never leak across the registry table.

mod common;

use xacpp::wire::events::XacppEvent;
use xacpp_activity::{ActivityHub, HubSnapshot, SnapshotItem, WindowCursor};

use common::{
    MockFeed, NotificationLog, complete_ev, content_delta_ev, content_end_ev, content_start_ev,
    pair_complete_ev, start_ev, text_of, think_delta_ev, think_end_ev, think_of, think_start_ev,
    tool_result_ev, tool_use_ev,
};

/// The activity table under test (≥3, exercising hash-table routing, not
/// just the two-activity shape pinned in buffer_projection.rs).
const ACTS: [&str; 3] = ["act-a", "act-b", "act-c"];
/// Identity tags woven into every payload field (pair / part ids / request
/// ids / texts), so any cross-activity bleed is visible in assertions.
const TAGS: [&str; 3] = ["a", "b", "c"];

// ---------------------------------------------------------------------------
// Turn stream construction.
// ---------------------------------------------------------------------------

/// One full live turn for `tag`: start → think block → content block →
/// tool call/result → pair_complete → complete. Every identity field is
/// tag-scoped; foreign tags appearing in a snapshot means bleed.
fn turn_stream(tag: &str) -> Vec<XacppEvent> {
    let pair = format!("p-{tag}");
    let t0 = format!("t0-{tag}");
    let t1 = format!("t1-{tag}");
    let req = format!("req-{tag}");
    vec![
        start_ev(&format!("user input {tag}")),
        think_start_ev(&pair, &t0),
        think_delta_ev(&pair, &t0, &format!("think-a-{tag}")),
        think_delta_ev(&pair, &t0, &format!("think-b-{tag}")),
        think_end_ev(&pair, &t0),
        content_start_ev(&pair, &t1),
        content_delta_ev(&pair, &t1, &format!("text-a-{tag}")),
        content_delta_ev(&pair, &t1, &format!("text-b-{tag}")),
        content_end_ev(&pair, &t1),
        tool_use_ev(&req),
        tool_result_ev(&req),
        pair_complete_ev(&pair),
        complete_ev(),
    ]
}

// ---------------------------------------------------------------------------
// Interleave strategies (deterministic; cross-activity order is the free
// variable, intra-activity order is untouched — that is the contract).
// ---------------------------------------------------------------------------

/// Round-robin: a1,b1,c1,a2,b2,c2,… — the tightest interleave; every
/// same-turn event is separated by two foreign-activity events.
fn interleave_round_robin(streams: &[Vec<XacppEvent>]) -> Vec<(usize, XacppEvent)> {
    let max_len = streams.iter().map(Vec::len).max().unwrap_or(0);
    let mut out = Vec::new();
    for col in 0..max_len {
        for (idx, stream) in streams.iter().enumerate() {
            if let Some(ev) = stream.get(col) {
                out.push((idx, ev.clone()));
            }
        }
    }
    out
}

/// Blocked round-robin: k consecutive events per activity per rotation
/// (k=3 splits every think/content block across foreign events while
/// keeping intra-activity order).
fn interleave_blocked(streams: &[Vec<XacppEvent>], k: usize) -> Vec<(usize, XacppEvent)> {
    let mut cursors = vec![0usize; streams.len()];
    let mut out = Vec::new();
    loop {
        let mut progressed = false;
        for (idx, stream) in streams.iter().enumerate() {
            let start = cursors[idx];
            let end = (start + k).min(stream.len());
            for ev in &stream[start..end] {
                out.push((idx, ev.clone()));
            }
            if end > start {
                cursors[idx] = end;
                progressed = true;
            }
        }
        if !progressed {
            break;
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Solo references.
// ---------------------------------------------------------------------------

/// Reference snapshot: the full turn stream fed alone on a fresh hub.
async fn solo_turn_snapshot(tag: &str, act: &str) -> HubSnapshot {
    let hub = ActivityHub::new();
    hub.register(act).await;
    let mut feed = MockFeed::new();
    for ev in turn_stream(tag) {
        feed = feed.live(act, ev);
    }
    feed.drive_all(&hub).await;
    hub.snapshot(act).await.expect("registered")
}

// ---------------------------------------------------------------------------
// Pins.
// ---------------------------------------------------------------------------

/// Deep cross-activity interleaving must be reducer-invisible: whatever
/// the cross-activity arrival order, each activity's model equals the
/// solo-feed model exactly (HubSnapshot equality covers render order,
/// version, turn state and cursor surface; the named assertions give the
/// failure a readable face).
#[tokio::test]
async fn three_activities_deep_interleave_matches_solo_feed() {
    let mut solo: Vec<HubSnapshot> = Vec::with_capacity(TAGS.len());
    for (i, tag) in TAGS.iter().enumerate() {
        solo.push(solo_turn_snapshot(tag, ACTS[i]).await);
    }

    let streams: Vec<Vec<XacppEvent>> = TAGS.iter().map(|tag| turn_stream(tag)).collect();

    for (label, items) in [
        ("round-robin", interleave_round_robin(&streams)),
        ("blocked(3)", interleave_blocked(&streams, 3)),
    ] {
        let hub = ActivityHub::new();
        for act in ACTS {
            hub.register(act).await;
        }
        let log = NotificationLog::attach(&hub).await;

        let mut feed = MockFeed::new();
        for (idx, ev) in items {
            feed = feed.live(ACTS[idx], ev);
        }
        feed.drive_all(&hub).await;

        for (i, act) in ACTS.iter().enumerate() {
            let tag = TAGS[i];
            let snap = hub.snapshot(act).await.expect("registered");

            // The pin: interleaved == solo, whole snapshot.
            assert_eq!(snap, solo[i], "[{label}] {act}: 交错喂与单独喂不等价");

            // Named assertions (readable failure surface; semantically
            // implied by the equality above).
            let expected_user = format!("user input {tag}");
            let users: Vec<String> = snap
                .items
                .iter()
                .filter_map(|item| match item {
                    SnapshotItem::User { inputs, .. } => Some(xacpp_activity::inputs_text(inputs)),
                    _ => None,
                })
                .collect();
            assert_eq!(users, vec![expected_user], "[{label}] {act} 用户气泡串了");
            assert_eq!(
                think_of(&snap.items),
                format!("think-a-{tag}think-b-{tag}"),
                "[{label}] {act} 思考内容串了"
            );
            assert_eq!(
                text_of(&snap.items),
                format!("text-a-{tag}text-b-{tag}"),
                "[{label}] {act} 正文内容串了"
            );
            let tools: Vec<&SnapshotItem> = snap
                .items
                .iter()
                .filter(|item| matches!(item, SnapshotItem::Tool { .. }))
                .collect();
            assert_eq!(tools.len(), 1, "[{label}] {act} 工具卡数量");
            let expected_req = format!("req-{tag}");
            match tools[0] {
                SnapshotItem::Tool {
                    request_id,
                    done,
                    result_text,
                    ..
                } => {
                    assert_eq!(request_id, &expected_req, "[{label}] {act} 工具卡串了");
                    assert_eq!(*done, Some(true), "[{label}] {act} 工具结果未归位");
                    assert_eq!(result_text.as_deref(), Some("2026-10-10"));
                }
                other => panic!("预期 Tool 条目，实际 {other:?}"),
            }
            assert_eq!(snap.turn_completed, 1, "[{label}] {act} 轮完结标记");
            assert!(!snap.turn_active, "[{label}] {act} complete 后不应在流式中");
        }

        // Notifications are per-activity: each activity sees exactly its
        // own version sequence 1..=n regardless of the interleaving.
        for (i, act) in ACTS.iter().enumerate() {
            let n = turn_stream(TAGS[i]).len() as u64;
            let expected: Vec<u64> = (1..=n).collect();
            assert_eq!(
                log.versions_of(act),
                expected,
                "[{label}] {act} 通知版本序列被打乱"
            );
        }
    }
}

/// History batches landing interleaved across activities stay
/// per-activity: the authoritative latest-window rebuild, the paging
/// cursor registration (tag-distinct values prove isolation) and
/// content validity never cross the table.
#[tokio::test]
async fn interleaved_history_batches_stay_per_activity() {
    // Solo reference: identical per-activity step sequence fed alone
    // (live in-flight prefix → history batch).
    let mut solo: Vec<HubSnapshot> = Vec::with_capacity(TAGS.len());
    for (i, tag) in TAGS.iter().enumerate() {
        solo.push(solo_history_snapshot(tag, i, ACTS[i]).await);
    }

    let hub = ActivityHub::new();
    for act in ACTS {
        hub.register(act).await;
    }
    let log = NotificationLog::attach(&hub).await;

    let mut feed = MockFeed::new();
    // Round 1: live starts, one per activity.
    for (i, act) in ACTS.iter().enumerate() {
        feed = feed.live(act, start_ev(&format!("user input {}", TAGS[i])));
    }
    // Round 2: live think starts.
    for (i, act) in ACTS.iter().enumerate() {
        feed = feed.live(
            act,
            think_start_ev(&format!("p-{}", TAGS[i]), &format!("t0-{}", TAGS[i])),
        );
    }
    // Round 3: live think deltas.
    for (i, act) in ACTS.iter().enumerate() {
        feed = feed.live(
            act,
            think_delta_ev(
                &format!("p-{}", TAGS[i]),
                &format!("t0-{}", TAGS[i]),
                &format!("think-a-{}", TAGS[i]),
            ),
        );
    }
    // Round 4: authoritative history batches with tag-distinct cursors.
    for (i, act) in ACTS.iter().enumerate() {
        feed = feed.history_with_cursor(act, history_batch(TAGS[i]), Some(tag_cursor(i)));
    }
    feed.drive_all(&hub).await;

    for (i, act) in ACTS.iter().enumerate() {
        let tag = TAGS[i];
        let snap = hub.snapshot(act).await.expect("registered");

        assert_eq!(snap, solo[i], "{act}: 交错喂与单独喂不等价（history 面）");
        assert!(snap.content_live, "{act} LatestWindow ingest 后应为 Live");
        assert_eq!(
            snap.next_before_seq,
            Some(10 + i as u64),
            "{act} 游标登记串了（应为 tag 专属水位）"
        );
        assert_eq!(snap.has_more, i % 2 == 0, "{act} hasMore 串了");
        assert_eq!(
            text_of(&snap.items),
            format!("text-a-{tag}text-b-{tag}"),
            "{act} 历史投影内容串了"
        );

        // Notifications are per-activity commits: 3 live events + 1
        // history batch (its fingerprint changes content_live/cursor/
        // projection) = versions 1..=4, in order.
        let expected: Vec<u64> = (1..=4).collect();
        assert_eq!(
            log.versions_of(act),
            expected,
            "{act} 通知版本序列被打乱（3 live + 1 history 批）"
        );
    }
}

// ---------------------------------------------------------------------------
// Shared helpers (history face).
// ---------------------------------------------------------------------------

/// Wire-form history batch for `tag`: the full turn projection (same
/// vocabulary as the live path, per the shared projection contract).
fn history_batch(tag: &str) -> Vec<(String, serde_json::Value)> {
    turn_stream(tag)
        .into_iter()
        .map(|ev| (ev.name, ev.data))
        .collect()
}

/// Tag-distinct cursor: watermark 10+i and an i-parity hasMore, so any
/// cross-activity cursor bleed flips an assertion.
fn tag_cursor(i: usize) -> WindowCursor {
    WindowCursor {
        next_before_seq: Some(10 + i as u64),
        has_more: i.is_multiple_of(2),
    }
}

/// Solo reference for the history-face pin: same per-activity step
/// sequence (live prefix → history batch), fed alone.
async fn solo_history_snapshot(tag: &str, idx: usize, act: &str) -> HubSnapshot {
    let hub = ActivityHub::new();
    hub.register(act).await;
    let pair = format!("p-{tag}");
    let t0 = format!("t0-{tag}");
    let mut feed = MockFeed::new()
        .live(act, start_ev(&format!("user input {tag}")))
        .live(act, think_start_ev(&pair, &t0))
        .live(act, think_delta_ev(&pair, &t0, &format!("think-a-{tag}")))
        .history_with_cursor(act, history_batch(tag), Some(tag_cursor(idx)));
    feed.drive_all(&hub).await;
    hub.snapshot(act).await.expect("registered")
}
