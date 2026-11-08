//! Content validity (Void → Live): the full transition matrix, snapshot
//! exposure, and registry voiding semantics (ARCHITECTURE.md, "Buffer &
//! Projection Model" / "Disconnection").

mod common;

use common::{MockFeed, NotificationLog, pair_complete_ev, start_ev};
use serde_json::json;
use xacpp_activity::{ActivityHub, WindowCursor};

const ACT: &str = "act-a";

async fn content_live(hub: &ActivityHub, activity: &str) -> Option<bool> {
    hub.snapshot(activity).await.map(|s| s.content_live)
}

/// Admission starts Void.
#[tokio::test]
async fn admission_starts_void() {
    let hub = ActivityHub::new();
    assert!(hub.register(ACT).await);
    assert_eq!(content_live(&hub, ACT).await, Some(false));
}

/// Interaction registration (lazy admission path included) never flips
/// validity: an off-page command admits the session but leaves it Void.
#[tokio::test]
async fn interaction_registration_stays_void() {
    let hub = ActivityHub::new();
    hub.register(ACT).await;
    hub.with_session(ACT, |session| {
        session
            .consume_command(
                "action_request",
                json!({"toolName":"shell","arguments":"ls","actionId":"a1",
                       "description":"d","alert":"info","intent":"i"}),
            )
            .expect("command consumes");
    })
    .await;
    assert_eq!(content_live(&hub, ACT).await, Some(false));
}

/// Live events never flip validity: streaming into a Void session leaves
/// it Void (the pre-admission past is still unknown).
#[tokio::test]
async fn live_events_stay_void() {
    let hub = ActivityHub::new();
    hub.register(ACT).await;
    MockFeed::new()
        .live(ACT, start_ev("hi"))
        .live(ACT, pair_complete_ev("p1"))
        .drive_all(&hub)
        .await;
    assert_eq!(content_live(&hub, ACT).await, Some(false));
}

/// The authoritative latest-window history ingest flips Void → Live.
#[tokio::test]
async fn latest_window_turns_live() {
    let hub = ActivityHub::new();
    hub.register(ACT).await;
    MockFeed::new()
        .history(
            ACT,
            vec![(
                "start".to_string(),
                json!({"inputs": [{"type": "text", "text": "hi"}]}),
            )],
        )
        .drive_all(&hub)
        .await;
    assert_eq!(content_live(&hub, ACT).await, Some(true));
}

/// An empty latest window is still authoritative (a fresh activity has no
/// past — empty is the truth, not a miss).
#[tokio::test]
async fn empty_latest_window_turns_live() {
    let hub = ActivityHub::new();
    hub.register(ACT).await;
    MockFeed::new().history(ACT, vec![]).drive_all(&hub).await;
    assert_eq!(content_live(&hub, ACT).await, Some(true));
}

/// Older-page paging never flips validity (paging does not write back):
/// the cursor-only bookkeeping step touches no content surface.
#[tokio::test]
async fn older_page_never_turns_live() {
    let hub = ActivityHub::new();
    hub.register(ACT).await;
    MockFeed::new()
        .cursor_only(
            ACT,
            WindowCursor {
                next_before_seq: Some(41),
                has_more: true,
            },
        )
        .drive_all(&hub)
        .await;
    assert_eq!(content_live(&hub, ACT).await, Some(false));
}

/// The Void → Live flip is a UI-visible mutation: the version bumps and
/// exactly one notification fires even when the ingested content itself
/// changed nothing observable (empty window onto an empty model).
#[tokio::test]
async fn validity_flip_is_a_projection_mutation() {
    let hub = ActivityHub::new();
    hub.register(ACT).await;
    let log = NotificationLog::attach(&hub).await;
    let version_before = hub.version(ACT).await.unwrap();

    MockFeed::new().history(ACT, vec![]).drive_all(&hub).await;

    assert!(
        hub.version(ACT).await.unwrap() > version_before,
        "Void→Live must bump the version"
    );
    assert_eq!(log.versions_of(ACT).len(), 1);

    // A second identical ingest does not flip again: validity is steady.
    let version_live = hub.version(ACT).await.unwrap();
    MockFeed::new().history(ACT, vec![]).drive_all(&hub).await;
    assert_eq!(hub.version(ACT).await.unwrap(), version_live);
}

/// unregister resets validity: re-admission starts Void again.
#[tokio::test]
async fn unregister_resets_validity() {
    let hub = ActivityHub::new();
    hub.register(ACT).await;
    MockFeed::new().history(ACT, vec![]).drive_all(&hub).await;
    assert_eq!(content_live(&hub, ACT).await, Some(true));

    assert!(hub.unregister(ACT).await);
    assert!(hub.register(ACT).await);
    assert_eq!(content_live(&hub, ACT).await, Some(false));
}

/// unregister_all voids the whole registry (connection-death semantics):
/// every session dies, snapshots are gone, re-admission starts Void.
#[tokio::test]
async fn unregister_all_voids_everything() {
    let hub = ActivityHub::new();
    hub.register(ACT).await;
    hub.register("act-b").await;
    MockFeed::new().history(ACT, vec![]).drive_all(&hub).await;

    assert_eq!(hub.unregister_all().await, 2);
    assert_eq!(hub.snapshot(ACT).await, None);
    assert_eq!(hub.snapshot("act-b").await, None);
    assert_eq!(
        hub.unregister_all().await,
        0,
        "voiding an empty hub is a no-op"
    );

    assert!(hub.register(ACT).await);
    assert_eq!(content_live(&hub, ACT).await, Some(false));
}

// ---------------------------------------------------------------------------
// Window paging cursor (same-class bookkeeping as content validity: it rides
// the atomic snapshot, is registered at authoritative latest-window ingests,
// and dies with the session).await.

async fn cursor(hub: &ActivityHub, activity: &str) -> Option<(Option<u64>, bool)> {
    hub.snapshot(activity)
        .await
        .map(|s| (s.next_before_seq, s.has_more))
}

/// The authoritative latest-window ingest registers the wire cursor
/// (`nextBeforeSeqInTopic` / `hasMore`) and the snapshot exposes it.
#[tokio::test]
async fn latest_window_registers_cursor() {
    let hub = ActivityHub::new();
    hub.register(ACT).await;
    MockFeed::new()
        .history_with_cursor(
            ACT,
            vec![],
            Some(WindowCursor {
                next_before_seq: Some(42),
                has_more: true,
            }),
        )
        .drive_all(&hub)
        .await;
    assert_eq!(cursor(&hub, ACT).await, Some((Some(42), true)));
}

/// A cursor-less latest-window ingest resets the cursor to "no older
/// history" (the authoritative window replaces the old one wholesale —
/// stale cursors must not survive a window replacement).
#[tokio::test]
async fn latest_window_without_cursor_resets_cursor() {
    let hub = ActivityHub::new();
    hub.register(ACT).await;
    MockFeed::new()
        .history_with_cursor(
            ACT,
            vec![],
            Some(WindowCursor {
                next_before_seq: Some(42),
                has_more: true,
            }),
        )
        .history(ACT, vec![])
        .drive_all(&hub)
        .await;
    assert_eq!(cursor(&hub, ACT).await, Some((None, false)));
}

/// Older-page paging writes no content back but advances the cursor
/// bookkeeping (validity untouched — paging never flips Void → Live).
#[tokio::test]
async fn older_page_advances_cursor_bookkeeping() {
    let hub = ActivityHub::new();
    hub.register(ACT).await;
    MockFeed::new()
        .history_with_cursor(
            ACT,
            vec![],
            Some(WindowCursor {
                next_before_seq: Some(42),
                has_more: true,
            }),
        )
        .cursor_only(
            ACT,
            WindowCursor {
                next_before_seq: Some(41),
                has_more: true,
            },
        )
        .drive_all(&hub)
        .await;
    assert_eq!(cursor(&hub, ACT).await, Some((Some(41), true)));
    assert_eq!(
        content_live(&hub, ACT).await,
        Some(true),
        "paging never touches validity"
    );
}

/// Cursor registration is a projection mutation: the version bumps and a
/// notification fires (consumers page from the snapshot — it must be
/// observable without side bookkeeping).
#[tokio::test]
async fn cursor_registration_is_a_projection_mutation() {
    let hub = ActivityHub::new();
    hub.register(ACT).await;
    let log = NotificationLog::attach(&hub).await;
    let version_before = hub.version(ACT).await.unwrap();

    MockFeed::new()
        .history_with_cursor(
            ACT,
            vec![],
            Some(WindowCursor {
                next_before_seq: Some(7),
                has_more: true,
            }),
        )
        .drive_all(&hub)
        .await;

    assert!(hub.version(ACT).await.unwrap() > version_before);
    assert_eq!(log.versions_of(ACT), vec![hub.version(ACT).await.unwrap()]);
}

/// unregister resets the cursor along with validity: re-admission starts
/// with no bookkeeping (the session — cursor included — dies).
#[tokio::test]
async fn unregister_resets_cursor() {
    let hub = ActivityHub::new();
    hub.register(ACT).await;
    MockFeed::new()
        .history_with_cursor(
            ACT,
            vec![],
            Some(WindowCursor {
                next_before_seq: Some(42),
                has_more: true,
            }),
        )
        .drive_all(&hub)
        .await;
    assert_eq!(cursor(&hub, ACT).await, Some((Some(42), true)));

    assert!(hub.unregister(ACT).await);
    assert!(hub.register(ACT).await);
    assert_eq!(cursor(&hub, ACT).await, Some((None, false)));
}

/// Direct cursor registration (set_window_cursor, the older-page
/// bookkeeping seam) advances the snapshot state without touching content
/// or validity.
#[tokio::test]
async fn set_window_cursor_is_bookkeeping_only() {
    let hub = ActivityHub::new();
    hub.register(ACT).await;
    let log = NotificationLog::attach(&hub).await;
    let version_before = hub.version(ACT).await.unwrap();

    hub.with_session(ACT, |session| {
        session.set_window_cursor(WindowCursor {
            next_before_seq: Some(13),
            has_more: true,
        })
    })
    .await;

    assert_eq!(cursor(&hub, ACT).await, Some((Some(13), true)));
    assert_eq!(
        content_live(&hub, ACT).await,
        Some(false),
        "cursor bookkeeping never flips validity"
    );
    assert!(hub.version(ACT).await.unwrap() > version_before);
    assert_eq!(log.versions_of(ACT), vec![hub.version(ACT).await.unwrap()]);
}
