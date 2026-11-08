//! User input identity: one logical input = one canonical identity.
//!
//! The user bubble stores the complete user-visible ordered inputs; its
//! snapshot key is the canonical-input hash, so a materialization-only
//! field drift across the live and history channels cannot split or merge
//! two logical inputs, two genuinely different inputs never share one key,
//! and repeated identical inputs get a deterministic `#{n}` suffix.
//!
//! Channel shapes (fixtures):
//! - live: provider part ids, local temp paths, `requireOrganized=true`;
//! - history: replay part ids, remote CDN urls, `remoteExpiresAt` set —
//!   the same logical input as the live stream, re-expressed.

use xacpp::wire::events::{ContentPart, FilePart, FileRef, ImagePart, TextPart};
use xacpp_activity::event::{PairCompletePayload, StartPayload};
use xacpp_activity::{
    ActivityEvent, ActivityEventCategory, ActivityEventEnvelope, ActivityModel, SnapshotItem,
    inputs_text,
};

// ---------------------------------------------------------------------------
// Fixture helpers
// ---------------------------------------------------------------------------

fn envelope_of(event: ActivityEvent) -> ActivityEventEnvelope {
    ActivityEventEnvelope {
        activity_id: "act-1".into(),
        seq: 0,
        pair_epoch: 0,
        category: ActivityEventCategory::Default,
        payload: event,
    }
}

fn apply_live(model: &mut ActivityModel, event: ActivityEvent) {
    model.apply_envelope(&envelope_of(event));
}

/// History channel: the event takes the real wire round-trip
/// (`to_wire` → decode) before entering the same per-event path.
fn apply_history(model: &mut ActivityModel, event: ActivityEvent) {
    model.apply_history_events([event.to_wire()]);
}

fn start(inputs: Vec<ContentPart>) -> ActivityEvent {
    ActivityEvent::Start(StartPayload { inputs })
}

fn text_part(part_id: &str, text: &str) -> ContentPart {
    ContentPart::Text(TextPart {
        text: text.into(),
        part_id: Some(part_id.into()),
        part_type: "default".to_string(),
    })
}

fn hidden_part(text: &str) -> ContentPart {
    ContentPart::Text(TextPart {
        text: text.into(),
        part_id: None,
        part_type: "thinking".to_string(),
    })
}

fn file_ref(sha: &str, local_uri: &str, remote_url: &str, organized: bool) -> FileRef {
    FileRef {
        remote_url: remote_url.into(),
        local_uri: local_uri.into(),
        remote_expires_at: None,
        mime_type: "image/png".to_string(),
        require_organized: organized,
        size_bytes: 7,
        sha256: sha.into(),
    }
}

fn image_part(
    part_id: &str,
    sha: &str,
    local_uri: &str,
    remote_url: &str,
    organized: bool,
) -> ContentPart {
    ContentPart::Image(ImagePart {
        source: file_ref(sha, local_uri, remote_url, organized),
        detail: None,
        width: None,
        height: None,
        part_id: Some(part_id.into()),
        part_type: "default".to_string(),
    })
}

fn file_part(part_id: &str, r: FileRef) -> ContentPart {
    ContentPart::File(FilePart {
        source: r,
        part_id: Some(part_id.into()),
        part_type: "default".to_string(),
    })
}

fn user_items(model: &ActivityModel) -> Vec<SnapshotItem> {
    model
        .snapshot()
        .into_iter()
        .filter(|item| matches!(item, SnapshotItem::User { .. }))
        .collect()
}

fn user_keys(model: &ActivityModel) -> Vec<String> {
    user_items(model)
        .into_iter()
        .map(|item| match item {
            SnapshotItem::User { key, .. } => key,
            _ => unreachable!("filtered to User"),
        })
        .collect()
}

fn user_texts(model: &ActivityModel) -> Vec<String> {
    user_items(model)
        .into_iter()
        .map(|item| match item {
            SnapshotItem::User { inputs, .. } => inputs_text(&inputs),
            _ => unreachable!("filtered to User"),
        })
        .collect()
}

fn user_inputs(model: &ActivityModel) -> Vec<Vec<ContentPart>> {
    user_items(model)
        .into_iter()
        .map(|item| match item {
            SnapshotItem::User { inputs, .. } => inputs,
            _ => unreachable!("filtered to User"),
        })
        .collect()
}

/// The canonical live expression of one logical image input.
fn live_image(part_id: &str) -> ContentPart {
    image_part(part_id, "SHA-A", "/tmp/inbound/x.png", "", true)
}

/// The history re-expression of the same logical image: replay part id,
/// materialized remote url, no local path, expiry stamped, not organized.
fn history_image(part_id: &str) -> ContentPart {
    let mut r = file_ref("SHA-A", "", "https://cdn.example/x.png", false);
    r.remote_expires_at = Some("2026-11-01T00:00:00Z".into());
    ContentPart::Image(ImagePart {
        source: r,
        detail: None,
        width: None,
        height: None,
        part_id: Some(part_id.into()),
        part_type: "default".to_string(),
    })
}

// ---------------------------------------------------------------------------
// Entry content: complete ordered visible inputs
// ---------------------------------------------------------------------------

/// Mixed text/image/file parts land in one entry, in payload order; text
/// is derived (`inputs_text`), never stored separately.
#[test]
fn mixed_parts_single_entry_in_order() {
    let mut model = ActivityModel::new();
    apply_live(
        &mut model,
        start(vec![
            text_part("i0", "look at "),
            live_image("i1"),
            file_part("i2", file_ref("SHA-B", "/tmp/inbound/a.pdf", "", true)),
            text_part("i3", " and the pdf"),
        ]),
    );

    let inputs = user_inputs(&model);
    assert_eq!(inputs.len(), 1, "one start = one user entry");
    assert_eq!(inputs[0].len(), 4, "all four parts survive");
    assert!(
        matches!(inputs[0][0], ContentPart::Text(_)),
        "position 0 = text"
    );
    assert!(
        matches!(inputs[0][1], ContentPart::Image(_)),
        "position 1 = image"
    );
    assert!(
        matches!(inputs[0][2], ContentPart::File(_)),
        "position 2 = file"
    );
    assert!(
        matches!(inputs[0][3], ContentPart::Text(_)),
        "position 3 = text"
    );
    assert_eq!(
        user_texts(&model),
        vec!["look at  and the pdf".to_string()],
        "text derived across positions in order"
    );
}

/// A media-only input (no text part at all) is kept as a user entry —
/// the old "non-empty text" gate must not drop it.
#[test]
fn media_only_input_is_kept() {
    let mut model = ActivityModel::new();
    apply_live(&mut model, start(vec![live_image("i0")]));

    let inputs = user_inputs(&model);
    assert_eq!(inputs.len(), 1, "pure media keeps its entry");
    assert_eq!(inputs[0].len(), 1);
    assert!(matches!(inputs[0][0], ContentPart::Image(_)));
    assert_eq!(user_texts(&model), vec![String::new()], "no text parts");
}

/// Hidden part types (non-`default`/`references`) never enter a user
/// entry; visible parts around them keep their order.
#[test]
fn hidden_parts_are_filtered() {
    let mut model = ActivityModel::new();
    apply_live(
        &mut model,
        start(vec![
            text_part("i0", "visible"),
            hidden_part("engine reasoning fragment"),
            live_image("i1"),
            hidden_part("another fragment"),
        ]),
    );

    let inputs = user_inputs(&model);
    assert_eq!(inputs.len(), 1, "visible parts keep the entry alive");
    assert_eq!(inputs[0].len(), 2, "hidden parts dropped");
    assert!(matches!(inputs[0][0], ContentPart::Text(_)));
    assert!(matches!(inputs[0][1], ContentPart::Image(_)));

    // A start carrying only hidden parts produces no user entry at all.
    let mut model = ActivityModel::new();
    apply_live(&mut model, start(vec![hidden_part("only fragment")]));
    assert!(
        user_items(&model).is_empty(),
        "hidden-only start = no user entry"
    );
}

// ---------------------------------------------------------------------------
// Identity: materialization drift stability vs. genuine difference
// ---------------------------------------------------------------------------

/// live and history re-expressions of one logical input share the key —
/// partId, localUri/remoteUrl, remoteExpiresAt and requireOrganized drift
/// is ignored while the content sha is present.
#[test]
fn materialization_drift_keeps_same_key_across_channels() {
    let mut live = ActivityModel::new();
    apply_live(
        &mut live,
        start(vec![text_part("live-i0", "hi"), live_image("live-i1")]),
    );

    let mut history = ActivityModel::new();
    apply_history(
        &mut history,
        start(vec![
            text_part("replay:i0", "hi"),
            history_image("replay:i1"),
        ]),
    );

    assert_eq!(
        user_keys(&live),
        user_keys(&history),
        "one logical input = one key across channels"
    );

    // Same model, adjacent re-expression (history pull after live start):
    // canonical equality merges the re-pushed start into one bubble.
    let mut merged = ActivityModel::new();
    apply_live(
        &mut merged,
        start(vec![text_part("live-i0", "hi"), live_image("live-i1")]),
    );
    apply_history(
        &mut merged,
        start(vec![
            text_part("replay:i0", "hi"),
            history_image("replay:i1"),
        ]),
    );
    assert_eq!(
        user_items(&merged).len(),
        1,
        "adjacent canonical-equal starts merge across channels"
    );
}

/// Same visible text but different media = different inputs: no merge,
/// no shared key.
#[test]
fn same_text_different_media_stay_distinct() {
    let mut model = ActivityModel::new();
    apply_live(
        &mut model,
        start(vec![text_part("a0", "compare"), live_image("a1")]),
    );
    apply_live(
        &mut model,
        start(vec![
            text_part("b0", "compare"),
            image_part("b1", "SHA-OTHER", "/tmp/inbound/y.png", "", true),
        ]),
    );

    let keys = user_keys(&model);
    assert_eq!(keys.len(), 2, "both inputs kept");
    assert_ne!(keys[0], keys[1], "different media = different keys");
}

/// With an empty sha the controlled localUri/remoteUrl fallback is the
/// identity — two different files never collapse, and the fallback
/// survives the wire round-trip (history sees the same URI).
#[test]
fn empty_sha_different_uris_do_not_collapse() {
    let mut model = ActivityModel::new();
    apply_live(
        &mut model,
        start(vec![file_part(
            "a0",
            file_ref("", "/data/a.bin", "", false),
        )]),
    );
    apply_history(
        &mut model,
        start(vec![file_part(
            "b0",
            file_ref("", "/data/b.bin", "", false),
        )]),
    );

    let keys = user_keys(&model);
    assert_eq!(keys.len(), 2, "different fallback URIs = different inputs");
    assert_ne!(keys[0], keys[1]);
}

// ---------------------------------------------------------------------------
// Replay merge and deterministic repetition suffix
// ---------------------------------------------------------------------------

/// Adjacent canonical-equal starts merge; the same canonical input
/// re-appearing later (not adjacent) stays a distinct bubble with a
/// deterministic `#{n}` suffix — and pair backfill still works per bubble.
#[test]
fn adjacent_merge_and_deterministic_repeat_suffix() {
    let input_x = || vec![text_part("x0", "repeat me"), live_image("x1")];
    let input_y = || vec![text_part("y0", "middle")];

    // Adjacent: live start + its history re-push → one bubble.
    let mut merged = ActivityModel::new();
    apply_live(&mut merged, start(input_x()));
    apply_history(&mut merged, start(input_x()));
    assert_eq!(user_items(&merged).len(), 1);

    // Non-adjacent: X … Y … X → three bubbles, keys u:{x}, u:{y}, u:{x}#2.
    let mut model = ActivityModel::new();
    apply_live(&mut model, start(input_x()));
    apply_live(
        &mut model,
        ActivityEvent::ThinkStart(xacpp_activity::event::BlockBoundaryPayload {
            round: "r1".into(),
            pair: "p1".into(),
            part_id: "t0".into(),
        }),
    );
    // Give the think block real content: close_pair drops empty blocks of
    // the closed pair, and the backfill test needs a surviving paired entry.
    apply_live(
        &mut model,
        ActivityEvent::ThinkPart(xacpp_activity::event::ThinkContentPayload {
            content: "reasoning".into(),
            round: "r1".into(),
            pair: "p1".into(),
            part_id: "t0".into(),
        }),
    );
    apply_live(&mut model, start(input_y()));
    apply_live(&mut model, start(input_x()));
    // Close p1 so pair backfill is exercised on the middle bubble.
    apply_live(
        &mut model,
        ActivityEvent::PairComplete(PairCompletePayload {
            pair: "p1".into(),
            context_window: 1000,
            token_usage: xacpp::wire::events::TokenUsage::default(),
            tier: None,
            model: None,
        }),
    );

    let keys = user_keys(&model);
    assert_eq!(
        keys.len(),
        3,
        "same-canonical non-adjacent inputs stay distinct"
    );
    assert_ne!(keys[0], keys[1]);
    assert_eq!(
        keys[2],
        format!("{}#2", keys[0]),
        "deterministic suffix on the repeated canonical input"
    );
    assert_eq!(
        user_texts(&model),
        vec![
            "repeat me".to_string(),
            "middle".to_string(),
            "repeat me".to_string(),
        ]
    );

    // Pair backfill: the think block (p1) sits between the first X and Y,
    // so the first bubble owns p1; the later bubbles have no paired entry
    // in range.
    let pairs: Vec<Option<String>> = user_items(&model)
        .iter()
        .filter_map(|item| match item {
            SnapshotItem::User { pair, .. } => Some(pair.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(pairs, vec![Some("p1".to_string()), None, None]);
}

/// The identity hash is deterministic across independently built models
/// (no process-local randomness in the key).
#[test]
fn key_is_deterministic_across_models() {
    let build = || {
        let mut model = ActivityModel::new();
        apply_live(
            &mut model,
            start(vec![text_part("i0", "stable"), live_image("i1")]),
        );
        user_keys(&model)
    };
    assert_eq!(
        build(),
        build(),
        "same canonical inputs = same key every time"
    );
}
