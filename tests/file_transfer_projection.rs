//! File-transfer integration fixtures: the xaact consumer face for the
//! "mock-serve ↔ xaact" file-transfer scenarios.
//!
//! Wire shapes follow the live protocol exactly (mock-serve emits
//! wire-native `{name, data}` events; these fixtures construct the same
//! shapes and drive the standard consumer chain: `from_xacpp_event` →
//! `runtime.dispatch` → `model.apply_envelope`). No ports, no network —
//! same stance as the e2e scenarios.
//!
//! Four paths exercised (the ones the app hits during real file
//! transfer against a live serve):
//!
//! 1. Media `content_part` events (image/file parts carrying `FileRef`
//!    text pointers) land verbatim in block `media` — never into text.
//! 2. `upload` progress events stay a side channel: `side_events`
//!    verbatim, turn projection untouched.
//! 3. `send_file` tool results keep their pointer-credential parts
//!    (remote URL / local URI / sha256) untouched in the tool card.
//! 4. Fetch-transaction words (1.1) are not event vocabulary: the wire
//!    gate rejects them loudly, so no fetch state can silently enter
//!    the projection through the event channel.

use serde_json::{Value, json};
use xacpp::wire::events::{ContentPart, FileRef, XacppEvent};
use xacpp_activity::{ActivityEvent, ActivityEventCategory, ActivityModel, ActivityRuntime};

/// The consumer device: per-activity runtime + session model, the shape
/// xamobile/agentx consumers keep.
struct Consumer {
    runtime: ActivityRuntime,
    model: ActivityModel,
}

impl Consumer {
    fn new() -> Self {
        Self {
            runtime: ActivityRuntime::new("act-1"),
            model: ActivityModel::new(),
        }
    }

    /// Applies one wire event live (dispatch assigns the seq, the model
    /// consumes the envelope).
    fn on_wire_event(&mut self, name: &str, data: Value) {
        let wire = XacppEvent::new(name, data);
        let typed = ActivityEvent::from_xacpp_event(&wire).expect("fixture wire shapes must parse");
        let envelope = self.runtime.dispatch(ActivityEventCategory::Default, typed);
        self.model.apply_envelope(&envelope);
    }
}

/// An image part carrying the full FileRef pointer credential set the
/// serve emits for send_file results (camelCase wire keys).
fn image_pointer_part() -> Value {
    json!({
        "type": "image",
        "source": {
            "remoteUrl": "https://files.example.test/m/f9a1.png",
            "localUri": "/tmp/agent/files/f9a1.png",
            "mimeType": "image/png",
            "requireOrganized": false,
            "sizeBytes": 20480,
            "sha256": "b6f9c1d2e3a4f5061728394a5b6c7d8e9f0a1b2c3d4e5f60718293a4b5c6d7e8"
        },
        "width": 640,
        "height": 480,
        "partId": "live-m1",
        "partType": "image"
    })
}

/// A generic file part with the same pointer credential set.
fn file_pointer_part() -> Value {
    json!({
        "type": "file",
        "source": {
            "remoteUrl": "https://files.example.test/m/report.pdf",
            "localUri": "/tmp/agent/files/report.pdf",
            "mimeType": "application/pdf",
            "requireOrganized": false,
            "sizeBytes": 8192,
            "sha256": "0a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f"
        },
        "partId": "live-m2",
        "partType": "document"
    })
}

/// Asserts the part is an Image whose FileRef carries the pointer
/// credentials verbatim (the "text pointer" form: no bytes inline, only
/// URLs/hashes a consumer can dereference itself).
fn expect_image_pointer(part: &ContentPart) {
    let ContentPart::Image(image) = part else {
        panic!("expected Image part, got {part:?}");
    };
    let expected = FileRef {
        remote_url: "https://files.example.test/m/f9a1.png".into(),
        local_uri: "/tmp/agent/files/f9a1.png".into(),
        remote_expires_at: None,
        mime_type: "image/png".into(),
        require_organized: false,
        size_bytes: 20480,
        sha256: "b6f9c1d2e3a4f5061728394a5b6c7d8e9f0a1b2c3d4e5f60718293a4b5c6d7e8".into(),
    };
    assert_eq!(image.source, expected, "FileRef must round-trip verbatim");
    assert_eq!(image.width, Some(640));
    assert_eq!(image.height, Some(480));
}

/// Scenario 1: media content_parts project into block media, verbatim,
/// without touching block text. Each new part_id opens its own implicit
/// snapshot block (the same behavior the app relies on to render one
/// gallery item per part).
#[test]
fn media_parts_project_into_block_media() {
    let mut consumer = Consumer::new();

    consumer.on_wire_event(
        "start",
        json!({"inputs": [{"type": "text", "text": "send me the chart"}]}),
    );
    consumer.on_wire_event(
        "content_start",
        json!({"partId": "live-c0", "round": "r1", "pair": "p1"}),
    );
    consumer.on_wire_event(
        "content_delta",
        json!({"round": "r1", "pair": "p1", "payload": {"type": "text", "text": "here it is", "partId": "live-c0"}}),
    );
    // Media parts: implicit blocks, full-snapshot semantics.
    consumer.on_wire_event(
        "content_part",
        json!({"round": "r1", "pair": "p1", "payload": image_pointer_part()}),
    );
    consumer.on_wire_event(
        "content_part",
        json!({"round": "r1", "pair": "p1", "payload": file_pointer_part()}),
    );
    consumer.on_wire_event(
        "pair_complete",
        json!({"pair": "p1", "contextWindow": 200000, "tokenUsage": {"id": "u1", "message": "m1", "promptTokens": 1, "completionTokens": 1, "totalTokens": 2}}),
    );
    consumer.on_wire_event(
        "complete",
        json!({"round": "r1", "assistantReply": [{"type": "text", "text": "done"}], "canceled": false}),
    );

    let turn = &consumer.model.get_turns()[0];
    assert_eq!(
        turn.contents.len(),
        3,
        "text block + two implicit media blocks"
    );

    // Text block: text intact, no media pollution.
    assert_eq!(turn.contents[0].text, "here it is");
    assert!(turn.contents[0].media.is_empty());

    // Image block: media verbatim, text empty.
    assert_eq!(turn.contents[1].text, "");
    assert_eq!(turn.contents[1].media.len(), 1);
    expect_image_pointer(&turn.contents[1].media[0]);

    // File block: media verbatim, partId routing survived finalization.
    assert_eq!(turn.contents[2].text, "");
    assert_eq!(turn.contents[2].media.len(), 1);
    let ContentPart::File(file) = &turn.contents[2].media[0] else {
        panic!("expected File part, got {:?}", turn.contents[2].media[0]);
    };
    assert_eq!(
        file.source.remote_url,
        "https://files.example.test/m/report.pdf"
    );
    assert_eq!(file.source.local_uri, "/tmp/agent/files/report.pdf");
    assert_eq!(file.source.mime_type, "application/pdf");
    assert_eq!(file.part_type, "document");
}

/// Scenario 2: upload progress/completed/error stay a side channel —
/// side_events verbatim, turn projection untouched.
#[test]
fn upload_side_channel_keeps_turn_clean() {
    let mut consumer = Consumer::new();

    consumer.on_wire_event(
        "start",
        json!({"inputs": [{"type": "text", "text": "upload the log"}]}),
    );
    // A live content stream alongside the uploads: proves uploads ride
    // the side channel without polluting the content projection.
    consumer.on_wire_event(
        "content_start",
        json!({"partId": "live-c0", "round": "r1", "pair": "p1"}),
    );
    consumer.on_wire_event(
        "content_delta",
        json!({"round": "r1", "pair": "p1", "payload": {"type": "text", "text": "uploading", "partId": "live-c0"}}),
    );
    consumer.on_wire_event(
        "upload",
        json!({"upload_event": "progress", "name": "log.txt", "uploaded": 512, "total": 1024}),
    );
    consumer.on_wire_event(
        "upload",
        json!({"upload_event": "progress", "name": "log.txt", "uploaded": 1024, "total": 1024}),
    );
    consumer.on_wire_event(
        "upload",
        json!({"upload_event": "completed", "name": "log.txt", "media_source": {
            "remoteUrl": "https://files.example.test/m/log.txt",
            "localUri": "/tmp/agent/files/log.txt",
            "mimeType": "text/plain",
            "requireOrganized": false,
            "sizeBytes": 1024,
            "sha256": "aa11bb22cc33dd44ee55ff6600778899aabbccddeeff00112233445566778899"
        }}),
    );
    consumer.on_wire_event(
        "upload",
        json!({"upload_event": "error", "name": "other.bin", "error": "disk full"}),
    );
    consumer.on_wire_event(
        "pair_complete",
        json!({"pair": "p1", "contextWindow": 200000, "tokenUsage": {"id": "u1", "message": "m1", "promptTokens": 1, "completionTokens": 1, "totalTokens": 2}}),
    );
    consumer.on_wire_event(
        "complete",
        json!({"round": "r1", "assistantReply": [{"type": "text", "text": "uploaded"}], "canceled": false}),
    );

    // Side channel: all four upload events kept verbatim, in order.
    let uploads: Vec<&ActivityEvent> = consumer
        .model
        .side_events
        .iter()
        .filter(|event| matches!(event, ActivityEvent::Upload(_)))
        .collect();
    assert_eq!(uploads.len(), 4, "progress, progress, completed, error");

    let mut seen = uploads.iter();
    match seen.next().unwrap() {
        ActivityEvent::Upload(xacpp::wire::events::UploadEvent::Progress {
            name,
            uploaded,
            total,
        }) => {
            assert_eq!((name.as_str(), *uploaded, *total), ("log.txt", 512, 1024));
        }
        other => panic!("expected first Upload::Progress, got {other:?}"),
    }
    match seen.next().unwrap() {
        ActivityEvent::Upload(xacpp::wire::events::UploadEvent::Progress { uploaded, .. }) => {
            assert_eq!(*uploaded, 1024)
        }
        other => panic!("expected second Upload::Progress, got {other:?}"),
    }
    match seen.next().unwrap() {
        ActivityEvent::Upload(xacpp::wire::events::UploadEvent::Completed {
            name,
            media_source,
        }) => {
            assert_eq!(name, "log.txt");
            assert_eq!(
                media_source.remote_url,
                "https://files.example.test/m/log.txt"
            );
            assert_eq!(media_source.local_uri, "/tmp/agent/files/log.txt");
            assert_eq!(media_source.size_bytes, 1024);
        }
        other => panic!("expected Upload::Completed, got {other:?}"),
    }
    match seen.next().unwrap() {
        ActivityEvent::Upload(xacpp::wire::events::UploadEvent::Error { name, error }) => {
            assert_eq!((name.as_str(), error.as_str()), ("other.bin", "disk full"));
        }
        other => panic!("expected Upload::Error, got {other:?}"),
    }

    // Turn projection untouched: the text block is the only content, no
    // upload artifact leaked into it.
    let turn = &consumer.model.get_turns()[0];
    assert_eq!(turn.contents.len(), 1);
    assert_eq!(turn.contents[0].text, "uploading");
    assert!(turn.contents[0].media.is_empty());
    assert!(turn.tools.is_empty());
    assert_eq!(turn.status, xacpp_activity::TurnStatus::Finalized);
}

/// Scenario 3: a send_file tool result keeps its pointer-credential
/// parts verbatim in the tool card — the app dereferences the pointers
/// itself; the projection must not drop or rewrite them.
#[test]
fn send_file_result_parts_keep_pointer_credentials() {
    let mut consumer = Consumer::new();

    consumer.on_wire_event(
        "start",
        json!({"inputs": [{"type": "text", "text": "send the png to the user"}]}),
    );
    // Open the streaming block first: the turn's active pair must exist
    // before tool_use registers its route.
    consumer.on_wire_event(
        "content_start",
        json!({"partId": "live-c0", "round": "r1", "pair": "p1"}),
    );
    consumer.on_wire_event(
        "content_delta",
        json!({"round": "r1", "pair": "p1", "payload": {"type": "text", "text": "on it", "partId": "live-c0"}}),
    );
    consumer.on_wire_event(
        "tool_use",
        json!({"requestId": "req-1", "toolName": "send_file", "index": 0, "arguments": "{\"path\":\"/tmp/out.png\"}", "intent": "deliver file"}),
    );
    consumer.on_wire_event(
        "tool_result",
        json!({
            "requestId": "req-1",
            "toolName": "send_file",
            "index": 0,
            "auto": true,
            "parts": [image_pointer_part(), {"type": "text", "text": "file delivered"}],
            "isError": false
        }),
    );
    consumer.on_wire_event(
        "pair_complete",
        json!({"pair": "p1", "contextWindow": 200000, "tokenUsage": {"id": "u1", "message": "m1", "promptTokens": 1, "completionTokens": 1, "totalTokens": 2}}),
    );
    consumer.on_wire_event(
        "complete",
        json!({"round": "r1", "assistantReply": [{"type": "text", "text": "done"}], "canceled": false}),
    );

    let turn = &consumer.model.get_turns()[0];
    assert_eq!(turn.tools.len(), 1);
    let card = &turn.tools[0];
    assert_eq!(card.tool_name, "send_file");
    let result = card.result.as_ref().expect("result must be projected");
    assert!(result.auto, "auto authorization flag must round-trip");
    assert_eq!(result.parts.len(), 2);

    // Pointer credentials verbatim: URL / URI / sha256 / size / mime.
    expect_image_pointer(&result.parts[0]);
    match &result.parts[1] {
        ContentPart::Text(text) => assert_eq!(text.text, "file delivered"),
        other => panic!("expected trailing Text part, got {other:?}"),
    }
}

/// Scenario 4: fetch-transaction words (xacpp 1.1) are Response/command
/// domain, not event vocabulary. The wire gate must reject them loudly —
/// guaranteeing no fetch state can silently enter the projection through
/// the event channel (fetch visibility on the consumer side rides the
/// Response layer, which xaact does not consume).
#[test]
fn fetch_transaction_words_stay_out_of_event_vocabulary() {
    for word in ["fetch_ready", "fetch_abort", "fetch_pause", "fetch_resume"] {
        let wire = XacppEvent::new(word, json!({}));
        let err = ActivityEvent::from_xacpp_event(&wire)
            .err()
            .unwrap_or_else(|| panic!("`{word}` must not be event vocabulary"));
        match err {
            xacpp_activity::ActivityError::UnknownEvent(name) => assert_eq!(name, word),
            other => panic!("expected UnknownEvent for `{word}`, got {other:?}"),
        }
    }
}
