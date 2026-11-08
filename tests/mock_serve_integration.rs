//! Real-TCP integration: xaact consumer ↔ xacpp-mock-serve.
//!
//! The mock serve emits real wire bytes over a real TCP connection; the
//! xaact consumer (the exact shape xamobile/agentx keep) ingests them
//! through the standard chain — `XacppSessionHandler::on_event` →
//! `ActivityEvent::from_xacpp_event` → `runtime.dispatch` →
//! `model.apply_envelope` — and the projection is asserted end-to-end.
//!
//! Scenarios (the file-transfer projection paths, now against real bytes):
//! 1. media push — the scripted media step (content_part text pointer +
//!    content_part image part, FileRef resolved from the actual resource
//!    bytes) projects into block media with metadata identical to the
//!    announced resource; the FileRef uri pulls the exact bytes back.
//! 2. mixed stream — an invoked turn mixing content_delta text and
//!    content_part pointer/image parts projects in order, text intact,
//!    media in their own blocks, turn finalized.
//! 3. upload side channel — real-wire `upload` progress/completed/error
//!    events stay verbatim in side_events with the turn untouched.
//! 4. anchored history — after a real disconnect/reconnect (same session
//!    object + matching credentials), `afterSeqInTopic` returns exactly
//!    the store window after the watermark and the projection converges
//!    (LatestWindow authoritative replacement).
//!
//! Connection order discipline (A-line finding): the session object is
//! created before the peer binds it and `peer.connect()` drives the wire —
//! the responder-side `session.wire(transport)` happens inside its
//! establish handler. Never invert wire/connect.

use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use xacpp::carrier::XacppTransport;
use xacpp::carrier::socket::SocketTransport;
use xacpp::ext::fetch::FetchPuller;
use xacpp::session::XacppSession;
use xacpp::session::handler::{EstablishDecision, EstablishHandler, XacppSessionHandler};
use xacpp::session::peer::XacppPeer;
use xacpp::wire::commands::XacppCommand;
use xacpp::wire::error::XacppError;
use xacpp::wire::events::XacppActivityEvent;
use xacpp::wire::message::XacppResponse;

use xacpp_activity::{
    ActivityEvent, ActivityEventCategory, ActivityModel, ActivityRuntime, RenderEntry,
    SnapshotItem, TurnStatus,
};
use xacpp_mock_serve::{MockScript, MockServe, ServeState};

// ------------------------------------------------------------------ glue

/// The xaact consumer device over a live session: every wire event is fed
/// through the standard conversion chain. Conversion failures are recorded
/// (and asserted empty at the end) instead of aborting the handler task —
/// a panic inside the session task would tear down the runtime with an
/// unrelated stack trace.
struct LiveConsumer {
    runtime: Mutex<ActivityRuntime>,
    model: Mutex<ActivityModel>,
    /// `(name, data)` in wire arrival order — the event-stream assertion face.
    observed: Mutex<Vec<(String, Value)>>,
    /// Conversion failures (`from_xacpp_event` rejections) — must stay empty.
    errors: Mutex<Vec<String>>,
}

impl LiveConsumer {
    fn new(activity_id: &str) -> Arc<Self> {
        Arc::new(Self {
            runtime: Mutex::new(ActivityRuntime::new(activity_id)),
            model: Mutex::new(ActivityModel::new()),
            observed: Mutex::new(Vec::new()),
            errors: Mutex::new(Vec::new()),
        })
    }

    async fn snapshot_model(&self) -> ActivityModel {
        self.model.lock().await.clone()
    }

    async fn observed(&self) -> Vec<(String, Value)> {
        self.observed.lock().await.clone()
    }

    async fn errors(&self) -> Vec<String> {
        self.errors.lock().await.clone()
    }

    /// True once an event with the given name arrived (optionally matched
    /// against a payload predicate).
    async fn saw(&self, name: &str, predicate: impl Fn(&Value) -> bool) -> bool {
        self.observed
            .lock()
            .await
            .iter()
            .any(|(n, d)| n == name && predicate(d))
    }
}

#[async_trait::async_trait]
impl XacppSessionHandler for LiveConsumer {
    async fn on_command(&self, _c: XacppCommand) -> Result<XacppResponse, XacppError> {
        Ok(XacppResponse::acknowledge())
    }

    async fn on_event(&self, e: XacppActivityEvent) -> Result<XacppResponse, XacppError> {
        self.observed
            .lock()
            .await
            .push((e.event.name.clone(), e.event.data.clone()));
        match ActivityEvent::from_xacpp_event(&e.event) {
            Ok(typed) => {
                let mut runtime = self.runtime.lock().await;
                let mut model = self.model.lock().await;
                let envelope = runtime.dispatch(ActivityEventCategory::Default, typed);
                model.apply_envelope(&envelope);
            }
            Err(err) => {
                self.errors
                    .lock()
                    .await
                    .push(format!("{}: {err}", e.event.name));
            }
        }
        Ok(XacppResponse::acknowledge())
    }
}

// ------------------------------------------------------- serve/client rig

/// Client accepts the full scripted vocabulary: the protocol layer gates
/// outgoing serve events on (serve produce ∩ client accept), so a narrow
/// accept list would starve scenarios that push other names.
fn client_capabilities() -> xacpp::wire::capability::Capabilities {
    xacpp::wire::capability::Capabilities {
        commands: vec![],
        produce_events: vec![],
        accept_events: xacpp_mock_serve::script::EVENT_NAMES
            .iter()
            .map(|name| json!({ "name": name }))
            .collect(),
        fetch_ext: Some(xacpp::wire::capability::FETCH_EXT_VERSION),
    }
}

struct NoopEstablish;

#[async_trait::async_trait]
impl EstablishHandler for NoopEstablish {
    async fn on_establish(
        &self,
        _transport: Arc<dyn XacppTransport>,
        _credentials: Option<String>,
    ) -> Result<EstablishDecision, XacppError> {
        unreachable!("client never receives establish requests in these tests");
    }

    async fn on_establish_confirm(
        &self,
        _transport: Arc<dyn XacppTransport>,
    ) -> Result<(String, Arc<dyn XacppSessionHandler>, String), XacppError> {
        unreachable!("challenge path unsupported");
    }
}

/// Client peer over an explicit session object (None = fresh). The session
/// is created BEFORE the peer binds it and `connect()` drives the wire —
/// reconnect paths pass the SAME held session (resume by construction).
async fn connect_client(
    addr: std::net::SocketAddr,
    held: Option<Arc<XacppSession>>,
) -> (XacppPeer, Arc<XacppSession>) {
    let transport: Arc<dyn XacppTransport> =
        Arc::new(SocketTransport::connect_to(addr.to_string()));
    let session = held.unwrap_or_else(|| Arc::new(XacppSession::new(None)));
    let peer = XacppPeer::new(
        client_capabilities(),
        transport,
        Some(Arc::clone(&session)),
        Arc::new(xacpp_mock_serve::serve::MockNegotiateHandler),
        Arc::new(NoopEstablish),
    );
    peer.connect().await.expect("客户端 TCP 连接");
    (peer, session)
}

async fn establish(
    peer: &XacppPeer,
    handler: Arc<dyn XacppSessionHandler>,
    credentials: Option<String>,
) -> Arc<XacppSession> {
    peer.establish(credentials, handler, |_| Ok(()))
        .await
        .expect("establish 握手")
}

async fn start_serve(script_json: &str) -> (MockServe, ServeState) {
    let script = MockScript::parse(script_json).expect("内置脚本必须合法");
    let state = ServeState::with_options(
        "mock-agent".into(),
        script.activities,
        None,
        script.fetch_resources,
        None,
    );
    let serve = MockServe::start(state.clone(), 0)
        .await
        .expect("serve 启动");
    (serve, state)
}

async fn wait_until(mut f: impl AsyncFnMut() -> bool) -> bool {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let ok = f().await;
        if ok || std::time::Instant::now() >= deadline {
            return ok;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

/// Writes a deterministic fixture file (pattern bytes) to the temp dir.
fn write_fixture_file(tag: &str, len: usize) -> (std::path::PathBuf, Vec<u8>) {
    let bytes = xacpp_mock_serve::script::pattern_bytes(len as u64);
    let path = std::env::temp_dir().join(format!(
        "xaact-integ-{tag}-{}-{:#x}.bin",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::write(&path, &bytes).expect("写夹具文件");
    (path, bytes)
}

/// One `conversation_history` response window (wire shapes mirrored).
struct HistoryWindow {
    events: Vec<xacpp::wire::events::XacppEvent>,
    next_before_seq_in_topic: Option<i64>,
    has_more: bool,
    latest_seq_in_topic: Option<i64>,
}

async fn pull_history(
    session: &Arc<XacppSession>,
    activity: &str,
    after_seq_in_topic: Option<i64>,
) -> HistoryWindow {
    let mut args = json!({ "activityId": activity });
    if let Some(watermark) = after_seq_in_topic {
        args["afterSeqInTopic"] = json!(watermark);
    }
    let resp = session
        .request_command(XacppCommand::generic("conversation_history", args))
        .await
        .expect("conversation_history 应答");
    let XacppResponse::Generic { name, data } = resp else {
        panic!("预期 conversation_history Generic 响应，实际 {resp:?}");
    };
    assert_eq!(name, "conversation_history");
    HistoryWindow {
        events: serde_json::from_value(data.get("events").cloned().unwrap_or(json!([])))
            .expect("events 数组"),
        next_before_seq_in_topic: data.get("nextBeforeSeqInTopic").and_then(|v| v.as_i64()),
        has_more: data
            .get("hasMore")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        latest_seq_in_topic: data.get("latestSeqInTopic").and_then(|v| v.as_i64()),
    }
}

fn token_usage() -> Value {
    json!({"id": "u1", "message": "m1", "promptTokens": 1, "completionTokens": 1, "totalTokens": 2})
}

async fn assert_no_conversion_errors(consumer: &LiveConsumer) {
    let errors = consumer.errors().await;
    assert!(
        errors.is_empty(),
        "wire 事件必须全部通过词汇转换，实际失败: {errors:?}"
    );
}

// ---------------------------------------------------------------- tests

/// Scenario 1 — media push: the scripted media step (text pointer part +
/// image part, FileRef resolved from the actual bytes) rides real TCP into
/// the xaact projection; block media carries the announced metadata, and
/// the FileRef uri pulls the exact resource bytes back.
#[tokio::test(flavor = "multi_thread")]
async fn s1_media_push_projects_fileref_metadata() {
    let (photo_path, photo_bytes) = write_fixture_file("s1", 120_000);
    let script = format!(
        r#"
{{
  "activities": [{{
    "activityId": "s1-act",
    "title": "媒体推送活动",
    "steps": [
      {{"delayMs": 0, "seq": 1, "event": {{"name": "start", "data": {{"inputs": [{{"type": "text", "text": "给我看图表"}}]}}}}}},
      {{"delayMs": 40, "seq": 2, "event": {{"name": "content_start", "data": {{"partId": "s0", "round": "r1", "pair": "p1"}}}}}},
      {{"delayMs": 40, "seq": 3, "event": {{"name": "content_delta", "data": {{"round": "r1", "pair": "p1", "payload": {{"type": "text", "text": "生成中", "partId": "s0"}}}}}}}},
      {{"type": "media", "delayMs": 60, "activityId": "s1-act",
        "uri": "mock://file/photo.png", "round": "r1", "pair": "p1",
        "text": "请查看截图"}},
      {{"delayMs": 150, "seq": 4, "event": {{"name": "pair_complete", "data": {{"pair": "p1", "contextWindow": 200000, "tokenUsage": {{"id": "u1", "message": "m1", "promptTokens": 1, "completionTokens": 1, "totalTokens": 2}}}}}}}},
      {{"delayMs": 40, "seq": 5, "event": {{"name": "complete", "data": {{"round": "r1", "assistantReply": [{{"type": "text", "text": "done"}}], "canceled": false}}}}}}
    ]
  }}],
  "fetchResources": [
    {{"uri": "mock://file/photo.png", "mime": "image/png", "path": "{}"}}
  ]
}}
"#,
        photo_path.display()
    );
    let (serve, _state) = start_serve(&script).await;

    let consumer = LiveConsumer::new("s1-act");
    let (peer, _session) = connect_client(serve.addr, None).await;
    peer.negotiate().await.unwrap();
    let session = establish(&peer, Arc::clone(&consumer) as _, Some("integ-s1".into())).await;

    // The media step lands: text pointer part, then the image part; the
    // scripted tail finalizes the turn.
    let arrived = wait_until(|| async { consumer.saw("complete", |_| true).await }).await;
    assert!(arrived, "媒体步骤+收尾应推完（至 complete）");

    let model = consumer.snapshot_model().await;
    assert_eq!(model.get_turns().len(), 1, "恰好一个 turn（pair p1）");
    let turn = &model.get_turns()[0];
    assert_eq!(turn.id, "p1");
    assert_eq!(turn.status, TurnStatus::Finalized);

    // The mock's media parts carry no partId: each is a full-snapshot
    // replace of the last streaming block — the image part wins, so the
    // block holds ONLY the media (text cleared by the snapshot semantics).
    assert_eq!(turn.contents.len(), 1, "media part 快照替换后单块");
    let block = &turn.contents[0];
    assert_eq!(block.text, "", "image 快照替换清空文本");
    assert_eq!(block.media.len(), 1, "恰好一个媒体 part");

    // FileRef metadata identical to the announced (real) resource.
    let xacpp::wire::events::ContentPart::Image(image) = &block.media[0] else {
        panic!("预期 Image part，实际 {:?}", block.media[0]);
    };
    assert_eq!(image.source.local_uri, "mock://file/photo.png");
    assert_eq!(image.source.mime_type, "image/png");
    assert_eq!(image.source.size_bytes, photo_bytes.len() as u64);
    assert_eq!(image.source.sha256, sha256_hex(&photo_bytes));

    // End-to-end closure: pull by the projected FileRef uri — the bytes
    // verify against the metadata the projection carries.
    let puller = FetchPuller::new(Arc::clone(&session)).await;
    let (got, meta) = puller.pull_to_vec("mock://file/photo.png").await.unwrap();
    assert_eq!(got, photo_bytes, "按投影 FileRef uri 拉回真实字节");
    assert_eq!(meta.sha256, image.source.sha256, "拉取元数据与投影一致");
    assert_eq!(meta.size_bytes, image.source.size_bytes);

    assert_no_conversion_errors(&consumer).await;
    let _ = serve.stop().await;
}

/// Scenario 2 — mixed stream: an invoked turn mixing two text deltas and
/// two content_parts (text pointer + image, both with partId → implicit
/// snapshot blocks) projects in order: text intact, pointer text its own
/// block, image block carrying the verbatim FileRef; turn finalized.
#[tokio::test(flavor = "multi_thread")]
async fn s2_mixed_stream_invoke_projection_order() {
    let script = format!(
        r#"
{{
  "activities": [{{
    "activityId": "s2-act",
    "title": "混合流活动",
    "invokeSteps": [
      {{"delayMs": 30, "seq": 2, "event": {{"name": "content_start", "data": {{"partId": "c0", "round": "r1", "pair": "p1"}}}}}},
      {{"delayMs": 30, "seq": 3, "event": {{"name": "content_delta", "data": {{"round": "r1", "pair": "p1", "payload": {{"type": "text", "text": "查询结果：42 件。", "partId": "c0"}}}}}}}},
      {{"delayMs": 30, "seq": 4, "event": {{"name": "content_delta", "data": {{"round": "r1", "pair": "p1", "payload": {{"type": "text", "text": "图表如下。", "partId": "c0"}}}}}}}},
      {{"delayMs": 30, "seq": 5, "event": {{"name": "content_part", "data": {{"round": "r1", "pair": "p1", "payload": {{"type": "text", "text": "附件：inventory.csv", "partId": "ptr-1", "partType": "default"}}}}}}}},
      {{"delayMs": 30, "seq": 6, "event": {{"name": "content_part", "data": {{"round": "r1", "pair": "p1", "payload": {{"type": "image", "source": {{"remoteUrl": "https://files.example.test/m/inv.png", "localUri": "mock://file/inv.png", "mimeType": "image/png", "requireOrganized": false, "sizeBytes": 20480, "sha256": "b6f9c1d2e3a4f5061728394a5b6c7d8e9f0a1b2c3d4e5f60718293a4b5c6d7e8"}}, "partId": "img-1", "partType": "default"}}}}}}}},
      {{"delayMs": 30, "seq": 7, "event": {{"name": "pair_complete", "data": {{"pair": "p1", "contextWindow": 200000, "tokenUsage": {token_usage}}}}}}},
      {{"delayMs": 30, "seq": 8, "event": {{"name": "complete", "data": {{"round": "r1", "assistantReply": [{{"type": "text", "text": "done"}}], "canceled": false}}}}}}
    ]
  }}]
}}
"#,
        token_usage = token_usage()
    );
    let (serve, _state) = start_serve(&script).await;

    let consumer = LiveConsumer::new("s2-act");
    let (peer, _session) = connect_client(serve.addr, None).await;
    peer.negotiate().await.unwrap();
    let session = establish(&peer, Arc::clone(&consumer) as _, Some("integ-s2".into())).await;

    // Drive the scripted invoke timeline over the real command face.
    let resp = session
        .request_command(XacppCommand::generic(
            "invoke_activity",
            json!({"activity": "s2-act", "messages": [{"type": "text", "text": "查库存并画图"}]}),
        ))
        .await
        .expect("invoke_activity 应答");
    assert!(
        matches!(resp, XacppResponse::Generic { ref name, .. } if name == "acknowledge"),
        "invoke_activity 应答 acknowledge，实际 {resp:?}"
    );

    let arrived = wait_until(|| async { consumer.saw("complete", |_| true).await }).await;
    assert!(arrived, "invoke 时间线应推完整个 turn（至 complete）");

    // Wire arrival order: the injected start (from the invoke payload) then
    // the seven scripted events, in order.
    let observed = consumer.observed().await;
    let names: Vec<&str> = observed.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(
        names,
        vec![
            "start",
            "content_start",
            "content_delta",
            "content_delta",
            "content_part",
            "content_part",
            "pair_complete",
            "complete"
        ],
        "线上事件顺序必须与脚本一致"
    );

    let model = consumer.snapshot_model().await;
    assert_eq!(model.get_turns().len(), 1);
    let turn = &model.get_turns()[0];
    assert_eq!(turn.id, "p1");
    assert_eq!(turn.status, TurnStatus::Finalized);

    // Projection order: streaming text block, pointer-text block, image block.
    assert_eq!(turn.contents.len(), 3, "文本块 + 指针文本块 + 图像块");
    assert_eq!(
        turn.contents[0].text, "查询结果：42 件。图表如下。",
        "两段 delta 按序完整拼接"
    );
    assert!(turn.contents[0].media.is_empty(), "文本块零媒体污染");

    assert_eq!(turn.contents[1].text, "附件：inventory.csv");
    assert!(turn.contents[1].media.is_empty());

    assert_eq!(turn.contents[2].text, "");
    assert_eq!(turn.contents[2].media.len(), 1);
    let xacpp::wire::events::ContentPart::Image(image) = &turn.contents[2].media[0] else {
        panic!("预期 Image part，实际 {:?}", turn.contents[2].media[0]);
    };
    assert_eq!(
        image.source.remote_url,
        "https://files.example.test/m/inv.png"
    );
    assert_eq!(image.source.local_uri, "mock://file/inv.png");
    assert_eq!(image.source.size_bytes, 20480);
    assert_eq!(
        image.source.sha256,
        "b6f9c1d2e3a4f5061728394a5b6c7d8e9f0a1b2c3d4e5f60718293a4b5c6d7e8"
    );

    assert_no_conversion_errors(&consumer).await;
    let _ = serve.stop().await;
}

/// Scenario 3 — upload side channel: real-wire upload
/// progress/progress/completed/error events stay verbatim in side_events
/// (in arrival order) while the turn projection stays untouched.
#[tokio::test(flavor = "multi_thread")]
async fn s3_upload_side_channel_over_real_wire() {
    let script = format!(
        r#"
{{
  "activities": [{{
    "activityId": "s3-act",
    "title": "上传活动",
    "steps": [
      {{"delayMs": 0, "seq": 1, "event": {{"name": "start", "data": {{"inputs": [{{"type": "text", "text": "上传日志"}}]}}}}}},
      {{"delayMs": 30, "seq": 2, "event": {{"name": "content_start", "data": {{"partId": "u0", "round": "r1", "pair": "p1"}}}}}},
      {{"delayMs": 30, "seq": 3, "event": {{"name": "content_delta", "data": {{"round": "r1", "pair": "p1", "payload": {{"type": "text", "text": "正在上传日志", "partId": "u0"}}}}}}}},
      {{"delayMs": 30, "seq": 4, "event": {{"name": "upload", "data": {{"upload_event": "progress", "name": "log.txt", "uploaded": 512, "total": 1024}}}}}},
      {{"delayMs": 30, "seq": 5, "event": {{"name": "upload", "data": {{"upload_event": "progress", "name": "log.txt", "uploaded": 1024, "total": 1024}}}}}},
      {{"delayMs": 30, "seq": 6, "event": {{"name": "upload", "data": {{"upload_event": "completed", "name": "log.txt", "media_source": {{"remoteUrl": "https://files.example.test/m/log.txt", "localUri": "/tmp/agent/files/log.txt", "mimeType": "text/plain", "requireOrganized": false, "sizeBytes": 1024, "sha256": "aa11bb22cc33dd44ee55ff6600778899aabbccddeeff00112233445566778899"}}}}}}}},
      {{"delayMs": 30, "seq": 7, "event": {{"name": "upload", "data": {{"upload_event": "error", "name": "other.bin", "error": "disk full"}}}}}},
      {{"delayMs": 30, "seq": 8, "event": {{"name": "pair_complete", "data": {{"pair": "p1", "contextWindow": 200000, "tokenUsage": {token_usage}}}}}}},
      {{"delayMs": 30, "seq": 9, "event": {{"name": "complete", "data": {{"round": "r1", "assistantReply": [{{"type": "text", "text": "uploaded"}}], "canceled": false}}}}}}
    ]
  }}]
}}
"#,
        token_usage = token_usage()
    );
    let (serve, _state) = start_serve(&script).await;

    let consumer = LiveConsumer::new("s3-act");
    let (peer, _session) = connect_client(serve.addr, None).await;
    peer.negotiate().await.unwrap();
    let _session = establish(&peer, Arc::clone(&consumer) as _, Some("integ-s3".into())).await;

    let arrived = wait_until(|| async { consumer.saw("complete", |_| true).await }).await;
    assert!(arrived, "steps 时间线应推完（至 complete）");
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Side channel: four uploads verbatim, in wire order.
    let model = consumer.snapshot_model().await;
    let uploads: Vec<&ActivityEvent> = model
        .side_events
        .iter()
        .filter(|event| matches!(event, ActivityEvent::Upload(_)))
        .collect();
    assert_eq!(uploads.len(), 4, "progress×2 + completed + error");
    match uploads[0] {
        ActivityEvent::Upload(xacpp::wire::events::UploadEvent::Progress {
            name,
            uploaded,
            total,
        }) => assert_eq!((name.as_str(), *uploaded, *total), ("log.txt", 512, 1024)),
        other => panic!("预期 Upload::Progress，实际 {other:?}"),
    }
    match uploads[1] {
        ActivityEvent::Upload(xacpp::wire::events::UploadEvent::Progress { uploaded, .. }) => {
            assert_eq!(*uploaded, 1024)
        }
        other => panic!("预期第二笔 Upload::Progress，实际 {other:?}"),
    }
    match uploads[2] {
        ActivityEvent::Upload(xacpp::wire::events::UploadEvent::Completed {
            name,
            media_source,
        }) => {
            assert_eq!(name, "log.txt");
            assert_eq!(
                media_source.remote_url,
                "https://files.example.test/m/log.txt"
            );
            assert_eq!(media_source.size_bytes, 1024);
        }
        other => panic!("预期 Upload::Completed，实际 {other:?}"),
    }
    match uploads[3] {
        ActivityEvent::Upload(xacpp::wire::events::UploadEvent::Error { name, error }) => {
            assert_eq!((name.as_str(), error.as_str()), ("other.bin", "disk full"))
        }
        other => panic!("预期 Upload::Error，实际 {other:?}"),
    }

    // Turn projection untouched by the side channel.
    assert_eq!(model.get_turns().len(), 1);
    let turn = &model.get_turns()[0];
    assert_eq!(turn.contents.len(), 1);
    assert_eq!(turn.contents[0].text, "正在上传日志");
    assert!(turn.contents[0].media.is_empty());
    assert!(turn.tools.is_empty());
    assert_eq!(turn.status, TurnStatus::Finalized);

    assert_no_conversion_errors(&consumer).await;
    let _ = serve.stop().await;
}

/// Scenario 4 — anchored history across a real disconnect/reconnect: the
/// first window covers the whole scripted store; after reconnecting the
/// SAME session with matching credentials, `afterSeqInTopic` returns
/// exactly the window after the watermark and the LatestWindow projection
/// converges to the authoritative snapshot.
#[tokio::test(flavor = "multi_thread")]
async fn s4_anchored_history_after_reconnect() {
    let script = format!(
        r#"
{{
  "activities": [{{
    "activityId": "s4-act",
    "title": "历史补齐活动",
    "historyStore": [
      {{"seq": 1, "event": {{"name": "start", "data": {{"inputs": [{{"type": "text", "text": "第一轮"}}]}}}}}},
      {{"seq": 2, "event": {{"name": "content_start", "data": {{"partId": "h0", "round": "r1", "pair": "p1"}}}}}},
      {{"seq": 3, "event": {{"name": "content_delta", "data": {{"round": "r1", "pair": "p1", "payload": {{"type": "text", "text": "历史回复一", "partId": "h0"}}}}}}}},
      {{"seq": 4, "event": {{"name": "pair_complete", "data": {{"pair": "p1", "contextWindow": 200000, "tokenUsage": {token_usage}}}}}}},
      {{"seq": 5, "event": {{"name": "start", "data": {{"inputs": [{{"type": "text", "text": "第二轮"}}]}}}}}},
      {{"seq": 6, "event": {{"name": "content_start", "data": {{"partId": "h1", "round": "r2", "pair": "p2"}}}}}},
      {{"seq": 7, "event": {{"name": "content_delta", "data": {{"round": "r2", "pair": "p2", "payload": {{"type": "text", "text": "历史回复二", "partId": "h1"}}}}}}}},
      {{"seq": 8, "event": {{"name": "pair_complete", "data": {{"pair": "p2", "contextWindow": 200000, "tokenUsage": {token_usage}}}}}}},
      {{"seq": 9, "event": {{"name": "start", "data": {{"inputs": [{{"type": "text", "text": "第三轮"}}]}}}}}},
      {{"seq": 10, "event": {{"name": "content_start", "data": {{"partId": "h2", "round": "r3", "pair": "p3"}}}}}}
    ]
  }}]
}}
"#,
        token_usage = token_usage()
    );
    let (serve, _state) = start_serve(&script).await;

    // --- connection 1: full window through the anchored fetch ---
    let consumer = LiveConsumer::new("s4-act");
    let (peer1, session) = connect_client(serve.addr, None).await;
    peer1.negotiate().await.unwrap();
    let established = establish(&peer1, Arc::clone(&consumer) as _, Some("integ-s4".into())).await;
    let credentials = established.credentials().await;
    assert!(!credentials.is_empty(), "首连应获得凭据");

    // Absent cursor = paging mode (empty page here); the anchored fetch
    // needs the explicit watermark: afterSeqInTopic=0 → the whole store.
    let full = pull_history(&session, "s4-act", Some(0)).await;
    assert_eq!(full.events.len(), 10, "afterSeqInTopic=0=全窗口");
    assert_eq!(full.latest_seq_in_topic, Some(10));
    assert_eq!(full.next_before_seq_in_topic, Some(1));
    assert!(full.has_more);

    let mut model = ActivityModel::new();
    model.apply_history_events(full.events.iter().cloned());
    // Authoritative snapshot: three user bubbles + three turn entries in
    // store order; p1/p2 finalized with their texts, p3 still streaming.
    assert_eq!(
        render_ids(&model),
        vec![
            RenderId::User("第一轮".into()),
            RenderId::Turn("p1".into()),
            RenderId::User("第二轮".into()),
            RenderId::Turn("p2".into()),
            RenderId::User("第三轮".into()),
            RenderId::Turn("p3".into()),
        ],
        "全窗口投影渲染序"
    );
    assert_eq!(model.get_turns().len(), 3);
    assert_eq!(model.get_turns()[0].id, "p1");
    assert_eq!(model.get_turns()[0].contents[0].text, "历史回复一");
    assert_eq!(model.get_turns()[0].status, TurnStatus::Finalized);
    assert_eq!(model.get_turns()[1].id, "p2");
    assert_eq!(model.get_turns()[1].contents[0].text, "历史回复二");
    assert_eq!(model.get_turns()[1].status, TurnStatus::Finalized);
    assert_eq!(model.get_turns()[2].id, "p3");
    assert_eq!(model.get_turns()[2].status, TurnStatus::Streaming);

    // --- real disconnect ---
    peer1.disconnect().await.expect("conn 1 断连");
    tokio::time::sleep(Duration::from_millis(150)).await;

    // --- connection 2: SAME session + matching credentials (reuse path) ---
    let (peer2, rebound) = connect_client(serve.addr, Some(Arc::clone(&session))).await;
    peer2.negotiate().await.unwrap();
    let re_established = establish(&peer2, Arc::clone(&consumer) as _, Some(credentials)).await;
    assert!(
        Arc::ptr_eq(&rebound, &re_established),
        "客户端持有的 session 与重连 establish 返回的是同一对象"
    );

    // Local watermark: the consumer applied everything through seq 6
    // (p2's content_start). The anchored fetch returns exactly the
    // window after the watermark, untruncated.
    let delta = pull_history(&session, "s4-act", Some(6)).await;
    let names: Vec<&str> = delta.events.iter().map(|e| e.name.as_str()).collect();
    assert_eq!(
        names,
        vec!["content_delta", "pair_complete", "start", "content_start"],
        "增量窗口=seq7..10，与 store 尾部逐条一致"
    );
    assert_eq!(
        delta.events[0]
            .data
            .get("payload")
            .and_then(|p| p.get("text"))
            .and_then(|t| t.as_str()),
        Some("历史回复二"),
        "增量首条是 p2 的回复文本"
    );
    assert_eq!(delta.next_before_seq_in_topic, Some(7));
    assert_eq!(delta.latest_seq_in_topic, Some(10));
    assert!(delta.has_more);

    // Reconnect recovery (the new-architecture form): the projection is
    // rebuilt from a fresh authoritative window — the re-pulled full store
    // converges to the identical projection, p2 finalized, p3 still
    // streaming. (Incremental identity-merge into the old model is retired:
    // the latest-window entry REPLACES the projection.)
    let rewindow = pull_history(&session, "s4-act", Some(0)).await;
    assert_eq!(rewindow.events.len(), 10, "重连后全窗口仍完整可得");
    let mut rebuilt = ActivityModel::new();
    rebuilt.apply_history_events(rewindow.events.iter().cloned());
    assert_eq!(
        render_ids(&rebuilt),
        vec![
            RenderId::User("第一轮".into()),
            RenderId::Turn("p1".into()),
            RenderId::User("第二轮".into()),
            RenderId::Turn("p2".into()),
            RenderId::User("第三轮".into()),
            RenderId::Turn("p3".into()),
        ],
        "重连后全窗口重建：投影与首拉一致，无丢失"
    );
    // Data face: the rebuilt projection completed p2 and keeps p3
    // streaming, exactly like the first pull.
    let turns = rebuilt.get_turns();
    let p2 = turns
        .iter()
        .find(|turn| turn.id == "p2")
        .expect("p2 应在重建投影中完成");
    assert_eq!(p2.contents[0].text, "历史回复二");
    assert_eq!(p2.status, TurnStatus::Finalized);
    let p3 = turns
        .iter()
        .find(|turn| turn.id == "p3")
        .expect("p3 应保留在飞");
    assert_eq!(p3.status, TurnStatus::Streaming);

    assert_no_conversion_errors(&consumer).await;
    let _ = serve.stop().await;
}

// ------------------------------------------------------- render-order face

/// Compact render-entry identity for ordering assertions.
#[derive(Debug, PartialEq, Eq)]
enum RenderId {
    User(String),
    Turn(String),
}

fn render_ids(model: &ActivityModel) -> Vec<RenderId> {
    let mut out: Vec<RenderId> = Vec::new();
    for entry in &model.entries {
        match entry {
            RenderEntry::User { inputs, .. } => {
                out.push(RenderId::User(xacpp_activity::inputs_text(inputs)))
            }
            RenderEntry::Think { pair, .. } | RenderEntry::Content { pair, .. } => {
                let id = RenderId::Turn(pair.clone());
                if out.last() != Some(&id) {
                    out.push(id);
                }
            }
            RenderEntry::Tool(_) => {}
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Reducer end-to-end: one event stream, identity-addressed merge. Real
// wire bytes from the mock serve → the standard consumer chain → snapshot
// assertions. The script JSON carries the strict 1.2 members (`status`/
// `isError`/`auto` on tool_result, `canceled` on complete) verbatim — the
// mock never rewrites `data`.
// ---------------------------------------------------------------------------

/// E1 — no-reasoning turn through the live stream: a ToolUse with no block
/// events before it (the historical active-pair guard discarded it, leaving
/// an orphan empty card) opens a normal card; the ToolResult lands on the
/// same card.
#[tokio::test(flavor = "multi_thread")]
async fn e1_no_reasoning_turn_normal_card() {
    let script = format!(
        r#"
{{
  "activities": [{{
    "activityId": "e1-act",
    "title": "无 reasoning 轮",
    "invokeSteps": [
      {{"delayMs": 30, "seq": 1, "event": {{"name": "start", "data": {{"inputs": [{{"type": "text", "text": "跑个命令"}}]}}}}}},
      {{"delayMs": 30, "seq": 2, "event": {{"name": "tool_use", "data": {{"requestId": "rq-e1", "toolName": "shell", "index": 0, "arguments": "{{}}", "intent": "取证"}}}}}},
      {{"delayMs": 30, "seq": 3, "event": {{"name": "pair_complete", "data": {{"pair": "pe1", "contextWindow": 200000, "tokenUsage": {token}}}}}}},
      {{"delayMs": 30, "seq": 4, "event": {{"name": "tool_result", "data": {{"requestId": "rq-e1", "toolName": "", "index": 0, "parts": [{{"type": "text", "text": "命令输出"}}], "status": "completed", "isError": false, "auto": false}}}}}},
      {{"delayMs": 30, "seq": 5, "event": {{"name": "complete", "data": {{"round": "r1", "assistantReply": [], "canceled": false}}}}}}
    ]
  }}]
}}
"#,
        token = token_usage()
    );
    let (serve, _state) = start_serve(&script).await;
    let consumer = LiveConsumer::new("e1-act");
    let (peer, session) = connect_client(serve.addr, None).await;
    peer.negotiate().await.unwrap();
    establish(&peer, Arc::clone(&consumer) as _, Some("integ-e1".into())).await;
    session
        .request_command(XacppCommand::generic(
            "invoke_activity",
            json!({"activity": "e1-act", "messages": []}),
        ))
        .await
        .expect("invoke_activity 应答");

    let arrived = wait_until(|| async { consumer.saw("complete", |_| true).await }).await;
    assert!(arrived, "invokeSteps 应推完");

    let model = consumer.snapshot_model().await;
    let cards: Vec<(String, String, Option<bool>)> = model
        .snapshot()
        .iter()
        .filter_map(|item| match item {
            SnapshotItem::Tool {
                request_id,
                tool_name,
                done,
                ..
            } => Some((request_id.clone(), tool_name.clone(), *done)),
            _ => None,
        })
        .collect();
    assert_eq!(
        cards,
        vec![("rq-e1".into(), "shell".into(), Some(true))],
        "无 reasoning 轮产正常卡：ToolUse 建卡 + ToolResult 命中同卡"
    );
    assert_no_conversion_errors(&consumer).await;
    let _ = serve.stop().await;
}

/// E2 — background card: live Running placeholder → live terminal report →
/// a pulled window still carrying only the Running placeholder arrives →
/// the completed state is kept (completion is never demoted).
#[tokio::test(flavor = "multi_thread")]
async fn e2_background_card_completion_survives_window() {
    let script = format!(
        r#"
{{
  "activities": [{{
    "activityId": "e2-act",
    "title": "后台卡",
    "invokeSteps": [
      {{"delayMs": 30, "seq": 1, "event": {{"name": "tool_use", "data": {{"requestId": "rq-bg", "toolName": "shell", "index": 0, "arguments": "{{}}", "intent": "长任务"}}}}}},
      {{"delayMs": 30, "seq": 2, "event": {{"name": "tool_result", "data": {{"requestId": "rq-bg", "toolName": "shell", "index": 0, "parts": [{{"type": "text", "text": "已转后台"}}], "status": "running", "isBackground": true, "isError": false, "auto": false}}}}}},
      {{"delayMs": 30, "seq": 3, "event": {{"name": "tool_result", "data": {{"requestId": "rq-bg", "toolName": "shell", "index": 0, "parts": [{{"type": "text", "text": "后台完成输出"}}], "status": "completed", "isError": false, "auto": false}}}}}},
      {{"delayMs": 30, "seq": 4, "event": {{"name": "complete", "data": {{"round": "r1", "assistantReply": [], "canceled": false}}}}}}
    ]
  }}],
  "historyStore": [
    {{"seq": 1, "event": {{"name": "tool_use", "data": {{"requestId": "rq-bg", "toolName": "shell", "index": 0, "arguments": "{{}}", "intent": "长任务"}}}}}},
    {{"seq": 2, "event": {{"name": "pair_complete", "data": {{"pair": "pe2", "contextWindow": 200000, "tokenUsage": {token}}}}}}},
    {{"seq": 3, "event": {{"name": "tool_result", "data": {{"requestId": "rq-bg", "toolName": "shell", "index": 0, "parts": [{{"type": "text", "text": "已转后台"}}], "status": "running", "isBackground": true, "isError": false, "auto": false}}}}}}
  ]
}}
"#,
        token = token_usage()
    );
    let (serve, _state) = start_serve(&script).await;
    let consumer = LiveConsumer::new("e2-act");
    let (peer, session) = connect_client(serve.addr, None).await;
    peer.negotiate().await.unwrap();
    establish(&peer, Arc::clone(&consumer) as _, Some("integ-e2".into())).await;
    session
        .request_command(XacppCommand::generic(
            "invoke_activity",
            json!({"activity": "e2-act", "messages": []}),
        ))
        .await
        .expect("invoke_activity 应答");

    let arrived = wait_until(|| async { consumer.saw("complete", |_| true).await }).await;
    assert!(arrived, "live 终报应到达");

    let done_card = |model: &ActivityModel| {
        model
            .snapshot()
            .iter()
            .find_map(|item| match item {
                SnapshotItem::Tool {
                    request_id, done, ..
                } if request_id == "rq-bg" => Some(*done),
                _ => None,
            })
            .expect("后台卡在快照中")
    };
    let model = consumer.snapshot_model().await;
    assert_eq!(done_card(&model), Some(true), "live 终报置完成");

    // Pull the window (serve's store still carries only the Running
    // placeholder) → the completed card must not regress.
    let window = pull_history(&session, "e2-act", Some(0)).await;
    let mut model = model;
    model.apply_history_events(window.events.iter().cloned());
    assert_eq!(
        done_card(&model),
        Some(true),
        "拉取窗口（仅 Running 占位）不得回退完成态"
    );

    let _ = serve.stop().await;
}

/// E3 — cut-window orphan: the pulled window carries a ToolResult whose
/// ToolUse is not in the window (the projection of an isolated Tool
/// message) → an independent completed card keeps the data.
#[tokio::test(flavor = "multi_thread")]
async fn e3_cut_window_orphan_result_keeps_data() {
    let script: &str = r#"
{
  "activities": [{
    "activityId": "e3-act",
    "title": "切割窗口孤儿",
    "historyStore": [
      {"seq": 1, "event": {"name": "tool_result", "data": {"requestId": "rq-orphan", "toolName": "", "index": 0, "parts": [{"type": "text", "text": "孤儿结果文本"}], "status": "completed", "isError": false, "auto": false}}}
    ]
  }]
}
"#;
    let (serve, _state) = start_serve(script).await;
    let consumer = LiveConsumer::new("e3-act");
    let (peer, session) = connect_client(serve.addr, None).await;
    peer.negotiate().await.unwrap();
    establish(&peer, Arc::clone(&consumer) as _, Some("integ-e3".into())).await;

    let window = pull_history(&session, "e3-act", Some(0)).await;
    let mut model = consumer.snapshot_model().await;
    model.apply_history_events(window.events.iter().cloned());
    let orphan: Option<(String, Option<bool>, Option<String>)> =
        model.snapshot().iter().find_map(|item| match item {
            SnapshotItem::Tool {
                request_id,
                done,
                result_text,
                ..
            } => Some((request_id.clone(), *done, result_text.clone())),
            _ => None,
        });
    assert_eq!(
        orphan,
        Some(("rq-orphan".into(), Some(true), Some("孤儿结果文本".into()))),
        "切割窗口的孤儿 ToolResult 自建完成卡，数据不丢"
    );
    let _ = serve.stop().await;
}

/// E4 — pulled terminal state overrides the streaming intermediate: live
/// deltas accumulate; the pulled window carries sealed parts + the close →
/// terminal content wins and the turn closes.
#[tokio::test(flavor = "multi_thread")]
async fn e4_pulled_terminal_overrides_streaming() {
    let script = format!(
        r#"
{{
  "activities": [{{
    "activityId": "e4-act",
    "title": "终态覆盖",
    "invokeSteps": [
      {{"delayMs": 30, "seq": 1, "event": {{"name": "content_start", "data": {{"partId": "c0", "round": "r1", "pair": "pe4"}}}}}},
      {{"delayMs": 30, "seq": 2, "event": {{"name": "content_delta", "data": {{"round": "r1", "pair": "pe4", "payload": {{"type": "text", "text": "部分", "partId": "c0"}}}}}}}}
    ],
    "historyStore": [
      {{"seq": 1, "event": {{"name": "content_start", "data": {{"partId": "replay:c0", "round": "r1", "pair": "pe4"}}}}}},
      {{"seq": 2, "event": {{"name": "content_part", "data": {{"round": "r1", "pair": "pe4", "payload": {{"type": "text", "text": "窗口终态文本", "partId": "replay:c0"}}}}}}}},
      {{"seq": 3, "event": {{"name": "pair_complete", "data": {{"pair": "pe4", "contextWindow": 200000, "tokenUsage": {token}}}}}}}
    ]
  }}]
}}
"#,
        token = token_usage()
    );
    let (serve, _state) = start_serve(&script).await;
    let consumer = LiveConsumer::new("e4-act");
    let (peer, session) = connect_client(serve.addr, None).await;
    peer.negotiate().await.unwrap();
    establish(&peer, Arc::clone(&consumer) as _, Some("integ-e4".into())).await;
    session
        .request_command(XacppCommand::generic(
            "invoke_activity",
            json!({"activity": "e4-act", "messages": []}),
        ))
        .await
        .expect("invoke_activity 应答");

    let arrived = wait_until(|| async { consumer.saw("content_delta", |_| true).await }).await;
    assert!(arrived, "live 中间态应到达");

    let model = consumer.snapshot_model().await;
    assert!(model.turn_active(), "流式中间态：轮开着");

    let window = pull_history(&session, "e4-act", Some(0)).await;
    let mut model = model;
    model.apply_history_events(window.events.iter().cloned());
    let texts: Vec<String> = model
        .snapshot()
        .iter()
        .filter_map(|item| match item {
            SnapshotItem::Text { text, .. } => Some(text.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        texts,
        vec!["窗口终态文本".to_string()],
        "同身份 (pair,partId) 的拉取终态覆盖流式中间态"
    );
    assert!(!model.turn_active(), "窗口的 pair_complete 收轮");
    let _ = serve.stop().await;
}

/// E5 — re-entry / rebuild idempotence: the same window pulled twice and
/// rebuilt into fresh models produces byte-identical projections (the
/// latest-window entry is a deterministic rebuild — retrying it neither
/// duplicates nor drifts).
#[tokio::test(flavor = "multi_thread")]
async fn e5_same_window_twice_snapshot_unchanged() {
    let script = format!(
        r#"
{{
  "activities": [{{
    "activityId": "e5-act",
    "title": "重放幂等",
    "historyStore": [
      {{"seq": 1, "event": {{"name": "start", "data": {{"inputs": [{{"type": "text", "text": "第一问"}}]}}}}}},
      {{"seq": 2, "event": {{"name": "think_start", "data": {{"partId": "rt0", "round": "r1", "pair": "pe5"}}}}}},
      {{"seq": 3, "event": {{"name": "think_part", "data": {{"content": "推理完成", "round": "r1", "pair": "pe5", "partId": "rt0"}}}}}},
      {{"seq": 4, "event": {{"name": "think_end", "data": {{"partId": "rt0", "round": "r1", "pair": "pe5"}}}}}},
      {{"seq": 5, "event": {{"name": "content_start", "data": {{"partId": "rc0", "round": "r1", "pair": "pe5"}}}}}},
      {{"seq": 6, "event": {{"name": "content_part", "data": {{"round": "r1", "pair": "pe5", "payload": {{"type": "text", "text": "窗口答案", "partId": "rc0"}}}}}}}},
      {{"seq": 7, "event": {{"name": "tool_use", "data": {{"requestId": "rq-e5", "toolName": "shell", "index": 0, "arguments": "{{}}", "intent": "查证"}}}}}},
      {{"seq": 8, "event": {{"name": "pair_complete", "data": {{"pair": "pe5", "contextWindow": 200000, "tokenUsage": {token}}}}}}},
      {{"seq": 9, "event": {{"name": "tool_result", "data": {{"requestId": "rq-e5", "toolName": "", "index": 0, "parts": [{{"type": "text", "text": "查证结果"}}], "status": "completed", "isError": false, "auto": false}}}}}}
    ]
  }}]
}}
"#,
        token = token_usage()
    );
    let (serve, _state) = start_serve(&script).await;
    let consumer = LiveConsumer::new("e5-act");
    let (peer, session) = connect_client(serve.addr, None).await;
    peer.negotiate().await.unwrap();
    establish(&peer, Arc::clone(&consumer) as _, Some("integ-e5".into())).await;

    let first = pull_history(&session, "e5-act", Some(0)).await;
    let mut model = ActivityModel::new();
    model.apply_history_events(first.events.iter().cloned());
    let snapshot = model.snapshot();

    // Second consumption of the SAME window (retry / re-entry): a fresh
    // rebuild from the re-pulled window must converge to the identical
    // projection.
    let second = pull_history(&session, "e5-act", Some(0)).await;
    let mut rebuilt = ActivityModel::new();
    rebuilt.apply_history_events(second.events.iter().cloned());
    assert_eq!(
        snapshot,
        rebuilt.snapshot(),
        "同窗口二次拉取重建：投影逐字节一致"
    );
    let _ = serve.stop().await;
}
