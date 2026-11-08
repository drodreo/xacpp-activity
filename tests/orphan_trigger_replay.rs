//! Deterministic replay of real trajectory segments pinning down when
//! empty-tool-card orphan shapes arise.
//!
//! Source material: activity c42e21f0 (2026-10-24 15:1x, nine rapid tool
//! rounds, trajectory L1573-L1595), device copy at /tmp/c42-copy.jsonl.
//! Copy shape: calls 2/3/7/9 produced orphan-shaped cards (tool_name='',
//! intent='', done=true, result_text intact); the rest are normal.
//!
//! Historical diagnosis (pre-rewrite, mechanism since retired): the orphan
//! branch fired when a ToolResult arrived with no request_id in routing;
//! the on-device cause was assistant without reasoning -> projection emits
//! no ThinkStart -> in-batch ToolUse dropped on active_pair=None ->
//! ToolResult took the orphan branch. **That drop path was removed in the
//! identity-merge rewrite** (ToolUse has no drop branch; requestId builds
//! the card directly).
//!
//! Post-rewrite assertions (inverted):
//! - a: full window of a no-reasoning round -> normal card (ToolUse builds
//!   the card, ToolResult hits the same card — the eradication criterion
//!   for the on-device empty-card defect).
//! - b: round with reasoning -> likewise a normal card (shape unchanged).
//! - c: cross-batch cut (ToolUse batch and ToolResult batch separated) ->
//!   the ToolUse batch still yields its card; an isolated ToolResult on a
//!   fresh model (copy-rebuild shape) -> orphan completed card, no data
//!   loss.
//! - d: ToolResult before ToolUse (in-batch reordering) -> ToolResult
//!   builds an orphan card first; the late ToolUse hits it by identity and
//!   completes the declaration fields (name/arguments/intent) — the final
//!   state matches the normal ordering.
//!
//! Projection semantics replicated (x-agent conversation_projection.rs):
//! - assistant(non-empty reasoning) -> ThinkStart/ThinkPart/ThinkEnd
//! - assistant(toolCalls=[X]) -> [tool_use(X), pair_complete(pair)]
//! - tool message -> tool_result(requestId=X, toolName='') (known line-A
//!   limitation: persisted tool messages carry no tool name)

use serde_json::{Value, json};
use xacpp::wire::events::XacppEvent;
use xacpp_activity::{ActivityModel, EventSource, SnapshotItem};

// ---------------------------------------------------------------- real ids

/// Real pair / requestIds from the call-5-9 segment (verbatim from
/// trajectory L1573-L1583).
mod real {
    pub const PAIR6: &str = "e04b072f-77e9-4a25-be4e-fc14ba7c98ed"; // round 7 (empty card)
    pub const PAIR7: &str = "41d295fd-6ea7-4019-a4d3-e06c5340e1a6"; // round 8 (normal)
    pub const RQ6: &str = "call_4100cc761d3b49bb9aa6da1c"; // call 7 -> empty card
    pub const RQ7: &str = "call_ff7b52d835c940599d330013"; // call 8 -> normal
}

// --------------------------------------- projection replica (wire shapes)

fn tool_use(rq: &str) -> XacppEvent {
    XacppEvent::new(
        "tool_use",
        json!({
            "requestId": rq,
            "toolName": "shell",
            "index": 0,
            "arguments": "{\"command\":\"...\",\"intent\":\"取证\"}",
            "intent": "取证"
        }),
    )
}

/// ToolResult as projected on line A: toolName is empty (persisted tool
/// messages carry no tool name).
fn tool_result(rq: &str) -> XacppEvent {
    XacppEvent::new(
        "tool_result",
        json!({
            "requestId": rq,
            "toolName": "",
            "index": 0,
            "parts": [{"type": "text", "text": "<tool-result-item>结果文本</tool-result-item>"}],
            "status": "completed",
            "isError": false,
            "auto": false
        }),
    )
}

fn pair_complete(pair: &str) -> XacppEvent {
    XacppEvent::new(
        "pair_complete",
        json!({"pair": pair, "contextWindow": 0, "tokenUsage": {"id": "", "message": "", "promptTokens": 0, "completionTokens": 0, "totalTokens": 0}}),
    )
}

/// Assistant round without reasoning projects to just [tool_use, pair_complete].
fn project_no_reasoning(pair: &str, rq: &str) -> Vec<XacppEvent> {
    vec![tool_use(rq), pair_complete(pair)]
}

/// Assistant round with reasoning (Think blocks lead).
fn project_with_reasoning(pair: &str, rq: &str) -> Vec<XacppEvent> {
    let part_id = format!("replay:thinking:{pair}");
    vec![
        XacppEvent::new(
            "think_start",
            json!({"partId": part_id, "round": "r1", "pair": pair}),
        ),
        XacppEvent::new(
            "think_part",
            json!({"content": "推理文本", "round": "r1", "pair": pair, "partId": part_id}),
        ),
        XacppEvent::new(
            "think_end",
            json!({"partId": part_id, "round": "r1", "pair": pair}),
        ),
        tool_use(rq),
        pair_complete(pair),
    ]
}

fn apply(model: &mut ActivityModel, events: Vec<XacppEvent>) {
    model.apply_history_events(events);
}

/// Snapshot card shape: (request_id, tool_name, done).
fn cards(model: &ActivityModel) -> Vec<(String, String, Option<bool>)> {
    model
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
        .collect()
}

/// Orphan-shaped cards: empty tool_name (data comes only from the
/// ToolResult event).
fn orphan_cards(model: &ActivityModel) -> Vec<(String, Option<bool>)> {
    cards(model)
        .into_iter()
        .filter(|(_, name, _)| name.is_empty())
        .map(|(rq, _, done)| (rq, done))
        .collect()
}

// ------- core inversion: a full no-reasoning window yields a normal card

/// Eradication criterion for the on-device empty-card defect (copy call
/// 7, 9aa6da1c): full window (assistant declaration + tool result in the
/// same batch, correctly ordered), assistant without reasoning.
/// Pre-rewrite: no ThinkStart in batch -> ToolUse dropped on
/// active_pair=None -> ToolResult took the orphan branch and built an
/// empty-named card. Post-rewrite: ToolUse builds the card keyed by
/// requestId (no drop branch) -> ToolResult hits the same card by
/// identity -> normal card.
#[test]
fn a_no_reasoning_full_window_produces_normal_card() {
    let mut m = ActivityModel::new();
    apply(
        &mut m,
        [
            project_no_reasoning(real::PAIR6, real::RQ6),
            vec![tool_result(real::RQ6)],
        ]
        .concat(),
    );
    let cs = cards(&m);
    assert_eq!(
        cs.len(),
        1,
        "exactly one card (ToolUse builds it, ToolResult merges in)"
    );
    assert_eq!(cs[0].0, real::RQ6);
    assert_eq!(
        cs[0].1, "shell",
        "card name comes from the ToolUse event — no more empty-named orphan shape"
    );
    assert_eq!(
        cs[0].2,
        Some(true),
        "result attached to the same card (done=Some(true))"
    );
    assert!(
        orphan_cards(&m).is_empty(),
        "no-reasoning rounds no longer produce orphan cards"
    );
}

/// Control (copy call 8, 9d330013): same full window, assistant with
/// reasoning -> Think blocks lead -> ToolUse builds normally -> ToolResult
/// hits -> normal card. The only difference from (a) is the presence of
/// the think block — the card shape is identical.
#[test]
fn b_with_reasoning_full_window_normal_card() {
    let mut m = ActivityModel::new();
    apply(
        &mut m,
        [
            project_with_reasoning(real::PAIR7, real::RQ7),
            vec![tool_result(real::RQ7)],
        ]
        .concat(),
    );
    let cs = cards(&m);
    assert_eq!(cs.len(), 1);
    assert_eq!(cs[0].0, real::RQ7);
    assert_eq!(
        cs[0].1, "shell",
        "round with reasoning: card name comes from the ToolUse event"
    );
    assert_eq!(cs[0].2, Some(true), "result attached to the same card");
    assert!(orphan_cards(&m).is_empty());
}

// -------------- cross-batch cut: ToolUse batch separated from ToolResult batch

/// Window-cut shape: batch 1 holds only the dangling assistant, batch 2
/// only the tool message (independent projection rebuild = fresh model).
/// Pre-rewrite batch 1's ToolUse was dropped with no active round (zero
/// cards); post-rewrite batch 1 yields its card as usual (no drop branch),
/// and batch 2's isolated ToolResult on the fresh model still takes the
/// orphan branch (its ToolUse is not local) — no data loss.
#[test]
fn c_cross_batch_cut_keeps_cards() {
    let mut m1 = ActivityModel::new();
    apply(&mut m1, project_no_reasoning(real::PAIR6, real::RQ6));
    let cs1 = cards(&m1);
    assert_eq!(
        cs1.len(),
        1,
        "dangling batch: ToolUse still builds its card (in flight)"
    );
    assert_eq!(cs1[0].1, "shell");
    assert_eq!(
        cs1[0].2,
        Some(false),
        "no result yet (the tool message is in the next batch)"
    );

    let mut m2 = ActivityModel::new(); // rebuild semantics: fresh model
    apply(&mut m2, vec![tool_result(real::RQ6)]);
    let cs2 = cards(&m2);
    assert_eq!(cs2.len(), 1);
    assert_eq!(
        cs2[0].1, "",
        "isolated ToolResult batch -> orphan empty-named card"
    );
    assert_eq!(
        cs2[0].2,
        Some(true),
        "orphan card carries the full result (no data loss)"
    );
}

// ------------------ ordering sensitivity: ToolResult before ToolUse

/// In-batch reordering: ToolResult arrives first (orphan card, empty
/// name) -> the later ToolUse hits it by requestId identity -> completes
/// the declaration fields (name/arguments/intent) — final state matches
/// the normal ordering.
#[test]
fn d_late_tool_use_completes_orphan_card() {
    let mut m = ActivityModel::new();
    apply(
        &mut m,
        [
            vec![tool_result(real::RQ7)],
            project_with_reasoning(real::PAIR7, real::RQ7),
        ]
        .concat(),
    );
    let cs = cards(&m);
    assert_eq!(cs.len(), 1, "same identity merges into one card");
    assert_eq!(cs[0].0, real::RQ7);
    assert_eq!(
        cs[0].1, "shell",
        "late ToolUse completes the declaration — the orphan shape self-heals"
    );
    assert_eq!(cs[0].2, Some(true), "result preserved (no data loss)");
}

// ---- hanging-tail-cut replica (behavioral anchor for the bindings decision)

/// Replica of bindings lib.rs hanging_tail_cut: answered = requestIds of
/// all tool_results inside the window; cut = index of the first unanswered
/// tool_use. Verifies the decision on three real window shapes and rules
/// out the "hanging-tail cut fails -> watermark skips a hole" hypothesis —
/// the cut trims dangling shapes correctly.
fn hanging_tail_cut(events: &[(&str, Value)]) -> Option<usize> {
    let answered: std::collections::HashSet<&str> = events
        .iter()
        .filter_map(|(name, data)| {
            if *name == "tool_result" {
                data.get("requestId").and_then(|v| v.as_str())
            } else {
                None
            }
        })
        .collect();
    events.iter().position(|(name, data)| {
        *name == "tool_use"
            && data
                .get("requestId")
                .and_then(|v| v.as_str())
                .is_none_or(|id| !answered.contains(id))
    })
}

#[test]
fn e_hanging_tail_cut_semantics() {
    // dangling assistant batch: cut (watermark None-d, re-read next round)
    let hang = vec![
        ("tool_use", json!({"requestId": real::RQ6})),
        (
            "pair_complete",
            json!({"pair": real::PAIR6, "contextWindow": 0, "tokenUsage": {"id": "", "message": "", "promptTokens": 0, "completionTokens": 0, "totalTokens": 0}}),
        ),
    ];
    assert_eq!(hanging_tail_cut(&hang), Some(0));
    // complete pair batch: pass through
    let full = vec![
        ("tool_use", json!({"requestId": real::RQ7})),
        (
            "pair_complete",
            json!({"pair": real::PAIR7, "contextWindow": 0, "tokenUsage": {"id": "", "message": "", "promptTokens": 0, "completionTokens": 0, "totalTokens": 0}}),
        ),
        ("tool_result", json!({"requestId": real::RQ7})),
    ];
    assert_eq!(hanging_tail_cut(&full), None);
    // isolated tool batch: pass through (the cut layer cannot recognize this shape — the orphan defense lives in the reducer layer)
    let lone = vec![("tool_result", json!({"requestId": real::RQ6}))];
    assert_eq!(hanging_tail_cut(&lone), None);
}

// --------------------------------------- copy merge-key identity anchor

/// Precondition of the copy-overwrite semantics: normal and orphan cards
/// share the key (tool:{requestId}) — bindings merge_entries rewrites the
/// intersection by entry_key, dropping the old row and appending the new
/// one. This anchor asserts both reducer paths yield the same request_id;
/// the shape difference ('' vs 'shell') exists only on the fresh-model
/// path with an isolated ToolResult — within the same model a late ToolUse
/// completes it (see d).
#[test]
fn f_same_request_id_both_paths() {
    let mut orphan = ActivityModel::new();
    apply(&mut orphan, vec![tool_result(real::RQ6)]);
    let orphan_card = &cards(&orphan)[0];

    let mut normal = ActivityModel::new();
    apply(
        &mut normal,
        [
            project_with_reasoning(real::PAIR6, real::RQ6),
            vec![tool_result(real::RQ6)],
        ]
        .concat(),
    );
    let normal_card = &cards(&normal)[0];

    assert_eq!(
        orphan_card.0, normal_card.0,
        "same request_id (same merge key)"
    );
    assert_ne!(
        orphan_card.1, normal_card.1,
        "shape divergence: '' vs 'shell'"
    );
}

/// Enumeration-surface anchor: three event sources (see the three-source
/// convergence in the reducer.rs header).
#[test]
fn g_event_source_surface() {
    let _ = [
        EventSource::Live,
        EventSource::BufferReplay,
        EventSource::HistoryProjection,
    ];
}
