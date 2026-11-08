//! Shared test infrastructure for the buffer & projection scenarios.
//!
//! [`MockFeed`] is an orchestrated streaming event source: a scripted
//! queue of feeding steps that drives the registry's unified conversion
//! entries exactly the way a binding layer would. Out-of-order and
//! duplicate injection are expressed by the script order itself (queue
//! the same step twice, or queue a step before its prerequisite).
//! [`NotificationLog`] collects change notifications for assertions.
//!
//! Each integration test target compiles this module separately, so not
//! every helper is consumed by every consumer — unused items are expected.
#![allow(dead_code)]

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use serde_json::{Value, json};
use xacpp::wire::events::XacppEvent;

use xacpp_activity::{ActivityEventCategory, ActivityEventEnvelope, ActivityHub, WindowCursor};

// ---------------------------------------------------------------------------
// Notification collection.
// ---------------------------------------------------------------------------

/// Collected `(activity_id, version)` change notifications.
#[derive(Clone, Default)]
pub struct NotificationLog {
    entries: Arc<Mutex<Vec<(String, u64)>>>,
}

impl NotificationLog {
    /// Installs a change callback on the hub that records every
    /// notification into this log.
    pub async fn attach(hub: &ActivityHub) -> Self {
        let log = Self::default();
        let sink = log.entries.clone();
        hub.set_on_change(Box::new(move |activity, version| {
            sink.lock().unwrap().push((activity.to_string(), version));
        }))
        .await;
        log
    }

    /// All recorded notifications, in firing order.
    pub fn entries(&self) -> Vec<(String, u64)> {
        self.entries.lock().unwrap().clone()
    }

    /// Versions recorded for one activity, in firing order.
    pub fn versions_of(&self, activity: &str) -> Vec<u64> {
        self.entries()
            .into_iter()
            .filter(|(entry_activity, _)| entry_activity == activity)
            .map(|(_, version)| version)
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Scripted feed.
// ---------------------------------------------------------------------------

/// One scripted feeding step.
pub enum FeedItem {
    /// Live stream event (routed through `ActivityHub::route_event`).
    Live {
        activity: String,
        category: ActivityEventCategory,
        event: XacppEvent,
    },
    /// Current-pair buffer replay batch (applied via the session).
    Replay {
        activity: String,
        envelopes: Vec<ActivityEventEnvelope>,
    },
    /// Authoritative latest-window history batch (window replacement),
    /// with the optional wire paging cursor (`None` = cursor-less ingest).
    History {
        activity: String,
        events: Vec<XacppEvent>,
        cursor: Option<WindowCursor>,
    },
    /// Cursor-only bookkeeping step (older-page pulls write no content
    /// back — the paging cursor advances via `set_window_cursor`).
    CursorOnly {
        activity: String,
        cursor: WindowCursor,
    },
}

/// Scripted, order-preserving feed: build the script with the builder
/// methods, then advance it step by step (`drive_step`) or in one batch
/// (`drive_all`). Duplicate/out-of-order injection = script the same step
/// twice, or script a step ahead of its prerequisite.
pub struct MockFeed {
    queue: VecDeque<FeedItem>,
}

impl Default for MockFeed {
    fn default() -> Self {
        Self::new()
    }
}

impl MockFeed {
    pub fn new() -> Self {
        Self {
            queue: VecDeque::new(),
        }
    }

    /// Queues one live wire event (default category).
    pub fn live(mut self, activity: &str, event: XacppEvent) -> Self {
        self.queue.push_back(FeedItem::Live {
            activity: activity.to_string(),
            category: ActivityEventCategory::Default,
            event,
        });
        self
    }

    /// Queues one live wire event with an explicit category.
    pub fn live_on(
        mut self,
        activity: &str,
        category: ActivityEventCategory,
        event: XacppEvent,
    ) -> Self {
        self.queue.push_back(FeedItem::Live {
            activity: activity.to_string(),
            category,
            event,
        });
        self
    }

    /// Queues a buffer-replay batch.
    pub fn replay(mut self, activity: &str, envelopes: Vec<ActivityEventEnvelope>) -> Self {
        self.queue.push_back(FeedItem::Replay {
            activity: activity.to_string(),
            envelopes,
        });
        self
    }

    /// Queues an authoritative latest-window history batch from
    /// `(name, data)` wire shapes.
    pub fn history(self, activity: &str, events: Vec<(String, Value)>) -> Self {
        self.history_with_cursor(activity, events, None)
    }

    /// Queues one latest-window history batch carrying the wire paging
    /// cursor (`nextBeforeSeqInTopic` / `hasMore` bookkeeping).
    pub fn history_with_cursor(
        mut self,
        activity: &str,
        events: Vec<(String, Value)>,
        cursor: Option<WindowCursor>,
    ) -> Self {
        self.queue.push_back(FeedItem::History {
            activity: activity.to_string(),
            events: events
                .into_iter()
                .map(|(name, data)| wire(&name, data))
                .collect(),
            cursor,
        });
        self
    }

    /// Queues a cursor-only step: the older-page pull's bookkeeping
    /// advance (content never enters the buffer — paging does not write
    /// back).
    pub fn cursor_only(mut self, activity: &str, cursor: WindowCursor) -> Self {
        self.queue.push_back(FeedItem::CursorOnly {
            activity: activity.to_string(),
            cursor,
        });
        self
    }

    /// Feeds exactly one step. Returns whether it was consumed (live steps
    /// report the router outcome; batch steps always consume).
    pub async fn drive_step(&mut self, hub: &ActivityHub) -> bool {
        let Some(item) = self.queue.pop_front() else {
            return false;
        };
        match item {
            FeedItem::Live {
                activity,
                category,
                event,
            } => hub.route_event(&activity, category, event).await,
            FeedItem::Replay {
                activity,
                envelopes,
            } => {
                hub.with_session(&activity, |session| session.apply_replay(envelopes))
                    .await;
                true
            }
            FeedItem::History {
                activity,
                events,
                cursor,
            } => {
                hub.with_session(&activity, |session| session.apply_history(events, cursor))
                    .await;
                true
            }
            FeedItem::CursorOnly { activity, cursor } => {
                hub.with_session(&activity, |session| session.set_window_cursor(cursor))
                    .await;
                true
            }
        }
    }

    /// Feeds every remaining step in order. Returns the number fed.
    pub async fn drive_all(&mut self, hub: &ActivityHub) -> usize {
        let mut fed = 0;
        while self.drive_step(hub).await {
            fed += 1;
        }
        fed
    }
}

// ---------------------------------------------------------------------------
// Wire-shape helpers (frozen from the phase-1/3 contract fixtures).
// ---------------------------------------------------------------------------

/// Wire event literal (the shape the transport decodes: `{name, data}`).
pub fn wire(name: &str, data: Value) -> XacppEvent {
    XacppEvent::new(name, data)
}

/// `start` with one text input part.
pub fn start_ev(text: &str) -> XacppEvent {
    wire("start", json!({"inputs": [{"type": "text", "text": text}]}))
}

pub fn think_start_ev(pair: &str, part_id: &str) -> XacppEvent {
    wire(
        "think_start",
        json!({"partId": part_id, "round": "r1", "pair": pair}),
    )
}

pub fn think_delta_ev(pair: &str, part_id: &str, content: &str) -> XacppEvent {
    wire(
        "think",
        json!({"content": content, "round": "r1", "pair": pair, "partId": part_id}),
    )
}

pub fn think_end_ev(pair: &str, part_id: &str) -> XacppEvent {
    wire(
        "think_end",
        json!({"partId": part_id, "round": "r1", "pair": pair}),
    )
}

pub fn content_start_ev(pair: &str, part_id: &str) -> XacppEvent {
    wire(
        "content_start",
        json!({"partId": part_id, "round": "r1", "pair": pair}),
    )
}

pub fn content_delta_ev(pair: &str, part_id: &str, text: &str) -> XacppEvent {
    wire(
        "content_delta",
        json!({"round": "r1", "pair": pair, "payload": {"type": "text", "text": text, "partId": part_id}}),
    )
}

pub fn content_end_ev(pair: &str, part_id: &str) -> XacppEvent {
    wire(
        "content_end",
        json!({"partId": part_id, "round": "r1", "pair": pair}),
    )
}

pub fn tool_use_ev(request_id: &str) -> XacppEvent {
    wire(
        "tool_use",
        json!({"requestId": request_id, "toolName": "shell", "index": 0,
               "arguments": "{\"cmd\":\"date\"}", "intent": "clock"}),
    )
}

pub fn tool_result_ev(request_id: &str) -> XacppEvent {
    wire(
        "tool_result",
        json!({"requestId": request_id, "toolName": "shell", "index": 0,
               "parts": [{"type": "text", "text": "2026-10-10"}],
               "status": "completed", "isError": false, "auto": false}),
    )
}

pub fn pair_complete_ev(pair: &str) -> XacppEvent {
    wire(
        "pair_complete",
        json!({"pair": pair, "contextWindow": 200000,
               "tokenUsage": {"id": "u1", "message": "m1", "promptTokens": 120,
                              "completionTokens": 80, "totalTokens": 200}}),
    )
}

pub fn complete_ev() -> XacppEvent {
    wire(
        "complete",
        json!({"round": "r1", "assistantReply": [{"type": "text", "text": "done"}],
               "canceled": false}),
    )
}

/// Concatenated text of every `Text` snapshot item (content assertions).
pub fn text_of(items: &[xacpp_activity::SnapshotItem]) -> String {
    items
        .iter()
        .filter_map(|item| match item {
            xacpp_activity::SnapshotItem::Text { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .concat()
}

/// Concatenated thinking text of every `Think` snapshot item.
pub fn think_of(items: &[xacpp_activity::SnapshotItem]) -> String {
    items
        .iter()
        .filter_map(|item| match item {
            xacpp_activity::SnapshotItem::Think { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .concat()
}
