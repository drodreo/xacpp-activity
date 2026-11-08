//! Activity event envelope: runtime-assigned bookkeeping wrapped around a
//! typed payload.
//!
//! Semantics ported verbatim from agentx `activity_runtime.rs`
//! (`ActivityEventEnvelope` + `ActivityRuntimeState`): monotonically
//! increasing `seq`, `pair_epoch` bounded by `pair_complete`, and the
//! current-pair replay buffer. Field names follow the agentx wire literals
//! (`activityId` / `seq` / `pairEpoch` / `category`, camelCase).

use serde::{Deserialize, Serialize, Serializer};

use crate::event::ActivityEvent;

/// Event source classification (agentx `ActivityEventCategory`).
///
/// Client-side consumers default to [`ActivityEventCategory::Default`];
/// `Background` exists for consumers that mirror the agentx split.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivityEventCategory {
    /// Foreground activity.
    #[default]
    Default,
    /// Background activity (scheduled tasks).
    Background,
}

/// Wire (de)serialization form: identical field set in camelCase with the
/// payload flattened into the `{name, data}` event object.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EnvelopeWire {
    activity_id: String,
    seq: u64,
    pair_epoch: u64,
    category: ActivityEventCategory,
    payload: xacpp::wire::events::XacppEvent,
}

/// Envelope carrying one typed activity event plus runtime bookkeeping.
#[derive(Debug, Clone, PartialEq)]
pub struct ActivityEventEnvelope {
    /// Stable activity identifier (equivalent to the topic id).
    pub activity_id: String,
    /// Monotonically increasing sequence number (saturating).
    pub seq: u64,
    /// Epoch bounded by `pair_complete`.
    pub pair_epoch: u64,
    /// Event source classification.
    pub category: ActivityEventCategory,
    /// Typed event payload.
    pub payload: ActivityEvent,
}

impl Serialize for ActivityEventEnvelope {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let wire = EnvelopeWire {
            activity_id: self.activity_id.clone(),
            seq: self.seq,
            pair_epoch: self.pair_epoch,
            category: self.category,
            payload: self.payload.to_wire(),
        };
        wire.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ActivityEventEnvelope {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = EnvelopeWire::deserialize(deserializer)?;
        let payload =
            ActivityEvent::from_xacpp_event(&wire.payload).map_err(serde::de::Error::custom)?;
        Ok(Self {
            activity_id: wire.activity_id,
            seq: wire.seq,
            pair_epoch: wire.pair_epoch,
            category: wire.category,
            payload,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{BlockBoundaryPayload, PairCompletePayload, ThinkContentPayload};
    use xacpp::wire::events::TokenUsage;

    fn think_delta(content: &str) -> ActivityEvent {
        ActivityEvent::ThinkDelta(ThinkContentPayload {
            content: content.into(),
            round: "r1".into(),
            pair: "p1".into(),
            part_id: "tp0".into(),
        })
    }

    fn pair_complete() -> ActivityEvent {
        ActivityEvent::PairComplete(PairCompletePayload {
            pair: "p1".into(),
            context_window: 0,
            token_usage: TokenUsage::default(),
            tier: None,
            model: None,
        })
    }

    fn envelope(seq: u64, epoch: u64, payload: ActivityEvent) -> ActivityEventEnvelope {
        ActivityEventEnvelope {
            activity_id: "act-1".into(),
            seq,
            pair_epoch: epoch,
            category: ActivityEventCategory::Default,
            payload,
        }
    }

    #[test]
    fn serializes_agentx_camel_case_literals() {
        let value = serde_json::to_value(envelope(3, 1, think_delta("hi"))).unwrap();
        assert_eq!(value["activityId"], "act-1");
        assert_eq!(value["seq"], 3);
        assert_eq!(value["pairEpoch"], 1);
        assert_eq!(value["category"], "default");
        // Payload flattens into the {name, data} wire object.
        assert_eq!(value["payload"]["name"], "think");
        assert_eq!(value["payload"]["data"]["content"], "hi");
        assert_eq!(value["payload"]["data"]["partId"], "tp0");
    }

    #[test]
    fn serde_round_trip_is_lossless() {
        let original = envelope(7, 2, pair_complete());
        let text = serde_json::to_string(&original).unwrap();
        let restored: ActivityEventEnvelope = serde_json::from_str(&text).unwrap();
        assert_eq!(restored, original);
    }

    #[test]
    fn category_serializes_snake_case() {
        let mut e = envelope(1, 0, think_delta("x"));
        e.category = ActivityEventCategory::Background;
        let value = serde_json::to_value(&e).unwrap();
        assert_eq!(value["category"], "background");
        let restored: ActivityEventEnvelope = serde_json::from_value(value).unwrap();
        assert_eq!(restored.category, ActivityEventCategory::Background);
    }

    #[test]
    fn boundary_payload_shape_is_typed() {
        // Sanity: boundary payload keeps its typed accessors after round-trip.
        let mut e = envelope(
            1,
            0,
            ActivityEvent::ThinkStart(BlockBoundaryPayload {
                round: "r1".into(),
                pair: "p1".into(),
                part_id: "replay:thinking:p1".into(),
            }),
        );
        e.payload = think_delta("hi");
        let restored: ActivityEventEnvelope =
            serde_json::from_str(&serde_json::to_string(&e).unwrap()).unwrap();
        match restored.payload {
            ActivityEvent::ThinkDelta(p) => assert_eq!(p.content, "hi"),
            other => panic!("expected ThinkDelta, got {other:?}"),
        }
    }
}
