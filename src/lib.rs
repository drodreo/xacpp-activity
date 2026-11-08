//! xacpp-activity — client-side activity session library on top of XACPP.
//!
//! Two-layer concept split:
//! - [`xacpp::session::XacppSession`] is the *connection channel* (protocol layer).
//! - This crate is the *activity session* layer: envelope runtime
//!   (seq/pair-epoch/buffer), a single unified conversion entry for all event
//!   sources, the turn/interaction model, and reliability judgment.
//!
//! Phase 1 scope: the strongly-typed activity event vocabulary
//! ([`ActivityEvent`]) with lossless (de)serialization against the wire forms
//! emitted by the x-agent xacpp listener (`event_bridge::convert_event`) and
//! the `conversation_history` projection.

pub mod error;

pub use error::ActivityError;
