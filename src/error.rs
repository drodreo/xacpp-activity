//! Error types for the activity session layer.

use thiserror::Error;

/// Errors raised when translating wire events into the typed vocabulary.
#[derive(Debug, Error)]
pub enum ActivityError {
    /// The event `name` is not part of the known activity event vocabulary.
    #[error("unknown activity event name: `{0}`")]
    UnknownEvent(String),

    /// The event payload does not match the declared shape for `name`.
    #[error("malformed payload for activity event `{name}`: {source}")]
    MalformedPayload {
        /// Wire event name that failed to parse.
        name: String,
        /// Underlying serde error.
        #[source]
        source: serde_json::Error,
    },
}
