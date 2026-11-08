//! Draft store: per-activity unsent input, owned by the consumer.
//!
//! Drafts are deliberately **not** session state (ARCHITECTURE.md,
//! "Draft persistence"): sessions die with the registry on connection
//! death, but drafts are bound to the consumer's lifetime (the process)
//! — a user typing through a reconnect must not lose input. The binding
//! layer holds one `DraftStore` instance for the process; hub voiding
//! and reconnects never touch it.
//!
//! Drafts are also absent from the session fingerprint: typing is not a
//! projection mutation and must never bump a version or fire a change
//! notification.
//!
//! Concurrency shape: async, `Send + Sync` (interior `tokio::sync::Mutex`),
//! matching the registry. No runtime is created here — callers supply the
//! context.

use std::collections::HashMap;

use tokio::sync::Mutex;

/// Per-activity unsent-input store. Lives and dies with its holder.
#[derive(Default)]
pub struct DraftStore {
    inner: Mutex<HashMap<String, String>>,
}

impl DraftStore {
    /// Creates an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the activity's draft. An empty string clears the entry
    /// (absent and empty are the same state).
    pub async fn set(&self, activity_id: &str, draft: &str) {
        let mut inner = self.inner.lock().await;
        if draft.is_empty() {
            inner.remove(activity_id);
        } else {
            inner.insert(activity_id.to_string(), draft.to_string());
        }
    }

    /// The activity's current draft, if any.
    pub async fn get(&self, activity_id: &str) -> Option<String> {
        let inner = self.inner.lock().await;
        inner.get(activity_id).cloned()
    }

    /// Clears the activity's draft (`false` when none was stored).
    pub async fn clear(&self, activity_id: &str) -> bool {
        let mut inner = self.inner.lock().await;
        inner.remove(activity_id).is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn set_get_roundtrip() {
        let store = DraftStore::new();
        assert_eq!(store.get("a").await, None);
        store.set("a", "hello").await;
        assert_eq!(store.get("a").await, Some("hello".to_string()));
        store.set("a", "hello world").await;
        assert_eq!(store.get("a").await, Some("hello world".to_string()));
    }

    #[tokio::test]
    async fn empty_string_clears() {
        let store = DraftStore::new();
        store.set("a", "typing").await;
        store.set("a", "").await;
        assert_eq!(store.get("a").await, None);
        // Clearing an absent entry is a no-op, not an error.
        store.set("a", "").await;
        assert_eq!(store.get("a").await, None);
    }

    #[tokio::test]
    async fn clear_reports_prior_presence() {
        let store = DraftStore::new();
        assert!(!store.clear("a").await);
        store.set("a", "x").await;
        assert!(store.clear("a").await);
        assert_eq!(store.get("a").await, None);
    }

    #[tokio::test]
    async fn entries_are_isolated_per_activity() {
        let store = DraftStore::new();
        store.set("a", "for a").await;
        store.set("b", "for b").await;
        store.clear("a").await;
        assert_eq!(store.get("a").await, None);
        assert_eq!(store.get("b").await, Some("for b".to_string()));
    }
}
