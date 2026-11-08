# xacpp-activity — Architecture

`xacpp-activity` (shorthand: **xaact**) is the client-side activity session
library on top of XACPP. It owns every piece of pure logic between the wire
and the UI projection, shared verbatim across platform bindings (Android,
iOS).

## Layered Positioning

Two-layer concept split:

- `xacpp::session::XacppSession` is the **connection channel** (protocol
  layer): transport, negotiation, command/response envelopes.
- `xacpp-activity` is the **activity session layer**: envelope runtime,
  unified event conversion, turn/interaction model, reliability judgment,
  per-activity buffer, and the projection surface consumed by UI.

Envelope semantics are ported verbatim from the agentx
`activity_runtime.rs` wire literals (`activityId` / `seq` / `pairEpoch` /
`category`, camelCase).

## Execution Model

The library is **async**. The two stateful surfaces — `ActivityHub`
(`registry.rs`) and `DraftStore` (`draft.rs`) — expose `async` methods and
synchronise through `tokio::sync::Mutex`. Everything else (`event`,
`envelope`, `model`, `reducer`, `runtime`) is pure computation and stays
synchronous: no IO, no locks, no `async`.

The shape is dictated by continuity, not preference. The layer above
(`xacpp`) is async; the layer below (the uniffi bindings) exports with
`async_runtime = "tokio"`. A synchronous middle would force every FFI call
site to wrap the hub in `spawn_blocking` purely to avoid parking a worker —
a discontinuity paid for at the boundary.

Rules that follow from it:

- **No runtime of its own.** Like `xacpp`, the library never creates a
  tokio runtime; it requires an ambient one. The single runtime start on
  the whole path is the uniffi scaffolding. Calling in from outside a
  runtime context **panics** (`tokio::sync::Mutex` needs a reactor to park
  on) — the previous `std::sync::Mutex` blocked silently instead, so this
  is a real contract change for callers, not just a signature change.
- **Ordering is the hub's job.** All mutations of one activity go through
  one lock, so per-activity ordering holds without callers serialising.
  Consumers do not need to funnel calls through a single thread, and must
  not assume the callback fires on the caller's thread.
- **The change callback fires outside the lock**, and must not re-enter the
  hub: the async mutex is non-reentrant, so re-entry deadlocks silently.
- **No poison state.** A panicking holder releases the lock normally; there
  is no `PoisonError` path and no recovery branch in callers.

## Core Concepts

### Event Envelope (`envelope.rs`)

Runtime bookkeeping wrapped around a typed payload: monotonically
increasing `seq`, `pair_epoch` bounded by `pair_complete`, and the event
category. Envelopes are the unit of buffering, replay, and watermark
accounting.

### Activity Runtime (`runtime.rs`)

One instance per activity: `seq` / `pair_epoch` counters, the current-pair
replay buffer, and the interactions table. The runtime is instance-level:
the consumer holding it keeps interactions alive across activity
recreation.

### Sync Plan (`runtime.rs`)

When a consumer is behind, recovery is decided in one place:

- **InBuffer** — the gap is covered by the replay buffer: replay envelopes,
  advance the watermark.
- **NeedHistory** — the gap exceeds buffer coverage: pull the authoritative
  projection via the session's `apply_history` (latest-window rebuild).

Sync planning covers a UI consumer resuming (page recreation, detached
collector) against a living session. Connection-level loss is out of
scope — the registry is voided on connection death (see
**Disconnection** below), so no watermark survives it.

### Event Vocabulary & Unified Conversion (`event.rs`)

`ActivityEvent` is the strongly-typed activity event vocabulary with
lossless (de)serialization against the wire forms emitted by the x-agent
xacpp listener (`event_bridge::convert_event`) and the
`conversation_history` projection. All event sources convert through this
single entry.

### Event Sources (`reducer.rs`)

- **Live** — real-time stream; incremental build.
- **BufferReplay** — current-pair buffer replay on reconnect; incremental
  build (also the carrier for the live in-flight tail across a
  latest-window rebuild).
- **HistoryProjection** — `conversation_history` latest-window ingest; the
  session layer applies it to a **fresh model** (replacement, not merge —
  see **History channels** below).

### Session Model (`model.rs`)

`ActivityModel`: turns, blocks, process messages. Block identity is
**structural** — one think slot per turn, an ordered contents list, a tool
list. Wire part ids never leak into the public model; they live only in
the reducer's internal routing tables (replay-prefixed and live provider
ids alike stay crate-private). The model also retains the most recently
applied `pair_complete` payload as the current activity's actual
context-window/token-usage fact; it rides the same atomic `HubSnapshot`
as the render items. Latest-window history rebuilds this fact in stream
order, older pages never write it back, and unknown cache data remains
`None` rather than becoming zero. When a history rebuild names the same
latest pair, its unknown cache field does not erase the known live value;
a different pair never inherits the previous pair's cache. A cold
historical rebuild with no live observation therefore remains unknown.

### Reducer (`reducer.rs`)

Events mutate the model through the reducer. Routing-table decisions are
made in one borrow of the routes; the model is mutated through a separate
borrow — never held simultaneously.

### Projection correctness targets

These are target-state invariants for the dedicated data-accuracy patch
line; the temporary gaps below are explicitly not yet conformant.

- **User input identity** — distinct inputs remain distinct even when
  their visible text is identical. One logical input appearing in live,
  history, and the in-flight tail projects exactly once. The user entry
  holds the complete user-visible ordered inputs (only `default`/
  `references` part types; a media-only input is kept); its snapshot key
  is the canonical-input hash — `part_id` and, when a content sha is
  present, the materialization products (`localUri`, `remoteUrl`,
  `remoteExpiresAt`, `requireOrganized`) are stripped before hashing, so
  channel drift cannot split or merge a logical input; with an empty sha
  the controlled `localUri`/`remoteUrl` pair is the fallback identity.
  Adjacent replay re-pushes of the same canonical input merge at snapshot
  derivation; non-adjacent repeats carry a deterministic `#{n}` suffix.
- **Incremental content fidelity** — every `ContentDelta` / `ThinkDelta`
  is an ordered increment; payload equality alone does not make two
  arrivals duplicates. Deltas append in arrival order, then a complete
  Part replaces and seals the block. History projection carries complete
  Parts, not deltas, so payload-based delta deduplication is not a history
  alignment mechanism.
- **Tool result content fidelity** — `parts=[]`, an empty Text part, and
  media-only parts are distinct input shapes and must remain
  distinguishable. A present result with zero text parts is not the same
  fact as a present empty-string text projection. Result-emptiness and
  text projections are derived from the actual parts, consistently across
  live snapshots and persisted copies.

### Temporary conformance gaps (deferred)

The following gaps are recorded explicitly and deferred to the dedicated
patch line while app display and experience work takes priority. Existing
unit/E2E passes do not close them.

- The reducer currently drops an adjacent delta whose serialized payload
  equals `last_delta`, for both content and thinking. This violates the
  ordered-increment target before an authoritative Part arrives.
- Tool snapshot derivation currently hardcodes `result_empty=false` and
  can produce `result_text=Some("")` when no text parts exist. Android
  live/copy projections also derive emptiness differently. No additional
  result state or speculative compatibility default is authorized by this
  gap record.

### Tool card lifecycle (`model.rs`)

The card lifecycle is **derived, not stored**: `ToolCard::done()` is a pure
function of the card's latest result — no result yet (ToolUse only) means
running, a `Running` placeholder means running, a terminal status means
done, and an unknown status means unknown (`None`). ToolResult merges by
exactly three status-driven rules: terminal statuses complete the card,
`Running` refreshes the content of a card that is not already terminal
(a terminal card is never demoted), and an unknown status refreshes the
content while keeping the known status (unknown never erases a known
state). A late `ToolUse` only completes an orphan card's declaration —
never its lifecycle.

### History channels (`registry.rs`)

- `apply_history(events, cursor)` — the authoritative **latest-window**
  ingest (page enter / reconnect realign): the batch builds a fresh
  `ActivityModel`, the only carried-over state is the live in-flight tail
  (`runtime.current_pair_events()`, replayed through the same per-event
  path), then the buffer steady state settles (trim). Content validity
  flips Void → Live and the window cursor is rewritten; the monotonic
  `turn_completed` marker carries over untouched (a realign neither
  advances nor rewinds it).
- `apply_history_rebuild(events, cursor)` — the **full-archive** channel:
  same authoritative semantics, but no tail replay (the archive is the
  complete record) and no trim.
- Older-page pulls write no content back: the cursor advances through
  `set_window_cursor` alone.

### Interaction Model (`interaction.rs`)

Registration and answering state machine for `action_request` /
`question`: `consume_command` assigns an id and registers the pending
interaction; answers are `Approve` / `ApproveAlways` / `Reject{reason}` /
`Answer` / `Skip`.

## Library / Binding Boundary

**In-library (shared, zero platform code):** everything above, plus the
multi-activity registry, per-activity event routing, change notification,
buffer steady state, trimming, content validity, and the draft store.

**Binding layer (per-platform thin wiring):** connection management
(xacpp client, reconnect, connection-death signaling that voids the
registry), handing inbound events to the library router, FFI/projection
glue, holding the process-lifetime `DraftStore` instance, and
pure-transient UI state (menu open, button press).

Each platform binding is a thin adapter; no session logic is implemented
twice.

## Buffer & Projection Model

The per-activity buffer is the single state authority the UI projects
from. The UI is a projection; pages bind and unbind, the buffer lives on.

- **Unit of accounting** — one `content_part` is one complete, minimal UI
  semantic unit. The steady-state cap is **30 parts per activity**.
  Trimming settles at turn completion; an in-flight streaming part never
  triggers a trim. History pulls ingested in bulk settle the same way.
- **Paging does not write back** — messages loaded by scrolling up enter
  the UI list only, never the buffer. The buffer holds the most recent 30
  parts; the UI temporarily extends beyond it.
- **Admission** — a session is admitted by `register`: page entry, or
  lazy admission when an off-page interaction command arrives (pending
  interactions are never dropped). Events for unregistered activities
  are dropped at the router.
- **Content validity** — a session's content is **Void** from admission
  until the first authoritative latest-window history ingest
  (`apply_history`) turns it **Live**. Live events, interaction registration, buffer replays and
  older-page paging never flip Void to Live. Validity rides the atomic
  snapshot; a consumer entering a page pulls history iff the session is
  new or Void, and replays the projection with zero round-trip iff
  Live. `unregister` resets validity (re-admission starts Void).
- **Window cursor** — the paging cursor (`nextBeforeSeqInTopic` /
  `hasMore`) is the same class of session-side bookkeeping as content
  validity: an authoritative latest-window ingest registers (or resets)
  it together with the window it replaces, older-page batches advance it
  without writing content back, and it rides the atomic snapshot
  (`next_before_seq` / `has_more`). Consumers page from the snapshot
  instead of keeping private cursor state — private state dies with a
  page; the session outlives pages. `unregister` resets it.
- **Disconnection** — connection death voids the whole registry: the
  binding layer drops or clears the hub, and every session dies —
  buffers and pending interactions included. Recovery after reconnect
  is pull-based: an authoritative history pull on next admission, and
  suspended interactions re-pushed by the serve suspend table. The
  library carries nothing across a disconnection.
- **Page re-entry, connection alive** — replay the buffer projection;
  zero network round-trip (gated by content validity above).
- **Draft persistence** — un-sent input per activity lives in a
  consumer-held `DraftStore`, separate from sessions: it survives page
  switches, registry voiding and reconnects, and dies only with the
  consumer (process). Drafts are not part of the session fingerprint —
  typing never bumps the projection version.
- **Projection protocol** — change notification carries a version number;
  the UI reads a **full snapshot** on version change. The 30-part cap
  bounds snapshot cost, so no incremental diff protocol exists.

## Event Path

Inbound events are routed by `activity` directly to the owning session in
the library — no detour through platform code before ingestion. Platform
code subscribes to change notifications and renders the projection.
