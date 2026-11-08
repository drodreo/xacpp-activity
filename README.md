# xacpp-activity

[![CI](https://github.com/drodreo/xacpp-activity/actions/workflows/ci.yml/badge.svg)](https://github.com/drodreo/xacpp-activity/actions/workflows/ci.yml)

Client-side activity session runtime on top of the [XACPP](https://crates.io/crates/xacpp) protocol.

## Layering

Two-layer concept split:

- `xacpp::session::XacppSession` is the *connection channel* (protocol layer).
- This crate is the *activity session* layer: envelope runtime
  (seq/pair-epoch/buffer), a single unified conversion entry for all event
  sources, the turn/interaction model, and reliability judgment.

## Modules

| Module | Responsibility |
|---|---|
| `event` | Strongly-typed activity event vocabulary, lossless wire (de)serialization |
| `envelope` | Event categories and envelopes (seq / pair epoch / buffer) |
| `interaction` | Pending interaction objects (permission requests, questions) |
| `model` | Turn/interaction session model: state machine, snapshots, render order |
| `draft` | Per-activity draft store |
| `reducer` | Unified event conversion across live / buffer replay / history projection |
| `runtime` | Envelope runtime: history rebuild, buffer projection, steady-state trim |
| `registry` | Multi-activity hub: routing, versioned snapshots, lifecycle |

See `ARCHITECTURE.md` for the full reference.

## Testing

```sh
cargo test
```

The `mock_serve_integration` target runs against a real TCP mock serve
(`xacpp-mock-serve`, a path dev-dependency on a sibling repository that is not
published); clone it next to this crate to run the full suite locally. CI runs
all single-repo targets on Linux, Windows and macOS.

## License

MIT
