# Parliant

Parliant is a Linux-first local meeting intelligence daemon. V1 captures an explicitly selected PipeWire source or sink monitor, transcribes audio through a provider boundary, keeps bounded finalized meeting text in memory, detects likely questions, generates private answer suggestions, exposes bounded read-only meeting context through MCP, and renders suggestions in a private Wayland overlay.

## V1 architecture

The Rust daemon owns capture, provider credentials, canonical meeting state, question detection, answer orchestration, local IPC, and the optional remote MCP bridge. The overlay is presentation-only. Raw audio is never persisted by the V1 implementation and finalized transcript state is memory-only by default.

Important boundaries:

- PipeWire target selection is explicit; Parliant does not silently fall back to another audio source.
- Only finalized transcript segments become canonical meeting text.
- Meeting transcript is untrusted data, never authorization or tool instructions.
- Answer generation is event-driven; it does not continuously invoke the answer model.
- MCP is read-only and bounded. V1 exposes no shell, process, file-write, or other machine-write tool.
- The remote bridge is disabled by default and binds only to loopback. A cloud client requires an authenticated encrypted tunnel; Parliant does not expose localhost directly to the public internet.
- The overlay never owns capture, credentials, or canonical state and never speaks answers automatically.

The full architecture contract is in [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## Build

Arch Linux dependencies and installation steps are documented in [`docs/INSTALL.md`](docs/INSTALL.md).

```bash
cargo build --workspace --release --locked
```

Developer verification:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo build --workspace --release --locked
python3 scripts/secret-scan.py
```

## Run

First inspect PipeWire and choose the exact object you intend to capture:

```bash
wpctl status
wpctl inspect <ID>
```

Capture-only diagnostic path:

```bash
cargo run --locked -p parliant-daemon -- capture --target '<NODE_NAME_OR_OBJECT_SERIAL>'
```

Complete V1 meeting pipeline:

```bash
export OPENAI_API_KEY='...'
cargo run --locked -p parliant-daemon -- meet \
  --target '<NODE_NAME_OR_OBJECT_SERIAL>' \
  --answer-model '<RESPONSES_API_MODEL>'
```

For a selected sink's playback monitor, add `--sink-monitor`.

In another terminal, launch the private overlay:

```bash
cargo run --locked -p parliant-overlay
```

The remote MCP bridge is opt-in. See [`docs/REMOTE_MCP.md`](docs/REMOTE_MCP.md) before enabling it.

## Verification status

The repository CI verifies formatting, warnings-denied Clippy, deterministic workspace tests, secret scanning, and a release build from the committed dependency lockfile. Hardware-, compositor-, and credential-dependent smoke tests remain separate runtime evidence and must not be inferred from CI. See [`docs/INSTALL.md`](docs/INSTALL.md) for the exact live verification procedures.

## Privacy

See [`docs/PRIVACY.md`](docs/PRIVACY.md). The short version: V1 has no raw-audio persistence path, no default transcript persistence path, metadata-only daemon diagnostics, a same-UID local overlay socket, and an optional authenticated read-only remote MCP boundary that remains disabled unless explicitly enabled.
