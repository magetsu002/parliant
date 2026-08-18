# Install and verify SIDECAR V1

SIDECAR V1 targets Arch Linux with PipeWire and a Wayland compositor. The core tests do not require audio hardware, a paid API, or a compositor, but the final runtime smoke tests do.

## Arch dependencies

Install the native build/runtime packages and Rust toolchain:

```bash
sudo pacman -S --needed base-devel rustup pipewire pipewire-audio wireplumber gtk3 gtk-layer-shell git python
rustup toolchain install stable --profile minimal --component rustfmt --component clippy
rustup default stable
```

Clone and build the exact candidate you intend to run:

```bash
git clone https://github.com/magetsu002/sidecar.git
cd sidecar
git fetch --all --prune
cargo build --workspace --release --locked
```

The binaries are:

```text
target/release/sidecar
target/release/sidecar-overlay
```

For a user-local installation:

```bash
install -Dm755 target/release/sidecar "$HOME/.local/bin/sidecar"
install -Dm755 target/release/sidecar-overlay "$HOME/.local/bin/sidecar-overlay"
```

## Automated release gate

Run before relying on a candidate head:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo build --workspace --release --locked
python3 scripts/secret-scan.py
```

## PipeWire smoke test

Identify an exact source or sink:

```bash
wpctl status
wpctl inspect <ID>
```

Source capture:

```bash
sidecar capture --target '<NODE_NAME_OR_OBJECT_SERIAL>'
```

Selected sink monitor:

```bash
sidecar capture --target '<SINK_NODE_NAME_OR_OBJECT_SERIAL>' --sink-monitor
```

While audio flows, require a negotiated format, emitted frames, monotonic/advancing timestamps, and clean Ctrl-C shutdown. Then try an invalid target and disconnect the selected real node while running; both cases must fail explicitly instead of switching to another source.

## Complete live meeting smoke test

Provide the OpenAI key only through the environment and choose a Responses API model supported by your account:

```bash
export OPENAI_API_KEY='YOUR_KEY_HERE'
sidecar meet \
  --target '<NODE_NAME_OR_OBJECT_SERIAL>' \
  --answer-model '<RESPONSES_API_MODEL>'
```

For playback capture add `--sink-monitor`.

In a second terminal:

```bash
sidecar-overlay
```

Verify all of the following on a real meeting/audio stream:

1. the explicit PipeWire target connects without fallback;
2. transcription reaches streaming/healthy state and finalized speech produces meeting context;
3. a question addressed to the user appears in the overlay;
4. answer text streams while transcription continues;
5. dismiss cancels the current suggestion;
6. copy affects only the local clipboard;
7. stop listening shuts capture/transcription down cooperatively;
8. provider failure appears as degraded/error state without exposing the API key;
9. no raw-audio or transcript file is created by SIDECAR.

The daemon summary is metadata-only: queue/drop counts, finalized/duplicate/question counts, provider failure counts, answer delta counts, and first-answer latency. It intentionally does not print transcript or answer text.

## Wayland overlay smoke test

Run `sidecar meet` plus `sidecar-overlay` inside the actual target compositor (for example Hyprland) and verify layer-shell placement, top-right anchoring, keyboard interaction, clipboard copy, pin behavior, dismiss/stop controls, daemon restart/reconnect, and overlay restart/reconnect.

## Remote MCP smoke test

Remote MCP is disabled unless `--remote-mcp` is supplied. Generate a high-entropy bearer token locally and keep it out of shell history where practical:

```bash
export SIDECAR_REMOTE_MCP_TOKEN="$(openssl rand -hex 32)"
sidecar meet \
  --target '<NODE_NAME_OR_OBJECT_SERIAL>' \
  --answer-model '<RESPONSES_API_MODEL>' \
  --remote-mcp
```

The daemon prints only the loopback MCP address, for example `127.0.0.1:43123`; it never prints the token. Point OpenAI Secure MCP Tunnel, or another authenticated TLS tunnel with equivalent guarantees, at `http://127.0.0.1:<PORT>/mcp` and configure the same bearer token on the client/tunnel side. Do not publish or directly port-forward the loopback endpoint.

From the supported cloud client, verify requests equivalent to:

- “What did they just ask me?”
- “What did Sarah say earlier about deployment?”

Then revoke/stop the local bridge and confirm later cloud requests fail. Also confirm the remote tool catalogue contains only read-only meeting tools and no raw-audio, credential, shell, process, or file-write capability.

Because Secure MCP Tunnel and ChatGPT connection setup can change independently of this repository, follow the current official OpenAI tunnel/client setup for the external leg rather than copying an old CLI invocation from this document.
