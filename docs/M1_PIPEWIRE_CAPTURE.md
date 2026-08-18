# M1 PipeWire capture

M1 is the first executable SIDECAR vertical slice. It captures audio only. It does not transcribe, detect questions, call OpenAI, expose MCP, generate answers, or render an overlay.

## Capture contract

- Linux uses PipeWire through `pipewire-rs`.
- The caller must provide an explicit `target.object` value (`node.name` or `object.serial`).
- A target can be treated as a source, or as a sink whose monitor ports are captured with `--sink-monitor`.
- SIDECAR sets PipeWire's `DONT_RECONNECT` stream flag and the `node.dont-reconnect` / `node.dont-fallback` properties. Loss of the selected target is surfaced instead of silently switching to another source.
- Captured bytes are normalized into owned `AudioFrame` values carrying sequence number, monotonic session timestamp, sample rate, channel count, and sample representation.
- M1 requests native-rate/native-channel interleaved F32LE audio. Future provider-specific conversion belongs at the provider boundary, not in this capture contract.
- The frame queue is bounded. If downstream work is slow, capture drops the newest frame and increments `dropped_full` rather than blocking capture or growing memory without bound.
- SIGINT and SIGTERM set the same cooperative cancellation token used by programmatic shutdown.
- Raw audio is not written to disk. The M1 daemon consumer only counts frames and bytes.

## Build and deterministic verification

On Arch Linux, the required development files are provided by the normal `pipewire` package.

```bash
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

The tests use deterministic in-memory audio fixtures. They do not require a microphone, meeting, PipeWire daemon, or paid API.

## Real PipeWire smoke test

Find the PipeWire object you intend to capture, then inspect it to obtain its exact `node.name` or `object.serial`:

```bash
wpctl status
wpctl inspect <ID>
```

Capture a source by its exact target value:

```bash
cargo run -p sidecar-daemon -- capture --target '<TARGET>'
```

Capture playback from an explicitly selected sink:

```bash
cargo run -p sidecar-daemon -- capture --target '<SINK_TARGET>' --sink-monitor
```

While audio is flowing, the daemon should report `connecting`, a negotiated format, and `streaming`. Stop it with Ctrl-C and verify a clean `Cancelled` stop plus the metadata-only capture summary. Removing the selected node while streaming should produce `selected source lost` and a non-zero exit instead of reconnecting elsewhere.

A successful build or fixture replay is **not** evidence that PipeWire runtime capture succeeded. Runtime success is claimed only after the command above has actually captured frames from a live PipeWire graph.
