# Engineer Handoff — SIDECAR v1 Foundation

## Goal

Build the first usable vertical slice of SIDECAR on Arch Linux without turning it into a broad meeting product.

The end-state vertical slice is:

```text
selected PipeWire playback node
  -> PCM frames
  -> transcription provider
  -> normalized final transcript segments
  -> rolling local transcript
  -> question candidate detector
  -> answer request
  -> streamed private suggestion
```

The MCP server and graphical overlay come after the transcript pipeline is reliable.

## First assignment

Implement **Milestone 1 — deterministic capture boundary** from `docs/MILESTONES.md`.

Do not start the OpenAI integration before the capture boundary and fixture replay contract are tested.

### Required deliverables

1. A small daemon executable with:
   - `sidecar doctor`
   - `sidecar devices`
   - `sidecar capture --target <pipewire-node>`
   - `sidecar replay <fixture>`

2. PipeWire capture adapter:
   - use `pw-record`/`pw-cat` as the initial implementation boundary;
   - stream raw PCM through stdout rather than recording to a file;
   - select an explicit target node;
   - surface process failures clearly;
   - support clean cancellation.

3. Audio frame contract:
   - mono PCM16;
   - explicit sample rate in metadata;
   - fixed-duration frame chunks chosen by the implementation and documented;
   - timestamps based on a monotonic clock.

4. Fixture replay path that produces the exact same audio-frame events without PipeWire.

5. Tests covering process argument construction, frame boundaries, cancellation, malformed fixture data, and end-of-stream behavior.

### Acceptance criteria

- Unit/integration tests pass without a microphone, browser, PipeWire daemon, or API key.
- A fixture can be replayed deterministically twice with identical frame metadata and payload hashes.
- Real capture smoke test on Arch can stream from a chosen PipeWire target for at least 30 seconds without writing audio to disk.
- Ctrl-C terminates the daemon and capture subprocess without leaving children behind.
- No transcript/STT/model dependency is introduced in this milestone.

## Architecture constraints

Do not couple capture to a particular transcription vendor. Downstream code consumes the `audio.frame.v1` event defined in `spec/events.schema.json`.

Do not use a browser extension for v1 capture. System-level PipeWire capture is the Linux product boundary.

Do not use a virtual microphone or inject anything into the user's input path.

## After Milestone 1

Proceed in order:

1. streaming transcription;
2. rolling transcript store;
3. question detection;
4. answer orchestration;
5. read-only MCP server;
6. private overlay;
7. latency/privacy/evaluation hardening.
