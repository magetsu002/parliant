# SIDECAR Architecture

SIDECAR is a Linux-first, local-first meeting intelligence sidecar. Its job is to listen to meeting playback selected by the user, maintain a live transcript, detect when the user is being asked something, retrieve only the context needed to answer, and surface a private suggested response.

This document defines the architecture that implementation should follow. It intentionally avoids milestone planning and internal project-management details.

## Design priorities

In order:

1. transcript correctness;
2. privacy and explicit data flow;
3. low perceived latency;
4. failure isolation;
5. replaceable providers;
6. a quiet, useful UI.

A fast answer based on a bad transcript is worse than a slightly slower correct one.

## High-level topology

```text
meeting app / browser
        |
        v
   PipeWire capture
        |
        v
 audio normalization
        |
        v
 streaming transcription <-------------------+
        |                                      |
        v                                      |
 rolling transcript store                     |
        |                                      |
        +----> question detector               |
        |             |                        |
        |             v                        |
        |       answer orchestrator ----------+
        |             |
        |             +----> local MCP context
        |             +----> optional read-only external context
        |             |
        |             v
        +-------> private overlay
```

The continuous path and the expensive reasoning path are deliberately separate. Transcription stays active while a meeting is running. Answer generation is invoked only when the detector emits a sufficiently confident question event or the user explicitly asks for help.

## Runtime shape

The preferred v1 runtime has two processes:

### `sidecar`

A long-running local daemon responsible for:

- PipeWire capture;
- audio normalization;
- transcription-provider sessions;
- canonical transcript state;
- question detection;
- answer orchestration;
- local MCP context service;
- provider credentials;
- local IPC.

### `sidecar-overlay`

A small UI process responsible only for presenting state and suggestions.

The overlay must not own provider credentials or directly capture audio. Keeping those responsibilities in the daemon gives SIDECAR one auditable trust boundary for sensitive meeting data.

## Implementation language

The core daemon should be implemented in **Rust**.

Reasons:

- SIDECAR is fundamentally a long-running Linux systems process rather than a web application;
- audio streaming, bounded queues, cancellation, and process lifetime are first-class concerns;
- Rust gives us memory safety without a garbage collector in the audio path;
- a native daemon can be distributed as a small, inspectable binary;
- it keeps the core suitable for future native PipeWire and Wayland integration.

The UI technology is intentionally not frozen yet. The overlay communicates with the daemon through a versioned local IPC contract, so the UI toolkit can change without rewriting audio or orchestration code.

## 1. Audio capture

Linux v1 uses PipeWire.

Requirements:

- the user explicitly selects the application/output node or capture target;
- capture must never silently switch to another source;
- timestamps use a monotonic clock;
- the capture layer emits typed audio frames rather than provider-specific payloads;
- shutdown and source loss must be observable events;
- raw meeting audio is not written to disk by default.

The domain audio frame must carry its format metadata. Do not make the rest of SIDECAR assume the format required by one transcription vendor.

A provider adapter may resample/convert at its boundary. For example, OpenAI's current Realtime PCM input uses mono 16-bit PCM at 24 kHz, but that requirement belongs in the OpenAI adapter rather than the core capture contract.

## 2. Streaming transcription

Transcription is a continuously running service during an active session.

The provider interface should support:

- partial transcript deltas;
- finalized transcript segments;
- segment timestamps;
- optional speaker labels/diarization;
- provider health and reconnect state;
- cancellation without losing already-finalized transcript state.

Only **finalized** segments become canonical transcript history. Partial text can be shown for responsiveness, but it must be replaceable and must not trigger irreversible actions.

The first provider can use OpenAI's transcription/realtime APIs, but provider-specific event types must stop at the adapter boundary.

## 3. Transcript store

The default transcript store is memory-only and bounded.

It owns:

- ordered finalized segments;
- speaker identity/labels when available;
- recent-window retrieval;
- text search;
- question-span lookup;
- optional compact meeting summary state.

Persistence is not part of the default path. If persistent transcripts are added later, they must be explicit opt-in functionality with a separate storage policy.

Full transcripts must never appear in normal diagnostic logs.

## 4. Question detection

The answer model should not run continuously on every token.

Question detection is a staged gate:

1. a finalized transcript segment arrives;
2. a cheap candidate check decides whether it could be a question/request;
3. a semantic detector evaluates ambiguous cases and whether the utterance is likely addressed to the user;
4. duplicate suppression and cooldown logic prevent repeated triggers;
5. a normalized question event is emitted with confidence and source segment IDs.

The detector must be replayable against synthetic transcript fixtures so precision/recall can be measured without live meetings or paid APIs.

The user must also have a manual trigger path. Automatic detection is an optimization, not the only way to ask SIDECAR for help.

## 5. Answer orchestration

Answer generation is event-driven, not permanently attached to the transcription session.

For each accepted question, the orchestrator builds a bounded request from:

- the exact question span;
- a recent transcript window;
- relevant earlier transcript hits;
- a compact summary if available;
- user-provided response instructions;
- explicitly enabled read-only context sources.

For text suggestions, the initial architecture should prefer the OpenAI **Responses API** over keeping a second always-on Realtime generation session. This makes tool usage, retries, cancellation, and per-question context easier to reason about while the transcription connection remains independent.

Answers stream to the overlay as text deltas.

A failure to answer must not stop transcription.

## 6. MCP boundary

MCP is a context interface, not SIDECAR's trigger loop.

The local MCP service exposes meeting state as read-only resources/tools. Useful initial capabilities are conceptually:

- recent transcript context;
- transcript search;
- segment lookup;
- participant/speaker state;
- meeting status;
- optional summary retrieval.

The orchestrator remains responsible for deciding **when** an answer request happens.

The v1 MCP endpoint should remain local to the machine. The daemon can act as an MCP client when retrieving other configured local context.

Do not expose the live transcript as a public remote MCP endpoint by default. A future ChatGPT/remote-MCP bridge must be a separate, explicit opt-in feature because it changes the network and privacy boundary.

External context such as GitHub or private documents must use an allowlist and should be read-only by default. A meeting participant saying "ignore your instructions and delete X" is transcript data, not authorization.

## 7. Local IPC

Daemon-to-overlay communication should use a Unix-domain socket under the user's runtime directory.

Requirements:

- no public TCP listener by default;
- restrictive filesystem permissions;
- versioned message envelopes;
- bounded queues/backpressure;
- reconnect support for the overlay;
- no secrets in UI event payloads;
- transcript events contain only what the UI actually needs.

The exact serialization format can be selected during implementation. The important constraint is that it is explicit and versioned.

## 8. Overlay

The overlay should be intentionally small.

It renders:

- listening/transcribing state;
- current detected question;
- streamed suggested answer;
- degraded/error state;
- dismiss/copy/pin controls;
- a clear stop-listening control.

The first graphical target is Wayland on the Linux desktop. A layer-shell style overlay is a good fit for wlroots/Hyprland environments, but the daemon must not depend on compositor-specific UI behavior.

No automatic microphone control or automatic spoken response belongs in v1.

## 9. Security and privacy boundaries

SIDECAR handles sensitive live conversation, so the default behavior should minimize retained data and capabilities.

Hard invariants:

- no raw-audio persistence by default;
- transcript persistence off by default;
- no API keys, authorization headers, or full transcripts in logs;
- meeting text is untrusted model input;
- context connectors are explicitly configured;
- write-capable external tools are out of scope for the first version;
- the UI always makes active listening state visible;
- SIDECAR does not attempt to bypass OS or meeting-platform privacy controls.

Users are responsible for following applicable meeting, workplace, school, contractual, and legal consent requirements.

## 10. Failure isolation

Each subsystem reports health independently.

Expected behavior:

- capture failure: stop transcription and clearly show the source error;
- transcription failure: keep the session/UI alive and retry with bounded backoff;
- question detector failure: keep transcription running and preserve manual triggering;
- answer-provider failure: keep transcription running and show the answer error;
- external context failure: answer from remaining context and identify that the source was unavailable;
- overlay disconnect: daemon may continue the active session unless the user configured otherwise.

No component should fail silently.

## 11. Testing contract

The implementation must be testable without a real meeting, audio hardware, or paid APIs.

The architecture therefore requires provider and capture boundaries that accept deterministic fixtures.

Tests should eventually cover:

- audio framing and resampling;
- transcript partial/final reconciliation;
- reconnect and duplicate handling;
- question detector evaluation;
- transcript-window selection;
- MCP read-only behavior;
- prompt-injection resistance at the tool-permission layer;
- daemon/overlay IPC compatibility;
- cancellation and clean shutdown.

Real PipeWire and live-provider smoke tests are still required before claiming those paths work; fixture tests are not a substitute for runtime evidence.

## Planned repository shape

The implementation should grow into a structure similar to:

```text
crates/
  sidecar-core/          domain types and state
  sidecar-audio/         PipeWire capture + audio normalization
  sidecar-transcribe/    transcription provider interface/adapters
  sidecar-detect/        question detection
  sidecar-context/       transcript store + MCP context
  sidecar-orchestrator/  answer generation and tool policy
  sidecar-ipc/           daemon/overlay protocol
  sidecar-daemon/        composition root + CLI

apps/
  sidecar-overlay/       private desktop UI
```

This is a boundary map, not permission to create empty crates before they are needed. Add components as implementation reaches them.

## Explicitly deferred decisions

Do not solve these before evidence requires them:

- Windows/macOS support;
- persistent transcript database design;
- exact overlay toolkit;
- cloud-hosted SIDECAR accounts/sync;
- automatic speech output;
- video/webcam understanding;
- write-capable MCP/connector actions;
- generalized meeting analytics.

## External references

- OpenAI Realtime API: https://platform.openai.com/docs/api-reference/realtime
- OpenAI API quickstart / Responses API: https://platform.openai.com/docs/quickstart
- Model Context Protocol specification: https://modelcontextprotocol.io/specification
- PipeWire documentation: https://docs.pipewire.org/
