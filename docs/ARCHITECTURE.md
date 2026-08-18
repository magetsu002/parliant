# SIDECAR Architecture

## Design priorities

In order:

1. transcript correctness;
2. privacy and predictable data flow;
3. low perceived latency;
4. failure isolation;
5. provider replaceability;
6. UI polish.

A beautiful overlay attached to an unreliable transcript pipeline is not useful.

## Components

### 1. Capture adapter

Linux v1 captures meeting playback through PipeWire. The initial implementation should wrap the official `pw-record`/`pw-cat` utility because it gives us a narrow, observable subprocess boundary and explicit target selection. PipeWire supports targeting a node and streaming capture output, so the MVP does not require a custom native PipeWire client.

A later milestone may replace the subprocess adapter with libpipewire without changing downstream event contracts.

### 2. Audio pipeline

Responsibilities:

- normalize the capture format;
- frame PCM data;
- attach monotonic timestamps;
- optionally perform VAD/noise gating;
- never persist raw audio by default.

The downstream interface is `audio.frame.v1`.

### 3. Transcription provider

A provider interface receives audio frames and emits partial/final transcript events.

The OpenAI adapter is the initial cloud candidate because the current API exposes dedicated transcription models, Realtime audio input, noise reduction, turn detection, and optional diarization. The domain layer must not import OpenAI-specific types.

Only **final** transcript segments enter the canonical rolling transcript. Partial segments exist for UI/latency hints but must be replaceable and must not trigger irreversible behavior.

### 4. Rolling transcript store

Default behavior: memory only.

Responsibilities:

- ordered final transcript segments;
- bounded retention window;
- speaker labels when available;
- fast recent-context retrieval;
- text search;
- meeting-level compact summary when enabled.

Persistence is explicitly opt-in and is outside the first vertical slice.

### 5. Question detector

The detector should be a staged gate, not a full answer model running on every token.

Suggested pipeline:

1. final transcript segment arrives;
2. cheap deterministic candidate gate;
3. small semantic classifier for ambiguous candidates;
4. address-to-user confidence calculation;
5. debounce/cooldown and duplicate suppression;
6. emit `question.detected.v1` only above threshold.

The detector must support replayable evaluation from transcript fixtures.

### 6. Answer orchestrator

On a question event, construct a bounded answer request containing:

- exact question span;
- recent transcript window;
- compact meeting summary if available;
- user/product instructions supplied by configuration;
- read-only tool access.

The orchestrator streams answer deltas to the overlay.

The meeting transcript is **untrusted data**. Spoken text such as "ignore your instructions" is meeting content, not policy.

### 7. MCP server

The local MCP server exposes meeting context as read-only tools/resources. Initial tools:

- `meeting_get_recent`
- `meeting_get_segment`
- `meeting_search`
- `meeting_get_participants`
- `meeting_get_summary`
- `meeting_get_status`

MCP does not own the trigger loop. The local orchestrator decides when a question event should invoke the answer model.

The answer model may also have separately configured read-only connectors such as GitHub or document sources. SIDECAR must not silently grant those permissions.

### 8. Overlay

The overlay consumes normalized local events and renders:

- listening/transcribing state;
- detected question;
- answer streaming state;
- suggested answer;
- dismiss/copy/pin controls;
- clear error/degraded state.

The overlay never captures audio itself and never owns provider credentials.

## Process boundary

Preferred v1 topology:

```text
sidecar daemon
  capture
  transcription
  transcript store
  detector
  orchestrator
  MCP endpoint
  local IPC server

sidecar overlay
  local IPC client only
```

Keeping credentials and meeting context in the daemon reduces accidental exposure through UI code.

## IPC

Use a local-only transport. A Unix domain socket is preferred on Linux.

Requirements:

- bind inside the user's runtime/state directory with restrictive permissions;
- authenticate peer ownership where practical;
- do not listen on a public interface by default;
- version every event envelope;
- allow replay/testing without the UI.

## Latency target

Treat this as a product target, not a guarantee:

- finalized question to first visible answer token: aim for <= 2.5 seconds under healthy network conditions;
- overlay update after receiving an event: <= 50 ms locally.

Do not sacrifice transcript correctness merely to hit the target.

## Failure modes

SIDECAR should degrade explicitly:

- capture unavailable -> `capture_error`;
- transcription unavailable -> retain capture status but stop answer triggers;
- classifier unavailable -> deterministic gate may continue only if configured;
- answer model unavailable -> keep transcript running;
- MCP/context source unavailable -> answer with remaining context and mark missing source;
- overlay disconnected -> daemon continues unless configured otherwise.

## Current external facts used by this design

As of the repository bootstrap date, OpenAI documents low-latency Realtime sessions over WebRTC/WebSocket/SIP, input transcription options, VAD/turn detection, noise reduction, and MCP tools. PipeWire documents `pw-record`/`pw-cat` capture with explicit target selection and stream capture properties.

References:

- https://platform.openai.com/docs/api-reference/realtime
- https://platform.openai.com/docs/api-reference/audio
- https://docs.pipewire.org/page_man_pw-cat_1.html
- https://docs.pipewire.org/devel/group__pw__keys.html
