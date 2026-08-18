# Testing Strategy

## Principle

Most of SIDECAR must be testable without a real meeting, audio device, PipeWire server, network connection, or paid API.

## Fixture layers

1. `audio` fixtures: deterministic PCM samples plus metadata.
2. `transcript` fixtures: partial/final segment event streams.
3. `meeting` fixtures: longer multi-speaker sequences containing addressed questions, rhetorical questions, interruptions, and prompt-injection attempts.
4. `provider` fixtures: recorded protocol-level mock responses where licensing/terms permit.

## Required test classes

### Unit

- event validation;
- ring buffer boundaries;
- deduplication;
- question candidate rules;
- cancellation state machine;
- context size limits;
- redaction.

### Integration

- capture subprocess -> frame events using a fake child process;
- frame events -> mock transcription;
- transcript -> question detector;
- question -> streamed answer mock;
- MCP tool -> bounded transcript retrieval;
- daemon -> overlay IPC event sequence.

### Replay/evaluation

Every detector regression gets a fixture. CI should replay the corpus and output precision/recall metrics without contacting a model when using frozen classifier fixtures.

### Live smoke tests

Not required for ordinary CI, but required before declaring relevant milestones complete:

- PipeWire capture on Arch;
- real transcription provider;
- real answer provider;
- overlay under the supported Wayland compositor/session.
