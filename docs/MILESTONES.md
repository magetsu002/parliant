# SIDECAR v1 Milestones

## M0 — Foundation

Status: defined by this bootstrap repository.

Deliverables:

- product boundary;
- architecture;
- event schema;
- privacy/threat model;
- engineering rules;
- deterministic fixture strategy.

## M1 — Deterministic PipeWire capture

Deliver a CLI/daemon capture boundary and fixture replay path.

Exit criteria are defined in `ENGINEER_HANDOFF.md`.

## M2 — Streaming transcription

Deliver a provider-neutral transcription interface and one production adapter.

Required behavior:

- partial and final segments are distinct;
- final segments receive stable IDs;
- reconnect/backoff does not duplicate final segments;
- timestamps remain monotonic;
- language/config hints are configurable;
- API secrets never appear in logs;
- fixture/mock provider works in CI.

Do not add question detection in this milestone.

## M3 — Transcript store and question detector

Deliver:

- bounded in-memory transcript store;
- recent-window retrieval;
- search;
- candidate question gate;
- semantic classification adapter;
- duplicate suppression;
- replayable evaluation corpus.

Track false positives and false negatives separately.

## M4 — Answer orchestration

Deliver:

- bounded context assembly;
- streaming answer output;
- cancellation if a newer question supersedes the current one;
- degraded answer state when a context source is unavailable;
- prompt-injection isolation tests.

No automatic speaking.

## M5 — Read-only MCP context server

Expose the meeting transcript/state through a standards-compliant MCP endpoint.

All v1 tools must be read-only. Add contract tests for tool schemas and bounded responses.

## M6 — Private Linux overlay

Deliver a minimal overlay showing status, question, and streamed answer.

Requirements:

- private local IPC only;
- no credentials in UI process;
- dismiss/copy/pin;
- keyboard shortcut;
- visible listening indicator;
- no automatic focus stealing while typing.

## M7 — Hardening and evals

Measure and improve:

- word error rate on representative technical meetings;
- speaker/turn accuracy where available;
- question precision/recall;
- addressed-to-user precision/recall;
- question-end to first-token latency;
- memory growth over long meetings;
- reconnect behavior;
- prompt injection resilience;
- accidental persistence/logging.

Only after M7 should cross-platform work be considered.
