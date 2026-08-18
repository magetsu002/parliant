# ADR 0002 — MCP exposes transcript context, not raw audio

Status: Accepted

## Decision

The SIDECAR MCP surface exposes normalized meeting transcript/state. Raw audio is not an MCP resource or tool payload in v1.

## Why

The answer model normally needs semantic meeting context, not the full original audio stream. Keeping audio outside MCP reduces bandwidth, privacy exposure, context ambiguity, and accidental persistence.

The transcription pipeline remains responsible for audio-specific uncertainty. The answer layer receives transcript confidence/metadata when useful.

## Consequences

- transcript quality is a critical dependency;
- audio-specific re-analysis is not available through MCP in v1;
- the MCP API stays compact and useful to multiple model providers;
- raw audio remains ephemeral by default.
