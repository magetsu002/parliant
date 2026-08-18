# ADR 0001 — Local-first daemon

Status: Accepted

## Decision

SIDECAR v1 runs a local daemon that owns capture, transcript state, provider credentials, question detection, orchestration, and MCP context serving. The UI is a separate local client.

## Why

Meeting data is sensitive, Linux system-audio capture is inherently local, and a daemon gives one place to enforce retention, credential, IPC, and cancellation semantics.

## Consequences

- no cloud control plane is required for v1;
- the UI can crash/restart without losing the active meeting state;
- cross-platform work will require platform-specific capture adapters later;
- local packaging and process lifecycle become first-class engineering work.
