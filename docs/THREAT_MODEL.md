# Threat Model

## Assets

- live meeting audio;
- transcript text;
- API credentials;
- external connector credentials;
- retrieved private source material;
- suggested answers;
- user identity/context configuration.

## Primary threats

### Spoken prompt injection

A participant can intentionally or accidentally say text that resembles model instructions.

Mitigation: transcript is always wrapped/labeled as untrusted meeting content. It cannot modify system instructions, tool approval policy, allowed-tool lists, retention policy, or secret handling.

### Over-privileged tools

A useful answer model may have access to GitHub/docs/etc.

Mitigation: v1 permits read-only tools only. Mutation is outside scope. Tool access is allowlisted per session.

### Transcript exfiltration through logs

Mitigation: structured logs record IDs, sizes, timings, status, and hashes where useful, not full transcript bodies.

### Local IPC snooping

Mitigation: local-only Unix-domain socket, restrictive filesystem permissions, peer ownership checks where practical, no TCP listener by default.

### Secret leakage

Mitigation: secrets supplied by environment/OS secret storage; never echoed; redact authorization headers; no secrets in crash reports.

### Child-process escape/failure

The initial PipeWire adapter invokes `pw-record`/`pw-cat`.

Mitigation: no shell interpolation; argv array only; explicit executable resolution; bounded buffers; child process group cleanup; input target validated as data, not shell syntax.

### Accidental recording persistence

Mitigation: capture streams to pipes. Any code path writing raw audio requires a separate explicit feature flag and future security review.

### Unbounded context / denial of service

Mitigation: bounded transcript ring buffer, bounded MCP results, per-request limits, cancellation, backpressure, and maximum answer concurrency of one per user by default.
