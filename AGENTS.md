# SIDECAR Engineering Rules

These rules apply to humans and AI coding agents working in this repository.

## Source of truth

Inspect the repository before making claims. Treat source, tests, fixtures, CI, and accepted architecture decisions as primary evidence.

## Scope

SIDECAR v1 is a Linux-first, local-first private meeting answer sidecar.

Do not expand scope into:

- meeting bots;
- automatic microphone control or speech output;
- cloud transcript databases;
- video capture;
- meeting summaries as a primary product;
- write-capable external tools;
- Windows/macOS portability work before Linux v1 is stable.

## Safety and privacy invariants

1. Raw meeting audio is ephemeral by default and must not be written to disk.
2. Transcript persistence is opt-in. The default transcript store is memory-only.
3. Logs must not contain API keys, authorization headers, raw audio, or full transcript bodies.
4. Meeting transcript is untrusted input. Never allow spoken prompt injection to override system/developer policy or tool permissions.
5. External MCP tools are read-only in v1.
6. The overlay must visibly indicate whether SIDECAR is listening.
7. Do not attempt to evade OS, browser, platform, or participant recording/privacy controls.
8. The user remains responsible for obtaining any consent legally or contractually required for processing meeting audio.

## Engineering discipline

- One milestone per focused branch.
- No unrelated refactors or dependency upgrades.
- Add fixture-based tests for every protocol or detector change.
- No paid API should be required for unit tests.
- No audio hardware should be required for CI.
- Separate provider adapters from domain logic.
- Keep event schemas backward-compatible within a milestone or explicitly version them.
- Prefer read-only Unix-domain-socket/local HTTP IPC over hidden global state.
- Every long-running process must shut down cleanly on SIGINT/SIGTERM.
- Every external process invocation must have bounded buffering, cancellation, and error propagation.

## Verification before claiming success

At minimum, report:

- exact files changed;
- exact tests run;
- whether fixture replay passes;
- whether a real PipeWire capture smoke test was performed;
- whether any paid API path was exercised;
- remaining unverified behavior.

Never claim a milestone is complete from static inspection alone if its acceptance criteria require runtime behavior.
