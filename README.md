# SIDECAR

**Intelligence beside you.**

SIDECAR is a local-first meeting intelligence tool for Linux. It is designed to capture meeting playback selected by the user, transcribe it in real time, detect when the user is being asked something, retrieve relevant context, and surface a private suggested answer without joining the meeting as a bot.

## Principles

- Linux/Arch first.
- Local-first state and explicit data flow.
- No raw-audio persistence by default.
- Transcript persistence off by default.
- Private suggestions stay on the user's machine.
- Answer generation runs only when needed instead of continuously.
- External context is explicit and read-only by default.
- No automatic speaking into meetings.
- Real meeting transcripts, credentials, and customer data must never be committed to this repository.

## Architecture

SIDECAR separates the always-on transcription path from the expensive reasoning path:

```text
meeting audio -> PipeWire -> transcription -> transcript store
                                      |
                                      v
                              question detector
                                      |
                                      v
                              answer orchestrator
                                 /          \
                         local MCP       allowed context
                                 \          /
                                      v
                               private overlay
```

The detailed architecture is documented in [`docs/ARCHITECTURE.md`](./docs/ARCHITECTURE.md).

## Status

Implementation has started milestone-by-milestone. M1 adds the Rust capture foundation: explicit PipeWire target selection, bounded in-memory audio frames, deterministic replay tests, cancellation, signal-driven shutdown, and observable source loss. Later transcription, meeting-state, reasoning, MCP, and overlay milestones are not implemented yet.

See [`docs/M1_PIPEWIRE_CAPTURE.md`](./docs/M1_PIPEWIRE_CAPTURE.md) for the exact capture and runtime-verification contract.

## Contributing

See [`CONTRIBUTING.md`](./CONTRIBUTING.md) before opening a pull request. Security-sensitive reports should follow [`SECURITY.md`](./SECURITY.md).

## License

No open-source license has been granted yet. The repository is publicly viewable, but the code and documentation remain all-rights-reserved until a license is chosen.
